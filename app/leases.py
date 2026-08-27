from __future__ import annotations

import functools
import threading
import time

import psycopg
from psycopg.rows import dict_row


def _db_retry(method):
    @functools.wraps(method)
    def wrapped(self, *args, **kwargs):
        for attempt in range(2):
            try:
                return method(self, *args, **kwargs)
            except (psycopg.OperationalError, psycopg.InterfaceError):
                if attempt == 1 or self._closed:
                    raise
                self._reconnect()
        raise RuntimeError("unreachable")
    return wrapped


class CollectorLeaseCoordinator:
    """Track per-deployment liveness so overlapping collectors form one coverage stream."""

    def __init__(self, database_url: str, instance_id: str, *, stale_after_ms: int = 15_000) -> None:
        self.database_url = database_url
        self.instance_id = instance_id
        self.stale_after_ms = stale_after_ms
        self._lock = threading.RLock()
        self._closed = False
        self.conn = self._connect()
        self._init_schema()

    def _connect(self):
        return psycopg.connect(self.database_url, row_factory=dict_row, connect_timeout=10)

    def _reconnect(self) -> None:
        with self._lock:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = self._connect()

    @_db_retry
    def _init_schema(self) -> None:
        with self._lock, self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS collector_leases (
                    instance_id TEXT PRIMARY KEY,
                    started_at_ms BIGINT NOT NULL,
                    heartbeat_ms BIGINT NOT NULL,
                    connected BOOLEAN NOT NULL,
                    last_message_at_ms BIGINT,
                    stopped_at_ms BIGINT
                )
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_collector_leases_heartbeat ON collector_leases (heartbeat_ms)")
            self.conn.commit()

    @_db_retry
    def update(self, *, connected: bool, heartbeat_ms: int | None = None, last_message_at_ms: int | None = None, stopped: bool = False) -> None:
        now_ms = int(time.time() * 1000) if heartbeat_ms is None else int(heartbeat_ms)
        with self._lock, self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO collector_leases(instance_id,started_at_ms,heartbeat_ms,connected,last_message_at_ms,stopped_at_ms)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT(instance_id) DO UPDATE SET
                    heartbeat_ms=GREATEST(collector_leases.heartbeat_ms,EXCLUDED.heartbeat_ms),
                    connected=CASE WHEN collector_leases.stopped_at_ms IS NOT NULL THEN FALSE ELSE EXCLUDED.connected END,
                    last_message_at_ms=CASE
                        WHEN collector_leases.last_message_at_ms IS NULL THEN EXCLUDED.last_message_at_ms
                        WHEN EXCLUDED.last_message_at_ms IS NULL THEN collector_leases.last_message_at_ms
                        ELSE GREATEST(collector_leases.last_message_at_ms,EXCLUDED.last_message_at_ms)
                    END,
                    stopped_at_ms=COALESCE(collector_leases.stopped_at_ms,EXCLUDED.stopped_at_ms)
                """,
                (self.instance_id, now_ms, now_ms, bool(connected), last_message_at_ms, now_ms if stopped else None),
            )
            self.conn.commit()

    @_db_retry
    def active_other_exists(self, *, now_ms: int | None = None) -> bool:
        now_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
        threshold = now_ms - self.stale_after_ms
        with self._lock, self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM collector_leases
                WHERE instance_id<>%s AND connected=TRUE AND stopped_at_ms IS NULL AND heartbeat_ms>=%s
                LIMIT 1
                """,
                (self.instance_id, threshold),
            )
            return cur.fetchone() is not None

    @_db_retry
    def latest_other_heartbeat_ms(self) -> int | None:
        with self._lock, self.conn.cursor() as cur:
            cur.execute("SELECT MAX(heartbeat_ms) AS t FROM collector_leases WHERE instance_id<>%s", (self.instance_id,))
            row = cur.fetchone()
            return None if row["t"] is None else int(row["t"])

    @_db_retry
    def snapshot(self) -> dict:
        now_ms = int(time.time() * 1000)
        with self._lock, self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS n FROM collector_leases
                WHERE connected=TRUE AND stopped_at_ms IS NULL AND heartbeat_ms>=%s
                """,
                (now_ms - self.stale_after_ms,),
            )
            active = int(cur.fetchone()["n"])
            cur.execute("SELECT heartbeat_ms,connected,last_message_at_ms,stopped_at_ms FROM collector_leases WHERE instance_id=%s", (self.instance_id,))
            own = cur.fetchone()
        return {
            "instance_id": self.instance_id,
            "active_instances": active,
            "stale_after_ms": self.stale_after_ms,
            "own_heartbeat_ms": None if own is None else int(own["heartbeat_ms"]),
            "own_connected": False if own is None else bool(own["connected"]),
            "own_last_message_at_ms": None if own is None or own["last_message_at_ms"] is None else int(own["last_message_at_ms"]),
            "own_stopped_at_ms": None if own is None or own["stopped_at_ms"] is None else int(own["stopped_at_ms"]),
        }

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            try:
                if not self.conn.closed:
                    now_ms = int(time.time() * 1000)
                    try:
                        with self.conn.cursor() as cur:
                            cur.execute(
                                """
                                UPDATE collector_leases
                                SET connected=FALSE, stopped_at_ms=COALESCE(stopped_at_ms,%s)
                                WHERE instance_id=%s
                                """,
                                (now_ms, self.instance_id),
                            )
                            self.conn.commit()
                    except (psycopg.OperationalError, psycopg.InterfaceError):
                        # Shutdown must stay terminal even if the final best-effort DB update fails.
                        pass
            finally:
                try:
                    self.conn.close()
                finally:
                    self._closed = True
