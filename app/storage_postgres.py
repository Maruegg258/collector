from __future__ import annotations

import threading
import time
from typing import Iterable

import psycopg
from psycopg.rows import dict_row

from .storage import TradeRecord


class PostgresTradeStore:
    backend_name = "postgres"
    db_path: str | None = None

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        self._lock = threading.RLock()
        self.conn = psycopg.connect(database_url, row_factory=dict_row)
        self.init_schema()

    def init_schema(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS trades (
                coin TEXT NOT NULL,
                time_ms BIGINT NOT NULL,
                tid BIGINT NOT NULL,
                side TEXT NOT NULL CHECK (side IN ('B', 'A')),
                px DOUBLE PRECISION NOT NULL,
                sz DOUBLE PRECISION NOT NULL,
                notional_usdc DOUBLE PRECISION NOT NULL,
                signed_notional_usdc DOUBLE PRECISION NOT NULL,
                trade_hash TEXT,
                inserted_at_ms BIGINT NOT NULL,
                PRIMARY KEY (time_ms, coin, tid)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_trades_coin_time ON trades (coin, time_ms)",
            """
            CREATE TABLE IF NOT EXISTS collector_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS coverage_gaps (
                id BIGSERIAL PRIMARY KEY,
                coin TEXT NOT NULL,
                start_ms BIGINT NOT NULL,
                end_ms BIGINT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('HEALED', 'UNRESOLVED')),
                reason TEXT NOT NULL,
                recovery_earliest_ms BIGINT,
                recovery_latest_ms BIGINT,
                recovered_trade_count INTEGER NOT NULL DEFAULT 0,
                created_at_ms BIGINT NOT NULL,
                UNIQUE(coin, start_ms, end_ms)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_gaps_coin_time ON coverage_gaps (coin, start_ms, end_ms)",
            """
            CREATE TABLE IF NOT EXISTS aggregate_buckets (
                coin TEXT NOT NULL,
                granularity TEXT NOT NULL CHECK (granularity IN ('4h', '1d')),
                start_ms BIGINT NOT NULL,
                end_ms BIGINT NOT NULL,
                trade_count BIGINT NOT NULL,
                buy_notional_usdc DOUBLE PRECISION NOT NULL,
                sell_notional_usdc DOUBLE PRECISION NOT NULL,
                net_delta_usdc DOUBLE PRECISION NOT NULL,
                base_delta_hype DOUBLE PRECISION NOT NULL,
                total_notional_usdc DOUBLE PRECISION NOT NULL,
                delta_ratio DOUBLE PRECISION,
                first_trade_time_ms BIGINT,
                last_trade_time_ms BIGINT,
                complete BOOLEAN NOT NULL,
                quality TEXT NOT NULL,
                unresolved_gap_count INTEGER NOT NULL,
                created_at_ms BIGINT NOT NULL,
                updated_at_ms BIGINT NOT NULL,
                PRIMARY KEY (coin, granularity, start_ms)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_aggregate_coin_time ON aggregate_buckets (coin, granularity, start_ms)",
        ]
        with self._lock, self.conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
            self.conn.commit()

    def insert_many(self, records: Iterable[TradeRecord]) -> tuple[int, int]:
        rows = [
            (
                r.coin, r.time_ms, r.tid, r.side, r.px, r.sz,
                r.notional_usdc, r.signed_notional_usdc, r.trade_hash,
                int(time.time() * 1000),
            )
            for r in records
        ]
        if not rows:
            return 0, 0
        sql = """
            INSERT INTO trades (
                coin, time_ms, tid, side, px, sz,
                notional_usdc, signed_notional_usdc, trade_hash, inserted_at_ms
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time_ms, coin, tid) DO NOTHING
        """
        with self._lock, self.conn.cursor() as cur:
            cur.executemany(sql, rows)
            inserted = max(0, cur.rowcount)
            self.conn.commit()
        return inserted, len(rows) - inserted

    def count(self, coin: str | None = None) -> int:
        with self._lock, self.conn.cursor() as cur:
            if coin is None:
                cur.execute("SELECT COUNT(*) AS n FROM trades")
            else:
                cur.execute("SELECT COUNT(*) AS n FROM trades WHERE coin = %s", (coin,))
            row = cur.fetchone()
        return int(row["n"])

    def first_trade_time_ms(self, coin: str | None = None) -> int | None:
        with self._lock, self.conn.cursor() as cur:
            if coin is None:
                cur.execute("SELECT MIN(time_ms) AS t FROM trades")
            else:
                cur.execute("SELECT MIN(time_ms) AS t FROM trades WHERE coin = %s", (coin,))
            row = cur.fetchone()
        return None if row["t"] is None else int(row["t"])

    def latest_trade_time_ms(self, coin: str | None = None) -> int | None:
        with self._lock, self.conn.cursor() as cur:
            if coin is None:
                cur.execute("SELECT MAX(time_ms) AS t FROM trades")
            else:
                cur.execute("SELECT MAX(time_ms) AS t FROM trades WHERE coin = %s", (coin,))
            row = cur.fetchone()
        return None if row["t"] is None else int(row["t"])

    def aggregate_window(self, coin: str, start_ms: int, end_ms: int) -> dict:
        with self._lock, self.conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS trade_count,
                    COALESCE(SUM(CASE WHEN side = 'B' THEN notional_usdc ELSE 0 END), 0) AS buy_notional_usdc,
                    COALESCE(SUM(CASE WHEN side = 'A' THEN notional_usdc ELSE 0 END), 0) AS sell_notional_usdc,
                    COALESCE(SUM(signed_notional_usdc), 0) AS net_delta_usdc,
                    COALESCE(SUM(CASE WHEN side = 'B' THEN sz ELSE -sz END), 0) AS base_delta_hype,
                    COALESCE(SUM(notional_usdc), 0) AS total_notional_usdc,
                    MIN(time_ms) AS first_trade_time_ms,
                    MAX(time_ms) AS last_trade_time_ms
                FROM trades WHERE coin = %s AND time_ms >= %s AND time_ms < %s
                """,
                (coin, start_ms, end_ms),
            )
            row = cur.fetchone()
        total = float(row["total_notional_usdc"])
        net = float(row["net_delta_usdc"])
        return {
            "trade_count": int(row["trade_count"]),
            "buy_notional_usdc": float(row["buy_notional_usdc"]),
            "sell_notional_usdc": float(row["sell_notional_usdc"]),
            "net_delta_usdc": net,
            "base_delta_hype": float(row["base_delta_hype"]),
            "total_notional_usdc": total,
            "delta_ratio": (net / total) if total > 0 else None,
            "first_trade_time_ms": None if row["first_trade_time_ms"] is None else int(row["first_trade_time_ms"]),
            "last_trade_time_ms": None if row["last_trade_time_ms"] is None else int(row["last_trade_time_ms"]),
        }

    def upsert_aggregate_bucket(self, coin: str, granularity: str, start_ms: int, end_ms: int, stats: dict, *, complete: bool, quality: str, unresolved_gap_count: int) -> None:
        now_ms = int(time.time() * 1000)
        with self._lock, self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO aggregate_buckets(
                    coin, granularity, start_ms, end_ms, trade_count,
                    buy_notional_usdc, sell_notional_usdc, net_delta_usdc,
                    base_delta_hype, total_notional_usdc, delta_ratio,
                    first_trade_time_ms, last_trade_time_ms, complete, quality,
                    unresolved_gap_count, created_at_ms, updated_at_ms
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (coin, granularity, start_ms) DO UPDATE SET
                    end_ms=EXCLUDED.end_ms, trade_count=EXCLUDED.trade_count,
                    buy_notional_usdc=EXCLUDED.buy_notional_usdc,
                    sell_notional_usdc=EXCLUDED.sell_notional_usdc,
                    net_delta_usdc=EXCLUDED.net_delta_usdc,
                    base_delta_hype=EXCLUDED.base_delta_hype,
                    total_notional_usdc=EXCLUDED.total_notional_usdc,
                    delta_ratio=EXCLUDED.delta_ratio,
                    first_trade_time_ms=EXCLUDED.first_trade_time_ms,
                    last_trade_time_ms=EXCLUDED.last_trade_time_ms,
                    complete=EXCLUDED.complete, quality=EXCLUDED.quality,
                    unresolved_gap_count=EXCLUDED.unresolved_gap_count,
                    updated_at_ms=EXCLUDED.updated_at_ms
                """,
                (
                    coin, granularity, start_ms, end_ms, int(stats["trade_count"]),
                    float(stats["buy_notional_usdc"]), float(stats["sell_notional_usdc"]),
                    float(stats["net_delta_usdc"]), float(stats["base_delta_hype"]),
                    float(stats["total_notional_usdc"]), stats["delta_ratio"],
                    stats["first_trade_time_ms"], stats["last_trade_time_ms"],
                    bool(complete), quality, unresolved_gap_count, now_ms, now_ms,
                ),
            )
            self.conn.commit()

    def aggregate_bucket_count(self, coin: str, granularity: str | None = None) -> int:
        with self._lock, self.conn.cursor() as cur:
            if granularity is None:
                cur.execute("SELECT COUNT(*) AS n FROM aggregate_buckets WHERE coin = %s", (coin,))
            else:
                cur.execute("SELECT COUNT(*) AS n FROM aggregate_buckets WHERE coin = %s AND granularity = %s", (coin, granularity))
            row = cur.fetchone()
        return int(row["n"])

    def get_aggregate_bucket(self, coin: str, granularity: str, start_ms: int) -> dict | None:
        with self._lock, self.conn.cursor() as cur:
            cur.execute("SELECT * FROM aggregate_buckets WHERE coin = %s AND granularity = %s AND start_ms = %s", (coin, granularity, start_ms))
            row = cur.fetchone()
        return None if row is None else dict(row)

    def delete_trades_before(self, cutoff_ms: int, *, coin: str | None = None) -> int:
        with self._lock, self.conn.cursor() as cur:
            if coin is None:
                cur.execute("DELETE FROM trades WHERE time_ms < %s", (cutoff_ms,))
            else:
                cur.execute("DELETE FROM trades WHERE coin = %s AND time_ms < %s", (coin, cutoff_ms))
            deleted = max(0, cur.rowcount)
            self.conn.commit()
        return deleted

    def delete_gaps_before(self, cutoff_ms: int, *, coin: str | None = None) -> int:
        with self._lock, self.conn.cursor() as cur:
            if coin is None:
                cur.execute("DELETE FROM coverage_gaps WHERE end_ms < %s", (cutoff_ms,))
            else:
                cur.execute("DELETE FROM coverage_gaps WHERE coin = %s AND end_ms < %s", (coin, cutoff_ms))
            deleted = max(0, cur.rowcount)
            self.conn.commit()
        return deleted

    def checkpoint_wal(self, *, truncate: bool = False) -> tuple[int, int, int]:
        return (0, 0, 0)

    def set_meta(self, key: str, value: str | int) -> None:
        with self._lock, self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO collector_meta(key, value) VALUES(%s, %s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
                (key, str(value)),
            )
            self.conn.commit()

    def get_meta_int(self, key: str) -> int | None:
        with self._lock, self.conn.cursor() as cur:
            cur.execute("SELECT value FROM collector_meta WHERE key = %s", (key,))
            row = cur.fetchone()
        return None if row is None else int(row["value"])

    def add_gap(self, coin: str, start_ms: int, end_ms: int, *, status: str, reason: str, recovery_earliest_ms: int | None = None, recovery_latest_ms: int | None = None, recovered_trade_count: int = 0) -> None:
        if end_ms <= start_ms:
            return
        with self._lock, self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO coverage_gaps(
                    coin,start_ms,end_ms,status,reason,recovery_earliest_ms,
                    recovery_latest_ms,recovered_trade_count,created_at_ms
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (coin,start_ms,end_ms) DO UPDATE SET
                    status=EXCLUDED.status, reason=EXCLUDED.reason,
                    recovery_earliest_ms=EXCLUDED.recovery_earliest_ms,
                    recovery_latest_ms=EXCLUDED.recovery_latest_ms,
                    recovered_trade_count=EXCLUDED.recovered_trade_count,
                    created_at_ms=EXCLUDED.created_at_ms
                """,
                (coin,start_ms,end_ms,status,reason,recovery_earliest_ms,recovery_latest_ms,recovered_trade_count,int(time.time()*1000)),
            )
            self.conn.commit()

    def gaps_overlapping(self, coin: str, start_ms: int, end_ms: int, *, status: str = "UNRESOLVED") -> list[dict]:
        with self._lock, self.conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM coverage_gaps WHERE coin=%s AND status=%s AND end_ms>%s AND start_ms<%s ORDER BY start_ms",
                (coin, status, start_ms, end_ms),
            )
            rows = cur.fetchall()
        return [dict(row) for row in rows]

    def unresolved_gap_count(self, coin: str) -> int:
        with self._lock, self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM coverage_gaps WHERE coin=%s AND status='UNRESOLVED'", (coin,))
            row = cur.fetchone()
        return int(row["n"])

    def close(self) -> None:
        with self._lock:
            self.conn.close()
