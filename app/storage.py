from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class TradeRecord:
    coin: str
    time_ms: int
    tid: int
    side: str
    px: float
    sz: float
    notional_usdc: float
    signed_notional_usdc: float
    trade_hash: str | None = None


class TradeStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.init_schema()

    def init_schema(self) -> None:
        with self._lock:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    coin TEXT NOT NULL,
                    time_ms INTEGER NOT NULL,
                    tid INTEGER NOT NULL,
                    side TEXT NOT NULL CHECK (side IN ('B', 'A')),
                    px REAL NOT NULL,
                    sz REAL NOT NULL,
                    notional_usdc REAL NOT NULL,
                    signed_notional_usdc REAL NOT NULL,
                    trade_hash TEXT,
                    inserted_at_ms INTEGER NOT NULL DEFAULT (
                        CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)
                    ),
                    PRIMARY KEY (time_ms, coin, tid)
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_coin_time ON trades (coin, time_ms)"
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS collector_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS coverage_gaps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coin TEXT NOT NULL,
                    start_ms INTEGER NOT NULL,
                    end_ms INTEGER NOT NULL,
                    status TEXT NOT NULL CHECK (status IN ('HEALED', 'UNRESOLVED')),
                    reason TEXT NOT NULL,
                    recovery_earliest_ms INTEGER,
                    recovery_latest_ms INTEGER,
                    recovered_trade_count INTEGER NOT NULL DEFAULT 0,
                    created_at_ms INTEGER NOT NULL,
                    UNIQUE(coin, start_ms, end_ms)
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_gaps_coin_time ON coverage_gaps (coin, start_ms, end_ms)"
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS aggregate_buckets (
                    coin TEXT NOT NULL,
                    granularity TEXT NOT NULL CHECK (granularity IN ('4h', '1d')),
                    start_ms INTEGER NOT NULL,
                    end_ms INTEGER NOT NULL,
                    trade_count INTEGER NOT NULL,
                    buy_notional_usdc REAL NOT NULL,
                    sell_notional_usdc REAL NOT NULL,
                    net_delta_usdc REAL NOT NULL,
                    base_delta_hype REAL NOT NULL,
                    total_notional_usdc REAL NOT NULL,
                    delta_ratio REAL,
                    first_trade_time_ms INTEGER,
                    last_trade_time_ms INTEGER,
                    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
                    quality TEXT NOT NULL,
                    unresolved_gap_count INTEGER NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (coin, granularity, start_ms)
                )
                """
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_aggregate_coin_time ON aggregate_buckets (coin, granularity, start_ms)"
            )
            self.conn.commit()

    def insert_many(self, records: Iterable[TradeRecord]) -> tuple[int, int]:
        rows = [
            (
                r.coin,
                r.time_ms,
                r.tid,
                r.side,
                r.px,
                r.sz,
                r.notional_usdc,
                r.signed_notional_usdc,
                r.trade_hash,
            )
            for r in records
        ]
        if not rows:
            return 0, 0

        with self._lock:
            before = self.conn.total_changes
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO trades (
                    coin, time_ms, tid, side, px, sz,
                    notional_usdc, signed_notional_usdc, trade_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            self.conn.commit()
            inserted = self.conn.total_changes - before
        return inserted, len(rows) - inserted

    def count(self, coin: str | None = None) -> int:
        with self._lock:
            if coin is None:
                row = self.conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()
            else:
                row = self.conn.execute(
                    "SELECT COUNT(*) AS n FROM trades WHERE coin = ?", (coin,)
                ).fetchone()
        return int(row["n"])

    def first_trade_time_ms(self, coin: str | None = None) -> int | None:
        with self._lock:
            if coin is None:
                row = self.conn.execute("SELECT MIN(time_ms) AS t FROM trades").fetchone()
            else:
                row = self.conn.execute(
                    "SELECT MIN(time_ms) AS t FROM trades WHERE coin = ?", (coin,)
                ).fetchone()
        return None if row["t"] is None else int(row["t"])

    def latest_trade_time_ms(self, coin: str | None = None) -> int | None:
        with self._lock:
            if coin is None:
                row = self.conn.execute("SELECT MAX(time_ms) AS t FROM trades").fetchone()
            else:
                row = self.conn.execute(
                    "SELECT MAX(time_ms) AS t FROM trades WHERE coin = ?", (coin,)
                ).fetchone()
        return None if row["t"] is None else int(row["t"])

    def aggregate_window(self, coin: str, start_ms: int, end_ms: int) -> dict:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT
                    COUNT(*) AS trade_count,
                    COALESCE(SUM(CASE WHEN side = 'B' THEN notional_usdc ELSE 0 END), 0) AS buy_notional_usdc,
                    COALESCE(SUM(CASE WHEN side = 'A' THEN notional_usdc ELSE 0 END), 0) AS sell_notional_usdc,
                    COALESCE(SUM(signed_notional_usdc), 0) AS net_delta_usdc,
                    COALESCE(SUM(CASE WHEN side = 'B' THEN sz ELSE -sz END), 0) AS base_delta_hype,
                    COALESCE(SUM(notional_usdc), 0) AS total_notional_usdc,
                    MIN(time_ms) AS first_trade_time_ms,
                    MAX(time_ms) AS last_trade_time_ms
                FROM trades
                WHERE coin = ? AND time_ms >= ? AND time_ms < ?
                """,
                (coin, start_ms, end_ms),
            ).fetchone()
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
            "first_trade_time_ms": None
            if row["first_trade_time_ms"] is None
            else int(row["first_trade_time_ms"]),
            "last_trade_time_ms": None
            if row["last_trade_time_ms"] is None
            else int(row["last_trade_time_ms"]),
        }

    def upsert_aggregate_bucket(
        self,
        coin: str,
        granularity: str,
        start_ms: int,
        end_ms: int,
        stats: dict,
        *,
        complete: bool,
        quality: str,
        unresolved_gap_count: int,
    ) -> None:
        now_ms = int(time.time() * 1000)
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO aggregate_buckets(
                    coin, granularity, start_ms, end_ms, trade_count,
                    buy_notional_usdc, sell_notional_usdc, net_delta_usdc,
                    base_delta_hype, total_notional_usdc, delta_ratio,
                    first_trade_time_ms, last_trade_time_ms, complete, quality,
                    unresolved_gap_count, created_at_ms, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(coin, granularity, start_ms) DO UPDATE SET
                    end_ms = excluded.end_ms,
                    trade_count = excluded.trade_count,
                    buy_notional_usdc = excluded.buy_notional_usdc,
                    sell_notional_usdc = excluded.sell_notional_usdc,
                    net_delta_usdc = excluded.net_delta_usdc,
                    base_delta_hype = excluded.base_delta_hype,
                    total_notional_usdc = excluded.total_notional_usdc,
                    delta_ratio = excluded.delta_ratio,
                    first_trade_time_ms = excluded.first_trade_time_ms,
                    last_trade_time_ms = excluded.last_trade_time_ms,
                    complete = excluded.complete,
                    quality = excluded.quality,
                    unresolved_gap_count = excluded.unresolved_gap_count,
                    updated_at_ms = excluded.updated_at_ms
                """,
                (
                    coin,
                    granularity,
                    start_ms,
                    end_ms,
                    int(stats["trade_count"]),
                    float(stats["buy_notional_usdc"]),
                    float(stats["sell_notional_usdc"]),
                    float(stats["net_delta_usdc"]),
                    float(stats["base_delta_hype"]),
                    float(stats["total_notional_usdc"]),
                    stats["delta_ratio"],
                    stats["first_trade_time_ms"],
                    stats["last_trade_time_ms"],
                    1 if complete else 0,
                    quality,
                    unresolved_gap_count,
                    now_ms,
                    now_ms,
                ),
            )
            self.conn.commit()

    def aggregate_bucket_count(
        self, coin: str, granularity: str | None = None
    ) -> int:
        with self._lock:
            if granularity is None:
                row = self.conn.execute(
                    "SELECT COUNT(*) AS n FROM aggregate_buckets WHERE coin = ?",
                    (coin,),
                ).fetchone()
            else:
                row = self.conn.execute(
                    """
                    SELECT COUNT(*) AS n FROM aggregate_buckets
                    WHERE coin = ? AND granularity = ?
                    """,
                    (coin, granularity),
                ).fetchone()
        return int(row["n"])

    def get_aggregate_bucket(
        self, coin: str, granularity: str, start_ms: int
    ) -> dict | None:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT * FROM aggregate_buckets
                WHERE coin = ? AND granularity = ? AND start_ms = ?
                """,
                (coin, granularity, start_ms),
            ).fetchone()
        return None if row is None else dict(row)

    def delete_trades_before(self, cutoff_ms: int, *, coin: str | None = None) -> int:
        with self._lock:
            before = self.conn.total_changes
            if coin is None:
                self.conn.execute("DELETE FROM trades WHERE time_ms < ?", (cutoff_ms,))
            else:
                self.conn.execute(
                    "DELETE FROM trades WHERE coin = ? AND time_ms < ?",
                    (coin, cutoff_ms),
                )
            self.conn.commit()
            return self.conn.total_changes - before

    def delete_gaps_before(self, cutoff_ms: int, *, coin: str | None = None) -> int:
        with self._lock:
            before = self.conn.total_changes
            if coin is None:
                self.conn.execute(
                    "DELETE FROM coverage_gaps WHERE end_ms < ?", (cutoff_ms,)
                )
            else:
                self.conn.execute(
                    "DELETE FROM coverage_gaps WHERE coin = ? AND end_ms < ?",
                    (coin, cutoff_ms),
                )
            self.conn.commit()
            return self.conn.total_changes - before

    def checkpoint_wal(self, *, truncate: bool = False) -> tuple[int, int, int]:
        mode = "TRUNCATE" if truncate else "PASSIVE"
        with self._lock:
            row = self.conn.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
        return int(row[0]), int(row[1]), int(row[2])

    def set_meta(self, key: str, value: str | int) -> None:
        with self._lock:
            self.conn.execute(
                """
                INSERT INTO collector_meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, str(value)),
            )
            self.conn.commit()

    def get_meta_int(self, key: str) -> int | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT value FROM collector_meta WHERE key = ?", (key,)
            ).fetchone()
        return None if row is None else int(row["value"])

    def add_gap(
        self,
        coin: str,
        start_ms: int,
        end_ms: int,
        *,
        status: str,
        reason: str,
        recovery_earliest_ms: int | None = None,
        recovery_latest_ms: int | None = None,
        recovered_trade_count: int = 0,
    ) -> None:
        if end_ms <= start_ms:
            return
        with self._lock:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO coverage_gaps(
                    coin, start_ms, end_ms, status, reason,
                    recovery_earliest_ms, recovery_latest_ms,
                    recovered_trade_count, created_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    coin,
                    start_ms,
                    end_ms,
                    status,
                    reason,
                    recovery_earliest_ms,
                    recovery_latest_ms,
                    recovered_trade_count,
                    int(time.time() * 1000),
                ),
            )
            self.conn.commit()

    def gaps_overlapping(
        self,
        coin: str,
        start_ms: int,
        end_ms: int,
        *,
        status: str = "UNRESOLVED",
    ) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT * FROM coverage_gaps
                WHERE coin = ? AND status = ? AND end_ms > ? AND start_ms < ?
                ORDER BY start_ms
                """,
                (coin, status, start_ms, end_ms),
            ).fetchall()
        return [dict(row) for row in rows]

    def unresolved_gap_count(self, coin: str) -> int:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT COUNT(*) AS n
                FROM coverage_gaps
                WHERE coin = ? AND status = 'UNRESOLVED'
                """,
                (coin,),
            ).fetchone()
        return int(row["n"])

    def close(self) -> None:
        with self._lock:
            self.conn.close()
