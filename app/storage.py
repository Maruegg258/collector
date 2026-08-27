from __future__ import annotations

import sqlite3
import threading
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
        duplicates = len(rows) - inserted
        return inserted, duplicates

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
            "first_trade_time_ms": None if row["first_trade_time_ms"] is None else int(row["first_trade_time_ms"]),
            "last_trade_time_ms": None if row["last_trade_time_ms"] is None else int(row["last_trade_time_ms"]),
        }

    def close(self) -> None:
        with self._lock:
            self.conn.close()
