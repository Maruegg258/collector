from __future__ import annotations

import sqlite3
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
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.init_schema()

    def init_schema(self) -> None:
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

    def count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS n FROM trades").fetchone()
        return int(row["n"])

    def latest_trade_time_ms(self) -> int | None:
        row = self.conn.execute("SELECT MAX(time_ms) AS t FROM trades").fetchone()
        return None if row["t"] is None else int(row["t"])

    def close(self) -> None:
        self.conn.close()
