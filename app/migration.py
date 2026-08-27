from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from .storage import TradeRecord
from .storage_postgres import PostgresTradeStore


@dataclass(frozen=True)
class MigrationReport:
    started_at_ms: int
    finished_at_ms: int
    cutoff_ms: int | None
    source_trade_count_at_cutoff: int
    target_trade_count_at_cutoff: int
    inserted_trades: int
    duplicate_trades: int
    meta_rows: int
    gap_rows: int
    aggregate_rows: int
    verified: bool

    def as_dict(self) -> dict:
        return {
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
            "duration_ms": self.finished_at_ms - self.started_at_ms,
            "cutoff_ms": self.cutoff_ms,
            "source_trade_count_at_cutoff": self.source_trade_count_at_cutoff,
            "target_trade_count_at_cutoff": self.target_trade_count_at_cutoff,
            "inserted_trades": self.inserted_trades,
            "duplicate_trades": self.duplicate_trades,
            "meta_rows": self.meta_rows,
            "gap_rows": self.gap_rows,
            "aggregate_rows": self.aggregate_rows,
            "verified": self.verified,
        }


def _open_sqlite_readonly(db_path: str) -> sqlite3.Connection:
    resolved = Path(db_path).resolve()
    conn = sqlite3.connect(f"file:{resolved}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def migrate_sqlite_snapshot_to_postgres(
    *,
    db_path: str,
    database_url: str,
    coin: str = "@107",
    batch_size: int = 5000,
) -> MigrationReport:
    started_at_ms = int(time.time() * 1000)
    source = _open_sqlite_readonly(db_path)
    target = PostgresTradeStore(database_url)

    try:
        cutoff_row = source.execute(
            "SELECT MAX(time_ms) AS t FROM trades WHERE coin = ?", (coin,)
        ).fetchone()
        cutoff_ms = None if cutoff_row["t"] is None else int(cutoff_row["t"])

        if cutoff_ms is None:
            source_trade_count = 0
        else:
            count_row = source.execute(
                "SELECT COUNT(*) AS n FROM trades WHERE coin = ? AND time_ms <= ?",
                (coin, cutoff_ms),
            ).fetchone()
            source_trade_count = int(count_row["n"])

        inserted_total = 0
        duplicate_total = 0
        last_time = -1
        last_tid = -1

        while cutoff_ms is not None:
            rows = source.execute(
                """
                SELECT coin, time_ms, tid, side, px, sz,
                       notional_usdc, signed_notional_usdc, trade_hash
                FROM trades
                WHERE coin = ? AND time_ms <= ?
                  AND (time_ms > ? OR (time_ms = ? AND tid > ?))
                ORDER BY time_ms, tid
                LIMIT ?
                """,
                (coin, cutoff_ms, last_time, last_time, last_tid, batch_size),
            ).fetchall()
            if not rows:
                break

            records = [
                TradeRecord(
                    coin=str(row["coin"]),
                    time_ms=int(row["time_ms"]),
                    tid=int(row["tid"]),
                    side=str(row["side"]),
                    px=float(row["px"]),
                    sz=float(row["sz"]),
                    notional_usdc=float(row["notional_usdc"]),
                    signed_notional_usdc=float(row["signed_notional_usdc"]),
                    trade_hash=row["trade_hash"],
                )
                for row in rows
            ]
            inserted, duplicates = target.insert_many(records)
            inserted_total += inserted
            duplicate_total += duplicates
            last_time = records[-1].time_ms
            last_tid = records[-1].tid

        meta_rows = source.execute(
            "SELECT key, value FROM collector_meta"
        ).fetchall()
        for row in meta_rows:
            target.set_meta(str(row["key"]), str(row["value"]))

        gap_rows = source.execute(
            "SELECT * FROM coverage_gaps WHERE coin = ? ORDER BY start_ms", (coin,)
        ).fetchall()
        for row in gap_rows:
            target.add_gap(
                coin,
                int(row["start_ms"]),
                int(row["end_ms"]),
                status=str(row["status"]),
                reason=str(row["reason"]),
                recovery_earliest_ms=None
                if row["recovery_earliest_ms"] is None
                else int(row["recovery_earliest_ms"]),
                recovery_latest_ms=None
                if row["recovery_latest_ms"] is None
                else int(row["recovery_latest_ms"]),
                recovered_trade_count=int(row["recovered_trade_count"]),
            )

        aggregate_rows = source.execute(
            "SELECT * FROM aggregate_buckets WHERE coin = ? ORDER BY granularity, start_ms",
            (coin,),
        ).fetchall()
        for row in aggregate_rows:
            stats = {
                "trade_count": int(row["trade_count"]),
                "buy_notional_usdc": float(row["buy_notional_usdc"]),
                "sell_notional_usdc": float(row["sell_notional_usdc"]),
                "net_delta_usdc": float(row["net_delta_usdc"]),
                "base_delta_hype": float(row["base_delta_hype"]),
                "total_notional_usdc": float(row["total_notional_usdc"]),
                "delta_ratio": row["delta_ratio"],
                "first_trade_time_ms": row["first_trade_time_ms"],
                "last_trade_time_ms": row["last_trade_time_ms"],
            }
            target.upsert_aggregate_bucket(
                coin,
                str(row["granularity"]),
                int(row["start_ms"]),
                int(row["end_ms"]),
                stats,
                complete=bool(row["complete"]),
                quality=str(row["quality"]),
                unresolved_gap_count=int(row["unresolved_gap_count"]),
            )

        if cutoff_ms is None:
            target_trade_count = 0
        else:
            with target.conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS n FROM trades WHERE coin = %s AND time_ms <= %s",
                    (coin, cutoff_ms),
                )
                target_trade_count = int(cur.fetchone()["n"])

        verified = target_trade_count == source_trade_count
        finished_at_ms = int(time.time() * 1000)
        return MigrationReport(
            started_at_ms=started_at_ms,
            finished_at_ms=finished_at_ms,
            cutoff_ms=cutoff_ms,
            source_trade_count_at_cutoff=source_trade_count,
            target_trade_count_at_cutoff=target_trade_count,
            inserted_trades=inserted_total,
            duplicate_trades=duplicate_total,
            meta_rows=len(meta_rows),
            gap_rows=len(gap_rows),
            aggregate_rows=len(aggregate_rows),
            verified=verified,
        )
    finally:
        source.close()
        target.close()
