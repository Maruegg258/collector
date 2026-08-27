from __future__ import annotations

import argparse
import os
import time

import psycopg
from psycopg.rows import dict_row

COIN = "@107"
FALSE_START_MS = 1787821103585
FALSE_END_MS = 1787831856098
TRUE_START_MS = 1787831854515
TRUE_END_MS = 1787831856098
EXPECTED_STATUS = "UNRESOLVED"
EXPECTED_REASON = "recentTrades_insufficient_overlap"


def classify_corrected_gap(
    recovery_earliest_ms: int | None,
    recovery_latest_ms: int | None,
) -> tuple[str, str, bool]:
    proven = (
        recovery_earliest_ms is not None
        and recovery_latest_ms is not None
        and recovery_earliest_ms <= TRUE_START_MS
        and recovery_latest_ms >= TRUE_END_MS
    )
    if proven:
        return "HEALED", "bookkeeping_correction_recentTrades_overlap_proof", True
    return "UNRESOLVED", "bookkeeping_correction_unverified_short_reconnect", False


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="apply the correction; otherwise dry-run")
    args = parser.parse_args()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")

    with psycopg.connect(database_url, row_factory=dict_row, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, coin, start_ms, end_ms, status, reason,
                       recovery_earliest_ms, recovery_latest_ms,
                       recovered_trade_count, created_at_ms
                FROM coverage_gaps
                WHERE coin=%s AND start_ms=%s AND end_ms=%s
                FOR UPDATE
                """,
                (COIN, FALSE_START_MS, FALSE_END_MS),
            )
            row = cur.fetchone()

            if row is None:
                print("gap_correction status=NOOP reason=false_gap_not_found")
                return

            if row["status"] != EXPECTED_STATUS or row["reason"] != EXPECTED_REASON:
                raise RuntimeError(
                    "refusing correction: exact gap exists but status/reason do not match expected bug signature"
                )

            recovery_earliest_ms = (
                None if row["recovery_earliest_ms"] is None else int(row["recovery_earliest_ms"])
            )
            recovery_latest_ms = (
                None if row["recovery_latest_ms"] is None else int(row["recovery_latest_ms"])
            )
            corrected_status, corrected_reason, proven = classify_corrected_gap(
                recovery_earliest_ms,
                recovery_latest_ms,
            )

            print(
                "gap_correction inspect "
                f"old_id={row['id']} false={FALSE_START_MS}:{FALSE_END_MS} "
                f"true={TRUE_START_MS}:{TRUE_END_MS} "
                f"recovery={recovery_earliest_ms}:{recovery_latest_ms} "
                f"recovered_trade_count={int(row['recovered_trade_count'])} "
                f"proof={proven} corrected_status={corrected_status}"
            )

            if not args.apply:
                print("gap_correction status=DRY_RUN")
                return

            cur.execute(
                """
                DELETE FROM coverage_gaps
                WHERE coin=%s AND start_ms=%s AND end_ms=%s
                  AND status=%s AND reason=%s
                """,
                (COIN, FALSE_START_MS, FALSE_END_MS, EXPECTED_STATUS, EXPECTED_REASON),
            )
            if cur.rowcount != 1:
                raise RuntimeError(f"expected to delete exactly one false gap row, deleted {cur.rowcount}")

            cur.execute(
                """
                INSERT INTO coverage_gaps(
                    coin,start_ms,end_ms,status,reason,
                    recovery_earliest_ms,recovery_latest_ms,
                    recovered_trade_count,created_at_ms
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (coin,start_ms,end_ms) DO UPDATE SET
                    status=EXCLUDED.status,
                    reason=EXCLUDED.reason,
                    recovery_earliest_ms=EXCLUDED.recovery_earliest_ms,
                    recovery_latest_ms=EXCLUDED.recovery_latest_ms,
                    recovered_trade_count=EXCLUDED.recovered_trade_count,
                    created_at_ms=EXCLUDED.created_at_ms
                """,
                (
                    COIN,
                    TRUE_START_MS,
                    TRUE_END_MS,
                    corrected_status,
                    corrected_reason,
                    recovery_earliest_ms,
                    recovery_latest_ms,
                    int(row["recovered_trade_count"]),
                    int(time.time() * 1000),
                ),
            )
            conn.commit()

            print(
                "gap_correction status=APPLIED "
                f"corrected_status={corrected_status} proof={proven} "
                f"new={TRUE_START_MS}:{TRUE_END_MS}"
            )


if __name__ == "__main__":
    main()
