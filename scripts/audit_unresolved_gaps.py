from __future__ import annotations

import json
import os
from collections import defaultdict

import psycopg
from psycopg.rows import dict_row

FOUR_HOURS_MS = 4 * 60 * 60 * 1000
MAX_SINGLE_GAP_MS = 5_000
MAX_TOTAL_GAP_MS = 10_000
MAX_GAP_COUNT = 2


def _merge(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[list[int]] = []
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)
    return [(start, end) for start, end in merged]


def _bucket_start(value_ms: int) -> int:
    return (value_ms // FOUR_HOURS_MS) * FOUR_HOURS_MS


def _classification(intervals: list[tuple[int, int]]) -> dict:
    merged = _merge(intervals)
    durations = [end - start for start, end in merged]
    independent = len(merged)
    total_ms = sum(durations)
    max_ms = max(durations, default=0)
    if independent == 0:
        integrity = "COMPLETE"
    elif independent <= MAX_GAP_COUNT and total_ms <= MAX_TOTAL_GAP_MS and max_ms <= MAX_SINGLE_GAP_MS:
        integrity = "MINOR_GAP"
    else:
        integrity = "MATERIAL_GAP"
    return {
        "spot_integrity": integrity,
        "independent_gap_count": independent,
        "merged_unresolved_duration_ms": total_ms,
        "max_single_gap_ms": max_ms,
        "merged_intervals": [{"start_ms": s, "end_ms": e, "duration_ms": e - s} for s, e in merged],
    }


def main() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required")

    with psycopg.connect(database_url, row_factory=dict_row, connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute("BEGIN READ ONLY")
            cur.execute(
                """
                SELECT id, coin, start_ms, end_ms, status, reason,
                       recovery_earliest_ms, recovery_latest_ms,
                       recovered_trade_count, created_at_ms
                FROM coverage_gaps
                WHERE coin=%s AND status='UNRESOLVED'
                ORDER BY start_ms, end_ms, id
                """,
                ("@107",),
            )
            rows = [dict(row) for row in cur.fetchall()]
            conn.rollback()

    bucket_intervals: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for row in rows:
        row["duration_ms"] = int(row["end_ms"]) - int(row["start_ms"])
        row["recovery_proves_full_gap"] = bool(
            row["recovery_earliest_ms"] is not None
            and row["recovery_latest_ms"] is not None
            and int(row["recovery_earliest_ms"]) <= int(row["start_ms"])
            and int(row["recovery_latest_ms"]) >= int(row["end_ms"])
        )
        start = int(row["start_ms"])
        end = int(row["end_ms"])
        cursor = _bucket_start(start)
        overlaps = []
        while cursor < end:
            bucket_end = cursor + FOUR_HOURS_MS
            overlap_start = max(start, cursor)
            overlap_end = min(end, bucket_end)
            if overlap_end > overlap_start:
                bucket_intervals[cursor].append((overlap_start, overlap_end))
                overlaps.append({
                    "window_start_ms": cursor,
                    "window_end_ms": bucket_end,
                    "overlap_duration_ms": overlap_end - overlap_start,
                })
            cursor = bucket_end
        row["overlapping_4h_buckets"] = overlaps

    bucket_summary = []
    for start_ms in sorted(bucket_intervals):
        summary = _classification(bucket_intervals[start_ms])
        bucket_summary.append({
            "window_start_ms": start_ms,
            "window_end_ms": start_ms + FOUR_HOURS_MS,
            **summary,
        })

    payload = {
        "audit_mode": "READ_ONLY",
        "coin": "@107",
        "unresolved_gap_count": len(rows),
        "thresholds": {
            "max_single_gap_ms": MAX_SINGLE_GAP_MS,
            "max_total_gap_ms_per_completed_4h": MAX_TOTAL_GAP_MS,
            "max_independent_gaps_per_completed_4h": MAX_GAP_COUNT,
        },
        "gaps": rows,
        "bucket_summary": bucket_summary,
    }
    print("GAP_AUDIT_JSON=" + json.dumps(payload, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
