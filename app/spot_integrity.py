from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SpotIntegrityConfig:
    max_single_gap_ms: int = 5_000
    max_total_gap_ms: int = 10_000
    max_gap_count: int = 2


def summarize_unresolved_gaps(
    gaps: list[dict],
    *,
    start_ms: int,
    end_ms: int,
    config: SpotIntegrityConfig = SpotIntegrityConfig(),
) -> dict:
    intervals: list[list[int]] = []
    for gap in gaps:
        start = max(start_ms, int(gap["start_ms"]))
        end = min(end_ms, int(gap["end_ms"]))
        if end > start:
            intervals.append([start, end])

    intervals.sort()
    merged: list[list[int]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)

    durations = [end - start for start, end in merged]
    gap_count = len(merged)
    total_gap_ms = sum(durations)
    max_gap_ms = max(durations, default=0)

    if gap_count == 0:
        integrity = "COMPLETE"
    elif (
        gap_count <= config.max_gap_count
        and total_gap_ms <= config.max_total_gap_ms
        and max_gap_ms <= config.max_single_gap_ms
    ):
        integrity = "MINOR_GAP"
    else:
        integrity = "MATERIAL_GAP"

    return {
        "spot_integrity": integrity,
        "independent_gap_count": gap_count,
        "unresolved_gap_duration_ms": total_gap_ms,
        "max_unresolved_gap_ms": max_gap_ms,
        "minor_gap_thresholds": {
            "max_single_gap_ms": config.max_single_gap_ms,
            "max_total_gap_ms_per_completed_4h": config.max_total_gap_ms,
            "max_independent_gaps_per_completed_4h": config.max_gap_count,
        },
    }
