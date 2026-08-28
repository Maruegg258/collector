from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SpotIntegrityConfig:
    """Compatibility container for the retired v1.1 cliff thresholds.

    Protocol v1.2.1 keeps gap duration/count as diagnostics only.  These fields
    remain so older callers can construct the config without breaking, but they
    are deliberately not used to classify a gap as minor/material.
    """

    max_single_gap_ms: int = 5_000
    max_total_gap_ms: int = 10_000
    max_gap_count: int = 2


def _gap_diagnostic(gap: dict, *, start_ms: int, end_ms: int) -> dict | None:
    start = max(start_ms, int(gap["start_ms"]))
    end = min(end_ms, int(gap["end_ms"]))
    if end <= start:
        return None
    return {
        "start_ms": start,
        "end_ms": end,
        "duration_ms": end - start,
        "status": gap.get("status", "UNRESOLVED"),
        "reason": gap.get("reason"),
        "recovery_earliest_ms": gap.get("recovery_earliest_ms"),
        "recovery_latest_ms": gap.get("recovery_latest_ms"),
        "recovered_trade_count": int(gap.get("recovered_trade_count") or 0),
        "created_at_ms": gap.get("created_at_ms"),
    }


def summarize_unresolved_gaps(
    gaps: list[dict],
    *,
    start_ms: int,
    end_ms: int,
    config: SpotIntegrityConfig = SpotIntegrityConfig(),
) -> dict:
    """Summarize engineering continuity without making a trading decision.

    v1.2.1 explicitly removes the old 5s/10s/2-gap cliff.  The collector
    records what is missing; the Monitor combines these facts with turnover,
    price/volatility, multi-window direction and event context to classify each
    4H/24H/3D window as ROBUST, MARGINAL or UNKNOWN.
    """

    diagnostics: list[dict] = []
    intervals: list[list[int]] = []
    for gap in gaps:
        diagnostic = _gap_diagnostic(gap, start_ms=start_ms, end_ms=end_ms)
        if diagnostic is None:
            continue
        diagnostics.append(diagnostic)
        intervals.append([diagnostic["start_ms"], diagnostic["end_ms"]])

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
    continuity_status = "COMPLETE" if gap_count == 0 else "UNRESOLVED_GAP"

    return {
        "continuity_status": continuity_status,
        # Compatibility alias.  This is engineering continuity only, not the
        # Protocol's final window-specific Decision Usability classification.
        "spot_integrity": continuity_status,
        "independent_gap_count": gap_count,
        "unresolved_gap_duration_ms": total_gap_ms,
        "max_unresolved_gap_ms": max_gap_ms,
        "gap_diagnostics": diagnostics,
        "fixed_cliff_thresholds_applied": False,
        "legacy_v1_1_thresholds_ignored": {
            "max_single_gap_ms": config.max_single_gap_ms,
            "max_total_gap_ms_per_completed_4h": config.max_total_gap_ms,
            "max_independent_gaps_per_completed_4h": config.max_gap_count,
        },
    }
