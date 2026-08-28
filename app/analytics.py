from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from .spot_integrity import SpotIntegrityConfig, summarize_unresolved_gaps
from .storage_protocol import StorageBackend

HOUR_MS = 60 * 60 * 1000
FOUR_HOURS_MS = 4 * HOUR_MS


@dataclass(frozen=True)
class DataQualityConfig:
    stale_after_ms: int = 60_000
    # Retained only for backwards-compatible construction.  Protocol v1.2.1
    # does not use coverage percentage as an automatic trading cliff.
    complete_threshold: float = 0.999
    integrity: SpotIntegrityConfig = SpotIntegrityConfig()


def _coverage_ratio_with_gaps(
    store: StorageBackend,
    coin: str,
    coverage_epoch_ms: int | None,
    start_ms: int,
    end_ms: int,
) -> tuple[float, list[dict]]:
    if coverage_epoch_ms is None or end_ms <= start_ms:
        return 0.0, []

    uncovered_ms = max(0, min(end_ms, coverage_epoch_ms) - start_ms)
    gaps = store.gaps_overlapping(coin, start_ms, end_ms)

    intervals: list[list[int]] = []
    for gap in gaps:
        gap_start = max(start_ms, int(gap["start_ms"]))
        gap_end = min(end_ms, int(gap["end_ms"]))
        if gap_end > gap_start:
            intervals.append([gap_start, gap_end])

    intervals.sort()
    merged: list[list[int]] = []
    for start, end in intervals:
        if not merged or start > merged[-1][1]:
            merged.append([start, end])
        else:
            merged[-1][1] = max(merged[-1][1], end)

    uncovered_ms += sum(end - start for start, end in merged)
    duration_ms = end_ms - start_ms
    ratio = max(0.0, min(1.0, 1.0 - uncovered_ms / duration_ms))
    return ratio, gaps


def _empty_stats() -> dict[str, Any]:
    return {
        "trade_count": 0,
        "buy_notional_usdc": 0.0,
        "sell_notional_usdc": 0.0,
        "net_delta_usdc": 0.0,
        "base_delta_hype": 0.0,
        "total_notional_usdc": 0.0,
        "delta_ratio": None,
        "first_trade_time_ms": None,
        "last_trade_time_ms": None,
    }


def _sum_stats(items: list[dict[str, Any]]) -> dict[str, Any]:
    if not items:
        return _empty_stats()
    total = sum(float(item["total_notional_usdc"]) for item in items)
    net = sum(float(item["net_delta_usdc"]) for item in items)
    first_times = [item.get("first_trade_time_ms") for item in items if item.get("first_trade_time_ms") is not None]
    last_times = [item.get("last_trade_time_ms") for item in items if item.get("last_trade_time_ms") is not None]
    return {
        "trade_count": sum(int(item["trade_count"]) for item in items),
        "buy_notional_usdc": sum(float(item["buy_notional_usdc"]) for item in items),
        "sell_notional_usdc": sum(float(item["sell_notional_usdc"]) for item in items),
        "net_delta_usdc": net,
        "base_delta_hype": sum(float(item["base_delta_hype"]) for item in items),
        "total_notional_usdc": total,
        "delta_ratio": (net / total) if total > 0 else None,
        "first_trade_time_ms": min(first_times) if first_times else None,
        "last_trade_time_ms": max(last_times) if last_times else None,
    }


def _load_completed_4h_bucket(
    store: StorageBackend,
    coin: str,
    start_ms: int,
    end_ms: int,
    coverage_epoch_ms: int | None,
    quality: DataQualityConfig,
) -> dict[str, Any]:
    archived = store.get_aggregate_bucket(coin, "4h", start_ms)
    raw_purged_before_ms = store.get_meta_int("raw_purged_before_ms")
    archive_missing = False

    if archived is not None:
        stats = {key: archived.get(key) for key in _empty_stats().keys()}
    elif raw_purged_before_ms is None or start_ms >= raw_purged_before_ms:
        stats = store.aggregate_window(coin, start_ms, end_ms)
    else:
        stats = _empty_stats()
        archive_missing = True

    coverage, gaps = _coverage_ratio_with_gaps(store, coin, coverage_epoch_ms, start_ms, end_ms)
    gap_summary = summarize_unresolved_gaps(
        gaps,
        start_ms=start_ms,
        end_ms=end_ms,
        config=quality.integrity,
    )
    history_ready = coverage_epoch_ms is not None and coverage_epoch_ms <= start_ms and not archive_missing
    continuity_status = gap_summary["continuity_status"] if history_ready else "UNKNOWN"
    exact_complete = history_ready and continuity_status == "COMPLETE"
    source_assessable = history_ready

    if archive_missing:
        quality_name = "ARCHIVE_MISSING"
    elif not history_ready:
        quality_name = "PARTIAL_HISTORY"
    else:
        quality_name = continuity_status

    return {
        "mode": "completed_binance_aligned_utc",
        "window_start_ms": start_ms,
        "window_end_ms": end_ms,
        "coverage_ratio": round(coverage, 6),
        "coverage_ratio_role": "diagnostic_only",
        "complete": exact_complete,
        "history_ready": history_ready,
        # Compatibility alias: source/history availability only.  Final FULL vs
        # DEGRADED Spot Mode still depends on Monitor Decision Usability.
        "usable_for_spot_mode": source_assessable,
        "source_assessable": source_assessable,
        "quality": quality_name,
        "continuity_status": continuity_status,
        "spot_integrity": continuity_status,
        "decision_usability": "UNASSESSED" if source_assessable else "UNKNOWN",
        "decision_usability_owner": "monitor",
        "unresolved_gap_count": len(gaps),
        "independent_gap_count": gap_summary["independent_gap_count"],
        "unresolved_gap_duration_ms": gap_summary["unresolved_gap_duration_ms"],
        "max_unresolved_gap_ms": gap_summary["max_unresolved_gap_ms"],
        "gap_diagnostics": gap_summary["gap_diagnostics"],
        "fixed_cliff_thresholds_applied": False,
        "archive_source": "materialized_4h" if archived is not None else ("raw_fallback" if not archive_missing else "missing"),
        **stats,
    }


def _build_window_from_buckets(buckets: list[dict[str, Any]], *, label: str) -> dict[str, Any]:
    stats = _sum_stats(buckets)
    if not buckets:
        return {
            "mode": "rolling_completed_4h_bins",
            "window_start_ms": None,
            "window_end_ms": None,
            "coverage_ratio": 0.0,
            "coverage_ratio_role": "diagnostic_only",
            "complete": False,
            "history_ready": False,
            "usable_for_spot_mode": False,
            "source_assessable": False,
            "quality": "PARTIAL_HISTORY",
            "continuity_status": "UNKNOWN",
            "spot_integrity": "UNKNOWN",
            "decision_usability": "UNKNOWN",
            "decision_usability_owner": "monitor",
            "unresolved_gap_count": 0,
            "independent_gap_count": 0,
            "unresolved_gap_duration_ms": 0,
            "max_unresolved_gap_ms": 0,
            "gap_bucket_count": 0,
            "repeated_gap_review_required": False,
            "gap_diagnostics": [],
            "fixed_cliff_thresholds_applied": False,
            **stats,
        }

    duration = sum(bucket["window_end_ms"] - bucket["window_start_ms"] for bucket in buckets)
    weighted_coverage = (
        sum(bucket["coverage_ratio"] * (bucket["window_end_ms"] - bucket["window_start_ms"]) for bucket in buckets) / duration
        if duration > 0
        else 0.0
    )
    history_ready = all(bool(bucket["history_ready"]) for bucket in buckets)
    exact_complete = all(bool(bucket["complete"]) for bucket in buckets)
    archive_missing = any(bucket["quality"] == "ARCHIVE_MISSING" for bucket in buckets)
    gap_bucket_count = sum(bucket["continuity_status"] == "UNRESOLVED_GAP" for bucket in buckets)

    if archive_missing:
        quality_name = "ARCHIVE_MISSING"
        continuity_status = "UNKNOWN"
    elif not history_ready:
        quality_name = "PARTIAL_HISTORY"
        continuity_status = "UNKNOWN"
    elif gap_bucket_count:
        quality_name = "UNRESOLVED_GAP"
        continuity_status = "UNRESOLVED_GAP"
    else:
        quality_name = "COMPLETE"
        continuity_status = "COMPLETE"

    source_assessable = history_ready and not archive_missing
    gap_diagnostics: list[dict[str, Any]] = []
    seen: set[tuple[int, int, str | None]] = set()
    for bucket in buckets:
        for gap in bucket.get("gap_diagnostics", []):
            key = (int(gap["start_ms"]), int(gap["end_ms"]), gap.get("reason"))
            if key in seen:
                continue
            seen.add(key)
            gap_diagnostics.append(dict(gap))

    return {
        "mode": "rolling_completed_4h_bins",
        "window_start_ms": buckets[0]["window_start_ms"],
        "window_end_ms": buckets[-1]["window_end_ms"],
        "coverage_ratio": round(weighted_coverage, 6),
        "coverage_ratio_role": "diagnostic_only",
        "complete": exact_complete,
        "history_ready": history_ready,
        "usable_for_spot_mode": source_assessable,
        "source_assessable": source_assessable,
        "quality": quality_name,
        "continuity_status": continuity_status,
        "spot_integrity": continuity_status,
        "decision_usability": "UNASSESSED" if source_assessable else "UNKNOWN",
        "decision_usability_owner": "monitor",
        "unresolved_gap_count": sum(int(bucket["unresolved_gap_count"]) for bucket in buckets),
        "independent_gap_count": sum(int(bucket["independent_gap_count"]) for bucket in buckets),
        "unresolved_gap_duration_ms": sum(int(bucket["unresolved_gap_duration_ms"]) for bucket in buckets),
        "max_unresolved_gap_ms": max(int(bucket["max_unresolved_gap_ms"]) for bucket in buckets),
        "gap_bucket_count": gap_bucket_count,
        "repeated_gap_review_required": gap_bucket_count > 1,
        "gap_diagnostics": gap_diagnostics,
        "fixed_cliff_thresholds_applied": False,
        "bucket_count": len(buckets),
        "label": label,
        **stats,
    }


def build_spot_demand_snapshot(
    store: StorageBackend,
    collector_state: dict[str, Any],
    *,
    coin: str = "@107",
    now_ms: int | None = None,
    quality: DataQualityConfig = DataQualityConfig(),
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    first_stored_ms = store.first_trade_time_ms(coin)
    latest_stored_ms = store.latest_trade_time_ms(coin)

    coverage_epoch_ms = store.get_meta_int("coverage_epoch_ms")
    if coverage_epoch_ms is None:
        coverage_epoch_ms = int(collector_state.get("started_at_ms") or now_ms)
        store.set_meta("coverage_epoch_ms", coverage_epoch_ms)

    last_trade_age_ms = None if latest_stored_ms is None else max(0, now_ms - latest_stored_ms)
    connected = bool(collector_state.get("connected"))
    stale = last_trade_age_ms is None or last_trade_age_ms > quality.stale_after_ms

    completed_4h_end_ms = (now_ms // FOUR_HOURS_MS) * FOUR_HOURS_MS
    recent_4h: list[dict[str, Any]] = []
    for offset in range(18, 0, -1):
        start_ms = completed_4h_end_ms - offset * FOUR_HOURS_MS
        recent_4h.append(
            _load_completed_4h_bucket(
                store,
                coin,
                start_ms,
                start_ms + FOUR_HOURS_MS,
                coverage_epoch_ms,
                quality,
            )
        )

    cumulative = 0.0
    recent_with_cvd: list[dict[str, Any]] = []
    for bucket in recent_4h:
        cumulative += float(bucket["net_delta_usdc"])
        recent_with_cvd.append({**bucket, "cumulative_delta_usdc": cumulative})

    windows = {
        "4h": dict(recent_with_cvd[-1]),
        "24h": _build_window_from_buckets(recent_with_cvd[-6:], label="24h"),
        "3d": _build_window_from_buckets(recent_with_cvd, label="3d"),
    }

    archive_missing = any(window["quality"] == "ARCHIVE_MISSING" for window in windows.values())
    history_ready = all(bool(window["history_ready"]) for window in windows.values())

    # v1.2.1 deliberately does not auto-degrade on a fixed gap duration/count.
    # This status answers whether the official source/history is available for
    # Monitor assessment.  The Monitor then assigns ROBUST/MARGINAL/UNKNOWN to
    # each 4H/24H/3D window and finalizes FULL vs DEGRADED Spot Mode.
    if not connected or stale or archive_missing:
        data_quality = "DEGRADED"
    elif history_ready:
        data_quality = "FULL"
    else:
        data_quality = "WARMING_UP"

    windows_with_gaps = [
        label for label, window in windows.items()
        if window["continuity_status"] == "UNRESOLVED_GAP"
    ]
    repeated_gap_review = bool(windows["3d"].get("repeated_gap_review_required"))

    return {
        "source": "hyperliquid_official",
        "market": "HYPE/USDC",
        "coin": coin,
        "protocol_version": "1.2.1",
        "generated_at_ms": now_ms,
        "data_quality": data_quality,
        "data_quality_scope": "collector_source_and_history_readiness",
        "full_spot_mode_ready": data_quality == "FULL",
        "full_spot_mode_ready_scope": "source_level_only_monitor_must_finalize_window_usability",
        "spot_integrity": windows["3d"]["continuity_status"] if history_ready else "UNKNOWN",
        "continuity_status": windows["3d"]["continuity_status"] if history_ready else "UNKNOWN",
        "monitor_review": {
            "decision_usability_required": history_ready,
            "decision_usability_values": ["ROBUST", "MARGINAL", "UNKNOWN"],
            "windows_with_unresolved_gaps": windows_with_gaps,
            "repeated_continuity_review_required": repeated_gap_review,
            "fixed_cliff_thresholds_applied": False,
            "note": (
                "Collector reports engineering continuity and observed trades only. The Monitor must classify each "
                "4H/24H/3D window as ROBUST/MARGINAL/UNKNOWN using gap diagnostics, turnover, price/volatility, "
                "multi-window direction and material-event context. A gap does not automatically degrade all windows."
            ),
        },
        "coverage_policy": {
            "basis": "persistent_coverage_epoch_minus_unresolved_gaps",
            "coverage_ratio_role": "diagnostic_only_not_a_trading_cliff",
            "gap_recovery_enabled": True,
            "recovery_method": "official recentTrades with strict overlap proof and bounded retry",
            "raw_storage": "short_retention",
            "historical_storage": "durable_completed_4h_aggregates",
            "continuity_materiality": "protocol_v1.2.1_window_specific_monitor_assessment",
            "fixed_gap_cliff_thresholds": "disabled",
            "missing_trade_policy": "never_fill_or_assume_zero",
            "note": (
                "24H and 3D protocol windows are rolling sums of the latest 6 and 18 completed Binance-aligned 4H buckets. "
                "Gap duration/count remain engineering evidence; they do not independently determine trading usability."
            ),
        },
        "collector": {
            "connected": connected,
            "last_error": collector_state.get("last_error"),
            "reconnects": int(collector_state.get("reconnects") or 0),
            "recovery_attempts": int(collector_state.get("recovery_attempts") or 0),
            "recovery_successes": int(collector_state.get("recovery_successes") or 0),
            "recovery_failures": int(collector_state.get("recovery_failures") or 0),
            "unresolved_gaps": store.unresolved_gap_count(coin),
            "stored_trades": store.count(coin),
            "first_stored_trade_time_ms": first_stored_ms,
            "latest_stored_trade_time_ms": latest_stored_ms,
            "coverage_epoch_ms": coverage_epoch_ms,
            "raw_purged_before_ms": store.get_meta_int("raw_purged_before_ms"),
            "last_trade_age_ms": last_trade_age_ms,
        },
        "windows": windows,
        "recent_4h": recent_with_cvd,
        "cvd_direction_hint": (
            "Monitor should evaluate recent_4h[].cumulative_delta_usdc with Binance HYPE price structure. "
            "Protocol v1.2.1 treats 3D as persistence/background, 24H as primary Spot direction and 4H as inflection/timing; "
            "final Decision Usability is ROBUST/MARGINAL/UNKNOWN and is not assigned by the Collector."
        ),
    }
