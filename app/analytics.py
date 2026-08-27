from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from .storage_protocol import StorageBackend

HOUR_MS = 60 * 60 * 1000
FOUR_HOURS_MS = 4 * HOUR_MS


@dataclass(frozen=True)
class DataQualityConfig:
    stale_after_ms: int = 60_000
    complete_threshold: float = 0.999


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
        # The newest completed bucket may not have been materialized by the
        # hourly maintenance task yet. It is still safe to compute from raw.
        stats = store.aggregate_window(coin, start_ms, end_ms)
    else:
        stats = _empty_stats()
        archive_missing = True

    coverage, gaps = _coverage_ratio_with_gaps(
        store,
        coin,
        coverage_epoch_ms,
        start_ms,
        end_ms,
    )
    complete = (
        not archive_missing
        and coverage >= quality.complete_threshold
        and len(gaps) == 0
    )
    return {
        "mode": "completed_binance_aligned_utc",
        "window_start_ms": start_ms,
        "window_end_ms": end_ms,
        "coverage_ratio": round(coverage, 6),
        "complete": complete,
        "quality": (
            "ARCHIVE_MISSING"
            if archive_missing
            else ("COMPLETE" if complete else ("GAPPED" if gaps else "PARTIAL_HISTORY"))
        ),
        "unresolved_gap_count": len(gaps),
        "archive_source": "materialized_4h" if archived is not None else ("raw_fallback" if not archive_missing else "missing"),
        **stats,
    }


def _build_window_from_buckets(
    buckets: list[dict[str, Any]],
    *,
    label: str,
) -> dict[str, Any]:
    stats = _sum_stats(buckets)
    if not buckets:
        return {
            "mode": "rolling_completed_4h_bins",
            "window_start_ms": None,
            "window_end_ms": None,
            "coverage_ratio": 0.0,
            "complete": False,
            "quality": "PARTIAL_HISTORY",
            "unresolved_gap_count": 0,
            **stats,
        }
    duration = sum(bucket["window_end_ms"] - bucket["window_start_ms"] for bucket in buckets)
    weighted_coverage = (
        sum(
            bucket["coverage_ratio"] * (bucket["window_end_ms"] - bucket["window_start_ms"])
            for bucket in buckets
        ) / duration
        if duration > 0
        else 0.0
    )
    complete = all(bool(bucket["complete"]) for bucket in buckets)
    if any(bucket["quality"] == "ARCHIVE_MISSING" for bucket in buckets):
        quality_name = "ARCHIVE_MISSING"
    elif any(bucket["quality"] == "GAPPED" for bucket in buckets):
        quality_name = "GAPPED"
    else:
        quality_name = "COMPLETE" if complete else "PARTIAL_HISTORY"
    return {
        "mode": "rolling_completed_4h_bins",
        "window_start_ms": buckets[0]["window_start_ms"],
        "window_end_ms": buckets[-1]["window_end_ms"],
        "coverage_ratio": round(weighted_coverage, 6),
        "complete": complete,
        "quality": quality_name,
        "unresolved_gap_count": sum(int(bucket["unresolved_gap_count"]) for bucket in buckets),
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
        end_ms = start_ms + FOUR_HOURS_MS
        recent_4h.append(
            _load_completed_4h_bucket(
                store,
                coin,
                start_ms,
                end_ms,
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

    history_complete = all(window["complete"] for window in windows.values())
    if not connected or stale:
        data_quality = "DEGRADED"
    elif history_complete:
        data_quality = "FULL"
    else:
        data_quality = "WARMING_UP"

    return {
        "source": "hyperliquid_official",
        "market": "HYPE/USDC",
        "coin": coin,
        "generated_at_ms": now_ms,
        "data_quality": data_quality,
        "full_spot_mode_ready": data_quality == "FULL",
        "coverage_policy": {
            "basis": "persistent_coverage_epoch_minus_unresolved_gaps",
            "gap_recovery_enabled": True,
            "recovery_method": "official recentTrades with strict overlap proof and bounded retry",
            "raw_storage": "short_retention",
            "historical_storage": "durable_completed_4h_aggregates",
            "note": (
                "24H and 3D protocol windows are rolling sums of the latest 6 and 18 "
                "completed Binance-aligned 4H buckets. This preserves protocol accuracy "
                "while allowing raw trade compaction."
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
            "Monitor should evaluate recent_4h[].cumulative_delta_usdc together with "
            "Binance HYPE price structure for direction/divergence; this field is not "
            "a separate bullish/bearish vote."
        ),
    }
