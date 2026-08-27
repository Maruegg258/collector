from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from .storage import TradeStore

HOUR_MS = 60 * 60 * 1000
FOUR_HOURS_MS = 4 * HOUR_MS
WINDOWS_MS = {
    "24h": 24 * HOUR_MS,
    "3d": 72 * HOUR_MS,
}


@dataclass(frozen=True)
class DataQualityConfig:
    stale_after_ms: int = 60_000
    complete_threshold: float = 0.999


def _coverage_ratio_with_gaps(
    store: TradeStore,
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


def build_spot_demand_snapshot(
    store: TradeStore,
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

    last_trade_age_ms = (
        None if latest_stored_ms is None else max(0, now_ms - latest_stored_ms)
    )
    connected = bool(collector_state.get("connected"))
    stale = (
        last_trade_age_ms is None
        or last_trade_age_ms > quality.stale_after_ms
    )

    windows: dict[str, Any] = {}

    completed_4h_end_ms = (now_ms // FOUR_HOURS_MS) * FOUR_HOURS_MS
    completed_4h_start_ms = completed_4h_end_ms - FOUR_HOURS_MS
    specs = [
        (
            "4h",
            completed_4h_start_ms,
            completed_4h_end_ms,
            "completed_binance_aligned_utc",
        ),
        ("24h", now_ms - WINDOWS_MS["24h"], now_ms, "rolling"),
        ("3d", now_ms - WINDOWS_MS["3d"], now_ms, "rolling"),
    ]

    for label, start_ms, end_ms, mode in specs:
        stats = store.aggregate_window(
            coin,
            start_ms,
            end_ms if label == "4h" else end_ms + 1,
        )
        coverage, gaps = _coverage_ratio_with_gaps(
            store,
            coin,
            coverage_epoch_ms,
            start_ms,
            end_ms,
        )
        complete = coverage >= quality.complete_threshold
        windows[label] = {
            "mode": mode,
            "window_start_ms": start_ms,
            "window_end_ms": end_ms,
            "coverage_ratio": round(coverage, 6),
            "complete": complete,
            "quality": (
                "COMPLETE"
                if complete
                else ("GAPPED" if gaps else "PARTIAL_HISTORY")
            ),
            "unresolved_gap_count": len(gaps),
            **stats,
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
            "recovery_method": "official recentTrades with strict overlap proof",
            "note": (
                "A gap is healed only when recentTrades spans from at/before "
                "gap start through at/after the first post-reconnect trade. "
                "Otherwise it remains unresolved."
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
            "last_trade_age_ms": last_trade_age_ms,
        },
        "windows": windows,
    }
