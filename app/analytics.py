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


def _coverage_ratio(reliable_since_ms: int | None, start_ms: int, end_ms: int) -> float:
    if reliable_since_ms is None or end_ms <= start_ms:
        return 0.0
    covered_start = max(start_ms, reliable_since_ms)
    covered_ms = max(0, end_ms - covered_start)
    return min(1.0, covered_ms / (end_ms - start_ms))


def _reliable_since_ms(collector_state: dict[str, Any], first_stored_ms: int | None) -> int | None:
    if first_stored_ms is None:
        return None
    started_at_ms = collector_state.get("started_at_ms")
    reconnects = int(collector_state.get("reconnects") or 0)
    connected_at_ms = collector_state.get("connected_at_ms")

    candidates = [first_stored_ms]
    if started_at_ms is not None:
        candidates.append(int(started_at_ms))
    # Until gap recovery exists, any reconnect invalidates continuity before the latest connection.
    if reconnects > 0 and connected_at_ms is not None:
        candidates.append(int(connected_at_ms))
    return max(candidates)


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
    reliable_since_ms = _reliable_since_ms(collector_state, first_stored_ms)
    last_trade_age_ms = None if latest_stored_ms is None else max(0, now_ms - latest_stored_ms)
    connected = bool(collector_state.get("connected"))
    stale = last_trade_age_ms is None or last_trade_age_ms > quality.stale_after_ms

    windows: dict[str, Any] = {}

    # Protocol primary 4H metric: last completed Binance-aligned UTC 4H bucket.
    completed_4h_end_ms = (now_ms // FOUR_HOURS_MS) * FOUR_HOURS_MS
    completed_4h_start_ms = completed_4h_end_ms - FOUR_HOURS_MS
    four_stats = store.aggregate_window(coin, completed_4h_start_ms, completed_4h_end_ms)
    four_coverage = _coverage_ratio(reliable_since_ms, completed_4h_start_ms, completed_4h_end_ms)
    four_complete = four_coverage >= quality.complete_threshold
    windows["4h"] = {
        "mode": "completed_binance_aligned_utc",
        "window_start_ms": completed_4h_start_ms,
        "window_end_ms": completed_4h_end_ms,
        "coverage_ratio": round(four_coverage, 6),
        "complete": four_complete,
        "quality": "COMPLETE" if four_complete else "PARTIAL_HISTORY",
        **four_stats,
    }

    for label, duration_ms in WINDOWS_MS.items():
        start_ms = now_ms - duration_ms
        stats = store.aggregate_window(coin, start_ms, now_ms + 1)
        coverage = _coverage_ratio(reliable_since_ms, start_ms, now_ms)
        complete = coverage >= quality.complete_threshold
        windows[label] = {
            "mode": "rolling",
            "window_start_ms": start_ms,
            "window_end_ms": now_ms,
            "coverage_ratio": round(coverage, 6),
            "complete": complete,
            "quality": "COMPLETE" if complete else "PARTIAL_HISTORY",
            **stats,
        }

    history_complete = all(window["complete"] for window in windows.values())
    reconnects = int(collector_state.get("reconnects") or 0)
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
            "basis": "continuous_current_process_since_latest_connect",
            "gap_recovery_enabled": False,
            "note": "Until gap recovery is implemented, data before the current process start or latest reconnect is not credited toward completeness.",
        },
        "collector": {
            "connected": connected,
            "last_error": collector_state.get("last_error"),
            "reconnects": reconnects,
            "stored_trades": store.count(coin),
            "first_stored_trade_time_ms": first_stored_ms,
            "latest_stored_trade_time_ms": latest_stored_ms,
            "reliable_since_ms": reliable_since_ms,
            "last_trade_age_ms": last_trade_age_ms,
        },
        "windows": windows,
    }
