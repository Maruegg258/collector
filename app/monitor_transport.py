from __future__ import annotations

import json
from typing import Any

MONITOR_LOG_TRANSPORT_VERSION = "HYPE-MONITOR-LOG-v1"

_WINDOW_FIELDS = (
    "window_start_ms",
    "window_end_ms",
    "archive_source",
    "history_ready",
    "coverage_ratio",
    "continuity_status",
    "unresolved_gap_count",
    "independent_gap_count",
    "unresolved_gap_duration_ms",
    "max_unresolved_gap_ms",
    "decision_usability",
    "net_delta_usdc",
    "delta_ratio",
    "base_delta_hype",
    "total_notional_usdc",
)


def _compact_gap_diagnostics(items: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    compact: list[dict[str, Any]] = []
    for item in items or []:
        compact.append(
            {
                "start_ms": item.get("start_ms"),
                "end_ms": item.get("end_ms"),
                "duration_ms": item.get("duration_ms"),
                "reason": item.get("reason"),
                "recovery_earliest_ms": item.get("recovery_earliest_ms"),
                "recovery_latest_ms": item.get("recovery_latest_ms"),
                "recovered_trade_count": item.get("recovered_trade_count"),
            }
        )
    return compact


def _compact_window(window: dict[str, Any]) -> dict[str, Any]:
    compact = {field: window.get(field) for field in _WINDOW_FIELDS}
    compact["gap_diagnostics"] = _compact_gap_diagnostics(window.get("gap_diagnostics"))
    return compact


def compact_monitor_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the compact, lossless-for-monitoring mirror of the canonical payload.

    This is a transport fallback only. It does not calculate Spot Demand or assign
    Monitor Decision Usability. Values are copied from the already-built canonical
    HYPE-SPOT-PAYLOAD-v1 response.
    """

    collector = payload.get("collector") or {}
    review = payload.get("monitor_review") or {}
    windows = payload.get("windows") or {}

    recent_4h: list[dict[str, Any]] = []
    for bucket in payload.get("recent_4h") or []:
        recent_4h.append(
            {
                "window_start_ms": bucket.get("window_start_ms"),
                "window_end_ms": bucket.get("window_end_ms"),
                "archive_source": bucket.get("archive_source"),
                "history_ready": bucket.get("history_ready"),
                "continuity_status": bucket.get("continuity_status"),
                "net_delta_usdc": bucket.get("net_delta_usdc"),
                "delta_ratio": bucket.get("delta_ratio"),
                "total_notional_usdc": bucket.get("total_notional_usdc"),
                "cumulative_delta_usdc": bucket.get("cumulative_delta_usdc"),
            }
        )

    return {
        "transport_version": MONITOR_LOG_TRANSPORT_VERSION,
        "schema_version": payload.get("schema_version"),
        "collector_interface_version": payload.get("collector_interface_version"),
        "protocol_version": payload.get("protocol_version"),
        "source": payload.get("source"),
        "market": payload.get("market"),
        "coin": payload.get("coin"),
        "payload_generated_at_ms": payload.get("payload_generated_at_ms"),
        "query_mode": payload.get("query_mode"),
        "requested_completed_4h_end_ms": payload.get("requested_completed_4h_end_ms"),
        "completed_4h_end_ms": payload.get("completed_4h_end_ms"),
        "boundary_match": payload.get("boundary_match"),
        "data_quality": payload.get("data_quality"),
        "full_spot_mode_ready": payload.get("full_spot_mode_ready"),
        "collector": {
            "connected": collector.get("connected"),
            "latest_stored_trade_time_ms": collector.get("latest_stored_trade_time_ms"),
            "last_trade_age_ms": collector.get("last_trade_age_ms"),
            "unresolved_gaps": collector.get("unresolved_gaps"),
            "reconnects": collector.get("reconnects"),
            "recovery_attempts": collector.get("recovery_attempts"),
            "recovery_successes": collector.get("recovery_successes"),
            "recovery_failures": collector.get("recovery_failures"),
        },
        "monitor_review": {
            "windows_with_unresolved_gaps": review.get("windows_with_unresolved_gaps"),
            "repeated_continuity_review_required": review.get("repeated_continuity_review_required"),
            "fixed_cliff_thresholds_applied": review.get("fixed_cliff_thresholds_applied"),
        },
        "windows": {
            label: _compact_window(windows.get(label) or {})
            for label in ("4h", "24h", "3d")
        },
        "recent_4h": recent_4h,
    }


def compact_monitor_payload_json(payload: dict[str, Any]) -> str:
    return json.dumps(
        compact_monitor_payload(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
