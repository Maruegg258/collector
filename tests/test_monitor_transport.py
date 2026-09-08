from __future__ import annotations

import json

from app.monitor_transport import (
    MONITOR_LOG_TRANSPORT_VERSION,
    compact_monitor_payload,
    compact_monitor_payload_json,
)


def sample_payload() -> dict:
    gap = {
        "start_ms": 100,
        "end_ms": 200,
        "duration_ms": 100,
        "status": "UNRESOLVED",
        "reason": "test-gap",
        "recovery_earliest_ms": None,
        "recovery_latest_ms": None,
        "recovered_trade_count": 0,
        "created_at_ms": 99,
    }
    window = {
        "window_start_ms": 0,
        "window_end_ms": 14_400_000,
        "archive_source": "materialized_4h",
        "history_ready": True,
        "coverage_ratio": 0.99999,
        "continuity_status": "UNRESOLVED_GAP",
        "unresolved_gap_count": 1,
        "independent_gap_count": 1,
        "unresolved_gap_duration_ms": 100,
        "max_unresolved_gap_ms": 100,
        "decision_usability": "UNASSESSED",
        "net_delta_usdc": 1234.5,
        "delta_ratio": 0.12,
        "base_delta_hype": 12.3,
        "total_notional_usdc": 10_000.0,
        "gap_diagnostics": [gap],
        "buy_notional_usdc": 5_617.25,
        "sell_notional_usdc": 4_382.75,
    }
    return {
        "schema_version": "HYPE-SPOT-PAYLOAD-v1",
        "collector_interface_version": "1.2.3",
        "protocol_version": "1.3.1",
        "source": "hyperliquid_official",
        "market": "HYPE/USDC",
        "coin": "@107",
        "payload_generated_at_ms": 14_500_000,
        "query_mode": "historical_boundary",
        "requested_completed_4h_end_ms": 14_400_000,
        "completed_4h_end_ms": 14_400_000,
        "boundary_match": True,
        "data_quality": "FULL",
        "full_spot_mode_ready": True,
        "collector": {
            "connected": True,
            "latest_stored_trade_time_ms": 14_399_000,
            "last_trade_age_ms": 1_000,
            "unresolved_gaps": 2,
            "reconnects": 0,
            "recovery_attempts": 1,
            "recovery_successes": 1,
            "recovery_failures": 0,
            "stored_trades": 999_999,
        },
        "monitor_review": {
            "windows_with_unresolved_gaps": ["4h"],
            "repeated_continuity_review_required": False,
            "fixed_cliff_thresholds_applied": False,
        },
        "windows": {"4h": window, "24h": window, "3d": window},
        "recent_4h": [
            {
                **window,
                "cumulative_delta_usdc": 1234.5,
            }
        ],
    }


def test_compact_monitor_payload_preserves_protocol_facts_without_raw_bloat():
    compact = compact_monitor_payload(sample_payload())

    assert compact["transport_version"] == MONITOR_LOG_TRANSPORT_VERSION
    assert compact["schema_version"] == "HYPE-SPOT-PAYLOAD-v1"
    assert compact["source"] == "hyperliquid_official"
    assert compact["market"] == "HYPE/USDC"
    assert compact["coin"] == "@107"
    assert compact["boundary_match"] is True
    assert compact["windows"]["4h"]["net_delta_usdc"] == 1234.5
    assert compact["windows"]["4h"]["delta_ratio"] == 0.12
    assert compact["windows"]["4h"]["gap_diagnostics"][0]["start_ms"] == 100
    assert compact["recent_4h"][0]["cumulative_delta_usdc"] == 1234.5

    # Transport mirror deliberately excludes bulky/raw-only facts that the Monitor
    # does not need for its v1.3.1 acquisition fallback.
    assert "stored_trades" not in compact["collector"]
    assert "buy_notional_usdc" not in compact["windows"]["4h"]
    assert "sell_notional_usdc" not in compact["windows"]["4h"]


def test_compact_monitor_payload_json_is_single_line_and_parseable():
    encoded = compact_monitor_payload_json(sample_payload())
    assert "\n" not in encoded
    parsed = json.loads(encoded)
    assert parsed["transport_version"] == MONITOR_LOG_TRANSPORT_VERSION
    assert parsed["completed_4h_end_ms"] == 14_400_000
    assert parsed["windows"]["24h"]["decision_usability"] == "UNASSESSED"
