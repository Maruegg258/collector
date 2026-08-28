from __future__ import annotations

import math

from app.analytics import FOUR_HOURS_MS, HOUR_MS, build_spot_demand_snapshot
from app.storage import TradeRecord, TradeStore


def add_trade(store: TradeStore, *, t: int, side: str, px: float, sz: float, tid: int) -> None:
    notional = px * sz
    store.insert_many([
        TradeRecord(
            coin="@107",
            time_ms=t,
            tid=tid,
            side=side,
            px=px,
            sz=sz,
            notional_usdc=notional,
            signed_notional_usdc=notional if side == "B" else -notional,
        )
    ])


def state(*, started: int, connected: bool = True, reconnects: int = 0, connected_at: int | None = None):
    return {
        "connected": connected,
        "started_at_ms": started,
        "connected_at_ms": connected_at if connected_at is not None else started,
        "last_error": None if connected else "websocket",
        "reconnects": reconnects,
    }


def seeded_full_history(tmp_path, *, now: int):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    started = now - 80 * HOUR_MS
    store.set_meta("coverage_epoch_ms", started)
    add_trade(store, t=started, side="B", px=100, sz=1, tid=1)
    add_trade(store, t=now - 1_000, side="A", px=100, sz=1, tid=2)
    return store, started


def test_completed_4h_aggregation_and_delta_ratio(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000 + 2 * HOUR_MS
    end = (now // FOUR_HOURS_MS) * FOUR_HOURS_MS
    start = end - FOUR_HOURS_MS
    add_trade(store, t=start - HOUR_MS, side="B", px=100, sz=99, tid=9)
    add_trade(store, t=start + HOUR_MS, side="B", px=100, sz=2, tid=1)
    add_trade(store, t=start + 2 * HOUR_MS, side="A", px=100, sz=1, tid=2)
    add_trade(store, t=now - 1_000, side="B", px=100, sz=1, tid=3)

    snap = build_spot_demand_snapshot(store, state(started=start - 1), now_ms=now)
    four = snap["windows"]["4h"]
    assert four["mode"] == "completed_binance_aligned_utc"
    assert four["trade_count"] == 2
    assert math.isclose(four["buy_notional_usdc"], 200.0)
    assert math.isclose(four["sell_notional_usdc"], 100.0)
    assert math.isclose(four["net_delta_usdc"], 100.0)
    assert math.isclose(four["delta_ratio"], 1 / 3)
    assert math.isclose(four["base_delta_hype"], 1.0)
    store.close()


def test_warming_up_until_full_history_exists(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    started = now - 5 * HOUR_MS
    add_trade(store, t=started, side="B", px=100, sz=1, tid=1)
    add_trade(store, t=now - 1_000, side="B", px=100, sz=1, tid=2)

    snap = build_spot_demand_snapshot(store, state(started=started), now_ms=now)
    assert snap["data_quality"] == "WARMING_UP"
    assert snap["windows"]["24h"]["quality"] == "PARTIAL_HISTORY"
    assert snap["windows"]["3d"]["quality"] == "PARTIAL_HISTORY"
    store.close()


def test_full_after_more_than_72h_continuous_current_process(tmp_path):
    now = 1_800_000_000_000
    store, started = seeded_full_history(tmp_path, now=now)
    snap = build_spot_demand_snapshot(store, state(started=started), now_ms=now)
    assert snap["protocol_version"] == "1.2.1"
    assert snap["data_quality"] == "FULL"
    assert snap["full_spot_mode_ready"] is True
    assert all(w["complete"] for w in snap["windows"].values())
    assert all(w["decision_usability"] == "UNASSESSED" for w in snap["windows"].values())
    store.close()


def test_redeploy_initializes_persistent_coverage_epoch_at_current_start(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    add_trade(store, t=now - 80 * HOUR_MS, side="B", px=100, sz=1, tid=1)
    add_trade(store, t=now - 1_000, side="A", px=100, sz=1, tid=2)
    current_start = now - HOUR_MS

    snap = build_spot_demand_snapshot(store, state(started=current_start), now_ms=now)
    assert snap["collector"]["stored_trades"] == 2
    assert snap["collector"]["coverage_epoch_ms"] == current_start
    assert snap["data_quality"] == "WARMING_UP"
    assert snap["windows"]["3d"]["complete"] is False
    store.close()


def test_unresolved_gap_is_engineering_fact_not_fixed_cliff(tmp_path):
    now = 1_800_000_000_000
    store, started = seeded_full_history(tmp_path, now=now)
    gap_start = now - 2 * HOUR_MS
    gap_end = gap_start + 6_000
    store.add_gap("@107", gap_start, gap_end, status="UNRESOLVED", reason="reconnect")

    snap = build_spot_demand_snapshot(store, state(started=started, reconnects=1), now_ms=now)
    four = snap["windows"]["4h"]
    assert snap["collector"]["unresolved_gaps"] == 1
    assert four["complete"] is False
    assert four["continuity_status"] == "UNRESOLVED_GAP"
    assert four["unresolved_gap_duration_ms"] == 6_000
    assert four["usable_for_spot_mode"] is True
    assert four["decision_usability"] == "UNASSESSED"
    assert four["fixed_cliff_thresholds_applied"] is False
    assert len(four["gap_diagnostics"]) == 1
    assert snap["data_quality"] == "FULL"
    assert snap["full_spot_mode_ready"] is True
    assert snap["monitor_review"]["decision_usability_required"] is True
    assert "4h" in snap["monitor_review"]["windows_with_unresolved_gaps"]
    store.close()


def test_three_micro_gaps_do_not_auto_degrade(tmp_path):
    now = 1_800_000_000_000
    store, started = seeded_full_history(tmp_path, now=now)
    base = now - 2 * HOUR_MS
    for i in range(3):
        start = base + i * 10_000
        store.add_gap("@107", start, start + 1_000, status="UNRESOLVED", reason=f"gap-{i}")

    snap = build_spot_demand_snapshot(store, state(started=started, reconnects=3), now_ms=now)
    four = snap["windows"]["4h"]
    assert four["independent_gap_count"] == 3
    assert four["continuity_status"] == "UNRESOLVED_GAP"
    assert four["decision_usability"] == "UNASSESSED"
    assert snap["data_quality"] == "FULL"
    assert snap["monitor_review"]["fixed_cliff_thresholds_applied"] is False
    store.close()


def test_gaps_across_multiple_buckets_preserve_window_independence(tmp_path):
    now = 1_800_000_000_000
    store, started = seeded_full_history(tmp_path, now=now)
    latest_end = (now // FOUR_HOURS_MS) * FOUR_HOURS_MS
    store.add_gap("@107", latest_end - HOUR_MS, latest_end - HOUR_MS + 1_000, status="UNRESOLVED", reason="gap-1")
    store.add_gap("@107", latest_end - 5 * HOUR_MS, latest_end - 5 * HOUR_MS + 1_000, status="UNRESOLVED", reason="gap-2")

    snap = build_spot_demand_snapshot(store, state(started=started, reconnects=2), now_ms=now)
    assert snap["windows"]["4h"]["continuity_status"] == "UNRESOLVED_GAP"
    assert snap["windows"]["24h"]["continuity_status"] == "UNRESOLVED_GAP"
    assert snap["windows"]["24h"]["repeated_gap_review_required"] is True
    assert snap["windows"]["24h"]["decision_usability"] == "UNASSESSED"
    assert snap["windows"]["3d"]["decision_usability"] == "UNASSESSED"
    assert snap["monitor_review"]["repeated_continuity_review_required"] is True
    assert snap["data_quality"] == "FULL"
    store.close()


def test_disconnected_is_degraded_even_with_history(tmp_path):
    now = 1_800_000_000_000
    store, started = seeded_full_history(tmp_path, now=now)
    snap = build_spot_demand_snapshot(store, state(started=started, connected=False), now_ms=now)
    assert snap["data_quality"] == "DEGRADED"
    assert snap["full_spot_mode_ready"] is False
    store.close()
