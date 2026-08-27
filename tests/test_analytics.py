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
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    started = now - 73 * HOUR_MS
    add_trade(store, t=started, side="B", px=100, sz=1, tid=1)
    add_trade(store, t=now - 1_000, side="A", px=100, sz=1, tid=2)

    snap = build_spot_demand_snapshot(store, state(started=started), now_ms=now)
    assert snap["data_quality"] == "FULL"
    assert snap["full_spot_mode_ready"] is True
    assert all(w["complete"] for w in snap["windows"].values())
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


def test_unresolved_reconnect_gap_reduces_coverage_without_resetting_epoch(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    started = now - 80 * HOUR_MS
    gap_start = now - 2 * HOUR_MS
    gap_end = gap_start + 60_000
    add_trade(store, t=started, side="B", px=100, sz=1, tid=1)
    add_trade(store, t=now - 1_000, side="A", px=100, sz=1, tid=2)
    store.set_meta("coverage_epoch_ms", started)
    store.add_gap(
        "@107",
        gap_start,
        gap_end,
        status="UNRESOLVED",
        reason="test_reconnect_gap",
    )

    snap = build_spot_demand_snapshot(
        store,
        state(started=started, reconnects=1, connected_at=gap_end),
        now_ms=now,
    )
    assert snap["collector"]["coverage_epoch_ms"] == started
    assert snap["collector"]["unresolved_gaps"] == 1
    assert snap["windows"]["3d"]["unresolved_gap_count"] == 1
    assert snap["windows"]["3d"]["complete"] is False
    assert snap["data_quality"] == "WARMING_UP"
    store.close()


def test_disconnected_is_degraded_even_with_history(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    started = now - 80 * HOUR_MS
    add_trade(store, t=started, side="B", px=100, sz=1, tid=1)
    add_trade(store, t=now - 1_000, side="A", px=100, sz=1, tid=2)

    snap = build_spot_demand_snapshot(store, state(started=started, connected=False), now_ms=now)
    assert snap["data_quality"] == "DEGRADED"
    assert snap["full_spot_mode_ready"] is False
    store.close()
