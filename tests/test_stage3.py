import asyncio
import os
import tempfile

from app.analytics import build_spot_demand_snapshot
from app.collector import HypeSpotCollector
from app.storage import TradeRecord, TradeStore


def record(time_ms: int, tid: int, side: str = "B") -> TradeRecord:
    return TradeRecord(
        coin="@107",
        time_ms=time_ms,
        tid=tid,
        side=side,
        px=80.0,
        sz=1.0,
        notional_usdc=80.0,
        signed_notional_usdc=80.0 if side == "B" else -80.0,
    )


def test_unresolved_gap_reduces_coverage():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = TradeStore(os.path.join(temp_dir, "test.sqlite3"))
        now_ms = 1_800_000_000_000
        epoch_ms = now_ms - 24 * 60 * 60 * 1000
        store.set_meta("coverage_epoch_ms", epoch_ms)
        store.insert_many([record(epoch_ms + 1, 1), record(now_ms - 1, 2)])
        store.add_gap(
            "@107",
            now_ms - 60 * 60 * 1000,
            now_ms - 30 * 60 * 1000,
            status="UNRESOLVED",
            reason="test",
        )
        snapshot = build_spot_demand_snapshot(
            store,
            {"connected": True, "started_at_ms": epoch_ms, "reconnects": 0},
            now_ms=now_ms,
        )
        assert snapshot["windows"]["24h"]["coverage_ratio"] < 1.0
        assert snapshot["windows"]["24h"]["unresolved_gap_count"] == 1
        store.close()


def test_healed_gap_does_not_reduce_coverage():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = TradeStore(os.path.join(temp_dir, "test.sqlite3"))
        now_ms = 1_800_000_000_000
        epoch_ms = now_ms - 24 * 60 * 60 * 1000
        store.set_meta("coverage_epoch_ms", epoch_ms)
        store.insert_many([record(epoch_ms + 1, 1), record(now_ms - 1, 2)])
        store.add_gap(
            "@107",
            now_ms - 60 * 60 * 1000,
            now_ms - 30 * 60 * 1000,
            status="HEALED",
            reason="test",
        )
        snapshot = build_spot_demand_snapshot(
            store,
            {"connected": True, "started_at_ms": epoch_ms, "reconnects": 0},
            now_ms=now_ms,
        )
        assert snapshot["windows"]["24h"]["coverage_ratio"] > 0.999
        store.close()


def test_recent_trades_recovery_heals_only_with_overlap():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = TradeStore(os.path.join(temp_dir, "test.sqlite3"))
        collector = HypeSpotCollector(store)
        collector._fetch_recent_sync = lambda: [
            {"coin": "@107", "side": "B", "px": "80", "sz": "1", "time": 1000, "tid": 1},
            {"coin": "@107", "side": "A", "px": "80", "sz": "1", "time": 2000, "tid": 2},
            {"coin": "@107", "side": "B", "px": "80", "sz": "1", "time": 3000, "tid": 3},
        ]
        asyncio.run(collector._recover_gap(1500, 2500))
        assert collector.state.recovery_successes == 1
        assert store.unresolved_gap_count("@107") == 0
        store.close()


def test_recent_trades_recovery_insufficient_overlap_stays_unresolved():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = TradeStore(os.path.join(temp_dir, "test.sqlite3"))
        collector = HypeSpotCollector(store)
        collector._fetch_recent_sync = lambda: [
            {"coin": "@107", "side": "B", "px": "80", "sz": "1", "time": 2000, "tid": 1},
            {"coin": "@107", "side": "A", "px": "80", "sz": "1", "time": 3000, "tid": 2},
        ]
        asyncio.run(collector._recover_gap(1500, 2500))
        assert collector.state.recovery_failures == 1
        assert store.unresolved_gap_count("@107") == 1
        store.close()
