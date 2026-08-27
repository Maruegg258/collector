from __future__ import annotations

from collections import namedtuple

from app.lifecycle import DAY_MS, FOUR_HOURS_MS, StorageLifecycle, StorageLifecycleConfig
from app.storage import TradeRecord
from app.storage_factory import SQLiteTradeStore as TradeStore


def add_trade(
    store: TradeStore,
    *,
    t: int,
    side: str,
    px: float = 100.0,
    sz: float = 1.0,
    tid: int,
) -> None:
    notional = px * sz
    store.insert_many(
        [
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
        ]
    )


def test_materializes_completed_4h_and_daily_buckets(tmp_path):
    db = tmp_path / "test.sqlite3"
    store = TradeStore(str(db))
    now = 1_800_000_000_000
    four_start = ((now // FOUR_HOURS_MS) - 2) * FOUR_HOURS_MS
    day_start = ((now // DAY_MS) - 2) * DAY_MS
    store.set_meta("coverage_epoch_ms", min(four_start, day_start) - 1)

    add_trade(store, t=day_start + 1_000, side="B", tid=3)
    add_trade(store, t=four_start + 1_000, side="B", sz=2, tid=1)
    add_trade(store, t=four_start + 2_000, side="A", sz=1, tid=2)

    lifecycle = StorageLifecycle(store)
    report = lifecycle.run_once(now_ms=now)

    bucket = store.get_aggregate_bucket("@107", "4h", four_start)
    assert bucket is not None
    assert bucket["trade_count"] == 2
    assert bucket["buy_notional_usdc"] == 200.0
    assert bucket["sell_notional_usdc"] == 100.0
    assert bucket["net_delta_usdc"] == 100.0
    assert bucket["complete"] == 1
    assert bucket["quality"] == "COMPLETE"
    assert report["aggregate_4h_total"] >= 1
    assert report["aggregate_1d_total"] >= 1
    store.close()


def test_unresolved_gap_archives_bucket_as_gapped(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    start = ((now // FOUR_HOURS_MS) - 1) * FOUR_HOURS_MS
    store.set_meta("coverage_epoch_ms", start - 1)
    add_trade(store, t=start + 1_000, side="B", tid=1)
    store.add_gap(
        "@107",
        start + 10_000,
        start + 20_000,
        status="UNRESOLVED",
        reason="test",
    )

    StorageLifecycle(store).run_once(now_ms=now)
    bucket = store.get_aggregate_bucket("@107", "4h", start)
    assert bucket is not None
    assert bucket["complete"] == 0
    assert bucket["quality"] == "GAPPED"
    assert bucket["unresolved_gap_count"] == 1
    store.close()


def test_archive_is_written_before_old_raw_is_purged(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    old_time = now - 15 * DAY_MS
    recent_time = now - DAY_MS
    old_four_start = (old_time // FOUR_HOURS_MS) * FOUR_HOURS_MS
    store.set_meta("coverage_epoch_ms", old_four_start - 1)
    add_trade(store, t=old_time, side="B", tid=1)
    add_trade(store, t=recent_time, side="A", tid=2)

    lifecycle = StorageLifecycle(
        store,
        config=StorageLifecycleConfig(raw_retention_days=14),
    )
    report = lifecycle.run_once(now_ms=now)

    assert report["purged_raw_trades"] == 1
    assert store.count("@107") == 1
    archived = store.get_aggregate_bucket("@107", "4h", old_four_start)
    assert archived is not None
    assert archived["trade_count"] == 1
    assert archived["buy_notional_usdc"] == 100.0
    store.close()


def test_purged_boundary_bucket_is_not_overwritten_by_partial_raw(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000 + 2 * 60 * 60 * 1000
    cutoff = now - 14 * DAY_MS
    boundary_start = (cutoff // FOUR_HOURS_MS) * FOUR_HOURS_MS
    store.set_meta("coverage_epoch_ms", boundary_start - 1)

    add_trade(store, t=cutoff - 60_000, side="B", tid=1)
    add_trade(store, t=cutoff + 60_000, side="A", tid=2)
    add_trade(store, t=now - DAY_MS, side="B", tid=3)

    lifecycle = StorageLifecycle(
        store,
        config=StorageLifecycleConfig(raw_retention_days=14),
    )
    first = lifecycle.run_once(now_ms=now)
    archived_before = store.get_aggregate_bucket("@107", "4h", boundary_start)
    assert archived_before is not None
    assert archived_before["trade_count"] == 2
    assert first["purged_raw_trades"] == 1

    lifecycle.run_once(now_ms=now + 10 * 60 * 1000)
    archived_after = store.get_aggregate_bucket("@107", "4h", boundary_start)
    assert archived_after is not None
    assert archived_after["trade_count"] == 2
    assert archived_after["buy_notional_usdc"] == 100.0
    assert archived_after["sell_notional_usdc"] == 100.0
    store.close()


def test_gap_ledger_is_retained_for_90_days_then_purged(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    old_gap_end = now - 91 * DAY_MS
    recent_gap_end = now - 10 * DAY_MS
    store.add_gap(
        "@107",
        old_gap_end - 1_000,
        old_gap_end,
        status="UNRESOLVED",
        reason="old",
    )
    store.add_gap(
        "@107",
        recent_gap_end - 1_000,
        recent_gap_end,
        status="UNRESOLVED",
        reason="recent",
    )

    report = StorageLifecycle(store).run_once(now_ms=now)
    assert report["purged_gap_records"] == 1
    assert store.unresolved_gap_count("@107") == 1
    store.close()


def test_disk_guardrail_levels(monkeypatch, tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    DiskUsage = namedtuple("DiskUsage", "total used free")
    lifecycle = StorageLifecycle(store)

    monkeypatch.setattr(
        "app.lifecycle.shutil.disk_usage",
        lambda _: DiskUsage(100, 79, 21),
    )
    assert lifecycle.run_once(now_ms=1_800_000_000_000)["status"] == "NORMAL"

    monkeypatch.setattr(
        "app.lifecycle.shutil.disk_usage",
        lambda _: DiskUsage(100, 85, 15),
    )
    assert lifecycle.run_once(now_ms=1_800_000_000_001)["status"] == "WARNING"

    monkeypatch.setattr(
        "app.lifecycle.shutil.disk_usage",
        lambda _: DiskUsage(100, 96, 4),
    )
    report = lifecycle.run_once(now_ms=1_800_000_000_002)
    assert report["status"] == "CRITICAL"
    assert report["warning_ratio"] == 0.80
    assert report["critical_ratio"] == 0.95
    assert report["policy"]["critical_action"] == "ALERT_ONLY_KEEP_14D_RETENTION"
    store.close()
