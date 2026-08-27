from __future__ import annotations

from collections import namedtuple

from app.lifecycle import FOUR_HOURS_MS, HOUR_MS, StorageLifecycle, StorageLifecycleConfig
from app.storage import TradeRecord
from app.storage_factory import SQLiteTradeStore as TradeStore


def add_trade(store: TradeStore, *, t: int, side: str, px: float = 100.0, sz: float = 1.0, tid: int) -> None:
    notional = px * sz
    store.insert_many([TradeRecord(coin="@107", time_ms=t, tid=tid, side=side, px=px, sz=sz, notional_usdc=notional, signed_notional_usdc=notional if side == "B" else -notional)])


def test_materializes_completed_4h_bucket(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    start = ((now // FOUR_HOURS_MS) - 1) * FOUR_HOURS_MS
    store.set_meta("coverage_epoch_ms", start - 1)
    add_trade(store, t=start + 1_000, side="B", sz=2, tid=1)
    add_trade(store, t=start + 2_000, side="A", sz=1, tid=2)
    report = StorageLifecycle(store).run_once(now_ms=now)
    bucket = store.get_aggregate_bucket("@107", "4h", start)
    assert bucket is not None
    assert bucket["net_delta_usdc"] == 100.0
    assert bucket["complete"] == 1
    assert bucket["quality"] == "COMPLETE"
    assert report["aggregate_4h_total"] >= 1
    store.close()


def test_minor_gap_archives_bucket_as_minor_gap_not_complete(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    start = ((now // FOUR_HOURS_MS) - 1) * FOUR_HOURS_MS
    store.set_meta("coverage_epoch_ms", start - 1)
    add_trade(store, t=start + 1_000, side="B", tid=1)
    store.add_gap("@107", start + 10_000, start + 11_583, status="UNRESOLVED", reason="test")
    StorageLifecycle(store).run_once(now_ms=now)
    bucket = store.get_aggregate_bucket("@107", "4h", start)
    assert bucket["quality"] == "MINOR_GAP"
    assert bucket["complete"] == 0
    store.close()


def test_material_gap_archives_bucket_as_material_gap(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    start = ((now // FOUR_HOURS_MS) - 1) * FOUR_HOURS_MS
    store.set_meta("coverage_epoch_ms", start - 1)
    add_trade(store, t=start + 1_000, side="B", tid=1)
    store.add_gap("@107", start + 10_000, start + 16_000, status="UNRESOLVED", reason="test")
    StorageLifecycle(store).run_once(now_ms=now)
    bucket = store.get_aggregate_bucket("@107", "4h", start)
    assert bucket["quality"] == "MATERIAL_GAP"
    assert bucket["complete"] == 0
    store.close()


def test_archive_written_before_12h_raw_purge(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    old_time = now - 20 * HOUR_MS
    recent_time = now - HOUR_MS
    old_start = (old_time // FOUR_HOURS_MS) * FOUR_HOURS_MS
    store.set_meta("coverage_epoch_ms", old_start - 1)
    add_trade(store, t=old_time, side="B", tid=1)
    add_trade(store, t=recent_time, side="A", tid=2)
    report = StorageLifecycle(store, config=StorageLifecycleConfig(raw_retention_hours=12)).run_once(now_ms=now)
    assert report["purged_raw_trades"] == 1
    assert store.count("@107") == 1
    archived = store.get_aggregate_bucket("@107", "4h", old_start)
    assert archived is not None
    assert archived["buy_notional_usdc"] == 100.0
    store.close()


def test_purged_boundary_bucket_not_overwritten_by_partial_raw(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    now = 1_800_000_000_000
    cutoff = now - 12 * HOUR_MS
    boundary_start = (cutoff // FOUR_HOURS_MS) * FOUR_HOURS_MS
    store.set_meta("coverage_epoch_ms", boundary_start - 1)
    add_trade(store, t=cutoff - 60_000, side="B", tid=1)
    add_trade(store, t=cutoff + 60_000, side="A", tid=2)
    lifecycle = StorageLifecycle(store, config=StorageLifecycleConfig(raw_retention_hours=12))
    lifecycle.run_once(now_ms=now)
    before = store.get_aggregate_bucket("@107", "4h", boundary_start)
    assert before is not None
    lifecycle.run_once(now_ms=now + 10 * 60 * 1000)
    after = store.get_aggregate_bucket("@107", "4h", boundary_start)
    assert after["trade_count"] == before["trade_count"]
    store.close()


def test_postgres_capacity_not_falsely_reported_normal(tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    lifecycle = StorageLifecycle(store)
    assert lifecycle.snapshot()["status"] == "NOT_RUN"
    store.close()


def test_disk_guardrail_levels(monkeypatch, tmp_path):
    store = TradeStore(str(tmp_path / "test.sqlite3"))
    DiskUsage = namedtuple("DiskUsage", "total used free")
    lifecycle = StorageLifecycle(store)
    monkeypatch.setattr("app.lifecycle.shutil.disk_usage", lambda _: DiskUsage(100, 85, 15))
    assert lifecycle.run_once(now_ms=1_800_000_000_000)["status"] == "WARNING"
    monkeypatch.setattr("app.lifecycle.shutil.disk_usage", lambda _: DiskUsage(100, 96, 4))
    report = lifecycle.run_once(now_ms=1_800_000_000_001)
    assert report["status"] == "CRITICAL"
    assert report["policy"]["raw_compaction"] == "12H_RAW_PLUS_DURABLE_4H_ARCHIVE"
    assert report["policy"]["spot_integrity_policy"] == "HYPE_PROTOCOL_V1_1_CONTINUITY_MATERIALITY"
    store.close()
