from __future__ import annotations

import os
import uuid

import pytest

from app.leases import CollectorLeaseCoordinator
from app.storage import TradeRecord
from app.storage_factory import SQLiteTradeStore
from app.storage_mirror import MirroringTradeStore
from app.storage_postgres import PostgresTradeStore


def test_postgres_leases_recognize_active_overlap_and_stop_is_terminal():
    database_url = os.getenv("TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("TEST_POSTGRES_URL not configured")

    prefix = uuid.uuid4().hex
    a = CollectorLeaseCoordinator(database_url, f"a-{prefix}", stale_after_ms=15_000)
    b = CollectorLeaseCoordinator(database_url, f"b-{prefix}", stale_after_ms=15_000)
    try:
        now = 1_800_000_000_000
        a.update(connected=True, heartbeat_ms=now, last_message_at_ms=now)
        assert b.active_other_exists(now_ms=now + 1_000) is True
        assert b.latest_other_heartbeat_ms() == now

        b.update(connected=True, heartbeat_ms=now + 2_000, last_message_at_ms=now + 2_000)
        assert a.active_other_exists(now_ms=now + 3_000) is True

        a.update(connected=False, heartbeat_ms=now + 4_000, last_message_at_ms=now + 3_000, stopped=True)
        assert b.active_other_exists(now_ms=now + 5_000) is False

        # A late heartbeat from a shutting-down task must not resurrect A.
        a.update(connected=True, heartbeat_ms=now + 6_000, last_message_at_ms=now + 6_000)
        a_snapshot = a.snapshot()
        assert a_snapshot["own_connected"] is False
        assert a_snapshot["own_stopped_at_ms"] is not None

        snapshot = b.snapshot()
        assert snapshot["active_instances"] >= 1
        assert snapshot["own_connected"] is True
    finally:
        a.close()
        b.close()


def test_sqlite_primary_mirrors_mutations_to_postgres(tmp_path):
    database_url = os.getenv("TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("TEST_POSTGRES_URL not configured")

    coin = f"mirror-{uuid.uuid4().hex}"
    primary = SQLiteTradeStore(str(tmp_path / "mirror.sqlite3"))
    target = PostgresTradeStore(database_url)
    store = MirroringTradeStore(primary, target)
    try:
        trade = TradeRecord(
            coin=coin,
            time_ms=1_800_000_000_000,
            tid=1,
            side="B",
            px=100.0,
            sz=2.0,
            notional_usdc=200.0,
            signed_notional_usdc=200.0,
        )
        assert store.insert_many([trade]) == (1, 0)
        assert primary.count(coin) == 1
        assert target.count(coin) == 1

        store.set_meta(f"mirror_meta_{coin}", 123)
        assert target.get_meta_int(f"mirror_meta_{coin}") == 123

        store.add_gap(
            coin,
            trade.time_ms + 10,
            trade.time_ms + 20,
            status="UNRESOLVED",
            reason="mirror-test",
        )
        assert target.unresolved_gap_count(coin) == 1

        stats = primary.aggregate_window(coin, trade.time_ms, trade.time_ms + 1_000)
        store.upsert_aggregate_bucket(
            coin,
            "4h",
            trade.time_ms,
            trade.time_ms + 14_400_000,
            stats,
            complete=False,
            quality="GAPPED",
            unresolved_gap_count=1,
        )
        mirrored_bucket = target.get_aggregate_bucket(coin, "4h", trade.time_ms)
        assert mirrored_bucket is not None
        assert mirrored_bucket["trade_count"] == 1
        assert mirrored_bucket["quality"] == "GAPPED"

        mirror = store.mirror_snapshot()
        assert mirror["failures"] == 0
        assert mirror["healthy"] is True
    finally:
        store.close()


def test_mirror_failure_remains_unhealthy_after_later_success(monkeypatch, tmp_path):
    database_url = os.getenv("TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("TEST_POSTGRES_URL not configured")

    primary = SQLiteTradeStore(str(tmp_path / "sticky.sqlite3"))
    target = PostgresTradeStore(database_url)
    store = MirroringTradeStore(primary, target)
    original_insert = target.insert_many
    calls = {"n": 0}

    def fail_once(records):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("synthetic mirror failure")
        return original_insert(records)

    monkeypatch.setattr(target, "insert_many", fail_once)
    try:
        first = TradeRecord("sticky", 1_800_000_000_000, 1, "B", 100.0, 1.0, 100.0, 100.0)
        second = TradeRecord("sticky", 1_800_000_001_000, 2, "B", 100.0, 1.0, 100.0, 100.0)
        assert store.insert_many([first]) == (1, 0)
        assert store.insert_many([second]) == (1, 0)
        state = store.mirror_snapshot()
        assert state["failures"] == 1
        assert state["healthy"] is False
        assert "synthetic mirror failure" in state["last_error"]
    finally:
        store.close()
