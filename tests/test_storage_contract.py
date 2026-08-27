from __future__ import annotations

import os
import uuid

import pytest

from app.storage import TradeRecord
from app.storage_factory import SQLiteTradeStore
from app.storage_postgres import PostgresTradeStore


def exercise_storage_contract(store, coin: str) -> None:
    t0 = 1_800_000_000_000
    records = [
        TradeRecord(
            coin=coin,
            time_ms=t0,
            tid=1,
            side="B",
            px=100.0,
            sz=2.0,
            notional_usdc=200.0,
            signed_notional_usdc=200.0,
        ),
        TradeRecord(
            coin=coin,
            time_ms=t0 + 1_000,
            tid=2,
            side="A",
            px=100.0,
            sz=1.0,
            notional_usdc=100.0,
            signed_notional_usdc=-100.0,
        ),
    ]

    assert store.insert_many(records) == (2, 0)
    assert store.insert_many(records) == (0, 2)
    assert store.count(coin) == 2
    assert store.first_trade_time_ms(coin) == t0
    assert store.latest_trade_time_ms(coin) == t0 + 1_000

    stats = store.aggregate_window(coin, t0, t0 + 2_000)
    assert stats["trade_count"] == 2
    assert stats["buy_notional_usdc"] == 200.0
    assert stats["sell_notional_usdc"] == 100.0
    assert stats["net_delta_usdc"] == 100.0
    assert stats["base_delta_hype"] == 1.0
    assert stats["total_notional_usdc"] == 300.0
    assert stats["delta_ratio"] == pytest.approx(1 / 3)

    meta_key = f"contract_{uuid.uuid4().hex}"
    store.set_meta(meta_key, 12345)
    assert store.get_meta_int(meta_key) == 12345

    store.add_gap(
        coin,
        t0 + 200,
        t0 + 400,
        status="UNRESOLVED",
        reason="contract",
    )
    assert store.unresolved_gap_count(coin) == 1
    assert len(store.gaps_overlapping(coin, t0, t0 + 1_000)) == 1

    store.upsert_aggregate_bucket(
        coin,
        "4h",
        t0,
        t0 + 14_400_000,
        stats,
        complete=False,
        quality="GAPPED",
        unresolved_gap_count=1,
    )
    assert store.aggregate_bucket_count(coin, "4h") == 1
    bucket = store.get_aggregate_bucket(coin, "4h", t0)
    assert bucket is not None
    assert bucket["trade_count"] == 2
    assert bucket["quality"] == "GAPPED"

    assert store.delete_trades_before(t0 + 500, coin=coin) == 1
    assert store.count(coin) == 1
    assert store.delete_gaps_before(t0 + 500, coin=coin) == 1
    assert store.unresolved_gap_count(coin) == 0


def test_sqlite_storage_contract(tmp_path):
    store = SQLiteTradeStore(str(tmp_path / "contract.sqlite3"))
    try:
        exercise_storage_contract(store, f"sqlite-{uuid.uuid4().hex}")
    finally:
        store.close()


def test_postgres_storage_contract():
    database_url = os.getenv("TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("TEST_POSTGRES_URL not configured")
    store = PostgresTradeStore(database_url)
    try:
        exercise_storage_contract(store, f"pg-{uuid.uuid4().hex}")
    finally:
        store.close()
