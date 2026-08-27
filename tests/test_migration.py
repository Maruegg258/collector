from __future__ import annotations

import os

import pytest

from app.migration import migrate_sqlite_snapshot_to_postgres
from app.storage import TradeRecord, TradeStore
from app.storage_postgres import PostgresTradeStore


def test_sqlite_snapshot_migrates_and_is_idempotent(tmp_path):
    database_url = os.getenv("TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("TEST_POSTGRES_URL not configured")

    coin = "@107"
    db_path = tmp_path / "migration.sqlite3"
    source = TradeStore(str(db_path))
    try:
        source.insert_many(
            [
                TradeRecord(coin, 1_800_000_000_000, 1, "B", 100.0, 2.0, 200.0, 200.0),
                TradeRecord(coin, 1_800_000_001_000, 2, "A", 100.0, 1.0, 100.0, -100.0),
            ]
        )
        source.set_meta("coverage_epoch_ms", 1_799_999_000_000)
        source.add_gap(
            coin,
            1_800_000_000_100,
            1_800_000_000_200,
            status="UNRESOLVED",
            reason="migration_test",
        )
        stats = source.aggregate_window(coin, 1_800_000_000_000, 1_800_000_002_000)
        source.upsert_aggregate_bucket(
            coin,
            "4h",
            1_800_000_000_000,
            1_800_014_400_000,
            stats,
            complete=False,
            quality="GAPPED",
            unresolved_gap_count=1,
        )
    finally:
        source.close()

    first = migrate_sqlite_snapshot_to_postgres(
        db_path=str(db_path),
        database_url=database_url,
        coin=coin,
        batch_size=1,
    )
    assert first.verified is True
    assert first.source_trade_count_at_cutoff == 2
    assert first.target_trade_count_at_cutoff >= 2
    assert first.inserted_trades >= 2
    assert first.meta_rows >= 1
    assert first.gap_rows == 1
    assert first.aggregate_rows == 1

    second = migrate_sqlite_snapshot_to_postgres(
        db_path=str(db_path),
        database_url=database_url,
        coin=coin,
        batch_size=1,
    )
    assert second.verified is True
    assert second.source_trade_count_at_cutoff == 2
    assert second.duplicate_trades >= 2

    target = PostgresTradeStore(database_url)
    try:
        assert target.count(coin) >= 2
        assert target.get_meta_int("coverage_epoch_ms") == 1_799_999_000_000
        assert target.unresolved_gap_count(coin) >= 1
        bucket = target.get_aggregate_bucket(coin, "4h", 1_800_000_000_000)
        assert bucket is not None
        assert bucket["trade_count"] == 2
        assert bucket["quality"] == "GAPPED"
    finally:
        target.close()
