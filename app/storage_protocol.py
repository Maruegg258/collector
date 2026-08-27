from __future__ import annotations

from typing import Iterable, Protocol

from .storage import TradeRecord


class StorageBackend(Protocol):
    backend_name: str
    db_path: str | None

    def insert_many(self, records: Iterable[TradeRecord]) -> tuple[int, int]: ...
    def count(self, coin: str | None = None) -> int: ...
    def first_trade_time_ms(self, coin: str | None = None) -> int | None: ...
    def latest_trade_time_ms(self, coin: str | None = None) -> int | None: ...
    def aggregate_window(self, coin: str, start_ms: int, end_ms: int) -> dict: ...
    def upsert_aggregate_bucket(
        self,
        coin: str,
        granularity: str,
        start_ms: int,
        end_ms: int,
        stats: dict,
        *,
        complete: bool,
        quality: str,
        unresolved_gap_count: int,
    ) -> None: ...
    def aggregate_bucket_count(self, coin: str, granularity: str | None = None) -> int: ...
    def get_aggregate_bucket(self, coin: str, granularity: str, start_ms: int) -> dict | None: ...
    def delete_trades_before(self, cutoff_ms: int, *, coin: str | None = None) -> int: ...
    def delete_gaps_before(self, cutoff_ms: int, *, coin: str | None = None) -> int: ...
    def checkpoint_wal(self, *, truncate: bool = False) -> tuple[int, int, int]: ...
    def set_meta(self, key: str, value: str | int) -> None: ...
    def get_meta_int(self, key: str) -> int | None: ...
    def add_gap(
        self,
        coin: str,
        start_ms: int,
        end_ms: int,
        *,
        status: str,
        reason: str,
        recovery_earliest_ms: int | None = None,
        recovery_latest_ms: int | None = None,
        recovered_trade_count: int = 0,
    ) -> None: ...
    def gaps_overlapping(
        self,
        coin: str,
        start_ms: int,
        end_ms: int,
        *,
        status: str = "UNRESOLVED",
    ) -> list[dict]: ...
    def unresolved_gap_count(self, coin: str) -> int: ...
    def close(self) -> None: ...
