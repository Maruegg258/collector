from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Iterable

from .storage import TradeRecord
from .storage_protocol import StorageBackend

logger = logging.getLogger(__name__)


class MirroringTradeStore:
    """Read from primary and best-effort mirror every mutation to target.

    Primary write results remain authoritative. Any mirror failure is sticky for the
    lifetime of the process so handoff cannot be declared healthy after a later,
    unrelated successful mirror write.

    During a PostgreSQL handoff, an optional active-peer check can suppress only
    UNRESOLVED gaps created by the legacy SQLite instance when a healthy stateless
    collector is already proving continuity in PostgreSQL.
    """

    def __init__(
        self,
        primary: StorageBackend,
        mirror: StorageBackend,
        *,
        active_peer_check: Callable[[], bool] | None = None,
    ) -> None:
        self.primary = primary
        self.mirror = mirror
        self.backend_name = primary.backend_name
        self.db_path = primary.db_path
        self.active_peer_check = active_peer_check
        self._state_lock = threading.RLock()
        self._mirror_writes = 0
        self._mirror_failures = 0
        self._suppressed_unresolved_gaps = 0
        self._last_mirror_error: str | None = None
        self._last_mirror_ok_ms: int | None = None

    def _record_ok(self) -> None:
        with self._state_lock:
            self._mirror_writes += 1
            self._last_mirror_ok_ms = int(time.time() * 1000)

    def _record_failure(self, operation: str, exc: Exception) -> None:
        with self._state_lock:
            self._mirror_failures += 1
            self._last_mirror_error = f"{operation}: {type(exc).__name__}: {exc}"
        logger.exception("postgres_mirror_failed operation=%s", operation)

    def _record_suppressed_gap(self) -> None:
        with self._state_lock:
            self._suppressed_unresolved_gaps += 1
            self._last_mirror_ok_ms = int(time.time() * 1000)

    def mirror_snapshot(self) -> dict:
        with self._state_lock:
            return {
                "enabled": True,
                "target_backend": self.mirror.backend_name,
                "writes": self._mirror_writes,
                "failures": self._mirror_failures,
                "suppressed_unresolved_gaps": self._suppressed_unresolved_gaps,
                "last_error": self._last_mirror_error,
                "last_ok_ms": self._last_mirror_ok_ms,
                "healthy": self._mirror_failures == 0,
            }

    def insert_many(self, records: Iterable[TradeRecord]) -> tuple[int, int]:
        records = list(records)
        primary_result = self.primary.insert_many(records)
        try:
            self.mirror.insert_many(records)
            self._record_ok()
        except Exception as exc:
            self._record_failure("insert_many", exc)
        return primary_result

    def count(self, coin: str | None = None) -> int:
        return self.primary.count(coin)

    def first_trade_time_ms(self, coin: str | None = None) -> int | None:
        return self.primary.first_trade_time_ms(coin)

    def latest_trade_time_ms(self, coin: str | None = None) -> int | None:
        return self.primary.latest_trade_time_ms(coin)

    def aggregate_window(self, coin: str, start_ms: int, end_ms: int) -> dict:
        return self.primary.aggregate_window(coin, start_ms, end_ms)

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
    ) -> None:
        self.primary.upsert_aggregate_bucket(
            coin,
            granularity,
            start_ms,
            end_ms,
            stats,
            complete=complete,
            quality=quality,
            unresolved_gap_count=unresolved_gap_count,
        )
        try:
            self.mirror.upsert_aggregate_bucket(
                coin,
                granularity,
                start_ms,
                end_ms,
                stats,
                complete=complete,
                quality=quality,
                unresolved_gap_count=unresolved_gap_count,
            )
            self._record_ok()
        except Exception as exc:
            self._record_failure("upsert_aggregate_bucket", exc)

    def aggregate_bucket_count(
        self, coin: str, granularity: str | None = None
    ) -> int:
        return self.primary.aggregate_bucket_count(coin, granularity)

    def get_aggregate_bucket(
        self, coin: str, granularity: str, start_ms: int
    ) -> dict | None:
        return self.primary.get_aggregate_bucket(coin, granularity, start_ms)

    def delete_trades_before(self, cutoff_ms: int, *, coin: str | None = None) -> int:
        deleted = self.primary.delete_trades_before(cutoff_ms, coin=coin)
        try:
            self.mirror.delete_trades_before(cutoff_ms, coin=coin)
            self._record_ok()
        except Exception as exc:
            self._record_failure("delete_trades_before", exc)
        return deleted

    def delete_gaps_before(self, cutoff_ms: int, *, coin: str | None = None) -> int:
        deleted = self.primary.delete_gaps_before(cutoff_ms, coin=coin)
        try:
            self.mirror.delete_gaps_before(cutoff_ms, coin=coin)
            self._record_ok()
        except Exception as exc:
            self._record_failure("delete_gaps_before", exc)
        return deleted

    def checkpoint_wal(self, *, truncate: bool = False) -> tuple[int, int, int]:
        return self.primary.checkpoint_wal(truncate=truncate)

    def set_meta(self, key: str, value: str | int) -> None:
        self.primary.set_meta(key, value)
        try:
            self.mirror.set_meta(key, value)
            self._record_ok()
        except Exception as exc:
            self._record_failure("set_meta", exc)

    def get_meta_int(self, key: str) -> int | None:
        return self.primary.get_meta_int(key)

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
    ) -> None:
        self.primary.add_gap(
            coin,
            start_ms,
            end_ms,
            status=status,
            reason=reason,
            recovery_earliest_ms=recovery_earliest_ms,
            recovery_latest_ms=recovery_latest_ms,
            recovered_trade_count=recovered_trade_count,
        )

        if status == "UNRESOLVED" and self.active_peer_check is not None:
            try:
                if self.active_peer_check():
                    self._record_suppressed_gap()
                    logger.info(
                        "postgres_mirror_gap_suppressed coin=%s start_ms=%s end_ms=%s reason=%s basis=active_stateless_lease",
                        coin,
                        start_ms,
                        end_ms,
                        reason,
                    )
                    return
            except Exception:
                logger.exception("postgres_mirror_active_peer_check_failed")

        try:
            self.mirror.add_gap(
                coin,
                start_ms,
                end_ms,
                status=status,
                reason=reason,
                recovery_earliest_ms=recovery_earliest_ms,
                recovery_latest_ms=recovery_latest_ms,
                recovered_trade_count=recovered_trade_count,
            )
            self._record_ok()
        except Exception as exc:
            self._record_failure("add_gap", exc)

    def gaps_overlapping(
        self,
        coin: str,
        start_ms: int,
        end_ms: int,
        *,
        status: str = "UNRESOLVED",
    ) -> list[dict]:
        return self.primary.gaps_overlapping(
            coin, start_ms, end_ms, status=status
        )

    def unresolved_gap_count(self, coin: str) -> int:
        return self.primary.unresolved_gap_count(coin)

    def ping(self) -> bool:
        try:
            self.primary.count()
            return True
        except Exception:
            return False

    def close(self) -> None:
        try:
            self.mirror.close()
        finally:
            self.primary.close()
