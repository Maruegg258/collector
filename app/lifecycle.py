from __future__ import annotations

import shutil
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .storage import TradeStore

HOUR_MS = 60 * 60 * 1000
FOUR_HOURS_MS = 4 * HOUR_MS
DAY_MS = 24 * HOUR_MS


@dataclass(frozen=True)
class StorageLifecycleConfig:
    raw_retention_days: int = 14
    gap_retention_days: int = 90
    warning_ratio: float = 0.60
    critical_ratio: float = 0.80


class StorageLifecycle:
    def __init__(
        self,
        store: TradeStore,
        *,
        coin: str = "@107",
        config: StorageLifecycleConfig = StorageLifecycleConfig(),
    ) -> None:
        self.store = store
        self.coin = coin
        self.config = config
        self._lock = threading.RLock()
        self._last_report: dict[str, Any] = {
            "status": "NOT_RUN",
            "raw_retention_days": config.raw_retention_days,
            "gap_retention_days": config.gap_retention_days,
            "warning_ratio": config.warning_ratio,
            "critical_ratio": config.critical_ratio,
        }

    def _bucket_quality(self, start_ms: int, end_ms: int) -> tuple[bool, str, int]:
        coverage_epoch_ms = self.store.get_meta_int("coverage_epoch_ms")
        gaps = self.store.gaps_overlapping(self.coin, start_ms, end_ms)
        complete = (
            coverage_epoch_ms is not None
            and coverage_epoch_ms <= start_ms
            and len(gaps) == 0
        )
        quality = "COMPLETE" if complete else ("GAPPED" if gaps else "PARTIAL_HISTORY")
        return complete, quality, len(gaps)

    @staticmethod
    def _ceil_to_bucket(value_ms: int, bucket_ms: int) -> int:
        return ((value_ms + bucket_ms - 1) // bucket_ms) * bucket_ms

    def _materialize_granularity(
        self,
        *,
        granularity: str,
        bucket_ms: int,
        now_ms: int,
        raw_purged_before_ms: int | None,
    ) -> int:
        first_raw_ms = self.store.first_trade_time_ms(self.coin)
        if first_raw_ms is None:
            return 0

        start_ms = (first_raw_ms // bucket_ms) * bucket_ms
        if raw_purged_before_ms is not None:
            start_ms = max(
                start_ms,
                self._ceil_to_bucket(raw_purged_before_ms, bucket_ms),
            )
        final_end_ms = (now_ms // bucket_ms) * bucket_ms
        materialized = 0

        while start_ms + bucket_ms <= final_end_ms:
            end_ms = start_ms + bucket_ms
            stats = self.store.aggregate_window(self.coin, start_ms, end_ms)
            complete, quality, unresolved_gap_count = self._bucket_quality(
                start_ms, end_ms
            )
            self.store.upsert_aggregate_bucket(
                self.coin,
                granularity,
                start_ms,
                end_ms,
                stats,
                complete=complete,
                quality=quality,
                unresolved_gap_count=unresolved_gap_count,
            )
            materialized += 1
            start_ms = end_ms

        return materialized

    def _db_files_bytes(self) -> int:
        db_path = Path(self.store.db_path)
        candidates = [
            db_path,
            Path(str(db_path) + "-wal"),
            Path(str(db_path) + "-shm"),
        ]
        total = 0
        for path in candidates:
            try:
                total += path.stat().st_size
            except FileNotFoundError:
                pass
        return total

    def _disk_status(self) -> dict[str, Any]:
        db_parent = Path(self.store.db_path).resolve().parent
        usage = shutil.disk_usage(db_parent)
        ratio = usage.used / usage.total if usage.total else 0.0
        if ratio >= self.config.critical_ratio:
            level = "CRITICAL"
        elif ratio >= self.config.warning_ratio:
            level = "WARNING"
        else:
            level = "NORMAL"
        return {
            "level": level,
            "used_bytes": usage.used,
            "total_bytes": usage.total,
            "free_bytes": usage.free,
            "usage_ratio": ratio,
            "db_files_bytes": self._db_files_bytes(),
        }

    def run_once(self, *, now_ms: int | None = None) -> dict[str, Any]:
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        raw_cutoff_ms = now_ms - self.config.raw_retention_days * DAY_MS
        gap_cutoff_ms = now_ms - self.config.gap_retention_days * DAY_MS

        with self._lock:
            previous_raw_waterline_ms = self.store.get_meta_int("raw_purged_before_ms")
            four_h_materialized = self._materialize_granularity(
                granularity="4h",
                bucket_ms=FOUR_HOURS_MS,
                now_ms=now_ms,
                raw_purged_before_ms=previous_raw_waterline_ms,
            )
            daily_materialized = self._materialize_granularity(
                granularity="1d",
                bucket_ms=DAY_MS,
                now_ms=now_ms,
                raw_purged_before_ms=previous_raw_waterline_ms,
            )

            raw_before = self.store.count(self.coin)
            purged_raw = self.store.delete_trades_before(
                raw_cutoff_ms,
                coin=self.coin,
            )
            raw_after = self.store.count(self.coin)
            raw_waterline_ms = max(previous_raw_waterline_ms or 0, raw_cutoff_ms)
            self.store.set_meta("raw_purged_before_ms", raw_waterline_ms)

            purged_gaps = self.store.delete_gaps_before(
                gap_cutoff_ms,
                coin=self.coin,
            )
            wal_checkpoint = self.store.checkpoint_wal()
            disk = self._disk_status()

            report = {
                "status": disk["level"],
                "ran_at_ms": now_ms,
                "raw_retention_days": self.config.raw_retention_days,
                "gap_retention_days": self.config.gap_retention_days,
                "raw_cutoff_ms": raw_cutoff_ms,
                "raw_purged_before_ms": raw_waterline_ms,
                "gap_cutoff_ms": gap_cutoff_ms,
                "raw_trades_before": raw_before,
                "raw_trades_after": raw_after,
                "purged_raw_trades": purged_raw,
                "purged_gap_records": purged_gaps,
                "aggregate_4h_materialized": four_h_materialized,
                "aggregate_1d_materialized": daily_materialized,
                "aggregate_4h_total": self.store.aggregate_bucket_count(
                    self.coin, "4h"
                ),
                "aggregate_1d_total": self.store.aggregate_bucket_count(
                    self.coin, "1d"
                ),
                "wal_checkpoint": {
                    "busy": wal_checkpoint[0],
                    "log_frames": wal_checkpoint[1],
                    "checkpointed_frames": wal_checkpoint[2],
                },
                "disk": disk,
                "policy": {
                    "critical_action": "ALERT_ONLY_KEEP_14D_RETENTION",
                    "note": (
                        "Critical disk usage does not silently shorten raw retention; "
                        "operator intervention is required."
                    ),
                },
            }
            self._last_report = report
            return report

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._last_report)
