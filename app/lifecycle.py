from __future__ import annotations

import shutil
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .storage_protocol import StorageBackend

HOUR_MS = 60 * 60 * 1000
FOUR_HOURS_MS = 4 * HOUR_MS
DAY_MS = 24 * HOUR_MS


@dataclass(frozen=True)
class StorageLifecycleConfig:
    # Raw trades are only needed for the live/current completed 4H boundary,
    # reconnect diagnostics, and short-horizon forensic work. Protocol-facing
    # 24H/3D history is reconstructed from durable completed 4H aggregates.
    raw_retention_hours: int = 12
    gap_retention_days: int = 90
    warning_ratio: float = 0.80
    critical_ratio: float = 0.95


class StorageLifecycle:
    def __init__(
        self,
        store: StorageBackend,
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
            "backend": store.backend_name,
            "raw_retention_hours": config.raw_retention_hours,
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

    def _materialize_completed_4h(
        self,
        *,
        now_ms: int,
        raw_purged_before_ms: int | None,
    ) -> int:
        first_raw_ms = self.store.first_trade_time_ms(self.coin)
        if first_raw_ms is None:
            return 0

        start_ms = (first_raw_ms // FOUR_HOURS_MS) * FOUR_HOURS_MS
        if raw_purged_before_ms is not None:
            # Never overwrite an archived boundary bucket using only the raw
            # tail that remains after compaction.
            start_ms = max(
                start_ms,
                self._ceil_to_bucket(raw_purged_before_ms, FOUR_HOURS_MS),
            )
        final_end_ms = (now_ms // FOUR_HOURS_MS) * FOUR_HOURS_MS
        materialized = 0

        while start_ms + FOUR_HOURS_MS <= final_end_ms:
            end_ms = start_ms + FOUR_HOURS_MS
            stats = self.store.aggregate_window(self.coin, start_ms, end_ms)
            complete, quality, unresolved_gap_count = self._bucket_quality(start_ms, end_ms)
            self.store.upsert_aggregate_bucket(
                self.coin,
                "4h",
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

    def _db_files_bytes(self) -> int | None:
        if self.store.backend_name != "sqlite" or not self.store.db_path:
            return None
        db_path = Path(self.store.db_path)
        candidates = [db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")]
        total = 0
        for path in candidates:
            try:
                total += path.stat().st_size
            except FileNotFoundError:
                pass
        return total

    def _disk_status(self) -> dict[str, Any]:
        if self.store.backend_name != "sqlite" or not self.store.db_path:
            return {
                "level": "EXTERNAL_MONITOR_REQUIRED",
                "used_bytes": None,
                "total_bytes": None,
                "free_bytes": None,
                "usage_ratio": None,
                "db_files_bytes": None,
                "note": (
                    "PostgreSQL volume utilization must be monitored by Railway metrics; "
                    "the stateless collector cannot inspect the database service volume."
                ),
            }

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
        raw_cutoff_ms = now_ms - self.config.raw_retention_hours * HOUR_MS
        gap_cutoff_ms = now_ms - self.config.gap_retention_days * DAY_MS

        with self._lock:
            previous_raw_waterline_ms = self.store.get_meta_int("raw_purged_before_ms")

            # Archive all completed 4H buckets before deleting any raw rows.
            four_h_materialized = self._materialize_completed_4h(
                now_ms=now_ms,
                raw_purged_before_ms=previous_raw_waterline_ms,
            )

            raw_before = self.store.count(self.coin)
            purged_raw = self.store.delete_trades_before(raw_cutoff_ms, coin=self.coin)
            raw_after = self.store.count(self.coin)
            raw_waterline_ms = max(previous_raw_waterline_ms or 0, raw_cutoff_ms)
            self.store.set_meta("raw_purged_before_ms", raw_waterline_ms)

            purged_gaps = self.store.delete_gaps_before(gap_cutoff_ms, coin=self.coin)
            wal_checkpoint = self.store.checkpoint_wal()
            disk = self._disk_status()

            status = disk["level"]
            if status == "EXTERNAL_MONITOR_REQUIRED":
                # Do not falsely call PostgreSQL volume capacity NORMAL.
                status = "EXTERNAL_MONITOR_REQUIRED"

            report = {
                "status": status,
                "backend": self.store.backend_name,
                "ran_at_ms": now_ms,
                "raw_retention_hours": self.config.raw_retention_hours,
                "gap_retention_days": self.config.gap_retention_days,
                "warning_ratio": self.config.warning_ratio,
                "critical_ratio": self.config.critical_ratio,
                "raw_cutoff_ms": raw_cutoff_ms,
                "raw_purged_before_ms": raw_waterline_ms,
                "gap_cutoff_ms": gap_cutoff_ms,
                "raw_trades_before": raw_before,
                "raw_trades_after": raw_after,
                "purged_raw_trades": purged_raw,
                "purged_gap_records": purged_gaps,
                "aggregate_4h_materialized": four_h_materialized,
                "aggregate_4h_total": self.store.aggregate_bucket_count(self.coin, "4h"),
                "wal_checkpoint": {
                    "busy": wal_checkpoint[0],
                    "log_frames": wal_checkpoint[1],
                    "checkpointed_frames": wal_checkpoint[2],
                },
                "disk": disk,
                "policy": {
                    "raw_compaction": "12H_RAW_PLUS_DURABLE_4H_ARCHIVE",
                    "archive_retention": "INDEFINITE",
                    "postgres_capacity_source": "RAILWAY_METRICS",
                    "critical_action": "ALERT_AND_REVIEW_VOLUME_OR_RETENTION",
                    "note": (
                        "Protocol-facing 24H/3D Spot Delta is reconstructed from completed "
                        "4H archives, so raw compaction does not discard required history."
                    ),
                },
            }
            self._last_report = report
            return report

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._last_report)
