from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from .analytics import build_spot_demand_snapshot
from .collector import HypeSpotCollector
from .lifecycle import StorageLifecycle, StorageLifecycleConfig
from .storage_factory import create_store

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "./data/hype_spot.sqlite3")
DATABASE_URL = os.getenv("DATABASE_URL")
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "sqlite")
HYPE_COIN = os.getenv("HYPE_COIN", "@107")
HYPERLIQUID_WS_URL = os.getenv(
    "HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws"
)
SUMMARY_INTERVAL_SECONDS = max(
    30, int(os.getenv("SUMMARY_INTERVAL_SECONDS", "60"))
)
STORAGE_MAINTENANCE_INTERVAL_SECONDS = max(
    300, int(os.getenv("STORAGE_MAINTENANCE_INTERVAL_SECONDS", "3600"))
)
RAW_RETENTION_DAYS = max(4, int(os.getenv("RAW_RETENTION_DAYS", "14")))
GAP_RETENTION_DAYS = max(7, int(os.getenv("GAP_RETENTION_DAYS", "90")))
VOLUME_WARNING_RATIO = float(os.getenv("VOLUME_WARNING_RATIO", "0.80"))
VOLUME_CRITICAL_RATIO = float(os.getenv("VOLUME_CRITICAL_RATIO", "0.95"))

store = create_store(
    backend=STORAGE_BACKEND,
    db_path=DB_PATH,
    database_url=DATABASE_URL,
)
collector = HypeSpotCollector(
    store,
    coin=HYPE_COIN,
    ws_url=HYPERLIQUID_WS_URL,
)
lifecycle = StorageLifecycle(
    store,
    coin=HYPE_COIN,
    config=StorageLifecycleConfig(
        raw_retention_days=RAW_RETENTION_DAYS,
        gap_retention_days=GAP_RETENTION_DAYS,
        warning_ratio=VOLUME_WARNING_RATIO,
        critical_ratio=VOLUME_CRITICAL_RATIO,
    ),
)
collector_task: asyncio.Task | None = None
summary_task: asyncio.Task | None = None
storage_task: asyncio.Task | None = None


async def periodic_summary() -> None:
    while True:
        await asyncio.sleep(SUMMARY_INTERVAL_SECONDS)
        state = collector.snapshot()
        spot = build_spot_demand_snapshot(store, state, coin=HYPE_COIN)
        windows = spot["windows"]
        storage = lifecycle.snapshot()
        disk = storage.get("disk") or {}
        disk_ratio = disk.get("usage_ratio")
        logger.info(
            "collector_summary connected=%s quality=%s stored=%s "
            "last_trade_age_ms=%s coverage_4h=%.3f coverage_24h=%.3f "
            "coverage_3d=%.3f reconnects=%s recoveries=%s/%s unresolved_gaps=%s "
            "backend=%s storage=%s disk_ratio=%s aggregate_4h=%s aggregate_1d=%s",
            state["connected"],
            spot["data_quality"],
            spot["collector"]["stored_trades"],
            spot["collector"]["last_trade_age_ms"],
            windows["4h"]["coverage_ratio"],
            windows["24h"]["coverage_ratio"],
            windows["3d"]["coverage_ratio"],
            state["reconnects"],
            state["recovery_successes"],
            state["recovery_attempts"],
            spot["collector"]["unresolved_gaps"],
            store.backend_name,
            storage.get("status", "NOT_RUN"),
            None if disk_ratio is None else round(float(disk_ratio), 4),
            storage.get("aggregate_4h_total"),
            storage.get("aggregate_1d_total"),
        )


async def periodic_storage_maintenance() -> None:
    while True:
        try:
            report = await asyncio.to_thread(lifecycle.run_once)
            level = report["status"]
            log = logger.info
            if level == "WARNING":
                log = logger.warning
            elif level == "CRITICAL":
                log = logger.critical
            disk = report.get("disk") or {}
            log(
                "storage_maintenance status=%s backend=%s raw=%s->%s purged=%s "
                "aggregate_4h=%s aggregate_1d=%s disk_ratio=%s db_files_bytes=%s",
                level,
                store.backend_name,
                report["raw_trades_before"],
                report["raw_trades_after"],
                report["purged_raw_trades"],
                report["aggregate_4h_total"],
                report["aggregate_1d_total"],
                disk.get("usage_ratio"),
                disk.get("db_files_bytes"),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("storage_maintenance failed")
        await asyncio.sleep(STORAGE_MAINTENANCE_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global collector_task, summary_task, storage_task
    collector_task = asyncio.create_task(
        collector.run(), name="hype-spot-collector"
    )
    summary_task = asyncio.create_task(
        periodic_summary(), name="collector-summary"
    )
    storage_task = asyncio.create_task(
        periodic_storage_maintenance(), name="storage-maintenance"
    )
    try:
        yield
    finally:
        collector.stop()
        for task in (storage_task, summary_task, collector_task):
            if task:
                task.cancel()
        for task in (storage_task, summary_task, collector_task):
            if task:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        store.close()


app = FastAPI(
    title="HYPE Spot Collector",
    version="0.5.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    state = collector.snapshot()
    return {
        "status": "ok" if state["connected"] else "degraded",
        "service": "hype-spot-collector",
        "version": "0.5.0",
        "time": datetime.now(timezone.utc).isoformat(),
        "storage_backend": store.backend_name,
        "collector": state,
        "storage": lifecycle.snapshot(),
    }


@app.get("/hype/spot-demand")
def hype_spot_demand() -> dict:
    return build_spot_demand_snapshot(
        store,
        collector.snapshot(),
        coin=HYPE_COIN,
    )


@app.get("/storage/status")
def storage_status() -> dict:
    return {
        "backend": store.backend_name,
        **lifecycle.snapshot(),
    }
