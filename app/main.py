from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Response

from .analytics import build_spot_demand_snapshot
from .collector import HypeSpotCollector
from .leases import CollectorLeaseCoordinator
from .lifecycle import StorageLifecycle, StorageLifecycleConfig
from .storage_factory import create_store

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "./data/hype_spot.sqlite3")
DATABASE_URL = os.getenv("DATABASE_URL")
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "sqlite").strip().lower()
HYPE_COIN = os.getenv("HYPE_COIN", "@107")
HYPERLIQUID_WS_URL = os.getenv("HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws")
SUMMARY_INTERVAL_SECONDS = max(30, int(os.getenv("SUMMARY_INTERVAL_SECONDS", "60")))
STORAGE_MAINTENANCE_INTERVAL_SECONDS = max(300, int(os.getenv("STORAGE_MAINTENANCE_INTERVAL_SECONDS", "3600")))
RAW_RETENTION_HOURS = max(8, int(os.getenv("RAW_RETENTION_HOURS", "12")))
# Protocol v1.2.1 keeps continuity-gap metadata as durable engineering facts.
GAP_RETENTION_DAYS: int | None = None
VOLUME_WARNING_RATIO = float(os.getenv("VOLUME_WARNING_RATIO", "0.80"))
VOLUME_CRITICAL_RATIO = float(os.getenv("VOLUME_CRITICAL_RATIO", "0.95"))
READINESS_MAX_MESSAGE_AGE_MS = max(10_000, int(os.getenv("READINESS_MAX_MESSAGE_AGE_MS", "30000")))

store = create_store(backend=STORAGE_BACKEND, db_path=DB_PATH, database_url=DATABASE_URL)

lease: CollectorLeaseCoordinator | None = None
if STORAGE_BACKEND in {"postgres", "postgresql"}:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required for PostgreSQL continuity leases")
    instance_id = os.getenv("RAILWAY_DEPLOYMENT_ID") or os.getenv("RAILWAY_REPLICA_ID") or f"local-{uuid.uuid4().hex}"
    lease = CollectorLeaseCoordinator(DATABASE_URL, instance_id)

collector = HypeSpotCollector(store, coin=HYPE_COIN, ws_url=HYPERLIQUID_WS_URL, lease=lease)
lifecycle = StorageLifecycle(
    store,
    coin=HYPE_COIN,
    config=StorageLifecycleConfig(
        raw_retention_hours=RAW_RETENTION_HOURS,
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
        logger.info(
            "collector_summary connected=%s quality=%s stored=%s last_trade_age_ms=%s "
            "coverage_4h=%.3f coverage_24h=%.3f coverage_3d=%.3f reconnects=%s "
            "recoveries=%s/%s unresolved_gaps=%s backend=%s storage=%s raw_hours=%s",
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
            STORAGE_BACKEND,
            storage.get("status", "NOT_RUN"),
            RAW_RETENTION_HOURS,
        )


async def periodic_storage_maintenance() -> None:
    while True:
        try:
            report = await asyncio.to_thread(lifecycle.run_once)
            logger.info(
                "storage_maintenance status=%s backend=%s raw=%s->%s purged=%s "
                "aggregate_4h=%s raw_hours=%s gap_retention=%s",
                report["status"],
                STORAGE_BACKEND,
                report["raw_trades_before"],
                report["raw_trades_after"],
                report["purged_raw_trades"],
                report["aggregate_4h_total"],
                report["raw_retention_hours"],
                report["policy"]["gap_metadata_retention"],
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("storage_maintenance failed")
        await asyncio.sleep(STORAGE_MAINTENANCE_INTERVAL_SECONDS)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global collector_task, summary_task, storage_task
    # Archive/compact once before the collector settles into periodic maintenance.
    await asyncio.to_thread(lifecycle.run_once)
    collector_task = asyncio.create_task(collector.run(), name="hype-spot-collector")
    summary_task = asyncio.create_task(periodic_summary(), name="collector-summary")
    storage_task = asyncio.create_task(periodic_storage_maintenance(), name="storage-maintenance")
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
        if lease is not None:
            lease.close()


app = FastAPI(title="HYPE Spot Collector", version="1.2.1", lifespan=lifespan)


def _readiness_snapshot() -> tuple[bool, dict]:
    state = collector.snapshot()
    now_ms = int(time.time() * 1000)
    try:
        store.count(HYPE_COIN)
        database_ok = True
        database_error = None
    except Exception as exc:
        database_ok = False
        database_error = f"{type(exc).__name__}: {exc}"
    last_message = state.get("last_message_at_ms")
    message_age_ms = None if last_message is None else max(0, now_ms - int(last_message))
    fresh = message_age_ms is not None and message_age_ms <= READINESS_MAX_MESSAGE_AGE_MS
    ready = bool(state["connected"] and database_ok and fresh)
    return ready, {
        "ready": ready,
        "storage_backend": STORAGE_BACKEND,
        "database_ok": database_ok,
        "database_error": database_error,
        "ws_connected": state["connected"],
        "last_message_age_ms": message_age_ms,
        "max_message_age_ms": READINESS_MAX_MESSAGE_AGE_MS,
        "continuity_mode": state.get("continuity_mode"),
        "lease": state.get("lease"),
    }


@app.get("/health")
def health() -> dict:
    state = collector.snapshot()
    return {
        "status": "ok" if state["connected"] else "degraded",
        "service": "hype-spot-collector",
        "version": "1.2.1",
        "protocol_compatibility": "HYPE_SWING_LONG_PROTOCOL_v1.2.1",
        "time": datetime.now(timezone.utc).isoformat(),
        "storage_backend": STORAGE_BACKEND,
        "collector": state,
        "storage": lifecycle.snapshot(),
    }


@app.get("/readiness")
def readiness(response: Response) -> dict:
    ready, payload = _readiness_snapshot()
    response.status_code = 200 if ready else 503
    return payload


@app.get("/hype/spot-demand")
def hype_spot_demand() -> dict:
    return build_spot_demand_snapshot(store, collector.snapshot(), coin=HYPE_COIN)


@app.get("/storage/status")
def storage_status() -> dict:
    return {"backend": STORAGE_BACKEND, **lifecycle.snapshot()}
