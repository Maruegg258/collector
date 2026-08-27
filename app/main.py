from __future__ import annotations

import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, Response

from .analytics import build_spot_demand_snapshot
from .collector import HypeSpotCollector
from .leases import CollectorLeaseCoordinator
from .lifecycle import StorageLifecycle, StorageLifecycleConfig
from .migration import migrate_sqlite_snapshot_to_postgres
from .storage_factory import create_store
from .storage_mirror import MirroringTradeStore

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "./data/hype_spot.sqlite3")
DATABASE_URL = os.getenv("DATABASE_URL")
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "sqlite").strip().lower()
POSTGRES_MIRROR_ENABLED = (
    os.getenv("POSTGRES_MIRROR_ENABLED", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
MIGRATE_SQLITE_TO_POSTGRES_ON_START = (
    os.getenv("MIGRATE_SQLITE_TO_POSTGRES_ON_START", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
HYPE_COIN = os.getenv("HYPE_COIN", "@107")
HYPERLIQUID_WS_URL = os.getenv("HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws")
SUMMARY_INTERVAL_SECONDS = max(30, int(os.getenv("SUMMARY_INTERVAL_SECONDS", "60")))
STORAGE_MAINTENANCE_INTERVAL_SECONDS = max(
    300, int(os.getenv("STORAGE_MAINTENANCE_INTERVAL_SECONDS", "3600"))
)
RAW_RETENTION_DAYS = max(4, int(os.getenv("RAW_RETENTION_DAYS", "14")))
GAP_RETENTION_DAYS = max(7, int(os.getenv("GAP_RETENTION_DAYS", "90")))
VOLUME_WARNING_RATIO = float(os.getenv("VOLUME_WARNING_RATIO", "0.80"))
VOLUME_CRITICAL_RATIO = float(os.getenv("VOLUME_CRITICAL_RATIO", "0.95"))
READINESS_MAX_MESSAGE_AGE_MS = max(
    10_000, int(os.getenv("READINESS_MAX_MESSAGE_AGE_MS", "30000"))
)

base_store = create_store(
    backend=STORAGE_BACKEND,
    db_path=DB_PATH,
    database_url=DATABASE_URL,
)
if POSTGRES_MIRROR_ENABLED:
    if STORAGE_BACKEND != "sqlite":
        raise RuntimeError("POSTGRES_MIRROR_ENABLED is only valid with sqlite primary")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required when POSTGRES_MIRROR_ENABLED=true")
    mirror_target = create_store(
        backend="postgres",
        db_path=DB_PATH,
        database_url=DATABASE_URL,
    )
    store = MirroringTradeStore(base_store, mirror_target)
else:
    store = base_store

lease: CollectorLeaseCoordinator | None = None
if STORAGE_BACKEND in {"postgres", "postgresql"}:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required for PostgreSQL continuity leases")
    instance_id = (
        os.getenv("RAILWAY_DEPLOYMENT_ID")
        or os.getenv("RAILWAY_REPLICA_ID")
        or f"local-{uuid.uuid4().hex}"
    )
    lease = CollectorLeaseCoordinator(DATABASE_URL, instance_id)

collector = HypeSpotCollector(
    store,
    coin=HYPE_COIN,
    ws_url=HYPERLIQUID_WS_URL,
    lease=lease,
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
migration_task: asyncio.Task | None = None
migration_state: dict = {
    "enabled": MIGRATE_SQLITE_TO_POSTGRES_ON_START,
    "status": "PENDING" if MIGRATE_SQLITE_TO_POSTGRES_ON_START else "DISABLED",
    "verified": False,
}


def mirror_state() -> dict:
    if isinstance(store, MirroringTradeStore):
        return store.mirror_snapshot()
    return {"enabled": False}


async def periodic_summary() -> None:
    while True:
        await asyncio.sleep(SUMMARY_INTERVAL_SECONDS)
        state = collector.snapshot()
        spot = build_spot_demand_snapshot(store, state, coin=HYPE_COIN)
        windows = spot["windows"]
        storage = lifecycle.snapshot()
        disk = storage.get("disk") or {}
        disk_ratio = disk.get("usage_ratio")
        mirror = mirror_state()
        logger.info(
            "collector_summary connected=%s quality=%s stored=%s "
            "last_trade_age_ms=%s coverage_4h=%.3f coverage_24h=%.3f "
            "coverage_3d=%.3f reconnects=%s recoveries=%s/%s unresolved_gaps=%s "
            "backend=%s mirror=%s mirror_failures=%s storage=%s disk_ratio=%s migration=%s",
            state["connected"], spot["data_quality"], spot["collector"]["stored_trades"],
            spot["collector"]["last_trade_age_ms"], windows["4h"]["coverage_ratio"],
            windows["24h"]["coverage_ratio"], windows["3d"]["coverage_ratio"],
            state["reconnects"], state["recovery_successes"], state["recovery_attempts"],
            spot["collector"]["unresolved_gaps"], STORAGE_BACKEND, mirror.get("enabled"),
            mirror.get("failures"), storage.get("status", "NOT_RUN"),
            None if disk_ratio is None else round(float(disk_ratio), 4),
            migration_state.get("status"),
        )


async def periodic_storage_maintenance() -> None:
    while True:
        try:
            report = await asyncio.to_thread(lifecycle.run_once)
            level = report["status"]
            log = logger.info if level == "NORMAL" else (logger.warning if level == "WARNING" else logger.critical)
            disk = report.get("disk") or {}
            log(
                "storage_maintenance status=%s backend=%s raw=%s->%s purged=%s aggregate_4h=%s aggregate_1d=%s disk_ratio=%s db_files_bytes=%s",
                level, STORAGE_BACKEND, report["raw_trades_before"], report["raw_trades_after"],
                report["purged_raw_trades"], report["aggregate_4h_total"], report["aggregate_1d_total"],
                disk.get("usage_ratio"), disk.get("db_files_bytes"),
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("storage_maintenance failed")
        await asyncio.sleep(STORAGE_MAINTENANCE_INTERVAL_SECONDS)


async def run_initial_migration() -> None:
    if not MIGRATE_SQLITE_TO_POSTGRES_ON_START:
        return
    if STORAGE_BACKEND != "sqlite":
        migration_state.update(status="SKIPPED", reason="source_backend_is_not_sqlite")
        return
    if not DATABASE_URL:
        migration_state.update(status="FAILED", reason="DATABASE_URL_missing")
        logger.error("sqlite_to_postgres_migration failed reason=DATABASE_URL_missing")
        return

    migration_state.update(status="RUNNING", started_at=datetime.now(timezone.utc).isoformat())
    try:
        report = await asyncio.to_thread(
            migrate_sqlite_snapshot_to_postgres,
            db_path=DB_PATH,
            database_url=DATABASE_URL,
            coin=HYPE_COIN,
        )
        migration_state.clear()
        migration_state.update(enabled=True, status="SUCCESS" if report.verified else "FAILED", **report.as_dict())
        log = logger.info if report.verified else logger.error
        log(
            "sqlite_to_postgres_migration status=%s verified=%s cutoff_ms=%s source=%s target=%s inserted=%s duplicates=%s meta=%s gaps=%s aggregates=%s duration_ms=%s",
            migration_state["status"], report.verified, report.cutoff_ms,
            report.source_trade_count_at_cutoff, report.target_trade_count_at_cutoff,
            report.inserted_trades, report.duplicate_trades, report.meta_rows,
            report.gap_rows, report.aggregate_rows,
            report.finished_at_ms - report.started_at_ms,
        )
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        migration_state.update(status="FAILED", reason=f"{type(exc).__name__}: {exc}")
        logger.exception("sqlite_to_postgres_migration failed")


@asynccontextmanager
async def lifespan(_: FastAPI):
    global collector_task, summary_task, storage_task, migration_task
    collector_task = asyncio.create_task(collector.run(), name="hype-spot-collector")
    summary_task = asyncio.create_task(periodic_summary(), name="collector-summary")
    storage_task = asyncio.create_task(periodic_storage_maintenance(), name="storage-maintenance")
    migration_task = asyncio.create_task(run_initial_migration(), name="sqlite-postgres-migration")
    try:
        yield
    finally:
        collector.stop()
        for task in (migration_task, storage_task, summary_task, collector_task):
            if task:
                task.cancel()
        for task in (migration_task, storage_task, summary_task, collector_task):
            if task:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        store.close()
        if lease is not None:
            lease.close()


app = FastAPI(title="HYPE Spot Collector", version="0.7.0", lifespan=lifespan)


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
        "mirror": mirror_state(),
    }


@app.get("/health")
def health() -> dict:
    state = collector.snapshot()
    return {
        "status": "ok" if state["connected"] else "degraded",
        "service": "hype-spot-collector",
        "version": "0.7.0",
        "time": datetime.now(timezone.utc).isoformat(),
        "storage_backend": STORAGE_BACKEND,
        "collector": state,
        "storage": lifecycle.snapshot(),
        "mirror": mirror_state(),
        "migration": migration_state,
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


@app.get("/migration/status")
def migration_status() -> dict:
    return dict(migration_state)
