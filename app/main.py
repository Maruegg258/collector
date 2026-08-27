from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from .analytics import build_spot_demand_snapshot
from .collector import HypeSpotCollector
from .storage import TradeStore

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

DB_PATH = os.getenv("DB_PATH", "./data/hype_spot.sqlite3")
HYPE_COIN = os.getenv("HYPE_COIN", "@107")
HYPERLIQUID_WS_URL = os.getenv(
    "HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws"
)
SUMMARY_INTERVAL_SECONDS = max(
    30, int(os.getenv("SUMMARY_INTERVAL_SECONDS", "60"))
)

store = TradeStore(DB_PATH)
collector = HypeSpotCollector(
    store,
    coin=HYPE_COIN,
    ws_url=HYPERLIQUID_WS_URL,
)
collector_task: asyncio.Task | None = None
summary_task: asyncio.Task | None = None


async def periodic_summary() -> None:
    while True:
        await asyncio.sleep(SUMMARY_INTERVAL_SECONDS)
        state = collector.snapshot()
        spot = build_spot_demand_snapshot(store, state, coin=HYPE_COIN)
        windows = spot["windows"]
        logger.info(
            "collector_summary connected=%s quality=%s stored=%s "
            "last_trade_age_ms=%s coverage_4h=%.3f coverage_24h=%.3f "
            "coverage_3d=%.3f reconnects=%s recoveries=%s/%s unresolved_gaps=%s",
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
        )


@asynccontextmanager
async def lifespan(_: FastAPI):
    global collector_task, summary_task
    collector_task = asyncio.create_task(
        collector.run(), name="hype-spot-collector"
    )
    summary_task = asyncio.create_task(
        periodic_summary(), name="collector-summary"
    )
    try:
        yield
    finally:
        collector.stop()
        for task in (summary_task, collector_task):
            if task:
                task.cancel()
        for task in (summary_task, collector_task):
            if task:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        store.close()


app = FastAPI(
    title="HYPE Spot Collector",
    version="0.3.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict:
    state = collector.snapshot()
    return {
        "status": "ok" if state["connected"] else "degraded",
        "service": "hype-spot-collector",
        "version": "0.3.0",
        "time": datetime.now(timezone.utc).isoformat(),
        "collector": state,
    }


@app.get("/hype/spot-demand")
def hype_spot_demand() -> dict:
    return build_spot_demand_snapshot(
        store,
        collector.snapshot(),
        coin=HYPE_COIN,
    )
