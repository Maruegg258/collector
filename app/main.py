from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from .collector import HypeSpotCollector
from .storage import TradeStore

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

DB_PATH = os.getenv("DB_PATH", "./data/hype_spot.sqlite3")
HYPE_COIN = os.getenv("HYPE_COIN", "@107")
HYPERLIQUID_WS_URL = os.getenv("HYPERLIQUID_WS_URL", "wss://api.hyperliquid.xyz/ws")

store = TradeStore(DB_PATH)
collector = HypeSpotCollector(store, coin=HYPE_COIN, ws_url=HYPERLIQUID_WS_URL)
collector_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    global collector_task
    collector_task = asyncio.create_task(collector.run(), name="hype-spot-collector")
    try:
        yield
    finally:
        collector.stop()
        if collector_task:
            collector_task.cancel()
            try:
                await collector_task
            except asyncio.CancelledError:
                pass
        store.close()


app = FastAPI(title="HYPE Spot Collector MVP", version="0.1.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    state = collector.snapshot()
    return {
        "status": "ok" if state["connected"] else "degraded",
        "service": "hype-spot-collector",
        "version": "0.1.0",
        "time": datetime.now(timezone.utc).isoformat(),
        "collector": state,
    }
