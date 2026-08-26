from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass
from typing import Any

import websockets

from .storage import TradeRecord, TradeStore

logger = logging.getLogger(__name__)


@dataclass
class CollectorState:
    connected: bool = False
    started_at_ms: int = 0
    connected_at_ms: int | None = None
    last_message_at_ms: int | None = None
    last_trade_time_ms: int | None = None
    messages_seen: int = 0
    trades_seen: int = 0
    trades_inserted: int = 0
    duplicates_ignored: int = 0
    reconnects: int = 0
    last_error: str | None = None


class HypeSpotCollector:
    def __init__(
        self,
        store: TradeStore,
        *,
        coin: str = "@107",
        ws_url: str = "wss://api.hyperliquid.xyz/ws",
    ) -> None:
        self.store = store
        self.coin = coin
        self.ws_url = ws_url
        self.state = CollectorState(started_at_ms=int(time.time() * 1000))
        self._stop = asyncio.Event()

    @staticmethod
    def parse_trade(raw: dict[str, Any]) -> TradeRecord:
        side = str(raw["side"])
        if side not in {"B", "A"}:
            raise ValueError(f"unsupported trade side: {side!r}")

        px = float(raw["px"])
        sz = float(raw["sz"])
        notional = px * sz
        signed = notional if side == "B" else -notional

        return TradeRecord(
            coin=str(raw["coin"]),
            time_ms=int(raw["time"]),
            tid=int(raw["tid"]),
            side=side,
            px=px,
            sz=sz,
            notional_usdc=notional,
            signed_notional_usdc=signed,
            trade_hash=raw.get("hash"),
        )

    def process_message(self, message: dict[str, Any]) -> tuple[int, int]:
        self.state.messages_seen += 1
        self.state.last_message_at_ms = int(time.time() * 1000)

        if message.get("channel") != "trades":
            return 0, 0

        data = message.get("data") or []
        records: list[TradeRecord] = []
        for raw in data:
            if raw.get("coin") != self.coin:
                continue
            record = self.parse_trade(raw)
            records.append(record)
            self.state.trades_seen += 1
            if (
                self.state.last_trade_time_ms is None
                or record.time_ms > self.state.last_trade_time_ms
            ):
                self.state.last_trade_time_ms = record.time_ms

        inserted, duplicates = self.store.insert_many(records)
        self.state.trades_inserted += inserted
        self.state.duplicates_ignored += duplicates
        return inserted, duplicates

    async def run(self) -> None:
        backoff = 1.0
        first_attempt = True
        while not self._stop.is_set():
            try:
                if not first_attempt:
                    self.state.reconnects += 1
                first_attempt = False

                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=10000,
                ) as ws:
                    self.state.connected = True
                    self.state.connected_at_ms = int(time.time() * 1000)
                    self.state.last_error = None
                    backoff = 1.0

                    await ws.send(
                        json.dumps(
                            {
                                "method": "subscribe",
                                "subscription": {
                                    "type": "trades",
                                    "coin": self.coin,
                                },
                            }
                        )
                    )

                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        try:
                            message = json.loads(raw)
                            self.process_message(message)
                        except Exception as exc:
                            logger.exception("failed to process websocket message")
                            self.state.last_error = f"message_processing: {exc}"

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = f"websocket: {type(exc).__name__}: {exc}"
                logger.warning("websocket disconnected: %s", self.state.last_error)
            finally:
                self.state.connected = False

            if self._stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def stop(self) -> None:
        self._stop.set()

    def snapshot(self) -> dict[str, Any]:
        payload = asdict(self.state)
        payload.update(
            {
                "coin": self.coin,
                "ws_url": self.ws_url,
                "stored_trades": self.store.count(),
                "stored_latest_trade_time_ms": self.store.latest_trade_time_ms(),
            }
        )
        return payload
