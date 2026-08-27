from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any

import websockets

from .leases import CollectorLeaseCoordinator
from .storage import TradeRecord
from .storage_protocol import StorageBackend

logger = logging.getLogger(__name__)
INFO_URL = "https://api.hyperliquid.xyz/info"


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
    recovery_attempts: int = 0
    recovery_successes: int = 0
    recovery_failures: int = 0
    recovered_trades_inserted: int = 0
    last_gap_status: str | None = None


class HypeSpotCollector:
    def __init__(self, store: StorageBackend, *, coin: str = "@107", ws_url: str = "wss://api.hyperliquid.xyz/ws", lease: CollectorLeaseCoordinator | None = None) -> None:
        self.store = store
        self.coin = coin
        self.ws_url = ws_url
        self.lease = lease
        self.state = CollectorState(started_at_ms=int(time.time() * 1000))
        self._stop = asyncio.Event()
        self._pending_gap_start_ms = None if lease is not None else store.get_meta_int("coverage_heartbeat_ms")
        self._last_heartbeat_persist_ms = 0

    @staticmethod
    def parse_trade(raw: dict[str, Any]) -> TradeRecord:
        side = str(raw["side"])
        if side not in {"B", "A"}:
            raise ValueError(f"unsupported trade side: {side!r}")
        px = float(raw["px"])
        sz = float(raw["sz"])
        notional = px * sz
        return TradeRecord(
            coin=str(raw["coin"]), time_ms=int(raw["time"]), tid=int(raw["tid"]), side=side,
            px=px, sz=sz, notional_usdc=notional,
            signed_notional_usdc=notional if side == "B" else -notional,
            trade_hash=raw.get("hash"),
        )

    def _persist_heartbeat(self, now_ms: int, *, force: bool = False) -> None:
        if self.lease is not None:
            if force or now_ms - self._last_heartbeat_persist_ms >= 5_000:
                self.lease.update(connected=self.state.connected, heartbeat_ms=now_ms, last_message_at_ms=self.state.last_message_at_ms)
                self._last_heartbeat_persist_ms = now_ms
            return
        if force or now_ms - self._last_heartbeat_persist_ms >= 5_000:
            self.store.set_meta("coverage_heartbeat_ms", now_ms)
            self._last_heartbeat_persist_ms = now_ms

    async def _lease_heartbeat_loop(self) -> None:
        if self.lease is None:
            return
        while self.state.connected and not self._stop.is_set():
            await asyncio.sleep(5)
            if self.state.connected and not self._stop.is_set():
                try:
                    self._persist_heartbeat(int(time.time() * 1000), force=True)
                except Exception:
                    logger.exception("collector_lease_heartbeat_failed")

    def _prepare_lease_handoff(self, now_ms: int) -> None:
        if self.lease is None:
            return
        if self.lease.active_other_exists(now_ms=now_ms):
            self._pending_gap_start_ms = None
            logger.info("continuity_handoff covered_by_active_peer instance=%s", self.lease.instance_id)
        else:
            self._pending_gap_start_ms = self.lease.latest_other_heartbeat_ms()
        self.lease.update(connected=True, heartbeat_ms=now_ms, last_message_at_ms=self.state.last_message_at_ms)
        self._last_heartbeat_persist_ms = now_ms

    def _mark_unexpected_disconnect(self) -> int | None:
        if self.lease is not None:
            now_ms = int(time.time() * 1000)
            if self.lease.active_other_exists(now_ms=now_ms):
                self._pending_gap_start_ms = None
                self.lease.update(connected=False, heartbeat_ms=now_ms, last_message_at_ms=self.state.last_message_at_ms)
                logger.info("continuity_disconnect covered_by_active_peer instance=%s", self.lease.instance_id)
                return None
            own = self.lease.snapshot()
            gap_start_ms = int(own.get("own_heartbeat_ms") or self.state.last_message_at_ms or self.state.connected_at_ms or now_ms)
            self._pending_gap_start_ms = gap_start_ms
            self.lease.update(connected=False, heartbeat_ms=gap_start_ms, last_message_at_ms=self.state.last_message_at_ms)
            return gap_start_ms
        gap_start_ms = self.state.last_message_at_ms or self.state.connected_at_ms or int(time.time() * 1000)
        self._pending_gap_start_ms = int(gap_start_ms)
        self.store.set_meta("coverage_heartbeat_ms", int(gap_start_ms))
        self._last_heartbeat_persist_ms = int(gap_start_ms)
        return int(gap_start_ms)

    def process_message(self, message: dict[str, Any]) -> tuple[int, int]:
        now_ms = int(time.time() * 1000)
        self.state.messages_seen += 1
        self.state.last_message_at_ms = now_ms
        self._persist_heartbeat(now_ms)
        if message.get("channel") != "trades":
            return 0, 0
        records: list[TradeRecord] = []
        for raw in message.get("data") or []:
            if raw.get("coin") != self.coin:
                continue
            record = self.parse_trade(raw)
            records.append(record)
            self.state.trades_seen += 1
            if self.state.last_trade_time_ms is None or record.time_ms > self.state.last_trade_time_ms:
                self.state.last_trade_time_ms = record.time_ms
        inserted, duplicates = self.store.insert_many(records)
        self.state.trades_inserted += inserted
        self.state.duplicates_ignored += duplicates
        return inserted, duplicates

    def _fetch_recent_sync(self) -> list[dict[str, Any]]:
        body = json.dumps({"type": "recentTrades", "coin": self.coin}).encode()
        req = urllib.request.Request(INFO_URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status != 200:
                raise RuntimeError(f"recentTrades HTTP {response.status}")
            payload = json.loads(response.read().decode())
        if not isinstance(payload, list):
            raise RuntimeError("recentTrades response is not a list")
        return payload

    async def _recover_gap(self, gap_start_ms: int, gap_end_ms: int) -> None:
        if gap_end_ms <= gap_start_ms:
            self.state.last_gap_status = "NO_GAP"
            return

        last_reason = "recentTrades_insufficient_overlap"
        last_earliest: int | None = None
        last_latest: int | None = None
        last_count = 0
        retry_delays = (0, 2, 5)

        for index, delay in enumerate(retry_delays):
            if delay:
                await asyncio.sleep(delay)
            self.state.recovery_attempts += 1
            try:
                raw_trades = await asyncio.to_thread(self._fetch_recent_sync)
                records = [self.parse_trade(raw) for raw in raw_trades if raw.get("coin") == self.coin]
                times = [record.time_ms for record in records]
                earliest = min(times) if times else None
                latest = max(times) if times else None
                inserted, duplicates = self.store.insert_many(records)
                self.state.recovered_trades_inserted += inserted
                self.state.duplicates_ignored += duplicates
                last_earliest, last_latest, last_count = earliest, latest, len(records)
                healed = earliest is not None and latest is not None and earliest <= gap_start_ms and latest >= gap_end_ms
                if healed:
                    self.store.add_gap(
                        self.coin, gap_start_ms, gap_end_ms, status="HEALED",
                        reason="recentTrades_overlap_proof", recovery_earliest_ms=earliest,
                        recovery_latest_ms=latest, recovered_trade_count=len(records),
                    )
                    self.state.last_gap_status = "HEALED"
                    self.state.recovery_successes += 1
                    logger.info("gap_recovery status=HEALED start_ms=%s end_ms=%s attempt=%s recent=%s inserted=%s", gap_start_ms, gap_end_ms, index + 1, len(records), inserted)
                    return
                last_reason = "recentTrades_insufficient_overlap"
            except Exception as exc:
                last_reason = f"recovery_error:{type(exc).__name__}"
                logger.warning("gap_recovery attempt=%s/%s failed: %s", index + 1, len(retry_delays), exc)

        self.store.add_gap(
            self.coin, gap_start_ms, gap_end_ms, status="UNRESOLVED", reason=last_reason,
            recovery_earliest_ms=last_earliest, recovery_latest_ms=last_latest,
            recovered_trade_count=last_count,
        )
        self.state.recovery_failures += 1
        self.state.last_gap_status = "UNRESOLVED"
        logger.warning("gap_recovery status=UNRESOLVED start_ms=%s end_ms=%s attempts=%s reason=%s", gap_start_ms, gap_end_ms, len(retry_delays), last_reason)

    async def run(self) -> None:
        backoff = 1.0
        first_attempt = True
        while not self._stop.is_set():
            lease_task: asyncio.Task | None = None
            try:
                if not first_attempt:
                    self.state.reconnects += 1
                first_attempt = False
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20, close_timeout=10, max_queue=10000) as ws:
                    self.state.connected = True
                    self.state.connected_at_ms = int(time.time() * 1000)
                    self.state.last_error = None
                    backoff = 1.0
                    if self.lease is not None:
                        self._prepare_lease_handoff(self.state.connected_at_ms)
                        lease_task = asyncio.create_task(self._lease_heartbeat_loop())
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": self.coin}}))
                    first_trade_batch = True
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        try:
                            message = json.loads(raw)
                            self.process_message(message)
                            if first_trade_batch and message.get("channel") == "trades" and (message.get("data") or []):
                                first_trade_batch = False
                                gap_start_ms = self._pending_gap_start_ms
                                if gap_start_ms is not None:
                                    batch_times = [int(item["time"]) for item in message["data"] if item.get("coin") == self.coin and "time" in item]
                                    if batch_times:
                                        await self._recover_gap(gap_start_ms, max(batch_times))
                                        self._pending_gap_start_ms = None
                        except Exception as exc:
                            logger.exception("failed to process websocket message")
                            self.state.last_error = f"message_processing: {exc}"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.state.last_error = f"websocket: {type(exc).__name__}: {exc}"
                logger.warning("websocket disconnected: %s", self.state.last_error)
            finally:
                if lease_task:
                    lease_task.cancel()
                    try:
                        await lease_task
                    except asyncio.CancelledError:
                        pass
                if self.state.connected:
                    if self._stop.is_set():
                        self._persist_heartbeat(int(time.time() * 1000), force=True)
                    else:
                        gap_start_ms = self._mark_unexpected_disconnect()
                        if gap_start_ms is not None:
                            logger.warning("continuity_gap_opened start_ms=%s basis=last_healthy_lease_or_message", gap_start_ms)
                self.state.connected = False
            if self._stop.is_set():
                break
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def stop(self) -> None:
        now_ms = int(time.time() * 1000)
        if self.lease is not None:
            try:
                self.lease.update(connected=False, heartbeat_ms=now_ms, last_message_at_ms=self.state.last_message_at_ms, stopped=True)
            except Exception:
                logger.exception("collector_lease_stop_failed")
        else:
            self._persist_heartbeat(now_ms, force=True)
        self._stop.set()

    def snapshot(self) -> dict[str, Any]:
        payload = asdict(self.state)
        payload.update({
            "coin": self.coin,
            "ws_url": self.ws_url,
            "stored_trades": self.store.count(self.coin),
            "stored_latest_trade_time_ms": self.store.latest_trade_time_ms(self.coin),
            "unresolved_gaps": self.store.unresolved_gap_count(self.coin),
            "continuity_mode": "instance_lease" if self.lease is not None else "single_instance_heartbeat",
        })
        if self.lease is not None:
            payload["lease"] = self.lease.snapshot()
        return payload
