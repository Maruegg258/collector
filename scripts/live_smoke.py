from __future__ import annotations

import asyncio
import json

import websockets

WS_URL = "wss://api.hyperliquid.xyz/ws"
COIN = "@107"


async def main() -> None:
    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as ws:
        await ws.send(
            json.dumps(
                {
                    "method": "subscribe",
                    "subscription": {"type": "trades", "coin": COIN},
                }
            )
        )

        for _ in range(30):
            raw = await asyncio.wait_for(ws.recv(), timeout=15)
            message = json.loads(raw)
            if message.get("channel") != "trades":
                continue
            trades = message.get("data") or []
            if not trades:
                continue
            trade = trades[0]
            assert trade["coin"] == COIN
            assert trade["side"] in {"B", "A"}
            notional = float(trade["px"]) * float(trade["sz"])
            assert notional > 0
            assert int(trade["time"]) > 0
            assert int(trade["tid"]) > 0
            print(
                json.dumps(
                    {
                        "ok": True,
                        "coin": trade["coin"],
                        "side": trade["side"],
                        "px": trade["px"],
                        "sz": trade["sz"],
                        "notional_usdc": notional,
                        "time": trade["time"],
                        "tid": trade["tid"],
                    }
                )
            )
            return

    raise RuntimeError("connected but no @107 trade observed")


if __name__ == "__main__":
    asyncio.run(main())
