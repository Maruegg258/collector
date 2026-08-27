# HYPE Spot Collector

Production HYPE/USDC Spot demand collector for `HYPE_SWING_LONG_PROTOCOL.md`.

## Purpose

This service supplies the Protocol's **Hyperliquid official HYPE/USDC Spot Demand** layer. It does not replace Binance derivatives data, ETF flow, or market-regime inputs.

## Production data path

- Source: `wss://api.hyperliquid.xyz/ws`
- Subscription: `trades` for mainnet HYPE Spot `@107`
- Official aggressor side: `B` = buy, `A` = sell
- USDC notional: `px * sz`
- Primary delta: aggressive-buy notional minus aggressive-sell notional
- Deduplication key: `(time_ms, coin, tid)`
- Durable backend: PostgreSQL on Railway
- Collector service itself is stateless

## Protocol-facing windows

The monitor evaluates completed Binance-aligned UTC 4H boundaries.

- `4h`: latest completed 4H bucket
- `24h`: rolling sum of the latest 6 completed 4H buckets
- `3d`: rolling sum of the latest 18 completed 4H buckets
- `recent_4h`: latest 18 completed buckets with cumulative delta for CVD direction/divergence analysis

Raw trades are retained for **12 hours by default** for current-bucket calculation, reconnect recovery and diagnostics. Completed 4H aggregates are retained indefinitely, so 24H/3D Protocol history does not depend on retaining every raw trade for days.

## Data quality

`/hype/spot-demand` reports:

- `data_quality`: `FULL`, `WARMING_UP`, or `DEGRADED`
- `full_spot_mode_ready`
- per-window coverage and unresolved gap counts
- archive source (`materialized_4h`, `raw_fallback`, or `missing`)
- collector freshness and reconnect/recovery counters

A missing or unprovable interval is never guessed. Gap recovery uses Hyperliquid official `recentTrades` with strict overlap proof and bounded retries. Unresolved gaps remain explicit.

## Reliability

- WebSocket ping/pong and exponential reconnect backoff
- PostgreSQL-backed deployment lease for zero-downtime Railway handoff
- PostgreSQL operation reconnect + one idempotent retry on connection loss
- `/readiness` requires WebSocket connected, database reachable and fresh messages
- GitHub Actions: unit tests, PostgreSQL storage contract, Hyperliquid live smoke

## Endpoints

- `/readiness` — Railway readiness gate
- `/health` — runtime diagnostics
- `/hype/spot-demand` — Protocol-facing Spot Demand snapshot
- `/storage/status` — compaction/archive status

## Storage capacity

The stateless Collector cannot inspect the Postgres service volume directly. PostgreSQL disk capacity must be monitored through Railway metrics. `/storage/status` therefore reports `EXTERNAL_MONITOR_REQUIRED` rather than falsely labelling Postgres disk usage `NORMAL`.

## Legacy migration utilities

SQLite migration/mirroring/handoff-heal modules remain in the repository only as offline migration/history tooling. They are no longer part of the production runtime path in `app.main`.

## Local run

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

SQLite remains available as the default local backend. Railway production sets `STORAGE_BACKEND=postgres` and `DATABASE_URL`.

## Tests

```bash
pip install -r requirements.txt pytest
pytest -q
```

## Live smoke test

```bash
python scripts/live_smoke.py
```
