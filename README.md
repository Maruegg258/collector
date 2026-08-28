# HYPE Spot Collector

Production HYPE/USDC Spot demand collector for `HYPE_SWING_LONG_PROTOCOL.md`.

## Purpose

This service supplies the Protocol's **Hyperliquid official HYPE/USDC Spot Demand** layer. It does not replace Binance derivatives data, ETF flow, market-regime inputs, or the Monitor's final trading interpretation.

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

Known continuity-gap metadata is also retained **indefinitely by default**. A gap remains an engineering fact even when the Monitor later concludes that it does not materially change the medium-term direction.

## Data quality — Protocol v1.2.1

`/hype/spot-demand` deliberately separates three concepts:

1. **Source/history readiness** — whether official trades are currently available and the required completed-window history exists.
2. **Engineering continuity** — whether a window is `COMPLETE` or contains an `UNRESOLVED_GAP`.
3. **Decision Usability** — the Monitor's final `ROBUST`, `MARGINAL`, or `UNKNOWN` classification for each 4H/24H/3D window.

The Collector does **not** assign final Decision Usability. It provides the observed facts needed by the Monitor:

- 4H / 24H / 3D Spot notional delta
- base delta, total notional and delta ratio
- latest 18 completed 4H buckets and cumulative delta
- per-window coverage ratio as a diagnostic only
- unresolved gap count, total duration and maximum duration
- per-gap start/end/duration/reason/recovery diagnostics
- archive source (`materialized_4h`, `raw_fallback`, or `missing`)
- collector freshness and reconnect/recovery counters

### No fixed gap cliff

Protocol v1.2.1 removes the old v1.1 rule where 5 seconds / 10 seconds / 2 gaps automatically separated `MINOR_GAP` from `MATERIAL_GAP`.

Gap duration and frequency remain important evidence, but the Collector no longer converts them into an automatic trading verdict. A 4H gap therefore does not automatically invalidate 24H or 3D. The Monitor evaluates each window independently using:

- gap diagnostics
- observed turnover and delta margin
- Binance price / volatility context
- multi-window direction consistency
- material market/protocol event context

Missing trades are never filled, imputed or assumed to be zero.

`full_spot_mode_ready` is therefore a **source-level readiness** flag only. The Monitor must still finalize 4H/24H/3D Decision Usability and apply the Protocol's FULL / DEGRADED Spot Mode rules.

Gap recovery continues to use Hyperliquid official `recentTrades` with strict overlap proof and bounded retries. Unresolved gaps remain explicit.

## Reliability

- WebSocket ping/pong and exponential reconnect backoff
- PostgreSQL-backed deployment lease for zero-downtime Railway handoff
- PostgreSQL operation reconnect + one idempotent retry on connection loss
- `/readiness` requires WebSocket connected, database reachable and fresh messages
- GitHub Actions: unit tests, PostgreSQL storage contract, Hyperliquid live smoke

## Endpoints

- `/readiness` — Railway readiness gate
- `/health` — runtime diagnostics and Protocol compatibility version
- `/hype/spot-demand` — Protocol-facing Spot Demand snapshot
- `/storage/status` — compaction/archive/retention status

## Storage capacity

The stateless Collector cannot inspect the Postgres service volume directly. PostgreSQL disk capacity must be monitored through Railway metrics. `/storage/status` therefore reports `EXTERNAL_MONITOR_REQUIRED` rather than falsely labelling Postgres disk usage `NORMAL`.

Raw trades remain short-retention; completed 4H aggregates and gap metadata are compact and durable. This preserves Protocol history without storing every raw trade indefinitely.

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
