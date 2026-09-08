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

## Monitor acquisition contract

`/hype/spot-demand` is the canonical read-only Monitor interface.

The response includes:

- `schema_version = HYPE-SPOT-PAYLOAD-v1`
- `collector_interface_version` (production currently `1.2.3`)
- `completed_4h_end_ms`
- `payload_generated_at_ms`
- `query_mode`
- `boundary_match`
- the existing 4H / 24H / 3D Delta, ratio, CVD and continuity diagnostics

The Monitor should first check `/readiness`, then request `/hype/spot-demand`, and verify the returned completed-4H boundary before using the payload in a Protocol decision.

For deterministic audit/replay, a completed historical boundary may be requested with:

`/hype/spot-demand?completed_4h_end_ms=<UTC_4H_BOUNDARY_MS>`

The requested value must be a completed Binance-aligned UTC 4H boundary. The service reconstructs the response from the existing raw/aggregate storage path; it does **not** create a per-run payload snapshot table.

`payload_persistence = read_only_computed_no_snapshot_storage` makes this contract explicit. This preserves the original lifecycle design: raw trades remain short-retention, completed 4H aggregates remain compact and durable, and API hardening does not introduce unbounded duplicate storage.

### ChatGPT / automation transport fallback — Protocol v1.3.1

The canonical HTTP response body remains the first-choice transport. Some ChatGPT execution environments can issue the HTTP request but cannot surface an arbitrary Railway JSON response body to the Monitor. Protocol v1.3.1 therefore adds transport-only fallbacks without creating a second Spot engine.

**Primary ChatGPT-compatible fallback:** GitHub Actions workflow `HYPE Monitor Payload Proxy`.

- Transport marker: `HYPE-PAYLOAD-PROXY-v1`
- Job-log prefix: `HYPE_PAYLOAD_PROXY_V1`
- Schedule: UTC completed-4H boundary +2 minutes and +7 minutes; `workflow_dispatch` is also available for diagnostics
- The workflow calls the same production `/readiness` and exact-boundary `/hype/spot-demand` endpoint
- It validates schema, official `@107` source, requested/returned boundary and `boundary_match=true`
- It prints only the compact Monitor-required facts to the Actions job log
- It does **not** recalculate Delta/CVD, assign ROBUST/MARGINAL/UNKNOWN, store raw trades, or become a second database

**Secondary observability fallback:** Collector structured application log `HYPE-MONITOR-LOG-v1`.

`app.main` mirrors the already-built canonical payload as a single-line `monitor_payload` for completed boundaries and API requests. This path is useful only when the Railway connector can surface application stdout; HTTP 200 or `collector_summary` alone is never enough to claim payload validation.

Formal acquisition priority:

`canonical HTTP body -> exact-boundary HYPE-PAYLOAD-PROXY-v1 job log -> HYPE-MONITOR-LOG-v1 if surfaced -> explicit acquisition downgrade`

Both fallback transports are **single-producer mirrors**. `_spot_demand_payload()` remains the only canonical payload producer.

For 24H/3D aggregate windows, `archive_source` may be absent at the aggregate object level. Durable provenance is validated with `history_ready=true` plus the underlying `recent_4h` bucket `archive_source` values; a null aggregate display field is not automatically equivalent to archive data being missing.

## Data quality — Protocol v1.2.1+

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
- archive source (`materialized_4h`, `raw_fallback`, or `missing`) at completed 4H bucket level
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
- GitHub Actions: unit tests, PostgreSQL storage contract, Hyperliquid live smoke, exact-boundary Monitor payload transport proxy

## Endpoints

- `/readiness` — Railway readiness gate
- `/health` — runtime diagnostics, interface version and payload schema
- `/hype/spot-demand` — Protocol-facing Spot Demand snapshot; optional historical completed-4H boundary query
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
