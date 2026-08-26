# HYPE Spot Collector

Minimal collector for Hyperliquid official HYPE/USDC spot trades (`@107`).

## MVP scope

- Connect to `wss://api.hyperliquid.xyz/ws`
- Subscribe to `trades` for `@107`
- Treat `B` as aggressing buy and `A` as aggressing sell
- Compute USDC notional as `px * sz`
- Persist trades in SQLite
- Deduplicate by `(time_ms, coin, tid)`
- Expose `/health`
- Reconnect with exponential backoff

This MVP intentionally does **not** yet implement rolling 4H/24H/3D Delta/CVD, gap recovery, coverage scoring, or API authentication.

## Run locally

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Then open:

```text
http://localhost:8000/health
```

## Tests

```bash
pip install pytest
pytest -q
```

## Live smoke test

```bash
python scripts/live_smoke.py
```

The smoke test requires outbound DNS/WebSocket access to Hyperliquid.

## Railway (later stage)

The Dockerfile already honors Railway's `PORT` variable. For persistent SQLite storage, mount a Railway Volume at `/data`.
