# Kalshi NO Bot

Paper-trades NO on every new Kalshi listing (excluding sports). Tracks PnL and outcomes to test whether newly listed markets are systematically overpriced on YES.

## How it works

Three concurrent async loops:

- **ingest_loop** — paginates Kalshi's market API every 60s, detects new markets, logs paper NO entries
- **price_loop** — polls the NO order book for every open market every 30s using the batch orderbook endpoint
- **resolve_loop** — checks open positions for settlement every 5 minutes, computes PnL on resolution

Sports markets are excluded via category and series ticker filters.

## Stack

- Python (asyncio + aiohttp)
- SQLite (WAL mode)
- Streamlit dashboard

## Setup

```bash
pip install -r requirements.txt
```

No API key required — all market data endpoints used are public.

## Run

```bash
# Start the bot
python main.py

# Start the dashboard (separate terminal)
streamlit run dashboard.py
```

## Schema

Three tables: `markets`, `positions`, `prices`.

Positions store NO bid/ask/mid at entry, spread, and 24h volume — so you can later filter by liquidity and see where edge actually concentrates.

## What you're testing

Whether buying NO on every new market beats:
- tail losses from YES-resolving markets
- spread costs

The dashboard's **PnL by entry price bucket** view is where the signal lives.
