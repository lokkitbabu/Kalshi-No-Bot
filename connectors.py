"""
Connectors for Polymarket and Kalshi.

Polymarket has two relevant APIs:
  - Gamma API  (gamma-api.polymarket.com)  — market metadata, token IDs
  - CLOB API   (clob.polymarket.com)       — live order books per token

NO is a first-class CLOB token with its own token_id. We store that ID at
ingest and poll it directly for bid/ask — never derive from YES.

Kalshi exposes no_bid/no_ask at the market level directly.
"""

import asyncio
import logging
from typing import AsyncIterator, Optional, Union

import aiohttp

from config import (
    POLYMARKET_GAMMA_API, POLYMARKET_CLOB_API,
    KALSHI_API, PAGE_SIZE, MAX_RETRIES, BACKOFF_BASE,
)

log = logging.getLogger(__name__)


# ── shared ────────────────────────────────────────────────────────────────────

async def _get(session: aiohttp.ClientSession, url: str, **kwargs) -> Union[dict, list]:
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, **kwargs) as resp:
                resp.raise_for_status()
                return await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = BACKOFF_BASE ** attempt
            log.warning("GET %s failed (%s), retry in %ds", url, exc, wait)
            await asyncio.sleep(wait)


def _f(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _book_snap(bid: Optional[float], ask: Optional[float], volume: Optional[float]) -> dict:
    mid    = ((bid + ask) / 2) if bid is not None and ask is not None else None
    spread = (ask - bid)       if bid is not None and ask is not None else None
    return {"no_bid": bid, "no_ask": ask, "no_mid": mid, "no_spread": spread, "volume_24h": volume}


# ── Polymarket ────────────────────────────────────────────────────────────────

async def fetch_polymarket_markets(session: aiohttp.ClientSession) -> AsyncIterator[dict]:
    """
    Yields normalized market dicts including no_token_id.
    Skips non-binary markets and markets with no NO token.
    """
    offset = 0
    while True:
        data = await _get(session, f"{POLYMARKET_GAMMA_API}/markets", params={
            "active": "true",
            "closed": "false",
            "limit": PAGE_SIZE,
            "offset": offset,
        })
        if not data:
            break

        for m in data:
            tokens = m.get("tokens", [])
            if len(tokens) != 2:
                continue

            no_tok = next((t for t in tokens if t.get("outcome", "").upper() == "NO"), None)
            if not no_tok or not no_tok.get("token_id"):
                continue

            # initial price comes from gamma — CLOB price update follows in price_loop
            no_price = _f(no_tok.get("price"))

            yield {
                "market_id":   m["id"],
                "venue":       "polymarket",
                "title":       m.get("question", ""),
                "category":    m.get("category"),
                "close_time":  m.get("endDate"),
                "no_token_id": no_tok["token_id"],
                "condition_id": m.get("conditionId"),
                # snapshot at ingest — spread unknown until CLOB poll
                "no_bid":      no_price,
                "no_ask":      no_price,
                "no_mid":      no_price,
                "no_spread":   None,
                "volume_24h":  _f(m.get("volume24hr")),
            }

        if len(data) < PAGE_SIZE:
            break
        offset += PAGE_SIZE


async def fetch_polymarket_no_book(
    session: aiohttp.ClientSession, no_token_id: str
) -> dict:
    """
    Hits the CLOB order book for the NO token directly.
    Returns a snap dict with no_bid, no_ask, no_mid, no_spread.
    """
    data = await _get(session, f"{POLYMARKET_CLOB_API}/book",
                      params={"token_id": no_token_id})

    bids = data.get("bids", [])   # [{price, size}, ...]
    asks = data.get("asks", [])

    best_bid = _f(bids[0]["price"]) if bids else None
    best_ask = _f(asks[0]["price"]) if asks else None

    # CLOB also returns last trade volume if needed
    volume = _f(data.get("volume"))

    return _book_snap(best_bid, best_ask, volume)


async def fetch_polymarket_resolution(
    session: aiohttp.ClientSession, market_id: str
) -> Optional[str]:
    data = await _get(session, f"{POLYMARKET_GAMMA_API}/markets/{market_id}")
    if not (data.get("closed") and data.get("resolutionTime")):
        return None
    tokens = data.get("tokens", [])
    for t in tokens:
        if _f(t.get("price")) == 1.0:
            return t.get("outcome", "").upper()
    return None


# ── Kalshi ────────────────────────────────────────────────────────────────────

async def fetch_kalshi_markets(session: aiohttp.ClientSession) -> AsyncIterator[dict]:
    cursor = None
    while True:
        params = {"limit": PAGE_SIZE, "status": "open"}
        if cursor:
            params["cursor"] = cursor

        data   = await _get(session, f"{KALSHI_API}/markets", params=params)
        markets = data.get("markets", [])
        if not markets:
            break

        for m in markets:
            no_bid  = _f(m.get("no_bid"))
            no_ask  = _f(m.get("no_ask"))
            snap    = _book_snap(no_bid, no_ask, _f(m.get("volume")))

            yield {
                "market_id":   m["ticker"],
                "venue":       "kalshi",
                "title":       m.get("title", ""),
                "category":    m.get("category"),
                "close_time":  m.get("close_time"),
                "no_token_id": m["ticker"],  # for Kalshi, ticker IS the poll key
                "condition_id": None,
                **snap,
            }

        cursor = data.get("cursor")
        if not cursor:
            break


async def fetch_kalshi_no_book(
    session: aiohttp.ClientSession, market_ticker: str
) -> dict:
    """Polls live NO book for a single Kalshi market."""
    data = await _get(session, f"{KALSHI_API}/markets/{market_ticker}/orderbook")
    book = data.get("orderbook", {})

    no_bids = book.get("no", [])   # [[price, size], ...]
    no_asks = book.get("yes", [])  # Kalshi: YES asks = NO bids from other side

    best_bid = _f(no_bids[0][0]) if no_bids else None
    best_ask = _f(no_asks[0][0]) if no_asks else None

    return _book_snap(best_bid, best_ask, None)


async def fetch_kalshi_resolution(
    session: aiohttp.ClientSession, market_ticker: str
) -> Optional[str]:
    data = await _get(session, f"{KALSHI_API}/markets/{market_ticker}")
    m = data.get("market", {})
    if m.get("status") == "finalized":
        result = m.get("result", "").upper()
        return result if result in ("YES", "NO") else None
    return None


# ── routing maps (used by main) ───────────────────────────────────────────────

MARKET_FETCHERS = {
    "polymarket": fetch_polymarket_markets,
    "kalshi":     fetch_kalshi_markets,
}

BOOK_FETCHERS = {
    "polymarket": fetch_polymarket_no_book,
    "kalshi":     fetch_kalshi_no_book,
}

RESOLVERS = {
    "polymarket": fetch_polymarket_resolution,
    "kalshi":     fetch_kalshi_resolution,
}
