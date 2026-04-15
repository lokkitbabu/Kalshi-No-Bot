"""
Connectors for Polymarket (Gamma + CLOB) and Kalshi.

Polymarket Gamma response shape (key fields):
  id              — market ID
  question        — title
  clobTokenIds    — ["<yes_token_id>", "<no_token_id>"]
  outcomes        — ["Yes", "No"]
  outcomePrices   — ["0.65", "0.35"]  (parallel to outcomes; YES is [0], NO is [1])
  endDate         — close time
  active/closed   — status booleans

Kalshi public endpoint: api.elections.kalshi.com (no auth required)
Kalshi market response includes no_bid_dollars / no_ask_dollars directly.
Kalshi orderbook returns only bids; NO ask = 1.00 - best_yes_bid.
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
    Gamma API response uses parallel arrays:
      clobTokenIds[0] = YES token, clobTokenIds[1] = NO token
      outcomePrices[0] = YES price, outcomePrices[1] = NO price
    """
    offset = 0
    while True:
        data = await _get(session, f"{POLYMARKET_GAMMA_API}/markets", params={
            "active":  "true",
            "closed":  "false",
            "limit":   PAGE_SIZE,
            "offset":  offset,
        })
        if not data:
            break

        for m in data:
            token_ids     = m.get("clobTokenIds", [])
            outcome_prices = m.get("outcomePrices", [])

            # must be binary (exactly 2 tokens) with CLOB enabled
            if len(token_ids) != 2 or not m.get("enableOrderBook", True):
                continue

            no_token_id = token_ids[1]   # NO is always index 1
            no_price    = _f(outcome_prices[1]) if len(outcome_prices) > 1 else None

            if not no_token_id or no_price is None:
                continue

            yield {
                "market_id":   m["id"],
                "venue":       "polymarket",
                "title":       m.get("question", ""),
                "category":    m.get("category"),
                "close_time":  m.get("endDate"),
                "no_token_id": no_token_id,
                "condition_id": m.get("conditionId"),
                # gamma price is mid approximation until CLOB poll
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
    """Hits the CLOB order book for the NO token directly."""
    data = await _get(session, f"{POLYMARKET_CLOB_API}/book",
                      params={"token_id": no_token_id})
    bids = data.get("bids", [])
    asks = data.get("asks", [])
    best_bid = _f(bids[0]["price"]) if bids else None
    best_ask = _f(asks[0]["price"]) if asks else None
    return _book_snap(best_bid, best_ask, _f(data.get("volume")))


async def fetch_polymarket_resolution(
    session: aiohttp.ClientSession, market_id: str
) -> Optional[str]:
    data = await _get(session, f"{POLYMARKET_GAMMA_API}/markets/{market_id}")
    if not (data.get("closed") and data.get("resolutionTime")):
        return None
    # outcomePrices settle to "1" for winner, "0" for loser
    prices = data.get("outcomePrices", [])
    if len(prices) == 2:
        if _f(prices[1]) == 1.0:
            return "NO"
        if _f(prices[0]) == 1.0:
            return "YES"
    return None


# ── Kalshi ────────────────────────────────────────────────────────────────────

async def fetch_kalshi_markets(session: aiohttp.ClientSession) -> AsyncIterator[dict]:
    """
    Public endpoint — no auth required.
    Market summary includes no_bid_dollars / no_ask_dollars directly.
    """
    cursor = None
    while True:
        params = {"limit": PAGE_SIZE, "status": "open"}
        if cursor:
            params["cursor"] = cursor

        data    = await _get(session, f"{KALSHI_API}/markets", params=params)
        markets = data.get("markets", [])
        if not markets:
            break

        for m in markets:
            no_bid = _f(m.get("no_bid_dollars"))
            no_ask = _f(m.get("no_ask_dollars"))
            snap   = _book_snap(no_bid, no_ask, _f(m.get("volume_24h_fp")))

            if snap["no_mid"] is None:
                continue

            yield {
                "market_id":   m["ticker"],
                "venue":       "kalshi",
                "title":       m.get("title", ""),
                "category":    m.get("category"),
                "close_time":  m.get("close_time"),
                "no_token_id": m["ticker"],
                "condition_id": None,
                **snap,
            }

        cursor = data.get("cursor")
        if not cursor:
            break


async def fetch_kalshi_no_book(
    session: aiohttp.ClientSession, market_ticker: str
) -> dict:
    """
    Orderbook only returns bids.
    NO ask is implied: 1.00 - best_yes_bid
    Response: orderbook_fp.no_dollars = [[price_str, count_str], ...]
              best bid is the LAST element (sorted worst → best)
    """
    data = await _get(session, f"{KALSHI_API}/markets/{market_ticker}/orderbook")
    ob   = data.get("orderbook_fp", {})

    no_dollars  = ob.get("no_dollars", [])
    yes_dollars = ob.get("yes_dollars", [])

    best_no_bid  = _f(no_dollars[-1][0])  if no_dollars  else None
    best_yes_bid = _f(yes_dollars[-1][0]) if yes_dollars else None
    implied_no_ask = (1.0 - best_yes_bid) if best_yes_bid is not None else None

    return _book_snap(best_no_bid, implied_no_ask, None)


async def fetch_kalshi_resolution(
    session: aiohttp.ClientSession, market_ticker: str
) -> Optional[str]:
    data = await _get(session, f"{KALSHI_API}/markets/{market_ticker}")
    m = data.get("market", {})
    if m.get("status") == "finalized":
        result = m.get("result", "").upper()
        return result if result in ("YES", "NO") else None
    return None


# ── routing maps ──────────────────────────────────────────────────────────────

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
