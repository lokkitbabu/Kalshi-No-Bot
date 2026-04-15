"""
Strategy: paper-buy NO on every new non-sports market.

NO mid = (no_bid + no_ask) / 2
Uses Decimal throughout to avoid float precision loss on subpenny prices.
Skips markets with no price data at entry.
"""

import logging
from decimal import Decimal, InvalidOperation

from db import insert_position

log = logging.getLogger(__name__)

EXCLUDED_CATEGORIES = {
    "sports", "basketball", "football", "soccer", "baseball",
    "hockey", "tennis", "golf", "mma", "boxing", "racing",
    "esports", "olympics", "nfl", "nba", "mlb", "nhl",
}

EXCLUDED_SERIES_PREFIXES = (
    "NBA", "NFL", "MLB", "NHL", "EPL", "MLS", "NCAAB", "NCAAF",
    "GOLF", "PGA", "TENNIS", "ATP", "WTA", "UFC", "NASCAR",
    "F1", "SOCCER", "ESPORTS",
)


def is_sports(market: dict) -> bool:
    category = (market.get("category") or "").lower()
    if category in EXCLUDED_CATEGORIES:
        return True
    series = market.get("series_ticker") or market.get("event_ticker") or ""
    return any(series.startswith(p) for p in EXCLUDED_SERIES_PREFIXES)


def _dec(v) -> Decimal | None:
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError):
        return None


def no_mid(market: dict) -> Decimal | None:
    bid = _dec(market.get("no_bid"))
    ask = _dec(market.get("no_ask"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2
    return None


def no_spread(market: dict) -> Decimal | None:
    bid = _dec(market.get("no_bid"))
    ask = _dec(market.get("no_ask"))
    if bid is not None and ask is not None:
        return ask - bid
    return None


def execute(conn, market: dict, strategy_name: str = "naive"):
    if is_sports(market):
        log.debug("Skipping sports market %s", market["market_id"])
        return

    mid = no_mid(market)
    if mid is None:
        log.warning("Skipping %s — no NO price data", market["market_id"])
        return

    snap = {
        "no_bid":    market.get("no_bid"),
        "no_ask":    market.get("no_ask"),
        "no_mid":    float(mid),
        "no_spread": float(no_spread(market)) if no_spread(market) is not None else None,
        "volume_24h": market.get("volume_24h"),
    }

    insert_position(conn, market["market_id"], snap, strategy=strategy_name)
    log.info("[%s] entry market=%s no_mid=%.4f spread=%s",
             strategy_name, market["market_id"], mid,
             f"{no_spread(market):.4f}" if no_spread(market) else "n/a")
