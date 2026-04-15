import logging
from decimal import Decimal, InvalidOperation
from typing import Optional

from db import insert_position

log = logging.getLogger(__name__)

# Category blocklist
EXCLUDED_CATEGORIES = {
    "sports", "basketball", "football", "soccer", "baseball",
    "hockey", "tennis", "golf", "mma", "boxing", "racing",
    "esports", "olympics", "nfl", "nba", "mlb", "nhl",
}

# Series prefix blocklist — includes multivariate/parlay series
EXCLUDED_SERIES_PREFIXES = (
    "NBA", "NFL", "MLB", "NHL", "EPL", "MLS", "NCAAB", "NCAAF",
    "GOLF", "PGA", "TENNIS", "ATP", "WTA", "UFC", "NASCAR",
    "F1", "SOCCER", "ESPORTS",
    # Kalshi multivariate parlay series — not standard binary markets
    "KXMVE", "KXMV",
)

# Market type blocklist — Kalshi multivariate markets
EXCLUDED_MARKET_TYPES = {"multivariate", "combo", "parlay"}


def is_excluded(market: dict) -> bool:
    category = (market.get("category") or "").lower()
    if category in EXCLUDED_CATEGORIES:
        return True

    market_type = (market.get("market_type") or "").lower()
    if any(t in market_type for t in EXCLUDED_MARKET_TYPES):
        return True

    series = (
        market.get("series_ticker") or
        market.get("event_ticker") or
        market.get("market_id") or ""
    )
    if any(series.upper().startswith(p) for p in EXCLUDED_SERIES_PREFIXES):
        return True

    return False


def _dec(v) -> Optional[Decimal]:
    try:
        return Decimal(str(v))
    except (InvalidOperation, TypeError):
        return None


def no_mid(market: dict) -> Optional[Decimal]:
    bid = _dec(market.get("no_bid"))
    ask = _dec(market.get("no_ask"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2
    return None


def no_spread(market: dict) -> Optional[Decimal]:
    bid = _dec(market.get("no_bid"))
    ask = _dec(market.get("no_ask"))
    if bid is not None and ask is not None:
        return ask - bid
    return None


def execute(conn, market: dict, strategy_name: str = "naive"):
    if is_excluded(market):
        log.debug("Skipping excluded market %s", market["market_id"])
        return

    mid = no_mid(market)
    if mid is None:
        log.warning("Skipping %s — no NO price data", market["market_id"])
        return

    spread = no_spread(market)
    snap = {
        "no_bid":    market.get("no_bid"),
        "no_ask":    market.get("no_ask"),
        "no_mid":    float(mid),
        "no_spread": float(spread) if spread is not None else None,
        "volume_24h": market.get("volume_24h"),
    }

    insert_position(conn, market["market_id"], snap, strategy=strategy_name)
    log.info("[%s] entry market=%s no_mid=%.4f spread=%s",
             strategy_name, market["market_id"], mid,
             f"{spread:.4f}" if spread is not None else "n/a")
