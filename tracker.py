from typing import Optional
"""
Tracker: resolves open positions when markets settle.

PnL formula (normalized to $1 contract):
  outcome == NO  ->  pnl =  1 - entry_no_mid   (win)
  outcome == YES ->  pnl = -entry_no_mid         (loss)

exit_no_price is the settlement value: 1.0 if NO won, 0.0 if YES won.
"""

import logging
import sqlite3
from collections.abc import Awaitable, Callable

from db import get_open_positions, resolve_position, mark_market_resolved

log = logging.getLogger(__name__)


def compute_pnl(entry_no_mid: float, outcome: str) -> tuple[float, float]:
    """Returns (pnl, exit_no_price)."""
    if outcome == "NO":
        return (1.0 - entry_no_mid, 1.0)
    else:
        return (-entry_no_mid, 0.0)


async def check_resolutions(
    conn: sqlite3.Connection,
    fetch_resolution_fn: Callable[[str], Awaitable[Optional[str]]],
):
    """
    fetch_resolution_fn: async (market_id: str) -> 'YES' | 'NO' | None
    Groups open positions by market to avoid redundant API calls.
    """
    open_positions = get_open_positions(conn)
    if not open_positions:
        return

    market_to_positions: dict[str, list] = {}
    for pos in open_positions:
        market_to_positions.setdefault(pos["market_id"], []).append(pos)

    resolved_count = 0
    for market_id, positions in market_to_positions.items():
        try:
            outcome = await fetch_resolution_fn(market_id)
        except Exception as exc:
            log.warning("Resolution check failed for %s: %s", market_id, exc)
            continue

        if outcome is None:
            continue

        mark_market_resolved(conn, market_id, outcome)

        for pos in positions:
            pnl, exit_price = compute_pnl(pos["entry_no_mid"], outcome)
            resolve_position(conn, pos["id"], exit_price, pnl)
            log.info(
                "Resolved market=%s outcome=%s strategy=%s entry=%.4f pnl=%.4f",
                market_id, outcome, pos["strategy_name"],
                pos["entry_no_mid"], pnl,
            )
            resolved_count += 1

    if resolved_count:
        log.info("Resolved %d positions this cycle", resolved_count)
