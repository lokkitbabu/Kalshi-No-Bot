"""
Three concurrent loops:

  ingest_loop  — polls for new markets, logs entry positions
  price_loop   — polls NO order books for all open markets, logs snapshots
  resolve_loop — checks open positions for resolution

price_loop uses the NO token_id / ticker stored at ingest to hit each
venue's order book directly. No YES data needed anywhere.
"""

import asyncio
import logging

import aiohttp

import db
import strategy
import tracker
from connectors import MARKET_FETCHERS, BOOK_FETCHERS, RESOLVERS
from config import POLL_INTERVAL_SECONDS, PRICE_POLL_SECONDS, RESOLVE_CHECK_SECONDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("main")


async def ingest_loop(conn, session: aiohttp.ClientSession):
    while True:
        for venue, fetcher in MARKET_FETCHERS.items():
            seen = db.get_all_market_ids(conn, venue)
            new_count = 0

            async for market in fetcher(session):
                mid = market.get("no_mid")
                if mid is None:
                    continue  # no price data yet — skip, will catch on next poll

                if market["market_id"] not in seen:
                    db.insert_market(conn, market)
                    db.insert_price(conn, market["market_id"], market)
                    strategy.execute(conn, market)
                    seen.add(market["market_id"])
                    new_count += 1

            log.info("[ingest] %s: %d new markets", venue, new_count)

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def price_loop(conn, session: aiohttp.ClientSession):
    """
    Polls the NO order book for every open market using the stored token ID.
    This is the authoritative price source — ingest prices are just for entry logging.
    """
    while True:
        for venue, book_fetcher in BOOK_FETCHERS.items():
            markets = db.get_open_market_tokens(conn, venue)

            # batch with small concurrency limit to avoid hammering APIs
            sem = asyncio.Semaphore(10)

            async def poll_one(row):
                async with sem:
                    try:
                        snap = await book_fetcher(session, row["no_token_id"])
                        if snap["no_mid"] is not None:
                            db.insert_price(conn, row["market_id"], snap)
                    except Exception as exc:
                        log.warning("Price poll failed %s: %s", row["market_id"], exc)

            await asyncio.gather(*[poll_one(m) for m in markets])
            log.debug("[price] %s: polled %d markets", venue, len(markets))

        await asyncio.sleep(PRICE_POLL_SECONDS)


async def resolve_loop(conn, session: aiohttp.ClientSession):
    while True:
        async def resolve_fn(market_id: str) -> str | None:
            row = conn.execute(
                "SELECT venue FROM markets WHERE market_id = ?", (market_id,)
            ).fetchone()
            if row is None:
                return None
            resolver = RESOLVERS.get(row["venue"])
            return await resolver(session, market_id) if resolver else None

        await tracker.check_resolutions(conn, resolve_fn)
        await asyncio.sleep(RESOLVE_CHECK_SECONDS)


async def main():
    conn = db.get_conn()
    db.init_db(conn)

    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await asyncio.gather(
            ingest_loop(conn, session),
            price_loop(conn, session),
            resolve_loop(conn, session),
        )


if __name__ == "__main__":
    asyncio.run(main())
