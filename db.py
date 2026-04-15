import sqlite3
from config import DB_PATH


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS markets (
            market_id    TEXT PRIMARY KEY,
            venue        TEXT NOT NULL,
            title        TEXT,
            category     TEXT,
            close_time   TIMESTAMP,
            listed_at    TIMESTAMP NOT NULL,
            no_token_id  TEXT,   -- Polymarket: CLOB token_id for the NO outcome token
            condition_id TEXT,   -- Polymarket: condition ID
            resolved     INTEGER DEFAULT 0,
            outcome      TEXT,
            resolved_at  TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS positions (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            market_id      TEXT NOT NULL REFERENCES markets(market_id),
            strategy_name  TEXT NOT NULL DEFAULT 'naive',
            entry_no_bid   REAL,
            entry_no_ask   REAL,
            entry_no_mid   REAL NOT NULL,
            entry_spread   REAL,
            entry_volume   REAL,
            entry_time     TIMESTAMP NOT NULL,
            exit_no_price  REAL,
            exit_time      TIMESTAMP,
            resolved       INTEGER DEFAULT 0,
            pnl            REAL
        );

        CREATE TABLE IF NOT EXISTS prices (
            market_id  TEXT NOT NULL REFERENCES markets(market_id),
            ts         TIMESTAMP NOT NULL,
            no_bid     REAL,
            no_ask     REAL,
            no_mid     REAL,
            no_spread  REAL,
            volume_24h REAL,
            PRIMARY KEY (market_id, ts)
        );

        CREATE INDEX IF NOT EXISTS idx_positions_unresolved
            ON positions(resolved) WHERE resolved = 0;
        CREATE INDEX IF NOT EXISTS idx_markets_unresolved
            ON markets(resolved) WHERE resolved = 0;
        CREATE INDEX IF NOT EXISTS idx_markets_venue
            ON markets(venue);
    """)
    conn.commit()


def get_all_market_ids(conn, venue: str) -> set[str]:
    rows = conn.execute(
        "SELECT market_id FROM markets WHERE venue = ?", (venue,)
    ).fetchall()
    return {r["market_id"] for r in rows}


def get_open_market_tokens(conn, venue: str) -> list[sqlite3.Row]:
    """Returns open markets with their NO token identifiers for direct polling."""
    return conn.execute("""
        SELECT market_id, no_token_id, condition_id
        FROM markets
        WHERE venue = ? AND resolved = 0 AND no_token_id IS NOT NULL
    """, (venue,)).fetchall()


def insert_market(conn, m: dict):
    conn.execute("""
        INSERT OR IGNORE INTO markets
            (market_id, venue, title, category, close_time,
             no_token_id, condition_id, listed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
    """, (
        m["market_id"], m["venue"], m["title"], m.get("category"),
        m.get("close_time"), m.get("no_token_id"), m.get("condition_id"),
    ))
    conn.commit()


def insert_position(conn, market_id: str, snap: dict, strategy: str = "naive"):
    conn.execute("""
        INSERT INTO positions
            (market_id, strategy_name,
             entry_no_bid, entry_no_ask, entry_no_mid,
             entry_spread, entry_volume, entry_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
    """, (
        market_id, strategy,
        snap.get("no_bid"), snap.get("no_ask"), snap["no_mid"],
        snap.get("no_spread"), snap.get("volume_24h"),
    ))
    conn.commit()


def insert_price(conn, market_id: str, snap: dict):
    conn.execute("""
        INSERT OR REPLACE INTO prices
            (market_id, ts, no_bid, no_ask, no_mid, no_spread, volume_24h)
        VALUES (?, datetime('now'), ?, ?, ?, ?, ?)
    """, (
        market_id,
        snap.get("no_bid"), snap.get("no_ask"), snap.get("no_mid"),
        snap.get("no_spread"), snap.get("volume_24h"),
    ))
    conn.commit()


def get_open_positions(conn) -> list[sqlite3.Row]:
    return conn.execute("""
        SELECT p.*, m.venue, m.outcome, m.no_token_id
        FROM positions p
        JOIN markets m ON p.market_id = m.market_id
        WHERE p.resolved = 0
    """).fetchall()


def resolve_position(conn, position_id: int, exit_no_price: float, pnl: float):
    conn.execute("""
        UPDATE positions
        SET resolved = 1, exit_no_price = ?, exit_time = datetime('now'), pnl = ?
        WHERE id = ?
    """, (exit_no_price, pnl, position_id))
    conn.commit()


def mark_market_resolved(conn, market_id: str, outcome: str):
    conn.execute("""
        UPDATE markets
        SET resolved = 1, outcome = ?, resolved_at = datetime('now')
        WHERE market_id = ?
    """, (outcome, market_id))
    conn.commit()
