import sqlite3
import time

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from config import DB_PATH
from db import init_db

st.set_page_config(page_title="NO Bot", layout="wide", page_icon="📉")

# auto-refresh every 15 seconds
st_autorefresh(interval=15_000, key="autorefresh")


@st.cache_resource
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    init_db(conn)
    return conn


conn = get_conn()


# ── data loaders ─────────────────────────────────────────────────────────────

def load_positions() -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT
            p.id, p.market_id, m.title, m.venue, m.category,
            p.entry_no_mid, p.entry_no_bid, p.entry_no_ask,
            p.entry_spread, p.entry_volume, p.entry_time,
            p.exit_no_price, p.exit_time,
            p.resolved, p.pnl, m.outcome
        FROM positions p
        JOIN markets m ON p.market_id = m.market_id
        ORDER BY p.entry_time DESC
    """, conn, parse_dates=["entry_time", "exit_time"])


def load_latest_prices() -> pd.DataFrame:
    """Most recent NO price snapshot per open market."""
    return pd.read_sql_query("""
        SELECT p.market_id, m.title, m.category, m.venue,
               p.no_mid, p.no_bid, p.no_ask, p.no_spread, p.volume_24h, p.ts
        FROM prices p
        JOIN markets m ON p.market_id = m.market_id
        WHERE p.ts = (
            SELECT MAX(ts) FROM prices p2 WHERE p2.market_id = p.market_id
        )
        AND m.resolved = 0
        ORDER BY p.ts DESC
    """, conn, parse_dates=["ts"])


def load_price_history(market_id: str) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT ts, no_bid, no_mid, no_ask
        FROM prices WHERE market_id = ?
        ORDER BY ts
    """, conn, params=(market_id,), parse_dates=["ts"])


def load_recent_markets(n=20) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT market_id, venue, title, category, listed_at
        FROM markets ORDER BY listed_at DESC LIMIT ?
    """, conn, params=(n,), parse_dates=["listed_at"])


# ── load data ─────────────────────────────────────────────────────────────────

df       = load_positions()
prices   = load_latest_prices()
recent   = load_recent_markets()

if df.empty:
    st.title("📉 NO Bot")
    st.info("Waiting for first ingest cycle… check `bot.log` for progress.")
    st.stop()

resolved = df[df["resolved"] == 1].copy()
open_pos = df[df["resolved"] == 0].copy()

# ── header ────────────────────────────────────────────────────────────────────

st.title("📉 NO Bot")
st.caption(f"Last updated: {time.strftime('%H:%M:%S')}  ·  auto-refreshes every 15s")

# ── top metrics ───────────────────────────────────────────────────────────────

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Open positions",   len(open_pos))
c2.metric("Resolved",         len(resolved))
c3.metric("Total markets",    len(df))
c4.metric("Venues tracked",   df["venue"].nunique() if "venue" in df.columns else "—")

if len(resolved):
    total_pnl = resolved["pnl"].sum()
    win_rate  = (resolved["pnl"] > 0).mean()
    c5.metric("Total PnL",  f"${total_pnl:.2f}", delta=f"{total_pnl:+.2f}")
    c6.metric("Win rate",   f"{win_rate:.1%}")
else:
    c5.metric("Total PnL",  "—")
    c6.metric("Win rate",   "—")

st.divider()

# ── tabs ──────────────────────────────────────────────────────────────────────

tab_live, tab_resolved, tab_analysis, tab_markets = st.tabs([
    "🟢 Live positions", "✅ Resolved", "📊 Analysis", "🆕 Recent markets"
])


# ── LIVE POSITIONS ────────────────────────────────────────────────────────────
with tab_live:
    if open_pos.empty:
        st.info("No open positions yet.")
    else:
        # join with latest prices for mark-to-market
        if not prices.empty:
            mtm = open_pos.merge(
                prices[["market_id", "no_mid", "no_spread", "ts"]].rename(
                    columns={"no_mid": "current_no_mid", "no_spread": "current_spread", "ts": "last_price_ts"}
                ),
                on="market_id", how="left"
            )
            mtm["unrealized_pnl"] = mtm["current_no_mid"] - mtm["entry_no_mid"]
            mtm["price_change"]   = mtm["current_no_mid"] - mtm["entry_no_mid"]
        else:
            mtm = open_pos.copy()
            mtm["current_no_mid"] = None
            mtm["unrealized_pnl"] = None

        st.caption(f"{len(open_pos)} open positions")

        show_cols = ["title", "venue", "category", "entry_no_mid",
                     "current_no_mid", "unrealized_pnl", "entry_spread", "entry_time"]
        show_cols = [c for c in show_cols if c in mtm.columns]

        st.dataframe(
            mtm[show_cols].sort_values("unrealized_pnl" if "unrealized_pnl" in mtm.columns else "entry_time"),
            use_container_width=True,
            column_config={
                "entry_no_mid":    st.column_config.NumberColumn("Entry NO", format="$%.3f"),
                "current_no_mid":  st.column_config.NumberColumn("Current NO", format="$%.3f"),
                "unrealized_pnl":  st.column_config.NumberColumn("Unrealized PnL", format="$%.3f"),
                "entry_spread":    st.column_config.NumberColumn("Spread", format="$%.3f"),
                "entry_time":      st.column_config.DatetimeColumn("Entered", format="MMM D, HH:mm"),
            },
            hide_index=True,
        )

        # price chart for selected market
        st.subheader("Price history")
        titles = mtm[["market_id", "title"]].drop_duplicates()
        label_map = dict(zip(titles["title"], titles["market_id"]))
        selected_title = st.selectbox("Select market", list(label_map.keys()), key="live_sel")
        if selected_title:
            px = load_price_history(label_map[selected_title])
            if not px.empty:
                st.line_chart(px.set_index("ts")[["no_bid", "no_mid", "no_ask"]])
            else:
                st.caption("No price history yet for this market.")


# ── RESOLVED ──────────────────────────────────────────────────────────────────
with tab_resolved:
    if resolved.empty:
        st.info("No resolved positions yet — check back once markets start settling.")
    else:
        r1, r2, r3 = st.columns(3)
        r1.metric("Wins",   int((resolved["pnl"] > 0).sum()))
        r2.metric("Losses", int((resolved["pnl"] < 0).sum()))
        r3.metric("Avg PnL per trade", f"${resolved['pnl'].mean():.3f}")

        # equity curve
        st.subheader("Equity curve")
        curve = resolved.sort_values("exit_time").copy()
        curve["cum_pnl"] = curve["pnl"].cumsum()
        st.line_chart(curve.set_index("exit_time")["cum_pnl"])

        # resolved table
        st.subheader("All resolved positions")
        resolved["hold_days"] = (resolved["exit_time"] - resolved["entry_time"]).dt.days
        st.dataframe(
            resolved[["title", "venue", "category", "entry_no_mid",
                       "exit_no_price", "pnl", "outcome", "hold_days", "exit_time"]]
            .sort_values("pnl"),
            use_container_width=True,
            column_config={
                "entry_no_mid":  st.column_config.NumberColumn("Entry NO", format="$%.3f"),
                "exit_no_price": st.column_config.NumberColumn("Exit NO",  format="$%.3f"),
                "pnl":           st.column_config.NumberColumn("PnL",      format="$%.3f"),
                "hold_days":     st.column_config.NumberColumn("Days held"),
                "exit_time":     st.column_config.DatetimeColumn("Resolved", format="MMM D HH:mm"),
            },
            hide_index=True,
        )


# ── ANALYSIS ──────────────────────────────────────────────────────────────────
with tab_analysis:
    if resolved.empty:
        st.info("Analysis available once positions start resolving.")
    else:
        resolved["hold_days"] = (resolved["exit_time"] - resolved["entry_time"]).dt.days

        col_a, col_b = st.columns(2)

        with col_a:
            st.subheader("PnL by entry price bucket")
            resolved["bucket"] = pd.cut(
                resolved["entry_no_mid"],
                bins=[0, 0.5, 0.7, 0.85, 0.92, 0.97, 1.0],
                labels=["<50¢", "50–70¢", "70–85¢", "85–92¢", "92–97¢", ">97¢"],
            )
            bucket = resolved.groupby("bucket", observed=True)["pnl"].agg(
                avg_pnl="mean", total_pnl="sum", count="count"
            )
            st.bar_chart(bucket["avg_pnl"])
            st.dataframe(bucket, use_container_width=True)

        with col_b:
            st.subheader("PnL by category")
            cat = resolved.groupby("category")["pnl"].agg(
                avg_pnl="mean", total_pnl="sum", count="count"
            ).sort_values("total_pnl")
            st.bar_chart(cat["total_pnl"])
            st.dataframe(cat, use_container_width=True)

        st.subheader("Time to resolution")
        hold_dist = resolved["hold_days"].value_counts().sort_index().rename("count")
        st.bar_chart(hold_dist)

        st.subheader("PnL distribution")
        pnl_hist = resolved["pnl"].value_counts(bins=20).sort_index().rename("count")
        st.bar_chart(pnl_hist)


# ── RECENT MARKETS ────────────────────────────────────────────────────────────
with tab_markets:
    st.subheader("Recently listed markets")
    if recent.empty:
        st.info("No markets ingested yet.")
    else:
        st.dataframe(
            recent,
            use_container_width=True,
            column_config={
                "listed_at": st.column_config.DatetimeColumn("Listed", format="MMM D HH:mm"),
            },
            hide_index=True,
        )

        if not prices.empty:
            st.subheader("Current NO prices (open markets)")
            st.dataframe(
                prices[["title", "venue", "category", "no_bid", "no_mid", "no_ask",
                         "no_spread", "volume_24h", "ts"]].sort_values("volume_24h", ascending=False),
                use_container_width=True,
                column_config={
                    "no_bid":      st.column_config.NumberColumn("NO bid",    format="$%.3f"),
                    "no_mid":      st.column_config.NumberColumn("NO mid",    format="$%.3f"),
                    "no_ask":      st.column_config.NumberColumn("NO ask",    format="$%.3f"),
                    "no_spread":   st.column_config.NumberColumn("Spread",    format="$%.3f"),
                    "volume_24h":  st.column_config.NumberColumn("24h vol",   format="%.0f"),
                    "ts":          st.column_config.DatetimeColumn("Last seen", format="HH:mm:ss"),
                },
                hide_index=True,
            )
