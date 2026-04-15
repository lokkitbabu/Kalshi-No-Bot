import sqlite3

import pandas as pd
import streamlit as st

from config import DB_PATH

st.set_page_config(page_title="Kalshi NO Bot", layout="wide")
st.title("Kalshi NO Bot — Paper Trading Dashboard")


@st.cache_resource
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    from db import init_db
    init_db(conn)
    return conn


conn = get_conn()


def load_positions() -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT
            p.id,
            p.market_id,
            m.title,
            m.category,
            p.strategy_name,
            p.entry_no_mid,
            p.entry_spread,
            p.entry_volume,
            p.entry_time,
            p.exit_no_price,
            p.exit_time,
            p.resolved,
            p.pnl,
            m.outcome
        FROM positions p
        JOIN markets m ON p.market_id = m.market_id
        ORDER BY p.entry_time DESC
    """, conn, parse_dates=["entry_time", "exit_time"])


def load_prices(market_id: str) -> pd.DataFrame:
    return pd.read_sql_query("""
        SELECT ts, no_mid, no_bid, no_ask, no_spread
        FROM prices
        WHERE market_id = ?
        ORDER BY ts
    """, conn, params=(market_id,), parse_dates=["ts"])


df = load_positions()

if df.empty:
    st.info("No data yet — run `python main.py` and wait for the first poll cycle.")
    st.stop()

resolved = df[df["resolved"] == 1]
open_pos  = df[df["resolved"] == 0]

# ── summary ──────────────────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total positions", len(df))
c2.metric("Resolved", len(resolved))
c3.metric("Open", len(open_pos))

if len(resolved):
    total_pnl = resolved["pnl"].sum()
    win_rate  = (resolved["pnl"] > 0).mean()
    c4.metric("Total PnL", f"${total_pnl:.2f}")
    c5.metric("Win rate", f"{win_rate:.1%}")

st.divider()

if len(resolved) == 0:
    st.info("No resolved positions yet.")
    st.stop()

# ── equity curve ─────────────────────────────────────────────────────────────
st.subheader("Equity curve")
curve = resolved.sort_values("exit_time").copy()
curve["cum_pnl"] = curve["pnl"].cumsum()
st.line_chart(curve.set_index("exit_time")["cum_pnl"])

# ── PnL by entry price bucket ─────────────────────────────────────────────────
st.subheader("PnL by NO entry price bucket")
resolved = resolved.copy()
resolved["bucket"] = pd.cut(
    resolved["entry_no_mid"],
    bins=[0, 0.5, 0.7, 0.85, 0.92, 0.97, 1.0],
    labels=["<50¢", "50–70¢", "70–85¢", "85–92¢", "92–97¢", ">97¢"],
)
bucket_pnl = resolved.groupby("bucket", observed=True)["pnl"].agg(["mean", "count"])
bucket_pnl.columns = ["avg_pnl", "count"]
st.bar_chart(bucket_pnl["avg_pnl"])
st.dataframe(bucket_pnl, use_container_width=True)

# ── PnL by category ───────────────────────────────────────────────────────────
st.subheader("PnL by category")
cat = resolved.groupby("category")["pnl"].agg(["mean", "sum", "count"])
cat.columns = ["avg_pnl", "total_pnl", "count"]
st.dataframe(cat.sort_values("total_pnl"), use_container_width=True)

# ── time to resolution ────────────────────────────────────────────────────────
st.subheader("Time to resolution (days)")
resolved["hold_days"] = (resolved["exit_time"] - resolved["entry_time"]).dt.days
st.bar_chart(resolved["hold_days"].value_counts().sort_index())

# ── strategy comparison ───────────────────────────────────────────────────────
st.subheader("Strategy comparison")
strat = resolved.groupby("strategy_name")["pnl"].agg(["mean", "sum", "count"])
strat.columns = ["avg_pnl", "total_pnl", "count"]
st.dataframe(strat, use_container_width=True)

# ── position table ────────────────────────────────────────────────────────────
st.subheader("All resolved positions")
st.dataframe(
    resolved[["market_id", "title", "category", "strategy_name",
              "entry_no_mid", "entry_spread", "pnl", "outcome", "hold_days"]]
    .sort_values("pnl"),
    use_container_width=True,
)

# ── price history for a single market ─────────────────────────────────────────
st.divider()
st.subheader("Price history")
market_ids = df["market_id"].unique().tolist()
selected = st.selectbox("Market", market_ids)
if selected:
    px = load_prices(selected)
    if len(px):
        st.line_chart(px.set_index("ts")[["no_bid", "no_mid", "no_ask"]])
    else:
        st.info("No price snapshots recorded for this market yet.")
