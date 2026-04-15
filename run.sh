#!/usr/bin/env bash
set -e

VENV=".venv"

if [ ! -d "$VENV" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV"
fi

source "$VENV/bin/activate"
pip install -q -r requirements.txt

echo ""
echo "Starting bot + dashboard (Ctrl+C stops both)"
echo "Dashboard → http://localhost:8501"
echo "Logs      → bot.log"
echo ""

# trap Ctrl+C and kill both processes
cleanup() {
    echo ""
    echo "Stopping..."
    kill "$BOT_PID" "$DASH_PID" 2>/dev/null
    wait "$BOT_PID" "$DASH_PID" 2>/dev/null
    exit 0
}
trap cleanup INT TERM

python main.py > bot.log 2>&1 &
BOT_PID=$!

streamlit run dashboard.py --server.headless true > /dev/null 2>&1 &
DASH_PID=$!

wait
