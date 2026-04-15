#!/usr/bin/env bash
set -e

VENV=".venv"

# create venv if missing
if [ ! -d "$VENV" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV"
fi

source "$VENV/bin/activate"

# install deps quietly if needed
pip install -q -r requirements.txt

echo ""
echo "Starting bot (background) + dashboard..."
echo "Dashboard → http://localhost:8501"
echo "Logs      → bot.log"
echo "Stop      → ./stop.sh"
echo ""

# start bot in background, log to file
nohup python main.py > bot.log 2>&1 &
echo $! > bot.pid

# start dashboard in foreground (Ctrl+C stops dashboard; bot keeps running)
streamlit run dashboard.py --server.headless true
