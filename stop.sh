#!/usr/bin/env bash
if [ -f bot.pid ]; then
    PID=$(cat bot.pid)
    if kill "$PID" 2>/dev/null; then
        echo "Bot stopped (pid $PID)"
    else
        echo "Bot was not running"
    fi
    rm bot.pid
else
    echo "No bot.pid found"
fi
