#!/bin/bash

echo "🛑 Stopping Air Quality Streaming System..."
echo ""

if [ -d "logs" ]; then
    for pidfile in logs/*.pid; do
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            name=$(basename "$pidfile" .pid)
            if ps -p $pid > /dev/null 2>&1; then
                echo "🛑 Stopping $name (PID: $pid)..."
                kill $pid 2>/dev/null
                sleep 1
                if ps -p $pid > /dev/null 2>&1; then
                    kill -9 $pid 2>/dev/null
                fi
            fi
            rm "$pidfile"
        fi
    done
fi

# Also kill by process name
pkill -f "websocket_server.py" 2>/dev/null
pkill -f "producer.py" 2>/dev/null
pkill -f "streaming_job.py" 2>/dev/null
pkill -f "spark-submit.*streaming_job" 2>/dev/null

echo "✅ Đã dừng tất cả services"

