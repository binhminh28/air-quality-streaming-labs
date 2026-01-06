#!/bin/bash

echo "🛑 Stopping Air Quality Streaming System..."
echo ""

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# Stop application processes
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

echo "✅ Đã dừng tất cả application services"
echo ""

# Option to stop Docker cluster
if [ "$1" == "--stop-cluster" ] || [ "$1" == "-c" ]; then
    echo "🐳 Stopping Docker cluster..."
    cd docker || exit 1
    docker-compose down
    cd "$SCRIPT_DIR" || exit 1
    echo "✅ Docker cluster stopped"
else
    echo "💡 Docker cluster vẫn đang chạy"
    echo "   Để dừng cluster: bash stop_all.sh --stop-cluster"
fi

