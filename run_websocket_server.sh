#!/bin/bash

echo "🚀 Starting WebSocket Server..."
echo ""

if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    echo "✅ Virtual environment activated."
else
    echo "❌ Virtual environment not found."
    exit 1
fi

if ! command -v python &> /dev/null
then
    echo "❌ Python not found in PATH."
    exit 1
fi

if ! docker ps | grep -q cassandra; then
    echo "❌ Cassandra container không đang chạy!"
    echo "   Chạy: cd docker && docker-compose up -d"
    exit 1
fi

echo "✅ Services đang chạy"
echo ""
echo "📝 Starting WebSocket Server..."
echo "   HTTP API: http://localhost:8765/api/data"
echo "   WebSocket: ws://localhost:8765/ws"
echo ""

python dashboard/websocket_server.py

