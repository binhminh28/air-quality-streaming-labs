#!/bin/bash

echo "🌐 Starting Streamlit Dashboard..."
echo ""

# Check virtual environment
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    echo "✅ Virtual environment activated."
else
    echo "❌ Virtual environment not found."
    exit 1
fi

# Check WebSocket server
echo "🔍 Checking WebSocket server..."
if curl -s http://localhost:8765/health > /dev/null 2>&1; then
    echo "✅ WebSocket server is running"
else
    echo "❌ WebSocket server is NOT running!"
    echo "   Run: bash start_all.sh"
    exit 1
fi

# Check if data is available
echo "🔍 Checking data availability..."
DATA_CHECK=$(curl -s http://localhost:8765/api/data?limit=1 2>&1)
if echo "$DATA_CHECK" | grep -q '"count":1'; then
    echo "✅ Data is available"
else
    echo "⚠️  No data available yet. Dashboard will show empty state."
    echo "   Wait a few seconds for Spark Streaming to process data."
fi

echo ""
echo "🚀 Starting Dashboard..."
echo ""
echo "📊 Dashboard will open at:"
echo "   Local:   http://localhost:8501"
echo "   Network: http://$(hostname -I | awk '{print $1}'):8501"
echo ""
echo "💡 Press Ctrl+C to stop"
echo ""

streamlit run dashboard/streamlit_app.py

