#!/bin/bash
# benchmark/run_suite.sh

if [ -z "$1" ]; then
    echo "Usage: ./benchmark/run_suite.sh [baseline|moderate|stress]"
    exit 1
fi

MODE=$1
LIMIT=2000
OUTPUT_CSV="benchmark/results_${MODE}.csv"

echo "=== BENCHMARK SUITE: $MODE ==="

# Cleanup old processes
pkill -f producer_tool.py
sleep 2

# 1. Run Producer
echo "▶️ Running Producer ($MODE)..."
if [ "$MODE" == "stress" ]; then LIMIT=5000; fi

# Dùng 'poetry run python' thay vì 'python3'
poetry run python benchmark/producer_tool.py $MODE --limit $LIMIT &
PRODUCER_PID=$!

wait $PRODUCER_PID
echo "✅ Producer finished."

# 2. Wait for processing
echo "⏳ Waiting 10s for Spark to finish..."
sleep 10

# 3. Analyze
echo "▶️ Analyzing Results..."
poetry run python benchmark/analyzer_tool.py --limit $LIMIT --output $OUTPUT_CSV

echo "=== DONE. Results: $OUTPUT_CSV ==="