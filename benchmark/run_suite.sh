#!/bin/bash
# benchmark/run_suite.sh

if [ -z "$1" ]; then
    echo "Usage: ./benchmark/run_suite.sh [baseline|moderate|stress]"
    exit 1
fi

MODE=$1
LIMIT=2000
if [ "$MODE" == "stress" ]; then LIMIT=10000; fi # Tăng limit để test lâu hơn
OUTPUT_CSV="benchmark/results_${MODE}.csv"
RESOURCE_LOG="benchmark/resources_${MODE}.log"

echo "=== BENCHMARK SUITE: $MODE ==="

# 0. Clean & Prepare
echo "🧹 Cleaning up old processes..."
pkill -f producer_tool.py
pkill -f docker stats
sleep 2

echo "🧹 Truncating Cassandra table (Ensure fresh data)..."
# Lệnh này xóa sạch bảng realtime_data để đảm bảo tính toán đúng
docker exec cassandra cqlsh -e "TRUNCATE air_quality.realtime_data;"

# 1. Start Resource Monitoring (NEW)
echo "📈 Starting Resource Monitor..."
# Ghi log CPU/RAM mỗi giây vào file
docker stats --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}" > $RESOURCE_LOG &
MONITOR_PID=$!

# 2. Run Producer
echo "▶️ Running Producer ($MODE)..."
poetry run python benchmark/producer_tool.py $MODE --limit $LIMIT &
PRODUCER_PID=$!

# Chờ Producer chạy xong
wait $PRODUCER_PID
echo "✅ Producer finished."

# 3. Wait for Spark to drain the queue (Lag handling)
echo "⏳ Waiting for Spark to finish processing..."
# Thay vì sleep cứng 10s, ta có thể sleep lâu hơn tùy mode
sleep 20 

# Stop monitor
kill $MONITOR_PID

# 4. Analyze
echo "▶️ Analyzing Results..."
poetry run python benchmark/analyzer_tool.py --limit $LIMIT --output $OUTPUT_CSV

echo "=== DONE. Results saved to $OUTPUT_CSV ==="
echo "=== Resource logs saved to $RESOURCE_LOG ==="