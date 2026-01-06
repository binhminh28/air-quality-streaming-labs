#!/bin/bash

echo "🚀 Starting Air Quality Streaming System..."
echo ""

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# Check virtual environment
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
    echo "✅ Virtual environment activated."
else
    echo "❌ Virtual environment not found."
    exit 1
fi

# Function to setup Docker cluster
setup_docker_cluster() {
    echo "🐳 Setting up Docker cluster..."
    
    # Cleanup old single-node containers
    docker stop cassandra kafka 2>/dev/null || true
    docker rm cassandra kafka 2>/dev/null || true
    
    # Start multi-node cluster
    cd docker || exit 1
    docker-compose down --remove-orphans 2>/dev/null || true
    docker-compose up -d
    
    echo "⏳ Waiting for services to start..."
    sleep 15
    
    # Check cluster status
    CASSANDRA_COUNT=$(docker ps --filter "name=cassandra" --format "{{.Names}}" | wc -l)
    KAFKA_COUNT=$(docker ps --filter "name=kafka" --format "{{.Names}}" | wc -l)
    
    if [ "$CASSANDRA_COUNT" -eq 0 ] || [ "$KAFKA_COUNT" -eq 0 ]; then
        echo "❌ Cluster failed to start properly!"
        exit 1
    fi
    
    echo "✅ Docker cluster started: $CASSANDRA_COUNT Cassandra node(s), $KAFKA_COUNT Kafka broker(s)"
    echo "   ⏳ Cassandra may need 30-60 seconds to fully start"
    
    cd "$SCRIPT_DIR" || exit 1
}

# Check Docker services
CASSANDRA_COUNT=$(docker ps --filter "name=cassandra" --format "{{.Names}}" | wc -l)
KAFKA_COUNT=$(docker ps --filter "name=kafka" --format "{{.Names}}" | wc -l)

if [ "$CASSANDRA_COUNT" -eq 0 ] || [ "$KAFKA_COUNT" -eq 0 ]; then
    echo "⚠️  Docker cluster not running. Starting cluster..."
    setup_docker_cluster
else
    echo "✅ Docker cluster đang chạy: $CASSANDRA_COUNT Cassandra node(s), $KAFKA_COUNT Kafka broker(s)"
fi

echo ""

# Create log directory
mkdir -p logs

# Function to start process in background
start_background() {
    local name=$1
    local command=$2
    local max_wait=${3:-10}
    echo "📝 Starting $name..."
    
    # Change to project directory to ensure correct paths
    cd "$(dirname "$0")" || exit 1
    
    nohup bash -c "source .venv/bin/activate 2>/dev/null; cd '$(pwd)'; $command" > "logs/${name}.log" 2>&1 &
    local pid=$!
    echo $pid > "logs/${name}.pid"
    echo "   PID: $pid"
    
    # Wait for process to start
    local count=0
    while [ $count -lt $max_wait ]; do
        if ps -p $pid > /dev/null 2>&1; then
            sleep 1
            count=$((count + 1))
        else
            echo "   ⚠️  Process may have failed. Check logs/${name}.log"
            break
        fi
    done
    
    sleep 2
}

# Function to check if service is ready
check_service() {
    local name=$1
    local check_cmd=$2
    local max_attempts=30
    local attempt=0
    
    echo "⏳ Waiting for $name to be ready..."
    while [ $attempt -lt $max_attempts ]; do
        if eval "$check_cmd" > /dev/null 2>&1; then
            echo "✅ $name is ready!"
            return 0
        fi
        sleep 1
        attempt=$((attempt + 1))
        if [ $((attempt % 5)) -eq 0 ]; then
            echo "   Still waiting... ($attempt/$max_attempts)"
        fi
    done
    echo "⚠️  $name may not be ready yet. Check logs/${name}.log"
    return 1
}

# Start WebSocket Server
start_background "websocket_server" "python dashboard/websocket_server.py" 5
check_service "WebSocket Server" "curl -s http://localhost:8765/health"

# Start Producer
start_background "producer" "python kafka/producer.py" 3

# Start Spark Streaming
start_background "spark_streaming" "bash run_spark_cassandra.sh" 10

echo ""
echo "✅ Tất cả services đã được khởi động!"
echo ""
echo "📊 Services đang chạy:"
echo "   - WebSocket Server: http://localhost:8765"
echo "   - Producer: Đang gửi dữ liệu vào Kafka"
echo "   - Spark Streaming: Đang xử lý và ghi vào Cassandra"
echo ""
echo "📝 Logs được lưu trong thư mục logs/"
echo ""
echo "💡 Để xem logs:"
echo "   tail -f logs/websocket_server.log"
echo "   tail -f logs/producer.log"
echo "   tail -f logs/spark_streaming.log"
echo ""
echo "💡 Để dừng tất cả:"
echo "   bash stop_all.sh"
echo ""
echo "🌐 Để chạy Dashboard:"
echo "   streamlit run dashboard/streamlit_app.py"
echo ""
echo "💡 Lưu ý: Đợi vài giây để Spark Streaming khởi động hoàn toàn trước khi chạy Dashboard"
