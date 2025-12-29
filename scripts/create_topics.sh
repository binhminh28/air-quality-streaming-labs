#!/bin/bash

# Script tạo Kafka topics cho dự án Air Quality Streaming
# Sử dụng Kafka container hoặc local Kafka

KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}

echo "🔧 Creating Kafka topics..."
echo "Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"

# Kiểm tra xem có đang chạy trong Docker container không
if command -v docker &> /dev/null; then
    KAFKA_CONTAINER=$(docker ps --filter "name=kafka" --format "{{.Names}}" | head -n 1)
    if [ ! -z "$KAFKA_CONTAINER" ]; then
        echo "📦 Found Kafka container: $KAFKA_CONTAINER"
        KAFKA_CMD="docker exec -it $KAFKA_CONTAINER kafka-topics"
    else
        echo "⚠️  Kafka container not found, using local kafka-topics command"
        KAFKA_CMD="kafka-topics"
    fi
else
    KAFKA_CMD="kafka-topics"
fi

# Tạo topic air_quality_realtime
echo "📝 Creating topic: air_quality_realtime"
if [ ! -z "$KAFKA_CONTAINER" ]; then
    docker exec -it $KAFKA_CONTAINER kafka-topics --create \
        --bootstrap-server localhost:29092 \
        --topic air_quality_realtime \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists
else
    $KAFKA_CMD --create \
        --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
        --topic air_quality_realtime \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists
fi

# Kiểm tra topic đã được tạo
echo ""
echo "✅ Verifying topics..."
if [ ! -z "$KAFKA_CONTAINER" ]; then
    docker exec -it $KAFKA_CONTAINER kafka-topics --list --bootstrap-server localhost:29092
else
    $KAFKA_CMD --list --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS
fi

echo ""
echo "🎉 Done! Topics created successfully."

