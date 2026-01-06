#!/bin/bash

# Script chạy Spark Streaming Job với Cassandra sink
# Tự động activate virtual environment và kiểm tra services

echo "🚀 Starting Spark Streaming Job with Cassandra sink..."
echo ""

# Activate virtual environment nếu có
if [ -f ".venv/bin/activate" ]; then
    echo "📦 Activating virtual environment..."
    source .venv/bin/activate
else
    echo "⚠️  Virtual environment không tìm thấy tại .venv/bin/activate"
    echo "   Đảm bảo đã chạy: poetry install hoặc python -m venv .venv"
fi

# Kiểm tra spark-submit có sẵn không
if ! command -v spark-submit &> /dev/null; then
    echo "❌ spark-submit không tìm thấy!"
    echo "   Đảm bảo đã cài đặt pyspark trong virtual environment"
    echo "   Chạy: poetry install hoặc pip install pyspark"
    exit 1
fi

# Kiểm tra Cassandra cluster đang chạy (ít nhất 1 node)
CASSANDRA_COUNT=$(docker ps --filter "name=cassandra" --format "{{.Names}}" | wc -l)
if [ "$CASSANDRA_COUNT" -eq 0 ]; then
    echo "❌ Cassandra cluster không đang chạy!"
    echo "   Chạy: cd docker && docker-compose up -d"
    exit 1
else
    echo "✅ Found $CASSANDRA_COUNT Cassandra node(s)"
fi

# Kiểm tra Kafka cluster đang chạy (ít nhất 1 broker)
KAFKA_COUNT=$(docker ps --filter "name=kafka" --format "{{.Names}}" | wc -l)
if [ "$KAFKA_COUNT" -eq 0 ]; then
    echo "❌ Kafka cluster không đang chạy!"
    echo "   Chạy: cd docker && docker-compose up -d"
    exit 1
else
    echo "✅ Found $KAFKA_COUNT Kafka broker(s)"
fi

echo "✅ Services đang chạy"
echo ""

# Chạy Spark job với Cassandra mode
echo "📝 Chạy Spark job với SINK_MODE=cassandra..."
echo ""

SINK_MODE=cassandra spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
  spark/streaming_job.py

