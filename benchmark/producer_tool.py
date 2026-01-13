import polars as pl
from kafka import KafkaProducer
import json
import time
import os
import argparse
import sys
from datetime import datetime, timezone

# Cấu hình mặc định
DEFAULT_TOPIC = "air_quality_realtime"
DEFAULT_BROKERS = "localhost:9092,localhost:9093,localhost:9094"
DATA_PATH = "./data/processed/air_quality_merged.parquet"

def json_serializer(data):
    # Đảm bảo datetime serialize đúng chuẩn ISO 8601
    if isinstance(data.get('datetime'), (datetime,)):
        data['datetime'] = data['datetime'].isoformat()
    return json.dumps(data).encode('utf-8')

def get_delay(mode):
    if mode == 'stress': return 0       # Max speed (Stress Test)
    elif mode == 'moderate': return 0.05 # ~20 msg/s
    else: return 1.0                    # 1 msg/s (Baseline)

def run_producer(mode, limit):
    brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BROKERS).split(',')
    topic = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC)
    
    print(f"🚀 [BENCHMARK PRODUCER] Mode: {mode.upper()} | Limit: {limit}")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=json_serializer,
            # Tối ưu hóa Batch gửi đi cho chế độ Stress
            linger_ms=10 if mode == 'stress' else 0,
            batch_size=16384
        )
    except Exception as e:
        sys.exit(f"❌ Lỗi Kafka: {e}")

    # Sử dụng Polars để đọc file Parquet
    if not os.path.exists(DATA_PATH):
        sys.exit(f"❌ Không thấy file data tại: {DATA_PATH}")
        
    try:
        df = pl.read_parquet(DATA_PATH)
        # Chuyển đổi sang list dictionary để lặp (iterator)
        records = df.to_dicts()
        print(f"✅ Đã load {len(records)} bản ghi từ Parquet.")
    except Exception as e:
         sys.exit(f"❌ Lỗi đọc Parquet: {e}")

    delay = get_delay(mode)
    count = 0
    start_time = time.time()
    
    try:
        while True:
            for row in records:
                # Quan trọng: Gán timestamp hiện tại (UTC) để đo latency chính xác
                row['datetime'] = datetime.now(timezone.utc).isoformat()
                
                producer.send(topic, row)
                count += 1
                
                if delay > 0:
                    time.sleep(delay)
                
                # Log throughput mỗi 1000 tin
                if count % 1000 == 0:
                    elapsed = time.time() - start_time
                    print(f"   Sent {count} msgs. Rate: {count/elapsed:.2f} msg/s")
                
                if limit and count >= limit:
                    producer.flush()
                    total_time = time.time() - start_time
                    print(f"\n✅ HOÀN THÀNH.")
                    print(f"   - Tổng tin gửi: {count}")
                    print(f"   - Thời gian:    {total_time:.2f}s")
                    print(f"   - Throughput:   {count/total_time:.2f} msg/s")
                    return

    except KeyboardInterrupt:
        print("\n⛔ Dừng bởi người dùng.")
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=['baseline', 'moderate', 'stress'])
    parser.add_argument("--limit", type=int, default=5000)
    args = parser.parse_args()
    run_producer(args.mode, args.limit)