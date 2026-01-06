import polars as pl
from kafka import KafkaProducer
import json
import time
import os
from datetime import datetime, timezone

# Cấu hình Kafka - hỗ trợ multiple brokers
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "air_quality_realtime")
# Hỗ trợ danh sách brokers: "localhost:9092,localhost:9093,localhost:9094" hoặc từ env
BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", 
    "localhost:9092,localhost:9093,localhost:9094"
)

def json_serializer(data):
    if isinstance(data.get('datetime'), (datetime,)):
        data['datetime'] = data['datetime'].isoformat()
    return json.dumps(data).encode('utf-8')

def start_streaming(parquet_file):
    # Chuyển đổi chuỗi thành list nếu cần
    if isinstance(BOOTSTRAP_SERVERS, str):
        servers = [s.strip() for s in BOOTSTRAP_SERVERS.split(',')]
    else:
        servers = BOOTSTRAP_SERVERS
    
    print(f"Connecting to Kafka brokers: {servers}")
    
    producer = KafkaProducer(
        bootstrap_servers=servers,
        value_serializer=json_serializer
    )

    df = pl.read_parquet(parquet_file)
    
    for row in df.iter_rows(named=True):
        # Sử dụng datetime hiện tại thay vì từ parquet
        current_datetime = datetime.now(timezone.utc)
        row['datetime'] = current_datetime.isoformat()
        
        producer.send(KAFKA_TOPIC, row)
        time.sleep(5)

if __name__ == "__main__":
    start_streaming("./data/processed/air_quality_5s_noise.parquet")