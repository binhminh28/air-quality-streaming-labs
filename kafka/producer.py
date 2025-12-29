import polars as pl
from kafka import KafkaProducer
import json
import time
from datetime import datetime

KAFKA_TOPIC = "air_quality_realtime"
BOOTSTRAP_SERVERS = 'localhost:9092'

def json_serializer(data):
    if isinstance(data.get('datetime'), (datetime,)):
        data['datetime'] = data['datetime'].isoformat()
    return json.dumps(data).encode('utf-8')

def start_streaming(parquet_file):
    print(f"📡 Connecting to Kafka at {BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=json_serializer
    )

    print(f"📂 Reading Parquet file: {parquet_file}")
    df = pl.read_parquet(parquet_file).sort("datetime")
    
    print("🚀 Start Streaming data...")
    
    for row in df.iter_rows(named=True):
        producer.send(KAFKA_TOPIC, row)
        
        print(f"Sent: {row['datetime']} | PM2.5: {row['pm25']:.2f}")

        time.sleep(5) 

if __name__ == "__main__":
    start_streaming("./data/processed/air_quality_5s_noise.parquet")