import polars as pl
from kafka import KafkaProducer
import json
import time
from datetime import datetime, timezone

KAFKA_TOPIC = "air_quality_realtime"
BOOTSTRAP_SERVERS = 'localhost:9092'

def json_serializer(data):
    if isinstance(data.get('datetime'), (datetime,)):
        data['datetime'] = data['datetime'].isoformat()
    return json.dumps(data).encode('utf-8')

def start_streaming(parquet_file):
    producer = KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
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