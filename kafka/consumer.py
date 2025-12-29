from kafka import KafkaConsumer
import json

KAFKA_TOPIC = "air_quality_realtime"
BOOTSTRAP_SERVERS = 'localhost:9092'

def start_consumer():
    print(f"Connecting to Kafka topic '{KAFKA_TOPIC}'...")
    
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='air-quality-monitoring-group', 
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Waiting for messages...")
    
    try:
        for message in consumer:
            data = message.value

            pm25 = data.get('pm25', 0)
            status = "🟢 Tốt" if pm25 < 50 else "🔴 Nguy hại" if pm25 > 150 else "🟡 Trung bình"
            
            print(f"Received: {data.get('datetime')} | PM2.5: {pm25:.2f} -> {status}")
            
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    start_consumer()