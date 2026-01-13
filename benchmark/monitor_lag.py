import time
import os
from cassandra.cluster import Cluster
from kafka import KafkaConsumer
import json

# Cấu hình
KAFKA_TOPIC = "air_quality_realtime"
CASSANDRA_HOSTS = ['localhost']
KEYSPACE = "air_quality"

def get_cassandra_count(session):
    try:
        # Lưu ý: count(*) trên Cassandra lớn có thể chậm, nhưng ổn cho benchmark nhỏ < 100k row
        row = session.execute(f"SELECT count(*) FROM realtime_data").one()
        return row[0]
    except Exception:
        return 0

def monitor():
    print("👀 STARTING LAG MONITOR (Ctrl+C to stop)...")
    
    # Connect Cassandra
    cluster = Cluster(CASSANDRA_HOSTS, port=9042)
    session = cluster.connect(KEYSPACE)
    
    # Connect Kafka to get total offsets (Estimating total produced)
    # Cách đơn giản nhất để biết tổng tin trong Kafka là xem Highwater mark
    consumer = KafkaConsumer(
        KAFKA_TOPIC, 
        bootstrap_servers='localhost:9092',
        group_id='monitor_tool'
    )
    
    try:
        while True:
            # 1. Lấy tổng tin hiện có trong Kafka (Total Produced)
            partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
            total_kafka_msgs = 0
            if partitions:
                from kafka import TopicPartition
                tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
                end_offsets = consumer.end_offsets(tps)
                total_kafka_msgs = sum(end_offsets.values())

            # 2. Lấy tổng tin đã vào Cassandra (Total Processed)
            processed_count = get_cassandra_count(session)
            
            # 3. Tính Lag
            lag = total_kafka_msgs - processed_count
            if lag < 0: lag = 0 # Do độ trễ cập nhật counter
            
            print(f"Time: {time.strftime('%H:%M:%S')} | Kafka: {total_kafka_msgs} | DB: {processed_count} | ⚠️ LAG: {lag}")
            time.sleep(2)
            
    except KeyboardInterrupt:
        print("Stopped.")
    finally:
        cluster.shutdown()
        consumer.close()

if __name__ == "__main__":
    monitor()