# Hướng dẫn Migration từ Single-node sang Multi-node Cluster

Tài liệu này mô tả các thay đổi đã được thực hiện để chuyển đổi dự án từ kiến trúc Single-node sang Multi-node Cluster.

## 📋 Tổng quan thay đổi

### Docker Compose
- ✅ **Kafka**: Từ 1 broker → 3 brokers (kafka-1, kafka-2, kafka-3)
- ✅ **Cassandra**: Từ 1 node → 2 nodes (cassandra-1, cassandra-2)
- ✅ **Resource Limits**: Đã thêm giới hạn memory cho tất cả services

### Code Changes
- ✅ **producer.py**: Hỗ trợ multiple Kafka bootstrap servers
- ✅ **streaming_job.py**: Hỗ trợ multiple Kafka brokers và Cassandra hosts
- ✅ **websocket_server.py**: Hỗ trợ multiple Cassandra contact points
- ✅ **create_topics.sh**: Cập nhật replication-factor=3 cho 3 brokers
- ✅ **init_cassandra.cql**: Cập nhật replication_factor=2 cho 2 nodes

## 🚀 Cách sử dụng

### 1. Khởi động Cluster

```bash
cd docker
docker-compose up -d
```

Kiểm tra các containers:
```bash
docker ps
```

Bạn sẽ thấy:
- `zookeeper` (port 22181)
- `kafka-1` (port 9092)
- `kafka-2` (port 9093)
- `kafka-3` (port 9094)
- `cassandra-1` (port 9042)
- `cassandra-2` (port 9043)

### 2. Tạo Kafka Topic với Replication Factor = 3

```bash
bash scripts/create_topics.sh
```

Hoặc thủ công:
```bash
docker exec -it kafka-1 kafka-topics --create \
  --bootstrap-server localhost:29092 \
  --topic air_quality_realtime \
  --partitions 3 \
  --replication-factor 3 \
  --if-not-exists
```

### 3. Khởi tạo Cassandra Schema

```bash
docker exec -it cassandra-1 cqlsh -f /scripts/init_cassandra.cql
```

Hoặc copy file vào container:
```bash
docker cp scripts/init_cassandra.cql cassandra-1:/tmp/
docker exec -it cassandra-1 cqlsh -f /tmp/init_cassandra.cql
```

### 4. Cấu hình Environment Variables (Tùy chọn)

Tạo file `.env` hoặc export các biến:

```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092,localhost:9093,localhost:9094"
export CASSANDRA_CONTACT_POINTS="localhost:9042,localhost:9043"
export CASSANDRA_HOSTS="localhost:9042,localhost:9043"
```

### 5. Chạy các ứng dụng

**Producer:**
```bash
python kafka/producer.py
```

**Spark Streaming:**
```bash
bash run_spark_cassandra.sh
```

**WebSocket Server:**
```bash
python dashboard/websocket_server.py
```

## 🔍 Kiểm tra Cluster Status

### Kiểm tra Kafka Cluster

```bash
# List tất cả brokers
docker ps | grep kafka

# Kiểm tra topic replication
docker exec -it kafka-1 kafka-topics --describe \
  --bootstrap-server localhost:29092 \
  --topic air_quality_realtime
```

Kết quả mong đợi: Topic sẽ có replication-factor=3, mỗi partition được replicate trên cả 3 brokers.

### Kiểm tra Cassandra Cluster

```bash
# List tất cả nodes
docker ps | grep cassandra

# Kiểm tra cluster status
docker exec -it cassandra-1 nodetool status
```

Kết quả mong đợi: Sẽ thấy 2 nodes (UN = Up Normal).

## 📝 Chi tiết thay đổi

### 1. docker-compose.yml

**Kafka Brokers:**
- 3 brokers với BROKER_ID: 1, 2, 3
- Ports: 9092, 9093, 9094 (mapped từ container port 9092)
- KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
- Memory limit: 512MB mỗi broker

**Cassandra Nodes:**
- 2 nodes: cassandra-1 (seed), cassandra-2
- Ports: 9042, 9043 (mapped từ container port 9042)
- CASSANDRA_SEEDS: cassandra-1 (cho node 2)
- Memory limit: 1GB mỗi node

### 2. Code Files

Tất cả các file Python đã được cập nhật để:
- Hỗ trợ danh sách hosts/ports (comma-separated)
- Sử dụng environment variables với giá trị mặc định
- In log khi kết nối để dễ debug

### 3. Scripts

- **create_topics.sh**: Replication-factor từ 1 → 3
- **init_cassandra.cql**: Replication_factor từ 1 → 2
- **run_spark_cassandra.sh**: Kiểm tra multiple containers

## ⚠️ Lưu ý quan trọng

1. **Resource Usage**: Với 3 Kafka brokers + 2 Cassandra nodes, tổng memory tối thiểu cần:
   - Kafka: 3 × 512MB = 1.5GB
   - Cassandra: 2 × 1GB = 2GB
   - Zookeeper: 512MB
   - **Tổng: ~4GB** (chưa kể overhead)

2. **Port Conflicts**: Đảm bảo các port 9092-9094 và 9042-9043 không bị conflict với services khác.

3. **Data Persistence**: Mỗi Cassandra node có volume riêng (`cassandra_data_1`, `cassandra_data_2`).

4. **Topic Creation**: Phải tạo topic với `replication-factor=3` để tận dụng 3 brokers.

5. **Cassandra Replication**: Keyspace phải có `replication_factor=2` để dữ liệu được replicate trên cả 2 nodes.

## 🔄 Rollback (Nếu cần)

Nếu muốn quay lại single-node, có thể:
1. Restore file `docker-compose.yml` từ git history
2. Hoặc comment out các services kafka-2, kafka-3, cassandra-2
3. Cập nhật lại replication factors về 1

## 📚 Tài liệu tham khảo

- Xem `CONFIG.md` để biết chi tiết về cấu hình environment variables
- Xem `README.md` để biết hướng dẫn sử dụng chung


