# Cấu hình Multi-Node Cluster

File này hướng dẫn cấu hình cho kiến trúc Multi-Node Cluster.

## Biến môi trường (Environment Variables)

### Kafka Configuration

```bash
# Multiple brokers separated by commas
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092,localhost:9093,localhost:9094"
export KAFKA_TOPIC="air_quality_realtime"
```

### Cassandra Configuration

```bash
# Multiple contact points for cassandra-driver (Python)
export CASSANDRA_CONTACT_POINTS="localhost:9042,localhost:9043"

# Multiple hosts for Spark Cassandra Connector
export CASSANDRA_HOSTS="localhost:9042,localhost:9043"
export CASSANDRA_KEYSPACE="air_quality"
export CASSANDRA_TABLE="realtime_data"
```

### Spark Configuration

```bash
export SINK_MODE="cassandra"  # or "console"
```

### Redis Configuration

```bash
# Default là "localhost" vì Spark job và WebSocket server chạy trên host
# Redis container expose port 6379 ra host
export REDIS_HOST="localhost"  # Default: "localhost"
export REDIS_PORT="6379"      # Default: "6379"
```

### WebSocket Server Configuration

```bash
export HTTP_PORT="8765"
```

## Sử dụng file .env (Khuyến nghị)

Tạo file `.env` trong thư mục gốc với nội dung:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094
KAFKA_TOPIC=air_quality_realtime

# Cassandra
CASSANDRA_CONTACT_POINTS=localhost:9042,localhost:9043
CASSANDRA_HOSTS=localhost:9042,localhost:9043
CASSANDRA_KEYSPACE=air_quality
CASSANDRA_TABLE=realtime_data

# Spark
SINK_MODE=cassandra

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# WebSocket
HTTP_PORT=8765
```

Sau đó load biến môi trường:

```bash
export $(cat .env | xargs)
```

## Cấu hình Docker Compose

File `docker/docker-compose.yml` đã được cấu hình với:

- **3 Kafka Brokers**: kafka-1 (9092), kafka-2 (9093), kafka-3 (9094)
- **2 Cassandra Nodes**: cassandra-1 (9042), cassandra-2 (9043)
- **1 Redis Instance**: redis (6379)
- **Resource Limits**: 
  - Kafka: 512MB mỗi broker
  - Cassandra: 1GB mỗi node
  - Redis: 256MB
  - Zookeeper: 512MB

## Lưu ý quan trọng

1. **Kafka Replication Factor**: Khi tạo topic, sử dụng `replication-factor=3` để đảm bảo dữ liệu được replicate trên cả 3 brokers.

2. **Cassandra Replication Factor**: Trong `init_cassandra.cql`, đảm bảo `replication_factor` phù hợp với số node (ví dụ: 2 cho 2 nodes).

3. **Port Mapping**: Các port được map ra host để các ứng dụng chạy ngoài Docker có thể kết nối:
   - Kafka: 9092, 9093, 9094
   - Cassandra: 9042, 9043
   - Redis: 6379

4. **Redis Connection**: 
   - Spark job và WebSocket server chạy trên host (không trong Docker network)
   - Redis container expose port 6379 ra host
   - Do đó, `REDIS_HOST` phải là `"localhost"` (không phải `"redis"`)
   - Nếu chạy Spark/WebSocket trong Docker container, thì dùng `REDIS_HOST="redis"`

5. **Resource Limits**: Đã thêm giới hạn memory để tránh treo máy. Có thể điều chỉnh trong `docker-compose.yml` nếu cần.

## Kiểm tra Cluster

### Kiểm tra Kafka Cluster

```bash
# List brokers
docker ps | grep kafka

# Kiểm tra topic replication
docker exec -it kafka-1 kafka-topics --describe \
  --bootstrap-server localhost:29092 \
  --topic air_quality_realtime
```

### Kiểm tra Cassandra Cluster

```bash
# List nodes
docker ps | grep cassandra

# Kiểm tra cluster status
docker exec -it cassandra-1 nodetool status
```


