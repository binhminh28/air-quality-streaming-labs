# Các bước tiếp theo để chạy hệ thống

Dựa trên README và các file hiện có, đây là các bước bạn nên làm tiếp:

## ✅ Đã hoàn thành

1. ✅ Producer (`kafka/producer.py`) - đọc Parquet và gửi vào Kafka
2. ✅ Streaming Job (`spark/streaming_job.py`) - đọc từ Kafka, tính AQI, ghi vào database
3. ✅ Script tạo Kafka topic (`scripts/create_topics.sh`)
4. ✅ Script khởi tạo Cassandra (`scripts/init_cassandra.cql`)
5. ✅ Docker Compose với Kafka, Zookeeper, Cassandra

## 📋 Các bước tiếp theo

### Bước 1: Khởi động Docker services

```bash
cd docker
docker-compose up -d
```

Kiểm tra các container đang chạy:
```bash
docker ps
```

Bạn sẽ thấy:
- `zookeeper` (port 22181)
- `kafka` (port 9092)
- `cassandra` (port 9042)

### Bước 2: Tạo Kafka topic

```bash
bash scripts/create_topics.sh
```

Hoặc nếu chạy trong Docker:
```bash
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:29092 \
  --topic air_quality_realtime \
  --partitions 3 \
  --replication-factor 1
```

### Bước 3: Khởi tạo Cassandra schema

```bash
# Kết nối vào Cassandra container
docker exec -it cassandra cqlsh

# Hoặc chạy file CQL trực tiếp
docker exec -i cassandra cqlsh < scripts/init_cassandra.cql
```

Trong cqlsh, chạy:
```cql
CREATE KEYSPACE IF NOT EXISTS air_quality
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE air_quality;

CREATE TABLE IF NOT EXISTS realtime_data (
    datetime TEXT PRIMARY KEY,
    pm25 FLOAT,
    aqi INT,
    quality TEXT,
    processed_at TIMESTAMP
);
```

### Bước 4: Chạy Producer

Đảm bảo bạn có file Parquet trong `data/processed/air_quality_5s_noise.parquet`:

```bash
python kafka/producer.py
```

Producer sẽ đọc từng dòng từ Parquet và gửi vào Kafka topic `air_quality_realtime` với tốc độ 1 record mỗi 5 giây.

### Bước 5: Chạy Spark Streaming Job

**Giải thích lệnh `spark-submit`:**

- `spark-submit`: Lệnh để chạy Spark application (Python script)
- `--packages`: Tải các thư viện cần thiết từ Maven repository
  - `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`: Connector để Spark đọc dữ liệu từ Kafka
  - `2.12`: Scala version, `3.5.0`: Spark version
- `spark/streaming_job.py`: File Python chứa code xử lý streaming

**Lưu ý:** Lần đầu chạy sẽ mất thời gian để download các packages. Các lần sau sẽ nhanh hơn vì đã cache.

#### Option A: Ghi ra Console (để debug) - **Khuyến nghị cho lần đầu**

```bash
# Đảm bảo đã activate virtual environment (nếu dùng)
source .venv/bin/activate

# Chạy Spark job
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/streaming_job.py
```

**Kết quả mong đợi:**
- Spark sẽ khởi động và hiển thị nhiều log INFO
- Bạn sẽ thấy: `🚀 Spark Job started. Listening to air_quality_realtime...`
- Bạn sẽ thấy: `📺 Writing to console (for debugging)`
- Sau đó sẽ thấy các **Batch** với dữ liệu đã xử lý (PM2.5, AQI, Quality)
- Job sẽ chạy liên tục cho đến khi bạn nhấn `Ctrl+C` để dừng

**Spark UI:** Bạn có thể mở trình duyệt tại `http://localhost:4040` để xem Spark UI (nếu có quyền truy cập)

#### Option B: Ghi vào Cassandra (sau khi đã test với console)

**Cách 1: Sử dụng script (khuyến nghị)**

```bash
# Script tự động kiểm tra services và chạy với đúng cấu hình
bash run_spark_cassandra.sh
```

**Cách 2: Chạy trực tiếp**

```bash
# QUAN TRỌNG: Phải set SINK_MODE=cassandra trước spark-submit
# Và thêm Cassandra connector vào --packages

SINK_MODE=cassandra spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 \
  spark/streaming_job.py
```

**Lưu ý quan trọng:** 
- ⚠️ **PHẢI** set `SINK_MODE=cassandra` trước lệnh `spark-submit`
- ⚠️ **PHẢI** thêm Cassandra connector: `com.datastax.spark:spark-cassandra-connector_2.12:3.2.0`
- Cần đảm bảo Cassandra đã chạy và schema đã được tạo (xem Bước 3)
- Connector Cassandra sẽ được tự động download lần đầu (có thể mất vài phút)

**Kiểm tra dữ liệu đã ghi vào Cassandra:**

Sau khi job chạy một lúc, mở terminal khác và kiểm tra:
```bash
docker exec -it cassandra cqlsh

USE air_quality;
SELECT * FROM realtime_data LIMIT 10;
```

### Bước 6: Kiểm tra dữ liệu

#### Nếu dùng Console mode:
- Dữ liệu sẽ hiển thị trực tiếp trong terminal của Spark job

#### Nếu dùng Cassandra mode:
```bash
docker exec -it cassandra cqlsh

USE air_quality;
SELECT * FROM realtime_data LIMIT 10;
```

## 🎯 Các bước tiếp theo (tùy chọn)

### 1. Tạo Dashboard

Theo README, bạn có thể tạo dashboard với:
- **Streamlit**: `dashboard/streamlit_app.py`
- **Grafana**: Kết nối với Cassandra
- **Kibana**: Nếu dùng Elasticsearch

### 2. Tạo Alert System

Tạo module cảnh báo khi AQI vượt ngưỡng:
- `alert/alert_rules.py` - Định nghĩa quy tắc cảnh báo
- `alert/alert_consumer.py` - Consumer đọc từ Kafka và phát cảnh báo

### 3. Cải thiện Producer

- Thêm nhiều pollutants (PM10, NO2, O3, etc.)
- Tăng tốc độ streaming (hiện tại 5 giây/record)
- Thêm error handling và retry logic

### 4. Tối ưu Spark Job

- Thêm xử lý cho nhiều pollutants
- Tính AQI tổng hợp (max của tất cả pollutants)
- Thêm windowing và aggregation

## 🔍 Troubleshooting

### Kafka không kết nối được
```bash
# Kiểm tra Kafka đang chạy
docker logs kafka

# Test kết nối
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:29092 --topic test
```

### Cassandra không kết nối được
```bash
# Kiểm tra Cassandra status
docker exec -it cassandra nodetool status

# Kiểm tra logs
docker logs cassandra
```

### Spark không tìm thấy packages
- Đảm bảo có internet để download packages
- Hoặc download trước và đặt vào `--jars` option

## 📚 Tài liệu tham khảo

- README.md - Tổng quan về dự án
- `docs/aqi_formula_vn.md` - Công thức tính AQI Việt Nam (nếu có)

