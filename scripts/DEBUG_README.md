# Debug System Tool

Script này giúp kiểm tra toàn bộ hệ thống Air Quality Streaming để tìm ra vấn đề.

## Cách sử dụng

```bash
# Chạy script debug
python scripts/debug_system.py
```

Hoặc:

```bash
cd /home/azureuser/air-quality-streaming-labs
python3 scripts/debug_system.py
```

## Script sẽ kiểm tra:

1. **Docker Services**: Zookeeper, Kafka, Cassandra có đang chạy không
2. **Kafka Topic**: Topic `air_quality_realtime` có tồn tại và có messages không
3. **Kafka Producer**: Producer có đang chạy và gửi dữ liệu không
4. **Spark Streaming Job**: Spark job có đang chạy và xử lý dữ liệu không
5. **Cassandra Connection**: Có thể kết nối đến Cassandra không
6. **Cassandra Data**: Có dữ liệu trong Cassandra không, dữ liệu có được cập nhật không
7. **Dashboard Connection**: Dashboard có thể đọc dữ liệu từ Cassandra không
8. **Streamlit Dashboard**: Streamlit có đang chạy không
9. **Monitor Updates**: Monitor xem có dữ liệu mới được thêm vào trong 30 giây không

## Output

Script sẽ hiển thị:
- ✅ (màu xanh): Component hoạt động tốt
- ❌ (màu đỏ): Component có vấn đề
- ⚠️ (màu vàng): Cảnh báo
- ℹ️ (màu xanh dương): Thông tin

## Ví dụ output

```
============================================================
  AIR QUALITY STREAMING SYSTEM - DEBUG TOOL
============================================================

ℹ️  Thời gian chạy: 2024-12-31 18:30:00

============================================================
1. KIỂM TRA DOCKER SERVICES
============================================================

✅ Zookeeper: zookeeper - Up 2 hours
✅ Kafka: kafka - Up 2 hours
✅ Cassandra: cassandra - Up 2 hours

============================================================
2. KIỂM TRA KAFKA TOPIC
============================================================

✅ Topic 'air_quality_realtime' tồn tại
ℹ️  Tổng số messages trong topic: 150
✅ Message mới nhất: {"datetime": "2024-12-31T18:30:00", "pm25": 18.5}

...
```

## Gửi log về

Sau khi chạy script, copy toàn bộ output và gửi về để phân tích.

## Troubleshooting

Nếu script báo lỗi, làm theo các bước:

1. **Docker services không chạy:**
   ```bash
   cd docker && docker-compose up -d
   ```

2. **Kafka topic không tồn tại:**
   ```bash
   bash scripts/create_topics.sh
   ```

3. **Producer không chạy:**
   ```bash
   python kafka/producer.py
   ```

4. **Spark Streaming không chạy:**
   ```bash
   bash run_spark_cassandra.sh
   ```

5. **Cassandra schema chưa tạo:**
   ```bash
   docker exec -it cassandra cqlsh -f /scripts/init_cassandra.cql
   ```

6. **Streamlit không chạy:**
   ```bash
   streamlit run dashboard/streamlit_app.py
   ```

