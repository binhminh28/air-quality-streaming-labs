# Fix Cassandra Schema

## Vấn đề

Khi chạy Spark Streaming, lỗi xuất hiện:
```
Columns not found in table air_quality.realtime_data: location_id, pm10, pm1, temperature, relativehumidity, um003, aqi_pm25, aqi_pm10, Quality
```

## Giải pháp

### 1. Cập nhật Cassandra Schema

Chạy script để thêm các cột mới:

```bash
bash scripts/update_cassandra_schema.sh
```

Hoặc chạy thủ công:

```bash
docker exec cassandra cqlsh -e "
USE air_quality;
ALTER TABLE realtime_data ADD IF NOT EXISTS location_id BIGINT;
ALTER TABLE realtime_data ADD IF NOT EXISTS pm10 FLOAT;
ALTER TABLE realtime_data ADD IF NOT EXISTS pm1 FLOAT;
ALTER TABLE realtime_data ADD IF NOT EXISTS temperature FLOAT;
ALTER TABLE realtime_data ADD IF NOT EXISTS relativehumidity FLOAT;
ALTER TABLE realtime_data ADD IF NOT EXISTS um003 FLOAT;
ALTER TABLE realtime_data ADD IF NOT EXISTS aqi_pm25 INT;
ALTER TABLE realtime_data ADD IF NOT EXISTS aqi_pm10 INT;
"
```

### 2. Xóa dữ liệu cũ (nếu cần)

Nếu muốn bắt đầu lại từ đầu:

```bash
docker exec cassandra cqlsh -e "USE air_quality; TRUNCATE realtime_data;"
```

### 3. Restart Spark Streaming

Sau khi cập nhật schema, restart Spark Streaming:

```bash
bash stop_all.sh
bash start_all.sh
```

## Kiểm tra Schema

Để xem schema hiện tại:

```bash
docker exec cassandra cqlsh -e "USE air_quality; DESCRIBE TABLE realtime_data;"
```

Schema đúng phải có các cột:
- datetime (PRIMARY KEY)
- location_id
- pm25, pm10, pm1
- temperature
- relativehumidity
- um003
- aqi, aqi_pm25, aqi_pm10
- quality
- processed_at

