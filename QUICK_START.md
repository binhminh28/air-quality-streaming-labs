# Quick Start Guide

## Khởi động tất cả services

### Cách 1: Sử dụng script tự động (Khuyến nghị)

```bash
# Khởi động tất cả services
bash start_all.sh

# Dừng tất cả services
bash stop_all.sh
```

Script sẽ tự động khởi động:
- ✅ WebSocket Server (port 8765)
- ✅ Kafka Producer
- ✅ Spark Streaming Job

### Cách 2: Khởi động thủ công

```bash
# Terminal 1: WebSocket Server
python dashboard/websocket_server.py

# Terminal 2: Producer
python kafka/producer.py

# Terminal 3: Spark Streaming
bash run_spark_cassandra.sh

# Terminal 4: Dashboard
streamlit run dashboard/streamlit_app.py
```

## Cập nhật Cassandra Schema

Nếu đã có dữ liệu cũ, cần cập nhật schema:

```bash
docker exec -it cassandra cqlsh -f /scripts/init_cassandra.cql
```

Hoặc chạy lại script init:

```bash
docker exec -it cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS air_quality WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE air_quality;
ALTER TABLE realtime_data ADD location_id BIGINT;
ALTER TABLE realtime_data ADD pm10 FLOAT;
ALTER TABLE realtime_data ADD pm1 FLOAT;
ALTER TABLE realtime_data ADD temperature FLOAT;
ALTER TABLE realtime_data ADD relativehumidity FLOAT;
ALTER TABLE realtime_data ADD um003 FLOAT;
ALTER TABLE realtime_data ADD aqi_pm25 INT;
ALTER TABLE realtime_data ADD aqi_pm10 INT;
"
```

## AQI Calculation - QCVN 05:2013/BTNMT

Hệ thống tính AQI theo tiêu chuẩn Việt Nam:

### Các thông số được sử dụng:
- **PM2.5**: Tính AQI riêng cho PM2.5
- **PM10**: Tính AQI riêng cho PM10
- **AQI Tổng hợp**: Lấy giá trị cao nhất giữa AQI PM2.5 và AQI PM10

### Breakpoints PM2.5 (µg/m³):
- 0-25: AQI 0-50 (Tốt)
- 25.1-50: AQI 51-100 (Trung bình)
- 50.1-100: AQI 101-150 (Kém)
- 100.1-150: AQI 151-200 (Xấu)
- 150.1-250: AQI 201-300 (Rất xấu)
- 250.1-350: AQI 301-400 (Nguy hại)
- 350.1-500: AQI 401-500 (Nguy hại)

### Breakpoints PM10 (µg/m³):
- 0-50: AQI 0-50 (Tốt)
- 50.1-100: AQI 51-100 (Trung bình)
- 100.1-200: AQI 101-150 (Kém)
- 200.1-300: AQI 151-200 (Xấu)
- 300.1-400: AQI 201-300 (Rất xấu)
- 400.1-500: AQI 301-400 (Nguy hại)
- 500.1-600: AQI 401-500 (Nguy hại)

### Công thức:
```
AQI = ((I_high - I_low) / (C_high - C_low)) * (C - C_low) + I_low
```

Trong đó:
- C: Nồng độ chất ô nhiễm
- C_low, C_high: Ngưỡng nồng độ
- I_low, I_high: Ngưỡng AQI tương ứng

## Các thuộc tính hiển thị

Dashboard hiển thị:
- **AQI**: Chỉ số chất lượng không khí tổng hợp
- **AQI PM2.5**: AQI riêng cho PM2.5
- **AQI PM10**: AQI riêng cho PM10
- **PM2.5, PM10, PM1**: Nồng độ bụi mịn
- **Temperature**: Nhiệt độ
- **Relative Humidity**: Độ ẩm tương đối
- **Location ID**: ID vị trí đo
- **Quality**: Mức chất lượng (Tốt/Trung bình/Kém/Xấu/Rất xấu/Nguy hại)

## Logs

Logs được lưu trong thư mục `logs/`:
- `logs/websocket_server.log`
- `logs/producer.log`
- `logs/spark_streaming.log`

Xem logs:
```bash
tail -f logs/websocket_server.log
```

