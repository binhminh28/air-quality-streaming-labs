-- Script khởi tạo keyspace và table cho Cassandra
-- Chạy: docker exec -it cassandra cqlsh -f /scripts/init_cassandra.cql

-- Tạo keyspace với replication_factor=2 cho cluster 2 nodes
CREATE KEYSPACE IF NOT EXISTS air_quality
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};

-- Sử dụng keyspace
USE air_quality;

-- Tạo table lưu trữ dữ liệu real-time với đầy đủ thuộc tính
CREATE TABLE IF NOT EXISTS realtime_data (
    datetime TEXT PRIMARY KEY,
    location_id BIGINT,
    pm25 FLOAT,
    pm10 FLOAT,
    pm1 FLOAT,
    temperature FLOAT,
    relativehumidity FLOAT,
    um003 FLOAT,
    aqi INT,
    aqi_pm25 INT,
    aqi_pm10 INT,
    quality TEXT,
    processed_at TIMESTAMP
);

-- Tạo index cho quality để query dễ dàng hơn
CREATE INDEX IF NOT EXISTS ON realtime_data (quality);

-- Tạo index cho processed_at để query theo thời gian
CREATE INDEX IF NOT EXISTS ON realtime_data (processed_at);

-- Tạo index cho location_id
CREATE INDEX IF NOT EXISTS ON realtime_data (location_id);

-- Hiển thị cấu trúc table
DESCRIBE TABLE realtime_data;
