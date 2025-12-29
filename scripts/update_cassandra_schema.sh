#!/bin/bash

echo "🔄 Updating Cassandra schema..."

docker exec cassandra cqlsh -e "
USE air_quality;

-- Thêm các cột mới nếu chưa có
ALTER TABLE realtime_data ADD IF NOT EXISTS location_id BIGINT;
ALTER TABLE realtime_data ADD IF NOT EXISTS pm10 FLOAT;
ALTER TABLE realtime_data ADD IF NOT EXISTS pm1 FLOAT;
ALTER TABLE realtime_data ADD IF NOT EXISTS temperature FLOAT;
ALTER TABLE realtime_data ADD IF NOT EXISTS relativehumidity FLOAT;
ALTER TABLE realtime_data ADD IF NOT EXISTS um003 FLOAT;
ALTER TABLE realtime_data ADD IF NOT EXISTS aqi_pm25 INT;
ALTER TABLE realtime_data ADD IF NOT EXISTS aqi_pm10 INT;

-- Hiển thị schema
DESCRIBE TABLE realtime_data;
"

echo "✅ Schema updated!"

