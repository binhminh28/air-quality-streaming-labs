# Real-time Air Quality Monitoring using Apache Kafka & Spark Structured Streaming

## 1. Giới thiệu

Dự án này xây dựng **hệ thống phân tích chất lượng không khí theo thời gian thực** dựa trên các nguyên lý và công nghệ cốt lõi của **Big Data**, bao gồm:

* **Streaming Ingestion** với Apache Kafka
* **Distributed Stream Processing** với Apache Spark Structured Streaming
* **Columnar Storage** bằng Parquet (tối ưu cho hệ thống Big Data)
* **Scalable Storage Layer**: PostgreSQL / Cassandra / Elasticsearch
* **Realtime Dashboard**: Grafana / Kibana / Streamlit
* **Alerting System** khi AQI vượt ngưỡng

Dự án tuân theo bản chất của Big Data, thể hiện rõ 3V mở rộng:

* **Volume:** dữ liệu cảm biến lớn, liên tục tăng theo thời gian
* **Velocity:** yêu cầu xử lý *real-time*
* **Variety:** hỗn hợp các chỉ số môi trường (pm2.5, pm10, no2, o3, humidity…)

---

## 2. Bản chất Big Data áp dụng trong dự án

Dựa trên lý thuyết môn học:

### 2.1. **Volume – Khối lượng**

Dữ liệu cảm biến được mô phỏng liên tục → số lượng lớn → phù hợp mô hình xử lý phân tán Spark.

### 2.2. **Velocity – Tốc độ**

Dữ liệu không khí cần cập nhật real-time → Kafka + Spark Streaming là lựa chọn tiêu chuẩn, giống như trong hệ thống IoT thực tế.

### 2.3. **Variety – Đa dạng**

Các chỉ số môi trường gồm nhiều loại dữ liệu:

* PM2.5, PM10 → numerical
* timestamp → time-series
* humidity, temperature → environmental context

### 2.4. **Hệ sinh thái Big Data được sử dụng**

Dựa trên nội dung trong giáo trình Hadoop–Spark:

* **Kafka** → công cụ ingest streaming (tương tự Flume/Sqoop nhưng cho real-time).
* **Spark Structured Streaming** → engine xử lý phân tán theo mô hình micro-batch.
* **Parquet** → định dạng lưu trữ cột, tối ưu cho Big Data (thay vì CSV).
* **PostgreSQL/Cassandra/Elasticsearch** → hệ thống lưu trữ phục vụ query hoặc dashboard.

### 2.5. **Tư duy Lambda Architecture**

Hệ thống đã thể hiện rõ 3 lớp:

* **Speed layer** → Spark Streaming xử lý real-time
* **Batch layer** → Parquet lưu trữ dữ liệu dạng bulk (phù hợp cho phân tích lại)
* **Serving layer** → PostgreSQL / Cassandra / Elasticsearch

---

## 3. Kiến trúc hệ thống

```
                 +--------------------------------------+
                 |      Distributed Batch Data Source    |
                 |   Parquet Files (mô phỏng sensor)     |
                 +-------------------+--------------------+
                                     |
                    (1) Producer đọc từng bản ghi & gửi Kafka
                                     |
                                     v
         +-----------------------------------------------------------+
         |                Apache Kafka Cluster (Distributed)         |
         |  - Nhiều broker                                          |
         |  - Topic: air_quality_raw (partitioned)                  |
         |  - Bảo đảm throughput cao và khả năng scale-out           |
         +-----------------------+-----------------------------------+
                                     |
                    (2) Spark Structured Streaming đọc partition song song
                                     |
                                     v
        +----------------------------------------------------------------+
        |              Spark Streaming Cluster (Distributed Compute)     |
        |  - Driver + nhiều Executor                                     |
        |  - Tiền xử lý dữ liệu                                          |
        |  - Tính AQI theo chuẩn Việt Nam                               |
        |  - Gắn nhãn chất lượng không khí                               |
        |  - Xử lý phân tán theo partition (parallel processing)         |
        +------------------------+---------------------------------------+
                                     |
                     (3) Ghi kết quả đã xử lý vào Storage phân tán
                                     |
                                     v
   +--------------------------- Distributed Storage Layer --------------------------+
   |  PostgreSQL (single-node) / Cassandra Cluster (distributed) / Elasticsearch   |
   |  - Lưu trữ dữ liệu đã xử lý                                                  |
   |  - Hỗ trợ truy vấn, phân tích và dashboard                                    |
   +-------------------------------------------------------------------------------+
                                     |
                                     v
        +-------------------------------------------------+
        |              Realtime Dashboard Layer           |
        |  Grafana / Kibana / Streamlit                   |
        |  - Hiển thị AQI real-time                       |
        |  - Heatmap các pollutant                        |
        |  - Cảnh báo theo thời gian thực                 |
        +-------------------------------------------------+

```

---

## 4. Lý do sử dụng Parquet thay vì CSV

Theo nội dung Big Data trong bài giảng (Hadoop/Spark):

| Tiêu chí             | CSV          | Parquet                  |
| -------------------- | ------------ | ------------------------ |
| Kiến trúc            | Row-based    | Columnar-based           |
| Phù hợp Big Data     | Không tối ưu | **Rất tối ưu**           |
| Tốc độ đọc           | Chậm         | **Nhanh gấp nhiều lần**  |
| Hỗ trợ schema        | Không        | **Có schema & metadata** |
| Dễ tương thích Spark | Trung bình   | **Tích hợp gốc**         |
| Dễ mô phỏng sensor   | Có           | **Rất tốt**              |

**Kết luận:** Parquet phản ánh đúng bản chất Big Data và tối ưu cho Spark → hoàn toàn phù hợp để mô phỏng dữ liệu sensor.

---

## 5. Các thành phần chính

### 5.1. Kafka – Ingestion Layer

* Producer đọc dữ liệu từ file Parquet
* Gửi từng bản ghi vào Kafka topic `air_quality_raw`
* Mô phỏng tốc độ 1 record/giây như sensor thực

### 5.2. Spark Structured Streaming – Processing Layer

* Nhận dữ liệu từ Kafka
* Parse JSON → xử lý → tính AQI Việt Nam (QCVN 05:2013)
* Gắn nhãn: Tốt / Trung bình / Kém / Xấu / Nguy hại
* Ghi kết quả vào DB

### 5.3. Storage Layer

Tùy chọn:

* PostgreSQL → dễ trình bày, dễ truy vấn
* Cassandra → align với triết lý Big Data (phân tán, scalable)
* Elasticsearch → tốt cho dashboard

### 5.4. Dashboard

* Biểu đồ AQI theo thời gian
* Heatmap theo pollutant
* Cảnh báo real-time

---

## 6. Cấu trúc thư mục dự án

```
air-quality-streaming-project/
│
├── docs/
│   ├── report.md
│   ├── architecture.png
│   ├── aqi_formula_vn.md
│   └── bigdata_explanation.md
│
├── data/
│   └── air_quality.parquet            # nguồn dữ liệu mô phỏng (thay CSV)
│
├── kafka/
│   ├── producer_parquet.py            # đọc parquet & gửi kafka
│   ├── producer_api.py
│   └── consumer_debug.py
│
├── spark/
│   ├── streaming_job.py               # pipeline streaming
│   ├── processor.py
│   ├── aqi_calculator.py
│   └── sink_postgres.py
│
├── dashboard/
│   ├── grafana/
│   ├── kibana/
│   └── streamlit_app.py
│
├── alert/
│   ├── alert_rules.py
│   └── alert_consumer.py
│
├── docker/
│   ├── docker-compose.yml
│   └── configs/
│
└── scripts/
    ├── create_topics.sh
    ├── init_postgres.sql
    └── generate_parquet.ipynb        # notebook tạo file parquet mẫu
```

---

## 7. Công thức AQI Việt Nam

Áp dụng theo **QCVN 05:2013/BTNMT**
AQI được tính cho từng pollutant theo hàm tuyến tính:

```
AQI = (I_hi - I_lo) / (BP_hi - BP_lo) * (C - BP_lo) + I_lo
```

AQI cuối cùng = max(AQI của tất cả các pollutant).
Chi tiết: `docs/aqi_formula_vn.md`.

---

## 8. Hướng dẫn chạy

### Bước 1: Start toàn bộ hệ thống

```
docker-compose up -d
```

### Bước 2: Tạo Kafka topic

```
bash scripts/create_topics.sh
```

### Bước 3: Chạy producer Parquet

```
python kafka/producer_parquet.py
```

### Bước 4: Chạy Spark Streaming

```
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark/streaming_job.py
```

### Bước 5: Mở dashboard

* Grafana: [http://localhost:3000](http://localhost:3000)
* Kibana: [http://localhost:5601](http://localhost:5601)
* Streamlit:

  ```
  streamlit run dashboard/streamlit_app.py
  ```

---

## 9. Kết luận

Dự án áp dụng đúng bản chất Big Data:

| Thành phần Big Data           | Công nghệ sử dụng                      |
| ----------------------------- | -------------------------------------- |
| Ingestion tốc độ cao          | Kafka                                  |
| Distributed Stream Processing | Spark Structured Streaming             |
| Columnar Storage              | Parquet                                |
| Scalable Storage              | PostgreSQL / Cassandra / Elasticsearch |
| Realtime Visualization        | Grafana / Kibana / Streamlit           |
| Alerting                      | Kafka + Spark                          |