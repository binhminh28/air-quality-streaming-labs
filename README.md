# TÊN DỰ ÁN: HỆ THỐNG GIÁM SÁT CHẤT LƯỢNG KHÔNG KHÍ THỜI GIAN THỰC (REAL-TIME AIR QUALITY MONITORING SYSTEM)

## 1. Giới thiệu tổng quan

Dự án này là một hệ thống xử lý dữ liệu lớn (Big Data) được thiết kế để giám sát, phân tích và cảnh báo chất lượng không khí theo thời gian thực. Hệ thống mô phỏng trọn vẹn quy trình kỹ thuật dữ liệu (Data Engineering) từ khâu thu thập, xử lý đến hiển thị, áp dụng các tiêu chuẩn tính toán chỉ số AQI thực tế của Việt Nam (QCVN 05:2013/BTNMT).

Hệ thống được xây dựng để giải quyết bài toán cốt lõi của Big Data, thỏa mãn mô hình 3V:

* **Volume (Khối lượng):** Khả năng xử lý lượng lớn dữ liệu log từ các trạm quan trắc (được mô phỏng).
* **Velocity (Tốc độ):** Yêu cầu xử lý và tính toán chỉ số AQI gần như tức thời (Real-time) ngay khi dữ liệu được sinh ra.
* **Variety (Đa dạng):** Xử lý dữ liệu hỗn hợp gồm chuỗi thời gian (time-series), các chỉ số hóa học (PM2.5, PM10, v.v.) và thông tin định danh trạm.

---

## 2. Kiến trúc hệ thống (System Architecture)

Dự án áp dụng kiến trúc **Stream Processing Pipeline** hiện đại, đảm bảo tính ổn định và khả năng mở rộng. Hệ thống bao gồm 4 tầng chính:

**Tầng 1: Thu thập dữ liệu (Ingestion Layer)**

* **Công nghệ:** Apache Kafka.
* **Chức năng:** Đóng vai trò là bộ đệm trung gian (Message Broker). Kafka tiếp nhận dữ liệu thô từ các trạm cảm biến (Producer) và lưu trữ tạm thời. Việc sử dụng Kafka giúp tách biệt (decouple) nguồn phát dữ liệu và bộ xử lý, đảm bảo hệ thống không bị nghẽn (backpressure) khi lưu lượng dữ liệu tăng đột biến.

**Tầng 2: Xử lý dữ liệu (Processing Layer)**

* **Công nghệ:** Apache Spark Structured Streaming.
* **Chức năng:** Đây là "bộ não" của hệ thống. Spark đọc dữ liệu liên tục từ Kafka, thực hiện việc làm sạch, kiểm tra định dạng (Schema validation) và áp dụng thuật toán tính toán AQI. Spark hoạt động theo cơ chế Micro-batch, giúp cân bằng giữa độ trễ thấp và năng lực xử lý lượng lớn dữ liệu.

**Tầng 3: Lưu trữ (Storage Layer)**

* **Công nghệ:** Apache Cassandra.
* **Chức năng:** Lưu trữ dữ liệu sau khi đã xử lý. Cassandra được lựa chọn vì đây là cơ sở dữ liệu NoSQL tối ưu cho việc ghi dữ liệu liên tục với tốc độ cao (Write-heavy) và truy vấn theo chuỗi thời gian, phù hợp với đặc thù dữ liệu cảm biến.

**Tầng 4: Hiển thị & Cảnh báo (Serving Layer)**

* **Công nghệ:** Streamlit & WebSocket.
* **Chức năng:** Cung cấp giao diện trực quan cho người dùng cuối. Dashboard kết nối qua WebSocket để nhận dữ liệu mới nhất từ hệ thống và vẽ biểu đồ biến thiên chất lượng không khí, đồng thời hiển thị cảnh báo màu sắc tương ứng với mức độ ô nhiễm.

---

## 3. Luồng dữ liệu (Data Flow)

Hành trình của một gói tin dữ liệu trong hệ thống diễn ra qua 5 bước chặt chẽ:

**1. Mô phỏng nguồn tin (Simulation Source):**
* Chương trình `producer.py` đọc tuần tự các dòng dữ liệu lịch sử từ file Parquet.
* Nó đóng vai trò như một "Sensor ảo", chèn timestamp hiện tại vào bản ghi để giả lập dữ liệu mới được sinh ra ngay tức thì (Real-time injection).


**2. Đệm dữ liệu (Message Queuing):**
* Dữ liệu được chuyển đổi (Serialize) sang định dạng JSON và đẩy vào Kafka topic `air_quality_realtime`.
* Kafka giữ vai trò bộ đệm, đảm bảo dữ liệu không bị mất nếu bộ xử lý phía sau bị quá tải.


**3. Xử lý luồng (Stream Processing):**
* Spark Streaming Job (`streaming_job.py`) liên tục lắng nghe Topic.
* **Parse & Validate:** Chuyển đổi JSON binary thành DataFrame có cấu trúc (Schema) định sẵn.
* **Business Logic:** Áp dụng UDF để tính AQI cho từng chỉ số (PM2.5, PM10), sau đó dùng thuật toán `max()` để lấy chỉ số AQI tổng hợp cuối cùng theo chuẩn QCVN 05:2013.
* **Gán nhãn:** Phân loại chất lượng (Tốt/Trung bình/Kém...) dựa trên chỉ số AQI vừa tính.


**4. Lưu trữ (Persistence):**
* Spark gom các kết quả xử lý lại và sử dụng kỹ thuật `foreachBatch` để ghi hàng loạt (Bulk Insert) vào bảng `realtime_data` trong Cassandra.


**5. Phân phối & Hiển thị (Serving & Visualization):**
* **API Layer (Quan trọng):** Một `websocket_server.py` chạy ngầm, đóng vai trò là Backend API. Nó thực hiện các truy vấn hiệu quả vào Cassandra để lấy dữ liệu mới nhất (Top N records).
* **Frontend:** Ứng dụng `streamlit_app.py` hoạt động theo cơ chế Polling (định kỳ gửi request) tới API Server trên để lấy dữ liệu JSON và vẽ lại biểu đồ mà không cần tải lại trang.

---

## 4. Các khái niệm & Quyết định kỹ thuật quan trọng

Trong quá trình xây dựng, nhóm phát triển đã đưa ra các quyết định kỹ thuật dựa trên đặc thù của dự án:

**Tại sao chọn Parquet thay vì CSV để giả lập dữ liệu?**
Parquet là định dạng lưu trữ dạng cột (Columnar Storage). Trong môi trường Big Data, Parquet cho tốc độ đọc nhanh hơn CSV gấp nhiều lần và quan trọng hơn là nó giữ nguyên được kiểu dữ liệu (Schema). Điều này giúp việc mô phỏng dữ liệu đầu vào chính xác và hiệu quả hơn.

**Tại sao sử dụng Spark Structured Streaming?**
Thay vì mô hình Streaming cũ (DStream), Structured Streaming cho phép làm việc với dữ liệu stream như một bảng vô hạn (Unbounded Table). Điều này giúp code dễ đọc hơn, dễ bảo trì hơn và tận dụng được sức mạnh tối ưu hóa của Spark SQL Engine. Ngoài ra, nó hỗ trợ "Exactly-once semantics", đảm bảo mỗi bản ghi chỉ được xử lý đúng một lần, tránh sai lệch số liệu.

**Cơ chế tính toán AQI (QCVN 05:2013/BTNMT)**
Hệ thống không sử dụng công thức AQI của Mỹ hay Châu Âu mà áp dụng Quy chuẩn kỹ thuật quốc gia của Việt Nam. Công thức này tính toán dựa trên nồng độ bụi trong khoảng thời gian nhất định, sử dụng phương pháp nội suy tuyến tính giữa các điểm cận (breakpoints) để ra chỉ số cuối cùng.

---

## 5. Hướng dẫn cài đặt và vận hành (Automated Deployment)

Hệ thống sử dụng các script tự động hóa để đơn giản hóa quy trình. Quy trình chuẩn bao gồm 3 giai đoạn: **Hạ tầng -> Khởi tạo -> Ứng dụng**.

### Yêu cầu tiên quyết (Prerequisites)

* **Docker & Docker Compose** (đã cài đặt).
* **Python 3.8+** (đã tạo môi trường ảo `.venv`).

### Quy trình khởi chạy

**Bước 1: Khởi động Hạ tầng (Infrastructure)**
Trước hết, cần dựng các container Kafka, Zookeeper và Cassandra. Script khởi chạy ứng dụng sẽ thất bại nếu bước này chưa hoàn tất.

```bash
# Di chuyển vào thư mục docker
cd docker
docker-compose up -d
# Quay lại thư mục gốc
cd ..

```

**Bước 2: Khởi tạo Dữ liệu (Setup Data)**
Chờ khoảng 30-60 giây để Cassandra khởi động xong, sau đó chạy các lệnh sau để tạo Topic và Bảng dữ liệu:

```bash
# 1. Tạo Kafka Topic
bash scripts/create_topics.sh

# 2. Khởi tạo Cassandra Schema
docker exec -it cassandra cqlsh -f /scripts/init_cassandra.cql

```

**Bước 3: Khởi chạy Ứng dụng (Start Backend)**
Sử dụng script `start_all.sh`. Script này sẽ tự động chạy ngầm (background) 3 dịch vụ cốt lõi:

1. **WebSocket Server**: Cầu nối dữ liệu cho Dashboard.
2. **Producer**: Bắt đầu mô phỏng gửi dữ liệu sensor.
3. **Spark Streaming**: Bắt đầu xử lý luồng dữ liệu và ghi vào DB.

```bash
bash start_all.sh

```

*Bạn có thể kiểm tra trạng thái các dịch vụ qua file log trong thư mục `logs/`.*

**Bước 4: Mở Dashboard (Frontend)**
Cuối cùng, khởi chạy giao diện người dùng Streamlit:

```bash
streamlit run dashboard/streamlit_app.py

```

*Truy cập Dashboard tại:* `http://localhost:8501`

---

### Cách dừng hệ thống

Để dừng tất cả các tiến trình Python (Producer, Spark, WebSocket) và dọn dẹp PID:

```bash
bash stop_all.sh

```

*Lưu ý: Lệnh này không tắt Docker containers. Nếu muốn tắt hẳn hạ tầng, hãy dùng `docker-compose down` trong thư mục docker.*

---

## 6. Cấu trúc thư mục dự án (Project Structure)

Dự án được tổ chức theo từng module chức năng, tách biệt rõ ràng giữa cấu hình hạ tầng, mã nguồn xử lý và giao diện người dùng.

```text
air-quality-streaming-labs/
│
├── 📂 data/                        # Chứa dữ liệu đầu vào cho mô phỏng
│   └── processed/
│       └── air_quality_merged.parquet  # File Parquet chứa dữ liệu lịch sử đã làm sạch, dùng để giả lập stream
│
├── 📂 docker/                      # Cấu hình hạ tầng container hóa
│   └── docker-compose.yml          # Định nghĩa các service: Kafka, Zookeeper, Cassandra
│
├── 📂 kafka/                       # Module Ingestion (Thu thập dữ liệu)
│   ├── producer.py                 # Script Python đọc file Parquet và gửi message vào Kafka (giả lập sensor)
│   └── consumer.py                 # Script debug để kiểm tra xem dữ liệu đã vào Kafka chưa
│
├── 📂 spark/                       # Module Processing (Xử lý dữ liệu)
│   ├── streaming_job.py            # Spark Job chính: Đọc Kafka -> Tính AQI -> Ghi xuống Cassandra
│   └── sink_cassandra.py           # Module hỗ trợ ghi dữ liệu vào Cassandra (Batch writer)
│
├── 📂 dashboard/                   # Module Visualization (Hiển thị)
│   ├── streamlit_app.py            # Ứng dụng Web hiển thị biểu đồ và cảnh báo Real-time
│   ├── websocket_server.py         # Server trung gian chuyển tiếp dữ liệu từ Backend lên Frontend
│   └── README.md                   # Hướng dẫn riêng cho phần Dashboard
│
├── 📂 scripts/                     # Các công cụ tiện ích (Utilities) & Setup
│   ├── create_topics.sh            # Script tạo Kafka topic (air_quality_realtime)
│   ├── init_cassandra.cql          # Script CQL khởi tạo Keyspace và Table trong Cassandra
│   ├── update_cassandra_schema.sh  # Script cập nhật schema DB khi có thay đổi
│   ├── data_preprocessing.py       # Script tiền xử lý dữ liệu thô ban đầu (ETL offline)
│   └── debug_system.py             # Script kiểm tra sức khỏe hệ thống (Health check)
│
├── 📂 logs/                        # Nơi lưu trữ log hoạt động của hệ thống
│   ├── spark_streaming.log         # Log lỗi và trạng thái của Spark Job
│   └── websocket_server.log        # Log kết nối của Dashboard
│
├── 📜 Các file quản lý & khởi chạy (Root)
│   ├── start_all.sh                # "One-click" script: Khởi động Docker và tạo môi trường
│   ├── stop_all.sh                 # Dừng và dọn dẹp toàn bộ hệ thống
│   ├── run_spark_cassandra.sh      # Lệnh submit Spark Job lên cluster
│   ├── run_dashboard.sh            # Lệnh khởi chạy Streamlit Dashboard
│   ├── run_websocket_server.sh     # Lệnh khởi chạy WebSocket Server
│   ├── pyproject.toml              # Quản lý dependencies (thư viện Python) bằng Poetry
│   └── poetry.lock                 # File khóa phiên bản thư viện để đảm bảo đồng bộ môi trường

```

### Giải thích chi tiết các thành phần chính:

1. **`docker/docker-compose.yml`**: Đây là bản thiết kế hạ tầng. Nó quy định Kafka chạy port 9092, Cassandra chạy port 9042 và Zookeeper quản lý Kafka.
2. **`kafka/producer.py`**: Thay vì chờ dữ liệu từ thiết bị thật, file này đóng vai trò "máy phát", đọc dữ liệu lịch sử từ folder `data/` và bắn vào hệ thống với tốc độ tùy chỉnh (ví dụ: 1 giây/bản ghi) để test khả năng chịu tải.
3. **`spark/streaming_job.py`**: Trái tim của hệ thống. File này chứa logic nghiệp vụ:
* Định nghĩa Schema cho dữ liệu JSON đầu vào.
* Chứa hàm UDF (User Defined Function) để tính toán chỉ số AQI theo chuẩn Việt Nam.
* Điều phối luồng dữ liệu từ Kafka sang Cassandra.


4. **`scripts/init_cassandra.cql`**: File định nghĩa cấu trúc dữ liệu lưu trữ (Data Model). Nó tạo bảng `realtime_data` với khóa chính phù hợp cho việc truy vấn theo thời gian.
5. **`start_all.sh`**: Script tự động hóa quy trình triển khai (DevOps), giúp người dùng mới không cần gõ từng lệnh Docker phức tạp mà chỉ cần chạy một file duy nhất để dựng môi trường.