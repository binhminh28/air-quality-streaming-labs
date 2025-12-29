# Streamlit Dashboard for Real-time Air Quality Monitoring

Dashboard này hiển thị dữ liệu real-time từ Cassandra thông qua WebSocket server.

## Cách chạy

### 1. Cài đặt dependencies

```bash
poetry install
```

Hoặc:

```bash
pip install streamlit pandas plotly cassandra-driver websockets fastapi uvicorn requests
```

### 2. Khởi động WebSocket Server

```bash
bash run_websocket_server.sh
```

Hoặc:

```bash
python dashboard/websocket_server.py
```

Server sẽ chạy trên:
- HTTP API: `http://localhost:8765/api/data`
- WebSocket: `ws://localhost:8765/ws`

### 3. Chạy Dashboard

Trong terminal khác:

```bash
streamlit run dashboard/streamlit_app.py
```

Dashboard sẽ mở tại `http://localhost:8501`

## Kiến trúc

```
Cassandra → WebSocket Server → Dashboard
```

1. **Cassandra**: Lưu trữ dữ liệu đã xử lý từ Spark Streaming
2. **WebSocket Server**: Đọc dữ liệu từ Cassandra và cung cấp qua HTTP API và WebSocket
3. **Dashboard**: Hiển thị dữ liệu real-time, tự động refresh mỗi 5 giây

## Tính năng

- **Real-time Metrics**: Hiển thị AQI, PM2.5, và chất lượng không khí hiện tại
- **Time-series Charts**: Biểu đồ AQI và PM2.5 theo thời gian
- **Distribution Analysis**: Histogram và pie chart phân bố dữ liệu
- **Auto-refresh**: Tự động cập nhật mỗi 5 giây
- **Latest Data Table**: Bảng dữ liệu mới nhất

## Troubleshooting

- **"Chưa có dữ liệu"**: Đảm bảo WebSocket server đang chạy và Cassandra có dữ liệu
- **"Connection error"**: Kiểm tra WebSocket server có đang chạy không: `curl http://localhost:8765/health`
- **"Lỗi parse datetime"**: Đã được sửa bằng cách sử dụng format='ISO8601'
