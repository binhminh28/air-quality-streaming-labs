from cassandra.cluster import Cluster
from datetime import datetime
import polars as pl
import os
import sys
import argparse

# Cấu hình Cassandra
HOSTS = ['localhost']
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
TABLE = os.getenv("CASSANDRA_TABLE", "realtime_data")

def analyze(limit=5000, output_file=None):
    print(f"🔍 [BENCHMARK ANALYZER] Fetching last {limit} records...")
    
    try:
        cluster = Cluster(HOSTS, port=9042)
        session = cluster.connect(KEYSPACE)
        # Chỉ lấy 2 cột cần thiết để giảm tải mạng
        query = f"SELECT datetime, processed_at FROM {TABLE} LIMIT {limit}"
        rows = session.execute(query)
    except Exception as e:
        sys.exit(f"❌ Lỗi Cassandra: {e}")

    # 1. Tiền xử lý dữ liệu thô
    data_list = []
    for row in rows:
        if row.datetime and row.processed_at:
            try:
                # row.datetime là String (từ Producer), row.processed_at là datetime object (từ Spark)
                # Parse string ISO sang datetime và bỏ timezone để trừ
                event_time = datetime.fromisoformat(row.datetime).replace(tzinfo=None)
                proc_time = row.processed_at.replace(tzinfo=None)
                
                latency = (proc_time - event_time).total_seconds()
                
                # Lọc nhiễu: chỉ lấy giá trị dương và < 5 phút
                if 0 <= latency < 300:
                    data_list.append({
                        "event_time": event_time,
                        "latency": latency
                    })
            except Exception:
                continue
    
    cluster.shutdown()

    if not data_list:
        print("⚠️ Không có dữ liệu hợp lệ (hoặc dữ liệu quá cũ/lệch giờ).")
        return

    # 2. Chuyển sang Polars DataFrame
    df = pl.DataFrame(data_list)
    
    # 3. Tính toán thống kê bằng Polars Expressions
    stats = df.select([
        pl.col("latency").count().alias("count"),
        pl.col("latency").min().alias("min"),
        pl.col("latency").max().alias("max"),
        pl.col("latency").mean().alias("avg"),
        pl.col("latency").std().alias("std_dev"),   # Độ lệch chuẩn (Jitter)
        pl.col("latency").median().alias("p50"),    # Trung vị
        pl.col("latency").quantile(0.95).alias("p95"),
        pl.col("latency").quantile(0.99).alias("p99") # Tail Latency
    ])

    # Hiển thị kết quả
    print("\n" + "="*50)
    print("📊 KẾT QUẢ PHÂN TÍCH HIỆU NĂNG (SYSTEM LATENCY)")
    print("="*50)
    res = stats.to_dicts()[0]
    
    print(f"🔹 Mẫu thử (Sample):    {res['count']} records")
    print(f"🔹 Min / Max:           {res['min']:.4f}s / {res['max']:.4f}s")
    print(f"🔹 Trung bình (Avg):    {res['avg']:.4f}s")
    print(f"🔹 Trung vị (P50):      {res['p50']:.4f}s")
    print("-" * 25)
    print(f"🔸 P95 (Tail Latency):  {res['p95']:.4f}s")
    print(f"🔸 P99 (Critical Tail): {res['p99']:.4f}s")
    print(f"🔸 Độ ổn định (StdDev): {res['std_dev']:.4f}s")
    print("="*50)

    # 4. Xuất file CSV nếu cần
    if output_file:
        df.write_csv(output_file)
        print(f"💾 Đã lưu raw data vào: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5000)
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()
    analyze(limit=args.limit, output_file=args.output)