from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import polars as pl
import os
import sys
import argparse

# Cấu hình Cassandra
HOSTS = ['localhost']
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
TABLE = os.getenv("CASSANDRA_TABLE", "realtime_data")

def analyze(limit=5000, output_file=None, valid_window_minutes=60):
    """
    Phân tích độ trễ của hệ thống.
    :param limit: Số lượng bản ghi tối đa lấy từ DB.
    :param output_file: Đường dẫn file CSV để lưu kết quả (nếu có).
    :param valid_window_minutes: Chỉ chấp nhận dữ liệu trong khoảng thời gian này (tránh tính toán nhầm dữ liệu cũ).
    """
    print(f"🔍 [BENCHMARK ANALYZER] Connecting to Cassandra...")
    print(f"   - Fetching last {limit} records.")
    print(f"   - Valid Time Window: Last {valid_window_minutes} minutes.")

    try:
        cluster = Cluster(HOSTS, port=9042)
        session = cluster.connect(KEYSPACE)
        # Lấy dữ liệu. Lưu ý: Cassandra LIMIT không đảm bảo thứ tự nếu không có WHERE,
        # nên ta sẽ lọc lại bằng Python bên dưới.
        query = f"SELECT datetime, processed_at FROM {TABLE} LIMIT {limit}"
        rows = session.execute(query)
    except Exception as e:
        sys.exit(f"❌ Lỗi kết nối Cassandra: {e}")

    # 1. Tiền xử lý dữ liệu (Client-side Filtering)
    data_list = []
    skipped_old = 0
    skipped_error = 0
    
    # Xác định mốc thời gian hợp lệ (Hiện tại - Window)
    now = datetime.now()
    cutoff_time = now - timedelta(minutes=valid_window_minutes)

    for row in rows:
        if row.datetime and row.processed_at:
            try:
                # Parse event_time (từ Producer - String ISO)
                # Lưu ý: fromisoformat có thể có timezone hoặc không
                event_time = datetime.fromisoformat(row.datetime)
                
                # Parse processed_at (từ Spark - datetime object)
                proc_time = row.processed_at

                # Quy chuẩn về Native Datetime (bỏ múi giờ) để so sánh
                if event_time.tzinfo:
                    event_time = event_time.replace(tzinfo=None)
                if proc_time.tzinfo:
                    proc_time = proc_time.replace(tzinfo=None)

                # --- QUAN TRỌNG: Lọc dữ liệu cũ ---
                if event_time < cutoff_time:
                    skipped_old += 1
                    continue

                # Tính độ trễ
                latency = (proc_time - event_time).total_seconds()
                
                # Lọc nhiễu: chỉ lấy giá trị dương và < 5 phút (tránh clock skew quá lớn)
                if 0 <= latency < 300:
                    data_list.append({
                        "event_time": event_time,
                        "processed_at": proc_time,
                        "latency": latency
                    })
                else:
                    skipped_error += 1
            except Exception:
                skipped_error += 1
                continue
    
    cluster.shutdown()

    # Báo cáo sơ bộ về dữ liệu
    print(f"   - Raw records fetched: {len(list(rows)) if 'rows' in locals() else 'Unknown'}")
    print(f"   - Valid records:       {len(data_list)}")
    print(f"   - Skipped (Old Data):  {skipped_old} (Out of window)")
    print(f"   - Skipped (Invalid):   {skipped_error} (Negative latency/Error)")

    if not data_list:
        print("\n⚠️  KHÔNG CÓ DỮ LIỆU HỢP LỆ ĐỂ PHÂN TÍCH.")
        print("   -> Gợi ý: Hãy kiểm tra lại Timezone hoặc Producer có đang chạy không?")
        return

    # 2. Chuyển sang Polars DataFrame
    df = pl.DataFrame(data_list)
    
    # 3. Tính toán thống kê
    stats = df.select([
        pl.col("latency").count().alias("count"),
        pl.col("latency").min().alias("min"),
        pl.col("latency").max().alias("max"),
        pl.col("latency").mean().alias("avg"),
        pl.col("latency").std().alias("std_dev"),
        pl.col("latency").median().alias("p50"),
        pl.col("latency").quantile(0.95).alias("p95"),
        pl.col("latency").quantile(0.99).alias("p99")
    ])

    # Hiển thị kết quả
    res = stats.to_dicts()[0]
    
    print("\n" + "="*50)
    print("📊 KẾT QUẢ PHÂN TÍCH HIỆU NĂNG (SYSTEM LATENCY)")
    print("="*50)
    print(f"🔹 Mẫu thử (Samples):   {res['count']}")
    print(f"🔹 Min Latency:         {res['min']:.4f} s")
    print(f"🔹 Max Latency:         {res['max']:.4f} s")
    print(f"🔹 Trung bình (Avg):    {res['avg']:.4f} s")
    print("-" * 50)
    print(f"🔸 P50 (Median):        {res['p50']:.4f} s")
    print(f"🔸 P95 (Tail Latency):  {res['p95']:.4f} s")
    print(f"🔸 P99 (Critical):      {res['p99']:.4f} s")
    print(f"🔸 Jitter (StdDev):     {res['std_dev']:.4f} s")
    print("="*50)

    # Cảnh báo hiệu năng
    if res['avg'] > 1.0:
        print("⚠️  CẢNH BÁO: Độ trễ trung bình > 1s. Hệ thống có thể đang quá tải.")
    if res['std_dev'] > 0.5:
        print("⚠️  CẢNH BÁO: Độ ổn định thấp (Jitter cao).")

    # 4. Xuất file CSV nếu cần
    if output_file:
        # Sort theo thời gian trước khi lưu để vẽ biểu đồ đẹp hơn
        df = df.sort("event_time")
        df.write_csv(output_file)
        print(f"\n💾 Đã lưu raw data đã lọc vào: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=5000, help="Số lượng bản ghi tối đa load từ DB")
    parser.add_argument("--output", type=str, default=None, help="Đường dẫn file output CSV")
    parser.add_argument("--window", type=int, default=60, help="Cửa sổ thời gian hợp lệ (phút) so với hiện tại")
    
    args = parser.parse_args()
    analyze(limit=args.limit, output_file=args.output, valid_window_minutes=args.window)