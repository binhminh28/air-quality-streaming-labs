import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import argparse
import os
import sys

def draw_charts(csv_file):
    if not os.path.exists(csv_file):
        sys.exit(f"❌ File không tồn tại: {csv_file}")

    print(f"📈 Đang xử lý biểu đồ từ: {csv_file}")
    
    # 1. Đọc dữ liệu bằng Polars
    try:
        df = pl.read_csv(csv_file)
    except Exception as e:
         sys.exit(f"❌ Lỗi đọc CSV: {e}")
    
    # Tính toán lại các chỉ số để vẽ đường tham chiếu
    p95 = df["latency"].quantile(0.95)
    avg = df["latency"].mean()

    # 2. Tạo Layout 2 biểu đồ
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Phân bố độ trễ (Latency Histogram)", "Độ trễ theo thời gian (Latency Time Series)"),
        vertical_spacing=0.15
    )

    # --- Biểu đồ 1: Histogram ---
    fig.add_trace(
        go.Histogram(
            x=df["latency"].to_list(),
            nbinsx=50,
            name="Tần suất",
            marker_color='#1f77b4',
            opacity=0.75
        ),
        row=1, col=1
    )
    # Đường P95
    fig.add_vline(x=p95, line_width=2, line_dash="dash", line_color="red", 
                  annotation_text=f"P95: {p95:.2f}s", row=1, col=1)

    # --- Biểu đồ 2: Time Series ---
    # Vẽ theo thứ tự mẫu (Sample Index)
    fig.add_trace(
        go.Scatter(
            y=df["latency"].to_list(),
            mode='lines',
            name="Latency (s)",
            line=dict(color='#2ca02c', width=1),
            opacity=0.8
        ),
        row=2, col=1
    )
    # Đường trung bình
    fig.add_hline(y=avg, line_width=2, line_color="orange", 
                  annotation_text=f"Avg: {avg:.2f}s", row=2, col=1)

    # 3. Style
    mode_name = "STRESS TEST" if "stress" in csv_file else "BENCHMARK"
    fig.update_layout(
        title_text=f"Báo cáo Hiệu năng Hệ thống ({mode_name})",
        height=800,
        showlegend=False,
        template="plotly_white"
    )
    
    fig.update_xaxes(title_text="Độ trễ (giây)", row=1, col=1)
    fig.update_yaxes(title_text="Số lượng bản ghi", row=1, col=1)
    fig.update_xaxes(title_text="Thứ tự bản ghi", row=2, col=1)
    fig.update_yaxes(title_text="Độ trễ (giây)", row=2, col=1)

    # 4. Xuất HTML Report
    output_html = csv_file.replace(".csv", "_report.html")
    fig.write_html(output_html)
    print(f"✅ Đã tạo báo cáo: {output_html}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_file", help="Đường dẫn file CSV kết quả")
    args = parser.parse_args()
    draw_charts(args.csv_file)