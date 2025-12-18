import polars as pl
import plotly.express as px
import plotly.graph_objects as go

# 1. Load dữ liệu
df = pl.read_parquet("data/processed/air_quality_5s_noise.parquet")

# 2. LÀM SẠCH: Bỏ cột PM10 (vì thiếu 96%) và um003 (ít dùng)
df_clean = df.drop(["pm10", "um003"])

# 3. TRANSFORMATION: Tính toán phân loại AQI (Đơn giản hóa)
# Công thức: 0-12: Tốt, 12-35: Trung bình, 35-55: Kém, >55: Xấu (Theo quy chuẩn US EPA sơ bộ)
def classify_aqi(pm25):
    if pm25 <= 12.0: return "Good"
    elif pm25 <= 35.4: return "Moderate"
    elif pm25 <= 55.4: return "Unhealthy for Sensitive"
    elif pm25 <= 150.4: return "Unhealthy"
    else: return "Hazardous"

# Áp dụng hàm (Map)
df_final = df_clean.with_columns(
    pl.col("pm25").map_elements(classify_aqi, return_dtype=pl.Utf8).alias("AQI_Category")
)

# Convert sang Pandas để vẽ đồ thị
pdf = df_final.to_pandas()

print("--- Dữ liệu sau khi làm sạch ---")
print(df_final.head())
print(df_final.group_by("AQI_Category").len()) # Thống kê xem bao nhiêu ngày xấu/tốt

# ---------------------------------------------------------
# 4. VẼ BIỂU ĐỒ (VISUALIZATION)
# ---------------------------------------------------------

# Biểu đồ 1: Xu hướng PM2.5 cả năm
fig1 = px.line(pdf, x="datetime", y="pm25", 
              title="Xu hướng Bụi mịn PM2.5 theo thời gian (2025)",
              color_discrete_sequence=["#FF5733"]) # Màu cam đỏ
fig1.add_hline(y=35.4, line_dash="dash", line_color="red", annotation_text="Ngưỡng nguy hại (US EPA)")
fig1.show()

# Biểu đồ 2: Tương quan Nhiệt độ vs PM2.5
# Insight: Xem thử trời nóng hay trời lạnh thì bụi nhiều hơn?
fig2 = px.scatter(pdf, x="temperature", y="pm25", color="AQI_Category",
                 title="Tương quan: Nhiệt độ và PM2.5",
                 opacity=0.6)
fig2.show()

# Biểu đồ 3: Heatmap theo giờ (Tìm giờ ô nhiễm nhất trong ngày)
# Tạo cột Giờ và Tháng
pdf['hour'] = pdf['datetime'].dt.hour
pdf['month'] = pdf['datetime'].dt.month
pivot_table = pdf.pivot_table(values='pm25', index='hour', columns='month', aggfunc='mean')

fig3 = px.imshow(pivot_table, 
                labels=dict(x="Tháng", y="Giờ trong ngày", color="PM2.5 TB"),
                x=['T1','T2','T3','T4','T5','T6','T7','T8','T9','T10','T11','T12'],
                title="Heatmap: Mức độ ô nhiễm theo Giờ và Tháng",
                color_continuous_scale="RdYlGn_r") # Xanh là tốt, Đỏ là xấu
fig3.show()