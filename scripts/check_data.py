import polars as pl

# 1. Load file Parquet đã xử lý
df = pl.read_parquet("data/processed/air_quality_merged.parquet")

# 2. Xem tổng quan
print(f"Kích thước dữ liệu: {df.shape}")
print(df.head())

# 3. Kiểm tra tỷ lệ Null (Quan trọng)
# Để biết cột nào dùng được, cột nào vứt đi (ví dụ nếu cột CO toàn null thì bỏ)
null_counts = df.null_count()
print("\n--- Số lượng giá trị Null từng cột ---")
print(null_counts)

# 4. Describe thống kê (Min, Max, Mean)
print("\n--- Thống kê mô tả ---")
print(df.describe())