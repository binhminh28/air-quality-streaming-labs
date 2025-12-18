import polars as pl
import numpy as np

def upscale_to_5s_with_noise(input_parquet, output_parquet):
    # 1. Đọc và tạo khung thời gian 5s
    df = pl.read_parquet(input_parquet).sort("datetime")
    
    time_grid = pl.datetime_range(
        start=df["datetime"].min(), end=df["datetime"].max(),
        interval="5s", eager=True
    ).alias("datetime").to_frame()

    # 2. Join & Interpolate (Tạo đường mượt)
    val_cols = [c for c in df.columns if c not in ["datetime", "location_id"]]
    
    upscaled = (
        time_grid.join(df, on="datetime", how="left")
        .with_columns(pl.col("location_id").fill_null(strategy="forward"))
        .with_columns([pl.col(c).interpolate() for c in val_cols])
    )

    # 3. THÊM NOISE (Quan trọng: Dùng Numpy vectorization)
    # Logic: Value_mới = Value_nội_suy * (1 + Random_Noise)
    # Noise theo phân phối chuẩn (Gaussian), độ lệch chuẩn 0.02 (dao động ~2-5%)
    n_rows = upscaled.height
    noise_std = 0.02 

    # Sinh noise cho tất cả các cột cùng lúc (nhanh hơn loop)
    # Lưu ý: Mỗi cột cần một mảng noise riêng để không bị "đồng bộ"
    noise_exprs = [
        (pl.col(c) * (1 + np.random.normal(0, noise_std, n_rows))).alias(c)
        for c in val_cols
    ]

    final_df = upscaled.with_columns(noise_exprs)

    # 4. Lưu
    final_df.write_parquet(output_parquet)
    print(f"Done! Rows: {n_rows:,}. Upscaled 5s with Gaussian Noise.")

if __name__ == "__main__":
    upscale_to_5s_with_noise("data/processed/air_quality_merged.parquet", "data/processed/air_quality_5s_noise.parquet")