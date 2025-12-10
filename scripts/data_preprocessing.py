import polars as pl

PARAM_MAP = {
    "pm2.5": "pm25", "pm2_5": "pm25", "pm1": "pm1", 
    "pm10": "pm10", "relativehumidity": "relativehumidity",
    "temperature": "temperature", "co": "co", "o3": "o3", 
    "so2": "so2", "no2": "no2", "um003": "um003"
}

def process_air_quality_data(input_pattern, output_file):
    try:
        # Step 1: Lazy Scan & Pre-processing
        q = (
            pl.scan_csv(
                input_pattern,
                schema_overrides={"value": pl.Float64, "lat": pl.Float64, "lon": pl.Float64, "location_id": pl.Int64},
                ignore_errors=True
            )
            .with_columns(
                pl.col("datetime").str.strip_chars().str.to_datetime("%Y-%m-%dT%H:%M:%S%z"),
                pl.col("parameter").str.strip_chars().str.to_lowercase().replace(PARAM_MAP)
            )
            .group_by(["location_id", "datetime", "parameter"])
            .agg(pl.col("value").mean())
        )

        # Step 2: Collect & Pivot
        df = (
            q.collect()
            .pivot(index=["datetime", "location_id"], on="parameter", values="value")
            .sort("datetime")
        )

        # Step 3: Save
        df.write_parquet(output_file, compression="snappy")
        print(f"Done! Saved to {output_file}")

    except Exception as e:
        print(f"Error processing data: {e}")

if __name__ == "__main__":
    INPUT_PATTERN = "data/raw/month=*/location-*.csv.gz"
    OUTPUT_FILE = "data/processed/air_quality_merged.parquet"

    process_air_quality_data(INPUT_PATTERN, OUTPUT_FILE)