from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, when, current_timestamp
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, FloatType

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "air_quality_realtime"

def create_spark_session():
    return SparkSession.builder \
        .appName("AirQuality_SparkStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("datetime", StringType(), True),
        StructField("pm25", FloatType(), True)
    ])

def apply_aqi_logic(df):
    df_with_aqi = df.withColumn("AQI", aqi_udf(col("pm25")))

    df_final = df_with_aqi.withColumn("Quality", 
        when(col("AQI") <= 50, "Tốt (Good)")
        .when((col("AQI") > 50) & (col("AQI") <= 100), "Trung bình (Moderate)")
        .when((col("AQI") > 100) & (col("AQI") <= 150), "Kém (Unhealthy for Sensitive)")
        .when((col("AQI") > 150) & (col("AQI") <= 200), "Xấu (Unhealthy)")
        .when((col("AQI") > 200) & (col("AQI") <= 300), "Rất Xấu (Very Unhealthy)")
        .otherwise("Nguy hại (Hazardous)")
    )
    
    return df_final

def calc_aqi_pm25_scalar(concentration):
    """
    Tính AQI cho PM2.5 theo công thức nội suy tuyến tính (QCVN 05:2013 / EPA).
    Input: Nồng độ PM2.5 (µg/m³)
    Output: Chỉ số AQI
    """
    if concentration is None:
        return 0
        
    c = float(concentration)
    
    breakpoints = [
        (0.0, 12.0, 0, 50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 350.4, 301, 400),
        (350.5, 500.4, 401, 500)
    ]
    
    for (c_low, c_high, i_low, i_high) in breakpoints:
        if c_low <= c <= c_high:
            aqi = ((i_high - i_low) / (c_high - c_low)) * (c - c_low) + i_low
            return int(round(aqi))
            
    if c > 500.4:
        return 500
        
    return 0

aqi_udf = udf(calc_aqi_pm25_scalar, IntegerType())

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"🚀 Spark Job started. Listening to {INPUT_TOPIC}...")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), get_schema()).alias("data")) \
        .select("data.*")

    processed_df = apply_aqi_logic(json_df)
    
    final_df = processed_df.withColumn("processed_at", current_timestamp())

    query = final_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", "/tmp/spark_checkpoints") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()