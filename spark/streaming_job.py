from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, when, current_timestamp, greatest
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, FloatType, LongType
import os

# Cấu hình Kafka - hỗ trợ multiple brokers
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092,localhost:9093,localhost:9094"
)
INPUT_TOPIC = os.getenv("KAFKA_TOPIC", "air_quality_realtime")

# Cấu hình Cassandra - hỗ trợ multiple hosts
SINK_MODE = os.getenv("SINK_MODE", "console")
CASSANDRA_HOSTS = os.getenv(
    "CASSANDRA_HOSTS",
    "localhost:9042,localhost:9043"
)  # Danh sách hosts:port cách nhau bởi dấu phẩy
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "air_quality")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "realtime_data")

def create_spark_session():
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    if SINK_MODE == "cassandra":
        packages += ",com.datastax.spark:spark-cassandra-connector_2.12:3.2.0"
    
    spark_builder = SparkSession.builder \
        .appName("AirQuality_SparkStreaming") \
        .config("spark.jars.packages", packages) \
        .config("spark.sql.shuffle.partitions", "2")
    
    # Cấu hình Cassandra với multiple hosts
    if SINK_MODE == "cassandra":
        # Parse danh sách hosts:port
        hosts_list = [h.strip() for h in CASSANDRA_HOSTS.split(',')]
        # Spark Cassandra connector hỗ trợ nhiều hosts bằng cách nối bằng dấu phẩy
        cassandra_hosts = ','.join(hosts_list)
        spark_builder = spark_builder.config("spark.cassandra.connection.host", cassandra_hosts)
        print(f"Cassandra hosts configured: {cassandra_hosts}")
    
    return spark_builder.getOrCreate()

def get_schema():
    return StructType([
        StructField("datetime", StringType(), True),
        StructField("location_id", LongType(), True),
        StructField("pm25", FloatType(), True),
        StructField("pm10", FloatType(), True),
        StructField("pm1", FloatType(), True),
        StructField("temperature", FloatType(), True),
        StructField("relativehumidity", FloatType(), True),
        StructField("um003", FloatType(), True)
    ])

def calc_aqi_pm25(concentration):
    """Tính AQI cho PM2.5 theo QCVN 05:2013/BTNMT (Việt Nam)"""
    if concentration is None:
        return 0
    c = float(concentration)
    
    # Breakpoints theo QCVN 05:2013/BTNMT cho PM2.5 (µg/m³)
    breakpoints = [
        (0.0, 25.0, 0, 50),      # Tốt
        (25.1, 50.0, 51, 100),   # Trung bình
        (50.1, 100.0, 101, 150), # Kém
        (100.1, 150.0, 151, 200), # Xấu
        (150.1, 250.0, 201, 300), # Rất xấu
        (250.1, 350.0, 301, 400), # Nguy hại
        (350.1, 500.0, 401, 500)  # Nguy hại
    ]
    
    for (c_low, c_high, i_low, i_high) in breakpoints:
        if c_low <= c <= c_high:
            aqi = ((i_high - i_low) / (c_high - c_low)) * (c - c_low) + i_low
            return int(round(aqi))
    
    if c > 500.0:
        return 500
    return 0

def calc_aqi_pm10(concentration):
    """Tính AQI cho PM10 theo QCVN 05:2013/BTNMT (Việt Nam)"""
    if concentration is None:
        return 0
    c = float(concentration)
    
    # Breakpoints theo QCVN 05:2013/BTNMT cho PM10 (µg/m³)
    breakpoints = [
        (0.0, 50.0, 0, 50),
        (50.1, 100.0, 51, 100),
        (100.1, 200.0, 101, 150),
        (200.1, 300.0, 151, 200),
        (300.1, 400.0, 201, 300),
        (400.1, 500.0, 301, 400),
        (500.1, 600.0, 401, 500)
    ]
    
    for (c_low, c_high, i_low, i_high) in breakpoints:
        if c_low <= c <= c_high:
            aqi = ((i_high - i_low) / (c_high - c_low)) * (c - c_low) + i_low
            return int(round(aqi))
    
    if c > 600.0:
        return 500
    return 0

def calc_vn_aqi(pm25_aqi, pm10_aqi):
    """Tính AQI tổng hợp theo QCVN 05:2013/BTNMT - lấy giá trị cao nhất"""
    if pm25_aqi is None:
        pm25_aqi = 0
    if pm10_aqi is None:
        pm10_aqi = 0
    return max(int(pm25_aqi), int(pm10_aqi))

aqi_pm25_udf = udf(calc_aqi_pm25, IntegerType())
aqi_pm10_udf = udf(calc_aqi_pm10, IntegerType())
vn_aqi_udf = udf(calc_vn_aqi, IntegerType())

def apply_aqi_logic(df):
    df_with_aqi_pm25 = df.withColumn("aqi_pm25", aqi_pm25_udf(col("pm25")))
    df_with_aqi_pm10 = df_with_aqi_pm25.withColumn("aqi_pm10", aqi_pm10_udf(col("pm10")))
    df_with_aqi = df_with_aqi_pm10.withColumn("AQI", vn_aqi_udf(col("aqi_pm25"), col("aqi_pm10")))

    df_final = df_with_aqi.withColumn("Quality", 
        when(col("AQI") <= 50, "Tốt (Good)")
        .when((col("AQI") > 50) & (col("AQI") <= 100), "Trung bình (Moderate)")
        .when((col("AQI") > 100) & (col("AQI") <= 150), "Kém (Unhealthy for Sensitive)")
        .when((col("AQI") > 150) & (col("AQI") <= 200), "Xấu (Unhealthy)")
        .when((col("AQI") > 200) & (col("AQI") <= 300), "Rất Xấu (Very Unhealthy)")
        .otherwise("Nguy hại (Hazardous)")
    )
    
    return df_final

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Connecting to Kafka brokers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Subscribing to topic: {INPUT_TOPIC}")

    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), get_schema()).alias("data")) \
        .select("data.*")

    processed_df = apply_aqi_logic(json_df)
    
    final_df = processed_df \
        .withColumnRenamed("AQI", "aqi") \
        .withColumnRenamed("Quality", "quality") \
        .withColumn("processed_at", current_timestamp())

    if SINK_MODE == "cassandra":
        def write_to_cassandra_batch(df, epoch_id):
            try:
                df.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .mode("append") \
                    .options(
                        table=CASSANDRA_TABLE,
                        keyspace=CASSANDRA_KEYSPACE
                    ) \
                    .save()
            except Exception as e:
                print(f"Error writing to Cassandra: {e}")
        
        query = final_df.writeStream \
            .outputMode("append") \
            .foreachBatch(write_to_cassandra_batch) \
            .option("checkpointLocation", "/tmp/spark_checkpoints_cassandra") \
            .start()
    else:
        query = final_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .option("checkpointLocation", "/tmp/spark_checkpoints") \
            .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
