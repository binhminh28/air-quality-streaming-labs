"""
Module hỗ trợ ghi dữ liệu Spark Streaming vào Cassandra
"""
from pyspark.sql import DataFrame

CASSANDRA_HOST = "localhost"
CASSANDRA_PORT = "9042"
CASSANDRA_KEYSPACE = "air_quality"
CASSANDRA_TABLE = "realtime_data"

def write_to_cassandra(df: DataFrame, epoch_id: int):
    """
    Foreach batch function để ghi dữ liệu vào Cassandra
    Sử dụng với foreachBatch trong Spark Streaming
    """
    try:
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(
                table=CASSANDRA_TABLE,
                keyspace=CASSANDRA_KEYSPACE,
                host=CASSANDRA_HOST,
                port=CASSANDRA_PORT
            ) \
            .save()
        
        print(f"✅ Batch {epoch_id}: Wrote {df.count()} records to Cassandra")
    except Exception as e:
        print(f"❌ Error writing batch {epoch_id} to Cassandra: {e}")

def create_cassandra_keyspace_and_table(spark):
    """
    Tạo keyspace và table trong Cassandra nếu chưa tồn tại
    Chạy một lần trước khi start streaming
    """
    try:
        # Tạo keyspace
        spark.sql(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        
        # Tạo table
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} (
                datetime STRING PRIMARY KEY,
                pm25 FLOAT,
                aqi INT,
                quality STRING,
                processed_at TIMESTAMP
            )
        """)
        
        print(f"✅ Created keyspace {CASSANDRA_KEYSPACE} and table {CASSANDRA_TABLE}")
    except Exception as e:
        print(f"⚠️  Note: Using existing Cassandra schema or manual setup required: {e}")
        print(f"   Please create keyspace '{CASSANDRA_KEYSPACE}' and table '{CASSANDRA_TABLE}' manually")

