from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# The Kafka connector package — Spark downloads this automatically on first run
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"

def create_spark_session():
    """Create a Spark session with the Kafka connector loaded."""
    return SparkSession.builder \
        .appName("NewsStreamProcessor") \
        .master("local[*]") \
        .config("spark.jars.packages", KAFKA_PACKAGE) \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # Define the structure of our news JSON so Spark can parse it
    news_schema = StructType([
        StructField("title", StringType(), True),
        StructField("link", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("published", StringType(), True),
        StructField("source", StringType(), True),
        StructField("category", StringType(), True),
        StructField("ticker", StringType(), True),
        StructField("fetched_at", StringType(), True),
    ])

    # 1. Read from Kafka as a stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "raw-news") \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Kafka gives us bytes in a "value" column — cast to string, then parse JSON
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), news_schema).alias("data")
    ).select("data.*")

    # 3. For now, just print parsed articles to the console to verify it works
    query = parsed_df.writeStream \
        .format("console") \
        .option("truncate", "true") \
        .outputMode("append") \
        .start()

    print("\nStreaming started. Reading from Kafka topic 'raw-news'...")
    print("Press Ctrl+C to stop.\n")

    query.awaitTermination()

if __name__ == "__main__":
    main()