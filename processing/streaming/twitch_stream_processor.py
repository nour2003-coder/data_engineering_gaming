from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

# Build Spark session
spark = SparkSession.builder \
    .appName("TwitchKafkaToPostgres") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# Kafka stream schema
schema = StructType([
    StructField("platform", StringType()),
    StructField("event_time", StringType()),
    StructField("ingestion_time", StringType()),
    StructField("stream_id", StringType()),
    StructField("streamer_id", StringType()),
    StructField("streamer_name", StringType()),
    StructField("game_id", StringType()),
    StructField("game_name", StringType()),
    StructField("title", StringType()),
    StructField("language", StringType()),
    StructField("viewer_count", IntegerType()),
    StructField("is_mature", BooleanType()),
    StructField("thumbnail_url", StringType())
])

# Read Kafka topic
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "twitch_streams") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON value
df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Convert string timestamps to proper timestamp type
from pyspark.sql.functions import to_timestamp
df_parsed = df_parsed \
    .withColumn("event_time", to_timestamp("event_time")) \
    .withColumn("ingestion_time", to_timestamp("ingestion_time"))

# Write each micro-batch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/twitch_db") \
        .option("dbtable", "twitch_streams") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append") \
        .save()

# Start streaming
query = df_parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()