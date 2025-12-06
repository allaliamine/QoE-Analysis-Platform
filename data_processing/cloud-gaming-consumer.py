from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, substring, current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cloud_gaming_consumer")

spark = SparkSession.builder \
    .appName("Cloud_Gaming_Consumer") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP_SERVERS = "kafka-0-s:9092"
TOPIC_NAME = "cloud_gaming_kpi"
CHECKPOINT_LOCATION = "/tmp/gaming-checkpoint"


gaming_schema = StructType([
    StructField("CPU_usage", IntegerType(), True),
    StructField("GPU_usage", IntegerType(), True),
    StructField("Bandwidth_MBps", DoubleType(), True),
    StructField("Latency_ms", IntegerType(), True),
    StructField("FrameRate_fps", IntegerType(), True),
    StructField("Jitter_ms", IntegerType(), True),
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .option("kafka.group.id", "spark-gaming-consumer-group") \
    .option("failOnDataLoss", "false") \
    .load()

final_df = df.select(
    # Skip Schema Registry header (5 bytes) and parse JSON
    from_json(
        substring(col("value"), 6, 999999).cast("string"),
        gaming_schema
    ).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select(
    "data.*",  # Flatten the structure
    "kafka_timestamp"
).withColumn(
    "processing_time", 
    current_timestamp()
)

# Write to console
query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .trigger(processingTime='5 seconds') \
    .start()

logger.info("Consumer started successfully")
query.awaitTermination()