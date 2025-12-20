from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.avro.functions import from_avro
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cloud_gaming_consumer")

spark = SparkSession.builder \
    .appName("Cloud_Gaming_Consumer") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP_SERVERS = "kafka-0-s:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC_NAME = "cloud_gaming_kpi"

schema_response = requests.get(
    f"{SCHEMA_REGISTRY_URL}/subjects/{TOPIC_NAME}-value/versions/latest"
)
avro_schema = json.loads(schema_response.text)["schema"]

print("Using Avro Schema:")
print(avro_schema)

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

final_df = df.selectExpr("substring(value, 6) as avro_value") \
    .select(
    from_avro(
        col("avro_value"),
        avro_schema,
        {"schema.registry.url": SCHEMA_REGISTRY_URL}
    ).alias("data")).select(
    "data.*",
).withColumn(
    "processing_time",
    current_timestamp()
)
query = final_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

logger.info("Consumer started successfully")
query.awaitTermination()