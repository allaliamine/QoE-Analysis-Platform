from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.avro.functions import from_avro
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("video_streaming_consumer")

spark = SparkSession.builder \
    .appName("Video_Streaming_Consumer") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP_SERVERS = "kafka-0-s:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC_NAME = "video_streaming_kpi"
API_URL = "http://video-streaming-api:8000/predict"

schema_response = requests.get(
    f"{SCHEMA_REGISTRY_URL}/subjects/{TOPIC_NAME}-value/versions/latest"
)
schema_response.raise_for_status()
response_json = schema_response.json()
logger.info(f"Schema registry response: {response_json}")
avro_schema = response_json["schema"]

print("Using Avro Schema:")
print(avro_schema)

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = df.selectExpr("substring(value, 6) as avro_value") \
    .select(
        from_avro(
            col("avro_value"),
            avro_schema,
            {"schema.registry.url": SCHEMA_REGISTRY_URL}
        ).alias("data")
    ).select("data.*") \
    .withColumn("processing_time", current_timestamp())


def process_batch(batch_df, batch_id):
    """Process each batch and call API for each record"""
    records = batch_df.collect()
    
    for row in records:
        try:
            payload = {
                "throughput": float(row["throughput"]),
                "avg_bitrate": float(row["avg_bitrate"]),
                "delay_qos": float(row["delay_qos"]),
                "jitter": float(row["jitter"]),
                "packet_loss": float(row["packet_loss"])
            }
            
            response = requests.post(API_URL, json=payload, timeout=5)
            
            if response.status_code == 200:
                prediction = response.json()
                logger.info(f"Record: {payload} => Prediction: {prediction}")
            else:
                logger.error(f"API error: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error processing record: {e}")


query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .start()

logger.info("Consumer started successfully")
query.awaitTermination()