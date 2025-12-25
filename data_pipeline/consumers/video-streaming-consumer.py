from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_format
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

# ClickHouse HTTP configuration
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_USER = "qoe_user"
CLICKHOUSE_PASSWORD = "qoe_password"
CLICKHOUSE_DATABASE = "raw_data"
CLICKHOUSE_TABLE = "video_streaming_raw"

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

final_df = parsed_df.select(
    col("throughput").cast("double").alias("throughput"),
    col("avg_bitrate").cast("double").alias("avg_bitrate"),
    col("delay_qos").cast("double").alias("delay_qos"),
    col("jitter").cast("double").alias("jitter"),
    col("packet_loss").cast("double").alias("packet_loss"),
    date_format(col("processing_time"), "yyyy-MM-dd HH:mm:ss").alias("processing_time")
)

def write_to_clickhouse(batch_df, batch_id):
    if batch_df.count() > 0:
        logger.info(f"[Video Streaming] Processing batch {batch_id} with {batch_df.count()} records")
        
        try:
            # Convert to CSV format WITH headers
            pandas_df = batch_df.toPandas()
            csv_data = pandas_df.to_csv(index=False, header=False)
            
            # Prepare INSERT query - explicitly list columns matching order
            insert_query = f"""INSERT INTO {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} (throughput, avg_bitrate, delay_qos, jitter, packet_loss, processing_time) FORMAT CSV"""
            
            # Send to ClickHouse via HTTP
            response = requests.post(
                f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/',
                params={
                    'query': insert_query,
                    'user': CLICKHOUSE_USER,
                    'password': CLICKHOUSE_PASSWORD,
                    'database': CLICKHOUSE_DATABASE
                },
                data=csv_data.encode('utf-8'),
                timeout=60
            )
            
            if response.status_code == 200:
                logger.info(f"[Video Streaming] Batch {batch_id} written successfully to ClickHouse")
            else:
                logger.error(f"[Video Streaming] HTTP Error {response.status_code}: {response.text}")
            
            # Optional: Show sample data in console for debugging
            logger.info(f"Sample data from batch {batch_id}:")
            batch_df.show(5, truncate=False)
            
        except Exception as e:
            logger.error(f"[Video Streaming] Error writing batch {batch_id}: {str(e)}")
            logger.error(f"DataFrame schema: {batch_df.schema}")


def predict_batch(batch_df, batch_id):
    """Call API for predictions and store in ClickHouse"""
    records = batch_df.select("throughput", "avg_bitrate", "delay_qos", "jitter", "packet_loss").collect()
    predictions_list = []
    
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
                
                # Prepare prediction record with input features and QoE score
                prediction_record = {
                    "throughput": row["throughput"],
                    "avg_bitrate": row["avg_bitrate"],
                    "delay_qos": row["delay_qos"],
                    "jitter": row["jitter"],
                    "packet_loss": row["packet_loss"],
                    "predicted_qoe": prediction.get("qoe_score", 0),
                    "qoe_class": prediction.get("qoe_class", "Unknown")
                }
                predictions_list.append(prediction_record)
            else:
                logger.error(f"API error: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"Error processing record: {e}")
    
    # Write predictions to ClickHouse if any predictions exist
    if predictions_list:
        try:
            import pandas as pd
            predictions_df = pd.DataFrame(predictions_list)
            csv_data = predictions_df.to_csv(index=False, header=False)
            
            insert_query = f"""INSERT INTO predictions.video_streaming_predictions (throughput, avg_bitrate, delay_qos, jitter, packet_loss, predicted_qoe) FORMAT CSV"""
            
            response = requests.post(
                f'http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/',
                params={
                    'query': insert_query,
                    'user': CLICKHOUSE_USER,
                    'password': CLICKHOUSE_PASSWORD,
                    'database': 'predictions'
                },
                data=csv_data.encode('utf-8'),
                timeout=60
            )
            
            if response.status_code == 200:
                logger.info(f"[Video Streaming] {len(predictions_list)} predictions stored in ClickHouse")
            else:
                logger.error(f"[Video Streaming] Failed to store predictions: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"[Video Streaming] Error storing predictions: {str(e)}")


query = final_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: (write_to_clickhouse(batch_df, batch_id), predict_batch(batch_df, batch_id))) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/video-streaming") \
    .start()

logger.info("Consumer started successfully")
query.awaitTermination()