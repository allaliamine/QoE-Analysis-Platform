from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, current_timestamp, date_format
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cloud_gaming_consumer")

spark = SparkSession.builder \
    .appName("Cloud_Gaming_Consumer") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint/cloud-gaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP_SERVERS = "kafka-0-s:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"
TOPIC_NAME = "cloud_gaming_kpi"
API_URL = "http://cloud-gaming-api:8000/predict"

# ClickHouse HTTP configuration
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = "8123"
CLICKHOUSE_USER = "qoe_user"
CLICKHOUSE_PASSWORD = "qoe_password"
CLICKHOUSE_DATABASE = "raw_data"
CLICKHOUSE_TABLE = "cloud_gaming_raw"

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
    col("CPU_usage"),
    col("GPU_usage"),
    col("Bandwidth_MBps"),
    col("Latency_ms"),
    col("FrameRate_fps"),
    col("Jitter_ms"),
    date_format(col("processing_time"), "yyyy-MM-dd HH:mm:ss").alias("processing_time")
)

def write_to_clickhouse(batch_df, batch_id):
    if batch_df.count() > 0:
        logger.info(f"[Cloud Gaming] Processing batch {batch_id} with {batch_df.count()} records")
        
        try:
            # Convert to CSV format WITHOUT headers
            pandas_df = batch_df.toPandas()
            csv_data = pandas_df.to_csv(index=False, header=False)
            
            # Prepare INSERT query - explicitly list columns matching order
            insert_query = f"""INSERT INTO {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE} (CPU_usage, GPU_usage, Bandwidth_MBps, Latency_ms, FrameRate_fps, Jitter_ms, processing_time) FORMAT CSV"""
            
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
                logger.info(f"[Cloud Gaming] Batch {batch_id} written successfully to ClickHouse")
            else:
                logger.error(f"[Cloud Gaming] HTTP Error {response.status_code}: {response.text}")
            
            # Optional: Show sample data in console for debugging
            logger.info(f"Sample data from batch {batch_id}:")
            batch_df.show(5, truncate=False)
            
        except Exception as e:
            logger.error(f"[Cloud Gaming] Error writing batch {batch_id}: {str(e)}")
            # Log the schema mismatch if any
            logger.error(f"DataFrame schema: {batch_df.schema}")



def process_batch(batch_df, batch_id):
    records = batch_df.collect()
    predictions_list = []
    
    for row in records:
        try:
            payload = {
                "CPU_usage": float(row["CPU_usage"]),
                "GPU_usage": float(row["GPU_usage"]),
                "Bandwidth_MBps": float(row["Bandwidth_MBps"]),
                "Latency_ms": float(row["Latency_ms"]),
                "FrameRate_fps": float(row["FrameRate_fps"]),
                "Jitter_ms": float(row["Jitter_ms"])
            }
            
            response = requests.post(API_URL, json=payload, timeout=5)
            
            if response.status_code == 200:
                prediction = response.json()
                logger.info(f"Record: {payload} => Prediction: {prediction}")
                
                # Prepare prediction record with input features and QoE score
                prediction_record = {
                    "CPU_usage": row["CPU_usage"],
                    "GPU_usage": row["GPU_usage"],
                    "Bandwidth_MBps": row["Bandwidth_MBps"],
                    "Latency_ms": row["Latency_ms"],
                    "FrameRate_fps": row["FrameRate_fps"],
                    "Jitter_ms": row["Jitter_ms"],
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
            
            insert_query = f"""INSERT INTO predictions.cloud_gaming_predictions (CPU_usage, GPU_usage, Bandwidth_MBps, Latency_ms, FrameRate_fps, Jitter_ms, predicted_qoe, qoe_class) FORMAT CSV"""
            
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
                logger.info(f"[Cloud Gaming] {len(predictions_list)} predictions stored in ClickHouse")
            else:
                logger.error(f"[Cloud Gaming] Failed to store predictions: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error(f"[Cloud Gaming] Error storing predictions: {str(e)}")


query = final_df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: (write_to_clickhouse(batch_df, batch_id), process_batch(batch_df, batch_id))) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint/cloud-gaming") \
    .start()


logger.info("Consumer started successfully")
query.awaitTermination()