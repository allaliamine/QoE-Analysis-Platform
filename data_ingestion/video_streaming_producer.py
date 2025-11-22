import json
import logging
import random
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger("video_streaming_producer")

def main():
    producer = KafkaProducer(
        bootstrap_servers='kafka-0-s:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    topic_name = "video_stream_kpi"
    logger.info("Kafka producer initialized for topic '%s'", topic_name)
    
    while True:
        message = {
            "throughput": round(random.uniform(1000, 5000), 2),
            "avg_bitrate": round(random.uniform(500, 4000), 2),
            "delay_qos": round(random.uniform(0, 200), 2),
            "jitter": round(random.uniform(0, 50), 2),
            "packet_loss": round(random.uniform(0, 5), 2)
        }
        try:
            producer.send(topic_name, value=message)
            logger.info("Sent message: %s", message)
        except KafkaError as e:
            logger.error("Failed to send message: %s", e)
        time.sleep(1)