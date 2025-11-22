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
logger = logging.getLogger("cloud_gaming_producer")


def main():
    producer = KafkaProducer(
        bootstrap_servers='kafka-0-s:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    topic_name = "cloud_gaming_kpi"
    logger.info("Kafka producer initialized for topic '%s'", topic_name)

    
    while True:
        message = {
            "CPU_usage": random.randint(50, 100),
            "GPU_usage": random.randint(40, 100),
            "Bandwidth_MBps": round(random.uniform(10, 100), 2),
            "Latency_ms": random.randint(20, 200),
            "FrameRate_fps": random.randint(24, 120),
            "Jitter_ms": random.randint(0, 30)
        }
        try:
            producer.send(topic_name, value=message)
            logger.info("Sent message: %s", message)
        except KafkaError as e:
            logger.error("Failed to send message: %s", e)
        time.sleep(1)
