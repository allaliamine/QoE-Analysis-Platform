import logging
import random
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger("cloud_gaming_producer")

schema_str = open("schemas/cloud_gaming_schema.json").read()


def main():

    # Connect to schema registry (inside docker network)
    schema_registry_client = SchemaRegistryClient(
        {"url": "http://schema-registry:8081"}
    )
    json_serializer = JSONSerializer(schema_str, schema_registry_client)

    # Kafka producer
    producer = Producer({
        "bootstrap.servers": "kafka-0-s:9092"
    })

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
            producer.produce(
                topic=topic_name,
                value=json_serializer(
                    message,
                    SerializationContext(topic_name, MessageField.VALUE)
                )
            )
            producer.flush()
            logger.info("Sent: %s", message)

        except Exception as e:
            logger.error("Failed to send message: %s", e)

        time.sleep(1)
