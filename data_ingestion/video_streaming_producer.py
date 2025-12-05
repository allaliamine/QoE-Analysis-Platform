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
logger = logging.getLogger("video_streaming_producer")

schema_str = open("schemas/video_streaming_schema.json").read()


def main():

    # Schema Registry client
    schema_registry_client = SchemaRegistryClient(
        {"url": "http://schema-registry:8081"}   # inside Docker network
    )
    json_serializer = JSONSerializer(schema_str, schema_registry_client)

    # Kafka Producer (Confluent)
    producer = Producer({
        "bootstrap.servers": "kafka-0-s:9092"
    })

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
            producer.produce(
                topic=topic_name,
                value=json_serializer(
                    message,
                    SerializationContext(topic_name, MessageField.VALUE)
                )
            )
            producer.flush()
            logger.info("Sent message: %s", message)

        except Exception as e:
            logger.error("Failed to send message: %s", e)

        time.sleep(1)