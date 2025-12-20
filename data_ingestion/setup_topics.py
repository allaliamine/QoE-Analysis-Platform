from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger("run_all")

def create_kafka_topics():
    """
    Create two Kafka topics for the QoE Analysis Platform
    """
    
    admin_client = KafkaAdminClient(
        bootstrap_servers=['kafka-0-s:9092'],
        client_id='qoe-topic-creator'
    )
    
    # Define topics
    topics = [
        NewTopic(name='video_streaming_kpi', num_partitions=1, replication_factor=1),
        NewTopic(name='cloud_gaming_kpi', num_partitions=1, replication_factor=1)
    ]
    
    try:
        # Create topics
        admin_client.create_topics(new_topics=topics, validate_only=False)
        logger.info("Topics created successfully.")
    except TopicAlreadyExistsError as e:
        logger.warning(f"Topic already exists: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
    finally:
        admin_client.close()
