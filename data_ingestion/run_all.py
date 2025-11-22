import threading
import could_gaming_producer
import video_streaming_producer
import setup_topics
import logging
import time

# # Create topics first
# setup_topics.create_kafka_topics()

# # Start producers in parallel
# threading.Thread(target=video_streaming_producer.main).start()
# threading.Thread(target=could_gaming_producer.main).start()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)
logger = logging.getLogger("run_all")

time.sleep(10)

# Create topics first
logger.info("Creating Kafka topics...")
setup_topics.create_kafka_topics()

# Start producers in parallel
logger.info("Starting video streaming producer...")
threading.Thread(target=video_streaming_producer.main).start()

logger.info("Starting cloud gaming producer...")
threading.Thread(target=could_gaming_producer.main).start()