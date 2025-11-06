"""
Kafka Producer Script
Phase 6: Distributed Messaging Integration
"""

from kafka import KafkaProducer
import json
import logging
import time

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = "localhost:9093"  # Use port 9093 for external connections
TOPIC = "scraping_tasks"


def create_producer():
    """Initialize Kafka producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks='all',
            retries=3
        )
        logger.info("✅ Kafka Producer initialized")
        return producer
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        exit(1)


def send_tasks(producer):
    """Send scraping tasks"""
    urls = [
        "https://quotes.toscrape.com/page/1/",
        "https://quotes.toscrape.com/page/2/",
        "https://quotes.toscrape.com/page/3/"
    ]

    print("\n" + "="*70)
    print("🚀 KAFKA PRODUCER TEST")
    print("="*70 + "\n")

    for idx, url in enumerate(urls):
        task = {
            "url": url,
            "task_type": "scrape",
            "timestamp": time.time()
        }

        future = producer.send(TOPIC, value=task)
        record = future.get(timeout=10)

        logger.info(f"✅ Task sent: {url}")
        logger.info(f"   Partition: {record.partition}, Offset: {record.offset}")
        time.sleep(1)

    producer.flush()
    print("\n" + "="*70)
    print("✅ PRODUCER TEST COMPLETE")
    print("="*70 + "\n")


if __name__ == "__main__":
    producer = create_producer()
    send_tasks(producer)  # Fixed: removed the extra ()
    producer.close()