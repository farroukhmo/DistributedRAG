"""
Kafka Producer
Sends scraping tasks to Kafka queue
"""

from kafka import KafkaProducer
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TaskProducer:
    """Send tasks to Kafka queue"""

    def __init__(self, bootstrap_servers='localhost:9093'):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3
            )
            logger.info("✅ Kafka Producer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def send_scraping_task(self, url, task_type='scrape'):
        """
        Send scraping task to Kafka

        Args:
            url: URL to scrape
            task_type: Type of task (scrape, process, index)
        """
        try:
            task = {
                'url': url,
                'task_type': task_type,
                'timestamp': str(pd.Timestamp.now())
            }

            future = self.producer.send(
                'scraping_tasks',
                key=task_type,
                value=task
            )

            # Wait for confirmation
            record_metadata = future.get(timeout=10)

            logger.info(f"✅ Task sent: {url}")
            logger.info(f"   Topic: {record_metadata.topic}")
            logger.info(f"   Partition: {record_metadata.partition}")
            logger.info(f"   Offset: {record_metadata.offset}")

            return True

        except Exception as e:
            logger.error(f"Failed to send task: {e}")
            return False

    def close(self):
        """Close producer"""
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer closed")


# Example usage
if __name__ == "__main__":
    import pandas as pd

    print("\n" + "=" * 70)
    print("🚀 KAFKA PRODUCER TEST")
    print("=" * 70 + "\n")

    producer = TaskProducer()

    # Send test tasks
    test_urls = [
        "https://example.com/page1",
        "https://example.com/page2",
        "https://example.com/page3"
    ]

    for url in test_urls:
        success = producer.send_scraping_task(url)
        if success:
            print(f"✅ Sent: {url}")
        else:
            print(f"❌ Failed: {url}")

    producer.close()

    print("\n" + "=" * 70)
    print("✅ PRODUCER TEST COMPLETE")
    print("=" * 70 + "\n")