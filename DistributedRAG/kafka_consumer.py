"""
Kafka Consumer Script
Phase 6: Distributed Messaging Integration
"""

from kafka import KafkaConsumer
import json
import logging
import time
from pymongo import MongoClient

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BROKER = "localhost:9093"  # Use port 9093 for external connections
TOPIC = "scraping_tasks"

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "scraped_data"
COLLECTION_NAME = "kafka_tasks"


def create_consumer():
    """Initialize Kafka consumer"""
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='scraper_workers',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            consumer_timeout_ms=60000
        )
        logger.info("✅ Kafka Consumer initialized")
        return consumer
    except Exception as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        exit(1)


def create_mongo_client():
    """Initialize MongoDB client"""
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logger.info(f"✅ Connected to MongoDB: {DB_NAME}.{COLLECTION_NAME}")
        return collection
    except Exception as e:
        logger.error(f"❌ Failed to connect to MongoDB: {e}")
        exit(1)


def process_task(task, collection):
    """Process a scraping task"""
    try:
        logger.info(f"📝 Processing task: {task.get('url')}")

        # Simulate processing
        time.sleep(2)

        # Store result in MongoDB
        result = {
            'url': task.get('url'),
            'task_type': task.get('task_type', 'scrape'),
            'status': 'completed',
            'received_at': task.get('timestamp'),
            'processed_at': time.time()
        }

        collection.insert_one(result)
        logger.info(f"✅ Task completed: {task.get('url')}")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to process task: {e}")
        return False


def start_consumer():
    """Start consuming messages"""
    print("\n" + "="*70)
    print("🚀 KAFKA CONSUMER STARTING")
    print("="*70 + "\n")

    consumer = create_consumer()
    collection = create_mongo_client()

    logger.info("🚀 Consumer started. Waiting for tasks...\n")

    try:
        message_count = 0
        for message in consumer:
            message_count += 1
            print("\n" + "="*70)
            print(f"📥 Message #{message_count} received")
            print(f"   Topic: {message.topic}")
            print(f"   Partition: {message.partition}")
            print(f"   Offset: {message.offset}")
            print("="*70)

            task = message.value
            success = process_task(task, collection)

            if success:
                print("✅ Task processed successfully\n")
            else:
                print("❌ Task processing failed\n")

    except KeyboardInterrupt:
        logger.info("\n⚠️  Consumer stopped by user")
    except Exception as e:
        logger.error(f"❌ Consumer error: {e}")
    finally:
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    start_consumer()