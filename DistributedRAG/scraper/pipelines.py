from pymongo import MongoClient
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DataCleaningPipeline:
    """Clean and validate scraped data before storage"""

    def process_item(self, item, spider):
        # Remove empty values
        item = {k: v for k, v in item.items() if v is not None and v != ''}

        # Add metadata
        item['processed_at'] = datetime.now().isoformat()
        item['spider_name'] = spider.name

        # Clean text fields
        if 'quote_text' in item and item['quote_text']:
            item['quote_text'] = item['quote_text'].strip()

        if 'author' in item and item['author']:
            item['author'] = item['author'].strip()

        logger.info(f"Cleaned item: {item.get('quote_text', 'N/A')[:50]}")
        return item


class MongoPipeline:
    """Store scraped data in MongoDB"""

    collection_name = 'scraped_items'

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.client = None
        self.db = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI', 'mongodb://localhost:27017'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'scraped_data')
        )

    def open_spider(self, spider):
        """Connect to MongoDB when spider opens"""
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]
            logger.info(f"Connected to MongoDB: {self.mongo_db}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            logger.warning("Items will not be stored in database")

    def close_spider(self, spider):
        """Close MongoDB connection when spider closes"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

    def process_item(self, item, spider):
        """Insert item into MongoDB"""
        if self.db is not None:
            try:
                # Insert into collection
                result = self.db[self.collection_name].insert_one(dict(item))
                logger.info(f"Item inserted with ID: {result.inserted_id}")
            except Exception as e:
                logger.error(f"Error inserting item into MongoDB: {e}")
        else:
            logger.warning("MongoDB not connected, item not saved")

        return item


class PostgreSQLPipeline:
    """Alternative: Store scraped data in PostgreSQL"""

    def __init__(self):
        self.connection = None
        self.cursor = None

    def open_spider(self, spider):
        """Connect to PostgreSQL"""
        try:
            import psycopg2
            self.connection = psycopg2.connect(
                host="localhost",
                database="scraped_data",
                user="postgres",
                password="password"
            )
            self.cursor = self.connection.cursor()

            # Create table if not exists
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS scraped_items (
                    id SERIAL PRIMARY KEY,
                    quote_text TEXT,
                    author VARCHAR(255),
                    tags TEXT[],
                    url TEXT,
                    scraped_at TIMESTAMP,
                    raw_html TEXT
                )
            """)
            self.connection.commit()
            logger.info("Connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")

    def close_spider(self, spider):
        """Close PostgreSQL connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed")

    def process_item(self, item, spider):
        """Insert item into PostgreSQL"""
        if self.connection and self.cursor:
            try:
                self.cursor.execute("""
                    INSERT INTO scraped_items (quote_text, author, tags, url, scraped_at, raw_html)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    item.get('quote_text'),
                    item.get('author'),
                    item.get('tags', []),
                    item.get('url'),
                    item.get('scraped_at'),
                    item.get('raw_html')
                ))
                self.connection.commit()
                logger.info("Item inserted into PostgreSQL")
            except Exception as e:
                logger.error(f"Error inserting item into PostgreSQL: {e}")
                self.connection.rollback()

        return item