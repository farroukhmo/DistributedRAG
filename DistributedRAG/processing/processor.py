"""
Data Processing Pipeline
Processes raw scraped data and stores cleaned results
"""

from pymongo import MongoClient
from datetime import datetime
import logging
import time
from .cleaner import TextCleaner, DataNormalizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessor:
    """Main data processing pipeline"""

    def __init__(self, mongo_uri='mongodb://localhost:27017', database='scraped_data'):
        self.mongo_uri = mongo_uri
        self.database = database
        self.client = None
        self.db = None
        self.normalizer = DataNormalizer()

    def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=2000)
            self.client.server_info()
            self.db = self.client[self.database]
            logger.info(f"✅ Connected to MongoDB: {self.database}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {e}")
            return False

    def disconnect(self):
        """Disconnect from MongoDB"""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")

    def get_raw_documents(self, limit=None):
        """Get raw documents from scraped_items collection"""
        collection = self.db['scraped_items']

        if limit:
            documents = list(collection.find().limit(limit))
        else:
            documents = list(collection.find())

        logger.info(f"Retrieved {len(documents)} raw documents")
        return documents

    def process_documents(self, documents):
        """Process a batch of documents"""
        processed = []
        failed = []

        start_time = time.time()

        for i, doc in enumerate(documents, 1):
            try:
                # Normalize document
                normalized = self.normalizer.normalize_document(doc)

                # Add processing timestamp
                normalized['processed_at'] = datetime.now().isoformat()

                # Validate
                is_valid, message = self.normalizer.validate_document(normalized)

                if is_valid:
                    processed.append(normalized)
                    if i % 20 == 0:
                        logger.info(f"Processed {i}/{len(documents)} documents...")
                else:
                    logger.warning(f"Invalid document: {message}")
                    failed.append({'doc': doc, 'reason': message})

            except Exception as e:
                logger.error(f"Error processing document: {e}")
                failed.append({'doc': doc, 'reason': str(e)})

        # Remove duplicates
        processed = self.normalizer.remove_duplicates(processed)

        elapsed_time = time.time() - start_time

        logger.info(f"✅ Processing complete!")
        logger.info(f"   - Processed: {len(processed)} documents")
        logger.info(f"   - Failed: {len(failed)} documents")
        logger.info(f"   - Time: {elapsed_time:.2f} seconds")
        logger.info(f"   - Speed: {len(documents)/elapsed_time:.2f} docs/second")

        return processed, failed

    def save_processed_data(self, documents, collection_name='processed_items'):
        """Save processed documents to new collection"""
        if not documents:
            logger.warning("No documents to save")
            return 0

        collection = self.db[collection_name]

        # Clear existing data (optional)
        # collection.delete_many({})

        try:
            result = collection.insert_many(documents)
            logger.info(f"✅ Saved {len(result.inserted_ids)} documents to '{collection_name}'")
            return len(result.inserted_ids)
        except Exception as e:
            logger.error(f"❌ Error saving documents: {e}")
            return 0

    def get_processing_stats(self):
        """Get statistics about processed data"""
        raw_collection = self.db['scraped_items']
        processed_collection = self.db['processed_items']

        raw_count = raw_collection.count_documents({})
        processed_count = processed_collection.count_documents({})

        stats = {
            'raw_documents': raw_count,
            'processed_documents': processed_count,
            'processing_rate': f"{processed_count/raw_count*100:.1f}%" if raw_count > 0 else "0%"
        }

        # Get sample processed document
        sample = processed_collection.find_one()

        return stats, sample

    def run_full_pipeline(self):
        """Run the complete processing pipeline"""
        logger.info("="*70)
        logger.info("🔄 STARTING DATA PROCESSING PIPELINE")
        logger.info("="*70)

        # Connect
        if not self.connect():
            return False

        try:
            # Get raw data
            logger.info("\n📥 Step 1: Fetching raw data...")
            raw_docs = self.get_raw_documents()

            if not raw_docs:
                logger.warning("No raw documents found!")
                return False

            # Process data
            logger.info(f"\n🔄 Step 2: Processing {len(raw_docs)} documents...")
            processed_docs, failed = self.process_documents(raw_docs)

            # Save processed data
            logger.info("\n💾 Step 3: Saving processed data...")
            saved_count = self.save_processed_data(processed_docs)

            # Show statistics
            logger.info("\n📊 Step 4: Processing Statistics")
            logger.info("="*70)
            stats, sample = self.get_processing_stats()

            for key, value in stats.items():
                logger.info(f"   {key}: {value}")

            if sample:
                logger.info("\n📝 Sample processed document:")
                logger.info(f"   Quote: {sample['quote'][:80]}...")
                logger.info(f"   Author: {sample['author']}")
                logger.info(f"   Tags: {', '.join(sample['tags'][:3])}")
                logger.info(f"   Word count: {sample['statistics']['word_count']}")

            logger.info("\n" + "="*70)
            logger.info("✅ PROCESSING PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("="*70)

            return True

        finally:
            self.disconnect()


# Run processing
if __name__ == "__main__":
    processor = DataProcessor()
    processor.run_full_pipeline()