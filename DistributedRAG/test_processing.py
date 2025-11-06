#!/usr/bin/env python
"""
Test Data Processing Pipeline
"""

import sys
import os

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from processing.processor import DataProcessor
from processing.cleaner import TextCleaner
from pymongo import MongoClient


def test_cleaner():
    """Test the cleaning functions"""
    print("\n" + "=" * 70)
    print("🧪 TESTING TEXT CLEANER")
    print("=" * 70 + "\n")

    cleaner = TextCleaner()

    # Test HTML removal
    test_html = '<div class="quote"><span>"Test quote"</span></div>'
    cleaned = cleaner.remove_html_tags(test_html)
    print(f"✅ HTML Removal Test:")
    print(f"   Input:  {test_html}")
    print(f"   Output: {cleaned}\n")

    # Test quote cleaning
    test_quote = '  "This is a test quote"  '
    cleaned = cleaner.clean_quote_text(test_quote)
    print(f"✅ Quote Cleaning Test:")
    print(f"   Input:  {test_quote}")
    print(f"   Output: {cleaned}\n")

    # Test tag cleaning
    test_tags = ['  Python  ', 'python', 'Data-Science', ' ', 'AI']
    cleaned = cleaner.clean_tags(test_tags)
    print(f"✅ Tag Cleaning Test:")
    print(f"   Input:  {test_tags}")
    print(f"   Output: {cleaned}\n")


def test_small_batch():
    """Test processing a small batch"""
    print("\n" + "=" * 70)
    print("🧪 TESTING SMALL BATCH PROCESSING (5 documents)")
    print("=" * 70 + "\n")

    processor = DataProcessor()

    if not processor.connect():
        print("❌ Cannot connect to MongoDB!")
        return

    try:
        # Get 5 documents
        raw_docs = processor.get_raw_documents(limit=5)

        if not raw_docs:
            print("❌ No documents found!")
            return

        # Process them
        processed, failed = processor.process_documents(raw_docs)

        print(f"\n✅ Results:")
        print(f"   Processed: {len(processed)}")
        print(f"   Failed: {len(failed)}")

        if processed:
            print(f"\n📝 Sample processed document:")
            doc = processed[0]
            print(f"   Quote: {doc['quote'][:60]}...")
            print(f"   Author: {doc['author']}")
            print(f"   Tags: {doc['tags']}")
            print(f"   Stats: {doc['statistics']}")

    finally:
        processor.disconnect()


def test_full_pipeline():
    """Test the full processing pipeline"""
    print("\n" + "=" * 70)
    print("🧪 TESTING FULL PROCESSING PIPELINE")
    print("=" * 70 + "\n")

    processor = DataProcessor()
    success = processor.run_full_pipeline()

    if success:
        print("\n✅ Full pipeline test PASSED!")
    else:
        print("\n❌ Full pipeline test FAILED!")


def verify_processed_data():
    """Verify processed data in MongoDB"""
    print("\n" + "=" * 70)
    print("🔍 VERIFYING PROCESSED DATA")
    print("=" * 70 + "\n")

    try:
        client = MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=2000)
        db = client['scraped_data']

        raw_count = db['scraped_items'].count_documents({})
        processed_count = db['processed_items'].count_documents({})

        print(f"📊 Database Status:")
        print(f"   Raw documents: {raw_count}")
        print(f"   Processed documents: {processed_count}")

        if processed_count > 0:
            # Get sample
            sample = db['processed_items'].find_one()
            print(f"\n📝 Sample Processed Document:")
            print(f"   Quote: {sample.get('quote', 'N/A')[:70]}...")
            print(f"   Author: {sample.get('author', 'N/A')}")
            print(f"   Tags: {', '.join(sample.get('tags', [])[:5])}")
            print(f"   Word Count: {sample.get('statistics', {}).get('word_count', 0)}")
            print(f"   Processed At: {sample.get('processed_at', 'N/A')}")

            print("\n✅ View in MongoDB Compass:")
            print("   Database: scraped_data")
            print("   Collection: processed_items")

        client.close()

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("🔬 DATA PROCESSING TEST SUITE")
    print("=" * 70)

    while True:
        print("\nSelect test:")
        print("1. Test cleaner functions")
        print("2. Test small batch (5 documents)")
        print("3. Run full pipeline (all documents)")
        print("4. Verify processed data")
        print("5. Run all tests")
        print("6. Exit")

        choice = input("\nEnter choice (1-6): ").strip()

        if choice == '1':
            test_cleaner()
        elif choice == '2':
            test_small_batch()
        elif choice == '3':
            test_full_pipeline()
        elif choice == '4':
            verify_processed_data()
        elif choice == '5':
            test_cleaner()
            test_small_batch()
            test_full_pipeline()
            verify_processed_data()
        elif choice == '6':
            print("\nGoodbye! 👋")
            break
        else:
            print("Invalid choice!")