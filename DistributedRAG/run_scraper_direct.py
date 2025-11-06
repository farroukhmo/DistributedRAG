#!/usr/bin/env python
"""
Direct scraper runner that ensures MongoDB pipeline is enabled
"""

import sys
import os

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from pymongo import MongoClient


def check_mongodb():
    """Check MongoDB connection"""
    try:
        client = MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=2000)
        client.server_info()
        print("✅ MongoDB is running and accessible\n")

        # Check existing data
        db = client['scraped_data']
        count = db['scraped_items'].count_documents({})
        print(f"📊 Current items in database: {count}\n")

        client.close()
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}\n")
        print("Please start MongoDB:")
        print("  docker start mongodb")
        print("  OR")
        print("  docker run -d -p 27017:27017 --name mongodb mongo:latest\n")
        return False


def run_scraper():
    """Run scraper with MongoDB pipeline enabled"""

    print("=" * 70)
    print("🕷️  STARTING WEB SCRAPER WITH MONGODB STORAGE")
    print("=" * 70 + "\n")

    # Check MongoDB first
    if not check_mongodb():
        print("⚠️  Cannot proceed without MongoDB!")
        return

    # Get default settings
    settings = get_project_settings()

    # FORCE enable MongoDB pipeline
    settings.set('ITEM_PIPELINES', {
        'scraper.pipelines.DataCleaningPipeline': 200,
        'scraper.pipelines.MongoPipeline': 300,
    })

    settings.set('MONGO_URI', 'mongodb://localhost:27017')
    settings.set('MONGO_DATABASE', 'scraped_data')
    settings.set('LOG_LEVEL', 'INFO')

    print("🔧 Settings configured:")
    print(f"   - Pipelines: {settings.get('ITEM_PIPELINES')}")
    print(f"   - MongoDB URI: {settings.get('MONGO_URI')}")
    print(f"   - Database: {settings.get('MONGO_DATABASE')}\n")

    # Import spider
    from scraper.spiders.main_spider import MainSpider

    # Create crawler process
    process = CrawlerProcess(settings)

    print("🚀 Starting scraper...\n")
    print("=" * 70)

    # Start crawling
    process.crawl(MainSpider)
    process.start()  # This blocks until crawling is finished

    print("\n" + "=" * 70)
    print("✅ SCRAPING COMPLETED!")
    print("=" * 70 + "\n")

    # Verify data was saved
    verify_data()


def verify_data():
    """Verify data was saved to MongoDB"""
    try:
        client = MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=2000)
        db = client['scraped_data']
        count = db['scraped_items'].count_documents({})

        print(f"📊 Total items in database: {count}")

        if count > 0:
            print("\n✅ SUCCESS! Data was saved to MongoDB")
            print("\nView your data:")
            print("  1. MongoDB Compass: mongodb://localhost:27017")
            print("  2. Command line: docker exec -it mongodb mongosh")
            print("  3. Python script: python view_data.py")

            # Show sample
            print("\n📝 Sample quote:")
            sample = db['scraped_items'].find_one()
            if sample:
                print(f"   Quote: {sample.get('quote_text', 'N/A')[:80]}...")
                print(f"   Author: {sample.get('author', 'N/A')}")
                print(f"   Tags: {', '.join(sample.get('tags', []))}")
        else:
            print("\n❌ No data was saved!")
            print("Check the logs above for errors.")

        client.close()

    except Exception as e:
        print(f"❌ Error checking database: {e}")


if __name__ == "__main__":
    run_scraper()