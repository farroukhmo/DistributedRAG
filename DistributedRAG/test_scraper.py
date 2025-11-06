#!/usr/bin/env python
"""
Test script for the distributed web scraper
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper.scraper_manager import SimpleScraperRunner, DistributedScraperManager
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_simple_scraper():
    """Test basic scraper without distribution"""
    print("\n" + "=" * 60)
    print("Testing Simple Scraper (No Distribution)")
    print("=" * 60 + "\n")

    runner = SimpleScraperRunner()
    runner.run_spider('main_spider')

    print("\n✅ Simple scraper test completed!")
    print("Check MongoDB for scraped data:")
    print("  Database: scraped_data")
    print("  Collection: scraped_items")


def test_distributed_scraper():
    """Test distributed scraper with Ray"""
    print("\n" + "=" * 60)
    print("Testing Distributed Scraper (with Ray)")
    print("=" * 60 + "\n")

    # URLs to scrape in parallel
    test_urls = [
        'http://quotes.toscrape.com/page/1/',
        'http://quotes.toscrape.com/page/2/',
        'http://quotes.toscrape.com/page/3/',
        'http://quotes.toscrape.com/page/4/',
    ]

    manager = DistributedScraperManager(num_workers=2)
    results = manager.distribute_scraping(test_urls)

    print(f"\n📊 Scraping Results: {results}")
    print("\n✅ Distributed scraper test completed!")

    manager.shutdown()


def check_mongodb_connection():
    """Check if MongoDB is accessible"""
    print("\n" + "=" * 60)
    print("Checking MongoDB Connection")
    print("=" * 60 + "\n")

    try:
        from pymongo import MongoClient
        client = MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=2000)
        client.server_info()
        print("✅ MongoDB is running and accessible")

        # Check if data exists
        db = client['scraped_data']
        count = db['scraped_items'].count_documents({})
        print(f"📊 Current items in database: {count}")

        client.close()
        return True
    except Exception as e:
        print(f"❌ MongoDB connection failed: {e}")
        print("\nTo fix:")
        print("1. Install MongoDB or run: docker run -d -p 27017:27017 --name mongodb mongo:latest")
        print("2. Make sure MongoDB service is running")
        return False


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("🕷️  DISTRIBUTED WEB SCRAPER - TEST SUITE")
    print("=" * 60)

    # Check MongoDB first
    mongo_ok = check_mongodb_connection()

    if not mongo_ok:
        print("\n⚠️  Warning: MongoDB not available. Scraper will run but data won't be saved.")
        response = input("\nContinue anyway? (y/n): ")
        if response.lower() != 'y':
            sys.exit(1)

    # Choose test
    print("\nSelect test to run:")
    print("1. Simple Scraper (recommended for first test)")
    print("2. Distributed Scraper (requires Ray)")
    print("3. Both")

    choice = input("\nEnter choice (1-3): ").strip()

    if choice == '1':
        test_simple_scraper()
    elif choice == '2':
        test_distributed_scraper()
    elif choice == '3':
        test_simple_scraper()
        test_distributed_scraper()
    else:
        print("Invalid choice")

    print("\n" + "=" * 60)
    print("✅ All tests completed!")
    print("=" * 60 + "\n")