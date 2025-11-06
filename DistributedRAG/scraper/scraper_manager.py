import ray
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import multiprocessing as mp
from typing import List
import logging
import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Initialize Ray for distributed computing
if not ray.is_initialized():
    ray.init(ignore_reinit_error=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@ray.remote
class ScraperWorker:
    """Ray actor for distributed scraping"""

    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.scraped_items = []
        logger.info(f"Worker {worker_id} initialized")

    def scrape_urls(self, urls: List[str], spider_name: str = 'main_spider'):
        """Scrape a list of URLs"""
        from scrapy.crawler import CrawlerRunner
        from twisted.internet import reactor

        # Import spider dynamically
        try:
            from scraper.spiders.main_spider import MainSpider
        except ImportError:
            from spiders.main_spider import MainSpider

        logger.info(f"Worker {self.worker_id} starting to scrape {len(urls)} URLs")

        # Configure settings
        settings = get_project_settings()
        settings.set('LOG_LEVEL', 'INFO')

        runner = CrawlerRunner(settings)

        # Queue spider with URLs
        deferred = runner.crawl(MainSpider, start_urls=urls)
        deferred.addBoth(lambda _: reactor.stop())

        # Run reactor
        if not reactor.running:
            reactor.run(installSignalHandlers=False)

        logger.info(f"Worker {self.worker_id} completed scraping")
        return {"worker_id": self.worker_id, "urls_scraped": len(urls)}


class DistributedScraperManager:
    """Manages distributed web scraping using Ray"""

    def __init__(self, num_workers: int = 4):
        self.num_workers = num_workers
        self.workers = []
        self._initialize_workers()

    def _initialize_workers(self):
        """Initialize Ray workers"""
        logger.info(f"Initializing {self.num_workers} scraper workers...")
        self.workers = [ScraperWorker.remote(i) for i in range(self.num_workers)]
        logger.info("All workers initialized")

    def distribute_scraping(self, urls: List[str], spider_name: str = 'main_spider'):
        """Distribute URLs across workers"""
        if not urls:
            logger.warning("No URLs to scrape")
            return []

        # Split URLs among workers
        chunk_size = len(urls) // self.num_workers + 1
        url_chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]

        logger.info(f"Distributing {len(urls)} URLs across {len(url_chunks)} workers")

        # Submit tasks to workers
        futures = []
        for i, chunk in enumerate(url_chunks):
            if chunk:
                worker = self.workers[i % len(self.workers)]
                future = worker.scrape_urls.remote(chunk, spider_name)
                futures.append(future)

        # Wait for all workers to complete
        results = ray.get(futures)
        logger.info(f"All workers completed. Results: {results}")

        return results

    def shutdown(self):
        """Shutdown Ray"""
        logger.info("Shutting down scraper manager...")
        ray.shutdown()


class SimpleScraperRunner:
    """Simple non-distributed scraper for testing"""

    def __init__(self):
        self.settings = get_project_settings()

    def run_spider(self, spider_name: str = 'main_spider', urls: List[str] = None):
        """Run a spider with optional custom URLs"""
        # Import spiders dynamically
        try:
            from scraper.spiders.main_spider import MainSpider, NewsSpider
        except ImportError:
            from spiders.main_spider import MainSpider, NewsSpider

        process = CrawlerProcess(self.settings)

        if spider_name == 'main_spider':
            spider_class = MainSpider
        elif spider_name == 'news_spider':
            spider_class = NewsSpider
        else:
            spider_class = MainSpider

        # Start crawling
        if urls:
            process.crawl(spider_class, start_urls=urls)
        else:
            process.crawl(spider_class)

        logger.info(f"Starting {spider_name}...")
        process.start()
        logger.info("Scraping completed")


# Example usage
if __name__ == "__main__":
    # Test simple scraper
    print("=== Testing Simple Scraper ===")
    runner = SimpleScraperRunner()
    runner.run_spider('main_spider')

    # Test distributed scraper
    # print("\n=== Testing Distributed Scraper ===")
    # manager = DistributedScraperManager(num_workers=2)
    # test_urls = [
    #     'http://quotes.toscrape.com/page/1/',
    #     'http://quotes.toscrape.com/page/2/',
    #     'http://quotes.toscrape.com/page/3/',
    #     'http://quotes.toscrape.com/page/4/',
    # ]
    # results = manager.distribute_scraping(test_urls)
    # print(f"Scraping results: {results}")
    # manager.shutdown()