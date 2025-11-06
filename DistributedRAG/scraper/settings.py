# Scrapy settings for distributed scraper project

BOT_NAME = 'distributed_rag_scraper'

SPIDER_MODULES = ['scraper.spiders']
NEWSPIDER_MODULE = 'scraper.spiders'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests
CONCURRENT_REQUESTS = 16

# Configure a delay for requests (in seconds)
DOWNLOAD_DELAY = 1

# Disable cookies
COOKIES_ENABLED = False

# Override the default request headers
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

# IMPORTANT: Enable pipelines!
ITEM_PIPELINES = {
    'scraper.pipelines.DataCleaningPipeline': 200,
    'scraper.pipelines.MongoPipeline': 300,
}

# MongoDB Configuration
MONGO_URI = 'mongodb://localhost:27017'
MONGO_DATABASE = 'scraped_data'

# Enable HTTP caching
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 86400
HTTPCACHE_DIR = 'httpcache'

# Configure logging
LOG_LEVEL = 'INFO'