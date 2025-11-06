import scrapy
from bs4 import BeautifulSoup
from datetime import datetime
import json


class MainSpider(scrapy.Spider):
    name = 'main_spider'
    allowed_domains = ['quotes.toscrape.com']
    start_urls = ['http://quotes.toscrape.com/']

    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_DELAY': 0.5,
        'ROBOTSTXT_OBEY': True,
    }

    def __init__(self, *args, **kwargs):
        super(MainSpider, self).__init__(*args, **kwargs)
        self.scraped_count = 0

    def parse(self, response):
        """Parse main page and extract quotes"""

        # Use BeautifulSoup for parsing
        soup = BeautifulSoup(response.text, 'lxml')

        # Extract all quotes from the page
        quotes = soup.find_all('div', class_='quote')

        for quote in quotes:
            # Extract data using BeautifulSoup
            text = quote.find('span', class_='text').get_text() if quote.find('span', class_='text') else None
            author = quote.find('small', class_='author').get_text() if quote.find('small', class_='author') else None
            tags = [tag.get_text() for tag in quote.find_all('a', class_='tag')]

            # Prepare data item
            item = {
                'quote_text': text,
                'author': author,
                'tags': tags,
                'url': response.url,
                'scraped_at': datetime.now().isoformat(),
                'raw_html': str(quote)  # Store raw HTML for later processing
            }

            self.scraped_count += 1
            self.logger.info(f'Scraped quote #{self.scraped_count}: {text[:50]}...')

            yield item

        # Follow pagination links
        next_page = soup.find('li', class_='next')
        if next_page:
            next_link = next_page.find('a')
            if next_link and next_link.get('href'):
                next_url = response.urljoin(next_link['href'])
                self.logger.info(f'Following next page: {next_url}')
                yield scrapy.Request(next_url, callback=self.parse)

        # Log completion
        self.logger.info(f'Total quotes scraped so far: {self.scraped_count}')


class NewsSpider(scrapy.Spider):
    """Alternative spider for news articles - customize as needed"""
    name = 'news_spider'

    def __init__(self, start_url=None, *args, **kwargs):
        super(NewsSpider, self).__init__(*args, **kwargs)
        self.start_urls = [start_url] if start_url else ['http://quotes.toscrape.com/']

    def parse(self, response):
        """Parse news articles"""
        soup = BeautifulSoup(response.text, 'lxml')

        # Extract article data (customize selectors based on target site)
        item = {
            'title': soup.find('h1').get_text() if soup.find('h1') else None,
            'content': soup.find('article').get_text() if soup.find('article') else None,
            'url': response.url,
            'scraped_at': datetime.now().isoformat(),
            'raw_html': response.text
        }

        yield item