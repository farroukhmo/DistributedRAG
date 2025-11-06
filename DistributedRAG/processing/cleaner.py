"""
Data Cleaning Module
Removes HTML tags, cleans text, and normalizes data
"""

import re
from bs4 import BeautifulSoup
import html
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TextCleaner:
    """Clean and normalize text data"""

    @staticmethod
    def remove_html_tags(text):
        """Remove all HTML tags from text"""
        if not text:
            return ""

        # Parse with BeautifulSoup
        soup = BeautifulSoup(text, 'lxml')

        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()

        # Get text
        text = soup.get_text()

        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)

        return text

    @staticmethod
    def clean_quote_text(text):
        """Clean quote text specifically"""
        if not text:
            return ""

        # Remove HTML entities
        text = html.unescape(text)

        # Remove extra quotes if wrapped
        text = text.strip('"').strip("'").strip()

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)

        # Remove zero-width characters
        text = re.sub(r'[\u200b-\u200d\ufeff]', '', text)

        return text.strip()

    @staticmethod
    def normalize_author_name(author):
        """Normalize author names"""
        if not author:
            return "Unknown"

        # Remove extra whitespace
        author = re.sub(r'\s+', ' ', author).strip()

        # Capitalize properly
        author = author.title()

        return author

    @staticmethod
    def clean_tags(tags):
        """Clean and normalize tags"""
        if not tags:
            return []

        cleaned_tags = []
        for tag in tags:
            if tag:
                # Remove whitespace and lowercase
                tag = tag.strip().lower()
                # Remove special characters
                tag = re.sub(r'[^\w\s-]', '', tag)
                if tag:
                    cleaned_tags.append(tag)

        # Remove duplicates while preserving order
        seen = set()
        unique_tags = []
        for tag in cleaned_tags:
            if tag not in seen:
                seen.add(tag)
                unique_tags.append(tag)

        return unique_tags

    @staticmethod
    def extract_metadata(raw_html):
        """Extract useful metadata from raw HTML"""
        if not raw_html:
            return {}

        soup = BeautifulSoup(raw_html, 'lxml')
        metadata = {}

        # Try to find schema.org data
        quote_div = soup.find('div', class_='quote')
        if quote_div:
            itemtype = quote_div.get('itemtype', '')
            if itemtype:
                metadata['schema_type'] = itemtype

        return metadata

    @staticmethod
    def calculate_text_stats(text):
        """Calculate statistics about the text"""
        if not text:
            return {
                'word_count': 0,
                'char_count': 0,
                'sentence_count': 0
            }

        words = text.split()
        sentences = re.split(r'[.!?]+', text)

        return {
            'word_count': len(words),
            'char_count': len(text),
            'sentence_count': len([s for s in sentences if s.strip()])
        }


class DataNormalizer:
    """Normalize and structure data"""

    @staticmethod
    def normalize_document(raw_doc):
        """Normalize a single document from MongoDB"""
        cleaner = TextCleaner()

        # Extract and clean fields
        quote_text = raw_doc.get('quote_text', '')
        author = raw_doc.get('author', '')
        tags = raw_doc.get('tags', [])
        raw_html = raw_doc.get('raw_html', '')

        # Clean text
        cleaned_quote = cleaner.clean_quote_text(quote_text)
        cleaned_author = cleaner.normalize_author_name(author)
        cleaned_tags = cleaner.clean_tags(tags)

        # Remove HTML from raw_html if needed
        if raw_html and '<' in raw_html:
            text_only = cleaner.remove_html_tags(raw_html)
        else:
            text_only = cleaned_quote

        # Calculate statistics
        stats = cleaner.calculate_text_stats(cleaned_quote)

        # Extract metadata
        metadata = cleaner.extract_metadata(raw_html)

        # Create normalized document
        normalized = {
            'quote': cleaned_quote,
            'author': cleaned_author,
            'tags': cleaned_tags,
            'url': raw_doc.get('url', ''),
            'source': 'quotes.toscrape.com',
            'scraped_at': raw_doc.get('scraped_at', ''),
            'processed_at': raw_doc.get('processed_at', ''),
            'statistics': stats,
            'metadata': metadata,
            'original_id': str(raw_doc.get('_id', ''))
        }

        return normalized

    @staticmethod
    def validate_document(doc):
        """Validate that document has required fields"""
        required_fields = ['quote', 'author']

        for field in required_fields:
            if field not in doc or not doc[field]:
                return False, f"Missing required field: {field}"

        # Check minimum quote length
        if len(doc['quote']) < 10:
            return False, "Quote too short"

        return True, "Valid"

    @staticmethod
    def remove_duplicates(documents):
        """Remove duplicate documents based on quote text"""
        seen = set()
        unique_docs = []
        duplicates = 0

        for doc in documents:
            # Create a signature for the document
            signature = doc['quote'].lower().strip()

            if signature not in seen:
                seen.add(signature)
                unique_docs.append(doc)
            else:
                duplicates += 1
                logger.info(f"Duplicate found: {doc['quote'][:50]}...")

        logger.info(f"Removed {duplicates} duplicates. {len(unique_docs)} unique documents remain.")

        return unique_docs


# Quick test
if __name__ == "__main__":
    # Test cleaning
    cleaner = TextCleaner()

    test_html = '<div class="quote">"Test quote with <b>HTML</b>"</div>'
    cleaned = cleaner.remove_html_tags(test_html)
    print(f"Cleaned HTML: {cleaned}")

    test_quote = '  "This is a test quote"  '
    cleaned_quote = cleaner.clean_quote_text(test_quote)
    print(f"Cleaned quote: {cleaned_quote}")

    test_tags = ['  Python  ', 'python', 'data-science', 'DATA-SCIENCE', '']
    cleaned_tags = cleaner.clean_tags(test_tags)
    print(f"Cleaned tags: {cleaned_tags}")