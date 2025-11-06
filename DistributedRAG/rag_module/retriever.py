"""
Retriever Module
Smart retrieval system for finding relevant documents
"""

from .embeddings import EmbeddingGenerator
from .vector_store import VectorStore
import logging
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Retriever:
    """Smart document retrieval system"""

    def __init__(self, vector_store: VectorStore, embedding_generator: EmbeddingGenerator):
        """
        Initialize retriever

        Args:
            vector_store: VectorStore instance
            embedding_generator: EmbeddingGenerator instance
        """
        self.vector_store = vector_store
        self.embedding_generator = embedding_generator
        logger.info("✅ Retriever initialized")

    def retrieve(self, query: str, n_results: int = 5, filter_by_author: Optional[str] = None) -> List[Dict]:
        """
        Retrieve relevant documents for a query

        Args:
            query: Search query text
            n_results: Number of results to return
            filter_by_author: Optional author name to filter by

        Returns:
            List of relevant documents with metadata
        """
        logger.info(f"Retrieving documents for query: '{query}'")

        # Search vector store
        results = self.vector_store.search_by_text(
            self.embedding_generator,
            query,
            n_results=n_results * 2 if filter_by_author else n_results
        )

        # Filter by author if specified
        if filter_by_author:
            results = [
                          r for r in results
                          if filter_by_author.lower() in r['author'].lower()
                      ][:n_results]

        logger.info(f"✅ Retrieved {len(results)} documents")
        return results

    def retrieve_by_topic(self, topic: str, n_results: int = 5) -> List[Dict]:
        """
        Retrieve documents by topic/tag

        Args:
            topic: Topic to search for
            n_results: Number of results

        Returns:
            List of relevant documents
        """
        query = f"quotes about {topic}"
        return self.retrieve(query, n_results)

    def retrieve_similar(self, reference_text: str, n_results: int = 5) -> List[Dict]:
        """
        Find documents similar to a reference text

        Args:
            reference_text: Reference text to compare
            n_results: Number of results

        Returns:
            List of similar documents
        """
        logger.info(f"Finding similar documents to: '{reference_text[:50]}...'")
        return self.retrieve(reference_text, n_results)

    def retrieve_by_author(self, author: str, n_results: int = 5) -> List[Dict]:
        """
        Retrieve quotes by a specific author

        Args:
            author: Author name
            n_results: Number of results

        Returns:
            List of documents by that author
        """
        return self.retrieve(
            query=f"quotes by {author}",
            n_results=n_results,
            filter_by_author=author
        )

    def get_context_for_query(self, query: str, max_context_length: int = 2000) -> str:
        """
        Get formatted context for a query (useful for LLM prompts)

        Args:
            query: Search query
            max_context_length: Maximum length of context

        Returns:
            Formatted context string
        """
        # Retrieve relevant documents
        results = self.retrieve(query, n_results=10)

        # Format context
        context_parts = []
        current_length = 0

        for i, result in enumerate(results, 1):
            quote_text = f"{i}. \"{result['quote']}\" - {result['author']}"

            if current_length + len(quote_text) > max_context_length:
                break

            context_parts.append(quote_text)
            current_length += len(quote_text)

        context = "\n".join(context_parts)
        logger.info(f"✅ Generated context with {len(context_parts)} quotes ({len(context)} chars)")

        return context

    def format_results(self, results: List[Dict], include_score: bool = True) -> str:
        """
        Format retrieval results as readable text

        Args:
            results: List of result dictionaries
            include_score: Include similarity scores

        Returns:
            Formatted string
        """
        if not results:
            return "No results found."

        formatted = []
        for i, result in enumerate(results, 1):
            formatted.append(f"\n{i}. \"{result['quote']}\"")
            formatted.append(f"   Author: {result['author']}")

            if result.get('tags'):
                formatted.append(f"   Tags: {', '.join(result['tags'][:3])}")

            if include_score and 'distance' in result:
                # Lower distance = more similar (convert to similarity score)
                similarity = max(0, 1 - result['distance'])
                formatted.append(f"   Relevance: {similarity:.2%}")

        return "\n".join(formatted)


# Test the retriever
if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("🧪 TESTING RETRIEVER")
    print("=" * 70 + "\n")

    # Initialize components
    embedding_gen = EmbeddingGenerator()
    vector_store = VectorStore(persist_directory='./test_chroma_db')
    retriever = Retriever(vector_store, embedding_gen)

    # Test retrieval
    query = "What is the meaning of life?"
    results = retriever.retrieve(query, n_results=3)

    print(f"Query: '{query}'")
    print("=" * 70)
    print(retriever.format_results(results))

    # Test context generation
    print("\n" + "=" * 70)
    print("Testing context generation...")
    print("=" * 70)
    context = retriever.get_context_for_query(query, max_context_length=500)
    print(f"\nContext:\n{context}")

    print("\n" + "=" * 70)
    print("✅ RETRIEVER TEST COMPLETE!")
    print("=" * 70 + "\n")