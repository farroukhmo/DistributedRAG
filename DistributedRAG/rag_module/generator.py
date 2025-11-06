"""
RAG Pipeline with LLM Integration
Combines retrieval with AI generation
"""

from .retriever import Retriever
import logging
from typing import Optional
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RAGPipeline:
    """Complete RAG pipeline with LLM integration"""

    def __init__(self, retriever: Retriever, use_openai: bool = False, api_key: Optional[str] = None):
        """
        Initialize RAG pipeline

        Args:
            retriever: Retriever instance
            use_openai: Whether to use OpenAI API (requires API key)
            api_key: OpenAI API key (optional)
        """
        self.retriever = retriever
        self.use_openai = use_openai

        # Initialize LLM if OpenAI is enabled
        if use_openai:
            try:
                import openai
                self.openai = openai

                if api_key:
                    self.openai.api_key = api_key
                elif os.getenv('OPENAI_API_KEY'):
                    self.openai.api_key = os.getenv('OPENAI_API_KEY')
                else:
                    logger.warning("OpenAI API key not provided. Set OPENAI_API_KEY environment variable.")
                    self.use_openai = False

                logger.info("✅ OpenAI integration enabled")
            except ImportError:
                logger.warning("OpenAI package not installed. Install with: pip install openai")
                self.use_openai = False
        else:
            logger.info("ℹ️  Running without LLM (retrieval-only mode)")

    def generate_response(self, query: str, n_results: int = 5, max_length: int = 500) -> str:
        """
        Generate a response using RAG

        Args:
            query: User query
            n_results: Number of documents to retrieve
            max_length: Maximum response length

        Returns:
            Generated response
        """
        logger.info(f"Generating response for: '{query}'")

        # Step 1: Retrieve relevant documents
        results = self.retriever.retrieve(query, n_results=n_results)

        if not results:
            return "I couldn't find any relevant information to answer your question."

        # Step 2: Generate response
        if self.use_openai:
            return self._generate_with_openai(query, results, max_length)
        else:
            return self._generate_without_llm(query, results)

    def _generate_with_openai(self, query: str, results: list, max_length: int) -> str:
        """Generate response using OpenAI"""
        # Prepare context from retrieved documents
        context = self.retriever.get_context_for_query(query, max_context_length=2000)

        # Create prompt
        prompt = f"""Based on the following quotes, provide a thoughtful answer to the question.

Question: {query}

Relevant Quotes:
{context}

Please provide an insightful answer that:
1. Directly addresses the question
2. References the most relevant quotes
3. Synthesizes ideas from multiple sources
4. Is concise (max {max_length} words)

Answer:"""

        try:
            # Call OpenAI API
            response = self.openai.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system",
                     "content": "You are a helpful assistant that provides insightful answers based on quotes and wisdom from various authors."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=max_length,
                temperature=0.7
            )

            answer = response.choices[0].message.content.strip()
            logger.info("✅ Generated response with OpenAI")
            return answer

        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            return self._generate_without_llm(query, results)

    def _generate_without_llm(self, query: str, results: list) -> str:
        """Generate response without LLM (retrieval-only)"""
        # Format results as a simple response
        response_parts = [
            f"Here are the most relevant quotes for your query: '{query}'\n",
            self.retriever.format_results(results[:3], include_score=True),
            "\n\nThese quotes may help answer your question!"
        ]

        return "\n".join(response_parts)

    def summarize_topic(self, topic: str, n_quotes: int = 5) -> str:
        """
        Generate a summary about a topic

        Args:
            topic: Topic to summarize
            n_quotes: Number of quotes to include

        Returns:
            Summary text
        """
        query = f"What do great thinkers say about {topic}?"
        return self.generate_response(query, n_results=n_quotes)

    def find_related_quotes(self, reference_quote: str, n_results: int = 5) -> str:
        """
        Find quotes related to a reference quote

        Args:
            reference_quote: Reference quote text
            n_results: Number of related quotes

        Returns:
            Formatted response with related quotes
        """
        results = self.retriever.retrieve_similar(reference_quote, n_results=n_results)

        response_parts = [
            f"Quotes similar to: \"{reference_quote[:100]}...\"\n",
            self.retriever.format_results(results, include_score=True)
        ]

        return "\n".join(response_parts)

    def get_author_insights(self, author: str, n_quotes: int = 5) -> str:
        """
        Get insights from a specific author

        Args:
            author: Author name
            n_quotes: Number of quotes

        Returns:
            Formatted response with author's quotes
        """
        results = self.retriever.retrieve_by_author(author, n_results=n_quotes)

        if not results:
            return f"No quotes found for author: {author}"

        response_parts = [
            f"Key insights from {author}:\n",
            self.retriever.format_results(results, include_score=False)
        ]

        return "\n".join(response_parts)


# Test the RAG pipeline
if __name__ == "__main__":
    from .embeddings import EmbeddingGenerator
    from .vector_store import VectorStore

    print("\n" + "=" * 70)
    print("🧪 TESTING RAG PIPELINE")
    print("=" * 70 + "\n")

    # Initialize components
    embedding_gen = EmbeddingGenerator()
    vector_store = VectorStore(persist_directory='./test_chroma_db')
    retriever = Retriever(vector_store, embedding_gen)

    # Initialize RAG pipeline (without OpenAI for testing)
    rag = RAGPipeline(retriever, use_openai=False)

    # Test query
    query = "What is the secret to happiness?"
    print(f"Query: '{query}'")
    print("=" * 70 + "\n")

    response = rag.generate_response(query, n_results=3)
    print(response)

    print("\n" + "=" * 70)
    print("✅ RAG PIPELINE TEST COMPLETE!")
    print("=" * 70 + "\n")