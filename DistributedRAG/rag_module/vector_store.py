"""
Vector Store using ChromaDB
Stores and retrieves document embeddings
"""

import chromadb
from chromadb.config import Settings
import logging
from typing import List, Dict, Optional
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VectorStore:
    """Manage vector embeddings in ChromaDB"""

    def __init__(self, persist_directory='./chroma_db', collection_name='quotes'):
        """
        Initialize ChromaDB vector store

        Args:
            persist_directory: Directory to store the database
            collection_name: Name of the collection
        """
        self.persist_directory = persist_directory
        self.collection_name = collection_name

        logger.info(f"Initializing ChromaDB at {persist_directory}")

        # Initialize ChromaDB client
        self.client = chromadb.PersistentClient(path=persist_directory)

        # Get or create collection
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"description": "Quote embeddings for RAG"}
        )

        logger.info(f"✅ Collection '{collection_name}' ready. Documents: {self.collection.count()}")

    def add_documents(self, documents, embeddings, ids=None):
        """
        Add documents with their embeddings to the vector store

        Args:
            documents: List of document dictionaries
            embeddings: List of embedding arrays
            ids: Optional list of IDs (will be auto-generated if not provided)
        """
        if len(documents) != len(embeddings):
            raise ValueError("Number of documents must match number of embeddings")

        # Generate IDs if not provided
        if ids is None:
            existing_count = self.collection.count()
            ids = [f"doc_{existing_count + i}" for i in range(len(documents))]

        # Prepare data for ChromaDB
        texts = [doc.get('quote', '') for doc in documents]

        # Prepare metadata
        metadatas = [
            {
                'author': doc.get('author', 'Unknown'),
                'tags': json.dumps(doc.get('tags', [])),
                'source': doc.get('source', ''),
                'url': doc.get('url', ''),
                'word_count': str(doc.get('statistics', {}).get('word_count', 0))
            }
            for doc in documents
        ]

        # Convert embeddings to list format
        embeddings_list = [emb.tolist() for emb in embeddings]

        logger.info(f"Adding {len(documents)} documents to vector store...")

        # Add to collection
        self.collection.add(
            embeddings=embeddings_list,
            documents=texts,
            metadatas=metadatas,
            ids=ids
        )

        logger.info(f"✅ Added {len(documents)} documents. Total: {self.collection.count()}")

    def search(self, query_embedding, n_results=5):
        """
        Search for similar documents

        Args:
            query_embedding: Query embedding array
            n_results: Number of results to return

        Returns:
            Search results with documents and distances
        """
        # Convert to list format
        query_embedding_list = query_embedding.tolist()

        # Search
        results = self.collection.query(
            query_embeddings=[query_embedding_list],
            n_results=n_results
        )

        return results

    def search_by_text(self, embedding_generator, query_text, n_results=5):
        """
        Search using text query (will generate embedding)

        Args:
            embedding_generator: EmbeddingGenerator instance
            query_text: Text query
            n_results: Number of results

        Returns:
            Formatted search results
        """
        # Generate query embedding
        query_embedding = embedding_generator.generate_embedding(query_text)

        # Search
        results = self.search(query_embedding, n_results=n_results)

        # Format results
        formatted_results = []
        for i in range(len(results['ids'][0])):
            formatted_results.append({
                'id': results['ids'][0][i],
                'quote': results['documents'][0][i],
                'author': results['metadatas'][0][i].get('author', 'Unknown'),
                'tags': json.loads(results['metadatas'][0][i].get('tags', '[]')),
                'distance': results['distances'][0][i],
                'url': results['metadatas'][0][i].get('url', '')
            })

        return formatted_results

    def get_collection_stats(self):
        """Get statistics about the collection"""
        return {
            'name': self.collection_name,
            'count': self.collection.count(),
            'persist_directory': self.persist_directory
        }

    def clear_collection(self):
        """Clear all documents from collection"""
        logger.warning(f"Clearing collection '{self.collection_name}'")
        self.client.delete_collection(self.collection_name)
        self.collection = self.client.create_collection(
            name=self.collection_name,
            metadata={"description": "Quote embeddings for RAG"}
        )
        logger.info("✅ Collection cleared")

    def delete_by_ids(self, ids):
        """Delete documents by IDs"""
        self.collection.delete(ids=ids)
        logger.info(f"Deleted {len(ids)} documents")


# Test the vector store
if __name__ == "__main__":
    from .embeddings import EmbeddingGenerator

    print("\n" + "=" * 70)
    print("🧪 TESTING VECTOR STORE")
    print("=" * 70 + "\n")

    # Initialize
    embedding_gen = EmbeddingGenerator()
    vector_store = VectorStore(persist_directory='./test_chroma_db')

    # Test documents
    test_docs = [
        {
            'quote': 'Life is what happens when you are busy making other plans.',
            'author': 'John Lennon',
            'tags': ['life', 'planning'],
            'source': 'test',
            'statistics': {'word_count': 12}
        },
        {
            'quote': 'The only way to do great work is to love what you do.',
            'author': 'Steve Jobs',
            'tags': ['work', 'passion'],
            'source': 'test',
            'statistics': {'word_count': 13}
        }
    ]

    # Generate embeddings
    embeddings = embedding_gen.embed_documents_batch(test_docs)

    # Add to vector store
    vector_store.add_documents(test_docs, embeddings)

    # Test search
    query = "What is the meaning of life?"
    results = vector_store.search_by_text(embedding_gen, query, n_results=2)

    print(f"\n✅ Search Results for: '{query}'")
    print("=" * 70)
    for i, result in enumerate(results, 1):
        print(f"\n{i}. {result['quote']}")
        print(f"   Author: {result['author']}")
        print(f"   Distance: {result['distance']:.4f}")

    # Stats
    stats = vector_store.get_collection_stats()
    print(f"\n📊 Collection Stats:")
    print(f"   Name: {stats['name']}")
    print(f"   Documents: {stats['count']}")

    print("\n" + "=" * 70)
    print("✅ VECTOR STORE TEST COMPLETE!")
    print("=" * 70 + "\n")