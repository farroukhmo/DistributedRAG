"""
Phase 4 Testing Script
Tests all RAG components and integration
"""

import logging
from integrate_rag_with_scraper import ScraperRAGIntegration
from rag_module.embeddings import EmbeddingGenerator
from rag_module.vector_store import VectorStore
from rag_module.retriever import Retriever
from rag_module.generator import RAGPipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_embedding_generation():
    """Test 1: Embedding Generation"""
    print("\n" + "=" * 70)
    print("TEST 1: EMBEDDING GENERATION")
    print("=" * 70)

    try:
        generator = EmbeddingGenerator()

        # Test single embedding
        test_text = "The only way to do great work is to love what you do."
        embedding = generator.generate_embedding(test_text)

        print(f"✅ Single Embedding Test:")
        print(f"   Text: {test_text}")
        print(f"   Embedding shape: {embedding.shape}")
        print(f"   First 5 values: {embedding[:5]}")

        # Test batch embeddings
        test_texts = [
            "Life is what happens when you are busy making other plans.",
            "The only impossible journey is the one you never begin."
        ]
        embeddings = generator.generate_embeddings_batch(test_texts)

        print(f"\n✅ Batch Embedding Test:")
        print(f"   Number of texts: {len(test_texts)}")
        print(f"   Embeddings generated: {len(embeddings)}")

        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_vector_store():
    """Test 2: Vector Store Operations"""
    print("\n" + "=" * 70)
    print("TEST 2: VECTOR STORE OPERATIONS")
    print("=" * 70)

    try:
        # Initialize
        embedding_gen = EmbeddingGenerator()
        vector_store = VectorStore(persist_directory='./test_chroma_db')

        # Clear existing data
        vector_store.clear_collection()

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
        for i, result in enumerate(results, 1):
            print(f"\n{i}. {result['quote']}")
            print(f"   Author: {result['author']}")
            print(f"   Distance: {result['distance']:.4f}")

        # Stats
        stats = vector_store.get_collection_stats()
        print(f"\n📊 Collection Stats:")
        print(f"   Documents: {stats['count']}")

        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_retriever():
    """Test 3: Retriever Functionality"""
    print("\n" + "=" * 70)
    print("TEST 3: RETRIEVER FUNCTIONALITY")
    print("=" * 70)

    try:
        # Initialize components
        embedding_gen = EmbeddingGenerator()
        vector_store = VectorStore(persist_directory='./test_chroma_db')
        retriever = Retriever(vector_store, embedding_gen)

        # Test retrieval
        query = "What is the meaning of life?"
        results = retriever.retrieve(query, n_results=2)

        print(f"Query: '{query}'")
        print(retriever.format_results(results))

        # Test context generation
        context = retriever.get_context_for_query(query, max_context_length=500)
        print(f"\n✅ Context Generated:")
        print(f"   Length: {len(context)} characters")
        print(f"   Preview: {context[:200]}...")

        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_rag_pipeline():
    """Test 4: RAG Pipeline"""
    print("\n" + "=" * 70)
    print("TEST 4: RAG PIPELINE")
    print("=" * 70)

    try:
        # Initialize components
        embedding_gen = EmbeddingGenerator()
        vector_store = VectorStore(persist_directory='./test_chroma_db')
        retriever = Retriever(vector_store, embedding_gen)

        # Initialize RAG pipeline (without OpenAI for testing)
        rag = RAGPipeline(retriever, use_openai=False)

        # Test query
        query = "What is the secret to happiness?"
        print(f"Query: '{query}'")

        response = rag.generate_response(query, n_results=2)
        print(f"\n💡 Response:\n{response}")

        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_mongodb_integration():
    """Test 5: MongoDB Integration"""
    print("\n" + "=" * 70)
    print("TEST 5: MONGODB INTEGRATION")
    print("=" * 70)

    try:
        # Initialize integration
        integrator = ScraperRAGIntegration(
            mongo_uri='mongodb://localhost:27017/',
            db_name='quotes_db',
            collection_name='quotes',
            vector_db_path='./test_chroma_db'
        )

        # Check health
        integrator.check_system_health()

        # Fetch sample data
        docs = integrator.fetch_scraped_data(limit=5)
        print(f"\n✅ Fetched {len(docs)} documents from MongoDB")

        if docs:
            print("\nSample document:")
            doc = docs[0]
            print(f"   Quote: {doc.get('quote', '')[:100]}...")
            print(f"   Author: {doc.get('author', 'Unknown')}")

        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        print("⚠️  Make sure MongoDB is running and has data!")
        import traceback
        traceback.print_exc()
        return False


def test_full_pipeline():
    """Test 6: Full Pipeline Integration"""
    print("\n" + "=" * 70)
    print("TEST 6: FULL PIPELINE INTEGRATION")
    print("=" * 70)

    try:
        # Initialize integration
        integrator = ScraperRAGIntegration(
            mongo_uri='mongodb://localhost:27017/',
            db_name='quotes_db',
            collection_name='quotes',
            vector_db_path='./test_chroma_db_full'
        )

        # Process small batch for testing
        print("\n📥 Fetching and processing data...")
        docs = integrator.fetch_scraped_data(limit=10)

        if not docs:
            print("⚠️  No documents found. Please run scraper first (Phases 1-3)")
            return False

        # Generate embeddings for sample
        embeddings = integrator.embedding_gen.embed_documents_batch(docs[:10])

        # Add to vector store
        ids = [str(doc.get('_id', f'doc_{i}')) for i, doc in enumerate(docs[:10])]
        integrator.vector_store.add_documents(docs[:10], embeddings, ids=ids)

        print(f"✅ Indexed {len(docs[:10])} documents")

        # Test query
        query = "What is success?"
        print(f"\n🔍 Testing query: '{query}'")
        response = integrator.query_rag_system(query, n_results=3)
        print(f"\n💡 Response:\n{response}")

        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def run_all_tests():
    """Run all Phase 4 tests"""
    print("\n" + "=" * 70)
    print("🚀 PHASE 4 - RAG INTEGRATION TEST SUITE")
    print("=" * 70)

    tests = [
        ("Embedding Generation", test_embedding_generation),
        ("Vector Store Operations", test_vector_store),
        ("Retriever Functionality", test_retriever),
        ("RAG Pipeline", test_rag_pipeline),
        ("MongoDB Integration", test_mongodb_integration),
        ("Full Pipeline Integration", test_full_pipeline)
    ]

    results = []

    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ Test '{test_name}' failed with error: {e}")
            results.append((test_name, False))

    # Summary
    print("\n" + "=" * 70)
    print("📊 TEST SUMMARY")
    print("=" * 70)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{status}: {test_name}")

    print("\n" + "=" * 70)
    print(f"Results: {passed}/{total} tests passed")
    print("=" * 70 + "\n")

    if passed == total:
        print("🎉 All tests passed! Phase 4 is complete!")
    else:
        print("⚠️  Some tests failed. Please check the errors above.")


if __name__ == "__main__":
    run_all_tests()