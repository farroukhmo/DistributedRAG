from rag_module.embeddings import EmbeddingGenerator

# Test
generator = EmbeddingGenerator()
embedding = generator.generate_embedding("Hello world")
print(f"✅ Embedding dimension: {len(embedding)}")
print(f"First 5 values: {embedding[:5]}")