"""
RAG (Retrieval-Augmented Generation) Module
"""
from .vector_store import VectorStore
from .embeddings import EmbeddingGenerator

# These will be created next
# from .retriever import Retriever
# from .generator import RAGPipeline

__all__ = ['VectorStore', 'EmbeddingGenerator']