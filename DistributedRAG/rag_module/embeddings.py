# rag_module/embeddings.py
"""
Embedding generator with lazy import and fallback.
"""

from typing import List
import numpy as np
import logging

logger = logging.getLogger(__name__)


class EmbeddingGenerator:
    def __init__(self, model_name: str = "all-MiniLM-L6-v2", dim: int = 384, use_sentence_transformers=True):
        """
        If sentence-transformers is available and use_sentence_transformers is True,
        we will attempt to load it lazily. Otherwise we fall back to deterministic numpy embeddings.
        """
        self.model_name = model_name
        self.dim = dim
        self._st_model = None
        self._use_st = use_sentence_transformers

        if self._use_st:
            try:
                # lazy import
                from sentence_transformers import SentenceTransformer
                self._st_model = SentenceTransformer(self.model_name)
                # if model exposes embedding dimension, update dim
                try:
                    self.dim = self._st_model.get_sentence_embedding_dimension()
                except Exception:
                    pass
                logger.info(f"Loaded SentenceTransformer model: {self.model_name} (dim={self.dim})")
            except Exception as e:
                logger.warning(f"Could not load sentence-transformers: {e}. Falling back to numpy embeddings.")
                self._st_model = None
                self._use_st = False
        else:
            logger.info("Using numpy fallback embeddings (sentence-transformers disabled).")

    def generate_embedding(self, text: str) -> np.ndarray:
        if self._st_model is not None:
            emb = self._st_model.encode(text, show_progress_bar=False)
            return np.asarray(emb)
        # deterministic fallback: hash tokens to vector
        return self._deterministic_numpy_embedding(text)

    def generate_embeddings_batch(self, texts: List[str]) -> List[np.ndarray]:
        if self._st_model is not None:
            embs = self._st_model.encode(texts, show_progress_bar=False)
            return [np.asarray(e) for e in embs]
        return [self._deterministic_numpy_embedding(t) for t in texts]

    def embed_documents_batch(self, documents: List[dict], batch_size: int = 32) -> List[np.ndarray]:
        """
        Accepts list of documents (dicts), extract textual content, and return embeddings list.
        This mirrors what your pipeline expects.
        """
        texts = []
        for doc in documents:
            # choose common content fields, fallback to str(doc)
            text = doc.get('quote') or doc.get('content') or doc.get('text') or str(doc)
            texts.append(text)
        return self.generate_embeddings_batch(texts)

    def _deterministic_numpy_embedding(self, text: str) -> np.ndarray:
        """
        Simple deterministic embedding: hash the text, seed RNG, produce vector.
        Not semantically meaningful but consistent across runs for testing.
        """
        seed = abs(hash(text)) % (2 ** 32)
        rng = np.random.RandomState(seed)
        return rng.rand(self.dim).astype(np.float32)
