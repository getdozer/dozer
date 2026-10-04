"""Qdrant vector-store layer: embeds the credit-card catalog for RAG.

The product catalog is fetched from Dozer's REST API, embedded and stored in
Qdrant. At query time the most semantically similar products are retrieved
and passed to the LLM as extra context (retrieval-augmented generation).

Embeddings use OpenAI when OPENAI_API_KEY is set; otherwise a deterministic
hashing embedder keeps the sample fully runnable offline (CI, air-gapped
demo environments).
"""

from __future__ import annotations

import hashlib
import os
from typing import Any

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_openai import OpenAIEmbeddings
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
EMBED_DIM = 256
COLLECTION = "credit_card_products"


class HashingEmbeddings(Embeddings):
    """Deterministic, dependency-free embeddings for offline/CI runs.

    Bags-of-token-hashes projected onto a fixed-size vector: good enough to
    cluster product descriptions by vocabulary overlap without any model
    download, and stable across runs so tests are reproducible.
    """

    def __init__(self, dim: int = EMBED_DIM) -> None:
        self.dim = dim

    def _embed(self, text: str) -> list[float]:
        vec = [0.0] * self.dim
        for token in text.lower().replace(",", " ").replace(".", " ").split():
            digest = hashlib.sha256(token.encode()).digest()
            index = int.from_bytes(digest[:4], "big") % self.dim
            sign = 1.0 if digest[4] % 2 else -1.0
            vec[index] += sign
        norm = sum(v * v for v in vec) ** 0.5 or 1.0
        return [v / norm for v in vec]

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._embed(t) for t in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text)


def get_embeddings() -> Embeddings:
    if os.getenv("OPENAI_API_KEY"):
        return OpenAIEmbeddings()
    return HashingEmbeddings()


class ProductVectorStore:
    """Thin Langchain-compatible wrapper around Qdrant for the catalog."""

    def __init__(self) -> None:
        self.client = QdrantClient(url=QDRANT_URL, timeout=10)
        self.embeddings = get_embeddings()
        self._ensure_collection()

    def _ensure_collection(self) -> None:
        if not self.client.collection_exists(COLLECTION):
            self.client.create_collection(
                collection_name=COLLECTION,
                vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE),
            )

    def index_products(self, products: list[dict[str, Any]]) -> int:
        """Embed product descriptions from Dozer into Qdrant (idempotent)."""
        if not products:
            return 0
        self.client.delete_collection(COLLECTION)
        self._ensure_collection()
        docs = [
            Document(
                page_content=(
                    f"{p['name']} ({p['category']} card). Annual fee: "
                    f"{p['annual_fee']}. APR: {p['apr']}. Credit limit: "
                    f"{p['credit_limit']}. Rewards: {p['rewards']}. "
                    f"{p['description']}"
                ),
                metadata={"product_id": int(p["product_id"]), "name": p["name"]},
            )
            for p in products
        ]
        vectors = self.embeddings.embed_documents([d.page_content for d in docs])
        self.client.upsert(
            collection_name=COLLECTION,
            points=[
                {
                    "id": d.metadata["product_id"],
                    "vector": vector,
                    "payload": {**d.metadata, "text": d.page_content},
                }
                for d, vector in zip(docs, vectors)
            ],
        )
        return len(docs)

    def search(self, query: str, k: int = 2) -> list[dict[str, Any]]:
        if self.client.count(collection_name=COLLECTION, exact=True).count == 0:
            return []
        vector = self.embeddings.embed_query(query)
        hits = self.client.query_points(collection_name=COLLECTION, query=vector, limit=k).points
        return [
            {"text": h.payload.get("text", ""), "name": h.payload.get("name", ""), "score": h.score}
            for h in hits
        ]
