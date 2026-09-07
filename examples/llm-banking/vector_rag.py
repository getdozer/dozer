"""Vector retrieval for card docs.

Tries LangChain + Chroma when installed. Otherwise uses a tiny in-process
cosine index with the same hash embeddings so the sample still runs.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from typing import Any


@dataclass
class Doc:
    page_content: str
    metadata: dict[str, Any]


class HashEmbedder:
    def __init__(self, dim: int = 64) -> None:
        self.dim = dim

    def embed(self, text: str) -> list[float]:
        vec = [0.0] * self.dim
        for tok in text.lower().split():
            h = hashlib.sha256(tok.encode()).digest()
            idx = int.from_bytes(h[:4], "big") % self.dim
            vec[idx] += 1.0
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]


def build_docs(profile_text: str, products: list[dict[str, str]], product_text_fn) -> list[Doc]:
    docs = [Doc(page_content=profile_text, metadata={"kind": "customer"})]
    for p in products:
        docs.append(
            Doc(
                page_content=product_text_fn(p),
                metadata={"kind": "product", "product_id": p["product_id"]},
            )
        )
    return docs


def _cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def retrieve_local(docs: list[Doc], query: str, k: int = 3) -> list[Doc]:
    emb = HashEmbedder()
    q = emb.embed(query)
    scored = [(_cosine(q, emb.embed(d.page_content)), d) for d in docs]
    scored.sort(key=lambda t: t[0], reverse=True)
    return [d for _, d in scored[:k]]


def retrieve(docs: list[Doc], query: str, k: int = 3) -> list[Doc]:
    try:
        from langchain_community.vectorstores import Chroma
        from langchain_core.documents import Document

        class _Emb:
            def __init__(self) -> None:
                self.inner = HashEmbedder()

            def embed_documents(self, texts: list[str]) -> list[list[float]]:
                return [self.inner.embed(t) for t in texts]

            def embed_query(self, text: str) -> list[float]:
                return self.inner.embed(text)

        lc_docs = [Document(page_content=d.page_content, metadata=d.metadata) for d in docs]
        store = Chroma.from_documents(lc_docs, embedding=_Emb())
        hits = store.similarity_search(query, k=k)
        return [Doc(page_content=h.page_content, metadata=dict(h.metadata)) for h in hits]
    except Exception:
        return retrieve_local(docs, query, k=k)
