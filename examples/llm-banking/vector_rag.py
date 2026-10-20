"""
Vector RAG module for credit card product similarity retrieval.
Computes vector embeddings and similarity scores to match customer spending profiles
with optimal credit card benefits and features.
"""

import math
import re
from typing import Dict, List, Any


def tokenize(text: str) -> List[str]:
    return re.findall(r"\b[a-zA-Z0-9_]+\b", text.lower())


class SimpleVectorStore:
    """
    Lightweight, self-contained vector database implementation.
    Supports cosine similarity retrieval over embedded documents.
    """

    def __init__(self):
        self.documents: List[Dict[str, Any]] = []
        self.vocab: Dict[str, int] = {}
        self.idf: Dict[str, float] = {}

    def _build_vocab(self):
        vocab_set = set()
        doc_count = len(self.documents)
        df: Dict[str, int] = {}

        for doc in self.documents:
            tokens = set(tokenize(doc["content"]))
            for t in tokens:
                vocab_set.add(t)
                df[t] = df.get(t, 0) + 1

        self.vocab = {term: idx for idx, term in enumerate(sorted(vocab_set))}
        self.idf = {
            term: math.log((doc_count + 1) / (df.get(term, 0) + 1)) + 1.0
            for term in self.vocab
        }

    def _embed(self, text: str) -> List[float]:
        tokens = tokenize(text)
        vec = [0.0] * len(self.vocab)
        if not tokens:
            return vec

        tf: Dict[str, int] = {}
        for t in tokens:
            tf[t] = tf.get(t, 0) + 1

        for t, count in tf.items():
            if t in self.vocab:
                idx = self.vocab[t]
                vec[idx] = (count / len(tokens)) * self.idf[t]

        # Normalize vector
        norm = math.sqrt(sum(x * x for x in vec))
        if norm > 0:
            vec = [x / norm for x in vec]
        return vec

    def add_documents(self, docs: List[Dict[str, Any]]):
        self.documents = list(docs)
        self._build_vocab()
        for doc in self.documents:
            doc["vector"] = self._embed(doc["content"])

    def similarity_search(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        if not self.documents:
            return []
        q_vec = self._embed(query)
        scored = []
        for doc in self.documents:
            dot = sum(a * b for a, b in zip(q_vec, doc["vector"]))
            scored.append((dot, doc))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [item[1] for item in scored[:top_k]]


def initialize_card_vector_store(products: List[Dict[str, Any]]) -> SimpleVectorStore:
    store = SimpleVectorStore()
    docs = []
    for p in products:
        content = (
            f"{p['card_name']} {p['primary_category']} category. "
            f"Annual fee: ${p['annual_fee']}. Perks: {p['perks_summary']}."
        )
        docs.append({
            "product_id": p["product_id"],
            "card_name": p["card_name"],
            "primary_category": p["primary_category"],
            "annual_fee": float(p["annual_fee"]),
            "min_credit_score": int(p["min_credit_score"]),
            "perks_summary": p["perks_summary"],
            "content": content,
        })
    store.add_documents(docs)
    return store
