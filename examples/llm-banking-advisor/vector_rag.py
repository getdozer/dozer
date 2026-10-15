"""LangChain Vector RAG and semantic catalog search for Dozer Banking Advisor."""

from __future__ import annotations

import hashlib
import math
from typing import Any, Dict, List, Optional, Tuple

from models import CardProduct, CustomerProfile


class LocalSemanticEmbedder:
    """Deterministic token hash embedder for offline / dependency-free vector indexing."""

    def __init__(self, dim: int = 128) -> None:
        self.dim = dim

    def embed(self, text: str) -> List[float]:
        vec = [0.0] * self.dim
        tokens = text.lower().replace(",", " ").replace(";", " ").replace(".", " ").split()
        for tok in tokens:
            h = hashlib.sha256(tok.encode("utf-8")).digest()
            idx = int.from_bytes(h[:4], byteorder="big") % self.dim
            vec[idx] += 1.0
        norm = math.sqrt(sum(x * x for x in vec)) or 1.0
        return [x / norm for x in vec]

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return [self.embed(t) for t in texts]

    def embed_query(self, text: str) -> List[float]:
        return self.embed(text)


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


class DozerGroundedRetriever:
    """LangChain-compatible semantic catalog retriever with transaction category alignment."""

    def __init__(self, cards: List[CardProduct]) -> None:
        self.cards = cards
        self.embedder = LocalSemanticEmbedder()
        self._doc_embeddings: List[Tuple[CardProduct, List[float], str]] = []
        self._index_cards()

    def _card_to_document_text(self, card: CardProduct) -> str:
        return (
            f"Card: {card.name}. Tier: {card.card_tier}. "
            f"Rewards Focus: {card.rewards_focus}. "
            f"Annual Fee: ${card.annual_fee_usd:.0f}. "
            f"Intro Bonus: {card.intro_bonus}. "
            f"Key Perks: {card.perks_summary}. "
            f"Target Audience: {card.target_segment}."
        )

    def _index_cards(self) -> None:
        self._doc_embeddings = []
        for card in self.cards:
            text = self._card_to_document_text(card)
            emb = self.embedder.embed(text)
            self._doc_embeddings.append((card, emb, text))

    def rank_cards_for_customer(
        self,
        customer: CustomerProfile,
        user_query: Optional[str] = None,
        top_k: int = 3,
    ) -> List[Tuple[CardProduct, float, Dict[str, Any]]]:
        """Rank eligible cards using hybrid scoring:

        1. Semantic Query Similarity (from user question/goal)
        2. Transaction Spend Alignment (from Dozer real-time category aggregation)
        3. Stated Goal Match
        """
        query_text = user_query or customer.primary_goals
        q_emb = self.embedder.embed(f"{query_text} {customer.segment} {customer.primary_goals}")

        results: List[Tuple[CardProduct, float, Dict[str, Any]]] = []

        for card, doc_emb, doc_text in self._doc_embeddings:
            # 1. Semantic similarity
            semantic_score = _cosine_similarity(q_emb, doc_emb)

            # 2. Spend Alignment Bonus
            spend_matches: Dict[str, float] = {}
            total_spend = customer.total_spend_usd or 1.0
            spend_bonus = 0.0

            text_lower = doc_text.lower()
            for cat, amt in customer.spend_by_category.items():
                if cat in text_lower or (cat == "dining" and "restaurant" in text_lower):
                    pct = amt / total_spend
                    spend_bonus += pct * 2.0  # proportional boost for where money is spent
                    spend_matches[cat] = amt

            # 3. Goal Alignment
            goal_bonus = 0.0
            for goal_word in customer.primary_goals.lower().split():
                if len(goal_word) > 3 and goal_word in text_lower:
                    goal_bonus += 0.25

            final_score = (semantic_score * 1.5) + spend_bonus + goal_bonus

            details = {
                "semantic_similarity": round(semantic_score, 3),
                "spend_bonus": round(spend_bonus, 3),
                "spend_matches": spend_matches,
                "document_summary": doc_text,
            }
            results.append((card, final_score, details))

        results.sort(key=lambda x: x[1], reverse=True)
        return results[:top_k]
