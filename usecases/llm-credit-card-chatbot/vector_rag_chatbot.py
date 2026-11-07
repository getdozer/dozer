"""Vector search and LangChain RAG pipeline grounded on Dozer customer profiles."""

from __future__ import annotations

import math
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Sequence

from dozer_client import CreditCardProduct, CustomerProfile, DozerRestClient


@dataclass(frozen=True)
class ScoredCardRecommendation:
    """Represents a ranked credit card recommendation retrieved from the vector index."""

    card: CreditCardProduct
    similarity_score: float
    estimated_annual_rewards: float


@dataclass(frozen=True)
class ChatbotResponse:
    """Structured output returned by the Dozer + Vector DB + LangChain advisor."""

    customer_id: int
    prompt_context: str
    recommendations: List[ScoredCardRecommendation]
    answer: str


class CardVectorIndex:
    """Indexes credit card product descriptions for semantic similarity retrieval."""

    def __init__(self, cards: Sequence[CreditCardProduct]) -> None:
        self._cards = list(cards)
        self._doc_vectors = [_embed_text(_card_document(card)) for card in self._cards]

    def search(
        self,
        query: str,
        profile: CustomerProfile,
        top_k: int = 2,
    ) -> List[ScoredCardRecommendation]:
        """Ranks eligible cards by semantic similarity combined with preferred spend category."""
        enriched_query = f"{query} {profile.preferred_category}"
        query_vec = _embed_text(enriched_query)
        scored: List[ScoredCardRecommendation] = []
        for card, doc_vec in zip(self._cards, self._doc_vectors):
            sim = _cosine_similarity(query_vec, doc_vec)
            if card.primary_reward_category == profile.preferred_category:
                sim = round(min(1.0, sim + 0.25), 4)
            annual_rewards = round(profile.total_spend * (card.cashback_pct / 100.0), 2)
            scored.append(
                ScoredCardRecommendation(
                    card=card,
                    similarity_score=round(sim, 4),
                    estimated_annual_rewards=annual_rewards,
                )
            )
        scored.sort(
            key=lambda item: (item.similarity_score, item.estimated_annual_rewards),
            reverse=True,
        )
        return scored[:top_k]


class BankingPersonalizationChatbot:
    """Combines Dozer real-time customer profiles, vector search, and LangChain generation."""

    def __init__(self, dozer_client: DozerRestClient) -> None:
        self.dozer_client = dozer_client

    def recommend(self, customer_id: int, user_question: str, top_k: int = 2) -> ChatbotResponse:
        """Generates a hyper-personalized credit card recommendation for a banking customer."""
        profile = self.dozer_client.get_customer_profile(customer_id)
        if profile is None:
            raise ValueError(f"Customer {customer_id} not found in Dozer profile store")
        eligible_cards = self.dozer_client.filter_eligible_cards(profile)
        if not eligible_cards:
            context = _build_prompt_context(profile, [], user_question)
            return ChatbotResponse(
                customer_id=customer_id,
                prompt_context=context,
                recommendations=[],
                answer=(
                    f"Hello {profile.full_name}, no credit card products currently match "
                    "your verification or underwriting profile."
                ),
            )
        index = CardVectorIndex(eligible_cards)
        recommendations = index.search(user_question, profile, top_k=top_k)
        context = _build_prompt_context(profile, recommendations, user_question)
        answer = _invoke_langchain_or_deterministic(profile, recommendations, context)
        return ChatbotResponse(
            customer_id=customer_id,
            prompt_context=context,
            recommendations=recommendations,
            answer=answer,
        )


def _card_document(card: CreditCardProduct) -> str:
    """Formats a credit card record into a searchable semantic document."""
    return (
        f"{card.card_name} ({card.tier}) category {card.primary_reward_category} "
        f"cashback {card.cashback_pct}% fee ${card.annual_fee}: {card.benefits_summary}"
    )


def _embed_text(text: str) -> Dict[str, float]:
    """Produces a normalized term-frequency embedding vector for deterministic RAG retrieval."""
    tokens = re.findall(r"[a-z0-9_]+", text.lower())
    counts: Dict[str, float] = {}
    for token in tokens:
        counts[token] = counts.get(token, 0.0) + 1.0
    norm = math.sqrt(sum(val * val for val in counts.values())) or 1.0
    return {token: val / norm for token, val in counts.items()}


def _cosine_similarity(vec_a: Dict[str, float], vec_b: Dict[str, float]) -> float:
    """Computes cosine similarity between two sparse normalized vectors."""
    return sum(weight * vec_b.get(token, 0.0) for token, weight in vec_a.items())


def _build_prompt_context(
    profile: CustomerProfile,
    recommendations: Sequence[ScoredCardRecommendation],
    user_question: str,
) -> str:
    """Constructs the LangChain system context from Dozer's unified profile and vector matches."""
    cards_block = "\n".join(
        f"- {rec.card.card_name} ({rec.card.cashback_pct}% on {rec.card.primary_reward_category}, "
        f"${rec.card.annual_fee:.0f}/yr fee, score={rec.similarity_score:.2f}): {rec.card.benefits_summary}"
        for rec in recommendations
    )
    return (
        f"Customer: {profile.full_name} (ID={profile.customer_id})\n"
        f"Credit Score: {profile.credit_score} | Annual Income: ${profile.annual_income:,.2f}\n"
        f"Recent Spend: ${profile.total_spend:,.2f} across {profile.tx_count} transactions "
        f"(Preferred Category: {profile.preferred_category})\n"
        f"Eligible Vector-Retrieved Cards:\n{cards_block}\n"
        f"Question: {user_question}"
    )


def _invoke_langchain_or_deterministic(
    profile: CustomerProfile,
    recommendations: Sequence[ScoredCardRecommendation],
    context: str,
) -> str:
    """Uses LangChain ChatOpenAI when OPENAI_API_KEY is configured, otherwise formats grounded advice."""
    if os.environ.get("OPENAI_API_KEY"):
        try:
            from langchain.prompts import PromptTemplate
            from langchain.schema.output_parser import StrOutputParser
            from langchain_openai import ChatOpenAI

            prompt = PromptTemplate.from_template(
                "You are a banking advisor powered by Dozer real-time customer profiles.\n"
                "{context}\nProvide a concise, personalized recommendation:"
            )
            chain = prompt | ChatOpenAI(temperature=0) | StrOutputParser()
            return str(chain.invoke({"context": context}))
        except Exception:
            pass
    top = recommendations[0]
    return (
        f"Hi {profile.full_name}, based on your {profile.preferred_category} spend of "
        f"${profile.total_spend:,.2f} and credit score of {profile.credit_score}, we recommend "
        f"{top.card.card_name} ({top.card.cashback_pct}% rewards on {top.card.primary_reward_category}, "
        f"estimated ${top.estimated_annual_rewards:,.2f} in rewards). {top.card.benefits_summary}"
    )
