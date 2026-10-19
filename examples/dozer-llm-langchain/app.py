"""Dozer + Vector Database + LangChain hyper-personalized banking chatbot sample."""

from __future__ import annotations

import csv
import math
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
import urllib.request
import json


BASE_DIR = Path(__file__).resolve().parent


@dataclass
class CustomerProfileContext:
    customer_id: int
    full_name: str
    credit_score: int
    annual_income: float
    preferred_reward_category: str
    total_transactions: int
    total_spend: float


@dataclass
class CreditCardProduct:
    card_id: str
    card_name: str
    min_credit_score: int
    min_annual_income: float
    annual_fee: float
    reward_category: str
    benefits_summary: str


class DozerRestClient:
    """Queries Dozer's real-time REST endpoints with local CSV fallback for offline testing."""

    def __init__(self, base_url: str = "http://localhost:8080", data_dir: Optional[Path] = None):
        self.base_url = base_url.rstrip("/")
        self.data_dir = data_dir or (BASE_DIR / "data")

    def _post_json(self, path: str, payload: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        url = f"{self.base_url}{path}/query"
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception:
            return None

    def get_unified_customer_profile(self, customer_id: int) -> CustomerProfileContext:
        records = self._post_json(
            "/unified_customer_profile",
            {"$filter": {"customer_id": customer_id}, "$limit": 1},
        )
        if records and len(records) > 0:
            row = records[0]
            return CustomerProfileContext(
                customer_id=int(row["customer_id"]),
                full_name=str(row["full_name"]),
                credit_score=int(row["credit_score"]),
                annual_income=float(row["annual_income"]),
                preferred_reward_category=str(row["preferred_reward_category"]),
                total_transactions=int(row["total_transactions"]),
                total_spend=float(row["total_spend"]),
            )

        # Fallback: compute the same join directly from the CSV sources
        customers_csv = self.data_dir / "customer_profiles" / "customer_profiles.csv"
        tx_csv = self.data_dir / "transactions" / "transactions.csv"

        customer_row: Optional[Dict[str, str]] = None
        with customers_csv.open("r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if int(r["customer_id"]) == customer_id:
                    customer_row = r
                    break

        if not customer_row:
            raise ValueError(f"Customer {customer_id} not found in Dozer dataset.")

        tx_count = 0
        tx_sum = 0.0
        with tx_csv.open("r", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if int(r["customer_id"]) == customer_id:
                    tx_count += 1
                    tx_sum += float(r["amount"])

        return CustomerProfileContext(
            customer_id=customer_id,
            full_name=customer_row["full_name"],
            credit_score=int(customer_row["credit_score"]),
            annual_income=float(customer_row["annual_income"]),
            preferred_reward_category=customer_row["preferred_reward_category"],
            total_transactions=tx_count,
            total_spend=round(tx_sum, 2),
        )

    def list_credit_card_products(self) -> List[CreditCardProduct]:
        records = self._post_json("/credit_card_products", {"$limit": 50})
        if records is None:
            cards_csv = self.data_dir / "credit_card_products" / "credit_card_products.csv"
            with cards_csv.open("r", encoding="utf-8") as f:
                records = list(csv.DictReader(f))

        return [
            CreditCardProduct(
                card_id=str(r["card_id"]),
                card_name=str(r["card_name"]),
                min_credit_score=int(r["min_credit_score"]),
                min_annual_income=float(r["min_annual_income"]),
                annual_fee=float(r["annual_fee"]),
                reward_category=str(r["reward_category"]),
                benefits_summary=str(r["benefits_summary"]),
            )
            for r in records
        ]


class CreditCardVectorStore:
    """Lightweight cosine-similarity vector store (with optional Chroma/LangChain backend)."""

    def __init__(self, products: List[CreditCardProduct]):
        self.products = products
        self.vocabulary = self._build_vocabulary(products)

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        return [
            tok.strip(".,!?:;()[]").lower()
            for tok in text.split()
            if tok.strip(".,!?:;()[]")
        ]

    def _build_vocabulary(self, products: List[CreditCardProduct]) -> List[str]:
        vocab = set()
        for p in products:
            doc = f"{p.card_name} {p.reward_category} {p.benefits_summary}"
            vocab.update(self._tokenize(doc))
        return sorted(vocab)

    def _embed(self, text: str) -> List[float]:
        tokens = self._tokenize(text)
        vec = [float(tokens.count(word)) for word in self.vocabulary]
        norm = math.sqrt(sum(v * v for v in vec)) or 1.0
        return [v / norm for v in vec]

    def similarity_search(
        self,
        query: str,
        profile: CustomerProfileContext,
        top_k: int = 2,
    ) -> List[CreditCardProduct]:
        eligible = [
            p
            for p in self.products
            if profile.credit_score >= p.min_credit_score
            and profile.annual_income >= p.min_annual_income
        ]
        if not eligible:
            return []

        enriched_query = f"{query} {profile.preferred_reward_category}"
        q_vec = self._embed(enriched_query)

        scored = []
        for product in eligible:
            doc = f"{product.card_name} {product.reward_category} {product.benefits_summary}"
            p_vec = self._embed(doc)
            score = sum(a * b for a, b in zip(q_vec, p_vec))
            if product.reward_category.lower() == profile.preferred_reward_category.lower():
                score += 0.5
            scored.append((score, product))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [item[1] for item in scored[:top_k]]


class PersonalizedBankingChatbot:
    """Combines Dozer unified customer context + Vector DB retrieval + LangChain prompt formatting."""

    def __init__(
        self,
        dozer_client: DozerRestClient,
        llm_callable: Optional[Callable[[str], str]] = None,
    ):
        self.dozer_client = dozer_client
        self.products = self.dozer_client.list_credit_card_products()
        self.vector_store = CreditCardVectorStore(self.products)
        self.llm_callable = llm_callable

    def build_prompt(
        self,
        profile: CustomerProfileContext,
        matched_cards: List[CreditCardProduct],
        user_question: str,
    ) -> str:
        cards_block = "\n".join(
            f"- {c.card_name} ({c.card_id}): category={c.reward_category}, "
            f"annual_fee=${c.annual_fee:.0f}, benefits={c.benefits_summary}"
            for c in matched_cards
        )
        return (
            "You are a hyper-personalized banking assistant powered by Dozer real-time customer context.\n"
            f"Customer Profile:\n"
            f"  Name: {profile.full_name} (ID: {profile.customer_id})\n"
            f"  Credit Score: {profile.credit_score}\n"
            f"  Annual Income: ${profile.annual_income:,.2f}\n"
            f"  Preferred Rewards: {profile.preferred_reward_category}\n"
            f"  Recent Transactions: {profile.total_transactions} totaling ${profile.total_spend:,.2f}\n\n"
            f"Eligible Credit Card Products (Retrieved from Vector Store):\n{cards_block}\n\n"
            f"Customer Question: {user_question}\n"
            "Provide a concise, personalized recommendation citing the customer's profile and best card match."
        )

    def ask(self, customer_id: int, user_question: str) -> Dict[str, Any]:
        profile = self.dozer_client.get_unified_customer_profile(customer_id)
        matched_cards = self.vector_store.similarity_search(user_question, profile)
        prompt = self.build_prompt(profile, matched_cards, user_question)

        if self.llm_callable is not None:
            answer = self.llm_callable(prompt)
        elif os.environ.get("OPENAI_API_KEY"):
            try:
                from langchain.chat_models import ChatOpenAI
                from langchain.schema import HumanMessage

                chat = ChatOpenAI(temperature=0.2)
                answer = chat([HumanMessage(content=prompt)]).content
            except Exception:
                answer = self._default_response(profile, matched_cards)
        else:
            answer = self._default_response(profile, matched_cards)

        return {
            "customer_profile": profile,
            "recommended_cards": [c.card_id for c in matched_cards],
            "prompt": prompt,
            "answer": answer,
        }

    @staticmethod
    def _default_response(
        profile: CustomerProfileContext, matched_cards: List[CreditCardProduct]
    ) -> str:
        if not matched_cards:
            return (
                f"Hello {profile.full_name}, based on your current profile we do not have a matching "
                "card product at this moment."
            )
        top = matched_cards[0]
        return (
            f"Hi {profile.full_name}! Based on your {profile.preferred_reward_category} preference, "
            f"credit score of {profile.credit_score}, and ${profile.total_spend:,.2f} in recent spend "
            f"across {profile.total_transactions} transactions, we recommend the **{top.card_name}** "
            f"(${top.annual_fee:.0f}/yr): {top.benefits_summary}"
        )


if __name__ == "__main__":
    client = DozerRestClient()
    bot = PersonalizedBankingChatbot(client)
    result = bot.ask(
        customer_id=101,
        user_question="Which credit card should I get for my upcoming international flights and hotel stays?",
    )
    print(result["answer"])
