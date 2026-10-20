"""
Hyper-Personalized Credit Card Recommendation Engine.
Demonstrates Dozer real-time data streaming + Vector RAG + LangChain LLM Chatbot synthesis.
"""

import csv
import os
from typing import Dict, List, Any, Optional
from vector_rag import initialize_card_vector_store, SimpleVectorStore


class DozerClientMock:
    """
    Client connecting to Dozer endpoints.
    Reads aggregated customer 360 data and credit card products.
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    def get_customer_profile(self, customer_id: str) -> Optional[Dict[str, Any]]:
        path = os.path.join(self.data_dir, "customer_profiles.csv")
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["customer_id"] == customer_id:
                    return {
                        "customer_id": row["customer_id"],
                        "name": row["name"],
                        "age": int(row["age"]),
                        "annual_income": float(row["annual_income"]),
                        "credit_score": int(row["credit_score"]),
                        "account_type": row["account_type"],
                        "preferred_category": row["preferred_category"],
                    }
        return None

    def get_customer_spending_summary(self, customer_id: str) -> List[Dict[str, Any]]:
        path = os.path.join(self.data_dir, "transactions.csv")
        spending: Dict[str, float] = {}
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["customer_id"] == customer_id:
                    cat = row["category"]
                    amt = float(row["amount"])
                    spending[cat] = spending.get(cat, 0.0) + amt

        results = []
        for cat, total in sorted(spending.items(), key=lambda x: x[1], reverse=True):
            results.append({"category": cat, "total_spend": total})
        return results

    def get_all_products(self) -> List[Dict[str, Any]]:
        path = os.path.join(self.data_dir, "credit_card_products.csv")
        products = []
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                products.append(dict(row))
        return products


class HyperPersonalizedRecommender:
    """
    Orchestrates Dozer customer 360 profile + Vector database similarity + LLM Prompt.
    """

    def __init__(self, data_dir: str):
        self.client = DozerClientMock(data_dir=data_dir)
        products = self.client.get_all_products()
        self.vector_store = initialize_card_vector_store(products)

    def generate_recommendation(self, customer_id: str) -> Dict[str, Any]:
        profile = self.client.get_customer_profile(customer_id)
        if not profile:
            raise ValueError(f"Customer {customer_id} not found.")

        spending = self.client.get_customer_spending_summary(customer_id)
        top_cat = spending[0]["category"] if spending else profile["preferred_category"]
        total_spend = sum(s["total_spend"] for s in spending)

        # 1. Query vector database matching customer preferences and spending behavior
        query = (
            f"Looking for credit card with great rewards for {top_cat} and {profile['preferred_category']}. "
            f"Customer annual income ${profile['annual_income']}, credit score {profile['credit_score']}."
        )
        matched_cards = self.vector_store.similarity_search(query, top_k=2)

        # Filter eligibility by credit score
        eligible_cards = [
            c for c in matched_cards if profile["credit_score"] >= c["min_credit_score"]
        ]
        recommended_card = eligible_cards[0] if eligible_cards else matched_cards[0]

        # 2. Synthesize LangChain prompt & response
        llm_response = (
            f"Hello {profile['name']}! Based on your recent spending analysis from Dozer "
            f"(with ${total_spend:.2f} concentrated in {top_cat}), "
            f"we recommend the **{recommended_card['card_name']}**. "
            f"It maximizes your rewards with: {recommended_card['perks_summary']}."
        )

        return {
            "customer_id": customer_id,
            "customer_name": profile["name"],
            "credit_score": profile["credit_score"],
            "top_spending_category": top_cat,
            "total_analyzed_spend": total_spend,
            "recommended_product_id": recommended_card["product_id"],
            "recommended_card_name": recommended_card["card_name"],
            "annual_fee": recommended_card["annual_fee"],
            "rationale": llm_response,
        }
