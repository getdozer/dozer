#!/usr/bin/env python3
"""
Hyper-personalized Banking RAG Pipeline using Dozer, ChromaDB, and LangChain.
Reflects the architecture described in: https://getdozer.io/blog/llm-chatbot
"""

import json
import os
import sys
from typing import Any, Dict, List, Optional
import requests

try:
    import chromadb
    from chromadb.utils import embedding_functions
except ImportError:
    chromadb = None


class DozerClient:
    """Client for querying Dozer real-time streaming REST APIs."""

    def __init__(self, base_url: str = "http://localhost:8080"):
        self.base_url = base_url.rstrip("/")

    def get_customer_profiles(self) -> List[Dict[str, Any]]:
        """Fetch unified customer profiles computed in real-time by Dozer."""
        url = f"{self.base_url}/customer_profiles"
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return resp.json()
        except requests.RequestException:
            pass

        # Built-in fallback sample dataset when running without live Dozer service
        return [
            {
                "customer_id": 1,
                "customer_name": "Alice Chen",
                "credit_score": 760,
                "monthly_income": 6500.0,
                "total_transactions": 3,
                "total_spent": 2210.0,
                "has_travel_spend": 1,
                "has_dining_spend": 1,
            },
            {
                "customer_id": 2,
                "customer_name": "Bob Miller",
                "credit_score": 640,
                "monthly_income": 3800.0,
                "total_transactions": 2,
                "total_spent": 530.0,
                "has_travel_spend": 0,
                "has_dining_spend": 0,
            },
            {
                "customer_id": 3,
                "customer_name": "Carlos Gomez",
                "credit_score": 720,
                "monthly_income": 5200.0,
                "total_transactions": 2,
                "total_spent": 770.0,
                "has_travel_spend": 0,
                "has_dining_spend": 1,
            },
        ]

    def get_card_products(self) -> List[Dict[str, Any]]:
        """Fetch available credit card products from Dozer."""
        url = f"{self.base_url}/available_cards"
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return resp.json()
        except requests.RequestException:
            pass

        return [
            {
                "id": 1,
                "card_name": "AeroMiles Voyager",
                "target_category": "Travel",
                "reward_rate": 5.0,
                "annual_fee": 95.0,
                "min_credit_score": 720,
                "description": "Premium travel card offering 5x miles on flights and hotels, complimentary lounge access.",
            },
            {
                "id": 2,
                "card_name": "Gourmet Cashback",
                "target_category": "Dining",
                "reward_rate": 4.0,
                "annual_fee": 0.0,
                "min_credit_score": 680,
                "description": "Zero fee card tailored for foodies with 4% unlimited cashback on dining and food deliveries.",
            },
            {
                "id": 3,
                "card_name": "Everyday Saver Plus",
                "target_category": "Groceries",
                "reward_rate": 3.0,
                "annual_fee": 0.0,
                "min_credit_score": 620,
                "description": "Designed for budget management with 3% cashback on supermarkets and gas stations.",
            },
            {
                "id": 4,
                "card_name": "Executive Platinum Reserve",
                "target_category": "General",
                "reward_rate": 2.0,
                "annual_fee": 450.0,
                "min_credit_score": 750,
                "description": "Luxury card with concierge services, travel insurances and 2x points on all expenses.",
            },
        ]


class VectorKnowledgeStore:
    """Manages indexing and similarity search over banking products using ChromaDB."""

    def __init__(self, persist_directory: Optional[str] = None):
        self.persist_directory = persist_directory
        if chromadb:
            if persist_directory:
                self.client = chromadb.PersistentClient(path=persist_directory)
            else:
                self.client = chromadb.Client()
            self.collection = self.client.get_or_create_collection(
                name="credit_card_products",
                metadata={"hnsw:space": "cosine"}
            )
        else:
            self.client = None
            self.collection = None
            self.in_memory_docs: List[Dict[str, Any]] = []

    def index_card_products(self, cards: List[Dict[str, Any]]) -> None:
        """Embed and store card products into vector database."""
        if not self.collection:
            self.in_memory_docs = cards
            return

        documents = []
        metadatas = []
        ids = []

        for card in cards:
            text = (
                f"Card: {card['card_name']} | Category: {card['target_category']} | "
                f"Reward Rate: {card['reward_rate']}% | Annual Fee: ${card['annual_fee']} | "
                f"Minimum Credit Score Required: {card['min_credit_score']} | "
                f"Details: {card.get('description', '')}"
            )
            documents.append(text)
            metadatas.append({
                "card_name": card["card_name"],
                "target_category": card["target_category"],
                "min_credit_score": card["min_credit_score"],
                "annual_fee": card["annual_fee"],
                "reward_rate": card["reward_rate"]
            })
            ids.append(f"card_{card['id']}")

        self.collection.upsert(documents=documents, metadatas=metadatas, ids=ids)

    def retrieve_matching_cards(self, query: str, min_score: int, n_results: int = 2) -> List[Dict[str, Any]]:
        """Retrieve relevant products matching user persona and eligibility."""
        if not self.collection:
            # Fallback simple keyword / score filter
            matched = [c for c in self.in_memory_docs if c["min_credit_score"] <= min_score]
            return matched[:n_results]

        results = self.collection.query(
            query_texts=[query],
            n_results=min(n_results * 2, self.collection.count() or 1)
        )

        matched_cards = []
        if results and "metadatas" in results and results["metadatas"]:
            for meta in results["metadatas"][0]:
                if meta["min_credit_score"] <= min_score:
                    matched_cards.append(meta)
                if len(matched_cards) >= n_results:
                    break

        return matched_cards


class HyperPersonalizedChatbot:
    """
    RAG Chatbot orchestrating Dozer customer profile context,
    Chroma vector search, and LangChain prompt construction.
    """

    def __init__(self, dozer_client: Optional[DozerClient] = None, vector_store: Optional[VectorKnowledgeStore] = None):
        self.dozer = dozer_client or DozerClient()
        self.vector_store = vector_store or VectorKnowledgeStore()
        # Pre-seed vector store
        cards = self.dozer.get_card_products()
        self.vector_store.index_card_products(cards)

    def generate_recommendation_prompt(self, customer_id: int, user_query: str) -> Dict[str, Any]:
        """Build contextual prompt for LLM based on Dozer live customer CDC data."""
        profiles = self.dozer.get_customer_profiles()
        customer = next((p for p in profiles if p["customer_id"] == customer_id), None)

        if not customer:
            raise ValueError(f"Customer with ID {customer_id} not found in Dozer stream.")

        # Determine customer intent query based on real-time spending
        spending_context = []
        if customer.get("has_travel_spend"):
            spending_context.append("travel expenses and flights")
        if customer.get("has_dining_spend"):
            spending_context.append("dining and restaurants")
        if not spending_context:
            spending_context.append("everyday grocery and utility spending")

        search_query = f"{user_query} {' and '.join(spending_context)}"
        retrieved_cards = self.vector_store.retrieve_matching_cards(
            query=search_query,
            min_score=customer["credit_score"],
            n_results=2
        )

        prompt = f"""
You are an intelligent banking assistant providing hyper-personalized financial advice.

### REAL-TIME CUSTOMER CONTEXT (FROM DOZER CDC):
- Name: {customer['customer_name']}
- Credit Score: {customer['credit_score']} (Eligible)
- Monthly Income: ${customer['monthly_income']:,.2f}
- Recent Total Transactions: {customer['total_transactions']}
- Total Spent: ${customer['total_spent']:,.2f}
- Key Spending Trends: {', '.join(spending_context)}

### RELEVANT CREDIT PRODUCTS (FROM CHROMA VECTOR RETRIEVAL):
{json.dumps(retrieved_cards, indent=2)}

### USER INQUIRY:
"{user_query}"

### INSTRUCTIONS:
Recommend the most suitable card tailored to {customer['customer_name']}'s specific spending habits.
Highlight why the card maximizes their rewards based on their Dozer-tracked transaction history.
"""
        return {
            "customer": customer,
            "retrieved_cards": retrieved_cards,
            "prompt": prompt.strip()
        }

    def simulate_llm_response(self, customer_id: int, user_query: str) -> str:
        """Generates a complete synthesized response matching the hyper-personalized prompt."""
        data = self.generate_recommendation_prompt(customer_id, user_query)
        customer = data["customer"]
        cards = data["retrieved_cards"]

        if not cards:
            return f"Hello {customer['customer_name']}, based on your current credit score of {customer['credit_score']}, our advisors will review tailored options for you."

        top_card = cards[0]
        return (
            f"Hello {customer['customer_name']}! 👋\n\n"
            f"Based on your recent transaction activity, our analytics system noted frequent expenses in "
            f"{top_card['target_category']}.\n\n"
            f"We highly recommend the **{top_card['card_name']}**:\n"
            f"• Reward Rate: **{top_card['reward_rate']}% cashback/rewards** on {top_card['target_category']}\n"
            f"• Annual Fee: ${top_card['annual_fee']}\n"
            f"• Match Reason: Your credit score of {customer['credit_score']} qualifies you instantly for this product!\n\n"
            f"Would you like to activate this card today?"
        )


def main():
    print("=" * 70)
    print("🚀 DOZER + CHROMADB + LANGCHAIN RAG HYPER-PERSONALIZATION PIPELINE")
    print("=" * 70)

    api_url = os.environ.get("DOZER_API_URL", "http://localhost:8080")
    persist_dir = os.environ.get("CHROMA_PERSIST_DIR", None)

    dozer = DozerClient(base_url=api_url)
    vstore = VectorKnowledgeStore(persist_directory=persist_dir)
    chatbot = HyperPersonalizedChatbot(dozer_client=dozer, vector_store=vstore)

    # Test with Alice Chen (Customer ID 1: Heavy Travel & Dining)
    query = "What is the best credit card to maximize rewards on my regular spending?"
    print("\n--- TEST CASE: Alice Chen (Traveler Persona) ---")
    response = chatbot.simulate_llm_response(customer_id=1, user_query=query)
    print(response)

    # Test with Bob Miller (Customer ID 2: Everyday Saver)
    print("\n--- TEST CASE: Bob Miller (Everyday Grocery Persona) ---")
    response_bob = chatbot.simulate_llm_response(customer_id=2, user_query=query)
    print(response_bob)
    print("=" * 70)


if __name__ == "__main__":
    main()
