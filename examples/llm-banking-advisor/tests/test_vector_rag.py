"""Tests for LangChain Vector RAG and Semantic Retrieval."""

import unittest
import sys
from pathlib import Path

SAMPLE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SAMPLE_DIR))

from dozer_client import DozerClient
from guardrails import FinancialGuardrails
from vector_rag import DozerGroundedRetriever, LocalSemanticEmbedder


class TestVectorRAG(unittest.TestCase):
    def setUp(self):
        self.client = DozerClient()
        self.cards = self.client.get_card_catalog()

    def test_semantic_embedder_properties(self):
        """Verify embedding dimension and unit length normalization."""
        embedder = LocalSemanticEmbedder(dim=64)
        emb = embedder.embed("travel flight airport lounge")
        self.assertEqual(len(emb), 64)
        norm = sum(x * x for x in emb)
        self.assertAlmostEqual(norm, 1.0, places=4)

    def test_retriever_ranks_travel_for_traveler(self):
        """Verify traveler profile receives top ranking for travel card among eligible products."""
        customer = self.client.get_customer_profile("CUST_001")
        eligible_cards, _ = FinancialGuardrails.filter_eligible_cards(customer, self.cards)
        retriever = DozerGroundedRetriever(eligible_cards)
        ranked = retriever.rank_cards_for_customer(customer, user_query="international flight perks")
        self.assertGreater(len(ranked), 0)
        top_card, top_score, meta = ranked[0]
        self.assertEqual(top_card.product_id, "CARD_001")
        self.assertGreater(meta["spend_bonus"], 0.0)
        self.assertIn("travel", meta["spend_matches"])

    def test_retriever_ranks_grocery_card_for_family_shopper(self):
        """Verify grocery/supermarket spend elevates grocery card among eligible products."""
        customer = self.client.get_customer_profile("CUST_003")
        eligible_cards, _ = FinancialGuardrails.filter_eligible_cards(customer, self.cards)
        retriever = DozerGroundedRetriever(eligible_cards)
        ranked = retriever.rank_cards_for_customer(customer)
        top_card, top_score, meta = ranked[0]
        self.assertEqual(top_card.product_id, "CARD_002")
        self.assertIn("groceries", meta["spend_matches"])


if __name__ == "__main__":
    unittest.main()
