"""Unit tests for the Dozer + Vector DB + LangChain credit card personalization sample."""

from __future__ import annotations

import unittest

from dozer_client import DozerRestClient
from vector_rag_chatbot import BankingPersonalizationChatbot


class TestDozerBankingChatbot(unittest.TestCase):
    """Validates unified profile materialization, underwriting gates, and vector RAG ranking."""

    def setUp(self) -> None:
        self.client = DozerRestClient(base_url="http://127.0.0.1:59999")
        self.chatbot = BankingPersonalizationChatbot(self.client)

    def test_unified_profile_and_travel_recommendation(self) -> None:
        """High-credit travel customer receives Dozer Horizon Reserve as top match."""
        resp = self.chatbot.recommend(
            customer_id=101,
            user_question="Which card gives me the best airport lounge and flight rewards?",
        )
        self.assertEqual(resp.customer_id, 101)
        self.assertGreaterEqual(len(resp.recommendations), 1)
        self.assertEqual(resp.recommendations[0].card.card_id, "cc_platinum_travel")
        self.assertIn("Dozer Horizon Reserve", resp.answer)

    def test_mid_tier_dining_customer_excludes_high_income_cards(self) -> None:
        """Customer 102 (score 690, income $68k) only receives eligible dining/everyday cards."""
        resp = self.chatbot.recommend(
            customer_id=102,
            user_question="I spend most of my money at restaurants and food delivery.",
        )
        card_ids = [r.card.card_id for r in resp.recommendations]
        self.assertIn("cc_gold_dining", card_ids)
        self.assertNotIn("cc_platinum_travel", card_ids)
        self.assertNotIn("cc_tech_founder", card_ids)

    def test_unverified_kyc_blocks_recommendations(self) -> None:
        """Customer 104 with kyc_verified=false is blocked by the hard eligibility gate."""
        resp = self.chatbot.recommend(
            customer_id=104,
            user_question="Recommend a business cloud card.",
        )
        self.assertEqual(resp.recommendations, [])
        self.assertIn("no credit card products", resp.answer)


if __name__ == "__main__":
    unittest.main()
