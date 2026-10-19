import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import DozerRestClient, PersonalizedBankingChatbot


class TestDozerLlmLangchainSample(unittest.TestCase):
    def setUp(self) -> None:
        self.client = DozerRestClient(base_url="http://127.0.0.1:59999")
        self.bot = PersonalizedBankingChatbot(self.client)

    def test_unified_customer_profile_aggregation(self) -> None:
        profile = self.client.get_unified_customer_profile(101)
        self.assertEqual(profile.full_name, "Alice Johnson")
        self.assertEqual(profile.credit_score, 765)
        self.assertEqual(profile.total_transactions, 3)
        self.assertAlmostEqual(profile.total_spend, 1155.75, places=2)

    def test_eligibility_and_vector_recommendation_for_travel_customer(self) -> None:
        res = self.bot.ask(
            customer_id=101,
            user_question="I want a card with airport lounge access and flight points.",
        )
        self.assertIn("CARD_TRAVEL_PRO", res["recommended_cards"])
        self.assertEqual(res["recommended_cards"][0], "CARD_TRAVEL_PRO")
        self.assertIn("Horizon Sapphire Travel Card", res["answer"])

    def test_eligibility_gate_filters_high_requirement_cards(self) -> None:
        # Customer 102 has credit_score=690, annual_income=68000 -> only eligible for CARD_CASH_EVERYDAY
        res = self.bot.ask(
            customer_id=102,
            user_question="Which card gives me the best cashback on groceries and gas?",
        )
        self.assertEqual(res["recommended_cards"], ["CARD_CASH_EVERYDAY"])
        self.assertIn("Everyday Cashback Plus Card", res["answer"])


if __name__ == "__main__":
    unittest.main()
