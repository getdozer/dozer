import unittest
from pathlib import Path
import sys

SAMPLE_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SAMPLE_ROOT))

from src.advisor import DozerLangChainBankingAdvisor
from src.dozer_client import DozerFeatureStoreClient
from src.vector_store import CreditCardVectorKnowledgeBase


class TestDozerLLMLangChainSample(unittest.TestCase):
    def setUp(self) -> None:
        self.client = DozerFeatureStoreClient(base_url="http://127.0.0.1:59999", timeout_sec=0.05)
        self.kb = CreditCardVectorKnowledgeBase.from_markdown_file()
        self.advisor = DozerLangChainBankingAdvisor(
            dozer_client=self.client,
            vector_kb=self.kb,
        )

    def test_customer_profile_and_realtime_spending_aggregation(self) -> None:
        profile = self.client.get_customer_profile("cust_101")
        self.assertEqual(profile.full_name, "Alice Rivera")
        self.assertEqual(profile.credit_score, 765)

        spend = {s.category: s.total_spend for s in self.client.get_customer_spending_summary("cust_101")}
        self.assertAlmostEqual(spend["travel"], 4250.00, places=2)
        self.assertAlmostEqual(spend["dining"], 420.00, places=2)
        self.assertAlmostEqual(spend["grocery"], 310.00, places=2)

    def test_vector_knowledge_base_semantic_retrieval(self) -> None:
        hits = self.kb.similarity_search("airport lounge access international travel precheck", top_k=2)
        self.assertEqual(len(hits), 2)
        top_ids = {h.document.card_id for h in hits}
        self.assertTrue(
            "card_sapphire_voyager" in top_ids or "card_zenith_reserve" in top_ids
        )
        self.assertGreater(hits[0].score, 0.0)

    def test_alice_underwriting_filters_zenith_and_recommends_sapphire_voyager(self) -> None:
        # Alice has credit_score=765, utilization=14.5%, but annual_income=$118,000 (< $130,000 for Zenith Reserve)
        resp = self.advisor.recommend(
            "cust_101",
            "Which travel card gives me airport lounge access and the highest net travel rewards?",
        )
        self.assertEqual(resp.recommended_card.card.card_id, "card_sapphire_voyager")
        self.assertTrue(resp.recommended_card.eligible)

        zenith_eval = next(
            e for e in resp.all_evaluations if e.card.card_id == "card_zenith_reserve"
        )
        self.assertFalse(zenith_eval.eligible)
        self.assertTrue(any("annual_income" in r for r in zenith_eval.ineligibility_reasons))
        self.assertIn("Dozer Sapphire Voyager", resp.advisor_message)

    def test_marcus_grocery_and_dining_recommends_cashback_preferred(self) -> None:
        # Marcus (credit_score=695, income=$74k, utilization=28%) is eligible for Cashback Preferred
        # and ineligible for Sapphire Voyager / Zenith Reserve.
        resp = self.advisor.recommend(
            "cust_102",
            "I spend mostly on groceries and dining and want strong cashback with a low fee.",
        )
        self.assertEqual(resp.recommended_card.card.card_id, "card_cashback_plus")
        self.assertTrue(resp.recommended_card.eligible)
        self.assertIn("Dozer Cashback Preferred", resp.advisor_message)

    def test_priya_high_income_low_utilization_unlocks_zenith_reserve(self) -> None:
        resp = self.advisor.recommend(
            "cust_103",
            "I want luxury travel perks, Centurion lounge access, and maximum flight points.",
        )
        self.assertEqual(resp.recommended_card.card.card_id, "card_zenith_reserve")
        self.assertTrue(resp.recommended_card.eligible)
        self.assertGreater(resp.recommended_card.net_ongoing_annual_value_usd, 3000.0)


if __name__ == "__main__":
    unittest.main()
