"""End-to-End Integration Tests for Banking Advisor."""

import unittest
import sys
from pathlib import Path

SAMPLE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SAMPLE_DIR))

from advisor import BankingAdvisor
from dozer_client import DozerClient


class TestBankingAdvisorIntegration(unittest.TestCase):
    def setUp(self):
        self.client = DozerClient()
        self.advisor = BankingAdvisor(dozer_client=self.client)

    def test_recommendation_cust_001_travel(self):
        """Elena Rostova -> Voyager Horizon Travel Elite."""
        result = self.advisor.recommend("CUST_001")
        self.assertEqual(result.customer_id, "CUST_001")
        self.assertEqual(result.recommended_product.product_id, "CARD_001")
        self.assertEqual(result.eligibility_status, "VERIFIED_ELIGIBLE")
        self.assertIn("Travel", result.rationale)
        self.assertIn("FICO", result.rationale)

    def test_recommendation_cust_002_credit_builder(self):
        """Marcus Vance (Student, 610 credit) -> Foundation Builder."""
        result = self.advisor.recommend("CUST_002")
        self.assertEqual(result.recommended_product.product_id, "CARD_003")
        self.assertEqual(result.recommended_product.annual_fee_usd, 0.0)

    def test_recommendation_cust_003_cashback_groceries(self):
        """Sarah Jenkins -> Harvest Cash & Groceries Plus."""
        result = self.advisor.recommend("CUST_003")
        self.assertEqual(result.recommended_product.product_id, "CARD_002")
        self.assertIn("groceries", result.spend_alignment)

    def test_recommendation_cust_005_luxury_executive(self):
        """Amina Diallo ($340k income, 790 FICO) -> Centurion Signature Sovereign."""
        result = self.advisor.recommend("CUST_005")
        self.assertEqual(result.recommended_product.product_id, "CARD_005")
        self.assertEqual(result.recommended_product.card_tier, "Luxury Executive")


if __name__ == "__main__":
    unittest.main()
