"""Tests for DozerClient data ingestion and SQL profile emulation."""

import unittest
import sys
from pathlib import Path

SAMPLE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SAMPLE_DIR))

from dozer_client import DozerClient


class TestDozerClient(unittest.TestCase):
    def setUp(self):
        self.client = DozerClient()

    def test_load_products(self):
        """Verify card products are ingested with correct types."""
        products = self.client.get_card_catalog()
        self.assertGreaterEqual(len(products), 5)
        product_ids = [p.product_id for p in products]
        self.assertIn("CARD_001", product_ids)
        self.assertIn("CARD_002", product_ids)
        for p in products:
            self.assertGreater(len(p.name), 0)
            self.assertGreaterEqual(p.min_credit_score, 500)
            self.assertGreaterEqual(p.annual_fee_usd, 0.0)

    def test_customer_profile_aggregation(self):
        """Verify customer profile reflects accurate SQL transaction joins and totals."""
        profile = self.client.get_customer_profile("CUST_001")
        self.assertEqual(profile.first_name, "Elena")
        self.assertEqual(profile.last_name, "Rostova")
        self.assertEqual(profile.credit_score, 750)
        self.assertEqual(profile.annual_income_usd, 115000)

        # Elena transactions: 840.50 + 420.00 + 38.50 + 610.20 + 12.75 + 95.00 = 2016.95
        self.assertAlmostEqual(profile.total_spend_usd, 2016.95, places=2)
        self.assertEqual(profile.transaction_count, 6)
        self.assertEqual(profile.top_categories[0], "travel")
        self.assertIn("travel", profile.spend_by_category)
        self.assertIn("dining", profile.spend_by_category)

    def test_unknown_customer_raises_keyerror(self):
        """Verify non-existent customer raises KeyError."""
        with self.assertRaises(KeyError):
            self.client.get_customer_profile("NON_EXISTENT_ID")


if __name__ == "__main__":
    unittest.main()
