import unittest
import os
import sys

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from recommend import HyperPersonalizedRecommender

class TestRecommendationEngine(unittest.TestCase):
    def setUp(self):
        self.data_dir = os.path.join(base_dir, "data")
        self.recommender = HyperPersonalizedRecommender(data_dir=self.data_dir)

    def test_travel_customer_recommendation(self):
        res = self.recommender.generate_recommendation("CUST001")
        self.assertEqual(res["customer_name"], "Alice Chen")
        self.assertEqual(res["top_spending_category"], "Travel")
        self.assertEqual(res["recommended_product_id"], "CARD_TRAVEL_ELITE")
        self.assertIn("Apex World Explorer Card", res["rationale"])

    def test_dining_customer_recommendation(self):
        res = self.recommender.generate_recommendation("CUST002")
        self.assertEqual(res["customer_name"], "Bob Miller")
        self.assertEqual(res["top_spending_category"], "Dining")
        self.assertEqual(res["recommended_product_id"], "CARD_DINING_REWARDS")
        self.assertIn("Gourmet Platinum Card", res["rationale"])

    def test_groceries_customer_recommendation(self):
        res = self.recommender.generate_recommendation("CUST003")
        self.assertEqual(res["customer_name"], "Charlie Davis")
        self.assertEqual(res["top_spending_category"], "Groceries")
        self.assertEqual(res["recommended_product_id"], "CARD_GROCERY_SAVER")

    def test_invalid_customer_raises_error(self):
        with self.assertRaises(ValueError):
            self.recommender.generate_recommendation("NON_EXISTENT_ID")

if __name__ == "__main__":
    unittest.main()
