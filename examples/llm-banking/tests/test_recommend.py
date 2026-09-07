import unittest

from load_data import build_customer_profile, load_products
from recommend import local_answer, rank_cards


class RankTests(unittest.TestCase):
    def test_travel_customer_gets_travel_card(self):
        profile = build_customer_profile("cust_002")
        ranked = rank_cards(profile, load_products())
        self.assertEqual(ranked[0]["product_id"], "card_travel")

    def test_starter_prefers_no_fee(self):
        profile = build_customer_profile("cust_003")
        ranked = rank_cards(profile, load_products())
        self.assertIn(ranked[0]["product_id"], {"card_starter", "card_cash"})
        self.assertEqual(float(ranked[0]["annual_fee_usd"]), 0.0)

    def test_local_answer_mentions_customer(self):
        profile = build_customer_profile("cust_001")
        ranked = rank_cards(profile, load_products())
        text = local_answer(profile, ranked, "which card?")
        self.assertIn("cust_001", text)
        self.assertIn(ranked[0]["name"], text)

    def test_profile_has_spend_breakdown(self):
        profile = build_customer_profile("cust_004")
        self.assertGreater(profile["total_spend"], 0)
        self.assertIn("flights", profile["spend_by_category"])


if __name__ == "__main__":
    unittest.main()
