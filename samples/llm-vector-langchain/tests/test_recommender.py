import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from recommender import build_customer_context, rank_products  # noqa: E402


class RecommenderTest(unittest.TestCase):
    def test_frequent_traveler_gets_travel_card(self) -> None:
        customer = {
            "customer_id": "cust_001",
            "income_band": "premium",
            "segment": "frequent_traveler",
            "goals": "travel rewards and lounge access",
            "risk_tolerance": "medium",
        }
        transactions = [
            {"merchant_category": "travel", "amount_usd": "500"},
            {"merchant_category": "travel", "amount_usd": "300"},
            {"merchant_category": "dining", "amount_usd": "120"},
        ]
        products = [
            {
                "product_id": "card_001",
                "name": "Aurora Travel Signature",
                "annual_fee_usd": "395",
                "rewards_focus": "travel dining lounge",
                "intro_offer": "bonus points",
                "eligibility": "premium income and excellent credit",
                "caveat": "high fee",
            },
            {
                "product_id": "card_002",
                "name": "Everyday Cashback Plus",
                "annual_fee_usd": "0",
                "rewards_focus": "groceries streaming cashback",
                "intro_offer": "statement credit",
                "eligibility": "starter or mass market income",
                "caveat": "lower travel earn",
            },
        ]

        ranked = rank_products(customer, transactions, products)

        self.assertEqual(ranked[0]["product_id"], "card_001")
        self.assertIn("rewards match travel spend", ranked[0]["reason"])

    def test_cashback_customer_prefers_no_fee_card(self) -> None:
        customer = {
            "customer_id": "cust_002",
            "income_band": "starter",
            "segment": "young_professional",
            "goals": "cashback on everyday spend, no annual fee",
            "risk_tolerance": "low",
        }
        transactions = [
            {"merchant_category": "groceries", "amount_usd": "122.23"},
            {"merchant_category": "groceries", "amount_usd": "98.11"},
            {"merchant_category": "streaming", "amount_usd": "18.99"},
        ]
        products = [
            {
                "product_id": "card_001",
                "name": "Aurora Travel Signature",
                "annual_fee_usd": "395",
                "rewards_focus": "travel dining lounge",
                "intro_offer": "bonus points",
                "eligibility": "premium income and excellent credit",
                "caveat": "high fee",
            },
            {
                "product_id": "card_002",
                "name": "Everyday Cashback Plus",
                "annual_fee_usd": "0",
                "rewards_focus": "groceries streaming cashback",
                "intro_offer": "statement credit",
                "eligibility": "starter or mass market income",
                "caveat": "lower travel earn",
            },
        ]

        ranked = rank_products(customer, transactions, products)

        self.assertEqual(ranked[0]["product_id"], "card_002")
        self.assertIn("no annual fee fits a starter income band", ranked[0]["reason"])

    def test_customer_context_summarizes_spend(self) -> None:
        customer = {
            "customer_id": "cust_002",
            "income_band": "starter",
            "segment": "young_professional",
            "goals": "cashback",
            "risk_tolerance": "low",
        }
        transactions = [
            {"merchant_category": "groceries", "amount_usd": "10.50"},
            {"merchant_category": "groceries", "amount_usd": "12.25"},
            {"merchant_category": "streaming", "amount_usd": "8.00"},
        ]

        context = build_customer_context(customer, transactions)

        self.assertIn("Recent spend: $30.75", context)
        self.assertIn("groceries (2)", context)


if __name__ == "__main__":
    unittest.main()