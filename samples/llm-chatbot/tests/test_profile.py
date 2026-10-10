import unittest

from llm_chatbot.profile import build_customer_profile, format_customer_profile


CUSTOMERS = [
    {
        "id": "customer-001",
        "full_name": "Priya Nair",
        "email": "priya.nair@example.com",
        "city": "Seattle",
        "state": "WA",
        "preferred_language": "English",
        "risk_tier": "low",
    }
]

TRANSACTIONS = [
    {
        "id": "transaction-001",
        "customer_id": "customer-001",
        "card_id": "card-001",
        "merchant": "SeaTac Airport Parking",
        "category": "travel",
        "amount_cents": 4200,
        "transaction_date": "2026-09-02",
    },
    {
        "id": "transaction-002",
        "customer_id": "customer-002",
        "card_id": "card-003",
        "merchant": "Apex Fuel Station",
        "category": "fuel",
        "amount_cents": 6120,
        "transaction_date": "2026-09-03",
    },
]

CREDIT_CARDS = [
    {
        "id": "card-001",
        "customer_id": "customer-001",
        "product_id": "travel-card",
        "last_four": "4102",
        "status": "active",
    },
    {
        "id": "card-003",
        "customer_id": "customer-002",
        "product_id": "secured-card",
        "last_four": "3311",
        "status": "active",
    },
]


class TestCustomerProfile(unittest.TestCase):
    def test_builds_profile_for_selected_customer(self):
        profile = build_customer_profile(
            "customer-001", CUSTOMERS, TRANSACTIONS, CREDIT_CARDS
        )

        self.assertEqual(profile["customer_id"], "customer-001")
        self.assertEqual(len(profile["transactions"]), 1)
        self.assertEqual(len(profile["cards"]), 1)
        self.assertEqual(profile["monthly_spend_cents"], {"2026-09": 4200})

    def test_missing_customer_raises_value_error(self):
        with self.assertRaises(ValueError):
            build_customer_profile("missing", CUSTOMERS, TRANSACTIONS, CREDIT_CARDS)

    def test_formats_profile_for_llm_context(self):
        profile = build_customer_profile(
            "customer-001", CUSTOMERS, TRANSACTIONS, CREDIT_CARDS
        )
        formatted = format_customer_profile(profile)

        self.assertIn("Customer: Priya Nair", formatted)
        self.assertIn("travel-card ending in 4102", formatted)
        self.assertIn("SeaTac Airport Parking", formatted)


if __name__ == "__main__":
    unittest.main()
