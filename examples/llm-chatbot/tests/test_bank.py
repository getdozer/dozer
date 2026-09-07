import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bank import card_text, get_profile, load_bank, profile


class BankTest(unittest.TestCase):
    def setUp(self):
        self.bank = load_bank()

    def test_seed_counts(self):
        self.assertEqual(len(self.bank["customers"]), 5)
        self.assertEqual(len(self.bank["transactions"]), 20)
        self.assertEqual(len(self.bank["cards"]), 5)

    def test_maya_travel_spend(self):
        maya = get_profile("c1", self.bank)
        self.assertEqual(maya["name"], "Maya Chen")
        self.assertGreater(maya["spend"], 2000)
        self.assertIn("travel", maya["top_spend"])
        self.assertIn("travel rewards", maya["text"])

    def test_omar_grocery_spend(self):
        omar = get_profile("c2", self.bank)
        self.assertIn("groceries", omar["top_spend"])
        self.assertNotIn("travel", omar["top_spend"])

    def test_unknown_customer(self):
        self.assertIsNone(get_profile("nope", self.bank))

    def test_card_text(self):
        atlas = next(c for c in self.bank["cards"] if c["card_id"] == "atlas")
        text = card_text(atlas)
        self.assertIn("Atlas Travel Reserve", text)
        self.assertIn("395", text)

    def test_profile_uses_only_that_customer_txns(self):
        c = self.bank["customers"][0]
        p = profile(c, self.bank["transactions"])
        mine = [t for t in self.bank["transactions"] if t["customer_id"] == c["customer_id"]]
        self.assertEqual(p["txn_count"], len(mine))


if __name__ == "__main__":
    unittest.main()
