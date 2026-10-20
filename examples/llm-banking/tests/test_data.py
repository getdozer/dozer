import unittest
import csv
import os

class TestBankingData(unittest.TestCase):
    def setUp(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(self.base_dir, "data")

    def test_customer_profiles_exist_and_valid(self):
        path = os.path.join(self.data_dir, "customer_profiles.csv")
        self.assertTrue(os.path.isfile(path))
        with open(path, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
            self.assertGreaterEqual(len(reader), 5)
            for row in reader:
                self.assertTrue(row["customer_id"].startswith("CUST"))
                self.assertGreater(int(row["credit_score"]), 500)
                self.assertGreater(float(row["annual_income"]), 20000)

    def test_transactions_exist_and_valid(self):
        path = os.path.join(self.data_dir, "transactions.csv")
        self.assertTrue(os.path.isfile(path))
        with open(path, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
            self.assertGreaterEqual(len(reader), 10)
            for row in reader:
                self.assertTrue(row["transaction_id"].startswith("TX"))
                self.assertGreater(float(row["amount"]), 0)

    def test_credit_card_products_exist_and_valid(self):
        path = os.path.join(self.data_dir, "credit_card_products.csv")
        self.assertTrue(os.path.isfile(path))
        with open(path, "r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
            self.assertGreaterEqual(len(reader), 5)
            for row in reader:
                self.assertTrue(row["product_id"].startswith("CARD_"))
                self.assertGreaterEqual(int(row["min_credit_score"]), 600)

if __name__ == "__main__":
    unittest.main()
