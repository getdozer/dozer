import unittest
from pathlib import Path

from llm_chatbot.catalog import load_credit_card_products, product_document


class TestCreditCardCatalog(unittest.TestCase):
    def test_loads_products_from_catalog(self):
        products = load_credit_card_products(
            Path(__file__).resolve().parents[1] / "data" / "credit_card_products.csv"
        )

        self.assertEqual(len(products), 5)
        self.assertEqual(products[0].product_id, "travel-card")

    def test_creates_document_with_product_metadata(self):
        products = load_credit_card_products(
            Path(__file__).resolve().parents[1] / "data" / "credit_card_products.csv"
        )
        product = next(item for item in products if item.product_id == "travel-card")
        document = product_document(product)

        page_content, metadata = document

        self.assertIn("Travel Signature Card", page_content)
        self.assertEqual(metadata["product_id"], "travel-card")


if __name__ == "__main__":
    unittest.main()
