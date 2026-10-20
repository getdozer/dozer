import unittest
import os
import sys

base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if base_dir not in sys.path:
    sys.path.insert(0, base_dir)

from vector_rag import initialize_card_vector_store
from recommend import DozerClientMock

class TestVectorRAG(unittest.TestCase):
    def setUp(self):
        self.data_dir = os.path.join(base_dir, "data")
        client = DozerClientMock(data_dir=self.data_dir)
        self.products = client.get_all_products()
        self.store = initialize_card_vector_store(self.products)

    def test_vector_indexing_and_retrieval(self):
        results = self.store.similarity_search("flights hotels travel worldwide", top_k=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["product_id"], "CARD_TRAVEL_ELITE")

    def test_dining_search(self):
        results = self.store.similarity_search("dining restaurants gourmet food delivery", top_k=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["product_id"], "CARD_DINING_REWARDS")

    def test_grocery_search(self):
        results = self.store.similarity_search("supermarket groceries wholesale", top_k=1)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["product_id"], "CARD_GROCERY_SAVER")

if __name__ == "__main__":
    unittest.main()
