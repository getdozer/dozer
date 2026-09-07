import unittest

from load_data import build_customer_profile, load_products, product_as_text, profile_as_text
from vector_rag import build_docs, retrieve_local


class VectorTests(unittest.TestCase):
    def test_travel_query_surfaces_travel_card(self):
        profile = build_customer_profile("cust_002")
        products = load_products()
        docs = build_docs(profile_as_text(profile), products, product_as_text)
        hits = retrieve_local(
            docs,
            "credit card travel flights hotels dining",
            k=3,
        )
        ids = [h.metadata.get("product_id") for h in hits if h.metadata.get("kind") == "product"]
        self.assertTrue(ids, hits)
        # travel or reserve should show up in top product hits
        self.assertTrue({"card_travel", "card_premium"} & set(ids), ids)


if __name__ == "__main__":
    unittest.main()
