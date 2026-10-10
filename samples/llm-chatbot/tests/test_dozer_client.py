import unittest

from llm_chatbot.dozer_client import normalize_records


class TestDozerClient(unittest.TestCase):
    def test_normalizes_a_list_response(self):
        response = [{"id": "customer-001"}, {"id": "customer-002"}]

        self.assertEqual(normalize_records(response, "customers"), response)

    def test_normalizes_nested_records_response(self):
        response = {"result": {"records": [{"id": "transaction-001"}]}}

        self.assertEqual(
            normalize_records(response, "transactions"), [{"id": "transaction-001"}]
        )

    def test_returns_empty_list_for_unrecognized_response(self):
        self.assertEqual(normalize_records({"error": "unavailable"}, "customers"), [])


if __name__ == "__main__":
    unittest.main()
