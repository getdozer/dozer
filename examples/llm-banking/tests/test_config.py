import unittest
import os

class TestDozerConfig(unittest.TestCase):
    def setUp(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.config_path = os.path.join(self.base_dir, "dozer-config.yaml")

    def test_dozer_config_exists_and_contains_required_sections(self):
        self.assertTrue(os.path.isfile(self.config_path))
        with open(self.config_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("app_name: llm_banking", content)
        self.assertIn("connections:", content)
        self.assertIn("sources:", content)
        self.assertIn("sql:", content)
        self.assertIn("endpoints:", content)
        self.assertIn("customer_spending_summary", content)

if __name__ == "__main__":
    unittest.main()
