import json
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]


class ConfigTests(unittest.TestCase):
    def test_config_text_shape(self):
        text = (ROOT / "dozer-config.yaml").read_text()
        self.assertIn("app_name: llm-banking-sample", text)
        self.assertIn("!LocalStorage", text)
        self.assertIn("INTO customer_profile", text)
        self.assertIn("sinks:", text)
        self.assertIn("!Dummy", text)
        self.assertNotIn("\nendpoints:", text)

    def test_schema_has_sinks_not_endpoints(self):
        schema = json.loads((REPO / "json_schemas" / "dozer.json").read_text())
        props = schema["properties"]
        self.assertIn("sinks", props)
        self.assertNotIn("endpoints", props)

    def test_csvs_exist_and_have_rows(self):
        for folder, name in [
            ("customer_profiles", "customer_profiles.csv"),
            ("transactions", "transactions.csv"),
            ("credit_card_products", "credit_card_products.csv"),
        ]:
            path = ROOT / "data" / folder / name
            self.assertTrue(path.exists(), path)
            lines = [ln for ln in path.read_text().splitlines() if ln.strip()]
            self.assertGreaterEqual(len(lines), 2)

    def test_sql_mentions_join(self):
        text = (ROOT / "dozer-config.yaml").read_text()
        self.assertRegex(text, re.compile(r"JOIN transactions", re.I))


if __name__ == "__main__":
    unittest.main()
