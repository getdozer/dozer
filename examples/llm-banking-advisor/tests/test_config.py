"""Tests for Dozer configuration schema and SQL validity."""

import unittest
import re
from pathlib import Path

SAMPLE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = SAMPLE_DIR / "dozer-config.yaml"


class TestDozerConfig(unittest.TestCase):
    def setUp(self):
        self.assertTrue(CONFIG_PATH.exists(), f"Missing config file at {CONFIG_PATH}")
        self.content = CONFIG_PATH.read_text(encoding="utf-8")

    def test_required_top_level_fields(self):
        """Verify presence of modern Dozer root fields."""
        self.assertRegex(self.content, re.compile(r"^version:\s*1", re.M))
        self.assertRegex(self.content, re.compile(r"^app_name:\s*llm-banking-advisor", re.M))
        self.assertRegex(self.content, re.compile(r"^connections:", re.M))
        self.assertRegex(self.content, re.compile(r"^sources:", re.M))
        self.assertRegex(self.content, re.compile(r"^sql:", re.M))
        self.assertRegex(self.content, re.compile(r"^sinks:", re.M))
        self.assertRegex(self.content, re.compile(r"^api:", re.M))

    def test_no_obsolete_endpoints_field(self):
        """Ensure obsolete 'endpoints' field is absent (causes serde deny_unknown_fields error)."""
        self.assertIsNone(re.search(r"^endpoints:", self.content, re.MULTILINE))

    def test_sources_reference_connections(self):
        """Verify sources reference connection bank_data."""
        self.assertIn("connection: bank_data", self.content)
        self.assertIn("name: bank_data", self.content)

    def test_streaming_sql_targets(self):
        """Verify SQL creates customer_profile and card_catalog target tables."""
        self.assertIn("INTO customer_profile", self.content)
        self.assertIn("INTO card_catalog", self.content)
        self.assertIn("JOIN transactions", self.content)
        self.assertIn("GROUP BY", self.content)

    def test_sinks_match_sql_targets(self):
        """Verify declared sinks match the SQL INTO target tables."""
        self.assertIn("table_name: customer_profile", self.content)
        self.assertIn("table_name: card_catalog", self.content)


if __name__ == "__main__":
    unittest.main()
