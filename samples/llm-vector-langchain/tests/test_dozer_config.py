import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml  # noqa: E402

from dozer_client import load_csv_datasets  # noqa: E402

SAMPLE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(SAMPLE_ROOT, "dozer-config.yaml")
DATA_DIR = os.path.join(SAMPLE_ROOT, "data")


class TagIgnoreLoader(yaml.SafeLoader):
    """SafeLoader that treats serde_yaml tags like `!LocalStorage` as plain mappings."""


def _construct_untagged(loader: yaml.SafeLoader, _suffix: str, node: yaml.Node):
    return loader.construct_mapping(node, deep=True)


TagIgnoreLoader.add_multi_constructor("!", _construct_untagged)


def load_config() -> dict:
    with open(CONFIG_PATH, encoding="utf-8") as handle:
        return yaml.load(handle, Loader=TagIgnoreLoader)


class DozerConfigTest(unittest.TestCase):
    def test_config_parses(self) -> None:
        config = load_config()
        self.assertEqual(config["app_name"], "llm-vector-langchain-sample")
        self.assertEqual(config["version"], 1)

    def test_config_sources_three_datasets(self) -> None:
        config = load_config()
        sources = {source["name"] for source in config["sources"]}
        self.assertEqual(
            sources,
            {"customer_profiles", "transactions", "credit_card_products"},
        )

    def test_config_has_sink_for_every_source(self) -> None:
        config = load_config()
        sinks = {sink["name"] for sink in config["sinks"]}
        self.assertEqual(
            sinks,
            {"customer_profiles", "transactions", "credit_card_products"},
        )

    def test_connection_uses_local_storage_with_csv_tables(self) -> None:
        config = load_config()
        self.assertEqual(len(config["connections"]), 1)
        connection = config["connections"][0]
        self.assertEqual(connection["name"], "local_customer_data")
        self.assertIn("details", connection["config"])
        self.assertEqual(connection["config"]["details"]["path"], "./data")
        tables = {table["name"]: table["config"] for table in connection["config"]["tables"]}
        self.assertEqual(set(tables), {"customer_profiles", "transactions", "credit_card_products"})
        for table_config in tables.values():
            self.assertEqual(table_config["extension"], ".csv")

    def test_csv_datasets_load(self) -> None:
        datasets = load_csv_datasets(os.path.join(SAMPLE_ROOT, "data"))
        self.assertEqual(len(datasets["customer_profiles"]), 4)
        self.assertEqual(len(datasets["transactions"]), 12)
        self.assertEqual(len(datasets["credit_card_products"]), 4)
        self.assertEqual(
            {row["customer_id"] for row in datasets["customer_profiles"]},
            {"cust_001", "cust_002", "cust_003", "cust_004"},
        )


if __name__ == "__main__":
    unittest.main()