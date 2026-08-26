from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import requests

DEFAULT_BASE_URL = "http://localhost:8080"

DATASETS = ("customer_profiles", "transactions", "credit_card_products")


class DozerClient:
    """Reads records from the REST endpoints exposed by a running Dozer app."""

    def __init__(self, base_url: str = DEFAULT_BASE_URL) -> None:
        self.base_url = base_url.rstrip("/")

    def fetch_endpoint(self, endpoint: str) -> list[dict[str, Any]]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [dict(row) for row in payload]
        if isinstance(payload, dict):
            for key in ("records", "data", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [dict(row) for row in value]
        raise ValueError(f"Unexpected Dozer response shape from {url}: {payload!r}")


def load_csv_datasets(data_dir: Path | str) -> dict[str, list[dict[str, Any]]]:
    """Read the sample CSV datasets that Dozer is configured to source from."""
    data_dir = Path(data_dir)
    datasets: dict[str, list[dict[str, Any]]] = {}
    for dataset in DATASETS:
        path = data_dir / dataset / f"{dataset}.csv"
        with path.open(newline="", encoding="utf-8") as handle:
            datasets[dataset] = [dict(row) for row in csv.DictReader(handle)]
    return datasets


def load_datasets(base_url: str, data_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """Fetch datasets from Dozer, falling back to the local CSV files."""
    try:
        client = DozerClient(base_url)
        return {
            dataset: client.fetch_endpoint(dataset) for dataset in DATASETS
        }
    except requests.RequestException:
        return load_csv_datasets(data_dir)