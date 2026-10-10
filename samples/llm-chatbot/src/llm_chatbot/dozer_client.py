from typing import Any

import requests


def normalize_records(response: Any, endpoint_name: str) -> list[dict[str, Any]]:
    if isinstance(response, list):
        return [record for record in response if isinstance(record, dict)]
    if not isinstance(response, dict):
        return []

    candidate_keys = (
        "records",
        "data",
        "results",
        "items",
        endpoint_name,
    )
    for key in candidate_keys:
        if key in response:
            return normalize_records(response[key], endpoint_name)

    for key in ("result", "response"):
        if key in response:
            return normalize_records(response[key], endpoint_name)

    return []


class DozerClient:
    def __init__(self, base_url: str, session=None) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()

    def get_records(self, endpoint_name: str) -> list[dict[str, Any]]:
        path = endpoint_name if endpoint_name.startswith("/") else f"/{endpoint_name}"
        response = self.session.get(f"{self.base_url}{path}", timeout=10)
        response.raise_for_status()
        return normalize_records(response.json(), endpoint_name)
