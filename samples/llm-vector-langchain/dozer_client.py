from __future__ import annotations

from typing import Any

import requests


class DozerClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    def fetch_endpoint(self, endpoint: str) -> list[dict[str, Any]]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [dict(row) for row in payload]
        if isinstance(payload, dict):
            for key in ("data", "records", "items"):
                value = payload.get(key)
                if isinstance(value, list):
                    return [dict(row) for row in value]
        raise ValueError(f"Unexpected Dozer response shape from {url}: {payload!r}")
