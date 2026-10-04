"""Unified customer profile client for the Dozer REST API.

Dozer turns the `customer_profile` SQL transform (see ../dozer-config.yaml)
into a low-latency REST endpoint. Every chatbot turn starts by pulling the
caller's profile from here, so the LLM always reasons over fresh, joined
customer data instead of a stale snapshot.

Endpoints served by Dozer (see ../dozer-config.yaml):
    GET  /profile            -> list profiles   (default limit 50)
    GET  /profile/{id}       -> single profile by primary key
    POST /profile/query      -> {"$filter": {...}, "$limit": n, "$skip": n}
    POST /profile/count
"""

from __future__ import annotations

import os
from typing import Any

import requests

DOZER_REST_URL = os.getenv("DOZER_REST_URL", "http://localhost:8080")
_TIMEOUT = 10


class DozerClient:
    """Thin client over Dozer's generated REST endpoints."""

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (base_url or DOZER_REST_URL).rstrip("/")

    def health(self) -> bool:
        try:
            response = requests.get(f"{self.base_url}/health", timeout=_TIMEOUT)
            payload = response.json()
        except (requests.RequestException, ValueError):
            return False
        return response.ok and payload.get("status") == "SERVING"

    def list_endpoints(self) -> list[str]:
        """GET / returns the paths of all endpoints defined in the config."""
        try:
            response = requests.get(self.base_url, timeout=_TIMEOUT)
        except requests.RequestException:
            return []
        return [
            entry if isinstance(entry, str) else entry.get("path", "")
            for entry in response.json()
        ]

    # ------------------------------------------------------------------
    # Generic helpers over Dozer's endpoint routes.
    # ------------------------------------------------------------------
    def _list(self, endpoint: str) -> list[dict[str, Any]]:
        response = requests.get(f"{self.base_url}/{endpoint}", timeout=_TIMEOUT)
        response.raise_for_status()
        return response.json()

    def _get(self, endpoint: str, record_id: Any) -> dict[str, Any] | None:
        response = requests.get(f"{self.base_url}/{endpoint}/{record_id}", timeout=_TIMEOUT)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def _query(self, endpoint: str, body: dict[str, Any]) -> list[dict[str, Any]]:
        response = requests.post(
            f"{self.base_url}/{endpoint}/query", json=body, timeout=_TIMEOUT
        )
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Sample-specific accessors.
    # ------------------------------------------------------------------
    def get_customer(self, customer_id: int) -> dict[str, Any] | None:
        return self._get("customers", customer_id)

    def get_transactions(self, customer_id: int, limit: int = 5) -> list[dict[str, Any]]:
        return self._query(
            "transactions",
            {
                "$filter": {"customer_id": customer_id},
                "$order_by": {"txn_date": "desc"},
                "$limit": limit,
            },
        )

    def get_products(self) -> list[dict[str, Any]]:
        return self._list("products")

    def get_profile(self, customer_id: int) -> dict[str, Any] | None:
        """The unified profile produced by Dozer's SQL join + aggregation."""
        records = self._query("profile", {"$filter": {"customer_id": customer_id}, "$limit": 1})
        return records[0] if records else None

    def all_profiles(self) -> list[dict[str, Any]]:
        return self._list("profile")
