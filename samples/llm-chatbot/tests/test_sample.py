"""End-to-end smoke tests for the Dozer + LLM chatbot sample.

Prerequisites (see README):
    docker compose up -d postgres qdrant
    dozer run            # or the dozer container from the compose file

Run:
    pytest tests/test_sample.py -v

The tests run keyless (mock LLM + hashing embeddings) and exercise the full
chain: Dozer REST -> unified profile -> Qdrant retrieval -> LLM prompt.
"""

from __future__ import annotations

import os

from dozer_client import DozerClient

DOZER_URL = os.getenv("DOZER_REST_URL", "http://localhost:8080")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")


def make_client() -> DozerClient:
    return DozerClient(DOZER_URL)


def test_dozer_health() -> None:
    assert make_client().health()


def test_endpoints_served() -> None:
    endpoints = make_client().list_endpoints()
    for expected in ("/customers", "/transactions", "/products", "/profile"):
        assert expected in endpoints, f"missing endpoint {expected} in {endpoints}"


def test_unified_profile_is_joined_and_aggregated() -> None:
    """The profile must combine customer columns with transaction aggregates."""
    profile = make_client().get_profile(1001)
    assert profile is not None, "customer 1001 missing from /profile"
    assert profile["first_name"] == "Alice"
    assert profile["segment"] == "premium"
    # Aggregates over the seeded transactions for Alice.
    assert profile["transaction_count"] == 4
    assert profile["total_spend"] == "2190.40"
    assert profile["last_transaction_date"] == "2026-10-02"


def test_transactions_query_dsl() -> None:
    txns = make_client().get_transactions(1003, limit=3)
    assert len(txns) == 3
    dates = [t["txn_date"] for t in txns]
    assert dates == sorted(dates, reverse=True), "expected $order_by txn_date desc"
    assert all(t["customer_id"] == 1003 for t in txns)


def test_chatbot_end_to_end_keyless() -> None:
    from chatbot import answer
    from vector_store import ProductVectorStore

    dozer = make_client()
    store = ProductVectorStore()
    indexed = store.index_products(dozer.get_products())
    assert indexed == 5, "expected the 5 seeded products to be indexed"

    reply = answer(dozer, store, 1001, "I travel often - which card fits me best?")
    assert "Alice" in reply, "reply must be personalized with the profile name"
    assert "premium" in reply, "reply must reference the profile segment"
    assert "Mock LLM" in reply, "keyless run must use the mock LLM"


def test_unknown_customer_is_handled() -> None:
    from chatbot import answer
    from vector_store import ProductVectorStore

    dozer = make_client()
    store = ProductVectorStore()
    reply = answer(dozer, store, 9999, "hello")
    assert "not found" in reply
