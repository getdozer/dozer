"""Comprehensive pytest verification suite for `examples/llm-vector-langchain` (#1690).

Covers:
1. Canonical `dozer-config.yaml` schema validation (`!Postgres`, 4 sources, SQL INTO views, 6 endpoints).
2. PostgreSQL `seed.sql` parsing and table record counts (`REPLICA IDENTITY FULL`).
3. `DozerClient` `/query` and `/count` operations with `$filter`, `$order_by`, and `$limit`.
4. Real-time Dozer SQL customer spending aggregations (`customer_spending_profiles`).
5. Real-time Dozer SQL credit card underwriting & eligibility joins (`eligible_card_recommendations`).
6. Incremental `DozerCDCSynchronizer` snapshot & diff event emission (`INSERT`, `UPDATE`, `DELETE`).
7. `DozerLangChainVectorStore` semantic search and credit-score metadata filtering.
8. Real-time Vector Store CDC `INSERT`, `UPDATE`, and `DELETE` index mutations.
9. End-to-end LangChain RAG hyper-personalization across travel, grocery, credit-builder, and tech personas.
10. Real-time multi-table CDC reactivity (customer credit upgrade + new transaction + new policy doc).
11. CLI execution (`--mode demo`, `--mode ask`, `--mode sync`).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SAMPLE_DIR = Path(__file__).resolve().parent.parent
if str(SAMPLE_DIR) not in sys.path:
    sys.path.insert(0, str(SAMPLE_DIR))

from app import main as cli_main  # noqa: E402
from dozer_client import (  # noqa: E402
    CDCEvent,
    CDCOperation,
    DozerCDCSynchronizer,
    DozerClient,
    DozerConfigValidationError,
    InMemoryDozerBackend,
    load_and_validate_dozer_config,
)
from rag_pipeline import DozerBankingRAGPipeline  # noqa: E402
from vector_store import (  # noqa: E402
    DeterministicBankingEmbeddings,
    DozerLangChainVectorStore,
)


CONFIG_PATH = SAMPLE_DIR / "dozer-config.yaml"
SEED_SQL_PATH = SAMPLE_DIR / "seed.sql"


def test_dozer_config_yaml_valid_schema() -> None:
    """1. Verify `dozer-config.yaml` uses `!Postgres`, 4 sources, valid `INTO` SQL, and 6 endpoints."""
    config = load_and_validate_dozer_config(CONFIG_PATH)

    assert config["app_name"] == "dozer-banking-llm-rag"
    assert config["version"] == 1
    assert config["api"]["rest"]["port"] == 8080
    assert config["api"]["grpc"]["port"] == 50051
    assert config["flags"]["push_events"] is True

    connections = config["connections"]
    assert len(connections) == 1
    pg_conn = connections[0]
    assert pg_conn["name"] == "banking_postgres"
    assert pg_conn["config"]["__tag__"] == "!Postgres"
    assert pg_conn["config"]["database"] == "dozer_banking"

    sources = config["sources"]
    source_names = {s["name"] for s in sources}
    assert source_names == {
        "customers",
        "transactions",
        "credit_card_products",
        "documents",
    }

    sql_text = config["sql"]
    assert "INTO customer_spending_profiles" in sql_text
    assert "INTO eligible_card_recommendations" in sql_text

    endpoints = config["endpoints"]
    assert len(endpoints) == 6
    endpoint_paths = {ep["path"]: ep for ep in endpoints}
    assert set(endpoint_paths.keys()) == {
        "/customer_profiles",
        "/eligible_cards",
        "/credit_card_products",
        "/documents",
        "/transactions",
        "/customers",
    }
    assert endpoint_paths["/customer_profiles"]["index"]["primary_key"] == ["customer_id"]
    assert endpoint_paths["/eligible_cards"]["index"]["primary_key"] == [
        "customer_id",
        "card_id",
    ]


def test_dozer_config_rejects_invalid_inline_endpoint_sql(tmp_path: Path) -> None:
    """Ensure validator catches invalid inline `sql:` on endpoints (a bug in competitor PRs)."""
    bad_yaml = tmp_path / "bad-dozer-config.yaml"
    bad_yaml.write_text(
        """
app_name: bad-config
version: 1
connections:
  - name: pg
    config: !Postgres
      user: postgres
sources:
  - name: customers
    table_name: customers
    connection: pg
sql: |
  SELECT * INTO customer_view FROM customers;
endpoints:
  - name: bad_ep
    path: /bad
    table_name: customer_view
    sql: "SELECT * FROM customers"
    index:
      primary_key: [customer_id]
""",
        encoding="utf-8",
    )
    with pytest.raises(DozerConfigValidationError, match="invalid inline 'sql'"):
        load_and_validate_dozer_config(bad_yaml)


def test_seed_sql_tables_and_records() -> None:
    """2. Verify `seed.sql` defines REPLICA IDENTITY FULL and loads all 4 tables accurately."""
    sql_raw = SEED_SQL_PATH.read_text(encoding="utf-8")
    assert "ALTER TABLE customers REPLICA IDENTITY FULL;" in sql_raw
    assert "ALTER TABLE transactions REPLICA IDENTITY FULL;" in sql_raw
    assert "ALTER TABLE credit_card_products REPLICA IDENTITY FULL;" in sql_raw
    assert "ALTER TABLE documents REPLICA IDENTITY FULL;" in sql_raw

    backend = InMemoryDozerBackend(seed_sql_path=SEED_SQL_PATH)
    assert len(backend.get_table_rows("customers")) == 4
    assert len(backend.get_table_rows("credit_card_products")) == 5
    assert len(backend.get_table_rows("transactions")) == 14
    assert len(backend.get_table_rows("documents")) == 8


def test_dozer_client_query_and_count() -> None:
    """3. Test `DozerClient.query` and `DozerClient.count` with `$filter`, `$order_by`, and `$limit`."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)

    assert client.count("/customer_profiles") == 4
    assert client.count("/documents") == 8
    assert client.count("/customer_profiles", filter_spec={"credit_score": {"$gte": 750}}) == 2

    high_score_profiles = client.query(
        "/customer_profiles",
        filter_spec={"credit_score": {"$gte": 750}},
        order_by={"credit_score": "desc"},
        limit=2,
    )
    assert [p["customer_id"] for p in high_score_profiles] == ["CUST-004", "CUST-001"]


def test_dozer_sql_spending_aggregation_accuracy() -> None:
    """4. Verify `customer_spending_profiles` SQL aggregation matches raw customer transactions."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)

    ava = client.get_customer_profile("CUST-001")
    assert ava is not None
    assert ava["full_name"] == "Ava Sharma"
    assert ava["transaction_count"] == 5
    assert ava["travel_spend"] == pytest.approx(4250.0)
    assert ava["dining_spend"] == pytest.approx(1180.0)
    assert ava["groceries_spend"] == pytest.approx(320.0)
    assert ava["tech_spend"] == pytest.approx(0.0)
    assert ava["total_spend"] == pytest.approx(5750.0)

    marcus = client.get_customer_profile("CUST-002")
    assert marcus is not None
    assert marcus["groceries_spend"] == pytest.approx(1640.0)
    assert marcus["dining_spend"] == pytest.approx(620.0)
    assert marcus["travel_spend"] == pytest.approx(240.0)
    assert marcus["total_spend"] == pytest.approx(2500.0)


def test_dozer_sql_card_eligibility_filtering() -> None:
    """5. Verify `eligible_card_recommendations` SQL inner join enforces credit score and income."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)

    # CUST-003 (Elena Rostova: score 645, income $48,000) only qualifies for CARD-BUILDER-SEC
    elena_cards = client.get_eligible_cards("CUST-003")
    assert [c["card_id"] for c in elena_cards] == ["CARD-BUILDER-SEC"]

    # CUST-001 (Ava Sharma: score 785, income $145,000) qualifies for all 5 cards
    ava_cards = client.get_eligible_cards("CUST-001")
    assert len(ava_cards) == 5
    assert "CARD-TRAVEL-SIG" in {c["card_id"] for c in ava_cards}

    # CUST-002 (Marcus Vance: score 715, income $82,000) qualifies for Cash Plus, Dining Gold, Builder
    marcus_cards = client.get_eligible_cards("CUST-002")
    assert {c["card_id"] for c in marcus_cards} == {
        "CARD-CASH-PLUS",
        "CARD-DINING-GOLD",
        "CARD-BUILDER-SEC",
    }


def test_cdc_synchronizer_initial_snapshot_and_incremental_diff() -> None:
    """6. Verify `DozerCDCSynchronizer.sync_once()` emits INSERT, UPDATE, and DELETE diffs."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)
    sync = DozerCDCSynchronizer(client, endpoint="/documents", primary_key_fields=("doc_id",))

    initial_events = sync.sync_once()
    assert len(initial_events) == 8
    assert all(ev.operation == CDCOperation.INSERT for ev in initial_events)

    # Second poll with no backend mutations emits 0 events
    assert sync.sync_once() == []

    # Mutate backend: insert DOC-100, update DOC-001, delete DOC-008
    client.backend.upsert_row(
        "documents",
        {
            "doc_id": "DOC-100",
            "title": "New Airport Fast-Track Policy",
            "category": "Travel",
            "card_id": "CARD-TRAVEL-SIG",
            "min_credit_score": 750,
            "content": "Complimentary fast-track security lane access at 80 airports.",
            "updated_at": "2026-03-01 00:00:00",
        },
    )
    doc_001 = client.query("/documents", filter_spec={"doc_id": "DOC-001"})[0]
    doc_001["content"] = doc_001["content"] + " Updated with $350 travel credit."
    client.backend.upsert_row("documents", doc_001)
    client.backend.delete_row("documents", "DOC-008")

    diff_events = sync.sync_once()
    ops_by_pk = {ev.primary_key: ev.operation for ev in diff_events}
    assert ops_by_pk == {
        "DOC-100": CDCOperation.INSERT,
        "DOC-001": CDCOperation.UPDATE,
        "DOC-008": CDCOperation.DELETE,
    }


def test_vector_store_semantic_search_and_metadata_filter() -> None:
    """7. Verify `DozerLangChainVectorStore` ranks domain docs accurately and enforces metadata filters."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)
    store = DozerLangChainVectorStore(embedding_function=DeterministicBankingEmbeddings())
    for doc_row in client.get_documents():
        store.upsert_dozer_record(doc_row)

    assert store.document_count == 8

    # Semantic search for airport lounge & international flights
    travel_hits = store.similarity_search(
        "airport lounge access Priority Pass and international flights",
        k=2,
    )
    assert travel_hits[0].metadata["doc_id"] == "DOC-001"

    # Metadata filter for customer with credit score 645 (must exclude min_credit_score > 645)
    filtered_hits = store.similarity_search(
        "airport lounge access and travel rewards",
        k=5,
        filter={"max_min_credit_score": 645},
    )
    assert len(filtered_hits) >= 1
    assert all(int(d.metadata["min_credit_score"]) <= 645 for d in filtered_hits)
    assert {d.metadata["doc_id"] for d in filtered_hits} == {"DOC-003", "DOC-008"}


def test_vector_store_cdc_insert_event() -> None:
    """8. Verify `CDCEvent(INSERT)` immediately indexes a new document into `DozerLangChainVectorStore`."""
    store = DozerLangChainVectorStore()
    initial_version = store.index_version

    insert_event = CDCEvent(
        endpoint="/documents",
        operation=CDCOperation.INSERT,
        primary_key="DOC-999",
        new_record={
            "doc_id": "DOC-999",
            "title": "Concierge Helicopter Transfer Benefit",
            "category": "Travel",
            "card_id": "CARD-TRAVEL-SIG",
            "min_credit_score": 750,
            "content": "Complimentary helicopter airport transfer for Platinum Reserve cardholders in NYC and London.",
            "updated_at": "2026-03-02 12:00:00",
        },
    )
    store.apply_cdc_event(insert_event)

    assert store.document_count == 1
    assert store.index_version == initial_version + 1
    hits = store.similarity_search("helicopter airport transfer NYC London", k=1)
    assert len(hits) == 1
    assert hits[0].metadata["doc_id"] == "DOC-999"
    assert "helicopter airport transfer" in hits[0].page_content


def test_vector_store_cdc_update_event() -> None:
    """9. Verify `CDCEvent(UPDATE)` re-embeds and replaces an existing document in-place."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)
    store = DozerLangChainVectorStore()
    for doc_row in client.get_documents():
        store.upsert_dozer_record(doc_row)

    version_before = store.index_version
    updated_record = {
        "doc_id": "DOC-001",
        "title": "Dozer Sapphire Reserve Travel — Updated 2026 Emerald Chauffeur Perks",
        "category": "Travel",
        "card_id": "CARD-TRAVEL-SIG",
        "min_credit_score": 750,
        "content": "Cardholders now receive complimentary private Maybach chauffeur airport transfers worldwide and a $550 annual travel credit.",
        "updated_at": "2026-03-05 09:00:00",
    }
    store.apply_cdc_event(
        CDCEvent(
            endpoint="/documents",
            operation=CDCOperation.UPDATE,
            primary_key="DOC-001",
            new_record=updated_record,
        )
    )

    assert store.document_count == 8
    assert store.index_version == version_before + 1
    updated_doc = store.get_document_by_id("DOC-001")
    assert updated_doc is not None
    assert "Maybach chauffeur" in updated_doc.page_content
    assert "$550 annual travel credit" in updated_doc.page_content


def test_vector_store_cdc_delete_event() -> None:
    """10. Verify `CDCEvent(DELETE)` removes the document from the vector index immediately."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)
    store = DozerLangChainVectorStore()
    for doc_row in client.get_documents():
        store.upsert_dozer_record(doc_row)

    assert store.get_document_by_id("DOC-001") is not None
    version_before = store.index_version

    store.apply_cdc_event(
        CDCEvent(
            endpoint="/documents",
            operation=CDCOperation.DELETE,
            primary_key="DOC-001",
        )
    )

    assert store.document_count == 7
    assert store.index_version == version_before + 1
    assert store.get_document_by_id("DOC-001") is None
    remaining_ids = {d.metadata["doc_id"] for d in store.similarity_search("travel lounge", k=10)}
    assert "DOC-001" not in remaining_ids


def test_rag_pipeline_hyper_personalization_travel_customer() -> None:
    """11. Verify RAG recommends `Dozer Sapphire Reserve Travel` for travel-heavy `CUST-001`."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)
    pipeline = DozerBankingRAGPipeline(dozer_client=client)

    res = pipeline.ask(
        customer_id="CUST-001",
        question="Which card should I get for international flights and airport lounge access?",
    )

    assert res.recommended_card is not None
    assert res.recommended_card["card_id"] == "CARD-TRAVEL-SIG"
    assert res.recommended_card["card_name"] == "Dozer Sapphire Reserve Travel"
    assert "DOC-001" in res.citations
    assert "Ava Sharma" in res.answer
    assert "Dozer Sapphire Reserve Travel" in res.answer
    assert "$4,250.00" in res.answer
    assert "[DOC-001]" in res.answer


def test_rag_pipeline_hyper_personalization_groceries_customer() -> None:
    """12. Verify RAG recommends `Dozer Everyday Cashback Plus` for grocery-heavy `CUST-002`."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)
    pipeline = DozerBankingRAGPipeline(dozer_client=client)

    res = pipeline.ask(
        customer_id="CUST-002",
        question="What is the best zero-fee cashback card for my weekly grocery and supermarket spend?",
    )

    assert res.recommended_card is not None
    assert res.recommended_card["card_id"] == "CARD-CASH-PLUS"
    assert res.recommended_card["card_name"] == "Dozer Everyday Cashback Plus"
    assert "DOC-002" in res.citations
    assert "Marcus Vance" in res.answer
    assert "Dozer Everyday Cashback Plus" in res.answer
    assert "$1,640.00" in res.answer
    assert "[DOC-002]" in res.answer


def test_rag_pipeline_hyper_personalization_credit_builder() -> None:
    """13. Verify RAG enforces underwriting for `CUST-003` (score 645) and recommends `Dozer Horizon Credit Builder`."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)
    pipeline = DozerBankingRAGPipeline(dozer_client=client)

    res = pipeline.ask(
        customer_id="CUST-003",
        question="Can I get a credit card to build my credit score with a low APR and zero annual fee?",
    )

    assert len(res.eligible_cards) == 1
    assert res.recommended_card is not None
    assert res.recommended_card["card_id"] == "CARD-BUILDER-SEC"
    assert "DOC-003" in res.citations
    assert all(int(d.metadata["min_credit_score"]) <= 645 for d in res.retrieved_documents)
    assert "Elena Rostova" in res.answer
    assert "Dozer Horizon Credit Builder" in res.answer
    assert "[DOC-003]" in res.answer


def test_rag_pipeline_hyper_personalization_tech_executive_customer() -> None:
    """Verify RAG recommends `Dozer Quantum Executive Commercial` for cloud/tech-heavy `CUST-004`."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)
    pipeline = DozerBankingRAGPipeline(dozer_client=client)

    res = pipeline.ask(
        customer_id="CUST-004",
        question="Which commercial card maximizes rewards on our AWS cloud infrastructure and SaaS spend?",
    )

    assert res.recommended_card is not None
    assert res.recommended_card["card_id"] == "CARD-TECH-EXEC"
    assert "DOC-004" in res.citations
    assert "David Chen" in res.answer
    assert "$5,800.00" in res.answer
    assert "[DOC-004]" in res.answer


def test_rag_pipeline_realtime_cdc_transaction_and_credit_score_upgrade() -> None:
    """14. Verify live Dozer CDC events dynamically shift RAG eligibility, spend totals, and citations."""
    client = DozerClient(offline=True, seed_sql_path=SEED_SQL_PATH)
    pipeline = DozerBankingRAGPipeline(dozer_client=client)

    # Before CDC: CUST-003 only qualifies for CARD-BUILDER-SEC
    before = pipeline.ask(
        customer_id="CUST-003",
        question="Which travel card with airport lounge access can I qualify for?",
    )
    assert before.recommended_card is not None
    assert before.recommended_card["card_id"] == "CARD-BUILDER-SEC"

    # Stream CDC events: upgrade CUST-003 credit score & income, insert $4,800 travel txn, insert DOC-009
    pipeline.apply_cdc_event(
        CDCEvent(
            endpoint="/customers",
            operation=CDCOperation.UPDATE,
            primary_key="CUST-003",
            new_record={
                "customer_id": "CUST-003",
                "full_name": "Elena Rostova",
                "email": "elena.rostova@example.com",
                "credit_score": 780,
                "annual_income": 135000.0,
                "account_tier": "Platinum",
                "preferred_reward_type": "Travel & Airport Lounge",
                "updated_at": "2026-03-01 10:00:00",
            },
        )
    )
    pipeline.apply_cdc_event(
        CDCEvent(
            endpoint="/transactions",
            operation=CDCOperation.INSERT,
            primary_key="TXN-9001",
            new_record={
                "txn_id": "TXN-9001",
                "customer_id": "CUST-003",
                "merchant_name": "Qatar Airways Business Class",
                "category": "Travel",
                "amount": 4800.0,
                "txn_timestamp": "2026-03-01 10:05:00",
            },
        )
    )
    pipeline.apply_cdc_event(
        CDCEvent(
            endpoint="/documents",
            operation=CDCOperation.INSERT,
            primary_key="DOC-009",
            new_record={
                "doc_id": "DOC-009",
                "title": "Platinum Upgrade Fast-Track Airport Lounge Welcome Offer",
                "category": "Travel",
                "card_id": "CARD-TRAVEL-SIG",
                "min_credit_score": 750,
                "content": "Newly upgraded Platinum members qualifying for Dozer Sapphire Reserve Travel receive instant lounge access and 15,000 bonus miles.",
                "updated_at": "2026-03-01 10:06:00",
            },
        )
    )

    after = pipeline.ask(
        customer_id="CUST-003",
        question="Which travel card with airport lounge access can I qualify for now?",
    )
    assert after.customer_profile["credit_score"] == 780
    assert after.customer_profile["travel_spend"] == pytest.approx(4800.0)
    assert after.recommended_card is not None
    assert after.recommended_card["card_id"] == "CARD-TRAVEL-SIG"
    assert any(c in after.citations for c in ("DOC-001", "DOC-009"))
    assert "Dozer Sapphire Reserve Travel" in after.answer
    assert "$4,800.00" in after.answer


def test_cli_demo_and_ask_execution(capsys: pytest.CaptureFixture[str]) -> None:
    """15. Verify `app.py` CLI runs cleanly in `--mode demo`, `--mode ask`, and `--mode sync`."""
    rc_demo = cli_main(["--mode", "demo", "--offline", "--config", str(CONFIG_PATH)])
    assert rc_demo == 0
    demo_out = capsys.readouterr().out
    assert "Dozer + Vector DB + LangChain Hyper-Personalized RAG Demo" in demo_out
    assert "Simulating Real-Time Dozer CDC Events for CUST-003" in demo_out

    rc_ask = cli_main(
        [
            "--mode",
            "ask",
            "--customer-id",
            "CUST-001",
            "--question",
            "Which travel card gives airport lounge access?",
            "--offline",
            "--config",
            str(CONFIG_PATH),
        ]
    )
    assert rc_ask == 0
    ask_out = capsys.readouterr().out
    assert "Dozer Sapphire Reserve Travel" in ask_out
    assert "DOC-001" in ask_out

    rc_sync = cli_main(["--mode", "sync", "--offline", "--config", str(CONFIG_PATH)])
    assert rc_sync == 0
    sync_out = capsys.readouterr().out
    assert "Synchronized 8 documents from Dozer" in sync_out
