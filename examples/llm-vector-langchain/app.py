"""CLI Entrypoint for the Dozer + LLM + Vector Database + LangChain Sample.

Usage examples:
  python app.py --mode demo --offline
  python app.py --mode ask --customer-id CUST-001 --question "Which travel card gives airport lounge access?" --offline
  python app.py --mode sync --offline
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional

try:
    from .dozer_client import (
        CDCEvent,
        CDCOperation,
        DozerClient,
        load_and_validate_dozer_config,
    )
    from .rag_pipeline import DozerBankingRAGPipeline
except ImportError:
    from dozer_client import (
        CDCEvent,
        CDCOperation,
        DozerClient,
        load_and_validate_dozer_config,
    )
    from rag_pipeline import DozerBankingRAGPipeline


def run_demo(pipeline: DozerBankingRAGPipeline, config_path: Path) -> int:
    """Run an end-to-end interactive demonstration including real-time CDC updates."""
    cfg = load_and_validate_dozer_config(config_path)
    print("=" * 78)
    print(f"Dozer + Vector DB + LangChain Hyper-Personalized RAG Demo ({cfg['app_name']})")
    print("=" * 78)
    print(
        f"Validated Dozer Config: {len(cfg['connections'])} connection(s), "
        f"{len(cfg['sources'])} source table(s), {len(cfg['endpoints'])} endpoint(s)."
    )
    print(
        f"Vector Store Indexed Documents: {pipeline.vector_store.document_count} "
        f"(Index Version: v{pipeline.vector_store.index_version})"
    )
    print("-" * 78)

    scenarios = [
        (
            "CUST-001",
            "Which credit card should I use for international flights and airport lounge access?",
        ),
        (
            "CUST-002",
            "What is the best zero-annual-fee card for my weekly supermarket and grocery shopping?",
        ),
        (
            "CUST-003",
            "Which card can I qualify for right now to build my credit score with a low APR?",
        ),
    ]

    for customer_id, question in scenarios:
        response = pipeline.ask(customer_id=customer_id, question=question)
        print(f"\n[Customer: {customer_id}] Q: {question}")
        print(response.answer)
        print(f"Citations: {response.citations}")
        print("-" * 78)

    print("\n>>> Simulating Real-Time Dozer CDC Events for CUST-003 (Elena Rostova)...")
    # 1. Customer credit score & income upgrade via Dozer CDC
    pipeline.apply_cdc_event(
        CDCEvent(
            endpoint="/customers",
            operation=CDCOperation.UPDATE,
            primary_key="CUST-003",
            new_record={
                "customer_id": "CUST-003",
                "full_name": "Elena Rostova",
                "email": "elena.rostova@example.com",
                "credit_score": 775,
                "annual_income": 125000.0,
                "account_tier": "Platinum",
                "preferred_reward_type": "Travel & Airport Lounge",
                "updated_at": "2026-03-01 10:00:00",
            },
        )
    )
    # 2. New high-value travel transaction streamed into Dozer `/transactions`
    pipeline.apply_cdc_event(
        CDCEvent(
            endpoint="/transactions",
            operation=CDCOperation.INSERT,
            primary_key="TXN-2001",
            new_record={
                "txn_id": "TXN-2001",
                "customer_id": "CUST-003",
                "merchant_name": "Emirates First Class Direct",
                "category": "Travel",
                "amount": 4800.0,
                "txn_timestamp": "2026-03-01 10:05:00",
            },
        )
    )
    # 3. New promotional travel policy document streamed into `/documents` & Vector Store
    pipeline.apply_cdc_event(
        CDCEvent(
            endpoint="/documents",
            operation=CDCOperation.INSERT,
            primary_key="DOC-009",
            new_record={
                "doc_id": "DOC-009",
                "title": "Spring 2026 Platinum Travel Upgrade & Fast-Track Lounge Promotion",
                "category": "Travel",
                "card_id": "CARD-TRAVEL-SIG",
                "min_credit_score": 750,
                "content": (
                    "Customers newly upgraded to Platinum tier with credit score >= 750 "
                    "who apply for Dozer Sapphire Reserve Travel (CARD-TRAVEL-SIG) receive "
                    "an extra 15,000 fast-track airline miles and instant digital Priority Pass lounge activation."
                ),
                "updated_at": "2026-03-01 10:06:00",
            },
        )
    )

    updated_response = pipeline.ask(
        customer_id="CUST-003",
        question="Now that my credit score and travel spend increased, which travel lounge card do I qualify for?",
    )
    print("\n[After Real-Time CDC Update — Customer: CUST-003]")
    print(updated_response.answer)
    print(
        f"Citations: {updated_response.citations} | "
        f"Vector Store Docs: {pipeline.vector_store.document_count} "
        f"(Index Version: v{updated_response.vector_index_version})"
    )
    print("=" * 78)
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dozer + LLM + Vector Database + LangChain Banking RAG Assistant"
    )
    parser.add_argument(
        "--mode",
        choices=("demo", "ask", "sync"),
        default="demo",
        help="Execution mode: 'demo' (full walkthrough), 'ask' (single query), or 'sync' (CDC vector sync).",
    )
    parser.add_argument(
        "--customer-id",
        default="CUST-001",
        help="Customer ID to query in 'ask' mode (e.g. CUST-001, CUST-002, CUST-003, CUST-004).",
    )
    parser.add_argument(
        "--question",
        default="Which credit card best fits my spending profile and what are the perks?",
        help="Natural language question for the RAG assistant.",
    )
    parser.add_argument(
        "--dozer-url",
        default=os.environ.get("DOZER_REST_URL", "http://localhost:8080"),
        help="Base URL of the Dozer REST API server.",
    )
    parser.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parent / "dozer-config.yaml"),
        help="Path to dozer-config.yaml.",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use the built-in in-memory Dozer SQL engine seeded from seed.sql.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    config_path = Path(args.config)
    load_and_validate_dozer_config(config_path)

    client = DozerClient(
        base_url=args.dozer_url,
        offline=args.offline,
        fallback_to_in_memory=True,
    )
    pipeline = DozerBankingRAGPipeline(dozer_client=client)

    if args.mode == "demo":
        return run_demo(pipeline, config_path)

    if args.mode == "sync":
        events = pipeline.cdc_synchronizer.sync_once()
        print(
            f"Synchronized {pipeline.vector_store.document_count} documents from Dozer "
            f"({len(events)} new CDC diff events). Vector Index Version: v{pipeline.vector_store.index_version}"
        )
        return 0

    if args.mode == "ask":
        response = pipeline.ask(customer_id=args.customer_id, question=args.question)
        print(response.answer)
        print(f"Citations: {', '.join(response.citations)}")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
