#!/usr/bin/env python3
"""Dozer LLM Banking Advisor CLI and Interactive Experience."""

from __future__ import annotations

import argparse
import json
import sys

from advisor import BankingAdvisor
from dozer_client import DozerClient


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Dozer + LangChain + Vector RAG Hyper-Personalized Banking Advisor"
    )
    parser.add_argument(
        "--customer-id",
        default="CUST_001",
        help="Target customer ID to evaluate (e.g. CUST_001, CUST_002, CUST_003)",
    )
    parser.add_argument(
        "--query",
        default=None,
        help="Optional custom user search query or financial preference",
    )
    parser.add_argument(
        "--dozer-url",
        default="http://localhost:8080",
        help="Dozer REST API base URL (default: http://localhost:8080)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output result in machine-readable JSON format",
    )
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Launch interactive terminal consultation mode",
    )

    args = parser.parse_args(argv)

    client = DozerClient(base_url=args.dozer_url)
    advisor = BankingAdvisor(dozer_client=client)

    if args.interactive:
        print("=" * 70)
        print("  🏦  Dozer AI Banking Advisor — Interactive Consultation Mode")
        print("=" * 70)
        print(f"Connected to data source: {'LIVE DOZER API' if client.is_live() else 'DOZER LOCAL STREAM EMULATION'}")
        print("Available Customer IDs: CUST_001, CUST_002, CUST_003, CUST_004, CUST_005")
        print("Type 'exit' or 'quit' to finish.\n")

        while True:
            try:
                cid = input("Enter Customer ID [default: CUST_001]: ").strip() or "CUST_001"
                if cid.lower() in ("exit", "quit"):
                    break
                q = input("What is the customer looking for? [Press Enter for automatic profile matching]: ").strip()
                if q.lower() in ("exit", "quit"):
                    break

                result = advisor.recommend(cid, user_query=q or None)
                print("\n" + "-" * 50)
                print(f"✨ Recommendation for {cid}: {result.recommended_product.name}")
                print(f"🏆 Fit Score: {result.fit_score}")
                print(f"📊 Annual Fee: ${result.recommended_product.annual_fee_usd:.0f}")
                print(f"💡 Rationale:\n{result.rationale}")
                print("-" * 50 + "\n")
            except (KeyboardInterrupt, EOFError):
                break
            except Exception as e:
                print(f"Error: {e}\n")
        return 0

    try:
        res = advisor.recommend(args.customer_id, user_query=args.query)
    except Exception as exc:
        print(f"Advisory Error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        payload = {
            "customer_id": res.customer_id,
            "recommended_product": res.recommended_product.to_dict(),
            "fit_score": res.fit_score,
            "eligibility_status": res.eligibility_status,
            "spend_alignment": res.spend_alignment,
            "rationale": res.rationale,
            "source_provenance": res.source_provenance,
            "all_evaluated_cards": res.all_evaluated_cards,
        }
        print(json.dumps(payload, indent=2))
        return 0

    print("=" * 70)
    print(f"🏦 Dozer AI Banking Advisor Report | Customer: {res.customer_id}")
    print(f"Source Provenance: {res.source_provenance.upper()}")
    print("=" * 70)
    print(f"Top Recommendation : {res.recommended_product.name}")
    print(f"Product Tier       : {res.recommended_product.card_tier}")
    print(f"Annual Fee         : ${res.recommended_product.annual_fee_usd:.0f}")
    print(f"Fit Score          : {res.fit_score}")
    print(f"Eligibility Status : {res.eligibility_status}")
    print("-" * 70)
    print("Advisor Rationale:")
    print(res.rationale)
    print("-" * 70)
    print("Evaluated Cards:")
    for card in res.all_evaluated_cards:
        if card.get("is_eligible"):
            print(f"  • [ELIGIBLE] {card['name']}: score {card['score']} (Fee: ${card['annual_fee_usd']:.0f})")
        else:
            reasons = "; ".join(card.get("ineligibility_reasons", []))
            print(f"  • [INELIGIBLE] {card['name']}: {reasons}")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
