#!/usr/bin/env python3
"""Dozer-shaped banking sample: unified profile + vector retrieval + card pick.

Offline by default (reads the same CSVs Dozer would ingest). Pass --use-clickhouse
if you sank customer_profile into ClickHouse via dozer-config.yaml.
"""

from __future__ import annotations

import argparse
import os
import sys

from load_data import (
    build_customer_profile,
    load_products,
    product_as_text,
    profile_as_text,
)
from recommend import local_answer, rank_cards
from vector_rag import build_docs, retrieve


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--customer-id", default="cust_002")
    parser.add_argument(
        "--question",
        default="Which credit card should we offer this customer and why?",
    )
    parser.add_argument(
        "--use-clickhouse",
        action="store_true",
        help="try reading customer_profile from ClickHouse first",
    )
    parser.add_argument(
        "--no-vector",
        action="store_true",
        help="skip chroma; just print ranked cards",
    )
    args = parser.parse_args(argv)

    profile = None
    if args.use_clickhouse:
        from clickhouse_client import fetch_profile

        ch = fetch_profile(args.customer_id)
        if ch:
            # ClickHouse row is the SQL aggregate; fill spend detail from CSV.
            profile = build_customer_profile(args.customer_id)
            profile.update({k: ch[k] for k in ch if k in profile or k == "total_spend"})
            print("loaded profile from ClickHouse + local txn detail")
        else:
            print("clickhouse miss; falling back to CSV profile", file=sys.stderr)

    if profile is None:
        profile = build_customer_profile(args.customer_id)

    products = load_products()
    ranked = rank_cards(profile, products)

    print("--- profile ---")
    print(profile_as_text(profile))
    print("--- ranked ---")
    for i, p in enumerate(ranked, 1):
        print(f"{i}. {p['name']} score={p['score']} ({p['why']})")

    if not args.no_vector:
        docs = build_docs(profile_as_text(profile), products, product_as_text)
        # bake profile terms into the query so hash/chroma retrieval isn't random
        q = f"{args.question} {profile['goals']} {' '.join(profile['top_categories'])}"
        hits = retrieve(docs, q, k=3)
        print("--- vector hits ---")
        for h in hits:
            print(f"* [{h.metadata.get('kind')}] {h.page_content[:120]}...")

    if os.getenv("OPENAI_API_KEY"):
        try:
            from langchain_openai import ChatOpenAI
            from langchain_core.prompts import ChatPromptTemplate

            ctx = profile_as_text(profile) + "\n" + "\n".join(
                f"- {p['name']}: {p['why']} (score {p['score']})" for p in ranked[:3]
            )
            prompt = ChatPromptTemplate.from_template(
                "You are a bank product advisor. Context:\n{ctx}\n\nQuestion: {q}\n"
                "Pick one card and say why in 3 sentences."
            )
            llm = ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"), temperature=0)
            msg = (prompt | llm).invoke({"ctx": ctx, "q": args.question})
            print("--- llm ---")
            print(msg.content)
            return 0
        except Exception as exc:
            print(f"llm failed ({exc}); using local answer", file=sys.stderr)

    print("--- answer ---")
    print(local_answer(profile, ranked, args.question))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
