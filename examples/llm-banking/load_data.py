"""Load the sample CSVs. Mirrors what Dozer ingests from LocalStorage."""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"


def _read_csv(folder: str, filename: str) -> list[dict[str, str]]:
    path = DATA / folder / filename
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def load_customers() -> list[dict[str, str]]:
    return _read_csv("customer_profiles", "customer_profiles.csv")


def load_transactions() -> list[dict[str, str]]:
    return _read_csv("transactions", "transactions.csv")


def load_products() -> list[dict[str, str]]:
    return _read_csv("credit_card_products", "credit_card_products.csv")


def build_customer_profile(customer_id: str) -> dict[str, Any]:
    """Same idea as the SQL INTO customer_profile in dozer-config.yaml."""
    customers = {c["customer_id"]: c for c in load_customers()}
    if customer_id not in customers:
        raise KeyError(f"unknown customer_id={customer_id}")

    cust = customers[customer_id]
    txns = [t for t in load_transactions() if t["customer_id"] == customer_id]
    spend_by_cat: dict[str, float] = defaultdict(float)
    for t in txns:
        spend_by_cat[t["merchant_category"]] += float(t["amount_usd"])

    total = sum(spend_by_cat.values())
    top = Counter(spend_by_cat).most_common(3)

    return {
        "customer_id": customer_id,
        "segment": cust["segment"],
        "income_band": cust["income_band"],
        "goals": cust["goals"],
        "risk_tolerance": cust["risk_tolerance"],
        "age": cust["age"],
        "total_spend": round(total, 2),
        "spend_by_category": dict(spend_by_cat),
        "top_categories": [c for c, _ in top],
        "transactions": txns,
    }


def profile_as_text(profile: dict[str, Any]) -> str:
    cats = ", ".join(
        f"{k} ${v:.2f}" for k, v in sorted(
            profile["spend_by_category"].items(), key=lambda kv: -kv[1]
        )
    )
    return (
        f"Customer {profile['customer_id']} ({profile['segment']}, "
        f"{profile['income_band']} income). Goals: {profile['goals']}. "
        f"Risk: {profile['risk_tolerance']}. Total recent spend "
        f"${profile['total_spend']:.2f}. Breakdown: {cats}."
    )


def product_as_text(product: dict[str, str]) -> str:
    return (
        f"{product['name']} (fee ${product['annual_fee_usd']}): "
        f"rewards on {product['rewards_focus']}. "
        f"Intro: {product['intro_offer']}. "
        f"Eligibility: {product['eligibility']}. "
        f"Note: {product['caveat']}."
    )
