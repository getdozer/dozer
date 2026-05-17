from __future__ import annotations

from collections import Counter
from typing import Any


def build_customer_context(
    customer: dict[str, Any],
    transactions: list[dict[str, Any]],
) -> str:
    categories = Counter(row["merchant_category"] for row in transactions)
    total_spend = sum(float(row["amount_usd"]) for row in transactions)
    top_categories = ", ".join(
        f"{category} ({count})" for category, count in categories.most_common(3)
    )
    return (
        f"Customer {customer['customer_id']} is in the {customer['segment']} "
        f"segment with income band {customer['income_band']}. Goals: "
        f"{customer['goals']}. Risk tolerance: {customer['risk_tolerance']}. "
        f"Recent spend: ${total_spend:.2f}. Top categories: {top_categories}."
    )


def build_product_context(product: dict[str, Any]) -> str:
    return (
        f"Card {product['name']} has an annual fee of "
        f"${float(product['annual_fee_usd']):.0f}. Rewards focus: "
        f"{product['rewards_focus']}. Intro offer: {product['intro_offer']}. "
        f"Eligibility: {product['eligibility']}. Caveat: {product['caveat']}."
    )


def rank_products(
    customer: dict[str, Any],
    transactions: list[dict[str, Any]],
    products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    category_counts = Counter(row["merchant_category"].lower() for row in transactions)
    goals = str(customer.get("goals", "")).lower()
    income_band = str(customer.get("income_band", "")).lower()
    ranked: list[dict[str, Any]] = []

    for product in products:
        focus = str(product.get("rewards_focus", "")).lower()
        eligibility = str(product.get("eligibility", "")).lower()
        fee = float(product.get("annual_fee_usd", 0) or 0)
        score = 0
        reasons: list[str] = []

        for category, count in category_counts.items():
            if category in focus:
                score += count * 3
                reasons.append(f"matches {category} spend")

        for goal in ("travel", "cashback", "balance transfer", "groceries"):
            if goal in goals and goal in focus:
                score += 4
                reasons.append(f"supports {goal} goal")

        if "premium" in income_band and fee > 0:
            score += 1
            reasons.append("premium segment can absorb an annual fee")
        elif "starter" in income_band and fee == 0:
            score += 2
            reasons.append("no annual fee suits starter income band")

        if "excellent credit" in eligibility and "starter" in income_band:
            score -= 2
            reasons.append("eligibility may be tight")

        ranked.append(
            {
                **product,
                "score": score,
                "reason": "; ".join(reasons) or "general fit",
            }
        )

    return sorted(ranked, key=lambda row: row["score"], reverse=True)
