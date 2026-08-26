from __future__ import annotations

from collections import Counter
from typing import Any

# Goal keywords that can be matched against both a customer's goals and a
# product's rewards focus.
GOAL_KEYWORDS = ("travel", "cashback", "groceries", "balance transfer")


def build_customer_context(
    customer: dict[str, Any],
    transactions: list[dict[str, Any]],
) -> str:
    """Turn a customer profile and their transactions into a single context string."""
    total_spend = sum(float(row["amount_usd"]) for row in transactions)
    categories = Counter(row["merchant_category"] for row in transactions)
    top_categories = ", ".join(
        f"{category} ({count})" for category, count in categories.most_common(3)
    )
    return (
        f"Customer {customer['customer_id']} is a {customer['segment']} with "
        f"income band {customer['income_band']}. Goals: {customer['goals']}. "
        f"Risk tolerance: {customer['risk_tolerance']}. Recent spend: "
        f"${total_spend:.2f} across {len(transactions)} transactions. "
        f"Top merchant categories: {top_categories}."
    )


def build_product_context(product: dict[str, Any]) -> str:
    """Turn a credit card product row into a context string."""
    return (
        f"Card {product['name']} charges an annual fee of "
        f"${float(product['annual_fee_usd']):.0f}. Rewards focus: "
        f"{product['rewards_focus']}. Intro offer: {product['intro_offer']}. "
        f"Eligibility: {product['eligibility']}. Caveat: {product['caveat']}."
    )


def rank_products(
    customer: dict[str, Any],
    transactions: list[dict[str, Any]],
    products: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Score every product against the customer and return them sorted best first."""
    spend_by_category = Counter(
        row["merchant_category"].lower() for row in transactions
    )
    goals = str(customer.get("goals", "")).lower()
    income_band = str(customer.get("income_band", "")).lower()

    ranked: list[dict[str, Any]] = []
    for product in products:
        focus = str(product.get("rewards_focus", "")).lower()
        eligibility = str(product.get("eligibility", "")).lower()
        fee = float(product.get("annual_fee_usd", 0) or 0)

        score = 0.0
        reasons: list[str] = []

        # Reward categories that match the customer's actual spending habits.
        for category, count in spend_by_category.items():
            if category in focus:
                score += count * 3
                reasons.append(f"rewards match {category} spend")

        # Stated goals that the product directly supports.
        for goal in GOAL_KEYWORDS:
            if goal in goals and goal in focus:
                score += 4
                reasons.append(f"supports {goal} goal")

        # Income band fit with the annual fee.
        if "premium" in income_band:
            if fee > 0:
                score += 1
                reasons.append("premium income can absorb the annual fee")
        elif fee == 0:
            score += 2
            reasons.append("no annual fee fits a starter income band")

        # Eligibility requirements that are likely out of reach.
        if "excellent credit" in eligibility and income_band in ("starter", "mass market"):
            score -= 2
            reasons.append("eligibility requirements may be out of reach")

        ranked.append(
            {
                **product,
                "score": score,
                "reason": "; ".join(reasons) if reasons else "general fit",
            }
        )

    return sorted(ranked, key=lambda row: row["score"], reverse=True)