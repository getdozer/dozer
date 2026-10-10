from collections import defaultdict
from datetime import date
from typing import Any


def _record_value(record: dict[str, Any], key: str) -> str:
    return str(record.get(key, "")).strip()


def _same_customer(record: dict[str, Any], customer_id: str) -> bool:
    return _record_value(record, "customer_id").casefold() == customer_id.casefold()


def _monthly_spend(transactions: list[dict[str, Any]]) -> dict[str, int]:
    spend_by_month: dict[str, int] = defaultdict(int)
    for transaction in transactions:
        transaction_date = transaction.get("transaction_date")
        month = (
            transaction_date[:7]
            if isinstance(transaction_date, str) and len(transaction_date) >= 7
            else "unknown"
        )
        spend_by_month[month] += int(transaction.get("amount_cents", 0))
    return dict(spend_by_month)


def build_customer_profile(
    customer_id: str,
    customers: list[dict[str, Any]],
    transactions: list[dict[str, Any]],
    credit_cards: list[dict[str, Any]],
) -> dict[str, Any]:
    customer = next(
        (
            record
            for record in customers
            if _record_value(record, "id").casefold() == customer_id.casefold()
        ),
        None,
    )
    if customer is None:
        raise ValueError(f"Customer {customer_id} was not found")

    customer_transactions = [
        record for record in transactions if _same_customer(record, customer_id)
    ]
    cards = [record for record in credit_cards if _same_customer(record, customer_id)]

    return {
        "customer_id": _record_value(customer, "id"),
        "full_name": _record_value(customer, "full_name"),
        "email": _record_value(customer, "email"),
        "city": _record_value(customer, "city"),
        "state": _record_value(customer, "state"),
        "preferred_language": _record_value(customer, "preferred_language"),
        "risk_tier": _record_value(customer, "risk_tier"),
        "cards": cards,
        "transactions": customer_transactions,
        "monthly_spend_cents": _monthly_spend(customer_transactions),
    }


def format_customer_profile(profile: dict[str, Any]) -> str:
    cards = profile["cards"]
    card_lines = [
        f"{card.get('product_id', 'unknown')} ending in {card.get('last_four', '????')} ({card.get('status', 'unknown')})"
        for card in cards
    ]
    transaction_lines = [
        f"{transaction.get('transaction_date', 'unknown')}: {transaction.get('merchant', 'unknown')} - {transaction.get('category', 'unknown')} - {transaction.get('amount_cents', 0)} cents"
        for transaction in profile["transactions"]
    ]
    spend_lines = [
        f"{month}: {amount} cents" for month, amount in profile["monthly_spend_cents"].items()
    ]

    return "\n".join(
        [
            f"Customer: {profile['full_name']} ({profile['customer_id']})",
            f"Location: {profile['city']}, {profile['state']}",
            f"Preferred language: {profile['preferred_language']}",
            f"Risk tier: {profile['risk_tier']}",
            "Cards:",
            *(f"- {line}" for line in card_lines),
            "Monthly spend:",
            *(f"- {line}" for line in spend_lines),
            "Recent transactions:",
            *(f"- {line}" for line in transaction_lines),
        ]
    )
