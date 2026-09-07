import csv
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"


def load_csv(name):
    path = DATA / name / f"{name}.csv"
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def load_bank():
    return {
        "customers": load_csv("customers"),
        "transactions": load_csv("transactions"),
        "cards": load_csv("cards"),
    }


def profile(customer, txns):
    mine = [t for t in txns if t["customer_id"] == customer["customer_id"]]
    spend = sum(float(t["amount"]) for t in mine)
    cats = Counter(t["category"] for t in mine)
    mix = []
    for cat, _n in cats.most_common(3):
        cat_spend = sum(float(t["amount"]) for t in mine if t["category"] == cat)
        mix.append(f"{cat} ${cat_spend:.0f}")
    return {
        "customer_id": customer["customer_id"],
        "name": customer["name"],
        "age": customer["age"],
        "income_band": customer["income_band"],
        "segment": customer["segment"],
        "goals": customer["goals"],
        "risk": customer["risk"],
        "txn_count": len(mine),
        "spend": round(spend, 2),
        "top_spend": ", ".join(mix),
        "text": (
            f"{customer['name']} is a {customer['segment']} "
            f"({customer['income_band']} income). Goals: {customer['goals']}. "
            f"Last 90 days: ${spend:.0f} across {len(mine)} txns. "
            f"Spend mix: {', '.join(mix)}."
        ),
    }


def all_profiles(bank=None):
    bank = bank or load_bank()
    return [profile(c, bank["transactions"]) for c in bank["customers"]]


def get_profile(customer_id, bank=None):
    bank = bank or load_bank()
    for c in bank["customers"]:
        if c["customer_id"] == customer_id:
            return profile(c, bank["transactions"])
    return None


def card_text(card):
    return (
        f"{card['name']}: rewards in {card['rewards']}. "
        f"Annual fee ${card['annual_fee']}. "
        f"Fits {card['eligibility']}. "
        f"Caveat: {card['caveat']}."
    )
