"""Rank cards from a unified customer profile. No network needed."""

from __future__ import annotations

from typing import Any


def rank_cards(profile: dict[str, Any], products: list[dict[str, str]]) -> list[dict[str, Any]]:
    goals = profile.get("goals", "").lower()
    income = profile.get("income_band", "").lower()
    cats = {k.lower(): v for k, v in profile.get("spend_by_category", {}).items()}

    ranked: list[dict[str, Any]] = []
    for p in products:
        focus = p.get("rewards_focus", "").lower()
        elig = p.get("eligibility", "").lower()
        fee = float(p.get("annual_fee_usd") or 0)
        score = 0
        why: list[str] = []

        for cat, amt in cats.items():
            if cat in focus:
                # weight by spend a bit
                bump = 2 + min(int(amt // 50), 6)
                score += bump
                why.append(f"{cat} spend fits rewards")

        for g in ("travel", "cashback", "build credit", "dining", "lounge"):
            if g in goals and g in focus:
                score += 4
                why.append(f"goal '{g}'")

        if income == "starter" and fee == 0:
            score += 3
            why.append("no fee")
        elif income == "premium" and fee >= 95:
            score += 2
            why.append("premium ok with fee")

        if "excellent credit" in elig and income == "starter":
            score -= 3
            why.append("eligibility stretch")
        if "thin credit" in elig and income == "starter":
            score += 2
            why.append("starter-friendly")

        ranked.append({**p, "score": score, "why": "; ".join(why) or "baseline"})

    ranked.sort(key=lambda r: r["score"], reverse=True)
    return ranked


def local_answer(profile: dict[str, Any], ranked: list[dict[str, Any]], question: str) -> str:
    top = ranked[0]
    return (
        f"Q: {question}\n"
        f"For {profile['customer_id']} I'd lead with {top['name']} "
        f"(score {top['score']}). Reason: {top['why']}. "
        f"Profile spend ${profile['total_spend']:.2f} skewed to "
        f"{', '.join(profile['top_categories'])}."
    )
