"""Data models for Dozer-backed LLM Banking Advisor."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Transaction:
    transaction_id: str
    customer_id: str
    merchant_name: str
    merchant_category: str
    amount_usd: float
    txn_date: str


@dataclass
class CustomerProfile:
    customer_id: str
    first_name: str
    last_name: str
    age: int
    credit_score: int
    income_band: str
    annual_income_usd: float
    segment: str
    primary_goals: str
    risk_tolerance: str
    total_spend_usd: float
    transaction_count: int
    max_single_transaction_usd: float
    spend_by_category: Dict[str, float] = field(default_factory=dict)
    top_categories: List[str] = field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"


@dataclass
class CardProduct:
    product_id: str
    name: str
    card_tier: str
    annual_fee_usd: float
    min_credit_score: int
    min_annual_income_usd: float
    rewards_focus: str
    intro_bonus: str
    apr_rate: str
    perks_summary: str
    target_segment: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "product_id": self.product_id,
            "name": self.name,
            "card_tier": self.card_tier,
            "annual_fee_usd": self.annual_fee_usd,
            "min_credit_score": self.min_credit_score,
            "min_annual_income_usd": self.min_annual_income_usd,
            "rewards_focus": self.rewards_focus,
            "intro_bonus": self.intro_bonus,
            "apr_rate": self.apr_rate,
            "perks_summary": self.perks_summary,
            "target_segment": self.target_segment,
        }


@dataclass
class RecommendationResult:
    customer_id: str
    recommended_product: CardProduct
    eligibility_status: str
    fit_score: float
    rationale: str
    spend_alignment: Dict[str, Any]
    all_evaluated_cards: List[Dict[str, Any]]
    source_provenance: str
