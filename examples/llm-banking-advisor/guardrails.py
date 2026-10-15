"""Financial eligibility and bank risk guardrails."""

from __future__ import annotations

from typing import Dict, List, Tuple

from models import CardProduct, CustomerProfile


class FinancialGuardrails:
    """Enforces bank underwriting and credit eligibility constraints.

    Hard constraints must be evaluated BEFORE vector retrieval or LLM generation
    to prevent recommending products the customer is legally or financially ineligible for.
    """

    @staticmethod
    def evaluate_card(
        customer: CustomerProfile, card: CardProduct
    ) -> Tuple[bool, List[str]]:
        """Evaluate a single card against customer financial parameters.

        Returns (is_eligible, reasons).
        """
        reasons: List[str] = []

        # 1. Minimum Age
        if customer.age < 18:
            reasons.append("Customer must be at least 18 years old.")

        # 2. Credit Score Threshold
        if customer.credit_score < card.min_credit_score:
            reasons.append(
                f"Credit score {customer.credit_score} is below required minimum of {card.min_credit_score}."
            )

        # 3. Minimum Annual Income
        if customer.annual_income_usd < card.min_annual_income_usd:
            reasons.append(
                f"Annual income ${customer.annual_income_usd:,.0f} is below minimum requirement of ${card.min_annual_income_usd:,.0f}."
            )

        # 4. Risk Tolerance / Premium Annual Fee Alignment
        if customer.risk_tolerance.lower() == "low" and card.annual_fee_usd > 200:
            reasons.append(
                f"Annual fee of ${card.annual_fee_usd:.0f} conflicts with conservative (low) risk tolerance."
            )

        is_eligible = len(reasons) == 0
        return is_eligible, reasons

    @classmethod
    def filter_eligible_cards(
        cls, customer: CustomerProfile, cards: List[CardProduct]
    ) -> Tuple[List[CardProduct], Dict[str, List[str]]]:
        """Split a card list into eligible cards and ineligible cards with failure reasons."""
        eligible: List[CardProduct] = []
        ineligible_reasons: Dict[str, List[str]] = {}

        for card in cards:
            is_ok, reasons = cls.evaluate_card(customer, card)
            if is_ok:
                eligible.append(card)
            else:
                ineligible_reasons[card.name] = reasons

        return eligible, ineligible_reasons
