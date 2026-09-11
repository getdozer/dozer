"""Tests for Financial Guardrails and Underwriting Rules."""

import unittest
import sys
from pathlib import Path

SAMPLE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SAMPLE_DIR))

from guardrails import FinancialGuardrails
from models import CardProduct, CustomerProfile


class TestFinancialGuardrails(unittest.TestCase):
    def setUp(self):
        self.premium_card = CardProduct(
            product_id="TEST_PREM",
            name="Platinum Elite",
            card_tier="Premium",
            annual_fee_usd=450.0,
            min_credit_score=750,
            min_annual_income_usd=100000.0,
            rewards_focus="Travel",
            intro_bonus="50k pts",
            apr_rate="18%",
            perks_summary="Lounges",
            target_segment="Affluent",
        )
        self.starter_card = CardProduct(
            product_id="TEST_START",
            name="Starter Cash",
            card_tier="Basic",
            annual_fee_usd=0.0,
            min_credit_score=580,
            min_annual_income_usd=20000.0,
            rewards_focus="Cashback",
            intro_bonus="None",
            apr_rate="24%",
            perks_summary="1% cashback",
            target_segment="Students",
        )

    def test_eligible_customer_passes(self):
        customer = CustomerProfile(
            customer_id="C_OK",
            first_name="Alice",
            last_name="Smith",
            age=32,
            credit_score=780,
            income_band="high",
            annual_income_usd=130000.0,
            segment="Professional",
            primary_goals="Travel",
            risk_tolerance="high",
            total_spend_usd=5000.0,
            transaction_count=10,
            max_single_transaction_usd=1000.0,
        )
        is_ok, reasons = FinancialGuardrails.evaluate_card(customer, self.premium_card)
        self.assertTrue(is_ok)
        self.assertEqual(len(reasons), 0)

    def test_underage_rejection(self):
        customer = CustomerProfile(
            customer_id="C_MINOR",
            first_name="Minor",
            last_name="Doe",
            age=17,
            credit_score=700,
            income_band="low",
            annual_income_usd=25000.0,
            segment="Student",
            primary_goals="Credit",
            risk_tolerance="low",
            total_spend_usd=100.0,
            transaction_count=2,
            max_single_transaction_usd=50.0,
        )
        is_ok, reasons = FinancialGuardrails.evaluate_card(customer, self.starter_card)
        self.assertFalse(is_ok)
        self.assertTrue(any("18 years old" in r for r in reasons))

    def test_low_credit_score_rejection(self):
        customer = CustomerProfile(
            customer_id="C_LOW_FICO",
            first_name="Bob",
            last_name="Ray",
            age=28,
            credit_score=680,
            income_band="middle",
            annual_income_usd=120000.0,
            segment="Tech",
            primary_goals="Travel",
            risk_tolerance="medium",
            total_spend_usd=2000.0,
            transaction_count=5,
            max_single_transaction_usd=400.0,
        )
        is_ok, reasons = FinancialGuardrails.evaluate_card(customer, self.premium_card)
        self.assertFalse(is_ok)
        self.assertTrue(any("Credit score" in r for r in reasons))

    def test_conservative_risk_tolerance_filters_high_fee(self):
        customer = CustomerProfile(
            customer_id="C_CONSERVATIVE",
            first_name="Clara",
            last_name="Oswald",
            age=45,
            credit_score=800,
            income_band="affluent",
            annual_income_usd=150000.0,
            segment="Family",
            primary_goals="Groceries",
            risk_tolerance="low",
            total_spend_usd=3000.0,
            transaction_count=8,
            max_single_transaction_usd=500.0,
        )
        is_ok, reasons = FinancialGuardrails.evaluate_card(customer, self.premium_card)
        self.assertFalse(is_ok)
        self.assertTrue(any("conservative (low) risk tolerance" in r for r in reasons))


if __name__ == "__main__":
    unittest.main()
