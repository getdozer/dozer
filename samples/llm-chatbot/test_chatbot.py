"""
Unit and integration tests for the Dozer + LangChain banking chatbot.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest
from dozer_retriever import CustomerProfile, DozerClient
from chatbot import FinancialAdvisorChatbot, USER_PROMPT_TEMPLATE, SYSTEM_PROMPT


def test_dozer_local_calculation():
    """Tests the fallback local calculation when Dozer service is not running."""
    client = DozerClient(base_url="http://non-existent-dozer-host:8080")
    profile = client.get_profile("CUST_001")

    assert profile is not None
    assert profile.customer_id == "CUST_001"
    assert profile.name == "Elena Rostova"
    assert profile.credit_score == 785
    # CUST_001 has 3 travel txns: 1250 + 480 + 2400 = 4130
    assert profile.travel_spend_90d == 4130.0
    # CUST_001 has 2 dining txns: 210 + 185.5 = 395.5
    assert profile.dining_spend_90d == 395.5
    assert profile.grocery_spend_90d == 0.0


def test_dozer_customer_2_groceries():
    """Tests grocery spender profile (CUST_002)."""
    client = DozerClient(base_url="http://non-existent-dozer-host:8080")
    profile = client.get_profile("CUST_002")

    assert profile is not None
    assert profile.customer_id == "CUST_002"
    assert profile.name == "Marcus Vance"
    # Groceries: 320 + 195.40 + 410 = 925.40
    assert profile.grocery_spend_90d == 925.40
    assert profile.travel_spend_90d == 0.0


def test_format_as_context():
    """Verify that CustomerProfile serializes spending and credit stats into prompt context."""
    profile = CustomerProfile(
        customer_id="CUST_TEST",
        name="Test User",
        age=30,
        occupation="Engineer",
        annual_income=100000.0,
        credit_score=750,
        loyalty_tier="Gold",
        total_transactions=5,
        total_spend_90d=2500.0,
        travel_spend_90d=1500.0,
        dining_spend_90d=500.0,
        grocery_spend_90d=500.0,
        business_spend_90d=0.0,
    )
    context = profile.format_as_context()
    assert "CUST_TEST" in context
    assert "750" in context
    assert "$1,500.00" in context
    assert "Dozer Unified View" in context


def test_chatbot_flow_with_mocked_llm(monkeypatch):
    """Verifies end-to-end chain invocation passing Dozer context and vector docs."""
    mock_llm = MagicMock()
    mock_llm.invoke.return_value = MagicMock(
        content="Personalized recommendation: AeroElite card for high travel spend."
    )

    with patch("chatbot.get_llm", return_value=mock_llm):
        bot = FinancialAdvisorChatbot()
        response = bot.generate_response(
            customer_id="CUST_001",
            query="Which card is best for my travel habits?",
        )

        assert "AeroElite" in response
        assert mock_llm.invoke.called
        call_args = mock_llm.invoke.call_args[0][0]
        # Inspect system and human prompt content
        system_msg = call_args[0].content
        human_msg = call_args[1].content

        assert "Apex Bank's Senior Financial Product Advisor" in system_msg
        assert "Elena Rostova" in human_msg
        assert "Travel Spend: $4,130.00" in human_msg
