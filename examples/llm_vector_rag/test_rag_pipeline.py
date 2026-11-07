#!/usr/bin/env python3
"""
Unit tests for Dozer RAG Pipeline.
Validates Dozer client parsing, Vector store indexing, and HyperPersonalizedChatbot logic.
"""

from rag_pipeline import DozerClient, VectorKnowledgeStore, HyperPersonalizedChatbot


def test_dozer_client_profiles():
    client = DozerClient()
    profiles = client.get_customer_profiles()
    assert len(profiles) >= 3
    alice = next(p for p in profiles if p["customer_id"] == 1)
    assert alice["customer_name"] == "Alice Chen"
    assert alice["credit_score"] == 760
    assert alice["has_travel_spend"] == 1


def test_dozer_client_cards():
    client = DozerClient()
    cards = client.get_card_products()
    assert len(cards) >= 4
    travel_card = next(c for c in cards if c["target_category"] == "Travel")
    assert travel_card["card_name"] == "AeroMiles Voyager"
    assert travel_card["min_credit_score"] == 720


def test_chatbot_prompt_generation():
    chatbot = HyperPersonalizedChatbot()
    data = chatbot.generate_recommendation_prompt(customer_id=1, user_query="best card for trips")
    
    assert data["customer"]["customer_id"] == 1
    assert "Alice Chen" in data["prompt"]
    assert "Travel" in data["prompt"]
    assert len(data["retrieved_cards"]) > 0


def test_chatbot_response_simulation():
    chatbot = HyperPersonalizedChatbot()
    res = chatbot.simulate_llm_response(customer_id=1, user_query="recommend card")
    assert "Alice Chen" in res
    assert "AeroMiles Voyager" in res
    assert "760" in res


if __name__ == "__main__":
    test_dozer_client_profiles()
    test_dozer_client_cards()
    test_chatbot_prompt_generation()
    test_chatbot_response_simulation()
    print("✅ All Dozer RAG Pipeline Unit Tests Passed Successfully!")
