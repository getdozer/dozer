"""Hyper-personalized Banking Advisor orchestrator integrating Dozer, Guardrails & LLM."""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from dozer_client import DozerClient
from guardrails import FinancialGuardrails
from models import CardProduct, CustomerProfile, RecommendationResult
from vector_rag import DozerGroundedRetriever


class BankingAdvisor:
    """End-to-end banking product recommender based on real-time Dozer customer analytics."""

    def __init__(self, dozer_client: Optional[DozerClient] = None) -> None:
        self.client = dozer_client or DozerClient()

    def recommend(
        self,
        customer_id: str,
        user_query: Optional[str] = None,
    ) -> RecommendationResult:
        """Generate a fully verified and grounded credit card recommendation."""
        # Step 1: Ingest unified customer profile from Dozer
        profile = self.client.get_customer_profile(customer_id)
        source_prov = "dozer_live_api" if self.client.is_live() else "dozer_local_sql_emulation"

        # Step 2: Ingest card product catalog
        all_cards = self.client.get_card_catalog()

        # Step 3: Enforce Financial and Risk Guardrails (Pre-filter)
        eligible_cards, ineligible_details = FinancialGuardrails.filter_eligible_cards(
            profile, all_cards
        )

        if not eligible_cards:
            raise ValueError(
                f"Customer {profile.full_name} ({customer_id}) did not meet eligibility criteria for any active products: {ineligible_details}"
            )

        # Step 4: Semantic Retrieval & Category Spend Alignment via Vector Index
        retriever = DozerGroundedRetriever(eligible_cards)
        ranked = retriever.rank_cards_for_customer(
            profile, user_query=user_query, top_k=len(eligible_cards)
        )

        top_card, top_score, top_meta = ranked[0]

        # Step 5: Synthesize Explanation (LLM if API key available, else Rule-Based Explainer)
        rationale = self._generate_explanation(
            profile=profile,
            card=top_card,
            score=top_score,
            meta=top_meta,
            user_query=user_query,
        )

        all_evaluated = [
            {
                "product_id": c.product_id,
                "name": c.name,
                "score": round(s, 3),
                "is_eligible": True,
                "annual_fee_usd": c.annual_fee_usd,
                "details": m,
            }
            for c, s, m in ranked
        ]
        for name, reasons in ineligible_details.items():
            all_evaluated.append({
                "name": name,
                "is_eligible": False,
                "ineligibility_reasons": reasons,
            })

        return RecommendationResult(
            customer_id=customer_id,
            recommended_product=top_card,
            eligibility_status="VERIFIED_ELIGIBLE",
            fit_score=round(top_score, 3),
            rationale=rationale,
            spend_alignment=top_meta["spend_matches"],
            all_evaluated_cards=all_evaluated,
            source_provenance=source_prov,
        )

    def _generate_explanation(
        self,
        profile: CustomerProfile,
        card: CardProduct,
        score: float,
        meta: Dict[str, Any],
        user_query: Optional[str],
    ) -> str:
        """Synthesize a personalized advisory note."""
        # Try OpenAI if OPENAI_API_KEY is present
        if os.getenv("OPENAI_API_KEY"):
            try:
                from langchain_core.prompts import ChatPromptTemplate
                from langchain_openai import ChatOpenAI

                prompt = ChatPromptTemplate.from_messages([
                    (
                        "system",
                        "You are an expert banking financial advisor. You explain why the selected "
                        "credit card was chosen for this customer based strictly on their verified "
                        "Dozer transaction data, annual income, credit score, and goals. Be concise (3-4 sentences).",
                    ),
                    (
                        "user",
                        "Customer: {customer_name} (Segment: {segment}, Credit Score: {score}, Income: ${income:,.0f}).\n"
                        "Recent Total Spend: ${total_spend:,.2f} across categories: {categories}.\n"
                        "Stated Goals: {goals}.\n"
                        "Selected Card: {card_name} (Annual Fee: ${fee}, Perks: {perks}).\n"
                        "Customer Question/Prompt: {query}\n"
                        "Provide a professional personalized recommendation.",
                    ),
                ])
                llm = ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"), temperature=0.2)
                chain = prompt | llm
                response = chain.invoke({
                    "customer_name": profile.full_name,
                    "segment": profile.segment,
                    "score": profile.credit_score,
                    "income": profile.annual_income_usd,
                    "total_spend": profile.total_spend_usd,
                    "categories": ", ".join(f"{k}: ${v:.2f}" for k, v in profile.spend_by_category.items()),
                    "goals": profile.primary_goals,
                    "card_name": card.name,
                    "fee": card.annual_fee_usd,
                    "perks": card.perks_summary,
                    "query": user_query or "Best matching credit card recommendation",
                })
                return str(response.content)
            except Exception:
                pass

        # Deterministic financial advisor synthesis
        matched_cats = ", ".join(
            f"{k.title()} (${v:,.2f})" for k, v in meta.get("spend_matches", {}).items()
        ) or "everyday purchases"

        fee_str = f"with a ${card.annual_fee_usd:.0f} annual fee" if card.annual_fee_usd > 0 else "with $0 annual fee"

        return (
            f"Based on {profile.full_name}'s recent transaction profile totaling ${profile.total_spend_usd:,.2f}, "
            f"the {card.name} is the optimal product ({fee_str}). "
            f"The customer's highest spending concentration aligns directly with {card.rewards_focus} ({matched_cats}), "
            f"while satisfying all creditworthiness standards (FICO {profile.credit_score} vs {card.min_credit_score} min). "
            f"Key benefits include: {card.perks_summary}"
        )
