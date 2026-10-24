from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, List, Optional

from .dozer_client import (
    CategorySpendSummary,
    CreditCardProduct,
    CustomerProfile,
    DozerFeatureStoreClient,
)
from .vector_store import CreditCardVectorKnowledgeBase, VectorSearchHit


@dataclass(frozen=True)
class CardEvaluation:
    card: CreditCardProduct
    eligible: bool
    ineligibility_reasons: List[str]
    annualized_rewards_usd: float
    net_first_year_value_usd: float
    net_ongoing_annual_value_usd: float
    semantic_relevance_score: float
    knowledge_excerpt: str


@dataclass(frozen=True)
class AdvisorRecommendationResponse:
    customer: CustomerProfile
    spending_summary: List[CategorySpendSummary]
    recommended_card: CardEvaluation
    all_evaluations: List[CardEvaluation]
    retrieved_knowledge: List[VectorSearchHit]
    prompt_context: str
    advisor_message: str


class DozerLangChainBankingAdvisor:
    """
    Combines:
      1. Real-time structured customer & transaction features from Dozer (`DozerFeatureStoreClient`)
      2. Semantic retrieval over unstructured product terms (`CreditCardVectorKnowledgeBase`)
      3. LangChain prompt construction & LLM synthesis (`ChatOpenAI` when `OPENAI_API_KEY` is set,
         or deterministic grounded synthesis for offline/CI execution).
    """

    def __init__(
        self,
        dozer_client: Optional[DozerFeatureStoreClient] = None,
        vector_kb: Optional[CreditCardVectorKnowledgeBase] = None,
    ) -> None:
        self.dozer_client = dozer_client or DozerFeatureStoreClient()
        self.vector_kb = vector_kb or CreditCardVectorKnowledgeBase.from_markdown_file()

    @staticmethod
    def evaluate_card_for_customer(
        customer: CustomerProfile,
        card: CreditCardProduct,
        spending_summary: List[CategorySpendSummary],
        semantic_score: float,
        knowledge_excerpt: str,
        annualization_multiplier: float = 12.0,
    ) -> CardEvaluation:
        reasons: List[str] = []
        if customer.credit_score < card.min_credit_score:
            reasons.append(
                f"credit_score {customer.credit_score} < required {card.min_credit_score}"
            )
        if customer.annual_income < card.min_annual_income:
            reasons.append(
                f"annual_income ${customer.annual_income:,.0f} < required ${card.min_annual_income:,.0f}"
            )
        if customer.credit_utilization_pct > card.max_utilization_pct:
            reasons.append(
                f"utilization {customer.credit_utilization_pct:.1f}% > max {card.max_utilization_pct:.1f}%"
            )

        monthly_rewards = 0.0
        for item in spending_summary:
            if item.category == "travel":
                rate = card.travel_rate
            elif item.category == "dining":
                rate = card.dining_rate
            elif item.category == "grocery":
                rate = card.grocery_rate
            else:
                rate = card.base_rate
            monthly_rewards += item.total_spend * rate

        annualized_rewards = round(monthly_rewards * annualization_multiplier, 2)
        net_ongoing = round(annualized_rewards - card.annual_fee, 2)
        net_first_year = round(net_ongoing + card.welcome_bonus_usd, 2)

        return CardEvaluation(
            card=card,
            eligible=len(reasons) == 0,
            ineligibility_reasons=reasons,
            annualized_rewards_usd=annualized_rewards,
            net_first_year_value_usd=net_first_year,
            net_ongoing_annual_value_usd=net_ongoing,
            semantic_relevance_score=semantic_score,
            knowledge_excerpt=knowledge_excerpt,
        )

    def build_langchain_prompt(
        self,
        customer: CustomerProfile,
        user_question: str,
        spending_summary: List[CategorySpendSummary],
        evaluations: List[CardEvaluation],
    ) -> str:
        spend_lines = "\n".join(
            f"  - {s.category}: ${s.total_spend:,.2f} across {s.tx_count} transactions"
            for s in spending_summary
        )
        eval_lines = "\n".join(
            f"  - {ev.card.card_name} ({ev.card.card_id}): "
            f"eligible={ev.eligible}, net_ongoing_annual_value=${ev.net_ongoing_annual_value_usd:,.2f}, "
            f"first_year_value=${ev.net_first_year_value_usd:,.2f}, "
            f"semantic_score={ev.semantic_relevance_score:.3f}"
            + (
                f", reasons={'; '.join(ev.ineligibility_reasons)}"
                if ev.ineligibility_reasons
                else ""
            )
            for ev in evaluations
        )
        return (
            "You are a grounded Banking & Credit Card AI Advisor powered by Dozer real-time data.\n"
            f"Customer Profile (from Dozer /customers):\n"
            f"  - Name: {customer.full_name} ({customer.customer_id})\n"
            f"  - Credit Score: {customer.credit_score}\n"
            f"  - Annual Income: ${customer.annual_income:,.2f}\n"
            f"  - Credit Utilization: {customer.credit_utilization_pct:.1f}%\n"
            f"  - Preferred Benefit: {customer.preferred_benefit}\n"
            f"Real-Time Spending Summary (from Dozer /customer_spending_summary):\n{spend_lines}\n"
            f"Underwriting & Reward Evaluations (Dozer + Vector KB):\n{eval_lines}\n"
            f"Customer Question: {user_question}\n"
        )

    def recommend(self, customer_id: str, user_question: str) -> AdvisorRecommendationResponse:
        customer = self.dozer_client.get_customer_profile(customer_id)
        spending_summary = self.dozer_client.get_customer_spending_summary(customer_id)
        cards = self.dozer_client.list_credit_cards()

        combined_query = f"{user_question} {customer.preferred_benefit}"
        vector_hits = self.vector_kb.similarity_search(combined_query, top_k=len(cards))
        score_by_card: Dict[str, VectorSearchHit] = {
            hit.document.card_id: hit for hit in vector_hits
        }

        evaluations: List[CardEvaluation] = []
        for card in cards:
            hit = score_by_card.get(card.card_id)
            evaluations.append(
                self.evaluate_card_for_customer(
                    customer=customer,
                    card=card,
                    spending_summary=spending_summary,
                    semantic_score=hit.score if hit else 0.0,
                    knowledge_excerpt=hit.document.content if hit else "",
                )
            )

        eligible_evals = [e for e in evaluations if e.eligible]
        eligible_evals.sort(
            key=lambda e: (
                e.net_ongoing_annual_value_usd + (e.semantic_relevance_score * 250.0),
                e.net_first_year_value_usd,
            ),
            reverse=True,
        )
        recommended = eligible_evals[0] if eligible_evals else evaluations[0]
        prompt_context = self.build_langchain_prompt(
            customer=customer,
            user_question=user_question,
            spending_summary=spending_summary,
            evaluations=evaluations,
        )

        advisor_message = self._synthesize_response(
            customer=customer,
            recommended=recommended,
            evaluations=evaluations,
            prompt_context=prompt_context,
        )

        return AdvisorRecommendationResponse(
            customer=customer,
            spending_summary=spending_summary,
            recommended_card=recommended,
            all_evaluations=evaluations,
            retrieved_knowledge=vector_hits,
            prompt_context=prompt_context,
            advisor_message=advisor_message,
        )

    def _synthesize_response(
        self,
        customer: CustomerProfile,
        recommended: CardEvaluation,
        evaluations: List[CardEvaluation],
        prompt_context: str,
    ) -> str:
        if os.environ.get("OPENAI_API_KEY"):
            try:
                from langchain.chat_models import ChatOpenAI
                from langchain.schema import HumanMessage, SystemMessage

                llm = ChatOpenAI(temperature=0.0)
                resp = llm(
                    [
                        SystemMessage(
                            content="Recommend the best eligible credit card grounded strictly in Dozer metrics and vector KB terms."
                        ),
                        HumanMessage(content=prompt_context),
                    ]
                )
                return str(resp.content)
            except Exception:
                pass

        ineligible_notes = [
            f"{ev.card.card_name} (ineligible: {', '.join(ev.ineligibility_reasons)})"
            for ev in evaluations
            if not ev.eligible
        ]
        ineligible_summary = (
            f" Filtered out: {'; '.join(ineligible_notes)}." if ineligible_notes else ""
        )
        return (
            f"Recommended Card for {customer.full_name}: {recommended.card.card_name} "
            f"({recommended.card.card_id}). Based on your real-time Dozer spending profile "
            f"and credit score of {customer.credit_score}, this card delivers an estimated "
            f"${recommended.annualized_rewards_usd:,.2f}/year in rewards "
            f"(${recommended.net_ongoing_annual_value_usd:,.2f}/year net of the "
            f"${recommended.card.annual_fee:,.0f} annual fee, and "
            f"${recommended.net_first_year_value_usd:,.2f} in year-one value including the "
            f"${recommended.card.welcome_bonus_usd:,.0f} welcome bonus). "
            f"Product highlights: {recommended.knowledge_excerpt}{ineligible_summary}"
        )
