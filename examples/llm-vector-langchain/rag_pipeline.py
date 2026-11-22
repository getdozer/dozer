"""LangChain Hyper-Personalized Banking RAG Pipeline powered by Dozer & Vector Search.

Combines:
1. Real-time structured customer context from Dozer (`/customer_profiles`, `/eligible_cards`,
   `/transactions`).
2. Real-time CDC-synchronized semantic vector retrieval (`DozerLangChainVectorStore`)
   with credit-score eligibility metadata filtering.
3. LangChain Expression Language (LCEL) pipeline (`ChatPromptTemplate | LLM | StrOutputParser`)
   supporting both OpenAI (`OPENAI_API_KEY`) and deterministic context-grounded offline execution.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.documents import Document
from langchain_core.language_models.llms import LLM
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

try:
    from .dozer_client import CDCEvent, DozerCDCSynchronizer, DozerClient
    from .vector_store import DeterministicBankingEmbeddings, DozerLangChainVectorStore
except ImportError:
    from dozer_client import CDCEvent, DozerCDCSynchronizer, DozerClient
    from vector_store import DeterministicBankingEmbeddings, DozerLangChainVectorStore


RAG_PROMPT_TEMPLATE = """You are a senior banking & credit card advisor powered by Dozer real-time streaming data and LangChain RAG.
Use ONLY the live Dozer customer spending profile, Dozer SQL pre-filtered eligible cards, and Vector Store policy documents below to answer the customer's question.
Always include exact spending figures, the top recommended eligible card, estimated annual net reward value, and explicit document citations in brackets like [DOC-001].

[DOZER_CUSTOMER_PROFILE]
{customer_profile_json}

[DOZER_RANKED_ELIGIBLE_CARDS]
{eligible_cards_json}

[VECTOR_RETRIEVED_DOCUMENTS]
{retrieved_documents_text}

[USER_QUESTION]
{question}
"""


@dataclass
class RAGResponse:
    """Structured result returned by `DozerBankingRAGPipeline.ask`."""

    customer_id: str
    question: str
    answer: str
    customer_profile: Dict[str, Any]
    eligible_cards: List[Dict[str, Any]]
    recommended_card: Optional[Dict[str, Any]]
    retrieved_documents: List[Document]
    citations: List[str] = field(default_factory=list)
    vector_index_version: int = 0


class RuleBasedBankingLLM(LLM):
    """Context-grounded LangChain LLM that synthesizes answers from Dozer + Vector context."""

    @property
    def _llm_type(self) -> str:
        return "dozer-context-grounded-banking-llm"

    @staticmethod
    def _extract_section(prompt: str, header: str, next_header: Optional[str]) -> str:
        start_idx = prompt.find(header)
        if start_idx == -1:
            return ""
        content_start = start_idx + len(header)
        if next_header:
            end_idx = prompt.find(next_header, content_start)
            if end_idx != -1:
                return prompt[content_start:end_idx].strip()
        return prompt[content_start:].strip()

    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> str:
        profile_raw = self._extract_section(
            prompt, "[DOZER_CUSTOMER_PROFILE]", "[DOZER_RANKED_ELIGIBLE_CARDS]"
        )
        cards_raw = self._extract_section(
            prompt, "[DOZER_RANKED_ELIGIBLE_CARDS]", "[VECTOR_RETRIEVED_DOCUMENTS]"
        )
        docs_raw = self._extract_section(
            prompt, "[VECTOR_RETRIEVED_DOCUMENTS]", "[USER_QUESTION]"
        )
        question_raw = self._extract_section(prompt, "[USER_QUESTION]", None)

        try:
            profile = json.loads(profile_raw) if profile_raw else {}
        except json.JSONDecodeError:
            profile = {}

        try:
            eligible_cards = json.loads(cards_raw) if cards_raw else []
        except json.JSONDecodeError:
            eligible_cards = []

        doc_ids = re.findall(r"\[(DOC-[A-Z0-9-]+)\]", docs_raw)
        unique_citations: List[str] = []
        for did in doc_ids:
            if did not in unique_citations:
                unique_citations.append(did)

        full_name = profile.get("full_name", "Valued Customer")
        cid = profile.get("customer_id", "UNKNOWN")
        score = profile.get("credit_score", "N/A")
        income = float(profile.get("annual_income", 0.0))
        tier = profile.get("account_tier", "Standard")
        total_spend = float(profile.get("total_spend", 0.0))
        travel_spend = float(profile.get("travel_spend", 0.0))
        dining_spend = float(profile.get("dining_spend", 0.0))
        groceries_spend = float(profile.get("groceries_spend", 0.0))
        tech_spend = float(profile.get("tech_spend", 0.0))

        if not eligible_cards:
            citation_str = " ".join(f"[{c}]" for c in unique_citations) or "[NONE]"
            return (
                f"Hello {full_name} ({cid}), based on your live Dozer profile "
                f"(credit score: {score}, annual income: ${income:,.2f}), no credit card "
                f"products currently meet the minimum underwriting threshold. {citation_str}"
            )

        top_card = eligible_cards[0]
        card_name = top_card["card_name"]
        card_id = top_card["card_id"]
        focus = top_card["category_focus"]
        rate = top_card["reward_rate_pct"]
        fee = float(top_card["annual_fee"])
        net_benefit = float(top_card.get("estimated_annual_net_benefit", 0.0))
        perks = top_card.get("perks_summary", "")
        welcome_bonus = top_card.get("welcome_bonus", "")

        citation_tags = " ".join(f"[{c}]" for c in unique_citations)
        alt_cards = [c["card_name"] for c in eligible_cards[1:3]]
        alt_summary = (
            f" Other eligible cards from Dozer SQL underwriting: {', '.join(alt_cards)}."
            if alt_cards
            else " No higher-tier cards are currently eligible at your credit score threshold."
        )

        lines = [
            f"Personalized Dozer RAG Recommendation for {full_name} ({cid} | {tier} Tier):",
            (
                f"1. Live Dozer Spending & Credit Profile: Credit Score {score}, "
                f"Annual Income ${income:,.2f}, Total Tracked Spend ${total_spend:,.2f} "
                f"(Travel: ${travel_spend:,.2f}, Dining: ${dining_spend:,.2f}, "
                f"Groceries: ${groceries_spend:,.2f}, Technology: ${tech_spend:,.2f})."
            ),
            (
                f"2. Top Recommended Card: {card_name} ({card_id}) — {rate}% rewards on {focus}, "
                f"${fee:,.2f} annual fee, welcome bonus: {welcome_bonus}, "
                f"estimated annual net benefit: ${net_benefit:,.2f}."
            ),
            f"3. Key Perks & Policy Grounds: {perks}",
            f"4. Retrieved Knowledge Base Context & Citations: {citation_tags} — {docs_raw.splitlines()[0] if docs_raw else ''}",
            f"5. Eligibility Summary (Question: '{question_raw}'):{alt_summary}",
        ]
        return "\n".join(lines)


class DozerBankingRAGPipeline:
    """End-to-end Dozer + Vector Store + LangChain RAG orchestrator."""

    def __init__(
        self,
        dozer_client: Optional[DozerClient] = None,
        vector_store: Optional[DozerLangChainVectorStore] = None,
        llm: Optional[Any] = None,
    ) -> None:
        self.dozer_client = dozer_client or DozerClient(offline=True)
        self.vector_store = vector_store or DozerLangChainVectorStore(
            embedding_function=DeterministicBankingEmbeddings()
        )
        self.cdc_synchronizer = DozerCDCSynchronizer(
            dozer_client=self.dozer_client,
            endpoint="/documents",
            primary_key_fields=("doc_id",),
        )
        self.cdc_synchronizer.add_listener(self.vector_store.apply_cdc_event)
        # Perform initial snapshot synchronization from Dozer `/documents` into Vector Store
        self.cdc_synchronizer.sync_once()

        self.llm = llm or self._init_default_llm()
        self.prompt = ChatPromptTemplate.from_template(RAG_PROMPT_TEMPLATE)
        self.chain = self.prompt | self.llm | StrOutputParser()

    @staticmethod
    def _init_default_llm() -> Any:
        api_key = os.environ.get("OPENAI_API_KEY", "").strip()
        if api_key:
            try:
                from langchain_openai import ChatOpenAI

                return ChatOpenAI(model="gpt-4o-mini", temperature=0.0)
            except ImportError:
                pass
        return RuleBasedBankingLLM()

    @staticmethod
    def compute_card_annual_net_benefit(
        customer_profile: Dict[str, Any],
        card: Dict[str, Any],
        question: str = "",
    ) -> float:
        """Compute estimated annualized net reward value ($) from live Dozer category spend."""
        travel_spend = float(customer_profile.get("travel_spend", 0.0))
        dining_spend = float(customer_profile.get("dining_spend", 0.0))
        groceries_spend = float(customer_profile.get("groceries_spend", 0.0))
        tech_spend = float(customer_profile.get("tech_spend", 0.0))
        total_spend = float(customer_profile.get("total_spend", 0.0))

        # Annualize sample period spend (x4 quarters)
        ann_travel = travel_spend * 4.0
        ann_dining = dining_spend * 4.0
        ann_groceries = groceries_spend * 4.0
        ann_tech = tech_spend * 4.0
        ann_total = max(total_spend * 4.0, ann_travel + ann_dining + ann_groceries + ann_tech)

        focus = str(card.get("category_focus", ""))
        rate = float(card.get("reward_rate_pct", 1.5)) / 100.0
        fee = float(card.get("annual_fee", 0.0))

        if focus == "Travel":
            other = max(0.0, ann_total - ann_travel - ann_dining)
            gross = (ann_travel * rate) + (ann_dining * 0.03) + (other * 0.01) + 300.0
        elif focus == "Groceries":
            other = max(0.0, ann_total - ann_groceries - ann_dining)
            gross = (ann_groceries * rate) + (ann_dining * 0.03) + (other * 0.015)
        elif focus == "Dining":
            other = max(0.0, ann_total - ann_dining - ann_groceries)
            gross = (ann_dining * rate) + (ann_groceries * 0.02) + (other * 0.01) + 120.0
        elif focus == "Technology":
            other = max(0.0, ann_total - ann_tech - ann_travel)
            gross = (ann_tech * rate) + (ann_travel * 0.03) + (other * 0.015)
        else:
            gross = ann_total * rate

        # Factor in customer preference & query intent alignment
        pref = str(customer_profile.get("preferred_reward_type", "")).lower()
        q_lower = question.lower()
        alignment_bonus = 0.0
        if focus.lower() in pref:
            alignment_bonus += 75.0
        if focus.lower() in q_lower:
            alignment_bonus += 100.0
        if focus == "Travel" and any(k in q_lower for k in ("lounge", "flight", "airline", "hotel")):
            alignment_bonus += 100.0
        if focus == "Groceries" and any(k in q_lower for k in ("grocery", "supermarket", "cashback")):
            alignment_bonus += 100.0
        if focus == "Technology" and any(k in q_lower for k in ("cloud", "aws", "saas", "tech")):
            alignment_bonus += 100.0

        return round(gross + alignment_bonus - fee, 2)

    def rank_eligible_cards(
        self,
        customer_profile: Dict[str, Any],
        eligible_cards: List[Dict[str, Any]],
        question: str = "",
    ) -> List[Dict[str, Any]]:
        enriched: List[Dict[str, Any]] = []
        for card in eligible_cards:
            card_copy = dict(card)
            card_copy["estimated_annual_net_benefit"] = self.compute_card_annual_net_benefit(
                customer_profile, card_copy, question=question
            )
            enriched.append(card_copy)
        enriched.sort(
            key=lambda c: (c["estimated_annual_net_benefit"], c.get("reward_rate_pct", 0.0)),
            reverse=True,
        )
        return enriched

    def apply_cdc_event(self, event: CDCEvent) -> CDCEvent:
        """Apply a streaming CDC event to Dozer and synchronize the Vector Store if needed."""
        return self.cdc_synchronizer.push_cdc_event(event)

    def ask(
        self,
        customer_id: str,
        question: str,
        top_k: int = 3,
    ) -> RAGResponse:
        """Run the full Dozer + Vector Store + LangChain RAG pipeline for a customer query."""
        profile = self.dozer_client.get_customer_profile(customer_id)
        if not profile:
            raise ValueError(f"Customer '{customer_id}' not found in Dozer `/customer_profiles`.")

        raw_eligible_cards = self.dozer_client.get_eligible_cards(customer_id)
        ranked_cards = self.rank_eligible_cards(profile, raw_eligible_cards, question=question)
        recommended_card = ranked_cards[0] if ranked_cards else None

        # Augment semantic query with customer's top spending category and recommended card focus
        augmented_query = question
        if recommended_card:
            augmented_query = (
                f"{question} {recommended_card['card_name']} "
                f"{recommended_card['category_focus']} {profile.get('preferred_reward_type', '')}"
            )

        eligible_card_ids = {c["card_id"] for c in ranked_cards}
        retrieved_docs = self.vector_store.similarity_search(
            query=augmented_query,
            k=top_k,
            filter={
                "max_min_credit_score": int(profile["credit_score"]),
                "card_ids": eligible_card_ids,
            }
            if eligible_card_ids
            else {"max_min_credit_score": int(profile["credit_score"])},
        )

        # Ensure primary document for the top recommended card is included if available
        if recommended_card:
            top_card_id = recommended_card["card_id"]
            if not any(d.metadata.get("card_id") == top_card_id for d in retrieved_docs):
                card_docs = self.vector_store.similarity_search(
                    query=recommended_card["card_name"],
                    k=1,
                    filter={"card_id": top_card_id},
                )
                if card_docs:
                    retrieved_docs = [card_docs[0]] + retrieved_docs[: max(0, top_k - 1)]

        citations = [str(doc.metadata.get("doc_id")) for doc in retrieved_docs if doc.metadata.get("doc_id")]
        formatted_docs = "\n\n".join(
            f"[{doc.metadata.get('doc_id', 'DOC')}] ({doc.metadata.get('category', 'General')} | "
            f"Min Score: {doc.metadata.get('min_credit_score', 0)})\n{doc.page_content}"
            for doc in retrieved_docs
        )

        answer = self.chain.invoke(
            {
                "customer_profile_json": json.dumps(profile, indent=2),
                "eligible_cards_json": json.dumps(ranked_cards, indent=2),
                "retrieved_documents_text": formatted_docs,
                "question": question,
            }
        )

        return RAGResponse(
            customer_id=customer_id,
            question=question,
            answer=answer,
            customer_profile=profile,
            eligible_cards=ranked_cards,
            recommended_card=recommended_card,
            retrieved_documents=retrieved_docs,
            citations=citations,
            vector_index_version=self.vector_store.index_version,
        )
