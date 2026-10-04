"""Hyper-personalized banking chatbot.

Per turn it:
  1. loads the caller's unified customer profile from Dozer (REST),
  2. retrieves the matching credit-card products from Qdrant (RAG),
  3. prompts the LLM with profile + retrieved products + chat history.

Works with OpenAI when OPENAI_API_KEY is set. Without a key it falls back
to a deterministic mock LLM that renders the same context template, so the
whole sample runs end-to-end with zero secrets (CI-friendly).
"""

from __future__ import annotations

import os
from typing import Any

from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables.history import RunnableWithMessageHistory
from langchain_core.chat_history import InMemoryChatMessageHistory

from dozer_client import DozerClient
from vector_store import ProductVectorStore

SYSTEM_TEMPLATE = """\
You are Avery, the virtual banking assistant. You hyper-personalize every \
answer using the customer's unified profile and only recommend products \
from the retrieved catalog.

# Unified customer profile (served by Dozer)
{profile}

# Recent transactions (served by Dozer)
{transactions}

Products: (retrieved via vector search over the catalog)
{products}

Rules:
- Ground every statement in the profile, the transactions or the catalog.
- Recommend at most two products and explain why they fit this customer.
- If the customer asks about something the context does not cover, say so \
plainly instead of inventing details.
"""


def _format_block(label: str, payload: Any) -> str:
    if not payload:
        return f"{label}: (none)"
    if isinstance(payload, str):
        return f"{label}: {payload}"
    if isinstance(payload, dict):
        lines = [f"  {k}: {v}" for k, v in payload.items()]
        return f"{label}:\n" + "\n".join(lines)
    lines = []
    for item in payload:
        if isinstance(item, dict):
            lines.append("  - " + ", ".join(f"{k}: {v}" for k, v in item.items()))
        else:
            lines.append(f"  - {item}")
    return f"{label}:\n" + "\n".join(lines)


def build_context(dozer: DozerClient, products: ProductVectorStore, customer_id: int, question: str):
    """Pull the unified profile from Dozer and RAG-match products."""
    profile = dozer.get_profile(customer_id)
    if profile is None:
        customer = dozer.get_customer(customer_id)
        if customer is None:
            return None, None, None
        profile = customer
    transactions = dozer.get_transactions(customer_id)
    matches = products.search(question, k=2)
    return profile, transactions, matches


def get_llm():
    """OpenAI chat model when a key is present, mock otherwise."""
    if os.getenv("OPENAI_API_KEY"):
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"), temperature=0.3)
    return MockLLM()


class MockLLM:
    """Deterministic stand-in for CI / keyless demos.

    Renders the same prompt the real model would see, so the plumbing
    (Dozer profile -> vector retrieval -> prompt) is fully exercised
    without network access or secrets. The reply is assembled from the
    retrieved products and the profile segment.
    """

    def bind(self, **_kwargs):  # parity with chat models
        return self

    def with_config(self, **_kwargs):
        return self

    def invoke(self, messages, config=None, **_kwargs):
        content = "\n".join(
            m.content if isinstance(m.content, str) else str(m.content) for m in messages
        )
        lines = content.splitlines()

        name, segment = "customer", "unknown"
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("first_name:") and name == "customer":
                name = stripped.split(":", 1)[1].strip() or "customer"
            if stripped.startswith("segment:"):
                segment = stripped.split(":", 1)[1].strip() or "unknown"

        product_names: list[str] = []
        for idx, line in enumerate(lines):
            if line.strip().startswith("Products:") or line.strip() == "Products":
                for product_line in lines[idx + 1 :]:
                    if not product_line.strip().startswith("- "):
                        break
                    product_names.append(product_line.strip()[2:].split(":")[0].strip())
                    if len(product_names) == 2:
                        break
                break

        if product_names:
            rec_block = "\n".join(f"- {p}" for p in product_names)
        else:
            rec_block = "- (no matching products indexed yet)"

        text = (
            f"Hi {name}! As a {segment}-segment customer, based on your unified "
            f"profile and recent activity, these picks from our catalog fit you best:\n"
            f"{rec_block}\n\n"
            "(Mock LLM response - set OPENAI_API_KEY for real answers.)"
        )
        return MockMessage(text)


class MockMessage:
    def __init__(self, content: str) -> None:
        self.content = content


_PROMPT = ChatPromptTemplate.from_messages(
    [
        ("system", SYSTEM_TEMPLATE),
        MessagesPlaceholder("history"),
        ("human", "{question}"),
    ]
)


def answer(
    dozer: DozerClient,
    products: ProductVectorStore,
    customer_id: int,
    question: str,
    history: InMemoryChatMessageHistory | None = None,
) -> str:
    profile, transactions, matches = build_context(dozer, products, customer_id, question)
    if profile is None:
        return f"Customer {customer_id} not found in Dozer. Try customer_id 1001-1005."

    llm = get_llm()
    prompt_value = _PROMPT.invoke(
        {
            "profile": _format_block("Profile", profile),
            "transactions": _format_block("Recent transactions", transactions),
            "products": (
                "\n".join(f"- {m['name']}: {m['text']}" for m in matches) or "(none)"
            ),
            "history": list(history.messages) if history else [],
            "question": question,
        }
    )
    response = llm.invoke(prompt_value.to_messages())
    return response.content


_SESSIONS: dict[str, InMemoryChatMessageHistory] = {}


def session_history(session_id: str) -> InMemoryChatMessageHistory:
    if session_id not in _SESSIONS:
        _SESSIONS[session_id] = InMemoryChatMessageHistory()
    return _SESSIONS[session_id]
