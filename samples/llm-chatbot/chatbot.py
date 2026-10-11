"""
Hyper-Personalized Credit Card Advisor Chatbot.

Orchestrates Dozer Customer 360 view + Chroma Vector Database + LangChain LLM.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

from dotenv import load_dotenv

from dozer_retriever import DozerClient
from langchain_community.vectorstores import Chroma
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate

load_dotenv()


def get_vector_store():
    """Initializes or loads persistent Chroma vector store."""
    persist_dir = os.getenv("CHROMA_PERSIST_DIRECTORY", "./chroma_db")
    try:
        from langchain_openai import OpenAIEmbeddings

        if os.getenv("OPENAI_API_KEY"):
            embeddings = OpenAIEmbeddings()
        else:
            from langchain_community.embeddings import FakeEmbeddings

            embeddings = FakeEmbeddings(size=1536)
    except ImportError:
        from langchain_community.embeddings import FakeEmbeddings

        embeddings = FakeEmbeddings(size=1536)

    # If Chroma directory doesn't exist yet, populate it on the fly
    if not os.path.exists(persist_dir) or not os.listdir(persist_dir):
        from ingest_knowledge_base import ingest

        print("Vector database not found. Running ingestion...")
        ingest()

    return Chroma(persist_directory=persist_dir, embedding_function=embeddings)


def get_llm():
    """Returns LLM instance or fallback mock for testing/offline mode."""
    api_key = os.getenv("OPENAI_API_KEY")
    if api_key and not api_key.startswith("your_"):
        from langchain_openai import ChatOpenAI

        model_name = os.getenv("LLM_MODEL", "gpt-4o-mini")
        return ChatOpenAI(model=model_name, temperature=0.2)

    # Offline / Mock LLM response generator for local testing
    from langchain_community.llms.fake import FakeListLLM

    return FakeListLLM(
        responses=[
            "Based on your profile from Dozer, your travel spending exceeds $4,000 in the last 90 days "
            "and your credit score is 785. The Apex AeroElite Global Travel Card is the ideal fit for you. "
            "It gives you 5x points on flights and hotels, offsetting its $495 annual fee."
        ]
    )


SYSTEM_PROMPT = """You are Apex Bank's Senior Financial Product Advisor.
Your objective is to provide hyper-personalized credit card recommendations and financial guidance.

You are equipped with two authoritative context sources:
1. Real-Time Customer Profile from Dozer: Demographic attributes, credit score, and 90-day spending patterns across categories.
2. Bank Product Knowledge Base: Credit card options, fees, rewards structures, and eligibility criteria.

Guidelines:
- Match the customer's actual spending category (travel, groceries, dining, business) with the card rewards.
- Check credit score eligibility (do not recommend cards if customer score is below the minimum).
- Explicitly explain WHY this recommendation is tailored to them citing their specific spending habits.
- Maintain a professional, helpful, and transparent tone.
"""

USER_PROMPT_TEMPLATE = """{customer_context}

--- RETRIEVED PRODUCT SPECIFICATIONS (Vector Database) ---
{product_context}
-----------------------------------------------------------

Customer Question: {query}

Please provide your hyper-personalized advice and product recommendation:"""


class FinancialAdvisorChatbot:
    """Chatbot combining Dozer real-time profile with Vector DB semantic search."""

    def __init__(self, dozer_client: Optional[DozerClient] = None):
        self.dozer_client = dozer_client or DozerClient()
        self.vector_store = get_vector_store()
        self.retriever = self.vector_store.as_retriever(search_kwargs={"k": 2})
        self.llm = get_llm()

    def generate_response(self, customer_id: str, query: str) -> str:
        """Runs the personalization pipeline for a specific customer query."""
        # 1. Fetch real-time unified customer profile from Dozer
        profile = self.dozer_client.get_profile(customer_id)
        if not profile:
            customer_context = f"Customer ID '{customer_id}' was not found in Dozer data."
        else:
            customer_context = profile.format_as_context()

        # 2. Retrieve relevant credit card products from Chroma
        docs = self.retriever.get_relevant_documents(query)
        product_context = "\n\n".join([doc.page_content for doc in docs])

        # 3. Construct prompt
        formatted_user_prompt = USER_PROMPT_TEMPLATE.format(
            customer_context=customer_context,
            product_context=product_context,
            query=query,
        )

        messages = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=formatted_user_prompt),
        ]

        # 4. Invoke LLM
        response = self.llm.invoke(messages)
        if hasattr(response, "content"):
            return str(response.content)
        return str(response)


def main():
    parser = argparse.ArgumentParser(description="Dozer + LangChain Hyper-Personalized Banking Assistant")
    parser.add_argument("--customer-id", type=str, default="CUST_001", help="Customer ID to personalize for")
    parser.add_argument(
        "--query",
        type=str,
        default="Which credit card would you recommend for me based on my spending history?",
        help="Customer query",
    )
    args = parser.parse_args()

    print(f"\n=======================================================")
    print(f" Apex Banking Assistant [Customer: {args.customer_id}]")
    print(f"=======================================================\n")

    bot = FinancialAdvisorChatbot()
    print(f"User Query: {args.query}\n")
    print("Generating hyper-personalized response...\n")

    response = bot.generate_response(args.customer_id, args.query)
    print("Advisor Response:\n")
    print(response)
    print("\n-------------------------------------------------------")


if __name__ == "__main__":
    main()
