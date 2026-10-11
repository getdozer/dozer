"""
Ingestion script for Bank Knowledge Base into Chroma Vector Database.

Extracts credit card terms, rewards, perks, and eligibility requirements
from credit_cards.json and embeds them into a persistent Chroma store.
"""

from __future__ import annotations

import json
import os
from typing import List

from dotenv import load_dotenv
from langchain_community.vectorstores import Chroma
from langchain_core.documents import Document

load_dotenv()

try:
    from langchain_openai import OpenAIEmbeddings

    def get_embeddings():
        if os.getenv("OPENAI_API_KEY"):
            return OpenAIEmbeddings()
        from langchain_community.embeddings import FakeEmbeddings

        return FakeEmbeddings(size=1536)
except ImportError:
    from langchain_community.embeddings import FakeEmbeddings

    def get_embeddings():
        return FakeEmbeddings(size=1536)


def build_knowledge_documents(kb_path: str) -> List[Document]:
    """Load JSON credit card records and convert to LangChain Documents."""
    with open(kb_path, "r", encoding="utf-8") as f:
        cards = json.load(f)

    docs = []
    for card in cards:
        content = (
            f"Credit Card Name: {card['name']}\n"
            f"Card ID: {card['card_id']}\n"
            f"Target Audience: {card['target_audience']}\n"
            f"Minimum Credit Score Required: {card['minimum_credit_score']}\n"
            f"Annual Fee: ${card['annual_fee']}\n"
            f"Rewards Structure: {card['rewards_structure']}\n"
            f"Card Perks & Benefits: {card['perks']}\n"
            f"APR: {card['apr']}\n"
            f"Introductory Offer: {card['introductory_offer']}\n"
        )
        metadata = {
            "card_id": card["card_id"],
            "name": card["name"],
            "min_credit_score": card["minimum_credit_score"],
            "annual_fee": card["annual_fee"],
        }
        docs.append(Document(page_content=content, metadata=metadata))
    return docs


def ingest():
    data_dir = os.path.join(os.path.dirname(__file__), "data")
    kb_path = os.path.join(data_dir, "credit_cards.json")
    persist_dir = os.getenv("CHROMA_PERSIST_DIRECTORY", "./chroma_db")

    print(f"Loading credit card knowledge base from: {kb_path}")
    docs = build_knowledge_documents(kb_path)
    print(f"Created {len(docs)} documents.")

    print(f"Indexing into Chroma at: {persist_dir}...")
    embeddings = get_embeddings()
    vectorstore = Chroma.from_documents(
        documents=docs,
        embedding=embeddings,
        persist_directory=persist_dir,
    )
    vectorstore.persist()
    print("Ingestion completed successfully.")


if __name__ == "__main__":
    ingest()
