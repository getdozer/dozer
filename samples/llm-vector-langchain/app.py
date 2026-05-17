from __future__ import annotations

import argparse
import hashlib
import os
from dataclasses import dataclass
from typing import Any

from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter

from dozer_client import DozerClient
from recommender import build_customer_context, build_product_context, rank_products


DEFAULT_BASE_URL = "http://localhost:8080"


@dataclass(frozen=True)
class RetrievalBundle:
    documents: list[Document]
    customer_context: str
    ranked_products: list[dict[str, Any]]


class HashEmbedding:
    """Small deterministic embedding used when no OpenAI key is configured."""

    def __init__(self, dimensions: int = 64) -> None:
        self.dimensions = dimensions

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._embed(text) for text in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text)

    def _embed(self, text: str) -> list[float]:
        vector = [0.0] * self.dimensions
        for token in text.lower().split():
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            index = int.from_bytes(digest[:4], byteorder="big") % self.dimensions
            vector[index] += 1.0
        magnitude = sum(value * value for value in vector) ** 0.5 or 1.0
        return [value / magnitude for value in vector]


def load_bundle(client: DozerClient, customer_id: str) -> RetrievalBundle:
    customers = client.fetch_endpoint("customer_profiles")
    transactions = client.fetch_endpoint("transactions")
    products = client.fetch_endpoint("credit_card_products")

    customer = next(
        (row for row in customers if row.get("customer_id") == customer_id),
        None,
    )
    if customer is None:
        raise ValueError(f"Customer {customer_id!r} was not returned by Dozer")

    customer_transactions = [
        row for row in transactions if row.get("customer_id") == customer_id
    ]
    ranked_products = rank_products(customer, customer_transactions, products)

    customer_context = build_customer_context(customer, customer_transactions)
    product_contexts = [build_product_context(product) for product in products]
    documents = [
        Document(
            page_content=customer_context,
            metadata={"kind": "customer", "customer_id": customer_id},
        )
    ]
    documents.extend(
        Document(
            page_content=content,
            metadata={"kind": "product", "product_id": product["product_id"]},
        )
        for content, product in zip(product_contexts, products)
    )

    return RetrievalBundle(
        documents=documents,
        customer_context=customer_context,
        ranked_products=ranked_products,
    )


def build_vector_store(documents: list[Document]) -> Chroma:
    splitter = RecursiveCharacterTextSplitter(chunk_size=700, chunk_overlap=80)
    chunks = splitter.split_documents(documents)
    embeddings = (
        OpenAIEmbeddings()
        if os.getenv("OPENAI_API_KEY")
        else HashEmbedding()
    )
    return Chroma.from_documents(chunks, embedding=embeddings)


def answer(bundle: RetrievalBundle, question: str) -> str:
    vector_store = build_vector_store(bundle.documents)
    retrieved = vector_store.as_retriever(search_kwargs={"k": 4}).invoke(question)
    retrieved_context = "\n\n".join(doc.page_content for doc in retrieved)
    ranked_context = "\n".join(
        f"- {product['name']}: score={product['score']}, reason={product['reason']}"
        for product in bundle.ranked_products[:3]
    )

    if not os.getenv("OPENAI_API_KEY"):
        best = bundle.ranked_products[0]
        return (
            f"Recommended card: {best['name']}.\n"
            f"Reason: {best['reason']}.\n\n"
            f"Retrieved context:\n{retrieved_context}"
        )

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "You recommend credit-card products using only the supplied "
                "Dozer context. Mention the card, why it fits, and one caveat.",
            ),
            (
                "human",
                "Customer context:\n{customer_context}\n\n"
                "Ranked products:\n{ranked_context}\n\n"
                "Retrieved context:\n{retrieved_context}\n\n"
                "Question: {question}",
            ),
        ]
    )
    model = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    message = (prompt | model).invoke(
        {
            "customer_context": bundle.customer_context,
            "ranked_context": ranked_context,
            "retrieved_context": retrieved_context,
            "question": question,
        }
    )
    return str(message.content)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--customer-id", default="cust_001")
    parser.add_argument(
        "--question",
        default="Which credit card should this customer be offered?",
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("DOZER_BASE_URL", DEFAULT_BASE_URL),
    )
    args = parser.parse_args()

    client = DozerClient(args.base_url)
    bundle = load_bundle(client, args.customer_id)
    print(answer(bundle, args.question))


if __name__ == "__main__":
    main()
