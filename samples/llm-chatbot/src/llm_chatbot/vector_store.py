from pathlib import Path

from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings

from .catalog import CreditCardProduct, load_credit_card_products, product_document


def build_retriever(products: list[CreditCardProduct], settings):
    documents = [
        Document(page_content=page_content, metadata=metadata)
        for page_content, metadata in (product_document(product) for product in products)
    ]
    vector_store = Chroma.from_documents(
        documents=documents,
        embedding=OpenAIEmbeddings(),
        collection_name=settings.collection_name,
        persist_directory=settings.chroma_directory,
        ids=[product.product_id for product in products],
    )
    return vector_store.as_retriever(search_kwargs={"k": 3})


def format_documents(documents: list[Document]) -> str:
    return "\n".join(document.page_content for document in documents)
