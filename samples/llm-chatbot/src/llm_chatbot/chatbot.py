import argparse
from pathlib import Path

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda
from langchain_openai import ChatOpenAI

from .config import Settings, load_settings
from .dozer_client import DozerClient
from .profile import build_customer_profile, format_customer_profile
from .vector_store import build_retriever, format_documents, load_credit_card_products


PROMPT_TEMPLATE = """
You are a helpful bank credit-card assistant.
Use the customer profile and retrieved product catalog to answer the question.
Do not invent account balances, transactions, or product terms.
If the information is insufficient, say what additional detail is needed.

Customer profile:
{customer_profile}

Relevant credit-card products:
{product_context}

Question: {question}
"""


def ask_question(settings: Settings, question: str) -> str:
    dozer_client = DozerClient(settings.dozer_api_base)
    customers = dozer_client.get_records("customers")
    transactions = dozer_client.get_records("transactions")
    credit_cards = dozer_client.get_records("credit_cards")

    profile = build_customer_profile(
        settings.customer_id, customers, transactions, credit_cards
    )
    customer_profile = format_customer_profile(profile)

    products_path = Path(__file__).resolve().parents[2] / "data" / "credit_card_products.csv"
    products = load_credit_card_products(products_path)
    retriever = build_retriever(products, settings)
    prompt = ChatPromptTemplate.from_template(PROMPT_TEMPLATE)
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    chain = (
        {
            "customer_profile": RunnableLambda(lambda _: customer_profile),
            "product_context": retriever | format_documents,
            "question": RunnableLambda(lambda _: question),
        }
        | prompt
        | llm
    )
    return chain.invoke(question).content


def main() -> None:
    parser = argparse.ArgumentParser(description="Ask a Dozer-backed credit-card chatbot")
    parser.add_argument("question", help="Question to ask the chatbot")
    parser.add_argument(
        "--customer-id",
        help="Dozer customer ID to profile; overrides DOZER_CUSTOMER_ID",
    )
    args = parser.parse_args()

    settings = load_settings()
    if args.customer_id:
        settings = Settings(
            dozer_api_base=settings.dozer_api_base,
            customer_id=args.customer_id,
            chroma_directory=settings.chroma_directory,
            collection_name=settings.collection_name,
        )

    print(ask_question(settings, args.question))


if __name__ == "__main__":
    main()
