import os
import requests
from langchain.llms import OpenAI
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma
from langchain.schema import Document

# Configuration
DOZER_REST_URL = os.getenv("DOZER_REST_URL", "http://localhost:8080")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Initialize LLM
llm = OpenAI(api_key=OPENAI_API_KEY, temperature=0.7)

# Initialize embeddings and vector store (in-memory for simplicity)
embeddings = OpenAIEmbeddings(api_key=OPENAI_API_KEY)
vectorstore = Chroma(collection_name="credit_cards", embedding_function=embeddings)

# Fetch credit card products from Dozer and index them
def index_credit_cards():
    response = requests.get(f"{DOZER_REST_URL}/credit_cards")
    response.raise_for_status()
    cards = response.json()
    docs = [
        Document(
            page_content=f"{card['product_name']}: {card['description']}",
            metadata={"id": card["id"], "product_name": card["product_name"]}
        )
        for card in cards
    ]
    if docs:
        vectorstore.add_documents(docs)

# Fetch customer profile from Dozer
def get_customer_profile(customer_id: int) -> dict:
    response = requests.get(f"{DOZER_REST_URL}/customer_profiles/{customer_id}")
    response.raise_for_status()
    return response.json()

# Fetch recent transactions from Dozer
def get_recent_transactions(customer_id: int) -> list:
    response = requests.get(f"{DOZER_REST_URL}/recent_transactions?customer_id={customer_id}")
    response.raise_for_status()
    return response.json()

# Generate personalized recommendation
def recommend_credit_card(customer_id: int) -> str:
    profile = get_customer_profile(customer_id)
    transactions = get_recent_transactions(customer_id)

    # Summarize spending
    spending_summary = "\n".join([f"- {t['category']}: ${t['amount']}" for t in transactions])

    # Retrieve relevant products from vector store
    query = f"Customer with income {profile['income']} and spending on travel and dining"
    relevant_docs = vectorstore.similarity_search(query, k=3)
    product_names = [doc.metadata["product_name"] for doc in relevant_docs]

    # Build prompt
    prompt = PromptTemplate(
        input_variables=["name", "age", "income", "spending", "products"],
        template="""
You are a financial advisor. Recommend a credit card for {name}, age {age}, income {income}.
Recent spending:
{spending}

Consider these products: {products}

Provide a personalized recommendation with reasoning.
"""
    )
    chain = LLMChain(llm=llm, prompt=prompt)
    response = chain.run(
        name=profile["name"],
        age=profile["age"],
        income=profile["income"],
        spending=spending_summary,
        products=", ".join(product_names)
    )
    return response

if __name__ == "__main__":
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY environment variable is required")
    index_credit_cards()
    print("Credit card products indexed.")
    customer_id = int(input("Enter customer ID: "))
    recommendation = recommend_credit_card(customer_id)
    print("\n=== Recommendation ===")
    print(recommendation)
