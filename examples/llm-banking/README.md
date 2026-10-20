# Hyper-Personalized Banking with Dozer, Vector DB & LangChain

This sample demonstrates how **Dozer** provides real-time streaming data aggregation (Customer 360 profile) to power a hyper-personalized financial recommendation chatbot using **Vector Databases** and **LangChain**.

Based on the Dozer hyper-personalization banking use case:
> *A bank implementing an LLM-based chatbot leverages Dozer to create a unified customer profile in real-time, passing it to an LLM as context to hyper-personalize credit card recommendations.*

---

## Architecture Overview

```
 [ Customer Profiles (CSV) ] ──┐
 [ Transactions Stream (CSV) ] ─┼─► [ Dozer Real-time Engine ] ─► [ REST / gRPC API ]
 [ Product Catalog (CSV) ] ────┘           (SQL Pipeline)                     │
                                                                               ▼
 [ Product Vector Database ] ◄─────── Semantic Search ───────── [ LangChain Orchestrator ]
       (Embeddings)                                                            │
                                                                               ▼
                                                            [ Personalized Chatbot Response ]
```

1. **Dozer SQL Streaming Layer**: Ingests multi-source banking data (`customer_profiles`, `transactions`), joins and aggregates spending patterns into real-time customer 360 endpoints (`/customer_spending_summary`).
2. **Vector Database RAG**: Embeds credit card product perks, features, and eligibility criteria into high-dimensional vector representations.
3. **LangChain Chatbot**: Retrieves the customer's top spending category and income from Dozer, executes vector similarity matching, and synthesizes a tailored recommendation explaining why this specific card matches their financial habits.

---

## Directory Structure

```
examples/llm-banking/
├── data/
│   ├── customer_profiles.csv        # Customer metadata, income, credit scores
│   ├── transactions.csv             # Streaming transaction logs & categories
│   └── credit_card_products.csv     # Product catalog & benefits
├── dozer-config.yaml                # Dozer data sources, SQL join & endpoints
├── vector_rag.py                    # Vector store & similarity search
├── recommend.py                     # Dozer + RAG + LLM recommendation pipeline
├── app.py                           # CLI interactive entry point
├── docker-compose.yml               # Containerized execution environment
└── tests/                           # Comprehensive test suite (100% passing)
```

---

## Quickstart

### 1. Run the Recommendation Demo

```bash
# Recommend for Alice Chen (Travel Enthusiast)
python app.py CUST001

# Recommend for Bob Miller (Dining & Nightlife)
python app.py CUST002

# Recommend for Charlie Davis (Everyday Groceries)
python app.py CUST003
```

### 2. Sample Output

```json
{
  "customer_id": "CUST001",
  "customer_name": "Alice Chen",
  "credit_score": 740,
  "top_spending_category": "Travel",
  "total_analyzed_spend": 695.5,
  "recommended_product_id": "CARD_TRAVEL_ELITE",
  "recommended_card_name": "Apex World Explorer Card",
  "annual_fee": 250.0,
  "rationale": "Hello Alice Chen! Based on your recent spending analysis from Dozer (with $695.50 concentrated in Travel), we recommend the **Apex World Explorer Card**. It maximizes your rewards with: 5x points on flights and hotels worldwide with zero foreign transaction fees and priority airport lounge access."
}
```

---

## Running Automated Tests

```bash
python -m unittest discover -s tests -v
```
All 11 unit tests run completely offline and verify data consistency, Dozer config validity, vector similarity search, and recommendation synthesis.
