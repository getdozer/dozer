# Dozer + LLM + Vector Database + LangChain Sample

Hyper-personalized **Banking & Credit Card AI Advisor** sample demonstrating how **Dozer** combines real-time structured customer/transaction streams with a **Vector Knowledge Base** and **LangChain** (`#1690`).

## Architecture

1. **Real-Time Structured Data Plane (`dozer-config.yaml` + `src/dozer_client.py`)**:
   - Ingests `customers`, `credit_cards`, and real-time `transactions` into Dozer.
   - Materializes real-time category spend aggregations (`customer_spending_summary`) via Dozer SQL (`GROUP BY customer_id, category`) and serves low-latency REST (`:8080`) and gRPC (`:50051`) endpoints.
2. **Unstructured Semantic Retrieval (`knowledge_base/credit_card_terms.md` + `src/vector_store.py`)**:
   - Indexes unstructured credit card terms, travel insurance coverage, lounge access rules, and underwriting restrictions for semantic similarity search.
3. **Grounded Underwriting & LangChain Synthesis (`src/advisor.py`)**:
   - Queries Dozer for the customer's live credit score, utilization, income, and category spend breakdown.
   - Evaluates card eligibility and calculates exact annualized rewards & net value (`net_ongoing_annual_value_usd`, `net_first_year_value_usd`).
   - Synthesizes a grounded recommendation via LangChain (`ChatOpenAI` when `OPENAI_API_KEY` is set, with deterministic offline synthesis for local/CI verification).

## Running the Sample & Tests

```bash
# 1. (Optional) Start Dozer real-time data endpoints
dozer -c examples/dozer-llm-langchain-sample/dozer-config.yaml

# 2. Run the unit & integration test suite
python3 -m unittest discover -s examples/dozer-llm-langchain-sample/tests -v
```
