# Dozer + LLM + Vector Database + LangChain Sample (Credit Card Personalization)

This sample implements the architecture described in the [Dozer LLM Chatbot article](https://getdozer.io/blog/llm-chatbot) for hyper-personalizing banking credit card recommendations (#1690):

1. **Real-Time Multi-Dataset Ingestion & SQL Materialization (`dozer-config.yaml`)**:
   - Ingests `customers.csv`, `transactions.csv`, and `credit_cards.csv`.
   - Joins customer demographics and streaming transactions into a unified `customer_profiles` view (`total_spend`, `tx_count`, `credit_score`, `annual_income`, `preferred_category`, `kyc_verified`) exposed at `/customer_profiles` and `/credit_cards`.
2. **Hard Eligibility Gate (`dozer_client.py`)**:
   - Queries Dozer's REST API and filters out credit card products whose `min_credit_score`, `min_annual_income`, or `kyc_verified` constraints are not met by the customer's live unified profile.
3. **Vector Similarity Retrieval & LangChain Grounding (`vector_rag_chatbot.py`)**:
   - Indexes eligible credit card products into a semantic vector index, retrieves top matches aligned with both the customer's question and live transaction profile, and invokes LangChain (`ChatOpenAI` when `OPENAI_API_KEY` is set, with a deterministic offline advisor mode for local testing).

## Running the Sample

```bash
# 1. Start Dozer to materialize the unified customer_profiles and credit_cards endpoints:
dozer run -c dozer-config.yaml

# 2. Run the automated verification suite:
python3 -m unittest test_chatbot.py
```
