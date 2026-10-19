# Dozer + LLM + Vector Database + LangChain Banking Chatbot Sample

This sample implements the hyper-personalized banking credit-card advisor architecture described in [Dozer's LLM Chatbot article](https://getdozer.io/blog/llm-chatbot).

## Architecture

1. **Multi-Dataset Ingestion & Real-Time Materialization (`dozer-config.yaml`)**:
   - Ingests `customer_profiles`, `transactions`, and `credit_card_products`.
   - Joins customer demographics with real-time transaction counts and spend totals into the `/unified_customer_profile` REST endpoint and exposes `/credit_card_products`.
2. **Vector Search & Hard Eligibility Filtering (`CreditCardVectorStore` in `app.py`)**:
   - Filters credit card products by the customer's credit score and annual income from Dozer's unified profile before ranking product descriptions by vector similarity.
3. **LangChain + LLM Context Injection (`PersonalizedBankingChatbot` in `app.py`)**:
   - Combines the unified customer profile from Dozer with the top-matching credit card products from the vector store into a grounded LangChain prompt.

## Running the Sample

```bash
# 1. Start Dozer (optional — app.py also includes an automatic local CSV fallback for offline testing)
dozer -c dozer-config.yaml

# 2. Run the chatbot sample
python app.py

# 3. Run the unit tests
python -m unittest discover -s tests
```
