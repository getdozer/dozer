# Dozer + LangChain customer profile sample

This sample turns Dozer REST endpoints into retrieval context for a credit-card
recommendation assistant. It follows the use case from the Dozer LLM article:
combine profile, transaction, and product data, index it in a vector database,
then answer with personalized product recommendations.

## What is included

- Three small CSV datasets under `data/`.
- `dozer-config.yaml`, which exposes the CSV files through Dozer LocalStorage
  endpoints.
- A LangChain application that reads the Dozer REST API, writes documents to
  Chroma, and answers a recommendation question.
- A small offline test for the recommendation/context-building logic.

## Run the sample

From this directory:

```bash
dozer --config-path dozer-config.yaml
```

In another terminal:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python app.py --customer-id cust_001
```

The application defaults to `http://localhost:8080`. Set `DOZER_BASE_URL` if
Dozer is running elsewhere.

```bash
DOZER_BASE_URL=http://localhost:8080 python app.py --customer-id cust_003
```

If `OPENAI_API_KEY` is set, the sample uses OpenAI through LangChain. Without an
API key it uses a deterministic local response, which keeps the sample runnable
for local development and CI.

## Dozer endpoints

The config exposes these endpoints:

- `/customer_profiles`: customer segment, income band, and declared goals.
- `/transactions`: transaction history grouped by customer.
- `/credit_card_products`: available card products and eligibility metadata.

The Python app queries each endpoint, builds one customer profile document plus
one product document per card, and stores them in Chroma. The retriever is then
used to choose the best product based on both customer fit and recent spend.

## Offline test

The core recommendation helpers do not require network access or LangChain, so
they can be checked with:

```bash
python -m unittest discover -s tests
```
