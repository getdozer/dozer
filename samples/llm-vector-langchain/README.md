# Dozer + LangChain credit-card recommendation sample

This sample is a working implementation of the use case described in the
[Dozer + LLM chatbot article](https://getdozer.io/blog/llm-chatbot): a bank
builds an LLM-powered assistant that uses Dozer to assemble a unified customer
picture (profile, transactions, products) and passes it to an LLM as context to
hyper-personalize credit-card recommendations.

It shows three pieces working together:

- **Dozer** is configured to source from three datasets at once: customer
  profiles, transactions, and credit-card products.
- The datasets are exposed through the Dozer API.
- A **LangChain** application reads the Dozer API, indexes the data in a
  **vector database** (Chroma), and answers a personalized product
  recommendation question with an LLM.

## Repository layout

```
llm-vector-langchain/
├── dozer-config.yaml              # Dozer app configuration
├── data/
│   ├── customer_profiles/         # customer_profiles.csv
│   ├── transactions/              # transactions.csv
│   └── credit_card_products/      # credit_card_products.csv
├── app.py                         # LangChain entry point
├── dozer_client.py                # reads datasets from the Dozer API
├── recommender.py                 # context building + offline ranking helpers
├── requirements.txt
└── tests/
    ├── test_recommender.py        # offline tests for recommender.py
    └── test_dozer_config.py       # checks dozer-config.yaml + CSV data
```

## How it works

`dozer-config.yaml` declares a single `LocalStorage` connection that reads three
CSV datasets, three sources that map those tables into Dozer, and a `Dummy`
sink for each one so every dataset is materialized and available through the
Dozer API:

| Endpoint                   | Contents                                            |
| -------------------------- | --------------------------------------------------- |
| `/customer_profiles`       | customer segment, income band, and stated goals     |
| `/transactions`            | per-customer transaction history                    |
| `/credit_card_products`    | available card products and eligibility metadata    |

`app.py` fetches the three datasets from the Dozer API, builds one document per
customer profile and one per product, indexes them in Chroma, and answers a
question such as *"Which credit card should this customer be offered and why?"*.

If `OPENAI_API_KEY` is set, embeddings and the final answer use OpenAI through
LangChain. Without a key the sample uses a deterministic local embedding and a
local answer, so it stays runnable offline and in CI.

If the Dozer API is not reachable, the application falls back to reading the
same CSV files under `data/`, so the recommendation flow can be exercised even
without a running Dozer instance.

## Run the sample

Prerequisites: a Rust toolchain for Dozer and Python 3.10+ for the application.

Start Dozer from this directory:

```bash
dozer run
```

In another terminal, install dependencies and run the app:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python app.py --customer-id cust_001
```

By default the app reads the Dozer API at `http://localhost:8080`. Point it at a
different host with `DOZER_BASE_URL`:

```bash
DOZER_BASE_URL=http://localhost:8080 python app.py --customer-id cust_003
```

Ask for a different customer or a different question:

```bash
python app.py --customer-id cust_004 --question "Which card fits a frequent traveler?"
```

## Run the tests

The tests do not need Dozer, a vector database, or an LLM:

```bash
python -m unittest discover -s tests
```