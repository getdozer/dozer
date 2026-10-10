# Dozer + LangChain + Chroma credit-card chatbot

This sample joins a customer's Dozer-sourced profile with a vectorized credit-card product catalog and passes both contexts to a LangChain OpenAI chat model.

## Compatibility

The sample uses the `api`, `connections`, `sources`, and `endpoints` configuration expected by Dozer releases that include the data-serving API. Current repository HEAD has removed that API layer, so the pipeline portion of `dozer-config.yaml` is covered by a deserialization test, while the `endpoints` section is an intentionally isolated release contract for an external Dozer binary. The sample does not change or resurrect Dozer's removed API implementation.

## Data flow

1. Create and populate the PostgreSQL database using `data/schema.sql` and `data/seed.sql`.
2. Start a Dozer release that supports endpoints with `dozer-config.yaml`; it exposes `/customers`, `/transactions`, and `/credit_cards`.
3. The Python client requests all three endpoints, filters records for `DOZER_CUSTOMER_ID`, and builds a unified profile containing cards, transactions, and monthly spend.
4. `data/credit_card_products.csv` is embedded into a local Chroma collection, and LangChain retrieves the three most relevant products for each question.
5. The customer profile, retrieved products, and question are passed to `ChatOpenAI`.

## Running

```bash
cd samples/llm-chatbot
python -m venv .venv
. .venv/bin/activate
pip install -e .

createdb --host localhost --username postgres dozer_llm_chatbot
psql --host localhost --username postgres --dbname dozer_llm_chatbot --file data/schema.sql
psql --host localhost --username postgres --dbname dozer_llm_chatbot --file data/seed.sql

export OPENAI_API_KEY="your OpenAI API key"
export DOZER_API_BASE="http://localhost:8080"
export DOZER_CUSTOMER_ID="customer-001"

llm-chatbot "Which card should I use for my next trip?"
```

Change the PostgreSQL credentials in `dozer-config.yaml` to match your local database. Keep `OPENAI_API_KEY` in your shell environment only.

## Local checks

```bash
cd samples/llm-chatbot
PYTHONPATH=src python3 -m unittest discover -t . -s tests -p "test_*.py" -v
```

The repository test `dozer-types/src/tests/llm_chatbot_sample_config.rs` parses the sample's connection/source/API structure with the current `Config` type after excluding the release-specific `endpoints` section.
