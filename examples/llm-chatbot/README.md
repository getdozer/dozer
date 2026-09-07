# Dozer + LangChain credit-card sample

Tiny bank chatbot from the [Dozer LLM writeup](https://getdozer.io/blog/llm-chatbot).

Dozer reads three CSVs (customers, transactions, cards) and SQL-joins a
`customer_360`. Current Dozer is a mover, not an API server, so `api.py`
is the app-side API LangChain hits. Cards go into Chroma; the customer
profile is the query.

## run it

```bash
./run.sh
```

That venv-installs, runs tests, starts the api, and asks for three customers.

Or by hand:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python api.py &
python chat.py --customer-id c1
python chat.py --customer-id c2 --question "anything with no annual fee?"
python chat.py --repl --customer-id c3
```

Set `OPENAI_API_KEY` if you want the model to write the answer. Without
it you still get the retrieved cards (bag-of-words embeddings, no download).

```bash
docker compose up api
# other terminal
python chat.py --customer-id c4 --base-url http://127.0.0.1:8000
```

## dozer

Needs a `dozer` binary (build this repo or grab a release):

```bash
dozer run -c dozer.yaml
```

`dozer.yaml` sources the same CSVs and materializes `customer_360` /
`card_catalog` into dummy sinks. Dummy just counts rows; the python api
is what the chatbot uses.

Customers: `c1` Maya (travel), `c2` Omar (groceries), `c3` Priya (family),
`c4` Leo (nomad), `c5` Ana (student).
