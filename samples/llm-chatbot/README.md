# Dozer + LLM + Vector DB + Langchain: hyper-personalized banking chatbot

A complete working sample accompanying the article
[*Hyper-personalization of LLM Chatbot using Dozer*](https://getdozer.io/blog/llm-chatbot)
(issue #1690).

A hypothetical bank wants an LLM chatbot that knows each customer. Dozer
sources the bank's operational datasets — **customer profiles**,
**transactions** and a **credit-card product catalog** — from Postgres,
joins them in real time into a **unified customer profile**, and serves it
over a low-latency REST/gRPC API. A Langchain chatbot then:

1. pulls the caller's unified profile from **Dozer** on every turn,
2. retrieves the most relevant credit-card products from a **Qdrant**
   vector store (RAG over the product catalog),
3. feeds profile + transactions + retrieved products to the **LLM** as
   context (OpenAI, or a built-in mock when no API key is set).

```
                    ┌────────────────────────────────────────────┐
                    │                  Dozer                     │
 Postgres ─────────►│  customers + transactions + products       │
 (raw datasets)     │  ── SQL join & aggregate ──►               │────► REST :8080
                    │  customer_profile (unified, real-time)     │────► gRPC :50051
                    └────────────────────────────────────────────┘
                                                        │
                              ┌─────────────────────────┴──────────┐
                              │           Langchain chatbot        │
   Qdrant ◄── index products ─│  profile + RAG products → LLM      │──► "Avery" assistant
   (vector store) ◄─ search ─ │  (OpenAI or keyless mock LLM)      │
                              └────────────────────────────────────┘
```

## Quick start

```bash
export OPENAI_API_KEY=sk-...   # optional - omit to run with the mock LLM
docker compose up --build
```

Then chat:

```bash
curl -s localhost:8000/chat \
  -H 'Content-Type: application/json' \
  -d '{"customer_id": 1001, "message": "I travel often - which card fits me best?"}'
```

Interactive terminal mode:

```bash
docker compose exec chatbot python main.py --cli --customer-id 1001
```

Inspect the data APIs Dozer generates:

```bash
curl -s localhost:8080/                     # all endpoint paths
curl -s localhost:8080/health               # {"status":"SERVING"}
curl -s localhost:8080/transactions/1       # single transaction by primary key
curl -s localhost:8080/transactions         # list
curl -s localhost:8080/profile/query \
  -H 'Content-Type: application/json' \
  -d '{"$filter": {"customer_id": 1001}, "$limit": 1}'   # unified profile
curl -s localhost:8080/transactions/query \
  -H 'Content-Type: application/json' \
  -d '{"$filter": {"customer_id": 1003}, "$order_by": {"amount": "desc"}, "$limit": 5}'
```

Without `OPENAI_API_KEY` everything still runs end-to-end: the chatbot
answers with a deterministic mock LLM and hashing embeddings, so the sample
is CI-friendly and needs zero secrets.

## What each piece does

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Postgres + Qdrant + Dozer + chatbot, one command |
| `dozer-config.yaml` | Dozer pipeline: Postgres sources, SQL join, API endpoints |
| `seed/init.sql` | Customer profiles, transactions, credit-card catalog |
| `pg_hba.conf` | Replication-friendly auth for Dozer's Postgres CDC |
| `chatbot/dozer_client.py` | REST client for Dozer's generated endpoints |
| `chatbot/vector_store.py` | Qdrant index + Langchain embeddings (OpenAI or hashing) |
| `chatbot/chatbot.py` | Prompt assembly: unified profile + RAG + history → LLM |
| `chatbot/main.py` | FastAPI app and CLI entrypoint |

## The Dozer pipeline

`dozer-config.yaml` ingests three tables and builds the unified profile with
plain SQL — Dozer materializes and keeps it up to date as transactions land:

```sql
select c.customer_id, c.first_name, ..., count(t.txn_id) as transaction_count,
       sum(t.amount) as total_spend, max(t.txn_date) as last_transaction_date
into customer_profile
from customers c
inner join transactions t on t.customer_id = c.customer_id
group by c.customer_id, ...;
```

Each `endpoints` entry becomes a REST + gRPC API on :8080/:50051 with
`/{id}` fetch, list, `/query` and `/count` routes generated automatically.

## Running without Docker

Requirements: a local `dozer` binary, Python 3.10+, a Postgres with the seed
loaded (`wal_level=logical`), and Qdrant.

```bash
# 1. Load seed data and start dependencies
psql postgresql://postgres:postgres@localhost:5436/bank -f seed/init.sql

# 2. Point Dozer at localhost (edit host in dozer-config.yaml) and run
dozer run

# 3. Chatbot
cd chatbot
pip install -r requirements.txt
export DOZER_REST_URL=http://localhost:8080
export QDRANT_URL=http://localhost:6333
uvicorn main:app --port 8000      # or: python main.py --cli --customer-id 1002
```

## Verify

With the stack up (compose or native), run the end-to-end smoke tests:

```bash
pip install pytest
DOZER_REST_URL=http://localhost:8080 QDRANT_URL=http://localhost:6333 \
  PYTHONPATH=chatbot pytest tests/test_sample.py -v
```

The tests exercise the full keyless chain: Dozer health, the unified profile
(join + aggregation), the query DSL (`$filter` + `$order_by`), and the
chatbot (Qdrant indexing → retrieval → personalized mock-LLM reply).

## Configuration

| Env var | Default | Description |
| --- | --- | --- |
| `OPENAI_API_KEY` | _(empty)_ | Enables real LLM + OpenAI embeddings; mock fallback when unset |
| `OPENAI_MODEL` | `gpt-4o-mini` | Chat model |
| `OPENAI_BASE_URL` | _(empty)_ | Point at any OpenAI-compatible server (vLLM, Ollama, ...) |
| `DOZER_REST_URL` | `http://localhost:8080` | Dozer REST API |
| `QDRANT_URL` | `http://localhost:6333` | Qdrant vector store |
