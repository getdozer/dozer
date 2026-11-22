# Dozer + LLM + Vector Database + LangChain Sample (`getdozer/dozer#1690`)

This sample implements the architecture described in the Dozer engineering article [**Hyper-personalization with LLMs, Vector Databases, and Dozer**](https://getdozer.io/blog/llm-chatbot): a real-time banking and credit-card advisory assistant that fuses **streaming structured customer data from Dozer** with **incremental CDC-synchronized vector search** and **LangChain**.

---

## 1. Architecture Overview

```text
+-----------------------------------------------------------------------------------+
|                        PostgreSQL 15 (wal_level = logical)                        |
|   Tables (REPLICA IDENTITY FULL):                                                 |
|   - customers              (credit_score, annual_income, account_tier, ...)       |
|   - transactions           (merchant_name, category, amount, txn_timestamp)       |
|   - credit_card_products   (reward_rate_pct, annual_fee, min_credit_score, ...)   |
|   - documents              (policy disclosures, perks guides, fee waiver terms)   |
+-----------------------------------------+-----------------------------------------+
                                          |
                          PostgreSQL Logical Replication (CDC)
                                          v
+-----------------------------------------------------------------------------------+
|                             Dozer Real-Time Data Engine                           |
|   Streaming SQL Transformations (`dozer-config.yaml`):                            |
|   1. `customer_spending_profiles`:                                                |
|      LEFT JOIN customers + transactions -> real-time category spend aggregations  |
|      (travel_spend, dining_spend, groceries_spend, tech_spend, total_spend)       |
|   2. `eligible_card_recommendations`:                                             |
|      INNER JOIN customers + credit_card_products                                  |
|      ON credit_score >= min_credit_score AND annual_income >= min_annual_income   |
|                                                                                   |
|   Low-Latency Endpoints: REST (:8080) & gRPC (:50051)                             |
|   - /customer_profiles   - /eligible_cards   - /documents   - /transactions       |
+-------------------+-------------------------------------------+-------------------+
                    |                                           |
   Real-Time Structured Context                     Incremental CDC Sync (`INSERT`,
   (`/customer_profiles`, `/eligible_cards`)        `UPDATE`, `DELETE` on `/documents`)
                    |                                           |
                    |                                           v
                    |                         +-------------------------------------+
                    |                         |   DozerLangChainVectorStore         |
                    |                         |   (Qdrant / In-Memory Vector Index) |
                    |                         |   - Metadata Filter:                |
                    |                         |     `min_credit_score <= customer`  |
                    |                         +-----------------+-------------------+
                    |                                           |
                    +---------------------+---------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                    LangChain RAG Pipeline (`rag_pipeline.py`)                     |
|   - Ranks Dozer SQL-eligible cards by live annualized net reward value ($)        |
|   - Retrieves top-k policy documents with `[DOC-ID]` citations                    |
|   - LCEL Chain: `ChatPromptTemplate | ChatOpenAI / RuleBasedBankingLLM | Parser`  |
+-----------------------------------------------------------------------------------+
```

---

## 2. How Each Component Maps to the Dozer Blog Post

| Requirement from Issue `#1690` & Blog Post | Implementation in This Sample |
| :--- | :--- |
| **Canonical Dozer Configuration** | [`dozer-config.yaml`](./dozer-config.yaml) uses `config: !Postgres`, defines 4 source tables, top-level streaming `sql: SELECT ... INTO <table_name>` views, and 6 indexed REST/gRPC endpoints with `index.primary_key`. |
| **Real-Time Structured Customer Context** | Dozer continuously joins `customers` with `transactions` into `/customer_profiles` and pre-filters eligible credit cards in `/eligible_cards` based on `credit_score` and `annual_income`. |
| **Incremental CDC to Vector Database** | [`dozer_client.py`](./dozer_client.py) (`DozerCDCSynchronizer`) and [`vector_store.py`](./vector_store.py) (`DozerLangChainVectorStore.apply_cdc_event`) process `INSERT`, `UPDATE`, and `DELETE` CDC events in real time without re-indexing the entire corpus. |
| **LangChain Orchestration** | [`rag_pipeline.py`](./rag_pipeline.py) (`DozerBankingRAGPipeline`) combines `langchain_core.vectorstores.VectorStore`, `langchain_core.embeddings.Embeddings`, and a LangChain Expression Language (LCEL) chain (`prompt \| llm \| StrOutputParser()`). |
| **Zero-Key Offline & CI Execution** | Works out-of-the-box with `OPENAI_API_KEY` (for `ChatOpenAI`) **or** 100% offline using `DeterministicBankingEmbeddings` + `RuleBasedBankingLLM` + `InMemoryDozerBackend`. |

---

## 3. Quickstart

### Option A: Run Locally (Zero External Services Needed)

1. Install Python dependencies:
   ```bash
   pip install -r examples/llm-vector-langchain/requirements.txt
   ```

2. Run the end-to-end demo (including real-time CDC `INSERT`/`UPDATE` simulation):
   ```bash
   python examples/llm-vector-langchain/app.py --mode demo --offline
   ```

3. Ask a custom question for a specific customer (`CUST-001` through `CUST-004`):
   ```bash
   python examples/llm-vector-langchain/app.py \
     --mode ask \
     --customer-id CUST-001 \
     --question "Which card gives me airport lounge access and 5% on flights?" \
     --offline
   ```

### Option B: Run Full Stack with Docker Compose (PostgreSQL CDC + Qdrant + Dozer)

```bash
cd examples/llm-vector-langchain
docker compose up --build
```

This starts:
- **`postgres`** (`postgres:15-alpine` with `wal_level=logical` and [`seed.sql`](./seed.sql))
- **`qdrant`** (`qdrant/qdrant:v1.9.0` on `:6333`)
- **`dozer`** (`getdozer/dozer:latest` running [`dozer-config.yaml`](./dozer-config.yaml) on `:8080` and `:50051`)
- **`rag-assistant`** (Python 3.11 container executing `app.py --mode demo`)

---

## 4. Running the Test Suite

```bash
pytest examples/llm-vector-langchain/tests -v
```
