# Hyper-Personalized Banking Advisor with Dozer, LangChain & Vector RAG

This sample demonstrates an end-to-end AI-powered banking advisor application inspired by Dozer's published article on [hyper-personalization using LLMs, Vector Databases, and LangChain](https://getdozer.io/blog/llm-chatbot).

---

## 🌟 Architecture Overview

Modern financial applications require real-time context. Static customer demographic data is not enough to make relevant recommendations; an advisor needs to understand real-time transaction spending velocity, category concentrations, and strict underwriting guardrails.

```
┌─────────────────────────────────┐
│     Multi-Source Datasets       │
│  - Customer Profiles (CSV)      │
│  - Real-time Transactions (CSV) │
│  - Card Product Catalog (CSV)   │
└────────────────┬────────────────┘
                 │
                 ▼
┌────────────────────────────────────────────────────────┐
│                      DOZER CORE                        │
│  - Ingests via !LocalStorage (connections)             │
│  - Streaming SQL: Computes real-time customer profile   │
│    JOINing profiles + transactions with GROUP BY       │
│  - Exposes queryable endpoints:                        │
│      • /customer_profile (REST port 8080 / gRPC 50051) │
│      • /card_catalog                                   │
└────────────────┬───────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│   Financial & Risk Guardrails   │
│  - Minimum Age Validation (18+) │
│  - FICO Score Thresholds        │
│  - Minimum Income Requirements  │
│  - Risk Tolerance Alignment     │
└────────────────┬────────────────┘
                 │ (Eligible Products Only)
                 ▼
┌─────────────────────────────────┐
│      LangChain Vector RAG       │
│  - Chroma Vector Store          │
│  - Semantic Catalog Embeddings  │
│  - Category Spend Weighting     │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│     Personalized Advisory       │
│  - Grounded Rationale           │
│  - Spend Alignment Highlights   │
│  - Dual LLM (OpenAI/Local Rule) │
└─────────────────────────────────┘
```

---

## 🚀 Key Highlights

1. **Modern Dozer Configuration**: Uses standard `sql INTO customer_profile` streaming transformations fully compliant with Dozer's schema.
2. **Hard Bank Eligibility Guardrails**: Rejects ineligible products *before* vector ranking and LLM prompting, preventing hallucinated product offers.
3. **Dual-Mode Operation**:
   - **Live Mode**: Queries Dozer REST API (`http://localhost:8080/customer_profile/query` and `http://localhost:8080/card_catalog`).
   - **Offline / Local Emulation Mode**: Emulates the exact Dozer streaming SQL pipeline locally so CI, unit tests, and local development run with zero external binary dependencies.
4. **LangChain & Vector RAG**: Semantic vector retrieval over card rewards and perks, augmented with dynamic weights derived from customer spend concentrations.
5. **Flexible CLI & Interactive Chat**: Test recommendations via interactive consultation mode or generate structured JSON reports.

---

## 📁 Project Structure

```
examples/llm-banking-advisor/
├── dozer-config.yaml                 # Modern Dozer configuration with streaming SQL
├── requirements.txt                  # Python dependencies
├── models.py                         # Typed dataclasses (CustomerProfile, CardProduct, etc.)
├── dozer_client.py                   # Dual-mode Dozer API client (Live REST & Local fallback)
├── guardrails.py                     # Bank underwriting & financial eligibility rules
├── vector_rag.py                     # LangChain Chroma vector store & Grounded Retriever
├── advisor.py                        # Banking Advisor orchestrator
├── app.py                            # CLI and Interactive terminal chat interface
├── data/
│   ├── customer_profiles/            # Customer demographic and financial profiles
│   ├── transactions/                 # Transaction streams with merchant categories
│   └── credit_card_products/         # Credit card product catalog with fee/perk details
└── tests/
    ├── test_config.py                # Schema & SQL validation tests
    ├── test_dozer_client.py          # Data ingestion and SQL aggregation tests
    ├── test_guardrails.py            # Underwriting rule validation tests
    ├── test_vector_rag.py            # Vector retrieval and semantic ranking tests
    └── test_advisor.py               # End-to-end integration tests
```

---

## ⚡ Quickstart

### 1. Run the Advisor CLI (Offline / Self-Contained Mode)

The advisor runs out-of-the-box with zero setup:

```bash
# Evaluate Elena Rostova (Frequent Traveler)
python3 app.py --customer-id CUST_001

# Evaluate Marcus Vance (Student seeking credit building)
python3 app.py --customer-id CUST_002

# Evaluate Sarah Jenkins (Family groceries and cashback)
python3 app.py --customer-id CUST_003

# Evaluate Amina Diallo (High Net Worth Executive)
python3 app.py --customer-id CUST_005
```

### 2. Output as Machine-Readable JSON

```bash
python3 app.py --customer-id CUST_001 --json
```

### 3. Interactive Terminal Consultation Mode

```bash
python3 app.py --interactive
```

---

## 🔧 Running with Live Dozer Daemon

To run with the real Dozer streaming engine:

1. Build or download the `dozer` binary:
   ```bash
   cargo build --release --bin dozer
   ```

2. Start the Dozer pipeline from this directory:
   ```bash
   dozer run
   ```
   Dozer will initialize the LocalStorage connections, execute the streaming SQL query to materialize `customer_profile`, and start serving REST API on `http://localhost:8080`.

3. In another terminal, run the application:
   ```bash
   python3 app.py --dozer-url http://localhost:8080 --customer-id CUST_001
   ```
   The application will automatically detect the live Dozer server and set `Source Provenance: DOZER_LIVE_API`.

---

## 🧪 Running Automated Tests

Run the complete test suite:

```bash
python3 -m unittest discover -s tests -v
```

All 19 tests validate:
- Top-level Dozer YAML configuration and streaming SQL `INTO` targets.
- Real-time customer transaction aggregation and join accuracy.
- Strict financial guardrail rejections (underage, credit score, income, risk).
- Semantic vector similarity and spend category weighting.
- Full end-to-end recommendation pipelines.
