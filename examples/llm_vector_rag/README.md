# Dozer + ChromaDB + LangChain: Hyper-Personalized Banking RAG

This sample demonstrates how to build a real-time, hyper-personalized financial advisory chatbot using **Dozer**, **ChromaDB**, and **LangChain**, reflecting the architecture described in the official article: [Hyper-personalization with LLMs, Vector Databases, and Dozer](https://getdozer.io/blog/llm-chatbot).

---

## 🏛️ Architecture Overview

```
                      +-----------------------------+
                      |   PostgreSQL Transactions   |
                      |   & Customer Data (CDC)     |
                      +--------------+--------------+
                                     |
                                     v
                       +-------------+-------------+
                       |        Dozer Engine       |
                       | Real-Time Aggregations    |
                       | & Unified Customer Stream |
                       +-------------+-------------+
                                     |
                        REST APIs (/customer_profiles)
                                     v
+-----------------------+     +------+------+     +--------------------------+
|  Credit Card Products | --> |  ChromaDB   | --> |     LangChain Pipeline   |
|   (Knowledge Base)    |     | VectorStore |     | Contextual Banking Advice|
+-----------------------+     +-------------+     +--------------------------+
```

### The Problem
Traditional RAG setups struggle with stale data because vector databases alone cannot track real-time transactional updates. By combining:
1. **Dozer**: Continuously ingests PostgreSQL transaction logs (CDC) and computes real-time customer spending profiles without batch delays.
2. **ChromaDB**: Houses banking product documents and performs dense vector semantic search.
3. **LangChain**: Dynamically synthesizes the live customer context and retrieved products into personalized recommendations.

---

## 🚀 Quickstart with Docker Compose

To run the entire end-to-end stack:

```bash
docker compose up --build
```

Services started:
- `postgres`: PostgreSQL 15 database initialized with customers, transactions, and card products.
- `dozer`: Dozer streaming pipeline serving real-time REST endpoints at `http://localhost:8080/customer_profiles`.
- `app`: Python RAG application querying Dozer and delivering tailored advice.

---

## 💻 Local Standalone Execution

Install requirements:
```bash
pip install -r requirements.txt
```

Run the pipeline:
```bash
python rag_pipeline.py
```

Run unit tests:
```bash
python test_rag_pipeline.py
```

---

## 📊 Endpoints & Transformations

- `/customer_profiles`: Combines customer demographic data with categorized transaction sums (Travel, Dining, Groceries).
- `/available_cards`: Exposes the credit card catalog with eligibility thresholds (minimum credit score, annual fee, reward rate).
