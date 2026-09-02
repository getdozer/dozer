# Dozer + LLM + Vector Database + Langchain Sample

This sample demonstrates how to build a hyper-personalized credit card recommendation chatbot using Dozer, Langchain, and a vector database (Chroma). It is based on the [Dozer blog article](https://getdozer.io/blog/llm-chatbot).

## Architecture

- **Dozer** ingests data from multiple sources (customer profiles, transactions, credit card products) and exposes unified REST/gRPC APIs.
- **Langchain** orchestrates the chatbot logic, using Dozer APIs to fetch real-time customer context.
- **Chroma** (vector database) stores product embeddings for similarity search.
- **LLM** (OpenAI GPT) generates personalized responses.

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- An OpenAI API key (set as `OPENAI_API_KEY`)

## Setup

1. Clone this repository and navigate to this directory.
2. Copy `.env.example` to `.env` and set your `OPENAI_API_KEY`.
3. Run `docker-compose up -d` to start PostgreSQL and Dozer.
4. Wait for Dozer to initialize (check logs with `docker-compose logs dozer`).
5. Install Python dependencies: `pip install -r requirements.txt`
6. Run the chatbot: `python langchain_app.py`

## How it works

1. Dozer reads from PostgreSQL tables (`customers`, `transactions`, `credit_cards`) and exposes REST endpoints at `http://localhost:8080`.
2. The Python app uses Langchain to:
   - Fetch customer profile and recent transactions via Dozer REST API.
   - Generate a summary of the customer's spending habits.
   - Retrieve relevant credit card products from Chroma based on the summary.
   - Use an LLM to craft a personalized recommendation.

## Files

- `dozer-config.yaml`: Dozer configuration with sources and SQL queries.
- `seed.sql`: Sample data for PostgreSQL.
- `langchain_app.py`: Main Python application.
- `docker-compose.yaml`: Orchestrates PostgreSQL and Dozer.

## Customization

- Modify `dozer-config.yaml` to add more datasets or change SQL logic.
- Update `seed.sql` with your own data.
- Adjust the Langchain prompt in `langchain_app.py` for different use cases.
