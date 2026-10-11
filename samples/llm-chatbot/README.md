# Hyper-Personalized Banking Chatbot with Dozer, LangChain, Vector Database & LLM

This sample demonstrates how to build a **hyper-personalized financial assistant** by combining:
1. **Dozer**: Real-time unified customer 360 profile generation across disparate data sources (customer demographics & transaction streams).
2. **Vector Database (Chroma)**: Semantic search over banking knowledge bases (credit card terms, rewards, fee structures, APR).
3. **LangChain**: Orchestration framework linking Dozer customer context, vector search, and LLM reasoning.
4. **LLM (OpenAI / Local LLMs)**: Generates tailored credit card advice based on individual spending patterns and credit scores.

---

## Use Case Scenario

Standard LLM chatbots provide generic financial advice. With Dozer, the chatbot queries the real-time unified customer profile:
- Spending breakdown over the last 90 days (e.g., $4,200 travel, $1,100 dining).
- Credit score, income, and preferred categories.

When the customer asks *"Which credit card is best for me?"*, LangChain:
1. Fetches the customer's profile via Dozer REST API.
2. Queries the Vector DB for credit cards matching their profile and rewards preference.
3. Feeds both contexts into the LLM to deliver a hyper-personalized recommendation:
   > *"Based on your high travel expenditure ($4,200 in the last 90 days) and your credit score of 780, we recommend the **AeroElite Global Travel Card**..."*

---

## Directory Structure

