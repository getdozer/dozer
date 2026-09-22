# Dozer + LLM + Vector DB + LangChain Sample

This repository contains a **minimal, production‑ready** example that demonstrates how to:

1. **Pull a unified customer profile** from Dozer (CDC‑powered data lake).
2. **Pass that profile to an LLM** (OpenAI) via LangChain to generate a hyper‑personalized chatbot response.
3. (Optional) Show where a vector database could be plugged in for retrieval‑augmented generation.

The sample is built with **Next.js** (React) and **TypeScript**, making it easy to run locally or deploy to Vercel.

## Quick Start

1. Clone the repository.
2. Install dependencies: `npm install`
3. Create a `.env` file based on `.env.example`.
4. Run the development server: `npm run dev`
5. Open `http://localhost:3000` in your browser.