"""FastAPI wrapper + CLI around the chatbot.

REST:  POST /chat {"customer_id": 1001, "message": "..."}
CLI:   python main.py --cli --customer-id 1001
"""

from __future__ import annotations

import argparse
import os

from fastapi import FastAPI
from pydantic import BaseModel

from chatbot import answer, session_history
from dozer_client import DozerClient
from vector_store import ProductVectorStore

app = FastAPI(title="Dozer LLM chatbot sample", version="1.0.0")
dozer = DozerClient()
products: ProductVectorStore | None = None


class ChatRequest(BaseModel):
    customer_id: int
    message: str
    session_id: str = "default"


class ChatResponse(BaseModel):
    customer_id: int
    reply: str
    products_used: list[str] = []


@app.on_event("startup")
def startup() -> None:
    global products
    products = ProductVectorStore()
    indexed = products.index_products(dozer.get_products())
    print(f"[chatbot] indexed {indexed} products from Dozer into Qdrant")


@app.post("/chat", response_model=ChatResponse)
def chat(request: ChatRequest) -> ChatResponse:
    assert products is not None
    history = session_history(request.session_id)
    matches = products.search(request.message, k=2)
    reply = answer(dozer, products, request.customer_id, request.message, history)
    history.add_user_message(request.message)
    history.add_ai_message(reply)
    return ChatResponse(
        customer_id=request.customer_id,
        reply=reply,
        products_used=[m["name"] for m in matches],
    )


@app.get("/health")
def health() -> dict:
    return {"dozer": dozer.health(), "chatbot": "ok"}


def cli() -> None:
    parser = argparse.ArgumentParser(description="Dozer + Langchain banking chatbot")
    parser.add_argument("--cli", action="store_true", help="interactive terminal chat")
    parser.add_argument("--customer-id", type=int, default=1001)
    args = parser.parse_args()

    global products
    products = ProductVectorStore()
    indexed = products.index_products(dozer.get_products())
    print(f"[chatbot] indexed {indexed} products from Dozer into Qdrant")

    session = session_history(f"cli-{args.customer_id}")
    print(f"Chatting as customer {args.customer_id}. Type 'quit' to exit.")
    while True:
        try:
            question = input("you> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not question or question.lower() in {"quit", "exit"}:
            break
        reply = answer(dozer, products, args.customer_id, question, session)
        session.add_user_message(question)
        session.add_ai_message(reply)
        print(f"avery> {reply}\n")


if __name__ == "__main__":
    if os.getenv("CHATBOT_CLI") or "--cli" in os.sys.argv:
        cli()
    else:
        import uvicorn

        uvicorn.run(app, host="0.0.0.0", port=8000)
