import argparse
import os
import tempfile
from urllib.error import URLError
from urllib.request import urlopen

from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.prompts import ChatPromptTemplate

from bank import all_profiles, card_text, get_profile, load_bank


class BagOfWords(Embeddings):
    """Local embedder so chroma works without an api key or a model download."""

    def __init__(self):
        self.vocab = {}

    def _tokens(self, text):
        out = []
        buf = []
        for ch in text.lower():
            if ch.isalnum():
                buf.append(ch)
            elif buf:
                out.append("".join(buf))
                buf = []
        if buf:
            out.append("".join(buf))
        return out

    def _fit(self, texts):
        for text in texts:
            for tok in self._tokens(text):
                if tok not in self.vocab:
                    self.vocab[tok] = len(self.vocab)
        if not self.vocab:
            self.vocab["empty"] = 0

    def _vec(self, text):
        v = [0.0] * len(self.vocab)
        for tok in self._tokens(text):
            i = self.vocab.get(tok)
            if i is not None:
                v[i] += 1.0
        n = sum(x * x for x in v) ** 0.5 or 1.0
        return [x / n for x in v]

    def embed_documents(self, texts):
        self._fit(texts)
        return [self._vec(t) for t in texts]

    def embed_query(self, text):
        if not self.vocab:
            self._fit([text])
        return self._vec(text)


def fetch_json(url):
    with urlopen(url, timeout=3) as r:
        import json

        return json.load(r)


def load_from_api(base):
    base = base.rstrip("/")
    profiles = {p["customer_id"]: p for p in fetch_json(base + "/customer_360")}
    cards = fetch_json(base + "/cards")
    return profiles, cards


def load_local():
    bank = load_bank()
    profiles = {p["customer_id"]: p for p in all_profiles(bank)}
    return profiles, bank["cards"]


def load_data(base):
    try:
        return load_from_api(base)
    except (URLError, TimeoutError, OSError):
        return load_local()


def index_cards(cards):
    docs = [
        Document(page_content=card_text(c), metadata={"card_id": c["card_id"], "name": c["name"]})
        for c in cards
    ]
    tmp = tempfile.mkdtemp(prefix="dozer-chroma-")
    return Chroma.from_documents(docs, embedding=BagOfWords(), persist_directory=tmp)


def retrieve(store, profile, k=3):
    return store.as_retriever(search_kwargs={"k": k}).invoke(profile["text"])


def local_answer(profile, docs, question):
    lines = [profile["text"], "", "Closest cards:"]
    for i, d in enumerate(docs, 1):
        lines.append(f"{i}. {d.metadata.get('name')}: {d.page_content}")
    lines.append("")
    lines.append(f"Q: {question}")
    if docs:
        lines.append(f"I'd start with {docs[0].metadata.get('name')} given the spend mix above.")
    return "\n".join(lines)


def llm_answer(profile, docs, question):
    from langchain_openai import ChatOpenAI

    ctx = "\n".join(f"- {d.page_content}" for d in docs)
    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "Recommend one credit card using only the customer profile and retrieved cards. "
                "Name the card, say why, and mention one caveat.",
            ),
            (
                "human",
                "Customer:\n{profile}\n\nCards:\n{cards}\n\nQuestion: {question}",
            ),
        ]
    )
    model = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    msg = (prompt | model).invoke(
        {"profile": profile["text"], "cards": ctx, "question": question}
    )
    return msg.content


def answer(profile, cards, question):
    store = index_cards(cards)
    docs = retrieve(store, profile)
    if os.getenv("OPENAI_API_KEY"):
        return llm_answer(profile, docs, question)
    return local_answer(profile, docs, question)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--customer-id", default="c1")
    p.add_argument("--question", default="which card should we offer and why?")
    p.add_argument("--base-url", default=os.getenv("BANK_API", "http://127.0.0.1:8000"))
    p.add_argument("--repl", action="store_true")
    args = p.parse_args()

    profiles, cards = load_data(args.base_url)
    profile = profiles.get(args.customer_id)
    if profile is None:
        # last chance: rebuild from csv if api was stale
        profile = get_profile(args.customer_id)
    if profile is None:
        known = ", ".join(sorted(profiles) or ["c1", "c2", "c3", "c4", "c5"])
        raise SystemExit(f"unknown customer {args.customer_id!r}. try {known}")

    if args.repl:
        print(profile["text"])
        print("ask something (empty line to quit)")
        while True:
            try:
                q = input("> ").strip()
            except EOFError:
                break
            if not q:
                break
            print(answer(profile, cards, q))
            print()
        return

    print(answer(profile, cards, args.question))


if __name__ == "__main__":
    main()
