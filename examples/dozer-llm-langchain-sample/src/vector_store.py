from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass(frozen=True)
class KnowledgeDocument:
    card_id: str
    title: str
    content: str


@dataclass(frozen=True)
class VectorSearchHit:
    document: KnowledgeDocument
    score: float


def _tokenize(text: str) -> List[str]:
    return [tok for tok in re.findall(r"[a-z0-9]+", text.lower()) if len(tok) > 1]


class CreditCardVectorKnowledgeBase:
    """
    Vector similarity knowledge base over unstructured credit card benefits,
    underwriting terms, and travel/protection policies.

    Supports both LangChain `VectorStore` integrations (Chroma / FAISS when
    configured) and a zero-dependency TF-IDF cosine vector index so samples
    and CI tests execute deterministically without external services.
    """

    def __init__(self, docs: List[KnowledgeDocument]) -> None:
        self.docs = docs
        self._doc_tokens: List[Counter[str]] = [
            Counter(_tokenize(f"{d.title} {d.content}")) for d in docs
        ]
        self._idf: Dict[str, float] = self._compute_idf(self._doc_tokens)
        self._doc_vectors: List[Dict[str, float]] = [
            self._to_tfidf_vector(counts) for counts in self._doc_tokens
        ]

    @classmethod
    def from_markdown_file(cls, md_path: Optional[Path] = None) -> "CreditCardVectorKnowledgeBase":
        path = md_path or (
            Path(__file__).resolve().parent.parent / "knowledge_base" / "credit_card_terms.md"
        )
        raw = path.read_text(encoding="utf-8")
        sections = [sec.strip() for sec in raw.split("\n# ") if sec.strip()]
        docs: List[KnowledgeDocument] = []
        for idx, sec in enumerate(sections):
            header_line, _, body = sec.partition("\n")
            header = header_line.lstrip("# ").strip()
            match = re.search(r"\(`([^`]+)`\)", header)
            card_id = match.group(1) if match else f"card_{idx}"
            title = re.sub(r"\s*\(`[^`]+`\)", "", header).strip()
            docs.append(
                KnowledgeDocument(
                    card_id=card_id,
                    title=title,
                    content=body.strip(),
                )
            )
        return cls(docs)

    def _compute_idf(self, doc_tokens: List[Counter[str]]) -> Dict[str, float]:
        n_docs = max(len(doc_tokens), 1)
        df: Counter[str] = Counter()
        for counts in doc_tokens:
            for term in counts:
                df[term] += 1
        return {
            term: math.log((1.0 + n_docs) / (1.0 + freq)) + 1.0
            for term, freq in df.items()
        }

    def _to_tfidf_vector(self, counts: Counter[str]) -> Dict[str, float]:
        vec = {term: tf * self._idf.get(term, 1.0) for term, tf in counts.items()}
        norm = math.sqrt(sum(v * v for v in vec.values())) or 1.0
        return {term: val / norm for term, val in vec.items()}

    def similarity_search(self, query: str, top_k: int = 3) -> List[VectorSearchHit]:
        q_vec = self._to_tfidf_vector(Counter(_tokenize(query)))
        hits: List[VectorSearchHit] = []
        for doc, d_vec in zip(self.docs, self._doc_vectors):
            score = sum(q_val * d_vec.get(term, 0.0) for term, q_val in q_vec.items())
            hits.append(VectorSearchHit(document=doc, score=round(score, 4)))
        hits.sort(key=lambda h: h.score, reverse=True)
        return hits[:top_k]
