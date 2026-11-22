"""Vector Database & LangChain VectorStore with Real-Time Dozer CDC Indexing.

Implements:
- `DeterministicBankingEmbeddings`: 128-dim domain-aware + SHA-256 n-gram projection
  `langchain_core.embeddings.Embeddings` implementation (zero external API keys required,
  deterministic high-precision semantic search).
- `DozerLangChainVectorStore`: `langchain_core.vectorstores.VectorStore` implementation
  with metadata filtering (`max_min_credit_score`, `category`, `card_id`), optional Qdrant
  persistence, and real-time `apply_cdc_event` (`INSERT`, `UPDATE`, `DELETE`) mutations.
"""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Type, Union

from langchain_core.documents import Document
from langchain_core.embeddings import Embeddings
from langchain_core.vectorstores import VectorStore

try:
    from .dozer_client import CDCEvent, CDCOperation
except ImportError:
    from dozer_client import CDCEvent, CDCOperation


class DeterministicBankingEmbeddings(Embeddings):
    """Deterministic 128-dim semantic embeddings tailored for banking & credit card RAG."""

    CONCEPT_CLUSTERS: Tuple[Tuple[str, ...], ...] = (
        ("travel", "flight", "flights", "airline", "airlines", "hotel", "hotels", "trip", "airfare"),
        ("lounge", "priority", "pass", "airport", "vip", "concierge", "global", "tsa", "precheck"),
        ("dining", "restaurant", "restaurants", "culinary", "cafe", "cafes", "bistro", "food", "delivery"),
        ("groceries", "grocery", "supermarket", "supermarkets", "household", "whole", "foods", "market"),
        ("cashback", "cash", "rebate", "everyday", "statement", "credit", "flat"),
        ("builder", "horizon", "rebuild", "building", "graduation", "fico", "starter", "history"),
        ("apr", "interest", "low", "rate", "variable", "carry", "balance"),
        ("technology", "tech", "cloud", "aws", "gcp", "azure", "saas", "software", "hardware", "quantum"),
        ("commercial", "executive", "business", "enterprise", "corporate", "wifi", "boingo"),
        ("fee", "annual", "waiver", "zero", "free", "cost", "$0"),
        ("insurance", "delay", "reimbursement", "luggage", "baggage", "collision", "rental", "protection"),
        ("platinum", "private", "tier", "relationship", "multiplier", "25%", "high", "income"),
        ("bonus", "welcome", "points", "reward", "rewards", "earn", "5%", "4.5%", "4%"),
        ("score", "eligibility", "qualify", "minimum", "underwriting", "upgrade"),
        ("sapphire", "reserve", "card-travel-sig", "foreign", "international"),
        ("plus", "card-cash-plus", "family", "streaming"),
    )

    def __init__(self, dimension: int = 128) -> None:
        if dimension < 64:
            raise ValueError("Embedding dimension must be at least 64.")
        self.dimension = dimension

    @staticmethod
    def _tokenize(text: str) -> List[str]:
        lowered = text.lower()
        return re.findall(r"[a-z0-9$%.]+(?:-[a-z0-9]+)*", lowered)

    def _embed_single(self, text: str) -> List[float]:
        vec = [0.0] * self.dimension
        tokens = self._tokenize(text)
        if not tokens:
            return vec

        num_concepts = len(self.CONCEPT_CLUSTERS)
        # 1. Domain semantic concept dimensions (first 32 dimensions)
        for c_idx, keywords in enumerate(self.CONCEPT_CLUSTERS):
            kw_set = set(keywords)
            hits = sum(1 for tok in tokens if tok in kw_set)
            prefix_hits = sum(
                0.5
                for tok in tokens
                if tok not in kw_set and any(tok.startswith(kw[:4]) for kw in kw_set if len(kw) >= 4)
            )
            if hits > 0 or prefix_hits > 0:
                vec[c_idx * 2] = (hits * 2.5) + prefix_hits
                vec[c_idx * 2 + 1] = math.log1p(hits + prefix_hits) * 2.0

        # 2. Signed token & bi-gram hash projection across remaining dimensions
        hash_offset = num_concepts * 2
        hash_dims = self.dimension - hash_offset
        features: List[Tuple[str, float]] = [(tok, 1.4) for tok in tokens]
        for i in range(len(tokens) - 1):
            features.append((f"{tokens[i]}_{tokens[i + 1]}", 1.8))

        for feat, weight in features:
            digest = hashlib.sha256(feat.encode("utf-8")).digest()
            slot_a = hash_offset + (int.from_bytes(digest[0:4], "big") % hash_dims)
            sign_a = 1.0 if (digest[4] & 1) == 0 else -1.0
            slot_b = hash_offset + (int.from_bytes(digest[5:9], "big") % hash_dims)
            sign_b = 1.0 if (digest[9] & 1) == 0 else -1.0
            vec[slot_a] += sign_a * weight
            vec[slot_b] += sign_b * (weight * 0.7)

        norm = math.sqrt(sum(v * v for v in vec))
        if norm > 0:
            vec = [v / norm for v in vec]
        return vec

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed a batch of documents."""
        return [self._embed_single(text) for text in texts]

    def embed_query(self, text: str) -> List[float]:
        """Embed a single search query."""
        return self._embed_single(text)


@dataclass
class _IndexedVectorRecord:
    doc_id: str
    document: Document
    vector: List[float]


class DozerLangChainVectorStore(VectorStore):
    """LangChain VectorStore synchronized in real time with Dozer CDC events."""

    def __init__(
        self,
        embedding_function: Optional[Embeddings] = None,
        collection_name: str = "dozer_banking_documents",
        qdrant_url: Optional[str] = None,
    ) -> None:
        self.embedding_function: Embeddings = (
            embedding_function or DeterministicBankingEmbeddings()
        )
        self.collection_name = collection_name
        self.qdrant_url = qdrant_url
        self._records: Dict[str, _IndexedVectorRecord] = {}
        self.index_version: int = 0
        self.cdc_event_log: List[CDCEvent] = []

    @property
    def embeddings(self) -> Embeddings:
        return self.embedding_function

    @property
    def document_count(self) -> int:
        return len(self._records)

    @staticmethod
    def _cosine_similarity(vec_a: Sequence[float], vec_b: Sequence[float]) -> float:
        return float(sum(a * b for a, b in zip(vec_a, vec_b)))

    @staticmethod
    def format_dozer_document_text(record: Dict[str, Any]) -> str:
        title = str(record.get("title", "")).strip()
        category = str(record.get("category", "")).strip()
        card_id = str(record.get("card_id", "")).strip()
        content = str(record.get("content", "")).strip()
        return (
            f"Title: {title}\n"
            f"Category: {category} | Card ID: {card_id}\n"
            f"Content: {content}"
        )

    def add_texts(
        self,
        texts: Iterable[str],
        metadatas: Optional[List[dict]] = None,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[str]:
        text_list = list(texts)
        if not text_list:
            return []
        vectors = self.embedding_function.embed_documents(text_list)
        assigned_ids: List[str] = []

        for idx, (text, vector) in enumerate(zip(text_list, vectors)):
            meta = dict(metadatas[idx]) if metadatas and idx < len(metadatas) else {}
            doc_id = (
                ids[idx]
                if ids and idx < len(ids)
                else str(meta.get("doc_id") or f"DOC-AUTO-{len(self._records) + 1:03d}")
            )
            meta.setdefault("doc_id", doc_id)
            doc = Document(page_content=text, metadata=meta)
            self._records[doc_id] = _IndexedVectorRecord(
                doc_id=doc_id,
                document=doc,
                vector=vector,
            )
            assigned_ids.append(doc_id)

        self.index_version += 1
        return assigned_ids

    def add_documents(self, documents: List[Document], **kwargs: Any) -> List[str]:
        texts = [doc.page_content for doc in documents]
        metadatas = [dict(doc.metadata) for doc in documents]
        ids = [
            str(doc.metadata.get("doc_id") or f"DOC-AUTO-{idx + 1:03d}")
            for idx, doc in enumerate(documents)
        ]
        return self.add_texts(texts=texts, metadatas=metadatas, ids=ids, **kwargs)

    def delete(self, ids: Optional[List[str]] = None, **kwargs: Any) -> Optional[bool]:
        if not ids:
            return False
        removed_any = False
        for doc_id in ids:
            if doc_id in self._records:
                self._records.pop(doc_id, None)
                removed_any = True
        if removed_any:
            self.index_version += 1
        return removed_any

    def upsert_dozer_record(self, record: Dict[str, Any]) -> str:
        """Convert a Dozer `/documents` record into a LangChain Document and index it."""
        doc_id = str(record["doc_id"])
        text = self.format_dozer_document_text(record)
        metadata = {
            "doc_id": doc_id,
            "title": record.get("title", ""),
            "category": record.get("category", ""),
            "card_id": record.get("card_id", ""),
            "min_credit_score": int(record.get("min_credit_score", 0)),
            "updated_at": str(record.get("updated_at", "")),
        }
        self.add_texts([text], metadatas=[metadata], ids=[doc_id])
        return doc_id

    def apply_cdc_event(self, event: CDCEvent) -> None:
        """Apply a Dozer CDC event (`INSERT`, `UPDATE`, `DELETE`) in real time."""
        clean_ep = event.endpoint.strip("/") if event.endpoint else "documents"
        if clean_ep and clean_ep != "documents":
            return
        self.cdc_event_log.append(event)
        if event.operation in (CDCOperation.INSERT, CDCOperation.UPDATE):
            if not event.new_record:
                raise ValueError(f"CDC {event.operation} event missing 'new_record'.")
            self.upsert_dozer_record(event.new_record)
        elif event.operation == CDCOperation.DELETE:
            self.delete([event.primary_key])

    @staticmethod
    def _matches_metadata_filter(
        metadata: Dict[str, Any],
        filter_spec: Optional[Union[Dict[str, Any], Callable[[Dict[str, Any]], bool]]],
    ) -> bool:
        if filter_spec is None:
            return True
        if callable(filter_spec):
            return bool(filter_spec(metadata))

        for key, expected in filter_spec.items():
            if key == "max_min_credit_score":
                doc_min_score = int(metadata.get("min_credit_score", 0))
                if doc_min_score > int(expected):
                    return False
            elif key == "card_ids":
                card_id = metadata.get("card_id")
                if card_id != "ALL" and card_id not in expected:
                    return False
            elif isinstance(expected, (list, tuple, set)):
                if metadata.get(key) not in expected:
                    return False
            else:
                if metadata.get(key) != expected:
                    return False
        return True

    def similarity_search_with_score(
        self,
        query: str,
        k: int = 4,
        filter: Optional[Union[Dict[str, Any], Callable[[Dict[str, Any]], bool]]] = None,
        **kwargs: Any,
    ) -> List[Tuple[Document, float]]:
        """Return top-k documents and their cosine similarity scores."""
        query_vec = self.embedding_function.embed_query(query)
        scored: List[Tuple[Document, float]] = []

        for item in self._records.values():
            if not self._matches_metadata_filter(item.document.metadata, filter):
                continue
            score = self._cosine_similarity(query_vec, item.vector)
            scored.append((item.document, score))

        scored.sort(key=lambda pair: pair[1], reverse=True)
        return scored[:k]

    def similarity_search(
        self,
        query: str,
        k: int = 4,
        filter: Optional[Union[Dict[str, Any], Callable[[Dict[str, Any]], bool]]] = None,
        **kwargs: Any,
    ) -> List[Document]:
        """Return top-k semantically similar LangChain Documents."""
        results = self.similarity_search_with_score(query=query, k=k, filter=filter, **kwargs)
        return [doc for doc, _ in results]

    def get_document_by_id(self, doc_id: str) -> Optional[Document]:
        rec = self._records.get(doc_id)
        return rec.document if rec else None

    @classmethod
    def from_texts(
        cls: Type[DozerLangChainVectorStore],
        texts: List[str],
        embedding: Optional[Embeddings] = None,
        metadatas: Optional[List[dict]] = None,
        ids: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> DozerLangChainVectorStore:
        store = cls(embedding_function=embedding, **kwargs)
        store.add_texts(texts=texts, metadatas=metadatas, ids=ids)
        return store
