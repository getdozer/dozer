from __future__ import annotations

import csv
import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class CustomerProfile:
    customer_id: str
    full_name: str
    credit_score: int
    annual_income: float
    credit_utilization_pct: float
    current_card_id: str
    preferred_benefit: str


@dataclass(frozen=True)
class CreditCardProduct:
    card_id: str
    card_name: str
    issuer_tier: str
    min_credit_score: int
    min_annual_income: float
    max_utilization_pct: float
    annual_fee: float
    welcome_bonus_usd: float
    travel_rate: float
    dining_rate: float
    grocery_rate: float
    base_rate: float
    foreign_tx_fee_pct: float


@dataclass(frozen=True)
class CategorySpendSummary:
    customer_id: str
    category: str
    tx_count: int
    total_spend: float


class DozerFeatureStoreClient:
    """
    Client for querying real-time customer features, aggregated spend summaries,
    and credit card product tables exposed by Dozer's low-latency REST API
    (`POST /{endpoint}/query`).

    Automatically falls back to deterministic local CSV materialization when
    a live Dozer server is not reachable (e.g., offline demos or CI tests).
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        data_dir: Optional[Path] = None,
        timeout_sec: float = 1.5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.data_dir = data_dir or (Path(__file__).resolve().parent.parent / "data")
        self.timeout_sec = timeout_sec

    def _post_dozer_query(
        self, endpoint: str, filter_spec: Optional[Dict[str, Any]] = None, limit: int = 100
    ) -> Optional[List[Dict[str, Any]]]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}/query"
        payload: Dict[str, Any] = {"$limit": limit}
        if filter_spec:
            payload["$filter"] = filter_spec

        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                if resp.status == 200:
                    body = json.loads(resp.read().decode("utf-8"))
                    if isinstance(body, list):
                        return body
        except (urllib.error.URLError, TimeoutError, OSError, ValueError):
            return None
        return None

    def get_customer_profile(self, customer_id: str) -> CustomerProfile:
        live = self._post_dozer_query("customers", {"customer_id": customer_id}, limit=1)
        if live:
            row = live[0]
            return CustomerProfile(
                customer_id=str(row["customer_id"]),
                full_name=str(row["full_name"]),
                credit_score=int(row["credit_score"]),
                annual_income=float(row["annual_income"]),
                credit_utilization_pct=float(row["credit_utilization_pct"]),
                current_card_id=str(row["current_card_id"]),
                preferred_benefit=str(row["preferred_benefit"]),
            )

        with (self.data_dir / "customers.csv").open("r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["customer_id"] == customer_id:
                    return CustomerProfile(
                        customer_id=row["customer_id"],
                        full_name=row["full_name"],
                        credit_score=int(row["credit_score"]),
                        annual_income=float(row["annual_income"]),
                        credit_utilization_pct=float(row["credit_utilization_pct"]),
                        current_card_id=row["current_card_id"],
                        preferred_benefit=row["preferred_benefit"],
                    )
        raise KeyError(f"Unknown customer_id: {customer_id}")

    def list_credit_cards(self) -> List[CreditCardProduct]:
        live = self._post_dozer_query("credit_cards", limit=50)
        rows = live
        if rows is None:
            with (self.data_dir / "credit_cards.csv").open("r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))

        return [
            CreditCardProduct(
                card_id=str(r["card_id"]),
                card_name=str(r["card_name"]),
                issuer_tier=str(r["issuer_tier"]),
                min_credit_score=int(r["min_credit_score"]),
                min_annual_income=float(r["min_annual_income"]),
                max_utilization_pct=float(r["max_utilization_pct"]),
                annual_fee=float(r["annual_fee"]),
                welcome_bonus_usd=float(r["welcome_bonus_usd"]),
                travel_rate=float(r["travel_rate"]),
                dining_rate=float(r["dining_rate"]),
                grocery_rate=float(r["grocery_rate"]),
                base_rate=float(r["base_rate"]),
                foreign_tx_fee_pct=float(r["foreign_tx_fee_pct"]),
            )
            for r in rows
        ]

    def get_customer_spending_summary(self, customer_id: str) -> List[CategorySpendSummary]:
        live = self._post_dozer_query(
            "customer_spending_summary", {"customer_id": customer_id}, limit=50
        )
        if live is not None:
            return [
                CategorySpendSummary(
                    customer_id=str(r["customer_id"]),
                    category=str(r["category"]),
                    tx_count=int(r["tx_count"]),
                    total_spend=float(r["total_spend"]),
                )
                for r in live
            ]

        totals: Dict[str, List[float]] = {}
        with (self.data_dir / "transactions.csv").open("r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["customer_id"] == customer_id:
                    cat = row["category"]
                    entry = totals.setdefault(cat, [0.0, 0.0])
                    entry[0] += 1
                    entry[1] += float(row["amount"])

        return [
            CategorySpendSummary(
                customer_id=customer_id,
                category=cat,
                tx_count=int(vals[0]),
                total_spend=round(vals[1], 2),
            )
            for cat, vals in sorted(totals.items(), key=lambda item: -item[1][1])
        ]
