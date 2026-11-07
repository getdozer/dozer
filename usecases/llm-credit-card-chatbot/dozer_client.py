"""Client adapter for querying unified customer profiles and card products from Dozer."""

from __future__ import annotations

import csv
import json
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class CustomerProfile:
    """Unified customer profile materialized by Dozer across profile and transaction streams."""

    customer_id: int
    full_name: str
    credit_score: int
    annual_income: float
    preferred_category: str
    kyc_verified: bool
    tx_count: int
    total_spend: float


@dataclass(frozen=True)
class CreditCardProduct:
    """Credit card product metadata served from the Dozer catalog endpoint."""

    card_id: str
    card_name: str
    tier: str
    min_credit_score: int
    min_annual_income: float
    annual_fee: float
    primary_reward_category: str
    cashback_pct: float
    benefits_summary: str


class DozerRestClient:
    """Fetches unified customer profiles and credit card products from Dozer REST endpoints."""

    def __init__(self, base_url: str = "http://127.0.0.1:8080", data_dir: Optional[Path] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.data_dir = data_dir or (Path(__file__).resolve().parent / "data")

    def get_customer_profile(self, customer_id: int) -> Optional[CustomerProfile]:
        """Queries Dozer /customer_profiles/query for a customer, falling back to local CSV join."""
        payload = {"$filter": {"customer_id": customer_id}}
        rows = self._post_query("/customer_profiles/query", payload)
        if rows:
            return _parse_customer_row(rows[0])
        return self._load_local_customer_profile(customer_id)

    def list_credit_cards(self) -> List[CreditCardProduct]:
        """Queries Dozer /credit_cards/query for all catalog cards, with local CSV fallback."""
        rows = self._post_query("/credit_cards/query", {})
        if rows:
            return [_parse_card_row(r) for r in rows]
        return self._load_local_credit_cards()

    def filter_eligible_cards(self, profile: CustomerProfile) -> List[CreditCardProduct]:
        """Applies hard underwriting gates before passing candidate products to vector retrieval."""
        if not profile.kyc_verified:
            return []
        return [
            card
            for card in self.list_credit_cards()
            if profile.credit_score >= card.min_credit_score
            and profile.annual_income >= card.min_annual_income
        ]

    def _post_query(self, endpoint: str, body: dict) -> List[dict]:
        """Executes a POST query against the Dozer REST server if reachable."""
        url = f"{self.base_url}{endpoint}"
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=2.5) as resp:
                parsed = json.loads(resp.read().decode("utf-8"))
                return parsed if isinstance(parsed, list) else []
        except Exception:
            return []

    def _load_local_customer_profile(self, customer_id: int) -> Optional[CustomerProfile]:
        """Materializes the customer profile from local CSV sources when Dozer REST is offline."""
        customers_path = self.data_dir / "customers.csv"
        tx_path = self.data_dir / "transactions.csv"
        if not customers_path.exists():
            return None
        target_row = _find_csv_row(customers_path, "customer_id", str(customer_id))
        if target_row is None:
            return None
        tx_count, total_spend = _aggregate_transactions(tx_path, str(customer_id))
        merged = dict(target_row)
        merged["tx_count"] = tx_count
        merged["total_spend"] = total_spend
        return _parse_customer_row(merged)

    def _load_local_credit_cards(self) -> List[CreditCardProduct]:
        """Loads credit card catalog records from data/credit_cards.csv."""
        cards_path = self.data_dir / "credit_cards.csv"
        if not cards_path.exists():
            return []
        with cards_path.open("r", encoding="utf-8") as handle:
            return [_parse_card_row(row) for row in csv.DictReader(handle)]


def _find_csv_row(csv_path: Path, key_field: str, expected_val: str) -> Optional[dict]:
    """Returns the first CSV dictionary row matching key_field == expected_val."""
    with csv_path.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if str(row.get(key_field, "")).strip() == expected_val:
                return row
    return None


def _aggregate_transactions(tx_path: Path, customer_id_str: str) -> tuple[int, float]:
    """Computes transaction count and total spend for a single customer."""
    if not tx_path.exists():
        return 0, 0.0
    count = 0
    total = 0.0
    with tx_path.open("r", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("customer_id", "")).strip() == customer_id_str:
                count += 1
                total += float(row.get("amount", 0.0))
    return count, round(total, 2)


def _parse_customer_row(row: dict) -> CustomerProfile:
    """Converts a raw REST or CSV dictionary into a typed CustomerProfile."""
    raw_kyc = row.get("kyc_verified", False)
    kyc_bool = raw_kyc if isinstance(raw_kyc, bool) else str(raw_kyc).strip().lower() == "true"
    return CustomerProfile(
        customer_id=int(row["customer_id"]),
        full_name=str(row["full_name"]),
        credit_score=int(row["credit_score"]),
        annual_income=float(row["annual_income"]),
        preferred_category=str(row["preferred_category"]),
        kyc_verified=kyc_bool,
        tx_count=int(row.get("tx_count", 0)),
        total_spend=float(row.get("total_spend", 0.0)),
    )


def _parse_card_row(row: dict) -> CreditCardProduct:
    """Converts a raw REST or CSV dictionary into a typed CreditCardProduct."""
    return CreditCardProduct(
        card_id=str(row["card_id"]),
        card_name=str(row["card_name"]),
        tier=str(row["tier"]),
        min_credit_score=int(row["min_credit_score"]),
        min_annual_income=float(row["min_annual_income"]),
        annual_fee=float(row["annual_fee"]),
        primary_reward_category=str(row["primary_reward_category"]),
        cashback_pct=float(row["cashback_pct"]),
        benefits_summary=str(row["benefits_summary"]),
    )
