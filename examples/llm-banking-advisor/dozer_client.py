"""Dozer API Client with automatic live REST querying and offline fallback."""

from __future__ import annotations

import csv
import json
import logging
import urllib.request
import urllib.error
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from models import CardProduct, CustomerProfile, Transaction

logger = logging.getLogger("dozer_client")
ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"


class DozerClient:
    """Client for Dozer real-time data movement and query engine.

    Supports dual-mode operation:
    1. Live Mode: Connects to a running Dozer REST API server (default: http://localhost:8080).
    2. Offline Mode: Emulates Dozer's real-time SQL aggregation directly from local storage,
       allowing dependency-free local testing and continuous integration without the Dozer binary.
    """

    def __init__(self, base_url: str = "http://localhost:8080", timeout_sec: float = 2.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self._is_live: Optional[bool] = None

    def is_live(self) -> bool:
        """Check if Dozer REST endpoint is responsive."""
        if self._is_live is not None:
            return self._is_live
        try:
            req = urllib.request.Request(f"{self.base_url}/card_catalog")
            with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                self._is_live = resp.status == 200
        except Exception:
            self._is_live = False
        return self._is_live

    def get_card_catalog(self) -> List[CardProduct]:
        """Fetch all card products either from Dozer API or local storage."""
        if self.is_live():
            try:
                req = urllib.request.Request(f"{self.base_url}/card_catalog")
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    if resp.status == 200:
                        records = json.loads(resp.read().decode("utf-8"))
                        return [
                            CardProduct(
                                product_id=r["product_id"],
                                name=r["name"],
                                card_tier=r.get("card_tier", "Standard"),
                                annual_fee_usd=float(r["annual_fee_usd"]),
                                min_credit_score=int(r["min_credit_score"]),
                                min_annual_income_usd=float(r["min_annual_income_usd"]),
                                rewards_focus=r["rewards_focus"],
                                intro_bonus=r.get("intro_bonus", "None"),
                                apr_rate=r.get("apr_rate", "N/A"),
                                perks_summary=r.get("perks_summary", ""),
                                target_segment=r.get("target_segment", "General"),
                            )
                            for r in records
                        ]
            except Exception as e:
                logger.warning(f"Dozer live fetch failed ({e}); falling back to local storage.")

        # Offline emulation
        return self._load_local_products()

    def get_customer_profile(self, customer_id: str) -> CustomerProfile:
        """Fetch customer profile either from Dozer streaming SQL endpoint or local storage."""
        if self.is_live():
            try:
                query_payload = json.dumps({
                    "$filter": {"customer_id": customer_id}
                }).encode("utf-8")
                req = urllib.request.Request(
                    f"{self.base_url}/customer_profile/query",
                    data=query_payload,
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=self.timeout_sec) as resp:
                    if resp.status == 200:
                        data = json.loads(resp.read().decode("utf-8"))
                        if data:
                            r = data[0]
                            local_profile = self._build_local_profile(customer_id)
                            return CustomerProfile(
                                customer_id=r["customer_id"],
                                first_name=r.get("first_name", local_profile.first_name),
                                last_name=r.get("last_name", local_profile.last_name),
                                age=int(r.get("age", local_profile.age)),
                                credit_score=int(r.get("credit_score", local_profile.credit_score)),
                                income_band=r.get("income_band", local_profile.income_band),
                                annual_income_usd=float(r.get("annual_income_usd", local_profile.annual_income_usd)),
                                segment=r.get("segment", local_profile.segment),
                                primary_goals=r.get("primary_goals", local_profile.primary_goals),
                                risk_tolerance=r.get("risk_tolerance", local_profile.risk_tolerance),
                                total_spend_usd=float(r.get("total_spend_usd", local_profile.total_spend_usd)),
                                transaction_count=int(r.get("transaction_count", local_profile.transaction_count)),
                                max_single_transaction_usd=float(r.get("max_single_transaction_usd", local_profile.max_single_transaction_usd)),
                                spend_by_category=local_profile.spend_by_category,
                                top_categories=local_profile.top_categories,
                            )
            except Exception as e:
                logger.warning(f"Dozer query failed ({e}); falling back to local emulation.")

        return self._build_local_profile(customer_id)

    def _load_local_products(self) -> List[CardProduct]:
        csv_path = DATA_DIR / "credit_card_products" / "credit_card_products.csv"
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return [
                CardProduct(
                    product_id=r["product_id"],
                    name=r["name"],
                    card_tier=r.get("card_tier", "Standard"),
                    annual_fee_usd=float(r["annual_fee_usd"]),
                    min_credit_score=int(r["min_credit_score"]),
                    min_annual_income_usd=float(r["min_annual_income_usd"]),
                    rewards_focus=r["rewards_focus"],
                    intro_bonus=r.get("intro_bonus", "None"),
                    apr_rate=r.get("apr_rate", "N/A"),
                    perks_summary=r.get("perks_summary", ""),
                    target_segment=r.get("target_segment", "General"),
                )
                for r in reader
            ]

    def _build_local_profile(self, customer_id: str) -> CustomerProfile:
        cust_path = DATA_DIR / "customer_profiles" / "customer_profiles.csv"
        txn_path = DATA_DIR / "transactions" / "transactions.csv"

        target_cust: Optional[Dict[str, str]] = None
        with open(cust_path, mode="r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["customer_id"] == customer_id:
                    target_cust = row
                    break

        if not target_cust:
            raise KeyError(f"Customer ID '{customer_id}' not found in profiles dataset.")

        spend_by_cat: Dict[str, float] = defaultdict(float)
        txns: List[Transaction] = []
        with open(txn_path, mode="r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["customer_id"] == customer_id:
                    amt = float(row["amount_usd"])
                    cat = row["merchant_category"].strip().lower()
                    spend_by_cat[cat] += amt
                    txns.append(
                        Transaction(
                            transaction_id=row["transaction_id"],
                            customer_id=row["customer_id"],
                            merchant_name=row["merchant_name"],
                            merchant_category=cat,
                            amount_usd=amt,
                            txn_date=row["txn_date"],
                        )
                    )

        total_spend = sum(spend_by_cat.values())
        max_txn = max([t.amount_usd for t in txns], default=0.0)
        top_cats = [cat for cat, _ in Counter(spend_by_cat).most_common(3)]

        return CustomerProfile(
            customer_id=customer_id,
            first_name=target_cust["first_name"],
            last_name=target_cust["last_name"],
            age=int(target_cust["age"]),
            credit_score=int(target_cust["credit_score"]),
            income_band=target_cust["income_band"],
            annual_income_usd=float(target_cust["annual_income_usd"]),
            segment=target_cust["segment"],
            primary_goals=target_cust["primary_goals"],
            risk_tolerance=target_cust["risk_tolerance"],
            total_spend_usd=round(total_spend, 2),
            transaction_count=len(txns),
            max_single_transaction_usd=round(max_txn, 2),
            spend_by_category=dict(spend_by_cat),
            top_categories=top_cats,
        )
