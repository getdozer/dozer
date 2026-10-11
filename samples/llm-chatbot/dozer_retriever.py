"""
Dozer Customer Profile Retriever & LangChain Tool Integration.

Queries Dozer's unified customer profile endpoint to supply real-time
financial context (spending patterns, credit score, demographics) to LangChain.
"""

from __future__ import annotations

import csv
import logging
import os
from typing import Any, Dict, Optional

import requests
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class CustomerProfile(BaseModel):
    customer_id: str
    name: str
    age: int
    occupation: str
    annual_income: float
    credit_score: int
    loyalty_tier: str
    total_transactions: int
    total_spend_90d: float
    travel_spend_90d: float
    dining_spend_90d: float
    grocery_spend_90d: float
    business_spend_90d: float

    def format_as_context(self) -> str:
        """Serializes profile attributes into structured context for LLM prompt."""
        return (
            f"--- REAL-TIME CUSTOMER PROFILE (Dozer Unified View) ---\n"
            f"Customer ID: {self.customer_id}\n"
            f"Name: {self.name}\n"
            f"Age: {self.age} | Occupation: {self.occupation}\n"
            f"Annual Income: ${self.annual_income:,.2f} | Credit Score: {self.credit_score} | Loyalty Tier: {self.loyalty_tier}\n"
            f"Total Spend (Last 90 Days): ${self.total_spend_90d:,.2f} across {self.total_transactions} transactions\n"
            f"  - Travel Spend: ${self.travel_spend_90d:,.2f}\n"
            f"  - Dining Spend: ${self.dining_spend_90d:,.2f}\n"
            f"  - Grocery Spend: ${self.grocery_spend_90d:,.2f}\n"
            f"  - Business Spend: ${self.business_spend_90d:,.2f}\n"
            f"--------------------------------------------------------"
        )


class DozerClient:
    """Client for Dozer REST API with fallback to local CSV data computation."""

    def __init__(self, base_url: Optional[str] = None):
        self.base_url = (base_url or os.getenv("DOZER_API_URL", "http://localhost:8080")).rstrip("/")
        self.endpoint_url = f"{self.base_url}/customer_profiles"

    def get_profile(self, customer_id: str) -> Optional[CustomerProfile]:
        """Fetch customer profile from Dozer endpoint or fallback locally."""
        try:
            # Query Dozer endpoint with filter for customer_id
            response = requests.get(
                self.endpoint_url,
                params={"filter": f'{{"customer_id": "{customer_id}"}}'},
                timeout=3.0,
            )
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and len(data) > 0:
                    return CustomerProfile(**data[0])
                if isinstance(data, dict) and "customer_id" in data:
                    return CustomerProfile(**data)
            logger.warning(
                "Dozer endpoint returned status %s. Falling back to local data.",
                response.status_code,
            )
        except Exception as e:
            logger.warning("Could not reach Dozer at %s (%s). Using local profile calculation.", self.endpoint_url, e)

        return self._compute_profile_locally(customer_id)

    def _compute_profile_locally(self, customer_id: str) -> Optional[CustomerProfile]:
        """
        Emulates Dozer SQL aggregation from CSV files if Dozer service is not running.
        Ensures development/testing runs seamlessly.
        """
        data_dir = os.path.join(os.path.dirname(__file__), "data")
        cust_file = os.path.join(data_dir, "customers.csv")
        txn_file = os.path.join(data_dir, "transactions.csv")

        if not os.path.exists(cust_file) or not os.path.exists(txn_file):
            return None

        customer_row = None
        with open(cust_file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["customer_id"] == customer_id:
                    customer_row = row
                    break

        if not customer_row:
            return None

        total_txns = 0
        total_spend = 0.0
        travel_spend = 0.0
        dining_spend = 0.0
        grocery_spend = 0.0
        biz_spend = 0.0

        with open(txn_file, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row["customer_id"] == customer_id:
                    amt = float(row["amount"])
                    cat = row["category"].lower()
                    total_txns += 1
                    total_spend += amt
                    if cat == "travel":
                        travel_spend += amt
                    elif cat == "dining":
                        dining_spend += amt
                    elif cat == "groceries":
                        grocery_spend += amt
                    elif cat in ("business_supplies", "advertising"):
                        biz_spend += amt

        return CustomerProfile(
            customer_id=customer_row["customer_id"],
            name=customer_row["name"],
            age=int(customer_row["age"]),
            occupation=customer_row["occupation"],
            annual_income=float(customer_row["annual_income"]),
            credit_score=int(customer_row["credit_score"]),
            loyalty_tier=customer_row["loyalty_tier"],
            total_transactions=total_txns,
            total_spend_90d=total_spend,
            travel_spend_90d=travel_spend,
            dining_spend_90d=dining_spend,
            grocery_spend_90d=grocery_spend,
            business_spend_90d=biz_spend,
        )
