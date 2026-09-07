"""Optional reader for the ClickHouse sink path."""

from __future__ import annotations

import os
from typing import Any


def fetch_profile(customer_id: str) -> dict[str, Any] | None:
    host = os.getenv("CLICKHOUSE_HOST", "localhost")
    port = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
    try:
        import clickhouse_connect
    except ImportError:
        return None

    try:
        client = clickhouse_connect.get_client(host=host, port=port)
        rows = client.query(
            "SELECT customer_id, segment, income_band, goals, risk_tolerance, total_spend "
            "FROM customer_profile WHERE customer_id = {cid:String}",
            parameters={"cid": customer_id},
        ).named_results()
        row = next(iter(rows), None)
        return dict(row) if row else None
    except Exception:
        return None
