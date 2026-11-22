"""Dozer REST & Incremental CDC Client with YAML Validator and Offline SQL Engine.

Implements:
- `load_and_validate_dozer_config`: Parses Dozer YAML configs (including `!Postgres` tags)
  and validates connections, sources, streaming SQL (`SELECT ... INTO`), and endpoints.
- `InMemoryDozerBackend`: Parses `seed.sql` and evaluates Dozer streaming SQL views
  (`customer_spending_profiles`, `eligible_card_recommendations`) with `$filter`,
  `$order_by`, `$limit`, `$skip`, and `/count` support.
- `DozerClient`: Unified client for live Dozer REST APIs (`:8080`) with automatic or
  explicit offline fallback.
- `CDCOperation`, `CDCEvent`, `DozerCDCSynchronizer`: Real-time incremental CDC state
  diffing and streaming event dispatch (`INSERT`, `UPDATE`, `DELETE`).
"""

from __future__ import annotations

import ast
import copy
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import requests
import yaml


class DozerConfigValidationError(ValueError):
    """Raised when a Dozer configuration file violates the canonical schema."""


class _DozerYamlLoader(yaml.SafeLoader):
    """Custom PyYAML SafeLoader that supports Dozer enum tags such as `!Postgres`."""


def _construct_dozer_tag(
    loader: yaml.SafeLoader, tag_suffix: str, node: yaml.Node
) -> Any:
    tag_name = f"!{tag_suffix}"
    if isinstance(node, yaml.MappingNode):
        mapping = loader.construct_mapping(node, deep=True)
        mapping["__tag__"] = tag_name
        return mapping
    if isinstance(node, yaml.SequenceNode):
        return {"__tag__": tag_name, "items": loader.construct_sequence(node, deep=True)}
    return {"__tag__": tag_name, "value": loader.construct_scalar(node)}


_DozerYamlLoader.add_multi_constructor("!", _construct_dozer_tag)


def load_and_validate_dozer_config(
    config_path: Union[str, Path]
) -> Dict[str, Any]:
    """Load and validate a `dozer-config.yaml` file against Dozer's canonical schema."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Dozer config file not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        config = yaml.load(fh, Loader=_DozerYamlLoader)

    if not isinstance(config, dict):
        raise DozerConfigValidationError("Dozer configuration root must be a YAML mapping.")

    required_top_keys = ("app_name", "version", "connections", "sources", "sql", "endpoints")
    for key in required_top_keys:
        if key not in config:
            raise DozerConfigValidationError(f"Missing required top-level key: '{key}'")

    if not isinstance(config["app_name"], str) or not config["app_name"].strip():
        raise DozerConfigValidationError("'app_name' must be a non-empty string.")

    connections = config.get("connections")
    if not isinstance(connections, list) or not connections:
        raise DozerConfigValidationError("'connections' must be a non-empty list.")

    connection_names = set()
    for conn in connections:
        if not isinstance(conn, dict) or "name" not in conn or "config" not in conn:
            raise DozerConfigValidationError(
                "Each connection must define 'name' and 'config' (e.g. `config: !Postgres`)."
            )
        conn_cfg = conn["config"]
        if not isinstance(conn_cfg, dict) or not str(conn_cfg.get("__tag__", "")).startswith("!"):
            raise DozerConfigValidationError(
                f"Connection '{conn.get('name')}' must use a valid Dozer YAML tag such as `!Postgres`."
            )
        connection_names.add(conn["name"])

    sources = config.get("sources")
    if not isinstance(sources, list) or not sources:
        raise DozerConfigValidationError("'sources' must be a non-empty list.")

    source_tables = set()
    for src in sources:
        if not isinstance(src, dict):
            raise DozerConfigValidationError("Each source entry must be a mapping.")
        for field in ("name", "table_name", "connection"):
            if field not in src or not src[field]:
                raise DozerConfigValidationError(f"Source is missing required field '{field}'.")
        if src["connection"] not in connection_names:
            raise DozerConfigValidationError(
                f"Source '{src['name']}' references unknown connection '{src['connection']}'."
            )
        source_tables.add(src["table_name"])

    sql_block = config.get("sql")
    if not isinstance(sql_block, str) or "INTO" not in sql_block.upper():
        raise DozerConfigValidationError(
            "'sql' must be a valid SQL string containing at least one `SELECT ... INTO <table>` query."
        )

    into_tables = set(
        re.findall(r"\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql_block, flags=re.IGNORECASE)
    )
    valid_endpoint_tables = source_tables | into_tables

    endpoints = config.get("endpoints")
    if not isinstance(endpoints, list) or not endpoints:
        raise DozerConfigValidationError("'endpoints' must be a non-empty list.")

    for ep in endpoints:
        if not isinstance(ep, dict):
            raise DozerConfigValidationError("Each endpoint entry must be a mapping.")
        if "sql" in ep:
            raise DozerConfigValidationError(
                f"Endpoint '{ep.get('name')}' contains invalid inline 'sql' field; "
                "use top-level 'sql: SELECT ... INTO <table>' and 'table_name' on endpoints."
            )
        for field in ("name", "path", "table_name"):
            if field not in ep or not ep[field]:
                raise DozerConfigValidationError(
                    f"Endpoint '{ep.get('name', '<unnamed>')}' is missing required field '{field}'."
                )
        if ep["table_name"] not in valid_endpoint_tables:
            raise DozerConfigValidationError(
                f"Endpoint '{ep['name']}' references unknown table_name '{ep['table_name']}'. "
                f"Known tables/views: {sorted(valid_endpoint_tables)}"
            )
        index_cfg = ep.get("index")
        if not isinstance(index_cfg, dict) or not index_cfg.get("primary_key"):
            raise DozerConfigValidationError(
                f"Endpoint '{ep['name']}' must declare 'index.primary_key'."
            )

    return config


class CDCOperation(str, Enum):
    """Dozer Change Data Capture (CDC) operation types."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


@dataclass
class CDCEvent:
    """Represents a single real-time CDC record mutation emitted by Dozer."""

    endpoint: str
    operation: CDCOperation
    primary_key: str
    old_record: Optional[Dict[str, Any]] = None
    new_record: Optional[Dict[str, Any]] = None
    seq_no: int = 0


class InMemoryDozerBackend:
    """In-memory Dozer SQL & REST engine backed by `seed.sql` for local/CI execution."""

    ENDPOINT_TO_TABLE: Dict[str, str] = {
        "customer_profiles": "customer_spending_profiles",
        "eligible_cards": "eligible_card_recommendations",
        "credit_card_products": "credit_card_products",
        "documents": "documents",
        "transactions": "transactions",
        "customers": "customers",
    }

    TABLE_PRIMARY_KEYS: Dict[str, Tuple[str, ...]] = {
        "customers": ("customer_id",),
        "transactions": ("txn_id",),
        "credit_card_products": ("card_id",),
        "documents": ("doc_id",),
        "customer_spending_profiles": ("customer_id",),
        "eligible_card_recommendations": ("customer_id", "card_id"),
    }

    NUMERIC_INT_FIELDS = {"credit_score", "min_credit_score", "transaction_count"}
    NUMERIC_FLOAT_FIELDS = {
        "annual_income",
        "min_annual_income",
        "amount",
        "reward_rate_pct",
        "annual_fee",
        "apr_pct",
    }

    def __init__(self, seed_sql_path: Optional[Union[str, Path]] = None) -> None:
        self.tables: Dict[str, List[Dict[str, Any]]] = {
            "customers": [],
            "transactions": [],
            "credit_card_products": [],
            "documents": [],
        }
        default_seed = Path(__file__).resolve().parent / "seed.sql"
        target_seed = Path(seed_sql_path) if seed_sql_path else default_seed
        if target_seed.exists():
            self.load_seed_sql(target_seed)

    def load_seed_sql(self, seed_sql_path: Union[str, Path]) -> None:
        """Parse PostgreSQL `INSERT INTO` statements from `seed.sql`."""
        sql_text = Path(seed_sql_path).read_text(encoding="utf-8")
        clean_lines = [
            line for line in sql_text.splitlines() if not line.strip().startswith("--")
        ]
        cleaned_sql = "\n".join(clean_lines)

        insert_pattern = re.compile(
            r"INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(([^)]+)\)\s*VALUES\s*(.+?);",
            flags=re.IGNORECASE | re.DOTALL,
        )
        for match in insert_pattern.finditer(cleaned_sql):
            table_name = match.group(1).strip()
            columns = [col.strip() for col in match.group(2).split(",")]
            values_blob = match.group(3).strip()
            tuples = self._parse_sql_tuples(values_blob)
            for row_values in tuples:
                if len(row_values) != len(columns):
                    continue
                record: Dict[str, Any] = {}
                for col, val in zip(columns, row_values):
                    record[col] = self._coerce_field(col, val)
                self.upsert_row(table_name, record)

    @staticmethod
    def _parse_sql_tuples(values_blob: str) -> List[List[Any]]:
        """Parse multi-row SQL `VALUES (...), (...)` literals safely."""
        rows: List[List[Any]] = []
        current_chars: List[str] = []
        depth = 0
        in_quote = False
        i = 0
        while i < len(values_blob):
            ch = values_blob[i]
            if ch == "'":
                if in_quote and i + 1 < len(values_blob) and values_blob[i + 1] == "'":
                    current_chars.append("\\'")
                    i += 2
                    continue
                in_quote = not in_quote
                current_chars.append(ch)
            elif not in_quote and ch == "(":
                if depth == 0:
                    current_chars = ["("]
                else:
                    current_chars.append(ch)
                depth += 1
            elif not in_quote and ch == ")":
                depth -= 1
                current_chars.append(")")
                if depth == 0:
                    tuple_literal = "".join(current_chars)
                    parsed = ast.literal_eval(tuple_literal)
                    rows.append(list(parsed))
            else:
                if depth > 0:
                    current_chars.append(ch)
            i += 1
        return rows

    def _coerce_field(self, field: str, value: Any) -> Any:
        if value is None:
            return None
        if field in self.NUMERIC_INT_FIELDS:
            return int(value)
        if field in self.NUMERIC_FLOAT_FIELDS:
            return float(value)
        return value

    def _resolve_table_name(self, endpoint_or_table: str) -> str:
        clean = endpoint_or_table.strip("/")
        return self.ENDPOINT_TO_TABLE.get(clean, clean)

    def _compute_customer_spending_profiles(self) -> List[Dict[str, Any]]:
        """Evaluate Dozer's `customer_spending_profiles` streaming SQL transformation."""
        profiles: List[Dict[str, Any]] = []
        txns = self.tables.get("transactions", [])
        for cust in self.tables.get("customers", []):
            cid = cust["customer_id"]
            cust_txns = [t for t in txns if t.get("customer_id") == cid]
            total_spend = round(sum(float(t.get("amount", 0.0)) for t in cust_txns), 2)
            travel_spend = round(
                sum(
                    float(t.get("amount", 0.0))
                    for t in cust_txns
                    if t.get("category") == "Travel"
                ),
                2,
            )
            dining_spend = round(
                sum(
                    float(t.get("amount", 0.0))
                    for t in cust_txns
                    if t.get("category") == "Dining"
                ),
                2,
            )
            groceries_spend = round(
                sum(
                    float(t.get("amount", 0.0))
                    for t in cust_txns
                    if t.get("category") == "Groceries"
                ),
                2,
            )
            tech_spend = round(
                sum(
                    float(t.get("amount", 0.0))
                    for t in cust_txns
                    if t.get("category") == "Technology"
                ),
                2,
            )
            profiles.append(
                {
                    "customer_id": cid,
                    "full_name": cust["full_name"],
                    "email": cust["email"],
                    "credit_score": int(cust["credit_score"]),
                    "annual_income": float(cust["annual_income"]),
                    "account_tier": cust["account_tier"],
                    "preferred_reward_type": cust["preferred_reward_type"],
                    "transaction_count": len(cust_txns),
                    "total_spend": total_spend,
                    "travel_spend": travel_spend,
                    "dining_spend": dining_spend,
                    "groceries_spend": groceries_spend,
                    "tech_spend": tech_spend,
                }
            )
        return profiles

    def _compute_eligible_card_recommendations(self) -> List[Dict[str, Any]]:
        """Evaluate Dozer's `eligible_card_recommendations` streaming SQL join."""
        recommendations: List[Dict[str, Any]] = []
        cards = self.tables.get("credit_card_products", [])
        for cust in self.tables.get("customers", []):
            score = int(cust["credit_score"])
            income = float(cust["annual_income"])
            for card in cards:
                if score >= int(card["min_credit_score"]) and income >= float(
                    card["min_annual_income"]
                ):
                    recommendations.append(
                        {
                            "customer_id": cust["customer_id"],
                            "full_name": cust["full_name"],
                            "credit_score": score,
                            "annual_income": income,
                            "card_id": card["card_id"],
                            "card_name": card["card_name"],
                            "category_focus": card["category_focus"],
                            "reward_rate_pct": float(card["reward_rate_pct"]),
                            "annual_fee": float(card["annual_fee"]),
                            "apr_pct": float(card["apr_pct"]),
                            "min_credit_score": int(card["min_credit_score"]),
                            "min_annual_income": float(card["min_annual_income"]),
                            "welcome_bonus": card["welcome_bonus"],
                            "perks_summary": card["perks_summary"],
                        }
                    )
        return recommendations

    def get_table_rows(self, endpoint_or_table: str) -> List[Dict[str, Any]]:
        table_name = self._resolve_table_name(endpoint_or_table)
        if table_name == "customer_spending_profiles":
            return self._compute_customer_spending_profiles()
        if table_name == "eligible_card_recommendations":
            return self._compute_eligible_card_recommendations()
        if table_name not in self.tables:
            raise KeyError(f"Unknown Dozer table or endpoint: '{endpoint_or_table}'")
        return [copy.deepcopy(r) for r in self.tables[table_name]]

    def upsert_row(self, endpoint_or_table: str, record: Dict[str, Any]) -> Dict[str, Any]:
        table_name = self._resolve_table_name(endpoint_or_table)
        if table_name not in self.tables:
            raise ValueError(f"Cannot directly mutate derived SQL view '{table_name}'.")
        pk_cols = self.TABLE_PRIMARY_KEYS[table_name]
        coerced = {k: self._coerce_field(k, v) for k, v in record.items()}
        target_key = tuple(str(coerced[col]) for col in pk_cols)

        rows = self.tables[table_name]
        for idx, existing in enumerate(rows):
            existing_key = tuple(str(existing[col]) for col in pk_cols)
            if existing_key == target_key:
                updated = {**existing, **coerced}
                rows[idx] = updated
                return copy.deepcopy(updated)
        rows.append(coerced)
        return copy.deepcopy(coerced)

    def delete_row(
        self, endpoint_or_table: str, primary_key: Union[str, Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        table_name = self._resolve_table_name(endpoint_or_table)
        if table_name not in self.tables:
            raise ValueError(f"Cannot directly delete from derived SQL view '{table_name}'.")
        pk_cols = self.TABLE_PRIMARY_KEYS[table_name]
        if isinstance(primary_key, dict):
            target_key = tuple(str(primary_key[col]) for col in pk_cols)
        else:
            target_key = (str(primary_key),)

        rows = self.tables[table_name]
        for idx, existing in enumerate(rows):
            existing_key = tuple(str(existing[col]) for col in pk_cols)
            if existing_key == target_key:
                return rows.pop(idx)
        return None

    def apply_cdc_event(self, event: CDCEvent) -> None:
        """Apply an INSERT, UPDATE, or DELETE CDC event to the underlying source table."""
        table_name = self._resolve_table_name(event.endpoint)
        if event.operation in (CDCOperation.INSERT, CDCOperation.UPDATE):
            if not event.new_record:
                raise ValueError(f"CDC {event.operation} requires 'new_record'.")
            self.upsert_row(table_name, event.new_record)
        elif event.operation == CDCOperation.DELETE:
            self.delete_row(table_name, event.primary_key)

    @staticmethod
    def _matches_filter(row: Dict[str, Any], filter_spec: Optional[Dict[str, Any]]) -> bool:
        if not filter_spec:
            return True
        for field, condition in filter_spec.items():
            val = row.get(field)
            if isinstance(condition, dict):
                for op, target in condition.items():
                    if op == "$eq" and val != target:
                        return False
                    if op == "$ne" and val == target:
                        return False
                    if op == "$gt" and (val is None or not (val > target)):
                        return False
                    if op == "$gte" and (val is None or not (val >= target)):
                        return False
                    if op == "$lt" and (val is None or not (val < target)):
                        return False
                    if op == "$lte" and (val is None or not (val <= target)):
                        return False
                    if op == "$in" and val not in target:
                        return False
            else:
                if val != condition:
                    return False
        return True

    def query(
        self,
        endpoint: str,
        filter_spec: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        skip: int = 0,
        order_by: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        rows = [
            row
            for row in self.get_table_rows(endpoint)
            if self._matches_filter(row, filter_spec)
        ]
        if order_by:
            for field, direction in reversed(list(order_by.items())):
                reverse = str(direction).lower() == "desc"
                rows.sort(key=lambda r: (r.get(field) is None, r.get(field)), reverse=reverse)
        return rows[skip : skip + limit]

    def count(
        self, endpoint: str, filter_spec: Optional[Dict[str, Any]] = None
    ) -> int:
        return len(
            [
                row
                for row in self.get_table_rows(endpoint)
                if self._matches_filter(row, filter_spec)
            ]
        )


class DozerClient:
    """Client for Dozer's low-latency REST API (`:8080`) with offline SQL backend support."""

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        offline: bool = False,
        fallback_to_in_memory: bool = True,
        seed_sql_path: Optional[Union[str, Path]] = None,
        timeout: float = 3.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.offline = offline
        self.fallback_to_in_memory = fallback_to_in_memory
        self.timeout = timeout
        self.backend = InMemoryDozerBackend(seed_sql_path=seed_sql_path)

    def query(
        self,
        endpoint: str,
        filter_spec: Optional[Dict[str, Any]] = None,
        limit: int = 100,
        skip: int = 0,
        order_by: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Execute a Dozer `POST /<endpoint>/query` request."""
        clean_ep = endpoint.strip("/")
        payload: Dict[str, Any] = {"$limit": limit, "$skip": skip}
        if filter_spec:
            payload["$filter"] = filter_spec
        if order_by:
            payload["$order_by"] = order_by

        if not self.offline:
            try:
                response = requests.post(
                    f"{self.base_url}/{clean_ep}/query",
                    json=payload,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                data = response.json()
                if isinstance(data, list):
                    return data
            except requests.RequestException:
                if not self.fallback_to_in_memory:
                    raise

        return self.backend.query(
            clean_ep,
            filter_spec=filter_spec,
            limit=limit,
            skip=skip,
            order_by=order_by,
        )

    def count(
        self,
        endpoint: str,
        filter_spec: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Execute a Dozer `POST /<endpoint>/count` request."""
        clean_ep = endpoint.strip("/")
        payload: Dict[str, Any] = {}
        if filter_spec:
            payload["$filter"] = filter_spec

        if not self.offline:
            try:
                response = requests.post(
                    f"{self.base_url}/{clean_ep}/count",
                    json=payload,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                data = response.json()
                if isinstance(data, dict) and "count" in data:
                    return int(data["count"])
                if isinstance(data, int):
                    return data
            except requests.RequestException:
                if not self.fallback_to_in_memory:
                    raise

        return self.backend.count(clean_ep, filter_spec=filter_spec)

    def get_customer_profile(self, customer_id: str) -> Optional[Dict[str, Any]]:
        """Fetch real-time aggregated customer profile from `/customer_profiles`."""
        results = self.query(
            "/customer_profiles",
            filter_spec={"customer_id": customer_id},
            limit=1,
        )
        return results[0] if results else None

    def get_eligible_cards(self, customer_id: str) -> List[Dict[str, Any]]:
        """Fetch real-time SQL pre-filtered eligible cards from `/eligible_cards`."""
        return self.query(
            "/eligible_cards",
            filter_spec={"customer_id": customer_id},
            order_by={"reward_rate_pct": "desc"},
        )

    def get_customer_transactions(self, customer_id: str) -> List[Dict[str, Any]]:
        """Fetch recent customer transactions from `/transactions`."""
        return self.query(
            "/transactions",
            filter_spec={"customer_id": customer_id},
            order_by={"txn_timestamp": "desc"},
        )

    def get_documents(
        self, filter_spec: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Fetch product disclosure & policy documents from `/documents`."""
        return self.query(
            "/documents",
            filter_spec=filter_spec,
            order_by={"doc_id": "asc"},
        )

    def apply_cdc_event(self, event: CDCEvent) -> CDCEvent:
        """Apply a streaming CDC mutation to the Dozer backend."""
        self.backend.apply_cdc_event(event)
        return event


class DozerCDCSynchronizer:
    """Synchronizes Dozer endpoint changes (INSERT, UPDATE, DELETE) into downstream listeners."""

    def __init__(
        self,
        dozer_client: DozerClient,
        endpoint: str = "/documents",
        primary_key_fields: Sequence[str] = ("doc_id",),
    ) -> None:
        self.dozer_client = dozer_client
        self.endpoint = "/" + endpoint.strip("/")
        self.primary_key_fields = tuple(primary_key_fields)
        self._snapshot: Dict[str, Dict[str, Any]] = {}
        self._seq_no: int = 0
        self._listeners: List[Callable[[CDCEvent], None]] = []

    def add_listener(self, callback: Callable[[CDCEvent], None]) -> None:
        """Register a listener callback invoked on every CDC event."""
        self._listeners.append(callback)

    def _extract_pk(self, record: Dict[str, Any]) -> str:
        return ":".join(str(record[field]) for field in self.primary_key_fields)

    def _emit(self, event: CDCEvent) -> None:
        for listener in self._listeners:
            listener(event)

    def sync_once(self) -> List[CDCEvent]:
        """Poll the Dozer endpoint, compute incremental diffs, and emit CDC events."""
        current_rows = self.dozer_client.query(self.endpoint, limit=1000)
        current_map: Dict[str, Dict[str, Any]] = {
            self._extract_pk(row): copy.deepcopy(row) for row in current_rows
        }

        emitted: List[CDCEvent] = []

        # Detect INSERT and UPDATE operations
        for pk, row in current_map.items():
            if pk not in self._snapshot:
                self._seq_no += 1
                event = CDCEvent(
                    endpoint=self.endpoint,
                    operation=CDCOperation.INSERT,
                    primary_key=pk,
                    old_record=None,
                    new_record=copy.deepcopy(row),
                    seq_no=self._seq_no,
                )
                self._snapshot[pk] = copy.deepcopy(row)
                emitted.append(event)
                self._emit(event)
            elif self._snapshot[pk] != row:
                self._seq_no += 1
                old_row = copy.deepcopy(self._snapshot[pk])
                event = CDCEvent(
                    endpoint=self.endpoint,
                    operation=CDCOperation.UPDATE,
                    primary_key=pk,
                    old_record=old_row,
                    new_record=copy.deepcopy(row),
                    seq_no=self._seq_no,
                )
                self._snapshot[pk] = copy.deepcopy(row)
                emitted.append(event)
                self._emit(event)

        # Detect DELETE operations
        deleted_pks = [pk for pk in self._snapshot if pk not in current_map]
        for pk in deleted_pks:
            self._seq_no += 1
            old_row = copy.deepcopy(self._snapshot.pop(pk))
            event = CDCEvent(
                endpoint=self.endpoint,
                operation=CDCOperation.DELETE,
                primary_key=pk,
                old_record=old_row,
                new_record=None,
                seq_no=self._seq_no,
            )
            emitted.append(event)
            self._emit(event)

        return emitted

    def push_cdc_event(self, event: CDCEvent) -> CDCEvent:
        """Directly apply a real-time CDC event (from Dozer gRPC stream or test) and notify listeners."""
        self.dozer_client.apply_cdc_event(event)
        self._seq_no += 1
        event.seq_no = self._seq_no

        if event.endpoint.strip("/") == self.endpoint.strip("/"):
            if event.operation in (CDCOperation.INSERT, CDCOperation.UPDATE) and event.new_record:
                self._snapshot[event.primary_key] = copy.deepcopy(event.new_record)
            elif event.operation == CDCOperation.DELETE:
                self._snapshot.pop(event.primary_key, None)

        self._emit(event)
        return event
