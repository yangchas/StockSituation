from __future__ import annotations

from typing import Any

from engine_next.contracts.schema_contracts import get_storage_schema_spec
from engine_next.domain.models import PersistenceWritePlan
from engine_next.source_policies.intraday_network_policy import find_storage_rule


def _build_bucket_key(dataset: str, trade_date: str | None, symbol: str | None, bucket_by: str) -> str:
    if bucket_by == "trade_date":
        return f"{dataset}:{trade_date or 'unknown'}"
    if bucket_by == "trade_date+tag":
        return f"{dataset}:{trade_date or 'unknown'}:latest"
    if bucket_by == "symbol":
        return f"{dataset}:{symbol or 'all'}"
    return f"{dataset}:{trade_date or symbol or 'default'}"


class TdenginePersistenceAdapter:
    """
    Storage planner for formal datasets.

    This adapter builds write plans first. Real TDengine client integration
    can later consume the returned plans without changing strategy logic.
    """

    def build_write_plan(
        self,
        dataset: str,
        rows: list[dict[str, Any]],
        trade_date: str | None = None,
        symbol: str | None = None,
    ) -> PersistenceWritePlan:
        rule = find_storage_rule(dataset)
        if rule is None:
            raise ValueError(f"No storage rule registered for dataset={dataset}")
        bucket_key = _build_bucket_key(dataset, trade_date, symbol, rule.bucket_by)
        return PersistenceWritePlan(
            dataset=dataset,
            primary_storage=rule.primary_storage.value,
            secondary_storage=rule.secondary_storage.value if rule.secondary_storage else None,
            bucket_key=bucket_key,
            row_count=len(rows),
            notes=(rule.notes,),
        )

    def prepare_rows(self, rows: list[dict[str, Any]], source: str) -> list[dict[str, Any]]:
        prepared: list[dict[str, Any]] = []
        for row in rows:
            normalized = dict(row)
            normalized.setdefault("source", source)
            prepared.append(normalized)
        return prepared

    def trim_formal_rows(self, dataset: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        fields = get_storage_schema_spec(dataset).formal_storage_row_fields
        return [{field: row.get(field) for field in fields} for row in rows]
