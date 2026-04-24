from __future__ import annotations

from typing import Any

from engine_next.contracts.schema_contracts import get_storage_schema_spec
from engine_next.domain.models import RedisViewMaterialization


def _default_redis_key(dataset: str, trade_date: str) -> str:
    spec = get_storage_schema_spec(dataset)
    return spec.redis_key_pattern.format(trade_date=trade_date)


class RedisViewBuilder:
    """Builds lightweight Redis view payloads from normalized rows."""

    def materialize(
        self,
        dataset: str,
        trade_date: str,
        rows: list[dict[str, Any]],
    ) -> tuple[RedisViewMaterialization, dict[str, dict[str, Any]]]:
        spec = get_storage_schema_spec(dataset)
        redis_key = _default_redis_key(dataset, trade_date)
        payload: dict[str, dict[str, Any]] = {}
        for row in rows:
            symbol = str(row.get("symbol") or row.get("code") or row.get("plate_name") or "")
            if not symbol:
                continue
            payload[symbol] = {field: row.get(field) for field in spec.runtime_cache_view_fields}
        materialization = RedisViewMaterialization(
            dataset=dataset,
            trade_date=trade_date,
            redis_key=redis_key,
            field_count=len(payload),
            notes=(f"materialized from {len(rows)} normalized rows",),
        )
        return materialization, payload
