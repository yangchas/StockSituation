from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from engine_next.contracts.schema_contracts import (
    ConsumerFieldMap,
    NormalizedSchemaSpec,
    RawSchemaSpec,
    SchemaFieldSpec,
    StorageSchemaSpec,
    get_storage_schema_spec,
)
from engine_next.contracts.source_semantics import SourceSemanticsSpec, get_source_semantics_spec
from engine_next.domain.enums import SourceName


@dataclass(frozen=True)
class ThsHotRankEntry:
    symbol: str
    rank: int
    heat: float | None = None
    name: str = ""
    source: str = "ths_hot_rank"


class ThsHotConnector:
    """Low-frequency THS hot-rank adapter."""

    def __init__(self) -> None:
        self._api = None

    def _get_api(self):
        if self._api is None:
            from ai.API.HotStockAPI import DataSource, HotStockAPI

            self._api = HotStockAPI(DataSource.THS)
        return self._api

    async def fetch_hot_rank(self, top_n: int = 100) -> list[dict[str, Any]]:
        return await self._get_api().get_trending_stocks(top_n=top_n, include_tags=True, include_topic=True)

    def validate_hot_rank(self, items: list[dict[str, Any]]) -> bool:
        return isinstance(items, list) and all(isinstance(item, dict) for item in items)

    def normalize_hot_rank(self, items: list[dict[str, Any]]) -> list[ThsHotRankEntry]:
        normalized: list[ThsHotRankEntry] = []
        for idx, item in enumerate(items, start=1):
            symbol = str(item.get("code") or item.get("stock_code") or "").strip()
            if not symbol:
                continue
            heat_raw = item.get("rate")
            try:
                heat = float(heat_raw) if heat_raw is not None else None
            except (TypeError, ValueError):
                heat = None
            normalized.append(
                ThsHotRankEntry(
                    symbol=symbol[-6:],
                    rank=idx,
                    heat=heat,
                    name=str(item.get("name", "")),
                )
            )
        return normalized

    def raw_schema_spec(self) -> RawSchemaSpec:
        return RawSchemaSpec(
            dataset="hot_rank",
            source="ths_hot_rank",
            payload_type="list[dict[str, Any]]",
            field_specs=(
                SchemaFieldSpec("code", "str"),
                SchemaFieldSpec("name", "str", required=False, nullable=True),
                SchemaFieldSpec("display_order", "int", required=False, nullable=True),
                SchemaFieldSpec("rate", "float", required=False, nullable=True),
                SchemaFieldSpec("concept_tags", "list", required=False, nullable=True),
                SchemaFieldSpec("topic", "dict", required=False, nullable=True),
            ),
            sample_payload=[{"code": "SZ000001", "name": "bank", "display_order": 1, "rate": 93.5}],
            notes=("THS hot rank is low-frequency only and should not be high-frequency polled intraday.",),
        )

    def consumer_field_map(self) -> tuple[ConsumerFieldMap, ...]:
        return (
            ConsumerFieldMap(
                dataset="hot_rank",
                field_name="code/rank/rate",
                consumer_module="ai.API.api",
                consumer_function="get_today_focus_stocks",
                dependency_level="optional",
                fallback_behavior="focus stock helper loses attention ranking but core source chain still works",
            ),
            ConsumerFieldMap(
                dataset="hot_rank",
                field_name="rank",
                consumer_module="engine_next.strategy_skill_layer.stock_profile",
                consumer_function="retail_attention_proxy placeholder",
                dependency_level="optional",
                fallback_behavior="retail attention falls back to neutral proxy",
                notes="Planned low-frequency retail attention proxy only.",
            ),
        )

    def normalized_schema_spec(self) -> NormalizedSchemaSpec:
        return NormalizedSchemaSpec(
            dataset="hot_rank",
            record_type="ThsHotRankEntry",
            key_fields=("symbol",),
            field_specs=(
                SchemaFieldSpec("symbol", "str"),
                SchemaFieldSpec("rank", "int"),
                SchemaFieldSpec("heat", "float", required=False, nullable=True),
                SchemaFieldSpec("name", "str", required=False, nullable=True),
                SchemaFieldSpec("source", "str"),
            ),
        )

    def storage_schema_spec(self) -> StorageSchemaSpec:
        return get_storage_schema_spec("hot_rank")

    def source_semantics_spec(self) -> SourceSemanticsSpec:
        return get_source_semantics_spec("hot_rank", SourceName.THS)

    def to_tdengine_rows(self, items: list[dict[str, Any]], trade_date: str) -> list[dict[str, Any]]:
        spec = self.storage_schema_spec()
        rows = [
            {
                "trade_date": trade_date,
                "symbol": item.symbol,
                "rank": item.rank,
                "heat": item.heat,
                "name": item.name,
                "source": item.source,
            }
            for item in self.normalize_hot_rank(items)
        ]
        return [{field: row.get(field) for field in spec.formal_storage_row_fields} for row in rows]

    def to_redis_view(self, items: list[dict[str, Any]], trade_date: str) -> dict[str, dict[str, Any]]:
        spec = self.storage_schema_spec()
        rows = self.to_tdengine_rows(items, trade_date)
        return {
            str(row["symbol"]): {field: row.get(field) for field in spec.runtime_cache_view_fields}
            for row in rows
            if row.get("symbol")
        }

    def health_check(self) -> dict[str, Any]:
        return {
            "source": "ths_hot_rank",
            "ready": True,
            "note": "THS hot rank is suitable only for low-frequency attention proxy refresh.",
        }
