from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
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
class WencaiQuerySpec:
    query: str
    max_stocks: int = 200


@dataclass(frozen=True)
class WencaiLimitTruthEntry:
    symbol: str
    lb_days: int | None
    source: str = "wencai"


@dataclass(frozen=True)
class WencaiSymbolEntry:
    symbol: str
    source: str = "wencai"


class WencaiConnector:
    """Wencai wrapper on top of UnifiedMarketDataFetcher."""

    def __init__(self, wencai_cookie: str | None = None) -> None:
        self._wencai_cookie = wencai_cookie
        self._fetcher = None

    def _get_fetcher(self):
        if self._fetcher is None:
            from ai.API.api import UnifiedMarketDataFetcher

            kwargs: dict[str, Any] = {}
            if self._wencai_cookie:
                kwargs["wencai_cookie"] = self._wencai_cookie
            self._fetcher = UnifiedMarketDataFetcher(**kwargs)
        return self._fetcher

    async def fetch_dataframe(self, spec: WencaiQuerySpec) -> pd.DataFrame:
        return await self._get_fetcher().get_wencai_data(spec.query, max_stocks=spec.max_stocks)

    async def fetch_broken_boards(self, max_stocks: int = 100) -> list[str]:
        return await self._get_fetcher().get_wencai_broken_boards(max_stocks=max_stocks)

    async def fetch_first_failed(self, max_stocks: int = 100) -> list[str]:
        return await self._get_fetcher().get_wencai_first_failed(max_stocks=max_stocks)

    async def fetch_limitup_with_lb_days(self, max_stocks: int = 500) -> pd.DataFrame:
        return await self._get_fetcher().get_wencai_limitup_with_lb_days(max_stocks=max_stocks)

    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        return isinstance(df, pd.DataFrame) and not df.empty

    def normalize_limitup_with_lb_days(self, df: pd.DataFrame) -> list[WencaiLimitTruthEntry]:
        if not self.validate_dataframe(df):
            return []
        normalized: list[WencaiLimitTruthEntry] = []
        for _, row in df.iterrows():
            code6 = str(row.get("code6", "")).strip()
            if not code6:
                continue
            lb_days_raw = row.get("lb_days")
            lb_days = int(lb_days_raw) if pd.notna(lb_days_raw) else None
            normalized.append(WencaiLimitTruthEntry(symbol=code6, lb_days=lb_days))
        return normalized

    def normalize_symbol_list(self, symbols: list[str]) -> list[WencaiSymbolEntry]:
        normalized: list[WencaiSymbolEntry] = []
        for symbol in symbols:
            text = str(symbol).strip().upper()
            if not text:
                continue
            text = text.replace("SZ", "").replace("SH", "").replace(".", "")
            code6 = text[-6:]
            if len(code6) == 6 and code6.isdigit():
                normalized.append(WencaiSymbolEntry(symbol=code6))
        return normalized

    def raw_schema_spec(self, dataset: str) -> RawSchemaSpec:
        if dataset == "limit_truth":
            return RawSchemaSpec(
                dataset=dataset,
                source="wencai",
                payload_type="pandas.DataFrame",
                field_specs=(
                    SchemaFieldSpec("formatted_code", "str", required=False, nullable=True),
                    SchemaFieldSpec("code6", "str"),
                    SchemaFieldSpec("lb_days", "int", required=False, nullable=True),
                ),
                sample_payload={"formatted_code": "000001.SZ", "code6": "000001", "lb_days": 2},
                notes=("Wencai truth keeps extra query columns, but code6 and lb_days are the stable consumer core.",),
            )
        if dataset == "broken_boards":
            return RawSchemaSpec(
                dataset=dataset,
                source="wencai",
                payload_type="list[str]",
                field_specs=(SchemaFieldSpec("symbol", "str"),),
                sample_payload=["000001.SZ", "600000.SH"],
                notes=("Broken board query is high-cost and symbol-only in current engine_v2 usage.",),
            )
        if dataset == "first_failed":
            return RawSchemaSpec(
                dataset=dataset,
                source="wencai",
                payload_type="list[str]",
                field_specs=(SchemaFieldSpec("symbol", "str"),),
                sample_payload=["000001.SZ", "600000.SH"],
                notes=("First-failed set supports failed promotion typing.",),
            )
        raise ValueError(f"Unsupported Wencai dataset={dataset}")

    def consumer_field_map(self, dataset: str) -> tuple[ConsumerFieldMap, ...]:
        if dataset == "limit_truth":
            return (
                ConsumerFieldMap(
                    dataset=dataset,
                    field_name="code6/lb_days",
                    consumer_module="engine_v2.v2_recap_engine_final",
                    consumer_function="run_audit",
                    dependency_level="required",
                    fallback_behavior="today_bans map remains empty and ladder success stats collapse",
                ),
            )
        if dataset == "broken_boards":
            return (
                ConsumerFieldMap(
                    dataset=dataset,
                    field_name="symbol",
                    consumer_module="engine_v2.ai.API.api",
                    consumer_function="get_wencai_broken_boards",
                    dependency_level="optional",
                    fallback_behavior="failed promotion typing loses one negative-feedback signal",
                ),
            )
        if dataset == "first_failed":
            return (
                ConsumerFieldMap(
                    dataset=dataset,
                    field_name="symbol",
                    consumer_module="engine_v2.ai.API.api",
                    consumer_function="get_wencai_first_failed",
                    dependency_level="optional",
                    fallback_behavior="first-failed set unavailable; use weaker yday_broken_board proxy",
                ),
            )
        raise ValueError(f"Unsupported Wencai dataset={dataset}")

    def normalized_schema_spec(self, dataset: str) -> NormalizedSchemaSpec:
        if dataset == "limit_truth":
            return NormalizedSchemaSpec(
                dataset=dataset,
                record_type="WencaiLimitTruthEntry",
                key_fields=("symbol",),
                field_specs=(
                    SchemaFieldSpec("symbol", "str"),
                    SchemaFieldSpec("lb_days", "int", required=False, nullable=True),
                    SchemaFieldSpec("source", "str"),
                ),
            )
        if dataset in {"broken_boards", "first_failed"}:
            return NormalizedSchemaSpec(
                dataset=dataset,
                record_type="WencaiSymbolEntry",
                key_fields=("symbol",),
                field_specs=(
                    SchemaFieldSpec("symbol", "str"),
                    SchemaFieldSpec("source", "str"),
                ),
            )
        raise ValueError(f"Unsupported Wencai dataset={dataset}")

    def storage_schema_spec(self, dataset: str) -> StorageSchemaSpec:
        return get_storage_schema_spec(dataset)

    def source_semantics_spec(self, dataset: str) -> SourceSemanticsSpec:
        return get_source_semantics_spec(dataset, SourceName.WENCAI)

    def to_tdengine_rows(self, dataset: str, payload: Any, trade_date: str) -> list[dict[str, Any]]:
        spec = self.storage_schema_spec(dataset)
        if dataset == "limit_truth":
            rows = [
                {"trade_date": trade_date, "symbol": item.symbol, "lb_days": item.lb_days, "source": item.source}
                for item in self.normalize_limitup_with_lb_days(payload)
            ]
        elif dataset in {"broken_boards", "first_failed"}:
            rows = [
                {"trade_date": trade_date, "symbol": item.symbol, "source": item.source}
                for item in self.normalize_symbol_list(payload)
            ]
        else:
            raise ValueError(f"Unsupported Wencai dataset={dataset}")
        return [{field: row.get(field) for field in spec.formal_storage_row_fields} for row in rows]

    def to_redis_view(self, dataset: str, payload: Any, trade_date: str) -> dict[str, dict[str, Any]]:
        spec = self.storage_schema_spec(dataset)
        rows = self.to_tdengine_rows(dataset, payload, trade_date)
        return {
            str(row["symbol"]): {field: row.get(field) for field in spec.runtime_cache_view_fields}
            for row in rows
            if row.get("symbol")
        }

    def health_check(self) -> dict[str, Any]:
        return {
            "source": "wencai",
            "ready": True,
            "note": "High-cost semantic source. Real fetch should run on server and under query budget.",
        }
