from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SchemaFieldSpec:
    name: str
    value_type: str
    unit: str = ""
    required: bool = True
    nullable: bool = False
    notes: str = ""


@dataclass(frozen=True)
class ConsumerFieldMap:
    dataset: str
    field_name: str
    consumer_module: str
    consumer_function: str
    dependency_level: str
    fallback_behavior: str
    notes: str = ""


@dataclass(frozen=True)
class RawSchemaSpec:
    dataset: str
    source: str
    payload_type: str
    field_specs: tuple[SchemaFieldSpec, ...]
    sample_payload: dict[str, Any] | list[Any]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class NormalizedSchemaSpec:
    dataset: str
    record_type: str
    key_fields: tuple[str, ...]
    field_specs: tuple[SchemaFieldSpec, ...]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class StorageSchemaSpec:
    dataset: str
    formal_storage_row_fields: tuple[str, ...]
    runtime_cache_view_fields: tuple[str, ...]
    trade_date_field: str | None
    redis_key_pattern: str
    source_quality: str = "unspecified"
    is_formal_default: bool = True
    fetch_cost: str = "medium"
    intended_use: tuple[str, ...] = ()
    runtime_writeback_keys: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


STORAGE_SCHEMA_SPECS: dict[str, StorageSchemaSpec] = {
    "daily_kline": StorageSchemaSpec(
        dataset="daily_kline",
        formal_storage_row_fields=(
            "trade_date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "pct_chg",
            "amount",
            "source",
        ),
        runtime_cache_view_fields=("symbol", "preclose", "close", "pct_chg", "amount", "trade_date", "source"),
        trade_date_field="trade_date",
        redis_key_pattern="cache:kline_ready:{trade_date}",
        source_quality="formal_authoritative",
        is_formal_default=True,
        fetch_cost="medium",
        intended_use=("offline_sync", "factor_dependency", "startup_preload", "recap_truth"),
        notes=(
            "Formal daily kline stays in TDengine.",
            "Runtime Redis view keeps only intraday-consumed fields for pre_close and amount lookups.",
        ),
    ),
    "hot_plates": StorageSchemaSpec(
        dataset="hot_plates",
        formal_storage_row_fields=("trade_date", "plate_name", "rank", "strength", "hot", "change_pct", "net_inflow_yi", "source"),
        runtime_cache_view_fields=("plate_name", "rank", "strength", "hot", "change_pct", "net_inflow_yi", "trade_date", "source"),
        trade_date_field="trade_date",
        redis_key_pattern="cache:hot_plates:{trade_date}",
        source_quality="formal_context",
        is_formal_default=True,
        fetch_cost="medium",
        intended_use=("startup_preload", "plate_persistence", "recap_truth"),
        notes=("Kaipan hot plates are date-bucketed and ranked.",),
    ),
    "yest_limit_pool": StorageSchemaSpec(
        dataset="yest_limit_pool",
        formal_storage_row_fields=(
            "trade_date",
            "symbol",
            "name",
            "lb_days",
            "plate",
            "seal_time",
            "turnover",
            "close_pct",
            "source",
        ),
        runtime_cache_view_fields=("symbol", "name", "lb_days", "plate", "close_pct", "turnover"),
        trade_date_field="trade_date",
        redis_key_pattern="cache:yest_limit_pool:{trade_date}",
        source_quality="formal_delayed",
        is_formal_default=True,
        fetch_cost="medium",
        intended_use=("startup_preload", "ladder_context", "recap_truth"),
        notes=("Yesterday limit-up pool feeds recap, ladder stats, and plate enrichment.",),
    ),
    "ban_reasons": StorageSchemaSpec(
        dataset="ban_reasons",
        formal_storage_row_fields=("trade_date", "symbol", "reason", "group_str", "gnsm", "sclt", "source"),
        runtime_cache_view_fields=("symbol", "reason", "group_str", "gnsm", "sclt"),
        trade_date_field="trade_date",
        redis_key_pattern="cache:ban_reasons:{trade_date}",
        source_quality="formal_enrichment",
        is_formal_default=True,
        fetch_cost="medium",
        intended_use=("plate_refinement", "reason_writeback", "recap_truth"),
        runtime_writeback_keys=("market:stock_plate", "market:stock_reason"),
        notes=("Ban reasons enrich market:stock_reason and plate refinement logic.",),
    ),
    "limit_truth": StorageSchemaSpec(
        dataset="limit_truth",
        formal_storage_row_fields=("trade_date", "symbol", "lb_days", "source"),
        runtime_cache_view_fields=("symbol", "lb_days"),
        trade_date_field="trade_date",
        redis_key_pattern="cache:limit_truth:{trade_date}",
        source_quality="temporary_substitute",
        is_formal_default=False,
        fetch_cost="high",
        intended_use=("recap_truth", "temporary_substitute"),
        notes=("Wencai limit truth is a post-close substitute truth set and should not overwrite later formal truth silently.",),
    ),
    "broken_boards": StorageSchemaSpec(
        dataset="broken_boards",
        formal_storage_row_fields=("trade_date", "symbol", "source"),
        runtime_cache_view_fields=("symbol",),
        trade_date_field="trade_date",
        redis_key_pattern="cache:broken_boards:{trade_date}",
        source_quality="temporary_auxiliary",
        is_formal_default=False,
        fetch_cost="high",
        intended_use=("failed_promotion_typing", "negative_feedback"),
        notes=("Broken board list is lightweight and symbol-only.",),
    ),
    "first_failed": StorageSchemaSpec(
        dataset="first_failed",
        formal_storage_row_fields=("trade_date", "symbol", "source"),
        runtime_cache_view_fields=("symbol",),
        trade_date_field="trade_date",
        redis_key_pattern="cache:first_failed:{trade_date}",
        source_quality="temporary_auxiliary",
        is_formal_default=False,
        fetch_cost="high",
        intended_use=("failed_promotion_typing", "negative_feedback"),
        notes=("First-failed list supports failed promotion typing.",),
    ),
    "hot_rank": StorageSchemaSpec(
        dataset="hot_rank",
        formal_storage_row_fields=("trade_date", "symbol", "rank", "heat", "name", "source"),
        runtime_cache_view_fields=("symbol", "rank", "heat", "name"),
        trade_date_field="trade_date",
        redis_key_pattern="cache:hot_rank:{trade_date}",
        source_quality="auxiliary_attention_proxy",
        is_formal_default=False,
        fetch_cost="high",
        intended_use=("auxiliary_attention_proxy",),
        notes=("THS hot rank is low-frequency attention proxy input.",),
    ),
    "daily_factors": StorageSchemaSpec(
        dataset="daily_factors",
        formal_storage_row_fields=(
            "trade_date",
            "symbol",
            "change_pct_5d",
            "avg_turnover_5d",
            "limit_up_days_5",
            "real_market_cap",
            "avg_cost",
            "bias_20",
            "profit_ratio",
            "vol_ratio",
            "rsi_6",
            "concentration",
            "ma5",
            "ma10",
            "ma20",
            "macd_dif",
            "macd_dea",
            "macd_hist",
            "kdj_k",
            "kdj_d",
            "kdj_j",
            "boll_up",
            "boll_mid",
            "boll_low",
            "t2_lb_days",
            "t2_pct",
            "structure_score_base",
            "shape_platform_ready",
            "shape_breakout_ready",
            "shape_repair_ready",
            "shape_overheat_risk",
            "shape_chip_cleanliness",
            "shape_trend_health",
            "shape_t2_repair_bias",
            "theme_core_base",
            "source",
        ),
        runtime_cache_view_fields=(
            "symbol",
            "change_pct_5d",
            "avg_turnover_5d",
            "limit_up_days_5",
            "real_market_cap",
            "avg_cost",
            "bias_20",
            "profit_ratio",
            "vol_ratio",
            "rsi_6",
            "concentration",
            "ma5",
            "ma10",
            "ma20",
            "macd_dif",
            "macd_dea",
            "macd_hist",
            "kdj_k",
            "kdj_d",
            "kdj_j",
            "boll_up",
            "boll_mid",
            "boll_low",
            "t2_lb_days",
            "t2_pct",
            "structure_score_base",
            "shape_platform_ready",
            "shape_breakout_ready",
            "shape_repair_ready",
            "shape_overheat_risk",
            "shape_chip_cleanliness",
            "shape_trend_health",
            "shape_t2_repair_bias",
            "theme_core_base",
            "trade_date",
            "source",
        ),
        trade_date_field="trade_date",
        redis_key_pattern="cache:stock_extra:{trade_date}",
        source_quality="formal_derived",
        is_formal_default=True,
        fetch_cost="medium",
        intended_use=("startup_preload", "intraday_scoring", "offline_sync"),
        notes=("Stock extra factor cache mirrors the fields consumed by engine_v2 scoring and recap context.",),
    ),
    "chip_peaks": StorageSchemaSpec(
        dataset="chip_peaks",
        formal_storage_row_fields=(
            "trade_date",
            "symbol",
            "peak_price",
            "concentration",
            "dense_area_count",
            "source",
        ),
        runtime_cache_view_fields=(
            "symbol",
            "peak_price",
            "avg_cost",
            "profit_ratio",
            "loss_ratio",
            "concentration",
            "dense_area_count",
            "trade_date",
            "source",
        ),
        trade_date_field="trade_date",
        redis_key_pattern="cache:chip_peaks:{trade_date}",
        source_quality="formal_derived",
        is_formal_default=True,
        fetch_cost="medium",
        intended_use=("startup_preload", "chip_context", "offline_sync"),
        notes=("Chip peak cache stays lightweight and never stores full kline history in Redis.",),
    ),
    "daily_dde": StorageSchemaSpec(
        dataset="daily_dde",
        formal_storage_row_fields=("trade_date", "symbol", "ddje", "ddx", "ddy", "ddz", "source"),
        runtime_cache_view_fields=("symbol", "ddje", "ddx", "ddy", "ddz"),
        trade_date_field="trade_date",
        redis_key_pattern="cache:dde_ready:{trade_date}",
        source_quality="formal_delayed",
        is_formal_default=True,
        fetch_cost="medium",
        intended_use=("startup_preload", "intraday_reference"),
        notes=("DDE remains date-bucketed and is mostly formal storage first.",),
    ),
    "stock_plate_mapping": StorageSchemaSpec(
        dataset="stock_plate_mapping",
        formal_storage_row_fields=("symbol", "plate", "source"),
        runtime_cache_view_fields=("symbol", "plate"),
        trade_date_field=None,
        redis_key_pattern="market:stock_plate",
        source_quality="runtime_enrichment",
        is_formal_default=False,
        fetch_cost="low",
        intended_use=("plate_refinement", "runtime_cache"),
        notes=("Dynamic plate mapping is intraday Redis-first with optional archival copy.",),
    ),
    "auction_snapshot": StorageSchemaSpec(
        dataset="auction_snapshot",
        formal_storage_row_fields=("trade_date", "symbol", "change_pct", "amount", "bid_amount", "source"),
        runtime_cache_view_fields=("symbol", "change_pct", "amount", "bid_amount"),
        trade_date_field="trade_date",
        redis_key_pattern="market:auction:{trade_date}:latest",
        source_quality="runtime_fastpath",
        is_formal_default=False,
        fetch_cost="low",
        intended_use=("auction_fastpath", "intraday_runtime_cache"),
        notes=("Auction snapshot remains Redis-first with optional historical archival.",),
    ),
}


def get_storage_schema_spec(dataset: str) -> StorageSchemaSpec:
    if dataset not in STORAGE_SCHEMA_SPECS:
        raise KeyError(f"No storage schema spec registered for dataset={dataset}")
    return STORAGE_SCHEMA_SPECS[dataset]


def get_required_field_names(field_specs: tuple[SchemaFieldSpec, ...]) -> tuple[str, ...]:
    return tuple(field.name for field in field_specs if field.required and not field.nullable)


def validate_record_against_schema(record: dict[str, Any], field_specs: tuple[SchemaFieldSpec, ...]) -> tuple[bool, tuple[str, ...]]:
    missing: list[str] = []
    for field in field_specs:
        if not field.required:
            continue
        if field.name not in record:
            missing.append(field.name)
            continue
        if record[field.name] is None and not field.nullable:
            missing.append(field.name)
    return (len(missing) == 0, tuple(missing))


def trim_record_to_fields(record: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: record.get(field) for field in fields}
