"""Connector contracts for engine_next."""

from .baostock_contracts import (
    BaostockAvailabilityResult,
    BaostockDailyKlineRequest,
    check_baostock_daily_kline_availability,
)
from .offline_sync_contracts import (
    ChipResult,
    DdeResult,
    FactorResult,
    GapFillPlan,
    IntegratedSyncResult,
    KlineWindow,
    PhysicalValidationResult,
    ResumeCheckpointState,
    WatermarkSnapshot,
    build_gap_fill_plan,
)
from .schema_contracts import (
    ConsumerFieldMap,
    NormalizedSchemaSpec,
    RawSchemaSpec,
    SchemaFieldSpec,
    StorageSchemaSpec,
    get_required_field_names,
    get_storage_schema_spec,
    trim_record_to_fields,
    validate_record_against_schema,
)
from .source_semantics import SourceSemanticsSpec, get_source_semantics_spec

__all__ = [
    "BaostockAvailabilityResult",
    "BaostockDailyKlineRequest",
    "ChipResult",
    "GapFillPlan",
    "ConsumerFieldMap",
    "DdeResult",
    "FactorResult",
    "IntegratedSyncResult",
    "KlineWindow",
    "NormalizedSchemaSpec",
    "RawSchemaSpec",
    "PhysicalValidationResult",
    "ResumeCheckpointState",
    "SchemaFieldSpec",
    "StorageSchemaSpec",
    "SourceSemanticsSpec",
    "WatermarkSnapshot",
    "build_gap_fill_plan",
    "check_baostock_daily_kline_availability",
    "get_required_field_names",
    "get_source_semantics_spec",
    "get_storage_schema_spec",
    "trim_record_to_fields",
    "validate_record_against_schema",
]
