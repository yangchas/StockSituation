from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

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
from engine_next.runtime.plate_mapping_registry import build_runtime_writebacks_from_reasons


@dataclass(frozen=True)
class KaipanHotPlate:
    name: str
    rank: int
    strength: float | None = None
    change_pct: float | None = None
    net_inflow_yi: float | None = None
    raw: dict[str, Any] | tuple[Any, ...] | list[Any] | None = None


@dataclass(frozen=True)
class KaipanBanReason:
    symbol: str
    reason: str
    group_str: str = ""
    gnsm: str = ""


@dataclass(frozen=True)
class KaipanBanPoolEntry:
    symbol: str
    name: str
    lb_days: int
    plate: str
    seal_time: str
    turnover: float
    close_pct: float


class KaipanConnector:
    """Kaipan adapter built on top of ai/API/StockAnalyzer.py."""

    def __init__(self) -> None:
        self._analyzer = None

    def _get_analyzer(self):
        if self._analyzer is None:
            from ai.API.StockAnalyzer import StockAnalyzer

            self._analyzer = StockAnalyzer()
        return self._analyzer

    def fetch_hot_plates(self, trade_date: str, today_mode: bool = False) -> list[KaipanHotPlate]:
        analyzer = self._get_analyzer()
        if today_mode:
            result = analyzer._call_api("getHisPlates", "")
        else:
            result = analyzer.get_his_plates(trade_date)
        plates: list[KaipanHotPlate] = []
        if not result:
            return plates

        items = result.get("list") or result.get("List") or result.get("data") or result.get("info") or []
        for idx, item in enumerate(items, start=1):
            strength = None
            change_pct = None
            net_inflow_yi = None
            if isinstance(item, dict):
                name = item.get("name") or item.get("plate_name") or item.get("PlateName") or ""
                try:
                    raw_strength = item.get("strength") or item.get("score")
                    strength = float(raw_strength) if raw_strength is not None else None
                except (TypeError, ValueError):
                    strength = None
                try:
                    raw_change = item.get("change_pct") or item.get("pct")
                    change_pct = float(raw_change) if raw_change is not None else None
                except (TypeError, ValueError):
                    change_pct = None
                try:
                    raw_net = item.get("net_inflow")
                    if raw_net is None:
                        raw_net = item.get("net_amount")
                    if raw_net is None:
                        raw_net = item.get("main_net")
                    net_inflow_yi = float(raw_net) if raw_net is not None else None
                except (TypeError, ValueError):
                    net_inflow_yi = None
            elif isinstance(item, (tuple, list)) and len(item) > 1:
                name = str(item[1])
                try:
                    strength = float(item[2]) if len(item) > 2 and item[2] is not None else None
                except (TypeError, ValueError):
                    strength = None
                try:
                    change_pct = float(item[3]) if len(item) > 3 and item[3] is not None else None
                except (TypeError, ValueError):
                    change_pct = None
                try:
                    net_raw = item[6] if len(item) > 6 else None
                    net_inflow_yi = (float(net_raw) / 1e8) if net_raw is not None else None
                except (TypeError, ValueError):
                    net_inflow_yi = None
            else:
                name = ""
            if name:
                plates.append(
                    KaipanHotPlate(
                        name=name,
                        rank=idx,
                        strength=strength,
                        change_pct=change_pct,
                        net_inflow_yi=net_inflow_yi,
                        raw=item,
                    )
                )
        return plates

    def fetch_today_hot_plates(self) -> list[KaipanHotPlate]:
        return self.fetch_hot_plates("", today_mode=True)

    def fetch_yesterday_bans_pool(self, trade_date: str, max_ban: int = 5) -> list[dict[str, Any]]:
        return self._get_analyzer().get_history_bans_pool(trade_date, max_ban=max_ban)

    def fetch_ban_reasons(self, symbol: str) -> list[KaipanBanReason]:
        analyzer = self._get_analyzer()
        result = analyzer.get_ban_reasons(symbol)
        parsed = analyzer.parse_ban_reasons(result) if result else []
        return [
            KaipanBanReason(
                symbol=str(item.get("stock_code", symbol))[-6:],
                reason=str(item.get("reason", "")),
                group_str=str(item.get("group_str", "")),
                gnsm=str(item.get("gnsm", "")),
            )
            for item in parsed
            if item.get("reason")
        ]

    def health_check(self) -> dict[str, Any]:
        return {
            "source": "kaipan",
            "ready": True,
            "note": "Import/runtime smoke only in local mode; use low-frequency requests intraday.",
        }

    def validate_hot_plates(self, plates: list[KaipanHotPlate]) -> bool:
        return all(plate.name and plate.rank > 0 for plate in plates)

    def normalize_hot_plates(self, plates: list[KaipanHotPlate], trade_date: str) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []
        for plate in plates:
            hot_value = None
            if isinstance(plate.raw, dict):
                raw_hot = plate.raw.get("hot") or plate.raw.get("score") or plate.raw.get("heat")
                try:
                    hot_value = float(raw_hot) if raw_hot is not None else None
                except (TypeError, ValueError):
                    hot_value = None
            elif isinstance(plate.raw, (tuple, list)) and len(plate.raw) > 2:
                try:
                    hot_value = float(plate.raw[2])
                except (TypeError, ValueError):
                    hot_value = None
            normalized.append(
                {
                    "trade_date": trade_date,
                    "plate_name": plate.name,
                    "rank": plate.rank,
                    "strength": plate.strength,
                    "hot": hot_value,
                    "change_pct": plate.change_pct,
                    "net_inflow_yi": plate.net_inflow_yi,
                    "source": "kaipan",
                }
            )
        return normalized

    def normalize_ban_pool(self, items: list[dict[str, Any]], trade_date: str) -> list[KaipanBanPoolEntry]:
        return [
            KaipanBanPoolEntry(
                symbol=str(item.get("code", ""))[-6:],
                name=str(item.get("name", "")),
                lb_days=int(item.get("lb_days", 0) or 0),
                plate=str(item.get("plate", "")),
                seal_time=str(item.get("seal_time", "")),
                turnover=float(item.get("turnover", 0.0) or 0.0),
                close_pct=float(item.get("close_pct", 0.0) or 0.0),
            )
            for item in items
            if item.get("code")
        ]

    def normalize_ban_reasons(self, items: list[KaipanBanReason]) -> list[dict[str, Any]]:
        return [
            {
                "symbol": item.symbol,
                "reason": item.reason,
                "group_str": item.group_str,
                "gnsm": item.gnsm,
                "source": "kaipan",
            }
            for item in items
        ]

    def raw_schema_spec(self, dataset: str) -> RawSchemaSpec:
        if dataset == "hot_plates":
            return RawSchemaSpec(
                dataset=dataset,
                source="kaipan",
                payload_type="dict[list|tuple|dict]",
                field_specs=(
                    SchemaFieldSpec("list", "list", notes="Primary Kaipan list payload"),
                    SchemaFieldSpec("name", "str", required=False, notes="May appear as tuple[1] or dict key"),
                    SchemaFieldSpec("strength", "float", required=False, nullable=True, notes="Board strength score; tuple[2] in engine_v2 field audit"),
                    SchemaFieldSpec("change_pct", "float", required=False, nullable=True, notes="Board change percent; tuple[3] in engine_v2 field audit"),
                    SchemaFieldSpec("net_inflow", "float", required=False, nullable=True, notes="Capital net inflow in yuan; tuple[6] in engine_v2 field audit"),
                    SchemaFieldSpec("hot", "float", required=False, nullable=True, notes="Popularity/heat score if present"),
                ),
                sample_payload={"list": [["BK001", "robotics", 5000, 1.077, 2.99, 286525837452, 4261895483], ["BK002", "ai", 4683, 0.96, 0.796, 158332548489, 768398979]]},
                notes=("Today and history share one API family; date semantics differ upstream.",),
            )
        if dataset == "yest_limit_pool":
            return RawSchemaSpec(
                dataset=dataset,
                source="kaipan",
                payload_type="list[dict[str, Any]]",
                field_specs=(
                    SchemaFieldSpec("code", "str"),
                    SchemaFieldSpec("name", "str"),
                    SchemaFieldSpec("lb_days", "int"),
                    SchemaFieldSpec("plate", "str", required=False, nullable=True),
                    SchemaFieldSpec("seal_time", "str", required=False, nullable=True),
                    SchemaFieldSpec("turnover", "float", unit="percent"),
                    SchemaFieldSpec("close_pct", "float", unit="percent"),
                ),
                sample_payload=[{"code": "000001", "name": "sample", "lb_days": 2, "plate": "robotics", "seal_time": "09:31:22", "turnover": 18.3, "close_pct": 10.01}],
                notes=("History bans pool iterates 1..max_ban and deduplicates by code in engine_v2.",),
            )
        if dataset == "ban_reasons":
            return RawSchemaSpec(
                dataset=dataset,
                source="kaipan",
                payload_type="dict[List]",
                field_specs=(
                    SchemaFieldSpec("StockID", "str"),
                    SchemaFieldSpec("List", "list"),
                    SchemaFieldSpec("Reason", "str", required=False, nullable=True),
                    SchemaFieldSpec("ZSCode", "list", required=False, nullable=True),
                    SchemaFieldSpec("SCLT", "str", required=False, nullable=True),
                    SchemaFieldSpec("GNSM", "str", required=False, nullable=True),
                    SchemaFieldSpec("Group_Str", "str", required=False, nullable=True),
                ),
                sample_payload={"StockID": "000001", "List": [{"Reason": "robotics", "ZSCode": ["880001"], "SCLT": "main line", "GNSM": "theme", "Group_Str": "robotics+ai"}]},
                notes=("Reason payload enriches real plate names and market:stock_reason writeback.",),
            )
        raise ValueError(f"Unsupported Kaipan dataset={dataset}")

    def consumer_field_map(self, dataset: str) -> tuple[ConsumerFieldMap, ...]:
        if dataset == "hot_plates":
            return (
                ConsumerFieldMap(
                    dataset=dataset,
                    field_name="name/rank/hot",
                    consumer_module="engine_v2.v2_recap_engine_final",
                    consumer_function="run_audit",
                    dependency_level="required",
                    fallback_behavior="empty today_plates/yest_plates lists",
                    notes="Recap compares yesterday vs today plate ranks and heat.",
                ),
                ConsumerFieldMap(
                    dataset=dataset,
                    field_name="name/rank",
                    consumer_module="engine_v2.v2_orc_final",
                    consumer_function="_fetch_kaipan_hot_plates",
                    dependency_level="required",
                    fallback_behavior="return []",
                    notes="Orchestrator keeps top hot plates only.",
                ),
            )
        if dataset == "yest_limit_pool":
            return (
                ConsumerFieldMap(
                    dataset=dataset,
                    field_name="code/name/lb_days/plate/seal_time/turnover/close_pct",
                    consumer_module="engine_v2.v2_orc_final",
                    consumer_function="get_yest_limit_pool",
                    dependency_level="required",
                    fallback_behavior="return empty pool and lose ladder/plate context",
                ),
                ConsumerFieldMap(
                    dataset=dataset,
                    field_name="code/lb_days",
                    consumer_module="engine_v2.v2_recap_engine_final",
                    consumer_function="run_audit",
                    dependency_level="required",
                    fallback_behavior="ladder stats degrade to zero",
                ),
            )
        if dataset == "ban_reasons":
            return (
                ConsumerFieldMap(
                    dataset=dataset,
                    field_name="Reason/ZSCode/SCLT/GNSM/Group_Str",
                    consumer_module="engine_v2.v2_orc_final",
                    consumer_function="get_yest_limit_pool",
                    dependency_level="optional",
                    fallback_behavior="fallback to pool plate field and skip real plate refinement",
                    notes="Successful enrichment writes market:stock_plate and market:stock_reason.",
                ),
            )
        raise ValueError(f"Unsupported Kaipan dataset={dataset}")

    def normalized_schema_spec(self, dataset: str) -> NormalizedSchemaSpec:
        if dataset == "hot_plates":
            return NormalizedSchemaSpec(
                dataset=dataset,
                record_type="dict[str, Any]",
                key_fields=("trade_date", "plate_name"),
                field_specs=(
                    SchemaFieldSpec("trade_date", "str"),
                    SchemaFieldSpec("plate_name", "str"),
                    SchemaFieldSpec("rank", "int"),
                    SchemaFieldSpec("strength", "float", required=False, nullable=True),
                    SchemaFieldSpec("hot", "float", required=False, nullable=True),
                    SchemaFieldSpec("change_pct", "float", required=False, nullable=True),
                    SchemaFieldSpec("net_inflow_yi", "float", required=False, nullable=True, unit="yi"),
                    SchemaFieldSpec("source", "str"),
                ),
            )
        if dataset == "yest_limit_pool":
            return NormalizedSchemaSpec(
                dataset=dataset,
                record_type="KaipanBanPoolEntry",
                key_fields=("trade_date", "symbol"),
                field_specs=(
                    SchemaFieldSpec("trade_date", "str"),
                    SchemaFieldSpec("symbol", "str"),
                    SchemaFieldSpec("name", "str"),
                    SchemaFieldSpec("lb_days", "int"),
                    SchemaFieldSpec("plate", "str", required=False, nullable=True),
                    SchemaFieldSpec("seal_time", "str", required=False, nullable=True),
                    SchemaFieldSpec("turnover", "float", unit="percent"),
                    SchemaFieldSpec("close_pct", "float", unit="percent"),
                    SchemaFieldSpec("source", "str"),
                ),
            )
        if dataset == "ban_reasons":
            return NormalizedSchemaSpec(
                dataset=dataset,
                record_type="dict[str, Any]",
                key_fields=("symbol", "reason"),
                field_specs=(
                    SchemaFieldSpec("trade_date", "str", required=False, nullable=True),
                    SchemaFieldSpec("symbol", "str"),
                    SchemaFieldSpec("reason", "str"),
                    SchemaFieldSpec("group_str", "str", required=False, nullable=True),
                    SchemaFieldSpec("gnsm", "str", required=False, nullable=True),
                    SchemaFieldSpec("sclt", "str", required=False, nullable=True),
                    SchemaFieldSpec("source", "str"),
                ),
            )
        raise ValueError(f"Unsupported Kaipan dataset={dataset}")

    def storage_schema_spec(self, dataset: str) -> StorageSchemaSpec:
        return get_storage_schema_spec(dataset)

    def source_semantics_spec(self, dataset: str) -> SourceSemanticsSpec:
        return get_source_semantics_spec(dataset, SourceName.KAIPAN)

    def to_tdengine_rows(self, dataset: str, payload: Any, trade_date: str) -> list[dict[str, Any]]:
        spec = self.storage_schema_spec(dataset)
        if dataset == "hot_plates":
            rows = self.normalize_hot_plates(payload, trade_date)
        elif dataset == "yest_limit_pool":
            rows = [
                {
                    "trade_date": trade_date,
                    "symbol": item.symbol,
                    "name": item.name,
                    "lb_days": item.lb_days,
                    "plate": item.plate,
                    "seal_time": item.seal_time,
                    "turnover": item.turnover,
                    "close_pct": item.close_pct,
                    "source": "kaipan",
                }
                for item in self.normalize_ban_pool(payload, trade_date)
            ]
        elif dataset == "ban_reasons":
            rows = []
            for row in self.normalize_ban_reasons(payload):
                row = dict(row)
                row["trade_date"] = trade_date
                rows.append(row)
        else:
            raise ValueError(f"Unsupported Kaipan dataset={dataset}")
        return [{field: row.get(field) for field in spec.formal_storage_row_fields} for row in rows]

    def to_redis_view(self, dataset: str, payload: Any, trade_date: str) -> dict[str, dict[str, Any]]:
        spec = self.storage_schema_spec(dataset)
        rows = self.to_tdengine_rows(dataset, payload, trade_date)
        key_field = "plate_name" if dataset == "hot_plates" else "symbol"
        return {
            str(row[key_field]): {field: row.get(field) for field in spec.runtime_cache_view_fields}
            for row in rows
            if row.get(key_field)
        }

    def build_runtime_writebacks(
        self,
        reasons: list[KaipanBanReason],
        trade_date: str,
        *,
        existing_themes: Sequence[str] = (),
        fallback_plate: str = "",
    ) -> dict[str, dict[str, Any]]:
        normalized_rows = self.normalize_ban_reasons(reasons)
        symbol = next((str(row.get("symbol") or "") for row in normalized_rows if row.get("symbol")), "")
        return build_runtime_writebacks_from_reasons(
            symbol=symbol,
            reason_rows=normalized_rows,
            existing_themes=existing_themes,
            fallback_plate=fallback_plate,
        )
