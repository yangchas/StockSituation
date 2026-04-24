from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from threading import RLock
from typing import Any
import logging

from engine_next.contracts.baostock_contracts import (
    BaostockAvailabilityResult,
    BaostockDailyKlineRequest,
    check_baostock_daily_kline_availability,
)
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


logger = logging.getLogger(__name__)


def normalize_baostock_symbol(symbol: str) -> str:
    code = str(symbol).lower().replace("sh.", "").replace("sz.", "").strip()
    return f"sh.{code}" if code.startswith("6") else f"sz.{code}"


@dataclass(frozen=True)
class BaostockDailyBar:
    trade_date: str
    symbol: str
    open: float
    high: float
    low: float
    close: float
    preclose: float
    pct_chg: float
    amount: float


class BaostockConnector:
    """Formal daily-kline source. Intended for server-side execution."""

    _session_lock = RLock()
    _query_lock = RLock()
    _logged_in = False
    _last_error = ""
    _last_error_stage = ""
    _last_error_at = ""
    _last_success_at = ""

    @staticmethod
    def _call_silently(func, *args, **kwargs):
        sink = StringIO()
        with redirect_stdout(sink), redirect_stderr(sink):
            return func(*args, **kwargs)

    @classmethod
    def _record_error(cls, *, stage: str, message: str) -> None:
        cls._last_error_stage = str(stage or "").strip()
        cls._last_error = str(message or "").strip()
        cls._last_error_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @classmethod
    def _record_success(cls) -> None:
        cls._last_error_stage = ""
        cls._last_error = ""
        cls._last_error_at = ""
        cls._last_success_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @classmethod
    def status_summary(cls) -> str:
        if cls._last_error:
            detail = cls._last_error.replace("|", "/")
            stage = cls._last_error_stage or "unknown"
            return f"login_failed:{stage}:{detail}"
        if cls._logged_in:
            return "ready"
        return "idle"

    def _ensure_logged_in(self, force: bool = False) -> None:
        import baostock as bs

        with self._session_lock:
            if self._logged_in and not force:
                return
            logger.debug("baostock login start | force=%s", force)
            login_result = self._call_silently(bs.login)
            if login_result.error_code != "0":
                self._logged_in = False
                self._record_error(stage="login", message=login_result.error_msg)
                raise RuntimeError(f"baostock login failed: {login_result.error_msg}")
            self._logged_in = True
            self._record_success()
            logger.debug("baostock login ready | force=%s", force)

    def _reset_session(self) -> None:
        import baostock as bs

        with self._session_lock:
            try:
                self._call_silently(bs.logout)
            except Exception:
                pass
            self._logged_in = False
        self._ensure_logged_in(force=True)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value in (None, ""):
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def availability_check(
        self,
        now: datetime,
        target_date: str,
        previous_trade_date: str,
    ) -> BaostockAvailabilityResult:
        return check_baostock_daily_kline_availability(now, target_date, previous_trade_date)

    def fetch_daily_kline(self, request: BaostockDailyKlineRequest) -> list[BaostockDailyBar]:
        symbol = normalize_baostock_symbol(request.symbol)
        logger.debug("baostock fetch start | symbol=%s | trade_date=%s", symbol, request.trade_date)
        rows = self.fetch_daily_kline_range(
            request.symbol,
            start_date=request.trade_date,
            end_date=request.trade_date,
        )
        logger.debug("baostock fetch done | symbol=%s | trade_date=%s | rows=%s", symbol, request.trade_date, len(rows))
        return rows

    def fetch_daily_kline_range(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> list[BaostockDailyBar]:
        import baostock as bs

        rows: list[BaostockDailyBar] = []
        normalized_symbol = normalize_baostock_symbol(symbol)
        with self._query_lock:
            self._ensure_logged_in()
            logger.debug(
                "baostock query issue | symbol=%s | start_date=%s | end_date=%s",
                normalized_symbol,
                start_date,
                end_date,
            )
            rs = self._call_silently(
                bs.query_history_k_data_plus,
                normalized_symbol,
                "date,code,open,high,low,close,preclose,pctChg,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3",
            )
            logger.debug(
                "baostock query response | symbol=%s | start_date=%s | end_date=%s | error_code=%s",
                normalized_symbol,
                start_date,
                end_date,
                rs.error_code,
            )
            if rs.error_code == "10001001":
                self._reset_session()
                logger.debug(
                    "baostock query retry after reset | symbol=%s | start_date=%s | end_date=%s",
                    normalized_symbol,
                    start_date,
                    end_date,
                )
                rs = self._call_silently(
                    bs.query_history_k_data_plus,
                    normalized_symbol,
                    "date,code,open,high,low,close,preclose,pctChg,amount",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="3",
                )
            if rs.error_code != "0":
                self._record_error(stage="query", message=f"{rs.error_code} - {rs.error_msg}")
                raise RuntimeError(f"baostock query failed: {rs.error_code} - {rs.error_msg}")

            while rs.next():
                row = rs.get_row_data()
                if not row or len(row) < 9:
                    continue
                rows.append(
                    BaostockDailyBar(
                        trade_date=row[0],
                        symbol=str(row[1]).split(".")[-1],
                        open=self._safe_float(row[2]),
                        high=self._safe_float(row[3]),
                        low=self._safe_float(row[4]),
                        close=self._safe_float(row[5]),
                        preclose=self._safe_float(row[6]),
                        pct_chg=self._safe_float(row[7]),
                        amount=self._safe_float(row[8]),
                    )
                )
        logger.debug(
            "baostock range fetch done | symbol=%s | start_date=%s | end_date=%s | rows=%s",
            normalized_symbol,
            start_date,
            end_date,
            len(rows),
        )
        return rows

    def validate_daily_kline(self, rows: list[BaostockDailyBar]) -> bool:
        return all(
            row.symbol
            and row.trade_date
            and row.high >= row.low
            and row.amount >= 0
            for row in rows
        )

    def normalize_daily_kline(self, rows: list[BaostockDailyBar]) -> list[dict[str, Any]]:
        return [
            {
                "trade_date": row.trade_date,
                "symbol": row.symbol,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "preclose": row.preclose,
                "pct_chg": row.pct_chg,
                "amount": row.amount,
                "source": "baostock",
            }
            for row in rows
        ]

    def raw_schema_spec(self) -> RawSchemaSpec:
        return RawSchemaSpec(
            dataset="daily_kline",
            source="baostock",
            payload_type="list[BaostockDailyBar]",
            field_specs=(
                SchemaFieldSpec("trade_date", "str", notes="YYYY-MM-DD"),
                SchemaFieldSpec("symbol", "str", notes="6-digit code without market prefix"),
                SchemaFieldSpec("open", "float", unit="yuan"),
                SchemaFieldSpec("high", "float", unit="yuan"),
                SchemaFieldSpec("low", "float", unit="yuan"),
                SchemaFieldSpec("close", "float", unit="yuan"),
                SchemaFieldSpec("preclose", "float", unit="yuan"),
                SchemaFieldSpec("pct_chg", "float", unit="percent"),
                SchemaFieldSpec("amount", "float", unit="yuan"),
            ),
            sample_payload={
                "trade_date": "2026-04-18",
                "symbol": "600000",
                "open": 10.01,
                "high": 10.35,
                "low": 9.98,
                "close": 10.22,
                "preclose": 9.91,
                "pct_chg": 3.13,
                "amount": 1280000000.0,
            },
            notes=(
                "Formal daily-kline source after 17:30.",
                "Before readiness, target date must fall back to previous trade day.",
            ),
        )

    def consumer_field_map(self) -> tuple[ConsumerFieldMap, ...]:
        return (
            ConsumerFieldMap(
                dataset="daily_kline",
                field_name="close",
                consumer_module="engine_v2.v2_orc_final",
                consumer_function="_get_pre_close_map",
                dependency_level="required",
                fallback_behavior="fallback to metadata pre_close when Redis/TDengine are missing",
                notes="Used as previous close anchor and pre_close_map source.",
            ),
            ConsumerFieldMap(
                dataset="daily_kline",
                field_name="amount",
                consumer_module="engine_v2.v2_orc_final",
                consumer_function="_get_pre_close_map",
                dependency_level="required",
                fallback_behavior="missing amount weakens liquidity context but does not stop metadata fallback",
            ),
            ConsumerFieldMap(
                dataset="daily_kline",
                field_name="open/high/low/close/preclose",
                consumer_module="engine_v2.recap_baostock_20260407",
                consumer_function="query_history_k_data_plus consumption",
                dependency_level="required",
                fallback_behavior="no recap truth if row missing",
                notes="Post-market recap computes intraday percent path from these columns.",
            ),
            ConsumerFieldMap(
                dataset="daily_kline",
                field_name="trade_date/symbol",
                consumer_module="engine_v2.v2_data_lifecycle",
                consumer_function="_sync_daily_kline",
                dependency_level="required",
                fallback_behavior="gap fill and checkpoint resume",
                notes="Daily-kline freshness is tracked by symbol watermark.",
            ),
        )

    def normalized_schema_spec(self) -> NormalizedSchemaSpec:
        return NormalizedSchemaSpec(
            dataset="daily_kline",
            record_type="dict[str, Any]",
            key_fields=("trade_date", "symbol"),
            field_specs=(
                SchemaFieldSpec("trade_date", "str", notes="YYYY-MM-DD"),
                SchemaFieldSpec("symbol", "str"),
                SchemaFieldSpec("open", "float", unit="yuan"),
                SchemaFieldSpec("high", "float", unit="yuan"),
                SchemaFieldSpec("low", "float", unit="yuan"),
                SchemaFieldSpec("close", "float", unit="yuan"),
                SchemaFieldSpec("preclose", "float", unit="yuan"),
                SchemaFieldSpec("pct_chg", "float", unit="percent"),
                SchemaFieldSpec("amount", "float", unit="yuan"),
                SchemaFieldSpec("source", "str"),
            ),
            notes=("Normalized shape is the single source for persistence and Redis cache building.",),
        )

    def storage_schema_spec(self) -> StorageSchemaSpec:
        return get_storage_schema_spec("daily_kline")

    def source_semantics_spec(self) -> SourceSemanticsSpec:
        return get_source_semantics_spec("daily_kline", SourceName.BARS)

    def to_tdengine_rows(self, rows: list[BaostockDailyBar]) -> list[dict[str, Any]]:
        fields = self.storage_schema_spec().formal_storage_row_fields
        return [{field: row[field] for field in fields} for row in self.normalize_daily_kline(rows)]

    def to_redis_view(self, rows: list[BaostockDailyBar]) -> dict[str, dict[str, Any]]:
        fields = self.storage_schema_spec().runtime_cache_view_fields
        payload: dict[str, dict[str, Any]] = {}
        for row in self.normalize_daily_kline(rows):
            payload[row["symbol"]] = {field: row[field] for field in fields}
        return payload

    def health_check(self) -> dict[str, Any]:
        return {
            "source": "baostock",
            "ready": True,
            "note": "Import/runtime smoke only in local mode; real fetch should run on server.",
        }
