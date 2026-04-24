from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from typing import Any, Optional

import pandas as pd

from engine_next.connectors.baostock_connector import BaostockConnector
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KlineReadyPlan:
    window_rows: tuple[dict[str, Any], ...]
    fetched_rows: tuple[dict[str, Any], ...] = ()


class RuntimeKlineService:
    """Engine-next private kline adapter without StockKLineService runtime ownership."""

    def __init__(
        self,
        tdengine_service: Optional[Any] = None,
        trading_calendar: Optional[Any] = None,
        baostock_connector: Optional[BaostockConnector] = None,
        redis_storage: Optional[Any] = None,
    ) -> None:
        self._tdengine = tdengine_service
        self._trading_calendar = trading_calendar
        self._baostock = baostock_connector
        self._redis_storage = redis_storage

    @property
    def tdengine(self) -> Any:
        if self._tdengine is None:
            from web.services.tdengine_service import TDengineService

            self._tdengine = TDengineService()
        return self._tdengine

    @property
    def trading_calendar(self) -> Any:
        if self._trading_calendar is None:
            from web.services.trading_calendar_service import TradingCalendarService

            self._trading_calendar = TradingCalendarService()
        return self._trading_calendar

    @property
    def baostock(self) -> BaostockConnector:
        if self._baostock is None:
            self._baostock = BaostockConnector()
        return self._baostock

    @property
    def redis_storage(self) -> Any:
        if self._redis_storage is None:
            sink = StringIO()
            with redirect_stdout(sink), redirect_stderr(sink):
                from web.redis_storage import RedisStorageManager

                self._redis_storage = RedisStorageManager()
        return self._redis_storage

    def get_cache_key(self, symbol: str, frequency: str = "d") -> str:
        code = str(symbol or "").split(".")[-1]
        return f"kline:{frequency}:{code}"

    def get_start_date_by_trading_days(self, end_date: str, trading_days_interval: int) -> str:
        current_date = datetime.strptime(end_date, "%Y-%m-%d")
        found_trading_days = 0
        while found_trading_days < trading_days_interval:
            current_date -= timedelta(days=1)
            date_str = current_date.strftime("%Y-%m-%d")
            if self.trading_calendar.is_trade_day(date_str):
                found_trading_days += 1
            if (datetime.strptime(end_date, "%Y-%m-%d") - current_date).days > 365 * 2:
                break
        return current_date.strftime("%Y-%m-%d")

    def _build_kline_dataframe(self, normalized: list[dict[str, Any]]) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "date": [row["trade_date"] for row in normalized],
                "open": [row["open"] for row in normalized],
                "high": [row["high"] for row in normalized],
                "low": [row["low"] for row in normalized],
                "close": [row["close"] for row in normalized],
                "volume": [0 for _ in normalized],
                "amount": [row["amount"] for row in normalized],
                "turn": [0.0 for _ in normalized],
                "pct_chg": [row["pct_chg"] for row in normalized],
            }
        )

    @staticmethod
    def _normalize_runtime_row(normalized_row: dict[str, Any]) -> dict[str, Any]:
        trade_date = str(normalized_row.get("trade_date") or normalized_row.get("date") or normalized_row.get("time") or "")
        return {
            "time": trade_date,
            "date": trade_date,
            "open": normalized_row.get("open", 0.0),
            "high": normalized_row.get("high", 0.0),
            "low": normalized_row.get("low", 0.0),
            "close": normalized_row.get("close", 0.0),
            "volume": normalized_row.get("volume", 0),
            "amount": normalized_row.get("amount", 0.0),
            "turn": normalized_row.get("turn", 0.0),
            "pct_chg": normalized_row.get("pct_chg", 0.0),
        }

    @staticmethod
    def _row_trade_date(row: dict[str, Any]) -> str:
        raw = row.get("time") or row.get("date") or row.get("trade_date") or ""
        return str(raw).split(" ")[0]

    def _merge_runtime_rows(
        self,
        base_rows: list[dict[str, Any]],
        fetched_rows: list[dict[str, Any]],
        *,
        start_date: str,
        target_date: str,
    ) -> list[dict[str, Any]]:
        merged: dict[str, dict[str, Any]] = {}
        for row in base_rows:
            trade_date = self._row_trade_date(row)
            if start_date <= trade_date <= target_date:
                merged[trade_date] = dict(row)
        for row in fetched_rows:
            trade_date = self._row_trade_date(row)
            if start_date <= trade_date <= target_date:
                merged[trade_date] = dict(row)
        return [merged[trade_date] for trade_date in sorted(merged)]

    def _persist_range(self, symbol: str, start_date: str, end_date: str) -> bool:
        try:
            logger.debug(
                "runtime kline range fetch start | symbol=%s | start_date=%s | end_date=%s",
                symbol,
                start_date,
                end_date,
            )
            rows = self.baostock.fetch_daily_kline_range(symbol, start_date=start_date, end_date=end_date)
            if not rows:
                logger.debug(
                    "runtime kline range fetch empty | symbol=%s | start_date=%s | end_date=%s",
                    symbol,
                    start_date,
                    end_date,
                )
                return False

            normalized = self.baostock.normalize_daily_kline(rows)
            dataframe = self._build_kline_dataframe(normalized)
            logger.debug(
                "runtime kline save tdengine | symbol=%s | start_date=%s | end_date=%s | rows=%s",
                symbol,
                start_date,
                end_date,
                len(dataframe),
            )
            saved = bool(self.tdengine.save_daily_kline(symbol, dataframe))
            logger.debug(
                "runtime kline range persist done | symbol=%s | start_date=%s | end_date=%s | saved=%s",
                symbol,
                start_date,
                end_date,
                saved,
            )
            return saved
        except Exception as exc:
            logger.warning(
                "runtime kline range persist failed | symbol=%s | start_date=%s | end_date=%s | error=%s",
                symbol,
                start_date,
                end_date,
                exc,
            )
            return False

    def persist_ready_plan(
        self,
        symbol: str,
        target_date: str,
        plan: KlineReadyPlan,
    ) -> bool:
        try:
            if plan.fetched_rows:
                dataframe = self._build_kline_dataframe(list(plan.fetched_rows))
                if not dataframe.empty and not self.tdengine.save_daily_kline(symbol, dataframe):
                    return False
            if plan.window_rows:
                self.redis_storage.store_data(
                    self.get_cache_key(symbol, "d"),
                    list(plan.window_rows)[-60:],
                    expire_seconds=86400,
                )
            return True
        except Exception as exc:
            logger.warning(
                "runtime kline deferred persist failed | symbol=%s | target_date=%s | error=%s",
                symbol,
                target_date,
                exc,
            )
            return False

    def ensure_kline_ready_plan(
        self,
        symbol: str,
        target_date: str,
        days: int = 60,
        latest_local: Optional[str] = None,
    ) -> KlineReadyPlan:
        start_date = self.get_start_date_by_trading_days(target_date, days)
        cache_key = self.get_cache_key(symbol, "d")
        l1_data: list[dict[str, Any]] = []
        try:
            cached = self.redis_storage.get_data(cache_key)
            if cached and isinstance(cached, list):
                l1_data = [dict(item) for item in cached if isinstance(item, dict)]
        except Exception as exc:
            logger.debug("runtime kline redis read failed | symbol=%s | error=%s", symbol, exc)

        if latest_local is None:
            latest_local = self.tdengine.get_latest_daily_date(symbol)
        logger.debug(
            "runtime kline ensure start | symbol=%s | target_date=%s | start_date=%s | latest_local=%s",
            symbol,
            target_date,
            start_date,
            latest_local or "-",
        )

        if latest_local and latest_local >= target_date:
            logger.debug(
                "runtime kline fetch plan | symbol=%s | mode=tdengine_ready | latest_local=%s | target_date=%s",
                symbol,
                latest_local or "-",
                target_date,
            )
            rows = self.tdengine.get_daily_kline(symbol, start_date, target_date) or []
            logger.debug(
                "runtime kline ensure done | symbol=%s | target_date=%s | rows=%s",
                symbol,
                target_date,
                len(rows),
            )
            return KlineReadyPlan(window_rows=tuple(rows))

        if l1_data and str(l1_data[-1].get("time", "")) >= target_date:
            logger.debug(
                "runtime kline fetch plan | symbol=%s | mode=redis_runtime_ready | rows=%s",
                symbol,
                len(l1_data),
            )
            filtered_rows = [
                dict(row)
                for row in l1_data
                if start_date <= self._row_trade_date(row) <= target_date
            ]
            fetched_rows = [
                {
                    "trade_date": self._row_trade_date(row),
                    "open": row.get("open", 0.0),
                    "high": row.get("high", 0.0),
                    "low": row.get("low", 0.0),
                    "close": row.get("close", 0.0),
                    "amount": row.get("amount", 0.0),
                    "pct_chg": row.get("pct_chg", 0.0),
                }
                for row in filtered_rows
            ]
            logger.debug(
                "runtime kline ensure done | symbol=%s | target_date=%s | rows=%s",
                symbol,
                target_date,
                len(filtered_rows),
            )
            return KlineReadyPlan(
                window_rows=tuple(filtered_rows),
                fetched_rows=tuple(fetched_rows),
            )

        fetch_start = start_date
        if latest_local and latest_local >= start_date:
            next_trade_day = self.trading_calendar.get_next_trade_day(latest_local)
            if next_trade_day:
                fetch_start = next_trade_day

        if fetch_start > target_date:
            logger.debug(
                "runtime kline fetch plan | symbol=%s | mode=skip_fetch | latest_local=%s | target_date=%s",
                symbol,
                latest_local or "-",
                target_date,
            )
            rows = self.tdengine.get_daily_kline(symbol, start_date, target_date) or []
            logger.debug(
                "runtime kline ensure done | symbol=%s | target_date=%s | rows=%s",
                symbol,
                target_date,
                len(rows),
            )
            return KlineReadyPlan(window_rows=tuple(rows))

        logger.debug(
            "runtime kline fetch plan | symbol=%s | mode=range_gap_fill | from=%s | to=%s | latest_local=%s",
            symbol,
            fetch_start,
            target_date,
            latest_local or "-",
        )
        source_rows = self.baostock.fetch_daily_kline_range(symbol, start_date=fetch_start, end_date=target_date)
        normalized_rows = self.baostock.normalize_daily_kline(source_rows) if source_rows else []
        runtime_rows = [self._normalize_runtime_row(row) for row in normalized_rows]
        base_rows = (
            self.tdengine.get_daily_kline(symbol, start_date, latest_local) or []
            if latest_local and latest_local >= start_date
            else []
        )
        merged_rows = self._merge_runtime_rows(
            base_rows,
            runtime_rows,
            start_date=start_date,
            target_date=target_date,
        )
        logger.debug(
            "runtime kline ensure done | symbol=%s | target_date=%s | rows=%s",
            symbol,
            target_date,
            len(merged_rows),
        )
        return KlineReadyPlan(
            window_rows=tuple(merged_rows),
            fetched_rows=tuple(normalized_rows),
        )

    def ensure_kline_ready(
        self,
        symbol: str,
        target_date: str,
        days: int = 60,
        latest_local: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        plan = self.ensure_kline_ready_plan(
            symbol,
            target_date,
            days=days,
            latest_local=latest_local,
        )
        self.persist_ready_plan(symbol, target_date, plan)
        return list(plan.window_rows)
