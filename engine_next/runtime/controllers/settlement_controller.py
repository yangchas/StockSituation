from __future__ import annotations

import logging
import json
from dataclasses import dataclass
from datetime import datetime

from engine_next.contracts.offline_sync_contracts import IntegratedSyncResult, WatermarkSnapshot
from engine_next.domain.enums import RunPhase
from engine_next.connectors.baostock_connector import BaostockConnector
from engine_next.runtime.offline_sync_executor import (
    OfflineSyncDecision,
    OfflineSyncRequest,
    ServerOnlyOfflineSyncExecutor,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SettlementExecutionResult:
    should_audit_integrated_sync: bool
    effective_sync_symbols: tuple[str, ...]
    integrated_sync_allowed: bool
    integrated_sync_results: tuple[IntegratedSyncResult, ...] = ()
    recap_ready: bool = False
    settlement_cached: bool = False
    settlement_running: bool = False


class SettlementController:
    """Owns formal integrated-sync gating and settlement execution."""

    def __init__(
        self,
        *,
        offline_executor: ServerOnlyOfflineSyncExecutor | None = None,
        auto_discovered_sync_limit: int = 50,
        redis_client: object | None = None,
    ) -> None:
        self._offline_executor = offline_executor or ServerOnlyOfflineSyncExecutor()
        self._auto_discovered_sync_limit = auto_discovered_sync_limit
        self._redis_client = redis_client

    @property
    def redis(self):
        if self._redis_client is None:
            import redis as redis_lib

            self._redis_client = redis_lib.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        return self._redis_client

    def _settlement_done_key(self, trade_date: str) -> str:
        return f"market:settlement:{trade_date.replace('-', '')}:done"

    def _settlement_running_key(self, trade_date: str) -> str:
        return f"market:settlement:{trade_date.replace('-', '')}:running"

    def _load_cached_settlement(self, trade_date: str) -> dict | None:
        try:
            raw = self.redis.get(self._settlement_done_key(trade_date))
        except Exception:
            return None
        if not raw:
            return None
        try:
            payload = json.loads(raw)
            return payload if isinstance(payload, dict) else {"raw": raw}
        except Exception:
            return {"raw": raw}

    def _load_running_settlement(self, trade_date: str) -> dict | None:
        try:
            raw = self.redis.get(self._settlement_running_key(trade_date))
        except Exception:
            return None
        if not raw:
            return None
        try:
            payload = json.loads(raw)
            return payload if isinstance(payload, dict) else {"raw": raw}
        except Exception:
            return {"raw": raw}

    def _persist_settlement_running(
        self,
        *,
        trade_date: str,
        phase: RunPhase,
        effective_sync_symbols: tuple[str, ...],
    ) -> None:
        payload = {
            "trade_date": trade_date,
            "phase": phase.value,
            "effective_targets": len(effective_sync_symbols),
            "started_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            self.redis.set(
                self._settlement_running_key(trade_date),
                json.dumps(payload, ensure_ascii=False),
                ex=6 * 60 * 60,
            )
        except Exception as exc:
            logger.warning("settlement running marker persist failed | trade_date=%s | error=%s", trade_date, exc)

    def _clear_settlement_running(self, trade_date: str) -> None:
        try:
            self.redis.delete(self._settlement_running_key(trade_date))
        except Exception as exc:
            logger.warning("settlement running marker clear failed | trade_date=%s | error=%s", trade_date, exc)

    def _persist_settlement_done(
        self,
        *,
        trade_date: str,
        phase: RunPhase,
        effective_sync_symbols: tuple[str, ...],
        integrated_sync_results: tuple[IntegratedSyncResult, ...],
    ) -> None:
        payload = {
            "trade_date": trade_date,
            "phase": phase.value,
            "effective_targets": len(effective_sync_symbols),
            "result_count": len(integrated_sync_results),
            "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        try:
            self.redis.set(self._settlement_done_key(trade_date), json.dumps(payload, ensure_ascii=False), ex=3 * 24 * 60 * 60)
        except Exception as exc:
            logger.warning("settlement done marker persist failed | trade_date=%s | error=%s", trade_date, exc)

    def should_audit_integrated_sync(
        self,
        *,
        phase: RunPhase,
        now: datetime,
        lifecycle_audit_ran: bool,
        scheduled_event_name: str,
        scheduled_event_executed: bool,
    ) -> bool:
        if lifecycle_audit_ran and phase in (RunPhase.PREMARKET, RunPhase.NIGHT):
            return True
        if phase == RunPhase.POSTMARKET:
            if scheduled_event_name == "postmarket_settlement_1740" and scheduled_event_executed:
                return True
            return now.strftime("%H:%M") >= "17:40"
        return False

    def execute(
        self,
        *,
        request: OfflineSyncRequest,
        phase: RunPhase,
        should_audit_integrated_sync: bool,
        integrated_sync_requested: bool,
        requested_symbols: tuple[str, ...],
        offline_decision: OfflineSyncDecision,
        watermark_snapshot: WatermarkSnapshot,
    ) -> SettlementExecutionResult:
        effective_sync_symbols: tuple[str, ...] = ()
        integrated_sync_allowed = bool(integrated_sync_requested)
        settlement_cached = False
        settlement_running = False

        if phase == RunPhase.POSTMARKET and should_audit_integrated_sync:
            cached_settlement = self._load_cached_settlement(request.target_date)
            if cached_settlement is not None:
                logger.debug(
                    "integrated sync reuse cached settlement | phase=%s | trade_date=%s | payload=%s",
                    phase.value,
                    request.target_date,
                    cached_settlement,
                )
                return SettlementExecutionResult(
                    should_audit_integrated_sync=should_audit_integrated_sync,
                    effective_sync_symbols=(),
                    integrated_sync_allowed=False,
                    integrated_sync_results=(),
                    recap_ready=True,
                    settlement_cached=True,
                )
            running_settlement = self._load_running_settlement(request.target_date)
            if running_settlement is not None:
                logger.debug(
                    "integrated sync reuse running settlement | phase=%s | trade_date=%s | payload=%s",
                    phase.value,
                    request.target_date,
                    running_settlement,
                )
                return SettlementExecutionResult(
                    should_audit_integrated_sync=should_audit_integrated_sync,
                    effective_sync_symbols=(),
                    integrated_sync_allowed=False,
                    integrated_sync_results=(),
                    recap_ready=False,
                    settlement_cached=False,
                    settlement_running=True,
                )

        if should_audit_integrated_sync:
            effective_sync_symbols = self._offline_executor.resolve_effective_target_symbols(
                request,
                offline_decision,
                watermark_snapshot,
            )
            logger.debug(
                "integrated sync audit | phase=%s | universe=%s | effective_targets=%s",
                phase.value,
                len(request.symbols),
                len(effective_sync_symbols),
            )

        if (
            should_audit_integrated_sync
            and integrated_sync_allowed
            and not requested_symbols
            and len(request.symbols) > self._auto_discovered_sync_limit
        ):
            allow_large_auto_discovered_sync = (
                phase == RunPhase.NIGHT
                or phase == RunPhase.POSTMARKET
                or (phase == RunPhase.PREMARKET and request.now.time().strftime("%H:%M") < "09:00")
            )
            if len(effective_sync_symbols) <= self._auto_discovered_sync_limit:
                logger.debug(
                    "integrated sync small-gap override | effective_targets=%s <= safe_limit=%s",
                    len(effective_sync_symbols),
                    self._auto_discovered_sync_limit,
                )
            elif not allow_large_auto_discovered_sync:
                integrated_sync_allowed = False

        integrated_sync_results: tuple[IntegratedSyncResult, ...] = ()
        heavy_sync_allowed = phase in (RunPhase.PREMARKET, RunPhase.POSTMARKET, RunPhase.NIGHT)
        if should_audit_integrated_sync and integrated_sync_allowed and heavy_sync_allowed and effective_sync_symbols:
            logger.debug(
                "integrated sync start | phase=%s | effective_targets=%s | universe=%s",
                phase.value,
                len(effective_sync_symbols),
                len(request.symbols),
            )
            if phase == RunPhase.POSTMARKET:
                self._persist_settlement_running(
                    trade_date=request.target_date,
                    phase=phase,
                    effective_sync_symbols=effective_sync_symbols,
                )
            try:
                integrated_sync_results = tuple(
                    self._offline_executor.execute_integrated_sync(
                        request,
                        watermark_snapshot=watermark_snapshot,
                    )
                )
                logger.debug("integrated sync done | results=%s", len(integrated_sync_results))
                if phase == RunPhase.POSTMARKET:
                    self._persist_settlement_done(
                        trade_date=request.target_date,
                        phase=phase,
                        effective_sync_symbols=effective_sync_symbols,
                        integrated_sync_results=integrated_sync_results,
                    )
            except Exception as exc:
                logger.error(
                    "[settlement] source_check_failure | tdengine=ok | redis=ok | baostock=%s | error=%s",
                    BaostockConnector.status_summary(),
                    exc,
                )
                raise
            finally:
                if phase == RunPhase.POSTMARKET:
                    self._clear_settlement_running(request.target_date)
        elif phase == RunPhase.POSTMARKET and should_audit_integrated_sync and integrated_sync_allowed and not effective_sync_symbols:
            self._persist_settlement_done(
                trade_date=request.target_date,
                phase=phase,
                effective_sync_symbols=effective_sync_symbols,
                integrated_sync_results=integrated_sync_results,
            )
            self._clear_settlement_running(request.target_date)

        recap_ready = False
        if phase == RunPhase.POSTMARKET and should_audit_integrated_sync:
            if not integrated_sync_requested:
                recap_ready = True
            elif not effective_sync_symbols:
                recap_ready = True
            elif integrated_sync_allowed and bool(integrated_sync_results):
                recap_ready = True

        return SettlementExecutionResult(
            should_audit_integrated_sync=should_audit_integrated_sync,
            effective_sync_symbols=effective_sync_symbols,
            integrated_sync_allowed=integrated_sync_allowed,
            integrated_sync_results=integrated_sync_results,
            recap_ready=recap_ready,
            settlement_cached=settlement_cached,
            settlement_running=settlement_running,
        )
