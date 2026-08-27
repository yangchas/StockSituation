from __future__ import annotations

import logging
import json
from dataclasses import dataclass
from datetime import datetime, time as dt_time

from engine_next.contracts.offline_sync_contracts import IntegratedSyncResult, WatermarkSnapshot
from engine_next.domain.enums import RunPhase
from engine_next.connectors.baostock_connector import BaostockConnector
from engine_next.runtime.offline_sync_executor import (
    OfflineSyncDecision,
    OfflineSyncRequest,
    OfflineSyncScope,
    ServerOnlyOfflineSyncExecutor,
)
from engine_next.runtime.startup_self_check import StartupSelfCheckService


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SettlementExecutionResult:
    should_audit_integrated_sync: bool
    effective_sync_symbols: tuple[str, ...]
    integrated_sync_allowed: bool
    integrated_sync_results: tuple[IntegratedSyncResult, ...] = ()
    sync_pipeline_targets: int = 0
    sync_network_targets: int = 0
    sync_analytics_targets: int = 0
    sync_factor_cache_gaps: int = 0
    sync_load_units: int = 0
    recap_ready: bool = False
    settlement_cached: bool = False
    settlement_running: bool = False
    settlement_payload: dict[str, object] | None = None


class SettlementController:
    """Owns formal integrated-sync gating and settlement execution."""

    def __init__(
        self,
        *,
        offline_executor: ServerOnlyOfflineSyncExecutor | None = None,
        auto_discovered_sync_limit: int = 50,
        redis_client: object | None = None,
        postmarket_settlement_time: dt_time = dt_time(17, 40),
    ) -> None:
        self._offline_executor = offline_executor or ServerOnlyOfflineSyncExecutor()
        self._auto_discovered_sync_limit = auto_discovered_sync_limit
        self._redis_client = redis_client
        self._postmarket_settlement_time = postmarket_settlement_time

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

    def _analytics_readiness_key(self, trade_date: str) -> str:
        return f"cache:analytics_readiness:{trade_date}"

    def _symbol_meta_key(self, trade_date: str) -> str:
        return f"cache:symbol_meta:{trade_date}"

    def _safe_hexists(self, key: str, field: str) -> bool:
        try:
            if hasattr(self.redis, "hexists"):
                return bool(self.redis.hexists(key, field))
            if hasattr(self.redis, "hget"):
                return self.redis.hget(key, field) not in (None, "")
        except Exception:
            return False
        return False

    def _safe_hlen(self, key: str) -> int:
        try:
            if hasattr(self.redis, "hlen"):
                return int(self.redis.hlen(key) or 0)
            if hasattr(self.redis, "hgetall"):
                return len(self.redis.hgetall(key) or {})
        except Exception:
            return 0
        return 0

    def _safe_hset_json_mapping(self, key: str, payloads: dict[str, dict[str, object]], *, ttl_seconds: int) -> None:
        if not payloads:
            return
        encoded = {
            str(field): json.dumps(payload, ensure_ascii=False)
            for field, payload in payloads.items()
            if str(field)
        }
        if not encoded:
            return
        try:
            if hasattr(self.redis, "pipeline"):
                pipe = self.redis.pipeline()
                pipe.hset(key, mapping=encoded)
                if hasattr(pipe, "expire"):
                    pipe.expire(key, ttl_seconds)
                pipe.execute()
                return
        except Exception:
            logger.debug("redis pipeline hset fallback | key=%s", key, exc_info=True)
        try:
            if hasattr(self.redis, "hset"):
                self.redis.hset(key, mapping=encoded)
                if hasattr(self.redis, "expire"):
                    self.redis.expire(key, ttl_seconds)
        except Exception as exc:
            logger.warning("analytics cache persist failed | key=%s | error=%s", key, exc)

    def _load_listing_dates(self, symbols: tuple[str, ...]) -> dict[str, str]:
        if not symbols:
            return {}
        try:
            from web.services.f10_service import F10DataService

            payload = F10DataService().batch_get_f10(list(symbols))
        except Exception:
            return {}

        listing_dates: dict[str, str] = {}
        for symbol in symbols:
            item = payload.get(symbol) or {}
            basic = item.get("basic") if isinstance(item, dict) else {}
            listing_date = str((basic or {}).get("listing_date") or "").strip()
            if listing_date:
                listing_dates[symbol] = listing_date
        return listing_dates

    def _load_kline_row_counts(self, symbols: tuple[str, ...], trade_date: str) -> dict[str, int]:
        if not symbols:
            return {}
        try:
            from engine_next.offline.integrated_sync import KlineWindowProvider

            provider = KlineWindowProvider()
        except Exception:
            return {}

        row_counts: dict[str, int] = {}
        for symbol in symbols:
            try:
                window = provider.load_existing_window(symbol, trade_date, lookback_days=60)
            except Exception:
                continue
            row_counts[symbol] = len(window.rows)
        return row_counts

    def _persist_startup_fact_cache(
        self,
        *,
        request: OfflineSyncRequest,
        watermark_snapshot: WatermarkSnapshot,
        integrated_sync_results: tuple[IntegratedSyncResult, ...],
    ) -> dict[str, int | str]:
        trade_date = request.target_date
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result_map = {result.symbol: result for result in integrated_sync_results}
        analytics_payloads: dict[str, dict[str, object]] = {}
        structural_candidates: list[str] = []

        for symbol in request.symbols:
            result = result_map.get(symbol)
            factor_cache_ready = self._safe_hexists(f"cache:stock_extra:{trade_date}", symbol)
            chip_cache_ready = self._safe_hexists(f"cache:chip_peaks:{trade_date}", symbol)
            dde_cache_ready = self._safe_hexists(f"cache:dde_ready:{trade_date}", symbol)
            latest_kline = watermark_snapshot.kline_latest_dates.get(symbol)
            latest_dde = watermark_snapshot.dde_latest_dates.get(symbol)
            latest_factor = watermark_snapshot.factor_latest_dates.get(symbol)

            payload = {
                "trade_date": trade_date,
                "updated_at": now_text,
                "kline_ready": bool(latest_kline and latest_kline >= trade_date),
                "dde_ready": bool((latest_dde and latest_dde >= trade_date) or dde_cache_ready),
                "factor_ready": bool(latest_factor and latest_factor >= trade_date),
                "chip_ready": chip_cache_ready,
                "redis_cache_ready": bool(factor_cache_ready and chip_cache_ready),
                "structural_factor_gap": False,
            }
            if result is not None:
                payload.update(
                    {
                        "kline_ready": bool(result.kline_ready),
                        "dde_ready": bool(result.dde_ready),
                        "factor_ready": bool(result.factor_ready),
                        "chip_ready": bool(result.chip_ready),
                        "redis_cache_ready": bool(result.redis_cache_ready),
                    }
                )
            analytics_payloads[symbol] = payload
            if payload["kline_ready"] and not factor_cache_ready and not bool(payload["chip_ready"]):
                structural_candidates.append(symbol)

        listing_dates = self._load_listing_dates(tuple(structural_candidates))
        kline_row_counts = self._load_kline_row_counts(tuple(structural_candidates), trade_date)
        cutoff_date = StartupSelfCheckService._factor_structural_cutoff_date(trade_date)
        symbol_meta_payloads: dict[str, dict[str, object]] = {}
        structural_gap_count = 0
        kline_rows_count = 0

        for symbol in structural_candidates:
            payload = analytics_payloads.get(symbol)
            if payload is None:
                continue
            row_count = int(kline_row_counts.get(symbol, 0) or 0)
            listing_date = str(listing_dates.get(symbol) or "").strip()
            if row_count > 0:
                payload["kline_rows"] = row_count
                kline_rows_count += 1
            structural_gap = False
            if 0 < row_count < 35:
                structural_gap = True
            elif listing_date and cutoff_date and listing_date >= cutoff_date:
                structural_gap = True
            payload["structural_factor_gap"] = structural_gap
            if structural_gap:
                structural_gap_count += 1
            if listing_date:
                symbol_meta_payloads[symbol] = {
                    "trade_date": trade_date,
                    "updated_at": now_text,
                    "listing_date": listing_date,
                }

        self._safe_hset_json_mapping(
            self._analytics_readiness_key(trade_date),
            analytics_payloads,
            ttl_seconds=7 * 24 * 60 * 60,
        )
        self._safe_hset_json_mapping(
            self._symbol_meta_key(trade_date),
            symbol_meta_payloads,
            ttl_seconds=30 * 24 * 60 * 60,
        )
        return {
            "startup_fact_cache_trade_date": trade_date,
            "startup_fact_analytics_count": len(analytics_payloads),
            "startup_fact_symbol_meta_count": len(symbol_meta_payloads),
            "startup_fact_structural_count": structural_gap_count,
            "startup_fact_kline_rows_count": kline_rows_count,
            "startup_fact_listing_date_count": len(symbol_meta_payloads),
        }

    def _startup_fact_cache_present(self, trade_date: str) -> bool:
        return self._safe_hlen(self._analytics_readiness_key(trade_date)) > 0

    def _persist_settlement_payload(self, trade_date: str, payload: dict[str, object]) -> None:
        try:
            self.redis.set(
                self._settlement_done_key(trade_date),
                json.dumps(payload, ensure_ascii=False),
                ex=3 * 24 * 60 * 60,
            )
        except Exception as exc:
            logger.warning("settlement done marker persist failed | trade_date=%s | error=%s", trade_date, exc)

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
        startup_fact_cache_stats: dict[str, int | str] | None = None,
    ) -> None:
        payload = self._build_settlement_payload(
            trade_date=trade_date,
            phase=phase,
            effective_sync_symbols=effective_sync_symbols,
            integrated_sync_results=integrated_sync_results,
            startup_fact_cache_stats=startup_fact_cache_stats,
        )
        self._persist_settlement_payload(trade_date, payload)

    def _build_settlement_payload(
        self,
        *,
        trade_date: str,
        phase: RunPhase,
        effective_sync_symbols: tuple[str, ...],
        integrated_sync_results: tuple[IntegratedSyncResult, ...],
        startup_fact_cache_stats: dict[str, int | str] | None = None,
    ) -> dict[str, object]:
        full_ready_count = 0
        partial_ready_count = 0
        failed_count = 0
        short_history_count = 0
        factor_cache_gap_count = 0

        for result in integrated_sync_results:
            all_ready = (
                result.kline_ready
                and result.dde_ready
                and result.factor_ready
                and result.chip_ready
                and result.redis_cache_ready
            )
            any_ready = (
                result.kline_ready
                or result.dde_ready
                or result.factor_ready
                or result.chip_ready
                or result.redis_cache_ready
            )
            if all_ready:
                full_ready_count += 1
            elif any_ready:
                partial_ready_count += 1
            else:
                failed_count += 1
            if any("too short" in str(note or "").lower() for note in result.notes):
                short_history_count += 1
            if result.factor_ready and not result.redis_cache_ready:
                factor_cache_gap_count += 1

        payload = {
            "trade_date": trade_date,
            "phase": phase.value,
            "effective_targets": len(effective_sync_symbols),
            "result_count": len(integrated_sync_results),
            "full_ready_count": full_ready_count,
            "partial_ready_count": partial_ready_count,
            "failed_count": failed_count,
            "short_history_count": short_history_count,
            "factor_cache_gap_count": factor_cache_gap_count,
            "completed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        if startup_fact_cache_stats:
            payload.update(startup_fact_cache_stats)
        return payload

    def _allow_auto_discovered_sync(
        self,
        *,
        phase: RunPhase,
        request: OfflineSyncRequest,
        pipeline_count: int,
        network_count: int,
        analytics_count: int,
        load_units: int,
    ) -> bool:
        if phase == RunPhase.NIGHT or phase == RunPhase.POSTMARKET:
            return True
        if phase == RunPhase.PREMARKET and request.now.time().strftime("%H:%M") < "09:00":
            return True
        if pipeline_count <= self._auto_discovered_sync_limit:
            return True

        max_network_targets = self._auto_discovered_sync_limit
        max_load_units = self._auto_discovered_sync_limit * 4
        allowed = network_count <= max_network_targets and load_units <= max_load_units
        logger.debug(
            "integrated sync scoped gate | phase=%s | pipe=%s | net=%s | analytics=%s | load=%s | "
            "net_limit=%s | load_limit=%s | allowed=%s",
            phase.value,
            pipeline_count,
            network_count,
            analytics_count,
            load_units,
            max_network_targets,
            max_load_units,
            allowed,
        )
        return allowed

    def _build_sync_scope_or_fallback(
        self,
        *,
        request: OfflineSyncRequest,
        offline_decision: OfflineSyncDecision,
        watermark_snapshot: WatermarkSnapshot,
    ) -> OfflineSyncScope:
        if hasattr(self._offline_executor, "build_sync_scope"):
            try:
                return self._offline_executor.build_sync_scope(
                    request,
                    watermark_snapshot,
                    offline_decision,
                )
            except Exception:
                logger.debug("build_sync_scope fallback engaged", exc_info=True)

        effective_sync_symbols = (
            self._offline_executor.resolve_effective_target_symbols(
                request,
                offline_decision,
                watermark_snapshot,
            )
            if hasattr(self._offline_executor, "resolve_effective_target_symbols")
            else ()
        )
        network_symbols = (
            self._offline_executor.resolve_network_target_symbols(
                request,
                watermark_snapshot,
                offline_decision,
            )
            if hasattr(self._offline_executor, "resolve_network_target_symbols")
            else effective_sync_symbols
        )
        analytics_symbols = (
            self._offline_executor.resolve_analytics_target_symbols(
                request,
                network_symbols,
                offline_decision,
            )
            if hasattr(self._offline_executor, "resolve_analytics_target_symbols")
            else effective_sync_symbols
        )
        target_symbols = (
            self._offline_executor.resolve_pipeline_target_symbols(
                request,
                watermark_snapshot,
                offline_decision,
            )
            if hasattr(self._offline_executor, "resolve_pipeline_target_symbols")
            else tuple(dict.fromkeys(network_symbols + analytics_symbols)) or effective_sync_symbols
        )
        factor_cache_gap_count = (
            offline_decision.dataset_gap_matrix.pending_count("factor_cache")
            if getattr(offline_decision, "dataset_gap_matrix", None) is not None
            else 0
        )
        return OfflineSyncScope(
            target_symbols=tuple(target_symbols),
            network_symbols=tuple(network_symbols),
            analytics_symbols=tuple(analytics_symbols),
            factor_cache_gap_count=factor_cache_gap_count,
            load_units=len(tuple(network_symbols)) * 4 + len(tuple(analytics_symbols)),
        )

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
            return now.time() >= self._postmarket_settlement_time
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
        sync_scope: OfflineSyncScope | None = None
        sync_pipeline_targets = 0
        sync_network_targets = 0
        sync_analytics_targets = 0
        sync_factor_cache_gaps = 0
        sync_load_units = 0

        if phase == RunPhase.POSTMARKET and should_audit_integrated_sync:
            cached_settlement = self._load_cached_settlement(request.target_date)
            if cached_settlement is not None:
                if not self._startup_fact_cache_present(request.target_date):
                    startup_fact_cache_stats = self._persist_startup_fact_cache(
                        request=request,
                        watermark_snapshot=watermark_snapshot,
                        integrated_sync_results=(),
                    )
                    cached_settlement = dict(cached_settlement)
                    cached_settlement.update(startup_fact_cache_stats)
                    cached_settlement["startup_fact_cache_rebuilt"] = True
                    self._persist_settlement_payload(request.target_date, cached_settlement)
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
                    sync_pipeline_targets=0,
                    sync_network_targets=0,
                    sync_analytics_targets=0,
                    sync_factor_cache_gaps=0,
                    sync_load_units=0,
                    recap_ready=True,
                    settlement_cached=True,
                    settlement_payload=cached_settlement,
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
                    sync_pipeline_targets=0,
                    sync_network_targets=0,
                    sync_analytics_targets=0,
                    sync_factor_cache_gaps=0,
                    sync_load_units=0,
                    recap_ready=False,
                    settlement_cached=False,
                    settlement_running=True,
                    settlement_payload=running_settlement,
                )

        if should_audit_integrated_sync:
            sync_scope = self._build_sync_scope_or_fallback(
                request=request,
                offline_decision=offline_decision,
                watermark_snapshot=watermark_snapshot,
            )
            effective_sync_symbols = sync_scope.target_symbols
            logger.debug(
                "integrated sync audit | phase=%s | universe=%s | effective_targets=%s",
                phase.value,
                len(request.symbols),
                len(effective_sync_symbols),
            )
            sync_pipeline_targets = sync_scope.pipeline_count
            sync_network_targets = sync_scope.network_count
            sync_analytics_targets = sync_scope.analytics_count
            sync_factor_cache_gaps = sync_scope.factor_cache_gap_count
            sync_load_units = sync_scope.load_units

        if (
            should_audit_integrated_sync
            and integrated_sync_allowed
            and not requested_symbols
            and len(request.symbols) > self._auto_discovered_sync_limit
        ):
            if sync_pipeline_targets <= self._auto_discovered_sync_limit:
                logger.debug(
                    "integrated sync small-gap override | effective_targets=%s <= safe_limit=%s",
                    sync_pipeline_targets,
                    self._auto_discovered_sync_limit,
                )
            elif not self._allow_auto_discovered_sync(
                phase=phase,
                request=request,
                pipeline_count=sync_pipeline_targets,
                network_count=sync_network_targets,
                analytics_count=sync_analytics_targets,
                load_units=sync_load_units,
            ):
                integrated_sync_allowed = False

        integrated_sync_results: tuple[IntegratedSyncResult, ...] = ()
        settlement_payload: dict[str, object] | None = None
        startup_fact_cache_stats: dict[str, int | str] | None = None
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
                execute_integrated_sync = self._offline_executor.execute_integrated_sync
                try:
                    integrated_sync_results = tuple(
                        execute_integrated_sync(
                            request,
                            watermark_snapshot=watermark_snapshot,
                            sync_scope=sync_scope,
                        )
                    )
                except TypeError:
                    integrated_sync_results = tuple(
                        execute_integrated_sync(
                            request,
                            watermark_snapshot=watermark_snapshot,
                        )
                    )
                logger.debug("integrated sync done | results=%s", len(integrated_sync_results))
                if phase == RunPhase.POSTMARKET:
                    startup_fact_cache_stats = self._persist_startup_fact_cache(
                        request=request,
                        watermark_snapshot=watermark_snapshot,
                        integrated_sync_results=integrated_sync_results,
                    )
                    settlement_payload = self._build_settlement_payload(
                        trade_date=request.target_date,
                        phase=phase,
                        effective_sync_symbols=effective_sync_symbols,
                        integrated_sync_results=integrated_sync_results,
                        startup_fact_cache_stats=startup_fact_cache_stats,
                    )
                    self._persist_settlement_done(
                        trade_date=request.target_date,
                        phase=phase,
                        effective_sync_symbols=effective_sync_symbols,
                        integrated_sync_results=integrated_sync_results,
                        startup_fact_cache_stats=startup_fact_cache_stats,
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
            startup_fact_cache_stats = self._persist_startup_fact_cache(
                request=request,
                watermark_snapshot=watermark_snapshot,
                integrated_sync_results=integrated_sync_results,
            )
            settlement_payload = self._build_settlement_payload(
                trade_date=request.target_date,
                phase=phase,
                effective_sync_symbols=effective_sync_symbols,
                integrated_sync_results=integrated_sync_results,
                startup_fact_cache_stats=startup_fact_cache_stats,
            )
            self._persist_settlement_done(
                trade_date=request.target_date,
                phase=phase,
                effective_sync_symbols=effective_sync_symbols,
                integrated_sync_results=integrated_sync_results,
                startup_fact_cache_stats=startup_fact_cache_stats,
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
            sync_pipeline_targets=sync_pipeline_targets,
            sync_network_targets=sync_network_targets,
            sync_analytics_targets=sync_analytics_targets,
            sync_factor_cache_gaps=sync_factor_cache_gaps,
            sync_load_units=sync_load_units,
            recap_ready=recap_ready,
            settlement_cached=settlement_cached,
            settlement_running=settlement_running,
            settlement_payload=settlement_payload,
        )
