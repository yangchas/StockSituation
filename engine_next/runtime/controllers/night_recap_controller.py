from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime

from engine_next.audit.recap_pipeline import (
    RecapCollisionResult,
    RecapIngestionService,
    RecapInputBundle,
    RecapPersistResult,
)
from engine_next.domain.enums import RunPhase


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NightRecapExecutionResult:
    status: str
    available: bool
    executed: bool = False
    cached: bool = False
    trade_date: str = ""
    redis_key: str = ""
    signal_hits: int = 0
    false_positives: int = 0
    continuation_failures: int = 0
    enrichment_misses: int = 0
    degraded_inputs: tuple[str, ...] = ()
    source_issues: tuple[str, ...] = ()
    advice: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


class NightRecapController:
    """Owns the 17:40+ postmarket recap execution and concise operator summary."""

    def __init__(
        self,
        *,
        ingestion_service: RecapIngestionService | None = None,
    ) -> None:
        self._ingestion_service = ingestion_service or RecapIngestionService()
        self._last_trade_date: str | None = None
        self._last_result: NightRecapExecutionResult | None = None
        self._last_failure_attempt_token: str | None = None
        self._last_failure_result: NightRecapExecutionResult | None = None

    def _summary_key(self, trade_date: str) -> str:
        return f"market:recap:{trade_date.replace('-', '')}:summary"

    def _report_key(self, trade_date: str) -> str:
        return f"market:recap:{trade_date.replace('-', '')}:report"

    def _load_persisted_summary(self, trade_date: str) -> NightRecapExecutionResult | None:
        raw = self._ingestion_service.redis.get(self._summary_key(trade_date))
        if raw:
            try:
                payload = json.loads(raw)
            except Exception:
                payload = None
            if isinstance(payload, dict):
                return NightRecapExecutionResult(
                    status="cached",
                    available=True,
                    executed=False,
                    cached=True,
                    trade_date=str(payload.get("trade_date") or trade_date),
                    redis_key=str(payload.get("redis_key") or self._report_key(trade_date)),
                    signal_hits=int(payload.get("signal_hits") or 0),
                    false_positives=int(payload.get("false_positives") or 0),
                    continuation_failures=int(payload.get("continuation_failures") or 0),
                    enrichment_misses=int(payload.get("enrichment_misses") or 0),
                    degraded_inputs=tuple(str(item) for item in payload.get("degraded_inputs") or ()),
                    source_issues=tuple(str(item) for item in payload.get("source_issues") or ()),
                    advice=tuple(str(item) for item in payload.get("advice") or ()),
                    notes=tuple(str(item) for item in payload.get("notes") or ()),
                )
        report_exists = self._ingestion_service.redis.exists(self._report_key(trade_date))
        if report_exists:
            return NightRecapExecutionResult(
                status="cached",
                available=True,
                executed=False,
                cached=True,
                trade_date=trade_date,
                redis_key=self._report_key(trade_date),
                notes=("recap report already persisted for this trade date",),
            )
        return None

    def _persist_summary(self, result: NightRecapExecutionResult) -> None:
        payload = {
            "trade_date": result.trade_date,
            "redis_key": result.redis_key,
            "signal_hits": result.signal_hits,
            "false_positives": result.false_positives,
            "continuation_failures": result.continuation_failures,
            "enrichment_misses": result.enrichment_misses,
            "degraded_inputs": list(result.degraded_inputs),
            "source_issues": list(result.source_issues[:8]),
            "advice": list(result.advice),
            "notes": list(result.notes[:8]),
        }
        try:
            self._ingestion_service.redis.set(
                self._summary_key(result.trade_date),
                json.dumps(payload, ensure_ascii=True),
                ex=30 * 24 * 60 * 60,
            )
        except Exception:
            logger.exception("night recap summary persist failed | trade_date=%s", result.trade_date)

    def should_run(self, *, phase: RunPhase, now: datetime) -> bool:
        return phase == RunPhase.POSTMARKET and now.strftime("%H:%M") >= "17:40"

    def execute(
        self,
        *,
        phase: RunPhase,
        now: datetime,
        trade_date: str,
        previous_trade_date: str,
        settlement_ready: bool,
    ) -> NightRecapExecutionResult:
        if not self.should_run(phase=phase, now=now):
            return NightRecapExecutionResult(status="idle", available=False)
        if not settlement_ready:
            return NightRecapExecutionResult(
                status="waiting",
                available=True,
                trade_date=trade_date,
                notes=("awaiting settlement completion before recap",),
            )
        persisted_summary = self._load_persisted_summary(trade_date)
        if persisted_summary is not None:
            self._last_trade_date = trade_date
            self._last_result = persisted_summary
            return persisted_summary
        if self._last_trade_date == trade_date and self._last_result is not None:
            return NightRecapExecutionResult(
                status="cached",
                available=True,
                executed=False,
                cached=True,
                trade_date=self._last_result.trade_date,
                redis_key=self._last_result.redis_key,
                signal_hits=self._last_result.signal_hits,
                false_positives=self._last_result.false_positives,
                continuation_failures=self._last_result.continuation_failures,
                enrichment_misses=self._last_result.enrichment_misses,
                degraded_inputs=self._last_result.degraded_inputs,
                source_issues=self._last_result.source_issues,
                advice=self._last_result.advice,
                notes=self._last_result.notes,
            )

        attempt_token = f"{trade_date}:{now.strftime('%H:%M')}"
        if self._last_failure_attempt_token == attempt_token and self._last_failure_result is not None:
            return NightRecapExecutionResult(
                status="failed",
                available=True,
                executed=False,
                cached=True,
                trade_date=self._last_failure_result.trade_date,
                redis_key=self._last_failure_result.redis_key,
                signal_hits=self._last_failure_result.signal_hits,
                false_positives=self._last_failure_result.false_positives,
                continuation_failures=self._last_failure_result.continuation_failures,
                enrichment_misses=self._last_failure_result.enrichment_misses,
                degraded_inputs=self._last_failure_result.degraded_inputs,
                source_issues=self._last_failure_result.source_issues,
                advice=self._last_failure_result.advice,
                notes=self._last_failure_result.notes,
            )

        logger.info("night recap start | trade_date=%s | previous_trade_date=%s", trade_date, previous_trade_date)
        try:
            bundle, collision, persist = asyncio.run(
                self._ingestion_service.build_run_and_persist(
                    trade_date=trade_date,
                    previous_trade_date=previous_trade_date,
                    write_enabled=True,
                )
            )
        except Exception as exc:
            logger.exception("night recap failed | trade_date=%s", trade_date)
            result = NightRecapExecutionResult(
                status="failed",
                available=True,
                executed=True,
                trade_date=trade_date,
                notes=(f"recap failed: {exc}",),
            )
            self._last_failure_attempt_token = attempt_token
            self._last_failure_result = result
            return result

        result = self._build_execution_result(bundle=bundle, collision=collision, persist=persist)
        self._last_trade_date = trade_date
        self._last_result = result
        self._persist_summary(result)
        self._last_failure_attempt_token = None
        self._last_failure_result = None
        logger.info(
            "night recap done | trade_date=%s | hits=%s | false_positives=%s | continuation_failures=%s",
            trade_date,
            result.signal_hits,
            result.false_positives,
            result.continuation_failures,
        )
        return NightRecapExecutionResult(
            status="ready",
            available=True,
            executed=True,
            cached=False,
            trade_date=result.trade_date,
            redis_key=result.redis_key,
            signal_hits=result.signal_hits,
            false_positives=result.false_positives,
            continuation_failures=result.continuation_failures,
            enrichment_misses=result.enrichment_misses,
            degraded_inputs=result.degraded_inputs,
            source_issues=result.source_issues,
            advice=result.advice,
            notes=result.notes,
        )

    def render_summary(self, result: NightRecapExecutionResult) -> tuple[str, ...]:
        if not result.available:
            return ()
        if result.status == "waiting":
            return ("recap_wait | awaiting settlement completion",)
        if result.status == "failed":
            first_note = result.notes[0] if result.notes else "recap execution failed"
            return (f"recap_failed | {first_note}",)
        lines = [
            (
                f"recap_status={'cached' if result.cached else 'ready'} "
                f"| hits={result.signal_hits} "
                f"| false_positive={result.false_positives} "
                f"| continuation_failures={result.continuation_failures}"
            ),
        ]
        if result.enrichment_misses:
            lines.append(f"recap_enrichment | misses={result.enrichment_misses}")
        if result.degraded_inputs:
            lines.append(f"recap_degraded | missing={','.join(result.degraded_inputs[:4])}")
        if result.source_issues:
            lines.append(f"recap_sources | issues={'; '.join(result.source_issues[:2])}")
        if result.advice:
            lines.append(f"recap_advice | {' | '.join(result.advice[:3])}")
        if result.redis_key:
            lines.append(f"recap_report={result.redis_key}")
        elif result.notes:
            lines.append(f"recap_note | {result.notes[0]}")
        return tuple(lines)

    def _build_execution_result(
        self,
        *,
        bundle: RecapInputBundle,
        collision: RecapCollisionResult,
        persist: RecapPersistResult,
    ) -> NightRecapExecutionResult:
        degraded_inputs = tuple(self._compress_missing_notes(bundle.notes))
        notes = tuple(bundle.notes) + tuple(collision.notes) + tuple(persist.notes)
        return NightRecapExecutionResult(
            status="ready",
            available=True,
            executed=True,
            cached=False,
            trade_date=bundle.trade_date,
            redis_key=persist.redis_key if persist.persisted else "",
            signal_hits=len(collision.signal_hit_rows),
            false_positives=len(collision.false_positive_rows),
            continuation_failures=len(collision.signal_miss_rows),
            enrichment_misses=len(collision.enrichment_miss_rows),
            degraded_inputs=degraded_inputs,
            source_issues=tuple(self._extract_source_issues(notes)),
            advice=self._build_advice(collision),
            notes=notes,
        )

    def _compress_missing_notes(self, notes: tuple[str, ...]) -> list[str]:
        compressed: list[str] = []
        for note in notes:
            if " is empty" not in note:
                continue
            compressed.append(note.replace(" is empty", "").replace(" ", "_"))
        return compressed

    def _extract_source_issues(self, notes: tuple[str, ...]) -> list[str]:
        issues: list[str] = []
        for note in notes:
            if "source failed:" in note or "baostock eod fetch failed" in note or "baostock eod normalize failed" in note:
                issues.append(note)
        return issues

    def _build_advice(self, collision: RecapCollisionResult) -> tuple[str, ...]:
        advice: list[str] = []
        hits = len(collision.signal_hit_rows)
        false_positives = len(collision.false_positive_rows)
        continuation_failures = len(collision.signal_miss_rows)

        if continuation_failures > max(hits, 0):
            advice.append("yesterday ladder weakened; lower blind continuation sizing tomorrow")
        elif hits:
            advice.append("signal chain still converts; keep leaders on watch tomorrow")

        top_new_mainline = next(
            (row for row in collision.plate_rotation_rows[:5] if bool(row.get("is_new_mainline"))),
            None,
        )
        if top_new_mainline is not None:
            advice.append(f"watch plate rotation into {top_new_mainline.get('plate_name') or '-'}")

        if false_positives > hits and false_positives >= 3:
            advice.append("false positives dominated; tighten auction and intraday confirmation")

        if not advice:
            advice.append("truth set is quiet; keep next-day plan focused on strongest mainline only")
        return tuple(advice)
