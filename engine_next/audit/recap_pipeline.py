from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from engine_next.contracts.baostock_contracts import BaostockDailyKlineRequest
from engine_next.runtime.plate_mapping_registry import (
    PLATE_MAPPING_S2P_KEY,
    RUNTIME_PRIMARY_PLATE_KEY,
    RUNTIME_REASON_KEY,
    build_yest_limit_theme_candidates,
    choose_primary_plate,
    choose_pool_primary_plate,
    decode_theme_list,
    encode_theme_list,
    merge_theme_payload_prioritized,
    merge_theme_lists,
)


@dataclass(frozen=True)
class RecapStageSpec:
    name: str
    trigger_time: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    purpose: str


@dataclass(frozen=True)
class RecapInputBundle:
    trade_date: str
    signal_snapshot: list[dict[str, Any]]
    today_limit_truth: list[dict[str, Any]]
    yesterday_limit_pool: list[dict[str, Any]]
    today_hot_plates: list[dict[str, Any]]
    yesterday_hot_plates: list[dict[str, Any]]
    broken_board_truth: list[dict[str, Any]]
    first_failed_truth: list[dict[str, Any]]
    eod_kline_truth: list[dict[str, Any]]
    stock_plate_writebacks: dict[str, str]
    stock_reason_writebacks: dict[str, str]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RecapCollisionResult:
    trade_date: str
    signal_hit_rows: list[dict[str, Any]]
    signal_miss_rows: list[dict[str, Any]]
    false_positive_rows: list[dict[str, Any]]
    plate_rotation_rows: list[dict[str, Any]]
    ladder_stats_rows: list[dict[str, Any]]
    enrichment_miss_rows: list[dict[str, Any]]
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RecapPersistResult:
    trade_date: str
    redis_key: str
    report_text: str
    persisted: bool
    notes: tuple[str, ...] = ()


POSTMARKET_RECAP_STAGES: tuple[RecapStageSpec, ...] = (
    RecapStageSpec(
        name="eod_kline_finalize",
        trigger_time="17:40+",
        inputs=("Baostock daily kline", "offline sync watermark"),
        outputs=("formal daily_kline in TDengine", "cache:kline_ready:{date} in Redis"),
        purpose="Finalize formal EOD kline truth after the Baostock ready window.",
    ),
    RecapStageSpec(
        name="signal_snapshot_load",
        trigger_time="17:40+",
        inputs=("market:snapshot:{date}:signals", "market:snapshot:{yyyymmdd}:signals"),
        outputs=("signal candidates for audit",),
        purpose="Load intraday signal snapshots for full collision and miss analysis.",
    ),
    RecapStageSpec(
        name="market_truth_fetch",
        trigger_time="17:40+",
        inputs=("Wencai today limit truth", "Baostock close truth"),
        outputs=("today limit-up truth", "close performance truth"),
        purpose="Build the formal post-market truth set for stocks and close performance.",
    ),
    RecapStageSpec(
        name="plate_rotation_compare",
        trigger_time="17:40+",
        inputs=("Kaipan yesterday hot plates", "Kaipan today hot plates"),
        outputs=("plate migration report", "mainline rotation clues"),
        purpose="Compare previous and current hot plates to detect rotation and continuity.",
    ),
    RecapStageSpec(
        name="yest_limit_ladder_compare",
        trigger_time="17:40+",
        inputs=("Kaipan yesterday bans", "Wencai today limit truth"),
        outputs=("ladder promotion stats", "continuation failure stats"),
        purpose="Measure ladder continuation, failed promotion, and negative feedback.",
    ),
    RecapStageSpec(
        name="signal_full_collision",
        trigger_time="17:40+",
        inputs=("signal snapshot", "today truth", "plate rotation", "yest limit ladder"),
        outputs=("buy hit list", "false positive list", "signal miss list", "strategy adjustment notes"),
        purpose="Run the post-market collision between intraday signals and formal truth.",
    ),
    RecapStageSpec(
        name="recap_report_persist",
        trigger_time="17:40+",
        inputs=("structured recap result",),
        outputs=("market:recap:{date}:report", "strategy memory candidate", "TDengine recap snapshot"),
        purpose="Persist structured recap outputs for audit and later memory feedback.",
    ),
)


def build_recap_questions() -> list[str]:
    return [
        "Which signals actually converted into limit-up truth, and which ones failed by the close?",
        "How did the hot plates rotate from yesterday to today, and where did mainline strength move?",
        "Which yesterday limit-up names failed promotion, and did they become negative-feedback markers?",
        "Did runtime stock plate or ban-reason writebacks miss key names that later mattered?",
        "Did the early auction or intraday signal chain miss obvious opportunities that later confirmed?",
        "What should be fed back into strategy memory or source-quality review after this collision?",
    ]


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".")[0]
    return text[-6:]


def build_recap_input_bundle(
    trade_date: str,
    signal_snapshot: list[dict[str, Any]],
    today_limit_truth: list[dict[str, Any]],
    yesterday_limit_pool: list[dict[str, Any]],
    today_hot_plates: list[dict[str, Any]],
    yesterday_hot_plates: list[dict[str, Any]],
    broken_board_truth: list[dict[str, Any]] | None = None,
    first_failed_truth: list[dict[str, Any]] | None = None,
    eod_kline_truth: list[dict[str, Any]] | None = None,
    stock_plate_writebacks: dict[str, str] | None = None,
    stock_reason_writebacks: dict[str, str] | None = None,
) -> RecapInputBundle:
    notes: list[str] = []
    if not signal_snapshot:
        notes.append("signal snapshot is empty")
    if not today_limit_truth:
        notes.append("today limit truth is empty")
    if not yesterday_limit_pool:
        notes.append("yesterday limit pool is empty")
    if not today_hot_plates:
        notes.append("today hot plates are empty")
    if not yesterday_hot_plates:
        notes.append("yesterday hot plates are empty")
    if not broken_board_truth:
        notes.append("broken board truth is empty")
    if not first_failed_truth:
        notes.append("first failed truth is empty")
    if not eod_kline_truth:
        notes.append("eod kline truth is empty")
    return RecapInputBundle(
        trade_date=trade_date,
        signal_snapshot=signal_snapshot,
        today_limit_truth=today_limit_truth,
        yesterday_limit_pool=yesterday_limit_pool,
        today_hot_plates=today_hot_plates,
        yesterday_hot_plates=yesterday_hot_plates,
        broken_board_truth=broken_board_truth or [],
        first_failed_truth=first_failed_truth or [],
        eod_kline_truth=eod_kline_truth or [],
        stock_plate_writebacks=stock_plate_writebacks or {},
        stock_reason_writebacks=stock_reason_writebacks or {},
        notes=tuple(notes),
    )


class RecapIngestionService:
    """Builds a real recap input bundle from Redis + Baostock + Kaipan + Wencai."""

    def __init__(self) -> None:
        self._redis = None
        self._kaipan = None
        self._wencai = None
        self._baostock = None

    @property
    def redis(self):
        if self._redis is None:
            import redis as redis_lib

            self._redis = redis_lib.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        return self._redis

    @property
    def kaipan(self):
        if self._kaipan is None:
            from engine_next.connectors import KaipanConnector

            self._kaipan = KaipanConnector()
        return self._kaipan

    @property
    def wencai(self):
        if self._wencai is None:
            from engine_next.connectors import WencaiConnector

            self._wencai = WencaiConnector()
        return self._wencai

    @property
    def baostock(self):
        if self._baostock is None:
            from engine_next.connectors import BaostockConnector

            self._baostock = BaostockConnector()
        return self._baostock

    def _load_signal_snapshot(self, trade_date: str) -> list[dict[str, Any]]:
        key_variants = [
            f"market:snapshot:{trade_date}:signals",
            f"market:snapshot:{trade_date.replace('-', '')}:signals",
            f"market:snapshot:{trade_date}",
        ]
        for key in key_variants:
            raw = self.redis.get(key)
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except Exception:
                continue
            if isinstance(payload, dict) and isinstance(payload.get("signals"), list):
                return payload["signals"]
            if isinstance(payload, list):
                return payload
        return []

    def _load_runtime_writebacks(self) -> tuple[dict[str, str], dict[str, str]]:
        plate_map = self.redis.hgetall(RUNTIME_PRIMARY_PLATE_KEY) or {}
        reason_map = self.redis.hgetall(RUNTIME_REASON_KEY) or {}
        return dict(plate_map), dict(reason_map)

    async def _safe_fetch_rows(self, name: str, fetcher) -> tuple[list[dict[str, Any]], list[str]]:
        try:
            rows = await fetcher()
        except Exception as exc:
            return [], [f"{name} source failed: {exc}"]
        if not rows:
            return [], []
        return list(rows), []

    async def _fetch_baostock_truth_rows(
        self,
        trade_date: str,
        signal_snapshot: list[dict[str, Any]],
        today_limit_truth: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        watch_codes = {
            _normalize_symbol(item.get("code") or item.get("symbol"))
            for item in signal_snapshot
            if item.get("code") or item.get("symbol")
        }
        watch_codes.update(
            _normalize_symbol(row.get("symbol"))
            for row in today_limit_truth
            if row.get("symbol")
        )
        eod_kline_truth: list[dict[str, Any]] = []
        notes: list[str] = []
        for symbol in sorted(code for code in watch_codes if code):
            request = BaostockDailyKlineRequest(symbol=symbol, trade_date=trade_date)
            try:
                bars = await asyncio.to_thread(self.baostock.fetch_daily_kline, request)
            except Exception as exc:
                notes.append(f"baostock eod fetch failed for {symbol}: {exc}")
                continue
            if not bars:
                continue
            try:
                eod_kline_truth.extend(self.baostock.to_tdengine_rows(bars))
            except Exception as exc:
                notes.append(f"baostock eod normalize failed for {symbol}: {exc}")
        return eod_kline_truth, notes

    async def _enrich_writebacks_from_yest_pool(
        self,
        trade_date: str,
        yesterday_limit_pool: list[dict[str, Any]],
        stock_plate_writebacks: dict[str, str],
        stock_reason_writebacks: dict[str, str],
    ) -> tuple[dict[str, str], dict[str, str], list[str]]:
        notes: list[str] = []
        updated_plate = dict(stock_plate_writebacks)
        updated_reason = dict(stock_reason_writebacks)
        pool_plate_map: dict[str, str] = {}

        pending_symbols = []
        for row in yesterday_limit_pool:
            symbol = _normalize_symbol(row.get("symbol"))
            if not symbol:
                continue
            pool_plate = str(row.get("plate") or "").strip()
            if pool_plate:
                pool_plate_map[symbol] = pool_plate
                existing_theme_raw = self.redis.hget(PLATE_MAPPING_S2P_KEY, symbol)
                merged_themes = build_yest_limit_theme_candidates(
                    pool_plate=pool_plate,
                    reason_candidates=(),
                    existing_themes=decode_theme_list(existing_theme_raw),
                )
                if merged_themes:
                    resolved_pool_plate = choose_pool_primary_plate(
                        merged_themes,
                        (),
                        fallback=pool_plate,
                    )
                    if resolved_pool_plate:
                        updated_plate[symbol] = resolved_pool_plate
                    try:
                        self.redis.hset(PLATE_MAPPING_S2P_KEY, symbol, encode_theme_list(merged_themes))
                    except Exception as exc:
                        notes.append(f"redis writeback failed for {PLATE_MAPPING_S2P_KEY}:{symbol}: {exc}")
            elif symbol not in updated_plate:
                updated_plate[symbol] = ""
            if pool_plate or symbol not in updated_plate or symbol not in updated_reason:
                pending_symbols.append(symbol)

        for symbol in pending_symbols:
            try:
                reasons = await asyncio.to_thread(self.kaipan.fetch_ban_reasons, symbol)
            except Exception as exc:
                notes.append(f"ban reason fetch failed for {symbol}: {exc}")
                continue
            if not reasons:
                continue
            writebacks = self.kaipan.build_runtime_writebacks(
                reasons,
                trade_date,
                existing_themes=decode_theme_list(self.redis.hget(PLATE_MAPPING_S2P_KEY, symbol)),
                fallback_plate=str(updated_plate.get(symbol) or ""),
                pool_plate=pool_plate_map.get(symbol, ""),
            )
            for code, themes in writebacks.get(PLATE_MAPPING_S2P_KEY, {}).items():
                merged_themes, merged_payload = merge_theme_payload_prioritized(
                    self.redis.hget(PLATE_MAPPING_S2P_KEY, code),
                    themes,
                )
                if not merged_themes:
                    continue
                try:
                    self.redis.hset(PLATE_MAPPING_S2P_KEY, code, merged_payload)
                except Exception as exc:
                    notes.append(f"redis writeback failed for {PLATE_MAPPING_S2P_KEY}:{code}: {exc}")
                if code not in updated_plate:
                    resolved_plate = choose_primary_plate(merged_themes)
                    if resolved_plate:
                        updated_plate[code] = resolved_plate
            for code, plate in writebacks.get(RUNTIME_PRIMARY_PLATE_KEY, {}).items():
                if plate and updated_plate.get(code) != str(plate):
                    updated_plate[code] = str(plate)
                    try:
                        self.redis.hset(RUNTIME_PRIMARY_PLATE_KEY, code, str(plate))
                    except Exception as exc:
                        notes.append(f"redis writeback failed for {RUNTIME_PRIMARY_PLATE_KEY}:{code}: {exc}")
            for code, reason in writebacks.get(RUNTIME_REASON_KEY, {}).items():
                if reason and code not in updated_reason:
                    updated_reason[code] = str(reason)
                    try:
                        self.redis.hset(RUNTIME_REASON_KEY, code, str(reason))
                    except Exception as exc:
                        notes.append(f"redis writeback failed for {RUNTIME_REASON_KEY}:{code}: {exc}")

        return updated_plate, updated_reason, notes

    async def build_real_input_bundle(self, trade_date: str, previous_trade_date: str) -> RecapInputBundle:
        signal_snapshot = self._load_signal_snapshot(trade_date)
        stock_plate_writebacks, stock_reason_writebacks = self._load_runtime_writebacks()

        async def _fetch_limit_truth():
            df = await self.wencai.fetch_limitup_with_lb_days(max_stocks=500)
            return self.wencai.to_tdengine_rows("limit_truth", df, trade_date)

        async def _fetch_broken():
            codes = await self.wencai.fetch_broken_boards(max_stocks=100)
            return self.wencai.to_tdengine_rows("broken_boards", codes, trade_date)

        async def _fetch_first_failed():
            codes = await self.wencai.fetch_first_failed(max_stocks=100)
            return self.wencai.to_tdengine_rows("first_failed", codes, trade_date)

        async def _fetch_yest_pool():
            raw = await asyncio.to_thread(self.kaipan.fetch_yesterday_bans_pool, previous_trade_date)
            return self.kaipan.to_tdengine_rows("yest_limit_pool", raw, previous_trade_date)

        async def _fetch_today_hot_plates():
            raw = await asyncio.to_thread(self.kaipan.fetch_today_hot_plates)
            return self.kaipan.to_tdengine_rows("hot_plates", raw, trade_date)

        async def _fetch_yesterday_hot_plates():
            raw = await asyncio.to_thread(self.kaipan.fetch_hot_plates, previous_trade_date)
            return self.kaipan.to_tdengine_rows("hot_plates", raw, previous_trade_date)

        (
            limit_truth_result,
            broken_result,
            first_failed_result,
            yest_pool_result,
            today_hot_result,
            yesterday_hot_result,
        ) = await asyncio.gather(
            self._safe_fetch_rows("today limit truth", _fetch_limit_truth),
            self._safe_fetch_rows("broken board truth", _fetch_broken),
            self._safe_fetch_rows("first failed truth", _fetch_first_failed),
            self._safe_fetch_rows("yesterday limit pool", _fetch_yest_pool),
            self._safe_fetch_rows("today hot plates", _fetch_today_hot_plates),
            self._safe_fetch_rows("yesterday hot plates", _fetch_yesterday_hot_plates),
        )
        today_limit_truth, limit_truth_notes = limit_truth_result
        broken_board_truth, broken_notes = broken_result
        first_failed_truth, first_failed_notes = first_failed_result
        yesterday_limit_pool, yest_pool_notes = yest_pool_result
        today_hot_plates, today_hot_notes = today_hot_result
        yesterday_hot_plates, yesterday_hot_notes = yesterday_hot_result

        (
            stock_plate_writebacks,
            stock_reason_writebacks,
            enrichment_notes,
        ) = await self._enrich_writebacks_from_yest_pool(
            trade_date=trade_date,
            yesterday_limit_pool=yesterday_limit_pool,
            stock_plate_writebacks=stock_plate_writebacks,
            stock_reason_writebacks=stock_reason_writebacks,
        )

        eod_kline_truth, eod_notes = await self._fetch_baostock_truth_rows(
            trade_date=trade_date,
            signal_snapshot=signal_snapshot,
            today_limit_truth=today_limit_truth,
        )

        bundle = build_recap_input_bundle(
            trade_date=trade_date,
            signal_snapshot=signal_snapshot,
            today_limit_truth=today_limit_truth,
            yesterday_limit_pool=yesterday_limit_pool,
            today_hot_plates=today_hot_plates,
            yesterday_hot_plates=yesterday_hot_plates,
            broken_board_truth=broken_board_truth,
            first_failed_truth=first_failed_truth,
            eod_kline_truth=eod_kline_truth,
            stock_plate_writebacks=stock_plate_writebacks,
            stock_reason_writebacks=stock_reason_writebacks,
        )
        extra_notes = (
            list(limit_truth_notes)
            + list(broken_notes)
            + list(first_failed_notes)
            + list(yest_pool_notes)
            + list(today_hot_notes)
            + list(yesterday_hot_notes)
            + list(enrichment_notes)
            + list(eod_notes)
        )
        if not extra_notes:
            return bundle
        return RecapInputBundle(
            trade_date=bundle.trade_date,
            signal_snapshot=bundle.signal_snapshot,
            today_limit_truth=bundle.today_limit_truth,
            yesterday_limit_pool=bundle.yesterday_limit_pool,
            today_hot_plates=bundle.today_hot_plates,
            yesterday_hot_plates=bundle.yesterday_hot_plates,
            broken_board_truth=bundle.broken_board_truth,
            first_failed_truth=bundle.first_failed_truth,
            eod_kline_truth=bundle.eod_kline_truth,
            stock_plate_writebacks=bundle.stock_plate_writebacks,
            stock_reason_writebacks=bundle.stock_reason_writebacks,
            notes=bundle.notes + tuple(extra_notes),
        )

    def run_collision(self, bundle: RecapInputBundle) -> RecapCollisionResult:
        today_truth_map = {
            _normalize_symbol(row.get("symbol")): row
            for row in bundle.today_limit_truth
            if row.get("symbol")
        }
        success_codes = set(today_truth_map.keys())
        yest_pool_map = {
            _normalize_symbol(row.get("symbol")): row
            for row in bundle.yesterday_limit_pool
            if row.get("symbol")
        }
        today_plate_map = {
            str(row.get("plate_name") or ""): row
            for row in bundle.today_hot_plates
            if row.get("plate_name")
        }
        yest_plate_map = {
            str(row.get("plate_name") or ""): row
            for row in bundle.yesterday_hot_plates
            if row.get("plate_name")
        }
        broken_codes = {
            _normalize_symbol(row.get("symbol"))
            for row in bundle.broken_board_truth
            if row.get("symbol")
        }
        first_failed_codes = {
            _normalize_symbol(row.get("symbol"))
            for row in bundle.first_failed_truth
            if row.get("symbol")
        }
        eod_truth_map = {
            _normalize_symbol(row.get("symbol")): row
            for row in bundle.eod_kline_truth
            if row.get("symbol")
        }

        signal_hit_rows: list[dict[str, Any]] = []
        signal_miss_rows: list[dict[str, Any]] = []
        false_positive_rows: list[dict[str, Any]] = []
        enrichment_miss_rows: list[dict[str, Any]] = []

        for sig in bundle.signal_snapshot:
            code = _normalize_symbol(sig.get("code") or sig.get("symbol"))
            if not code:
                continue
            eod_row = eod_truth_map.get(code, {})
            row = {
                "symbol": code,
                "signal": sig,
                "close": eod_row.get("close"),
                "pct_chg": eod_row.get("pct_chg"),
                "plate": bundle.stock_plate_writebacks.get(code),
                "reason": bundle.stock_reason_writebacks.get(code),
            }
            if code in success_codes:
                truth_row = today_truth_map.get(code, {})
                row["lb_days"] = truth_row.get("lb_days")
                row["result"] = "limit_up"
                signal_hit_rows.append(row)
            else:
                row["result"] = "not_limit_up"
                row["is_broken_board"] = code in broken_codes
                false_positive_rows.append(row)

        for code, yest_row in yest_pool_map.items():
            if code in success_codes:
                continue
            signal_miss_rows.append(
                {
                    "symbol": code,
                    "yesterday": yest_row,
                    "plate": bundle.stock_plate_writebacks.get(code) or yest_row.get("plate"),
                    "reason": bundle.stock_reason_writebacks.get(code),
                    "is_broken_board": code in broken_codes,
                    "is_first_failed": code in first_failed_codes,
                    "result": "failed_continuation",
                }
            )
            if not bundle.stock_plate_writebacks.get(code) and not yest_row.get("plate"):
                enrichment_miss_rows.append({"symbol": code, "missing": "market:stock_plate"})
            if not bundle.stock_reason_writebacks.get(code):
                enrichment_miss_rows.append({"symbol": code, "missing": "market:stock_reason"})

        plate_rotation_rows = []
        for plate_name, today_row in today_plate_map.items():
            prev_rank = yest_plate_map.get(plate_name, {}).get("rank")
            plate_rotation_rows.append(
                {
                    "plate_name": plate_name,
                    "today_rank": today_row.get("rank"),
                    "yesterday_rank": prev_rank,
                    "is_new_mainline": prev_rank is None,
                }
            )
        plate_rotation_rows.sort(
            key=lambda row: (
                9999 if row.get("today_rank") is None else int(row["today_rank"]),
                str(row.get("plate_name") or ""),
            )
        )

        ladder_totals: dict[int, list[int]] = {}
        for code, yest_row in yest_pool_map.items():
            lb_days = int(yest_row.get("lb_days") or 0)
            ladder_totals.setdefault(lb_days, [0, 0])
            ladder_totals[lb_days][0] += 1
            today_lb = int(today_truth_map.get(code, {}).get("lb_days") or 0)
            if today_lb > lb_days or (code in success_codes and lb_days >= 9):
                ladder_totals[lb_days][1] += 1

        ladder_stats_rows = [
            {"lb_days": lb_days, "total": total, "success": success}
            for lb_days, (total, success) in sorted(ladder_totals.items(), reverse=True)
        ]

        notes = list(bundle.notes)
        notes.append(f"broken boards fetched: {len(bundle.broken_board_truth)}")
        notes.append(f"first failed fetched: {len(bundle.first_failed_truth)}")
        notes.append(f"eod kline rows fetched: {len(bundle.eod_kline_truth)}")

        return RecapCollisionResult(
            trade_date=bundle.trade_date,
            signal_hit_rows=signal_hit_rows,
            signal_miss_rows=signal_miss_rows,
            false_positive_rows=false_positive_rows,
            plate_rotation_rows=plate_rotation_rows,
            ladder_stats_rows=ladder_stats_rows,
            enrichment_miss_rows=enrichment_miss_rows,
            notes=tuple(notes),
        )

    def render_text_report(self, result: RecapCollisionResult) -> str:
        lines = [
            f"# Market Recap {result.trade_date}",
            "",
            "## Signal Collision",
            f"- signal hits: {len(result.signal_hit_rows)}",
            f"- false positives: {len(result.false_positive_rows)}",
            f"- continuation failures: {len(result.signal_miss_rows)}",
            f"- enrichment misses: {len(result.enrichment_miss_rows)}",
            "",
            "## Plate Rotation",
        ]
        if result.plate_rotation_rows:
            for row in result.plate_rotation_rows[:10]:
                lines.append(
                    f"- {row.get('plate_name')}: today_rank={row.get('today_rank')} "
                    f"yesterday_rank={row.get('yesterday_rank')} new_mainline={row.get('is_new_mainline')}"
                )
        else:
            lines.append("- no plate rotation rows")

        lines.append("")
        lines.append("## Ladder Stats")
        if result.ladder_stats_rows:
            for row in result.ladder_stats_rows:
                total = int(row.get("total") or 0)
                success = int(row.get("success") or 0)
                rate = round((success / total) * 100, 1) if total else 0.0
                lines.append(f"- {row.get('lb_days')}B -> total={total} success={success} rate={rate}%")
        else:
            lines.append("- no ladder stats")

        if result.notes:
            lines.append("")
            lines.append("## Notes")
            lines.extend(f"- {note}" for note in result.notes)

        return "\n".join(lines)

    def persist_text_report(
        self,
        trade_date: str,
        report_text: str,
        *,
        ttl_seconds: int = 30 * 24 * 60 * 60,
        write_enabled: bool = True,
    ) -> RecapPersistResult:
        tag = trade_date.replace("-", "")
        redis_key = f"market:recap:{tag}:report"
        notes: list[str] = []
        persisted = False
        if write_enabled:
            try:
                self.redis.set(redis_key, report_text, ex=ttl_seconds)
                persisted = True
            except Exception as exc:
                notes.append(f"redis persist failed: {exc}")
        else:
            notes.append("write disabled; report not persisted")
        return RecapPersistResult(
            trade_date=trade_date,
            redis_key=redis_key,
            report_text=report_text,
            persisted=persisted,
            notes=tuple(notes),
        )

    async def build_run_and_persist(
        self,
        trade_date: str,
        previous_trade_date: str,
        *,
        write_enabled: bool = True,
    ) -> tuple[RecapInputBundle, RecapCollisionResult, RecapPersistResult]:
        bundle = await self.build_real_input_bundle(trade_date, previous_trade_date)
        collision = self.run_collision(bundle)
        report_text = self.render_text_report(collision)
        persist_result = self.persist_text_report(
            trade_date=trade_date,
            report_text=report_text,
            write_enabled=write_enabled,
        )
        return bundle, collision, persist_result
