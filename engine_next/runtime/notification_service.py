from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import smtplib
import ssl
from dataclasses import dataclass
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request

from engine_next.domain.enums import RunPhase

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RuntimeNotificationPayload:
    category: str
    phase_label: str
    subject: str
    body: str
    digest: str
    signal_digest: str
    html_body: str | None = None


class RuntimeNotificationService:
    """Thin side-channel notifier for operator-facing runtime summaries."""

    DEDUPE_TTL_SECONDS = 2 * 24 * 60 * 60
    DEFAULT_CONFIG_CANDIDATES = (
        "engine_next_notify.json",
        "config/engine_next_notify.json",
    )

    def __init__(self, *, redis_client: Any | None = None) -> None:
        self._redis = redis_client
        self._memory_dedup: dict[str, str] = {}
        self._config_path = self._resolve_config_path()
        self._config = self._load_config_dict(self._config_path)
        smtp_config = self._smtp_config()
        webhook_config = self._webhook_config()
        self._enabled = self._env_or_config_bool("ENGINE_NEXT_NOTIFY_ENABLED", self._config.get("enabled"), default=False)
        self._smtp_host = self._env_or_config_text("ENGINE_NEXT_NOTIFY_SMTP_HOST", smtp_config.get("host"))
        self._smtp_port = self._env_or_config_int("ENGINE_NEXT_NOTIFY_SMTP_PORT", smtp_config.get("port"), default=465)
        self._smtp_user = self._env_or_config_text("ENGINE_NEXT_NOTIFY_SMTP_USER", smtp_config.get("user"))
        self._smtp_password = self._env_or_config_text("ENGINE_NEXT_NOTIFY_SMTP_PASSWORD", smtp_config.get("password"))
        self._smtp_from = self._env_or_config_text("ENGINE_NEXT_NOTIFY_EMAIL_FROM", smtp_config.get("from"))
        self._smtp_to = self._env_or_config_csv("ENGINE_NEXT_NOTIFY_EMAIL_TO", smtp_config.get("to"))
        self._smtp_starttls = self._env_or_config_bool("ENGINE_NEXT_NOTIFY_SMTP_STARTTLS", smtp_config.get("starttls"), default=False)
        self._webhook_urls = self._env_or_config_csv("ENGINE_NEXT_NOTIFY_WEBHOOK_URL", webhook_config.get("urls"))
        self._webhook_timeout = self._env_or_config_float("ENGINE_NEXT_NOTIFY_WEBHOOK_TIMEOUT", webhook_config.get("timeout"), default=5.0)
        self._dedupe_prefix = self._env_or_config_text(
            "ENGINE_NEXT_NOTIFY_DEDUPE_PREFIX",
            self._config.get("dedupe_prefix"),
            default="engine_next:notify",
        )

    @property
    def enabled(self) -> bool:
        return self._enabled and bool(self._smtp_to or self._webhook_urls)

    def _smtp_config(self) -> dict[str, Any]:
        value = self._config.get("smtp")
        return value if isinstance(value, dict) else {}

    def _webhook_config(self) -> dict[str, Any]:
        value = self._config.get("webhooks")
        return value if isinstance(value, dict) else {}

    def notify_if_needed(
        self,
        *,
        result,
        request,
        summary_text: str,
    ) -> bool:
        if not self.enabled:
            return False
        if getattr(request, "historical_replay", False):
            return False
        if request.now.strftime("%Y-%m-%d") != str(getattr(request, "trade_date", "") or "").strip():
            return False
        payload = self._build_payload(result=result, request=request, summary_text=summary_text)
        if payload is None:
            return False
        dedupe_key = f"{self._dedupe_prefix}:{request.trade_date}:{payload.category}"
        if self._is_duplicate(dedupe_key=dedupe_key, digest=payload.signal_digest):
            return False
        delivered = False
        if self._smtp_to:
            delivered = self._send_email(payload) or delivered
        if self._webhook_urls:
            delivered = self._send_webhooks(payload, trade_date=request.trade_date) or delivered
        if delivered:
            self._remember_digest(dedupe_key=dedupe_key, digest=payload.signal_digest)
        return delivered

    def notify_auction_report(self, *, report, request, category: str = "auction_evidence") -> bool:
        """Send an already-built report; the caller owns fact provenance."""

        if not self.enabled or not self._smtp_to or getattr(request, "historical_replay", False):
            return False
        trade_date = str(getattr(request, "trade_date", "") or "").strip()
        if not trade_date or str(report.metadata.get("trade_date") or "").strip() != trade_date:
            return False
        if report.metadata.get("data_origin") not in {"production_realtime", "production_capture", "current_cache_only"}:
            return False
        payload = RuntimeNotificationPayload(
            category=category,
            phase_label="竞价事实" if category == "auction_evidence" else "开盘事实",
            subject=report.subject,
            body=report.text_body,
            digest=report.html_sha256,
            signal_digest=report.html_sha256,
            html_body=report.html_body,
        )
        return self._deliver_email_once(payload=payload, trade_date=trade_date)

    def notify_open_confirmation_report(self, *, report, request, category: str = "opening_facts") -> bool:
        """Send an already-built opening-facts report through the same notifier.

        The notifier does not interpret facts and does not retry an ambiguous
        provider result.  Application-level dedup remains keyed by
        ``trade_date + category``.
        """
        if not self.enabled or not self._smtp_to or getattr(request, "historical_replay", False):
            return False
        trade_date = str(getattr(request, "trade_date", "") or "").strip()
        metadata = getattr(report, "metadata", {}) or {}
        if not trade_date or str(metadata.get("trade_date") or "").strip() != trade_date:
            return False
        if metadata.get("data_origin") not in {"production_realtime", "production_capture", "current_cache_only"}:
            return False
        html_body = getattr(report, "html_body", None)
        text_body = getattr(report, "text_body", None) or getattr(report, "markdown_body", None)
        subject = str(getattr(report, "subject", "") or "").strip()
        if not subject or not text_body:
            return False
        digest = str(getattr(report, "html_sha256", "") or hashlib.sha256(str(html_body or text_body).encode("utf-8")).hexdigest())
        payload = RuntimeNotificationPayload(
            category=category,
            phase_label="开盘事实",
            subject=subject,
            body=str(text_body),
            digest=digest,
            signal_digest=digest,
            html_body=html_body,
        )
        return self._deliver_email_once(payload=payload, trade_date=trade_date)

    def _deliver_email_once(self, *, payload: RuntimeNotificationPayload, trade_date: str) -> bool:
        dedupe_key = f"{self._dedupe_prefix}:{trade_date}:{payload.category}"
        # Claim before touching SMTP.  A timeout may mean the provider accepted
        # the message; therefore an automatic second attempt is never made.
        if not self._claim_delivery_slot(dedupe_key=dedupe_key, digest=payload.signal_digest):
            return False
        delivered = self._send_email(payload)
        if delivered:
            self._remember_digest(dedupe_key=dedupe_key, digest=payload.signal_digest)
        return delivered

    def _claim_delivery_slot(self, *, dedupe_key: str, digest: str) -> bool:
        if dedupe_key in self._memory_dedup:
            return False
        marker = f"claimed:{digest}"
        try:
            if self._redis is not None and hasattr(self._redis, "setnx"):
                claimed = bool(self._redis.setnx(dedupe_key, marker))
                if claimed and hasattr(self._redis, "expire"):
                    self._redis.expire(dedupe_key, self.DEDUPE_TTL_SECONDS)
                if not claimed:
                    return False
            elif self._redis is not None and hasattr(self._redis, "get") and self._redis.get(dedupe_key):
                return False
        except Exception:
            logger.debug("notification dedupe claim failed; using local claim", exc_info=True)
        self._memory_dedup[dedupe_key] = marker
        return True

    def _build_payload(self, *, result, request, summary_text: str) -> RuntimeNotificationPayload | None:
        normalized_summary = str(summary_text or "").strip()
        if not normalized_summary:
            return None
        category = self._resolve_category(result=result, summary_text=normalized_summary)
        if not category:
            return None
        phase_label_map = {
            "auction": "竞价",
            "open_confirm": "开盘确认",
            "postmarket": "盘后",
        }
        phase_label = phase_label_map[category]
        if category == "postmarket":
            if "降级说明 |" in normalized_summary:
                phase_label = "正式复盘降级"
            elif "冻结说明 |" in normalized_summary or "系统状态：冻结复盘中" in normalized_summary:
                phase_label = "收盘快速复盘"
            elif "复盘就绪" in normalized_summary:
                phase_label = "正式复盘"
        subject = f"[A股策略][{request.trade_date}][{phase_label}]"
        body = self._compose_body(
            category=category,
            trade_date=request.trade_date,
            phase_label=phase_label,
            summary_text=normalized_summary,
        )
        selected_lines = self._select_lines(normalized_summary, category=category)
        signal_text = self._material_signal_text(selected_lines)
        if not signal_text:
            return None
        signal_digest = hashlib.sha1(self._digest_text(signal_text).encode("utf-8")).hexdigest()
        digest = hashlib.sha1(self._digest_text(body).encode("utf-8")).hexdigest()
        return RuntimeNotificationPayload(
            category=category,
            phase_label=phase_label,
            subject=subject,
            body=body,
            digest=digest,
            signal_digest=signal_digest,
        )

    def _resolve_category(self, *, result, summary_text: str) -> str:
        if result.phase == RunPhase.AUCTION and "当前阶段：竞价" in summary_text:
            return "auction"
        if result.phase == RunPhase.INTRADAY and "当前阶段：开盘确认" in summary_text:
            return "open_confirm"
        if result.phase == RunPhase.POSTMARKET and "当前阶段：盘后" in summary_text:
            return "postmarket"
        return ""

    def _compose_body(self, *, category: str, trade_date: str, phase_label: str, summary_text: str) -> str:
        selected_lines = self._select_lines(summary_text, category=category)
        header = [
            f"交易日：{trade_date}",
            f"阶段：{phase_label}",
            "",
        ]
        return "\n".join((*header, *selected_lines)).strip()

    @classmethod
    def _select_lines(cls, summary_text: str, *, category: str = "") -> tuple[str, ...]:
        lines = [str(line).rstrip() for line in str(summary_text or "").splitlines() if str(line).strip()]
        if not lines:
            return ()
        if category == "postmarket":
            return cls._select_postmarket_lines(lines)
        if category in {"auction", "open_confirm"}:
            return cls._select_runtime_lines(lines, category=category, limit=72)
        prefixes = (
            "当前阶段：",
            "系统状态：",
            "行情状态：",
            "竞价状态：",
            "策略看板 |",
            "情绪总览 |",
            "盘前预案 |",
            "【盘中作战摘要】",
            "  作战结论 |",
            "盘面翻译 |",
            "主线裁判 |",
            "主叙事 |",
            "时间迁移 |",
            "策略异常 |",
            "基础行情 |",
            "观察理由 |",
            "证伪条件 |",
            "【竞价执行图】",
            "【开盘执行图】",
            "【盘中执行图】",
            "  进攻 |",
            "  近买点 |",
            "  跟踪 |",
            "  修复 |",
            "  回避 |",
            "【主买点池】",
            "【核心观察池】",
            "【明日观察池】",
            "主买理由 |",
            "观察理由 |",
            "明日理由 |",
            "推票诊断 |",
            "过滤追踪 |",
            "买点分布 |",
            "数量摘要 |",
            "【风险提示】",
            "  - 回避 |",
            "  - 模式 |",
            "  - 数据 |",
        )
        selected: list[str] = []
        seen: set[str] = set()
        for line in lines:
            if line.startswith(prefixes):
                if line not in seen:
                    selected.append(line)
                    seen.add(line)
        if not selected:
            return tuple(lines[:24])
        return tuple(selected[:48])

    @classmethod
    def _select_runtime_lines(cls, lines: list[str], *, category: str, limit: int) -> tuple[str, ...]:
        selected: list[str] = []
        selected.extend(cls._pick_first_lines(lines, ("当前阶段：", "系统状态：", "行情状态：", "竞价状态：")))

        selected.append("【短线看盘】")
        selected.extend(cls._pick_first_lines(lines, ("策略看板 |", "情绪总览 |")))
        selected.extend(cls._collect_filtered_block(
            lines,
            "【盘中作战摘要】",
            wanted_prefixes=("  作战结论 |", "  盘面翻译 |", "  当前动作 |", "  风险约束 |"),
            max_lines=5,
        ))

        selected.append("【资金主线】")
        if category == "auction":
            selected.extend(cls._collect_block(lines, "【梯队映射】", max_lines=7))
            selected.extend(cls._collect_block(lines, "【题材竞价分桶】", max_lines=8))
            selected.extend(cls._pick_first_lines(lines, ("盘面翻译 |", "主线裁判 |", "主叙事 |", "时间迁移 |")))
        else:
            selected.extend(cls._collect_filtered_block(
                lines,
                "【盘中作战摘要】",
                wanted_prefixes=("  盘面翻译 |", "  主线裁判 |", "  变化迁移 |", "  主线证据 |"),
                max_lines=5,
            ))
            selected.extend(cls._pick_first_lines(lines, ("盘面翻译 |", "主线裁判 |", "主叙事 |", "时间迁移 |")))

        selected.append("【交易机会】")
        if category == "auction":
            selected.extend(cls._collect_filtered_block(
                lines,
                "【竞价执行图】",
                wanted_prefixes=("  进攻 |", "  近买点 |", "  跟踪 |", "  修复 |"),
                max_lines=6,
            ))
        else:
            selected.extend(cls._collect_filtered_block(
                lines,
                "【开盘执行图】",
                wanted_prefixes=("  进攻 |", "  近买点 |", "  跟踪 |", "  修复 |"),
                max_lines=6,
            ))
        selected.extend(cls._collect_block(lines, "【主买点池】", max_lines=7))
        selected.extend(cls._collect_block(lines, "【核心观察池】", max_lines=8))
        selected.extend(cls._collect_block(lines, "【强事实候选观察】", max_lines=5))
        selected.extend(cls._collect_filtered_block(
            lines,
            "【盘中作战摘要】",
            wanted_prefixes=("  机会个股 |",),
            max_lines=2,
        ))
        selected.extend(cls._pick_first_lines(lines, ("主买理由 |", "观察理由 |", "证伪条件 |")))

        selected.append("【避坑与证伪】")
        if category == "auction":
            selected.extend(cls._collect_filtered_block(
                lines,
                "【竞价执行图】",
                wanted_prefixes=("  回避 |",),
                max_lines=3,
            ))
        else:
            selected.extend(cls._collect_filtered_block(
                lines,
                "【开盘执行图】",
                wanted_prefixes=("  回避 |",),
                max_lines=3,
            ))
        selected.extend(cls._collect_filtered_block(
            lines,
            "【盘中作战摘要】",
            wanted_prefixes=("  避坑方向 |", "  推票诊断 |", "  过滤追踪 |", "  买点分布 |"),
            max_lines=5,
        ))
        selected.extend(cls._pick_first_lines(lines, ("推票诊断 |", "过滤追踪 |", "买点分布 |")))
        selected.extend(cls._collect_risk_lines(lines))
        return cls._dedupe_and_compact(selected, limit=limit)

    @classmethod
    def _select_postmarket_lines(cls, lines: list[str]) -> tuple[str, ...]:
        selected: list[str] = []
        selected.extend(cls._pick_first_lines(lines, ("当前阶段：", "系统状态：", "行情状态：")))
        selected.extend(cls._pick_first_lines(lines, ("冻结说明 |", "降级说明 |")))

        selected.append("【收盘速览】")
        selected.extend(cls._pick_first_lines(lines, ("策略看板 |", "情绪总览 |")))
        selected.extend(cls._collect_filtered_block(
            lines,
            "【收盘定性】",
            wanted_prefixes=("  ↓ 结论 |", "  → 情绪分 |", "  ↓ 情绪分 |", "  × 情绪分 |", "  → 晋级率 |", "  ↓ 晋级率 |", "  ○ 核按钮率 |", "  → 红开率 |"),
            max_lines=6,
        ))

        selected.append("【资金主线】")
        selected.extend(cls._collect_filtered_block(
            lines,
            "【复盘主线裁判】",
            wanted_prefixes=("  主路线 |", "  备选路线 |", "  回避路线 |", "  动作翻译 |", "  数据口径 |"),
            max_lines=6,
        ))
        selected.extend(cls._collect_filtered_block(
            lines,
            "【游资复盘摘要】",
            wanted_prefixes=("  一句话总评 |", "  主线/次线/淘汰线 |", "  资金路线 |", "  强度代表 |", "  只能观察 |", "  明确避开 |"),
            max_lines=7,
        ))

        selected.append("【明日机会】")
        selected.extend(cls._collect_block(lines, "【明日观察池】", max_lines=5))
        selected.extend(cls._pick_first_lines(lines, ("盘面翻译 |", "主线裁判 |", "时间迁移 |")))
        selected.extend(cls._pick_first_lines(lines, ("明日理由 |", "证伪条件 |", "推票诊断 |")))

        selected.append("【避坑与风控】")
        selected.extend(cls._collect_risk_lines(lines))

        selected.append("【关键事实】")
        selected.extend(cls._collect_filtered_block(
            lines,
            "【个股行为分桶】",
            wanted_prefixes=(
                "  昨日涨停晋级 |",
                "  首板新发酵 |",
                "  低开转强 |",
                "  降级 |",
            ),
            max_lines=5,
        ))
        selected.extend(cls._collect_block(lines, "【题材阶段胜负】", max_lines=4))
        selected.extend(cls._collect_block(lines, "【今日热点】", max_lines=4))
        selected.extend(cls._collect_block(lines, "【涨停板块】", max_lines=3))
        return cls._dedupe_and_compact(selected, limit=42)

    @staticmethod
    def _pick_first_lines(lines: list[str], prefixes: tuple[str, ...]) -> list[str]:
        found: list[str] = []
        for prefix in prefixes:
            for line in lines:
                if line.startswith(prefix):
                    found.append(line)
                    break
        return found

    @classmethod
    def _collect_block(cls, lines: list[str], title: str, *, max_lines: int) -> list[str]:
        start = cls._find_line_index(lines, title)
        if start < 0:
            return []
        block: list[str] = []
        for idx in range(start, len(lines)):
            line = lines[idx]
            if idx > start and cls._is_block_title(line):
                break
            block.append(line)
            if len(block) >= max_lines:
                break
        return block

    @classmethod
    def _collect_filtered_block(
        cls,
        lines: list[str],
        title: str,
        *,
        wanted_prefixes: tuple[str, ...],
        max_lines: int,
    ) -> list[str]:
        block = cls._collect_block(lines, title, max_lines=80)
        if not block:
            return []
        selected = [block[0]]
        for line in block[1:]:
            if line.startswith(wanted_prefixes):
                selected.append(line)
            if len(selected) >= max_lines:
                break
        return selected

    @staticmethod
    def _find_line_index(lines: list[str], prefix: str) -> int:
        for idx, line in enumerate(lines):
            if line.startswith(prefix):
                return idx
        return -1

    @staticmethod
    def _is_block_title(line: str) -> bool:
        return bool(re.match(r"^【[^】]+】", str(line or "")))

    @classmethod
    def _collect_risk_lines(cls, lines: list[str]) -> list[str]:
        risk_lines: list[str] = []
        for title in ("【风险提示】", "【复盘风控】"):
            block = cls._collect_block(lines, title, max_lines=16)
            if not block:
                continue
            risk_lines.append(block[0])
            for line in block[1:]:
                if line.startswith(("  - 模式 |", "  - 缺失 |", "  - 数据 |")):
                    risk_lines.append(line)
                elif line.startswith("  - 回避 |"):
                    risk_lines.append(cls._compact_avoid_line(line))
            break
        return risk_lines

    @classmethod
    def _dedupe_and_compact(cls, lines: list[str], *, limit: int) -> tuple[str, ...]:
        selected: list[str] = []
        seen: set[str] = set()
        for raw_line in lines:
            line = cls._compact_notification_line(raw_line)
            if not line or line in seen:
                continue
            selected.append(line)
            seen.add(line)
            if len(selected) >= limit:
                break
        return tuple(selected)

    @staticmethod
    def _compact_notification_line(line: str) -> str:
        text = str(line or "").rstrip()
        logical_text = text.strip()
        if not logical_text:
            return ""
        if logical_text.startswith("推票诊断 |"):
            indexed_parts: dict[str, str] = {}
            payload = logical_text.split("|", 1)[1].strip()
            for part in payload.split(";"):
                part = part.strip()
                if "=" not in part:
                    continue
                key = part.split("=", 1)[0].strip()
                if key in {"候选", "切片", "主买", "观察", "近买", "展示主买", "展示观察", "阻塞", "买点", "门槛", "原因"}:
                    indexed_parts[key] = part
            order = ("候选", "主买", "观察", "近买", "展示主买", "展示观察", "阻塞", "门槛", "原因", "买点")
            keep_parts = [indexed_parts[key] for key in order if key in indexed_parts]
            if keep_parts:
                return "推票诊断 | " + ";".join(keep_parts[:9])
        if logical_text.startswith("过滤追踪 |") and len(logical_text) > 220:
            payload = logical_text.split("|", 1)[1].strip()
            if payload and payload != "-":
                items = [item.strip() for item in payload.split(";") if item.strip()]
                return "过滤追踪 | " + "; ".join(items[:3])
        if logical_text.startswith("买点分布 |") and len(logical_text) > 220:
            payload = logical_text.split("|", 1)[1].strip()
            if payload and payload != "-":
                items = [item.strip() for item in payload.split("；") if item.strip()]
                if len(items) <= 1:
                    items = [item.strip() for item in payload.split(";") if item.strip()]
                return "买点分布 | " + "; ".join(items[:4])
        if logical_text.startswith("时间迁移 |") and len(logical_text) > 220:
            chunks = [chunk.strip() for chunk in logical_text.split(";") if chunk.strip()]
            if chunks:
                return "; ".join(chunks[:3])
        if logical_text.startswith(("主线裁判 |", "主叙事 |")) and len(logical_text) > 220:
            chunks = [chunk.strip() for chunk in logical_text.split("|") if chunk.strip()]
            if len(chunks) > 1:
                return " | ".join(chunks[:5])
        if logical_text.startswith(("  昨日涨停晋级 |", "  首板新发酵 |", "  低开转强 |", "  赚钱效应 |", "  亏钱效应 |", "  大成交核心 |")):
            parts = [part.strip() for part in logical_text.split("|")]
            if len(parts) >= 3:
                return " | ".join(parts[:3])
        max_len = 220
        if len(text) > max_len:
            return text[: max_len - 1].rstrip() + "…"
        return text

    @staticmethod
    def _compact_avoid_line(line: str) -> str:
        text = str(line or "").rstrip()
        logical_text = text.strip()
        prefix = "  - 回避 |"
        if not logical_text.startswith("- 回避 |"):
            return text
        payload = logical_text.split("|", 1)[1].strip()
        if payload in {"", "-"}:
            return text
        items = [item.strip() for item in payload.split(",") if item.strip()]
        compact_items: list[str] = []
        for item in items[:3]:
            symbol = item.split("=", 1)[0].strip()
            action = ""
            buy_point = ""
            for token in item.split("/"):
                if token.startswith("买点="):
                    buy_point = token.split("=", 1)[1].strip()
                elif token in {"回避", "回避追高", "观察", "试错"}:
                    action = token
            compact_items.append("/".join(part for part in (symbol, action, buy_point) if part))
        suffix = f" 等{len(items)}只" if len(items) > 3 else ""
        return prefix + " " + ("; ".join(compact_items) + suffix if compact_items else payload[:180])

    @classmethod
    def _material_signal_text(cls, lines: tuple[str, ...]) -> str:
        material_lines = tuple(
            line
            for line in lines
            if cls._line_is_material_signal(line)
        )
        if not material_lines:
            return ""
        if not cls._has_actionable_or_diagnostic_signal(material_lines):
            return ""
        return "\n".join(material_lines)

    @staticmethod
    def _line_is_material_signal(line: str) -> bool:
        text = str(line or "").rstrip()
        if not text.strip():
            return False
        material_prefixes = (
            "情绪总览 |",
            "主线裁判 |",
            "主叙事 |",
            "时间迁移 |",
            "策略异常 |",
            "基础行情 |",
            "  进攻 |",
            "  近买点 |",
            "  跟踪 |",
            "  修复 |",
            "  回避 |",
            "主买理由 |",
            "观察理由 |",
            "明日理由 |",
            "证伪条件 |",
            "推票诊断 |",
            "过滤追踪 |",
            "买点分布 |",
            "数量摘要 |",
            "  - 模式 |",
            "【强事实候选观察】",
        )
        if not text.startswith(material_prefixes):
            return False
        noise_values = (
            "主线裁判 | -",
            "主叙事 | -",
            "时间迁移 | -",
            "证伪条件 | -",
            "  进攻 | 无",
            "  近买点 | 无",
            "  跟踪 | 无",
            "  修复 | 无",
            "  回避 | 无",
        )
        return text not in noise_values

    @staticmethod
    def _has_actionable_or_diagnostic_signal(lines: tuple[str, ...]) -> bool:
        text = "\n".join(lines)
        if "【主买点池】" in text or "主买理由 |" in text:
            return True
        if "【强事实候选观察】" in text:
            return True
        if re.search(r"  (进攻|近买点|跟踪|修复) \| (?!无\b).+", text):
            return True
        if "主线裁判 |" in text and not re.search(r"主线裁判 \| (-|无确认|未识别|策略推演未完成|.*执行=只观察)", text):
            return True
        if "主叙事 |" in text and not re.search(r"主叙事 \| (-|无明确主线|当前主叙事暂无|.*等待确认)", text):
            return True
        if "推票诊断 |" in text and not re.search(r"推票诊断 \| (全局决策缺失|无低风险买点|当前不生成个股推荐)", text):
            return True
        for line in lines:
            if line.startswith("买点分布 |"):
                payload = line.split("|", 1)[1].strip()
                if payload and payload != "-" and "watch_only" not in payload and "观察承接" not in payload:
                    return True
        if "  - 模式 |" in text and not re.search(r"只观察|等待|watch_only|fallback_facts_only", text):
            return True
        return False

    @staticmethod
    def _digest_text(text: str) -> str:
        normalized = str(text or "")
        normalized = re.sub(r"\d{2}:\d{2}:\d{2}", "HH:MM:SS", normalized)
        normalized = re.sub(r"\d{2}:\d{2}", "HH:MM", normalized)
        normalized = re.sub(r"滞后\s+\d+\s+秒", "滞后 N 秒", normalized)
        normalized = re.sub(r"lag=\d+s", "lag=Ns", normalized)
        normalized = re.sub(r"最新\s+HH:MM:SS", "最新 HH:MM:SS", normalized)
        normalized = re.sub(r"时间=HH:MM", "时间=HH:MM", normalized)
        return normalized.strip()

    def _is_duplicate(self, *, dedupe_key: str, digest: str) -> bool:
        cached = self._get_cached_digest(dedupe_key)
        return bool(cached and cached == digest)

    def _remember_digest(self, *, dedupe_key: str, digest: str) -> None:
        self._memory_dedup[dedupe_key] = digest
        try:
            if hasattr(self._redis, "setex"):
                self._redis.setex(dedupe_key, self.DEDUPE_TTL_SECONDS, digest)
            elif hasattr(self._redis, "set"):
                self._redis.set(dedupe_key, digest, ex=self.DEDUPE_TTL_SECONDS)
        except Exception:
            logger.debug("notification dedupe redis write failed", exc_info=True)

    def _get_cached_digest(self, dedupe_key: str) -> str:
        if dedupe_key in self._memory_dedup:
            return self._memory_dedup[dedupe_key]
        try:
            if hasattr(self._redis, "get"):
                value = self._redis.get(dedupe_key)
                if value:
                    text = value.decode("utf-8", errors="ignore") if isinstance(value, bytes) else str(value)
                    self._memory_dedup[dedupe_key] = text
                    return text
        except Exception:
            logger.debug("notification dedupe redis read failed", exc_info=True)
        return ""

    def _send_email(self, payload: RuntimeNotificationPayload) -> bool:
        if not (self._smtp_host and self._smtp_from and self._smtp_to):
            return False
        message = EmailMessage()
        message["Subject"] = payload.subject
        message["From"] = self._smtp_from
        message["To"] = ", ".join(self._smtp_to)
        message.set_content(payload.body, subtype="plain", charset="utf-8")
        if str(payload.html_body or "").strip():
            message.add_alternative(str(payload.html_body), subtype="html", charset="utf-8")
        try:
            if self._smtp_port == 465 and not self._smtp_starttls:
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL(self._smtp_host, self._smtp_port, context=context, timeout=10) as client:
                    if self._smtp_user and self._smtp_password:
                        client.login(self._smtp_user, self._smtp_password)
                    client.send_message(message)
            else:
                with smtplib.SMTP(self._smtp_host, self._smtp_port, timeout=10) as client:
                    client.ehlo()
                    if self._smtp_starttls:
                        context = ssl.create_default_context()
                        client.starttls(context=context)
                        client.ehlo()
                    if self._smtp_user and self._smtp_password:
                        client.login(self._smtp_user, self._smtp_password)
                    client.send_message(message)
            logger.info("runtime notification delivered | channel=email | subject=%s", payload.subject)
            return True
        except Exception:
            logger.exception("runtime notification failed | channel=email | subject=%s", payload.subject)
            return False

    def _send_webhooks(self, payload: RuntimeNotificationPayload, *, trade_date: str) -> bool:
        success = False
        request_body = json.dumps(
            {
                "subject": payload.subject,
                "trade_date": trade_date,
                "phase": payload.phase_label,
                "category": payload.category,
                "text": payload.body,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        for webhook_url in self._webhook_urls:
            try:
                req = urllib_request.Request(
                    webhook_url,
                    data=request_body,
                    headers={"Content-Type": "application/json; charset=utf-8"},
                    method="POST",
                )
                with urllib_request.urlopen(req, timeout=self._webhook_timeout) as resp:
                    status = int(getattr(resp, "status", 200) or 200)
                    if 200 <= status < 300:
                        success = True
                        logger.info(
                            "runtime notification delivered | channel=webhook | status=%s | subject=%s",
                            status,
                            payload.subject,
                        )
                    else:
                        logger.warning(
                            "runtime notification non-2xx | channel=webhook | status=%s | subject=%s",
                            status,
                            payload.subject,
                        )
            except urllib_error.HTTPError as exc:
                logger.warning(
                    "runtime notification failed | channel=webhook | status=%s | subject=%s",
                    getattr(exc, "code", "?"),
                    payload.subject,
                )
            except Exception:
                logger.exception("runtime notification failed | channel=webhook | subject=%s", payload.subject)
        return success

    @classmethod
    def _resolve_config_path(cls) -> Path | None:
        explicit = os.getenv("ENGINE_NEXT_NOTIFY_CONFIG", "").strip()
        candidates: list[Path] = []
        if explicit:
            candidates.append(Path(explicit).expanduser())
        candidates.extend(Path(item) for item in cls.DEFAULT_CONFIG_CANDIDATES)
        for path in candidates:
            try:
                resolved = path.resolve(strict=False)
            except Exception:
                resolved = path
            if resolved.exists() and resolved.is_file():
                return resolved
        return None

    @staticmethod
    def _load_config_dict(path: Path | None) -> dict[str, Any]:
        if path is None:
            return {}
        try:
            raw = path.read_text(encoding="utf-8")
            payload = json.loads(raw)
            if isinstance(payload, dict):
                logger.info("runtime notification config loaded | path=%s", path)
                return payload
        except Exception:
            logger.exception("runtime notification config load failed | path=%s", path)
        return {}

    @staticmethod
    def _parse_csv_value(raw: object) -> tuple[str, ...]:
        if raw is None:
            return ()
        if isinstance(raw, (list, tuple)):
            return tuple(str(item).strip() for item in raw if str(item).strip())
        text = str(raw).strip()
        if not text:
            return ()
        return tuple(item for item in (part.strip() for part in text.replace(";", ",").split(",")) if item)

    @staticmethod
    def _parse_bool_value(raw: object, *, default: bool = False) -> bool:
        if raw is None:
            return default
        if isinstance(raw, bool):
            return raw
        text = str(raw).strip().lower()
        if text in {"1", "true", "yes", "y", "on"}:
            return True
        if text in {"0", "false", "no", "n", "off"}:
            return False
        return default

    @staticmethod
    def _safe_float(value: object, *, default: float) -> float:
        try:
            return float(str(value).strip())
        except (TypeError, ValueError, AttributeError):
            return default

    def _env_or_config_text(self, env_name: str, config_value: object, *, default: str = "") -> str:
        raw = os.getenv(env_name, "")
        if raw.strip():
            return raw.strip()
        if config_value is None:
            return default
        text = str(config_value).strip()
        return text or default

    def _env_or_config_int(self, env_name: str, config_value: object, *, default: int) -> int:
        raw = os.getenv(env_name, "").strip()
        if raw:
            return self._safe_int(raw, default=default)
        if config_value is None:
            return default
        return self._safe_int(str(config_value), default=default)

    def _env_or_config_float(self, env_name: str, config_value: object, *, default: float) -> float:
        raw = os.getenv(env_name, "").strip()
        if raw:
            return self._safe_float(raw, default=default)
        if config_value is None:
            return default
        return self._safe_float(config_value, default=default)

    def _env_or_config_bool(self, env_name: str, config_value: object, *, default: bool) -> bool:
        raw = os.getenv(env_name, "").strip()
        if raw:
            return self._parse_bool_value(raw, default=default)
        return self._parse_bool_value(config_value, default=default)

    def _env_or_config_csv(self, env_name: str, config_value: object) -> tuple[str, ...]:
        raw = os.getenv(env_name, "").strip()
        if raw:
            return self._parse_csv_value(raw)
        return self._parse_csv_value(config_value)

    @staticmethod
    def _parse_csv_env(name: str) -> tuple[str, ...]:
        raw = os.getenv(name, "").strip()
        if not raw:
            return ()
        return tuple(
            item
            for item in (part.strip() for part in raw.replace(";", ",").split(","))
            if item
        )

    @staticmethod
    def _safe_int(value: str, *, default: int) -> int:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return default
