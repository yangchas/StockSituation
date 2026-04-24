from __future__ import annotations

import json
import re
from typing import Any, Iterable, Sequence


PLATE_MAPPING_S2P_KEY = "config:plate_mapping:s2p"
PLATE_MAPPING_INFO_KEY = "config:plate_mapping:info"
RUNTIME_PRIMARY_PLATE_KEY = "market:stock_plate"
RUNTIME_REASON_KEY = "market:stock_reason"


GENERIC_PLATE_NAMES = {
    "国企改革",
    "地方国企改革",
    "央企改革",
    "国企",
    "央企",
    "中字头",
    "融资融券",
    "转融券",
    "昨日涨停",
    "昨日曾涨停",
    "昨日首板",
    "证金持股",
    "汇金持股",
    "MSCI中国",
    "沪股通",
    "深股通",
}

GENERIC_PLATE_KEYWORDS = (
    "国企",
    "央企",
    "融资融券",
    "转融券",
    "昨日涨停",
    "昨日首板",
    "昨日曾涨停",
    "证金持股",
    "汇金持股",
    "MSCI",
    "沪股通",
    "深股通",
)

PREFERRED_PLATE_KEYWORDS = (
    "机器人",
    "算力",
    "人工智能",
    "AI",
    "半导体",
    "芯片",
    "低空",
    "飞行汽车",
    "华为",
    "军工",
    "光模块",
    "液冷",
    "铜缆",
)

_SPLIT_PATTERN = re.compile(r"[+,，,、/|；;]+")
_REASON_TAIL_PATTERN = re.compile(r"[。.].*$")

EXTRA_GENERIC_PLATE_NAMES = {
    "数字经济",
    "机器人",
    "金融",
    "地产链",
    "金融概念",
}


def _normalize_symbol(value: Any) -> str:
    text = str(value or "").strip()
    if "." in text:
        text = text.split(".")[0]
    return text[-6:] if text else ""


def normalize_plate_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = _REASON_TAIL_PATTERN.sub("", text)
    for suffix in ("概念", "板块", "题材"):
        if text.endswith(suffix) and len(text) > len(suffix) + 1:
            text = text[: -len(suffix)]
    return text.strip()


def is_generic_plate(name: str) -> bool:
    cleaned = normalize_plate_name(name)
    if not cleaned:
        return True
    if cleaned in EXTRA_GENERIC_PLATE_NAMES:
        return True
    if cleaned in GENERIC_PLATE_NAMES:
        return True
    return any(keyword in cleaned for keyword in GENERIC_PLATE_KEYWORDS)


def split_plate_tokens(*values: Any) -> list[str]:
    tokens: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        for part in _SPLIT_PATTERN.split(text):
            cleaned = normalize_plate_name(part)
            if not cleaned or cleaned in tokens:
                continue
            tokens.append(cleaned)
    return tokens


def merge_theme_lists(existing: Iterable[str], new_values: Iterable[str]) -> list[str]:
    merged: list[str] = []
    for raw in list(existing) + list(new_values):
        cleaned = normalize_plate_name(raw)
        if not cleaned or cleaned in merged:
            continue
        merged.append(cleaned)
    return merged


def choose_primary_plate(candidates: Sequence[str], fallback: str = "") -> str:
    cleaned_candidates = merge_theme_lists((), candidates)
    preferred = [name for name in cleaned_candidates if not is_generic_plate(name)]
    if not preferred:
        fallback_clean = normalize_plate_name(fallback)
        if fallback_clean:
            return fallback_clean
        return cleaned_candidates[0] if cleaned_candidates else ""
    for name in preferred:
        if any(keyword in name for keyword in PREFERRED_PLATE_KEYWORDS):
            return name
    return preferred[0]


def build_plate_candidates_from_reason(*, reason: str = "", group_str: str = "", gnsm: str = "") -> list[str]:
    candidates = split_plate_tokens(group_str, gnsm)
    if reason:
        head = _REASON_TAIL_PATTERN.sub("", reason)
        candidates = merge_theme_lists(candidates, split_plate_tokens(head))
    return candidates


def decode_theme_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return merge_theme_lists((), raw)
    if isinstance(raw, tuple):
        return merge_theme_lists((), list(raw))
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except Exception:
            return split_plate_tokens(text)
        return decode_theme_list(parsed)
    return []


def encode_theme_list(values: Iterable[str]) -> str:
    return json.dumps(merge_theme_lists((), values), ensure_ascii=False)


def merge_theme_payload(existing_raw: Any, new_values: Iterable[str]) -> tuple[list[str], str]:
    merged = merge_theme_lists(decode_theme_list(existing_raw), new_values)
    return merged, encode_theme_list(merged)


def build_runtime_writebacks_from_reasons(
    *,
    symbol: str,
    reason_rows: Sequence[dict[str, Any]],
    existing_themes: Sequence[str] = (),
    fallback_plate: str = "",
) -> dict[str, dict[str, Any]]:
    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        return {
            PLATE_MAPPING_S2P_KEY: {},
            RUNTIME_PRIMARY_PLATE_KEY: {},
            RUNTIME_REASON_KEY: {},
        }

    reason_texts: list[str] = []
    candidates = list(existing_themes)
    for row in reason_rows:
        reason = str(row.get("reason") or "")
        if reason:
            reason_texts.append(reason)
        candidates = merge_theme_lists(
            candidates,
            build_plate_candidates_from_reason(
                reason=reason,
                group_str=row.get("group_str") or "",
                gnsm=row.get("gnsm") or "",
            ),
        )

    primary_plate = choose_primary_plate(candidates, fallback=fallback_plate)
    return {
        PLATE_MAPPING_S2P_KEY: {normalized_symbol: list(candidates)} if candidates else {},
        RUNTIME_PRIMARY_PLATE_KEY: {normalized_symbol: primary_plate} if primary_plate else {},
        RUNTIME_REASON_KEY: {normalized_symbol: reason_texts[0]} if reason_texts else {},
    }
