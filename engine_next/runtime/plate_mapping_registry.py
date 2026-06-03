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

INVALID_PLATE_KEYWORDS = (
    "公司位于",
    "主营业务",
    "主要为",
    "生产和销售",
    "研发",
    "客户提供",
    "主要产品",
    "产品包括",
    "产品应用于",
    "贴牌加工",
    "知名品牌",
    "公司已投",
    "互动易",
    "投资者关系",
    "招股说明书",
    "半年报",
    "三季报",
    "年报",
    "月日",
)

REGION_ONLY_PLATE_NAMES = {
    "北京市",
    "上海市",
    "天津市",
    "重庆市",
    "河北省",
    "山西省",
    "辽宁省",
    "吉林省",
    "黑龙江省",
    "江苏省",
    "浙江省",
    "安徽省",
    "福建省",
    "江西省",
    "山东省",
    "河南省",
    "湖北省",
    "湖南省",
    "广东省",
    "海南省",
    "四川省",
    "贵州省",
    "云南省",
    "陕西省",
    "甘肃省",
    "青海省",
    "台湾省",
    "内蒙古",
    "广西",
    "西藏",
    "宁夏",
    "新疆",
    "深圳市",
    "广州市",
    "杭州市",
    "苏州市",
    "南京市",
}

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

PRIMARY_PLATE_ALIAS_EXACT = {
    "\u73bb\u7483\u57fa\u677f": "\u82af\u7247",
    "\u5148\u8fdb\u5c01\u88c5": "\u82af\u7247",
    "\u5149\u82af\u7247": "\u82af\u7247",
    "HBM": "\u82af\u7247",
    "\u5b58\u50a8": "\u82af\u7247",
    "EDA": "\u82af\u7247",
    "\u9ad8\u901f\u8fde\u63a5": "\u901a\u4fe1",
    "\u5149\u7ea4": "\u901a\u4fe1",
    "CPO": "\u901a\u4fe1",
    "\u94dc\u7f06\u9ad8\u901f\u8fde\u63a5": "\u901a\u4fe1",
}

PRIMARY_PLATE_ALIAS_KEYWORDS = (
    ("\u82af\u7247", "\u82af\u7247"),
    ("\u534a\u5bfc\u4f53", "\u82af\u7247"),
    ("\u5148\u8fdb\u5c01\u88c5", "\u82af\u7247"),
    ("\u73bb\u7483\u57fa\u677f", "\u82af\u7247"),
    ("\u5149\u82af\u7247", "\u82af\u7247"),
    ("\u9ad8\u901f\u8fde\u63a5", "\u901a\u4fe1"),
    ("\u5149\u7ea4", "\u901a\u4fe1"),
    ("\u901a\u4fe1", "\u901a\u4fe1"),
)

_SPLIT_PATTERN = re.compile(r"[+,，,、/|；;]+")
_REASON_TAIL_PATTERN = re.compile(r"[。.].*$")
_TRAILING_BRACKET_DETAIL_PATTERN = re.compile(r"([（(].*[）)])$")
_PURE_NUMBER_PATTERN = re.compile(r"^\d+(?:\.\d+)?$")

EXTRA_GENERIC_PLATE_NAMES = {
    "数字经济",
    "机器人",
    "金融",
    "地产链",
    "金融概念",
}
HARD_GENERIC_PLATE_KEYWORDS = (
    "\u56fd\u4f01",
    "\u592e\u4f01",
    "\u4e2d\u5b57\u5934",
    "\u878d\u8d44\u878d\u5238",
    "\u8f6c\u878d\u5238",
    "\u6628\u65e5\u6da8\u505c",
    "\u6628\u65e5\u66fe\u6da8\u505c",
    "\u6628\u65e5\u9996\u677f",
    "\u8bc1\u91d1\u6301\u80a1",
    "\u6c47\u91d1\u6301\u80a1",
    "MSCI",
)


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
    if _TRAILING_BRACKET_DETAIL_PATTERN.search(text):
        head = re.split(r"[（(]", text, maxsplit=1)[0].strip()
        if head:
            text = head
    for suffix in ("概念", "板块", "题材"):
        if text.endswith(suffix) and len(text) > len(suffix) + 1:
            text = text[: -len(suffix)]
    if "一季报" in text or "一季度" in text:
        return "一季报增长"
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


def is_valid_plate_candidate(name: str) -> bool:
    cleaned = normalize_plate_name(name)
    if not cleaned:
        return False
    if cleaned in REGION_ONLY_PLATE_NAMES:
        return False
    if _PURE_NUMBER_PATTERN.fullmatch(cleaned):
        return False
    if len(cleaned) > 12:
        return False
    if cleaned.isascii() and len(cleaned) > 2:
        return False
    if any(keyword in cleaned for keyword in ("公告", "同比", "减亏", "上年增长", "晚")):
        return False
    if any(keyword in cleaned for keyword in INVALID_PLATE_KEYWORDS):
        return False
    return True


def is_hard_generic_plate(name: str) -> bool:
    cleaned = normalize_plate_name(name)
    if not cleaned:
        return True
    return any(keyword in cleaned for keyword in HARD_GENERIC_PLATE_KEYWORDS)


def split_plate_tokens(*values: Any) -> list[str]:
    tokens: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        for part in _SPLIT_PATTERN.split(text):
            cleaned = normalize_plate_name(part)
            if not cleaned or not is_valid_plate_candidate(cleaned) or cleaned in tokens:
                continue
            tokens.append(cleaned)
    return tokens


def merge_theme_lists(existing: Iterable[str], new_values: Iterable[str]) -> list[str]:
    merged: list[str] = []
    for raw in list(existing) + list(new_values):
        split_tokens = split_plate_tokens(raw)
        if split_tokens:
            for cleaned in split_tokens:
                if cleaned in merged:
                    continue
                merged.append(cleaned)
            continue
        cleaned = normalize_plate_name(raw)
        if not cleaned or not is_valid_plate_candidate(cleaned) or cleaned in merged:
            continue
        merged.append(cleaned)
    return merged


def choose_primary_plate(candidates: Sequence[str], fallback: str = "") -> str:
    cleaned_candidates = merge_theme_lists((), candidates)
    preferred = [name for name in cleaned_candidates if not is_generic_plate(name)]
    if not preferred:
        fallback_clean = normalize_plate_name(fallback)
        if fallback_clean and is_valid_plate_candidate(fallback_clean):
            return fallback_clean
        return cleaned_candidates[0] if cleaned_candidates else ""
    for name in preferred:
        if any(keyword in name for keyword in PREFERRED_PLATE_KEYWORDS):
            return name
    return preferred[0]


def build_plate_candidates_from_reason(*, reason: str = "", group_str: str = "", gnsm: str = "") -> list[str]:
    reason_head = str(reason or "").split("；", 1)[0].split(";", 1)[0].strip()
    candidates = split_plate_tokens(group_str, reason_head)
    if candidates:
        return candidates
    gnsm_text = str(gnsm or "").strip()
    if gnsm_text and len(gnsm_text) <= 24:
        return split_plate_tokens(gnsm_text)
    return []


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


def merge_theme_payload_prioritized(existing_raw: Any, new_values: Iterable[str]) -> tuple[list[str], str]:
    prioritized = merge_theme_lists((), new_values)
    merged = merge_theme_lists(prioritized, decode_theme_list(existing_raw))
    return merged, encode_theme_list(merged)


def prioritize_core_themes(
    primary_values: Iterable[str],
    secondary_values: Iterable[str] = (),
    *,
    max_count: int = 2,
) -> list[str]:
    ranked = merge_theme_lists((), primary_values)
    fallback = merge_theme_lists((), secondary_values)
    merged: list[str] = []
    for name in ranked:
        if name in merged:
            continue
        merged.append(name)
        if len(merged) >= max_count:
            return merged
    for name in fallback:
        if name in merged:
            continue
        merged.append(name)
        if len(merged) >= max_count:
            break
    return merged


def choose_pool_primary_plate(
    pool_candidates: Sequence[str],
    fallback_candidates: Sequence[str] = (),
    *,
    fallback: str = "",
) -> str:
    merged_pool = merge_theme_lists((), pool_candidates)
    for name in merged_pool:
        if name == "\u673a\u5668\u4eba":
            return name
        if not is_hard_generic_plate(name):
            return name
    return choose_primary_plate((*merged_pool, *fallback_candidates), fallback=fallback)


def collapse_runtime_primary_plate(name: str) -> str:
    cleaned = normalize_plate_name(name)
    if not cleaned:
        return ""
    exact = PRIMARY_PLATE_ALIAS_EXACT.get(cleaned)
    if exact:
        return exact
    for keyword, target in PRIMARY_PLATE_ALIAS_KEYWORDS:
        if keyword in cleaned:
            return target
    return cleaned


def choose_runtime_primary_plate(
    candidates: Sequence[str],
    *,
    fallback: str = "",
    pool_plate: str = "",
    reason_candidates: Sequence[str] = (),
) -> str:
    pool_candidates = merge_theme_lists((), split_plate_tokens(pool_plate))
    reason_list = merge_theme_lists((), reason_candidates)
    if pool_candidates:
        primary_candidate = choose_pool_primary_plate(
            candidates,
            reason_list,
            fallback=fallback or pool_plate,
        )
    else:
        primary_candidate = choose_primary_plate(candidates, fallback=fallback)

    candidate_family = collapse_runtime_primary_plate(primary_candidate)
    fallback_clean = normalize_plate_name(fallback)
    fallback_family = collapse_runtime_primary_plate(fallback_clean)
    pool_family = collapse_runtime_primary_plate(pool_candidates[0]) if pool_candidates else ""

    if fallback_clean and not is_generic_plate(fallback_clean):
        if not candidate_family:
            return fallback_clean
        if fallback_family == candidate_family:
            return fallback_clean
        if pool_family and pool_family == fallback_family:
            return fallback_clean
        if not pool_family:
            return fallback_clean

    return candidate_family or primary_candidate


def order_runtime_theme_candidates(
    candidates: Sequence[str],
    *,
    primary_plate: str = "",
) -> list[str]:
    ordered = merge_theme_lists((), candidates)
    if not ordered:
        return []

    primary = collapse_runtime_primary_plate(primary_plate) if primary_plate else ""
    if not primary:
        return ordered

    result: list[str] = []
    primary_aliases = {primary}
    for name in ordered:
        if not name:
            continue
        if collapse_runtime_primary_plate(name) == primary and primary not in result:
            result.append(name)
            primary_aliases.add(name)
    for name in ordered:
        if not name or name in result:
            continue
        if name in primary_aliases:
            continue
        result.append(name)
    return result


def build_yest_limit_theme_candidates(
    *,
    pool_plate: str = "",
    reason_candidates: Sequence[str] = (),
    existing_themes: Sequence[str] = (),
) -> list[str]:
    pool_candidates = split_plate_tokens(pool_plate)
    parsed_reason_candidates = merge_theme_lists((), reason_candidates)
    merged_existing = merge_theme_lists((), existing_themes)
    if not pool_candidates:
        if parsed_reason_candidates:
            return prioritize_core_themes(parsed_reason_candidates, merged_existing, max_count=2)
        return merged_existing

    primary = choose_pool_primary_plate(
        pool_candidates,
        parsed_reason_candidates,
        fallback=pool_plate or (parsed_reason_candidates[0] if parsed_reason_candidates else ""),
    )
    ordered: list[str] = []
    if primary:
        ordered.append(primary)
    for bucket in (parsed_reason_candidates, pool_candidates, merged_existing):
        for name in bucket:
            if not name or name in ordered:
                continue
            ordered.append(name)
    return ordered


def build_runtime_writebacks_from_reasons(
    *,
    symbol: str,
    reason_rows: Sequence[dict[str, Any]],
    existing_themes: Sequence[str] = (),
    fallback_plate: str = "",
    pool_plate: str = "",
) -> dict[str, dict[str, Any]]:
    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        return {
            PLATE_MAPPING_S2P_KEY: {},
            RUNTIME_PRIMARY_PLATE_KEY: {},
            RUNTIME_REASON_KEY: {},
        }

    reason_texts: list[str] = []
    reason_candidates: list[str] = []
    for row in reason_rows:
        reason = str(row.get("reason") or "")
        if reason:
            reason_texts.append(reason)
        reason_candidates = merge_theme_lists(
            reason_candidates,
            build_plate_candidates_from_reason(
                reason=reason,
                group_str=row.get("group_str") or "",
                gnsm=row.get("gnsm") or "",
            ),
        )
    candidates = build_yest_limit_theme_candidates(
        pool_plate=pool_plate,
        reason_candidates=reason_candidates,
        existing_themes=existing_themes,
    )
    if not candidates:
        candidates = merge_theme_lists(reason_candidates, existing_themes)

    primary_plate = choose_runtime_primary_plate(
        candidates,
        fallback=fallback_plate or pool_plate,
        pool_plate=pool_plate,
        reason_candidates=reason_candidates,
    )
    ordered_candidates = order_runtime_theme_candidates(candidates, primary_plate=primary_plate)
    return {
        PLATE_MAPPING_S2P_KEY: {normalized_symbol: ordered_candidates} if ordered_candidates else {},
        RUNTIME_PRIMARY_PLATE_KEY: {normalized_symbol: primary_plate} if primary_plate else {},
        RUNTIME_REASON_KEY: {normalized_symbol: reason_texts[0]} if reason_texts else {},
    }
