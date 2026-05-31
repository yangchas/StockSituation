from __future__ import annotations

from collections.abc import Iterable

from engine_next.domain.models import StockSelectionContext, StockStateSnapshot, ThemeSelectionContext
from engine_next.strategy_skill_layer.relative_amount import (
    is_relative_amount_top_or_fallback,
    relative_amount_floor,
    snapshot_amount_2m_top,
)


def dedupe_text_items(items: Iterable[str], *, limit: int, sep: str = "、") -> str:
    deduped: list[str] = []
    for item in items:
        if item and item not in deduped:
            deduped.append(item)
    return sep.join(deduped[:limit]) if deduped else ""


def rank_pct_bucket_text(value: float) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return "-"
    if number <= 0.20:
        return "top20%"
    if number <= 0.35:
        return "top35%"
    if number <= 0.50:
        return "top50%"
    return "back50%"


def snapshot_2m_follow_tag(snapshot: StockStateSnapshot | None, *, concise: bool = False) -> str:
    if snapshot is None or snapshot.auction_amount <= 0:
        return ""
    ratio = float(snapshot.amount_2m or 0.0) / float(snapshot.auction_amount or 1.0)
    if ratio >= 1.2:
        return "2mFollow=strong" if concise else "前2分钟强承接"
    if ratio < 0.75:
        return "2mFollow=weak" if concise else "前2分钟承接弱"
    if snapshot.amount_2m >= snapshot.auction_amount:
        return "" if concise else "换手承接跟上"
    return ""


def _snapshot_samples_or_empty(
    snapshots: Iterable[StockStateSnapshot] | None,
) -> tuple[StockStateSnapshot, ...] | None:
    if snapshots is None:
        return None
    if isinstance(snapshots, tuple):
        return snapshots
    return tuple(snapshots)


def leader_turnover_quality(snapshot: StockStateSnapshot) -> str:
    if (
        snapshot.open_pct >= 0.02
        and snapshot.auction_amount >= 20_000_000
        and (snapshot.speed_1m > 0 or snapshot_amount_2m_top(snapshot, max_rank_pct=0.25, fallback=30_000_000))
    ):
        if snapshot_amount_2m_top(snapshot, max_rank_pct=0.15, fallback=50_000_000) or snapshot.speed_1m > 0.01:
            return "放量确认"
        return "温和确认"
    if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot_amount_2m_top(snapshot, max_rank_pct=0.35, fallback=20_000_000):
        return "低开转强"
    if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
        return "回落修复"
    return "强弱待判"


def leader_truth_code(snapshot: StockStateSnapshot) -> str:
    amount_ratio_2m = (snapshot.amount_2m / snapshot.auction_amount) if snapshot.auction_amount > 0 else 0.0
    if (
        0.02 <= snapshot.open_pct <= 0.07
        and snapshot.auction_amount >= 30_000_000
        and (snapshot_amount_2m_top(snapshot, max_rank_pct=0.20, fallback=40_000_000) or snapshot.speed_1m > 0.01)
        and amount_ratio_2m >= 0.95
        and snapshot.current_pct >= max(snapshot.open_pct - 0.01, 0.0)
    ):
        return "true_strong"
    if snapshot.open_pct >= 0.095 and snapshot.current_pct < snapshot.open_pct - 0.03:
        return "gap_weak"
    if snapshot.open_pct >= 0.095:
        return "hard_to_chase"
    if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot_amount_2m_top(snapshot, max_rank_pct=0.35, fallback=20_000_000):
        return "low_open_strong"
    if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
        return "pullback_rebound"
    if (
        snapshot.current_pct < 0.0
        or snapshot.current_pct < snapshot.open_pct - 0.04
        or (snapshot.auction_amount > 0 and amount_ratio_2m < 0.75 and snapshot.speed_1m <= 0)
    ):
        return "undertake_weak"
    return "pending"


def entry_window_label(snapshot: StockStateSnapshot, *, phase_label: str) -> str:
    if phase_label == "postmarket":
        if snapshot.open_pct >= 0.095:
            return "一字不追"
        if (
            0.02 <= snapshot.open_pct <= 0.07
            and snapshot.leader_rank_in_theme <= 2
            and snapshot.auction_amount >= 20_000_000
        ):
            if snapshot_amount_2m_top(snapshot, max_rank_pct=0.20, fallback=40_000_000) or snapshot.speed_1m > 0.01:
                return "换手确认"
            return "放量观察"
        if snapshot.open_pct <= 0.01 and snapshot.current_pct > 0.03 and snapshot_amount_2m_top(snapshot, max_rank_pct=0.35, fallback=20_000_000):
            return "低吸回流"
        if snapshot.open_pct < 0.0 and snapshot.current_pct <= 0.0:
            return "承接不足"
        return "等待确认"
    if snapshot.open_pct >= 0.095:
        return "一字不追"
    if (
        0.02 <= snapshot.open_pct <= 0.07
        and snapshot.leader_rank_in_theme <= 2
        and snapshot.auction_amount >= 20_000_000
    ):
        if snapshot_amount_2m_top(snapshot, max_rank_pct=0.20, fallback=40_000_000) or snapshot.speed_1m > 0.01:
            return "换手确认"
        return "确认后看"
    if snapshot.open_pct <= 0.01 and snapshot.auction_amount > 0 and snapshot_amount_2m_top(snapshot, max_rank_pct=0.35, fallback=20_000_000):
        return "低位试错"
    if snapshot.open_pct < 0.0 and snapshot.current_pct <= 0.0:
        return "先等承接"
    return "等待确认"


def extreme_type_label(snapshot: StockStateSnapshot) -> str:
    if snapshot.auction_amount >= 100_000_000 and 0.02 <= snapshot.open_pct <= 0.07:
        return "竞价强承接"
    if snapshot_amount_2m_top(snapshot, max_rank_pct=0.10, fallback=80_000_000) and snapshot.speed_1m > 0.01:
        return "开盘放量"
    if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot_amount_2m_top(snapshot, max_rank_pct=0.35, fallback=20_000_000):
        return "低开转强"
    if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
        return "回落修复"
    if snapshot.open_pct >= 0.095:
        return "一字顶强"
    return "普通承接"


def extreme_behavior_score(snapshot: StockStateSnapshot) -> float:
    score = 0.0
    score += max(snapshot.auction_amount / 100_000_000, 0.0) * 20
    score += max(snapshot.amount_2m / 100_000_000, 0.0) * 16
    score += max(snapshot.speed_1m, 0.0) * 500
    score += max(snapshot.current_pct, 0.0) * 120
    if snapshot.leader_rank_in_theme <= 2:
        score += 12
    if 0.02 <= snapshot.open_pct <= 0.07:
        score += 10
    elif snapshot.open_pct >= 0.095:
        score -= 8
    if snapshot.volume_intensity >= 2.5:
        score += 8
    return score


def rebound_type_label(snapshot: StockStateSnapshot) -> str:
    if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot_amount_2m_top(snapshot, max_rank_pct=0.35, fallback=20_000_000):
        return "低开走强"
    if snapshot.open_pct < 0.0 and snapshot.current_pct > 0.0:
        return "回落翻红"
    if snapshot_amount_2m_top(snapshot, max_rank_pct=0.15, fallback=50_000_000) and snapshot.speed_1m > 0.01:
        return "放量拉升"
    if snapshot.amount_2m >= snapshot.auction_amount > 0:
        return "量能接力"
    return "普通修复"


def rebound_behavior_score(snapshot: StockStateSnapshot) -> float:
    score = 0.0
    if snapshot.open_pct <= 0.01:
        score += 25
    if snapshot.open_pct < 0.0:
        score += 18
    score += max(snapshot.current_pct, 0.0) * 160
    score += max(snapshot.amount_2m / 100_000_000, 0.0) * 18
    score += max(snapshot.speed_1m, 0.0) * 600
    if snapshot.amount_2m >= snapshot.auction_amount > 0:
        score += 10
    if snapshot.leader_rank_in_theme <= 2:
        score += 8
    return score


def is_low_open_rebound_snapshot(snapshot: StockStateSnapshot | None) -> bool:
    if snapshot is None:
        return False
    if snapshot.open_pct < 0.0 and snapshot.current_pct >= 0.05:
        return True
    if snapshot.open_pct > 0.01:
        return False
    if snapshot.current_pct < 0.03:
        return False
    if not snapshot_amount_2m_top(snapshot, max_rank_pct=0.35, fallback=20_000_000):
        return False
    if snapshot.amount_2m < snapshot.auction_amount and snapshot.speed_1m <= 0.008:
        return False
    return True


def classify_opening_entry_behavior(
    snapshot: StockStateSnapshot,
    *,
    amount_2m_floor: float = 0.0,
) -> str:
    if snapshot.open_pct >= 0.05 and snapshot.current_pct <= snapshot.open_pct - 0.03:
        return "high_open_distribution"
    if snapshot.open_pct <= 0.01 and snapshot.current_pct >= 0.03 and snapshot.amount_2m >= amount_2m_floor > 0.0:
        return "low_open_repair"
    if snapshot.amount_2m >= max(snapshot.auction_amount, amount_2m_floor) and snapshot.speed_1m > 0.0:
        return "volume_confirm"
    if snapshot.current_pct >= 0.095 or snapshot.is_locked or snapshot.touched_limit_today:
        return "limit_attack"
    if snapshot.amount_2m > 0 and snapshot.auction_amount > 0 and snapshot.amount_2m < snapshot.auction_amount * 0.75:
        return "weak_follow"
    return "mixed"


def opening_entry_behavior_label(behavior: str) -> str:
    labels = {
        "high_open_distribution": "高开兑现",
        "low_open_repair": "低开转强",
        "volume_confirm": "放量确认",
        "limit_attack": "涨停攻击",
        "weak_follow": "承接弱",
        "mixed": "混合",
    }
    return labels.get(str(behavior or ""), str(behavior or "unknown"))


def stock_focus_evidence_labels(
    snapshot: StockStateSnapshot | None,
    *,
    phase_label: str,
    selection: StockSelectionContext | None = None,
    all_snapshots: Iterable[StockStateSnapshot] | None = None,
    is_fakeout: bool = False,
) -> tuple[str, ...]:
    if snapshot is None:
        return ("证据不足",)
    sample_tuple = _snapshot_samples_or_empty(all_snapshots)
    evidence: list[str] = []
    if is_fakeout:
        evidence.append("竞价骗炮风险")
    follow_text = snapshot_2m_follow_tag(snapshot, concise=False)
    if follow_text:
        evidence.append(follow_text)
    if snapshot.leader_rank_in_theme <= 2:
        evidence.append("前排辨识度")
    if snapshot.auction_amount >= 50_000_000:
        evidence.append("竞价额达标")
    if snapshot.volume_intensity >= 2.5:
        evidence.append("买一承接偏强")
    if snapshot.speed_1m > 0:
        evidence.append("开盘有加速")
    if 0.02 <= snapshot.open_pct <= 0.07:
        evidence.append("开幅不算高")
    elif snapshot.open_pct >= 0.095:
        evidence.append("高开偏热")

    amount_2m_floor = (
        relative_amount_floor(sample_tuple, "amount_2m", top_n=160, fallback=20_000_000)
        if sample_tuple is not None
        else 20_000_000
    )
    opening_behavior = classify_opening_entry_behavior(snapshot, amount_2m_floor=amount_2m_floor)
    if opening_behavior == "high_open_distribution":
        evidence.append("高开兑现风险")
    elif opening_behavior == "low_open_repair":
        evidence.append("低开转强确认")
    elif opening_behavior == "volume_confirm":
        evidence.append("前2分钟放量确认")
    elif opening_behavior == "weak_follow":
        evidence.append("前2分钟承接弱")
    if is_relative_amount_top_or_fallback(
        sample_tuple,
        snapshot,
        "amount_2m",
        top_n=120,
        fallback=30_000_000,
    ):
        evidence.append("前2分钟放量")
    if is_low_open_rebound_snapshot(snapshot):
        evidence.append("低开转强确认")
    if snapshot.market_cap_yi >= 80:
        evidence.append("容量票特征")
    if snapshot.resistance_gap > 0.08:
        evidence.append("上方压力大")
    if snapshot.ths_hot_rank is not None and snapshot.ths_hot_rank <= 20:
        evidence.append("热榜位次靠前")
    if selection is not None:
        if selection.auction_open_bucket == "flat_open":
            evidence.append("平开结构更健康")
        elif selection.auction_open_bucket == "healthy_high_open":
            evidence.append("高开不算热")
        elif selection.auction_open_bucket in {"overheat_high_open", "near_limit_open"}:
            evidence.append("高开偏热")
        if selection.open_follow_state == "confirmed":
            evidence.append("开盘跟随确认")
        elif selection.open_follow_state == "repair_strength":
            evidence.append("低开转强")
        elif selection.open_follow_state == "weak_follow":
            evidence.append("开盘跟随一般")
        elif selection.open_follow_state == "faded":
            evidence.append("开盘掉队")
    if phase_label == "postmarket" and snapshot.current_pct != 0:
        evidence.append("收盘强弱已定型")
    return tuple(evidence)


def stock_focus_evidence_text(
    snapshot: StockStateSnapshot | None,
    *,
    phase_label: str,
    selection: StockSelectionContext | None = None,
    all_snapshots: Iterable[StockStateSnapshot] | None = None,
    is_fakeout: bool = False,
    limit: int = 4,
) -> str:
    labels = stock_focus_evidence_labels(
        snapshot,
        phase_label=phase_label,
        selection=selection,
        all_snapshots=all_snapshots,
        is_fakeout=is_fakeout,
    )
    merged = dedupe_text_items(labels, limit=limit)
    return merged if merged else "仅有基础观察信号"


def stock_decision_meta_tags(
    snapshot: StockStateSnapshot | None,
    *,
    selection: StockSelectionContext | None = None,
    theme_selection: ThemeSelectionContext | None = None,
    all_snapshots: Iterable[StockStateSnapshot] | None = None,
    setup_text: str = "",
    limit: int = 5,
) -> str:
    tags: list[str] = []
    sample_tuple = _snapshot_samples_or_empty(all_snapshots)
    follow_tag = snapshot_2m_follow_tag(snapshot, concise=True)
    if follow_tag:
        tags.append(follow_tag)
    if snapshot is not None:
        amount_2m_floor = (
            relative_amount_floor(sample_tuple, "amount_2m", top_n=160, fallback=20_000_000)
            if sample_tuple is not None
            else 20_000_000
        )
        behavior = classify_opening_entry_behavior(snapshot, amount_2m_floor=amount_2m_floor)
        if behavior != "mixed":
            tags.append(f"行为={opening_entry_behavior_label(behavior)}")
    if setup_text:
        tags.append(f"setup={setup_text}")
    if selection is not None:
        tags.append(f"dayK={selection.daily_height_bucket}")
        tags.append(f"2m={rank_pct_bucket_text(selection.stock_amount_2m_rank_in_theme_pct)}")
        if theme_selection is not None:
            tags.append(f"role={theme_selection.plate_role}")
    return " / ".join(tags[:limit])
