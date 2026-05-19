from __future__ import annotations

from engine_next.domain.models import StockSelectionContext, StockStateSnapshot, ThemeSelectionContext


def is_high_dayk_weak_trap(
    snapshot: StockStateSnapshot | None,
    stock_selection: StockSelectionContext | None,
    theme_selection: ThemeSelectionContext | None = None,
    *,
    phase_label: str | None = None,
) -> bool:
    if snapshot is None or stock_selection is None:
        return False
    if phase_label is not None and phase_label not in {"auction", "opening", "open_confirm", "intraday"}:
        return False
    if stock_selection.daily_height_bucket != "high":
        return False
    if stock_selection.open_follow_state in {"confirmed", "repair_strength"}:
        return False
    if snapshot.amount_2m >= max(snapshot.auction_amount * 1.1, 30_000_000) and snapshot.current_pct >= snapshot.open_pct:
        return False

    risk_score = 0
    if snapshot.lb_days >= 2 or stock_selection.is_true_leader:
        risk_score += 1
    if snapshot.open_pct <= -0.05:
        risk_score += 1
    if snapshot.current_pct <= min(-0.04, snapshot.open_pct - 0.02):
        risk_score += 1
    if stock_selection.open_follow_state in {"weak_follow", "faded"}:
        risk_score += 1
    if snapshot.auction_amount > 0 and snapshot.amount_2m > 0 and snapshot.amount_2m < snapshot.auction_amount * 0.8:
        risk_score += 1
    if (
        stock_selection.theme_fakeout_level in {"high", "extreme"}
        or stock_selection.theme_x_score >= 5.6
        or (
            theme_selection is not None
            and (
                theme_selection.fakeout_level in {"high", "extreme"}
                or theme_selection.x_score >= 5.6
            )
        )
    ):
        risk_score += 1
    if stock_selection.kline_pattern in {"high_open_then_weak", "high_divergence", "explosive_failed_board"}:
        risk_score += 1
    if snapshot.market_cap_yi >= 200:
        risk_score += 1

    if phase_label == "auction":
        return risk_score >= 3 and snapshot.open_pct <= -0.06
    return risk_score >= 3
