from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketTopNSliceComparison:
    top10_amount: float
    top20_amount: float
    top10_vs_prev_ratio: float
    top20_vs_prev_ratio: float
    overall_vs_prev_ratio: float
    strength_state: str

    @property
    def is_weak(self) -> bool:
        return self.strength_state in {"very_weak", "weak"}

    @property
    def is_strong(self) -> bool:
        return self.strength_state == "strong"


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except (TypeError, ValueError):
        return default


def classify_strength_state(ratio: float) -> str:
    if ratio <= 0.7:
        return "very_weak"
    if ratio <= 0.85:
        return "weak"
    if ratio >= 1.25:
        return "strong"
    return "neutral"


def build_market_topn_slice_comparison(
    summary: object | None,
    *,
    prefix: str = "auction",
) -> MarketTopNSliceComparison:
    if summary is None:
        return MarketTopNSliceComparison(
            top10_amount=0.0,
            top20_amount=0.0,
            top10_vs_prev_ratio=1.0,
            top20_vs_prev_ratio=1.0,
            overall_vs_prev_ratio=1.0,
            strength_state="neutral",
        )
    prefix = str(prefix or "auction").strip() or "auction"
    top10_amount = _safe_float(getattr(summary, f"{prefix}_top10_amount", 0.0))
    top20_amount = _safe_float(getattr(summary, f"{prefix}_top20_amount", 0.0))
    top10_vs_prev_ratio = _safe_float(getattr(summary, f"{prefix}_top10_vs_prev_ratio", 1.0), 1.0)
    top20_vs_prev_ratio = _safe_float(getattr(summary, f"{prefix}_top20_vs_prev_ratio", 1.0), 1.0)
    overall_vs_prev_ratio = min(top10_vs_prev_ratio, top20_vs_prev_ratio)
    return MarketTopNSliceComparison(
        top10_amount=top10_amount,
        top20_amount=top20_amount,
        top10_vs_prev_ratio=top10_vs_prev_ratio,
        top20_vs_prev_ratio=top20_vs_prev_ratio,
        overall_vs_prev_ratio=overall_vs_prev_ratio,
        strength_state=classify_strength_state(overall_vs_prev_ratio),
    )


def build_opening_2m_slice_comparison(summary: object | None) -> MarketTopNSliceComparison:
    return build_market_topn_slice_comparison(summary, prefix="open_2m")


def topn_expansion_factor(comparison: MarketTopNSliceComparison) -> float:
    if comparison.strength_state == "very_weak":
        return 1.35
    if comparison.strength_state == "weak":
        return 1.18
    if comparison.strength_state == "strong":
        return 0.9
    return 1.0
