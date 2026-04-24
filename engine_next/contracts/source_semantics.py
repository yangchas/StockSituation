from __future__ import annotations

from dataclasses import dataclass

from engine_next.domain.enums import RunPhase, SourceName


@dataclass(frozen=True)
class SourceSemanticsSpec:
    dataset: str
    source: SourceName
    source_quality: str
    is_formal: bool
    is_temporary: bool
    fetch_cost: str
    allowed_phases: tuple[RunPhase, ...]
    intended_use: tuple[str, ...]
    fresh_after: str = ""
    runtime_writeback_keys: tuple[str, ...] = ()
    notes: tuple[str, ...] = ()


SOURCE_SEMANTICS_SPECS: dict[tuple[str, SourceName], SourceSemanticsSpec] = {
    ("daily_kline", SourceName.BARS): SourceSemanticsSpec(
        dataset="daily_kline",
        source=SourceName.BARS,
        source_quality="formal_authoritative",
        is_formal=True,
        is_temporary=False,
        fetch_cost="medium",
        allowed_phases=(RunPhase.POSTMARKET, RunPhase.NIGHT),
        intended_use=("offline_sync", "factor_dependency", "startup_preload", "recap_truth"),
        fresh_after="17:30",
        notes=(
            "Baostock daily kline is the formal daily truth after the post-close update window.",
            "Before readiness, the caller must fall back to the previous trade date.",
        ),
    ),
    ("hot_plates", SourceName.KAIPAN): SourceSemanticsSpec(
        dataset="hot_plates",
        source=SourceName.KAIPAN,
        source_quality="formal_context",
        is_formal=True,
        is_temporary=False,
        fetch_cost="medium",
        allowed_phases=(RunPhase.POSTMARKET, RunPhase.NIGHT),
        intended_use=("startup_preload", "plate_persistence", "recap_truth"),
        fresh_after="postmarket",
        notes=("Kaipan hot plates are suitable for recap and multi-day persistence analysis.",),
    ),
    ("yest_limit_pool", SourceName.KAIPAN): SourceSemanticsSpec(
        dataset="yest_limit_pool",
        source=SourceName.KAIPAN,
        source_quality="formal_delayed",
        is_formal=True,
        is_temporary=False,
        fetch_cost="medium",
        allowed_phases=(RunPhase.NIGHT,),
        intended_use=("startup_preload", "ladder_context", "recap_truth"),
        fresh_after="after_midnight",
        notes=("Yesterday limit pool is a delayed formal source and should not be treated as intraday truth.",),
    ),
    ("ban_reasons", SourceName.KAIPAN): SourceSemanticsSpec(
        dataset="ban_reasons",
        source=SourceName.KAIPAN,
        source_quality="formal_enrichment",
        is_formal=True,
        is_temporary=False,
        fetch_cost="medium",
        allowed_phases=(RunPhase.AUCTION, RunPhase.INTRADAY, RunPhase.POSTMARKET, RunPhase.NIGHT),
        intended_use=("plate_refinement", "reason_writeback", "recap_truth"),
        fresh_after="intraday_on_demand",
        runtime_writeback_keys=("market:stock_plate", "market:stock_reason"),
        notes=("Ban reasons can be queried intraday in small batches for plate refinement and writeback.",),
    ),
    ("limit_truth", SourceName.WENCAI): SourceSemanticsSpec(
        dataset="limit_truth",
        source=SourceName.WENCAI,
        source_quality="temporary_substitute",
        is_formal=False,
        is_temporary=True,
        fetch_cost="high",
        allowed_phases=(RunPhase.POSTMARKET, RunPhase.NIGHT),
        intended_use=("recap_truth", "temporary_substitute"),
        fresh_after="postmarket",
        notes=(
            "Wencai limit truth is usable after the close, but it remains a substitute truth layer.",
            "It should not silently overwrite a later formal Kaipan-style delayed truth source.",
        ),
    ),
    ("broken_boards", SourceName.WENCAI): SourceSemanticsSpec(
        dataset="broken_boards",
        source=SourceName.WENCAI,
        source_quality="temporary_auxiliary",
        is_formal=False,
        is_temporary=True,
        fetch_cost="high",
        allowed_phases=(RunPhase.POSTMARKET, RunPhase.NIGHT),
        intended_use=("failed_promotion_typing", "negative_feedback"),
        fresh_after="postmarket",
        notes=("Broken board list is a high-cost auxiliary set and should stay low-frequency.",),
    ),
    ("first_failed", SourceName.WENCAI): SourceSemanticsSpec(
        dataset="first_failed",
        source=SourceName.WENCAI,
        source_quality="temporary_auxiliary",
        is_formal=False,
        is_temporary=True,
        fetch_cost="high",
        allowed_phases=(RunPhase.POSTMARKET, RunPhase.NIGHT),
        intended_use=("failed_promotion_typing", "negative_feedback"),
        fresh_after="postmarket",
        notes=("First-failed set is a high-cost auxiliary negative-feedback input.",),
    ),
    ("hot_rank", SourceName.THS): SourceSemanticsSpec(
        dataset="hot_rank",
        source=SourceName.THS,
        source_quality="auxiliary_attention_proxy",
        is_formal=False,
        is_temporary=True,
        fetch_cost="high",
        allowed_phases=(RunPhase.POSTMARKET, RunPhase.NIGHT),
        intended_use=("auxiliary_attention_proxy",),
        fresh_after="postmarket",
        notes=("THS hot rank is a low-frequency auxiliary attention proxy.",),
    ),
    ("hot_rank", SourceName.TDENGINE): SourceSemanticsSpec(
        dataset="hot_rank",
        source=SourceName.TDENGINE,
        source_quality="historical_archive",
        is_formal=False,
        is_temporary=True,
        fetch_cost="medium",
        allowed_phases=(RunPhase.POSTMARKET, RunPhase.NIGHT),
        intended_use=("historical_archive",),
        notes=("TDengine hot-rank rows are historical archive, not intraday polling targets.",),
    ),
    ("hot_rank", SourceName.REDIS): SourceSemanticsSpec(
        dataset="hot_rank",
        source=SourceName.REDIS,
        source_quality="runtime_cache",
        is_formal=False,
        is_temporary=True,
        fetch_cost="low",
        allowed_phases=(RunPhase.PREMARKET, RunPhase.AUCTION, RunPhase.INTRADAY, RunPhase.POSTMARKET, RunPhase.NIGHT),
        intended_use=("runtime_cache",),
        notes=("Redis hot-rank view is only a cache projection, not a truth source.",),
    ),
}


def get_source_semantics_spec(dataset: str, source: SourceName) -> SourceSemanticsSpec:
    key = (dataset, source)
    if key not in SOURCE_SEMANTICS_SPECS:
        raise KeyError(f"No source semantics spec registered for dataset={dataset}, source={source.value}")
    return SOURCE_SEMANTICS_SPECS[key]
