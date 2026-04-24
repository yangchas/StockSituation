from __future__ import annotations

from engine_next.domain.enums import FetchIntent, RunPhase, SourceName, StorageTier
from engine_next.domain.models import (
    IntradayNetworkRule,
    IntradayRequestDecision,
    RedisWritebackPlan,
    StorageRoutingRule,
)


INTRADAY_NETWORK_RULES: tuple[IntradayNetworkRule, ...] = (
    IntradayNetworkRule(
        name="auction_fallback_chain",
        intent=FetchIntent.AUCTION_ANCHOR_RECOVERY,
        allowed_phases=(RunPhase.AUCTION, RunPhase.INTRADAY),
        preferred_sources=(SourceName.REDIS, SourceName.TDENGINE, SourceName.WENCAI, SourceName.BARS),
        max_symbols_per_request=1000,
        max_requests_per_minute=2,
        redis_write_key="market:auction:{date}:latest",
        reason="竞价锚点缺失时允许少量兜底请求，但必须优先本地源，且结果要回写 Redis 供后续复用。",
    ),
    IntradayNetworkRule(
        name="auction_coverage_recovery",
        intent=FetchIntent.AUCTION_COVERAGE_RECOVERY,
        allowed_phases=(RunPhase.AUCTION, RunPhase.INTRADAY),
        preferred_sources=(SourceName.TDENGINE, SourceName.WENCAI, SourceName.BARS),
        max_symbols_per_request=300,
        max_requests_per_minute=1,
        redis_write_key="market:auction:coverage:{date}",
        reason="主锚点拿到后，允许小样本补齐核心池，不允许盘中全市场高成本轮询。",
    ),
    IntradayNetworkRule(
        name="stock_plate_enrichment",
        intent=FetchIntent.STOCK_PLATE_ENRICHMENT,
        allowed_phases=(RunPhase.AUCTION, RunPhase.INTRADAY),
        preferred_sources=(SourceName.KAIPAN,),
        max_symbols_per_request=30,
        max_requests_per_minute=1,
        redis_write_key="market:stock_plate",
        reason="当个股板块过于泛化或缺失时，允许调用 Kaipan 涨停原因中的所属板块做精修，并回写 Redis。",
    ),
    IntradayNetworkRule(
        name="hot_plate_discovery",
        intent=FetchIntent.HOT_PLATE_DISCOVERY,
        allowed_phases=(RunPhase.PREMARKET, RunPhase.AUCTION, RunPhase.INTRADAY, RunPhase.POSTMARKET),
        preferred_sources=(SourceName.KAIPAN,),
        max_symbols_per_request=20,
        max_requests_per_minute=1,
        redis_write_key="cache:hot_plates:{date}",
        reason="Kaipan hot plates support small-batch intraday refresh and should be cached by trade_date in Redis.",
    ),
    IntradayNetworkRule(
        name="yest_limit_pool_build",
        intent=FetchIntent.YEST_LIMIT_POOL_BUILD,
        allowed_phases=(RunPhase.PREMARKET, RunPhase.AUCTION, RunPhase.INTRADAY, RunPhase.POSTMARKET),
        preferred_sources=(SourceName.KAIPAN,),
        max_symbols_per_request=200,
        max_requests_per_minute=1,
        redis_write_key="cache:yest_limit_pool:{date}",
        reason="Yesterday limit pool is a startup-critical lightweight context dataset and may be repaired on demand.",
    ),
)


REDIS_WRITEBACK_PLANS: tuple[RedisWritebackPlan, ...] = (
    RedisWritebackPlan(
        key="market:stock_plate",
        fields=("symbol", "plate"),
        ttl_seconds=None,
        notes="个股所属板块长期缓存。Kaipan 补齐后直接覆盖泛化板块。",
    ),
    RedisWritebackPlan(
        key="market:stock_reason",
        fields=("symbol", "ban_reason_text"),
        ttl_seconds=None,
        notes="记录 Kaipan 涨停原因原文，便于复盘和二次解析。",
    ),
    RedisWritebackPlan(
        key="market:auction:{date}:latest",
        fields=("symbol", "change_pct", "amount", "bid_amount"),
        ttl_seconds=172800,
        notes="竞价阶段兜底恢复后的归一化快照。",
    ),
)


STORAGE_ROUTING_RULES: tuple[StorageRoutingRule, ...] = (
    StorageRoutingRule(
        dataset="daily_kline",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="离线日线 K 的正式获取源固定为 Baostock；获取后正式入 TDengine，盘中仅读 Redis 清洗视图。",
    ),
    StorageRoutingRule(
        dataset="daily_dde",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="DDE 为离线正式数据，清洗后灌 Redis 以供盘中低成本使用。",
    ),
    StorageRoutingRule(
        dataset="daily_factors",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="Formal factor rows stay in TDengine and trimmed cache:stock_extra:{date} views go to Redis.",
    ),
    StorageRoutingRule(
        dataset="chip_peaks",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="Formal chip rows stay in TDengine and trimmed cache:chip_peaks:{date} views go to Redis.",
    ),
    StorageRoutingRule(
        dataset="hot_plates",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="Kaipan hot plates are formal recap truth and also cached for startup preload.",
    ),
    StorageRoutingRule(
        dataset="yest_limit_pool",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="Yesterday limit pool feeds recap, ladder stats, and startup preload context.",
    ),
    StorageRoutingRule(
        dataset="ban_reasons",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="Ban reasons are archived formally and also support market:stock_reason style Redis enrichment.",
    ),
    StorageRoutingRule(
        dataset="limit_truth",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="Wencai post-close truth is formalized with a date bucket and can seed recap cache views.",
    ),
    StorageRoutingRule(
        dataset="broken_boards",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="Broken board list is lightweight but still date-sensitive negative-feedback input.",
    ),
    StorageRoutingRule(
        dataset="first_failed",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="First-failed list is a date-bucketed failed promotion signal.",
    ),
    StorageRoutingRule(
        dataset="hot_rank",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.TDENGINE,
        secondary_storage=StorageTier.REDIS,
        bucket_by="trade_date",
        notes="THS hot rank is low-frequency auxiliary attention data.",
    ),
    StorageRoutingRule(
        dataset="auction_snapshot",
        has_trade_date_dimension=True,
        primary_storage=StorageTier.REDIS,
        secondary_storage=StorageTier.TDENGINE,
        bucket_by="trade_date+tag",
        notes="实时竞价优先 Redis；标准化历史快照可异步落 TDengine。",
    ),
    StorageRoutingRule(
        dataset="stock_plate_mapping",
        has_trade_date_dimension=False,
        primary_storage=StorageTier.REDIS,
        secondary_storage=StorageTier.TDENGINE,
        bucket_by="symbol",
        notes="盘中实时修正优先 Redis；如需审计可异步沉淀历史版本到 TDengine。",
    ),
)


def allow_intraday_request(intent: FetchIntent, phase: RunPhase) -> IntradayRequestDecision:
    for rule in INTRADAY_NETWORK_RULES:
        if rule.intent == intent:
            if phase in rule.allowed_phases:
                return IntradayRequestDecision(
                    allowed=True,
                    intent=intent,
                    phase=phase,
                    chosen_source=rule.preferred_sources[0],
                    redis_write_key=rule.redis_write_key,
                    notes=rule.reason,
                )
            return IntradayRequestDecision(
                allowed=False,
                intent=intent,
                phase=phase,
                chosen_source=None,
                redis_write_key=None,
                notes=f"{intent.value} is not allowed during {phase.value}.",
            )
    return IntradayRequestDecision(
        allowed=False,
        intent=intent,
        phase=phase,
        chosen_source=None,
        redis_write_key=None,
        notes="No registered intraday rule.",
    )


def find_storage_rule(dataset: str) -> StorageRoutingRule | None:
    for rule in STORAGE_ROUTING_RULES:
        if rule.dataset == dataset:
            return rule
    return None
