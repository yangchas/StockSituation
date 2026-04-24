from __future__ import annotations

from enum import Enum


class FetchIntent(str, Enum):
    AUCTION_ANCHOR_RECOVERY = "auction_anchor_recovery"
    AUCTION_COVERAGE_RECOVERY = "auction_coverage_recovery"
    STOCK_PLATE_ENRICHMENT = "stock_plate_enrichment"
    HOT_PLATE_DISCOVERY = "hot_plate_discovery"
    YEST_LIMIT_POOL_BUILD = "yest_limit_pool_build"


class SourceName(str, Enum):
    REDIS = "redis"
    TDENGINE = "tdengine"
    KAIPAN = "kaipan"
    WENCAI = "wencai"
    BARS = "bars"
    THS = "ths"


class RunPhase(str, Enum):
    PREMARKET = "premarket"
    AUCTION = "auction"
    INTRADAY = "intraday"
    POSTMARKET = "postmarket"
    NIGHT = "night"


class StorageTier(str, Enum):
    REDIS = "redis"
    TDENGINE = "tdengine"
    MEMORY = "memory"


class ExecutionEnvironment(str, Enum):
    LOCAL_WINDOWS = "local_windows"
    SERVER = "server"


class StartupReadinessLevel(str, Enum):
    FULL_READY = "full_ready"
    TRADE_READY_DEGRADED = "trade_ready_degraded"
    OBSERVE_ONLY = "observe_only"
    POSTMARKET_ONLY = "postmarket_only"


class StartupAction(str, Enum):
    NOOP = "noop"
    PRELOAD_ONLY = "preload_only"
    FAST_REPAIR_ALLOWED = "fast_repair_allowed"
    HEAVY_SYNC_ALLOWED = "heavy_sync_allowed"
    HEAVY_SYNC_LIMITED = "heavy_sync_limited"
    HEAVY_SYNC_BLOCKED = "heavy_sync_blocked"
    AUCTION_FALLBACK_RECOVERY = "auction_fallback_recovery"
    POSTMARKET_RECAP_READY = "postmarket_recap_ready"
    DEFER_TO_POSTMARKET = "defer_to_postmarket"


class StockArchetype(str, Enum):
    DRAGON_LEADER = "dragon_leader"
    CORE_TREND = "core_trend"
    INSTITUTIONAL_TREND = "institutional_trend"
    STRONG_OPERATOR = "strong_operator"
    CONTRARIAN_STRENGTH = "contrarian_strength"
    FOLLOWER = "follower"
    UNKNOWN = "unknown"


class StockStage(str, Enum):
    SEED = "seed"
    CONFIRMATION = "confirmation"
    MAIN_RISE = "main_rise"
    HIGH_ACCELERATION = "high_acceleration"
    HIGH_DIVERGENCE = "high_divergence"
    FAILED_PROMOTION = "failed_promotion"
    ICE_POINT_REBOUND = "ice_point_rebound"
    TREND_REPAIR = "trend_repair"
    UNKNOWN = "unknown"


class FeedbackState(str, Enum):
    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"


class ExposureState(str, Enum):
    UNDEREXPOSED = "underexposed"
    BALANCED = "balanced"
    OVEREXPOSED = "overexposed"


class TradeWindowState(str, Enum):
    EARLY_BOARDING = "early_boarding"
    CHASE_RISK = "chase_risk"
    HOLD_ONLY = "hold_only"
    AVOID = "avoid"


class LeaderTier(str, Enum):
    ABSOLUTE = "absolute"
    CORE = "core"
    SECONDARY = "secondary"
    FOLLOWER = "follower"
    UNKNOWN = "unknown"


class FailedPromotionType(str, Enum):
    NONE = "none"
    YDAY_BREAK_AFTER_LIMIT = "yday_break_after_limit"
    INTRADAY_RELOCK_FAIL = "intraday_relock_fail"
    WEAK_CONTINUATION = "weak_continuation"
    UNKNOWN = "unknown"


class OperatorStyleHint(str, Enum):
    HOT_MONEY = "hot_money"
    INSTITUTION = "institution"
    MIXED = "mixed"
    UNKNOWN = "unknown"
