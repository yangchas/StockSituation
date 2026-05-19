from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OfflineStageSpec:
    name: str
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    rules: tuple[str, ...]


KLINE_FACTOR_PIPELINE: tuple[OfflineStageSpec, ...] = (
    OfflineStageSpec(
        name="target_date_resolution",
        inputs=("now", "override_date", "trading_calendar"),
        outputs=("target_trade_date",),
        rules=(
            "override_date 优先级最高",
            "非交易日或 15:30 前默认回退到上一交易日",
            "17:30 前不把当日 Baostock 日线当作正式值",
        ),
    ),
    OfflineStageSpec(
        name="watermark_preload",
        inputs=("TDengine latest daily_kline", "TDengine latest daily_factors", "Redis factor cache"),
        outputs=("kline watermark map", "factor watermark map"),
        rules=(
            "启动前批量预加载，不在盘中频繁查 TDengine",
            "watermark 用于增量更新与缺口检测",
        ),
    ),
    OfflineStageSpec(
        name="integrated_kline_sync",
        inputs=("Baostock", "symbol universe", "checkpoint", "physical validator"),
        outputs=("fresh daily_kline", "checkpoint updates"),
        rules=(
            "按 symbol 增量更新，不全量重复拉取",
            "checkpoint 标记完成前必须通过物理存在校验",
            "checkpoint 已完成但物理缺失时，移除 checkpoint 并重跑",
        ),
    ),
    OfflineStageSpec(
        name="gap_fill_and_resume",
        inputs=("watermarks", "checkpoint", "missing symbols", "history requirement"),
        outputs=("gap fill plan",),
        rules=(
            "缺啥补啥",
            "支持断点续传",
            "当因子计算要求更长窗口时，可回溯补足历史长度",
        ),
    ),
    OfflineStageSpec(
        name="factor_calculation",
        inputs=("daily_kline history", "daily_factors history", "chip/factor dependencies"),
        outputs=("daily_factors", "stock_extra cache payload"),
        rules=(
            "日线更新后再触发因子计算",
            "因子结果同时写 TDengine 与 Redis cache",
            "盘中直接读取 cache:stock_extra:{date}，不临时重算",
        ),
    ),
    OfflineStageSpec(
        name="post_write_validation",
        inputs=("TDengine save result", "Redis cache result", "physical validator"),
        outputs=("final readiness state",),
        rules=(
            "daily_kline、daily_factors、Redis cache 三者都要校验",
            "任一维缺失则保留待补状态，不伪装成功",
        ),
    ),
)


FACTOR_FIELDS = (
    "bias_20",
    "profit_ratio",
    "vol_ratio",
    "rsi_6",
    "concentration",
    "structure_score_base",
    "shape_platform_ready",
    "shape_breakout_ready",
    "shape_repair_ready",
    "shape_overheat_risk",
    "shape_chip_cleanliness",
    "shape_trend_health",
    "shape_t2_repair_bias",
    "theme_core_base",
)


FACTOR_PIPELINE_NOTES = (
    "calc_daily_score 使用 daily_factors/daily_kline 作为日级底分输入。",
    "PatternFactory 会消费因子、板块共振、历史记忆与盘中强弱做 setup 匹配。",
    "cache:stock_extra:{date} 是盘中主链读取的轻量因子视图。",
)
