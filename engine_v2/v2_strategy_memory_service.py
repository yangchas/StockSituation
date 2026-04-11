import json
import os
import logging
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional
from datetime import datetime

logger = logging.getLogger("StrategyMemory")

@dataclass
class EnvironmentDNA:
    sentiment_score: float      # 赚钱效应 (0-10)
    max_lb: int                 # 最高标高度
    leader_feedback: str        # 龙头竞价状态 (Lock/Divergent/Weak)
    top_sector_hotness: float   # 领涨板块平均量比
    is_new_theme_emerging: bool # 是否有新题材低位放量
    total_auction_amt: float = 0.0 # 全场竞价总成交额 (亿元)
    avg_market_pct: float = 0.0    # 全场竞价平均涨幅 (%)
    momentum_slope: float = 1.0    # 板块动能斜率 (3-10分钟增速)
    date_ref: str = ""

class StrategyMemoryService:
    def __init__(self, memory_path: str = r"strategy_memory.json"):
        self.memory_path = memory_path
        self._ensure_path()
        self.memories: List[Dict] = self._load()

    def _ensure_path(self):
        # 物理解决 os.path.dirname('') 导致的 FileNotFoundError
        abs_path = os.path.abspath(self.memory_path)
        dir_name = os.path.dirname(abs_path)
        
        if dir_name and not os.path.exists(dir_name):
            os.makedirs(dir_name, exist_ok=True)
            
        if not os.path.exists(abs_path):
            with open(abs_path, 'w', encoding='utf-8') as f:
                json.dump([], f)

    def _load(self) -> List[Dict]:
        try:
            with open(self.memory_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load memory: {e}")
            return []

    def save_case(self, dna: EnvironmentDNA, strategy_label: str, result_comment: str):
        """保存一个典型的实战案例"""
        case = {
            "dna": asdict(dna),
            "strategy": strategy_label,
            "comment": result_comment,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M")
        }
        self.memories.append(case)
        with open(self.memory_path, 'w', encoding='utf-8') as f:
            json.dump(self.memories, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ [Memory] 已存储环境记忆: {strategy_label} ({dna.date_ref})")

    def find_similar(self, current_dna: EnvironmentDNA, threshold: float = 0.8) -> List[Dict]:
        """寻找相似的历史环境"""
        results = []
        for case in self.memories:
            target = case['dna']
            score = 0.0
            # 1. 情绪分相似度 (权重 0.4)
            score += 0.4 * (1.0 - abs(current_dna.sentiment_score - target['sentiment_score']) / 10.0)
            # 2. 最高标相似度 (权重 0.3)
            score += 0.3 * (1.0 - min(1.0, abs(current_dna.max_lb - target['max_lb']) / 5.0))
            # 3. 龙头反馈对齐 - 模糊分类匹配 (权重 0.2)
            def _feedback_class(fb: str) -> str:
                fb = fb.lower()
                if "lock" in fb or "稳板" in fb: return "LOCKED"
                if "diverge" in fb or "分歧" in fb or "nuclear" in fb: return "DIVERGENT"
                if "weak" in fb or "弱" in fb: return "WEAK"
                return "OTHER"
            if _feedback_class(current_dna.leader_feedback) == _feedback_class(target['leader_feedback']):
                score += 0.2
            else:
                score += 0.05 # 部分相似，不完全归零
            
            # 4. 动能斜率相似度 (权重 0.1)
            score += 0.1 * (1.0 - min(1.0, abs(current_dna.momentum_slope - target.get('momentum_slope', 1.0)) / 5.0))
            if score >= 0.68:  # V9.0: 放宽至 0.68，容纳近似市场环境
                case['similarity'] = round(score * 100, 1)
                results.append(case)
        
        return sorted(results, key=lambda x: x['similarity'], reverse=True)

V2StrategyMemoryService = StrategyMemoryService

# 种子数据：2026-04-07 的化工日
if __name__ == "__main__":
    service = StrategyMemoryService()
    # 注入“冷启动破壳模式” (以 2026-04-07 为物理样准)
    cold_start_pattern = EnvironmentDNA(
        sentiment_score=4.4,
        max_lb=7,
        leader_feedback="WeakProtection",
        top_sector_hotness=0.8,
        is_new_theme_emerging=True,
        momentum_slope=16.0, # 10分钟内热度激增 16 倍 (从 239 到 3882)
        date_ref="2026-04-07"
    )
    service.save_case(
        cold_start_pattern, 
        "PATTERN_COLD_START_BREAKOUT (冷启动破壳)", 
        "当最高标滞涨、中位身位缺失时，发现低开(+0%~2%)但量能脉冲极强的【新标兵】。当其所属板块斜率 > 500% 时，无视开盘弱势，强制切换为主攻方向。"
    )
