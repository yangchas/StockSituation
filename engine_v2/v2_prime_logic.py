import asyncio
import json
import logging
from datetime import datetime, timedelta
import redis

# 尝试导入通用服务
try:
    from web.services.kaipan_plate_service import fetch_kaipan_plate_rank
except ImportError:
    fetch_kaipan_plate_rank = None

logger = logging.getLogger("CommanderPrime")

class ResonancePrimeService:
    """
    Commander-Prime: 战略指挥逻辑服务。
    负责将物理数据 (V5) 与博弈基因 (V1/ZT/Kaipan) 进行深度对撞。
    """
    def __init__(self, r_client: redis.Redis):
        self.r = r_client
        self.hot_plates_map = {} # {name: {strength, net_amount, rank}}
        self.last_sync_time = 0
        self.amount_baselines = {} # 存储 2 分钟前的成交额快照

    async def sync_kaipan_hotspots(self):
        """同步开盘啦板块热度榜 (含强度与净额)"""
        if not fetch_kaipan_plate_rank: return
        now = time.time()
        if now - self.last_sync_time < 60: return 
        
        try:
            raw_plates = await fetch_kaipan_plate_rank()
            if raw_plates:
                self.hot_plates_map = {
                    p['name']: {
                        'strength': float(p.get('strength', 0)),
                        'net_amount': float(p.get('main_net_amount', 0)),
                        'rank': i + 1
                    } for i, p in enumerate(raw_plates)
                }
                self.last_sync_time = now
                logger.info(f"✅ [Commander] 已同步 Kaipanla 板块能级 (Count: {len(self.hot_plates_map)})")
        except Exception as e:
            logger.warning(f"⚠️ [Commander] Kaipanla 排行同步失败: {e}")

    def calculate_battle_kpis(self, date_str: str) -> dict:
        """
        计算昨日涨停标兵的晋级率、爆头率、红开率。
        """
        zt_key = f"limit_up_{date_str}"
        zt_raw = self.r.get(zt_key)
        if not zt_raw: return {}

        zt_items = json.loads(zt_raw)
        total = len(zt_items)
        if total == 0: return {}

        promotion_cnt = 0
        headshot_cnt = 0
        red_open_cnt = 0
        total_bid_amt = 0.0

        for it in zt_items:
            code = it.get("code") or it.get("股票代码")
            q = self.r.hgetall(f"stock:quote:{code}")
            if not q: continue

            cp = float(q.get("change_pct", 0))
            op = float(q.get("open_pct", 0)) if q.get("open_pct") else cp
            
            if op > 0: red_open_cnt += 1
            if cp >= 9.8: promotion_cnt += 1
            # 爆头定义：大幅高开 (>5%) 但目前翻绿 (<0%)
            if op > 5.0 and cp < 0: headshot_cnt += 1
            
            # 封单金额检测 (来源于 Rust p0925 快照)
            # 假设存储在 quote 的 bid_amount 字段
            total_bid_amt += float(q.get("bid_amount", 0))

        return {
            "total_count": total,
            "red_open_rate": red_open_cnt / total,
            "promotion_rate": promotion_cnt / total,
            "headshot_rate": headshot_cnt / total,
            "avg_bid_amt": total_bid_amt / total if total > 0 else 0,
            "battle_status": self._judge_status(headshot_cnt / total, promotion_cnt / total)
        }

    def _judge_status(self, headshot_rate: float, promotion_rate: float) -> str:
        if headshot_rate > 0.15: return "⚠️ 爆头临界 (Danger)"
        if promotion_rate > 0.4: return "🚀 强势晋级 (Bullish)"
        if headshot_rate < 0.05 and promotion_rate < 0.15: return "❄️ 情绪冰点 (Frozen)"
        return "🛡️ 震荡博弈 (Neutral)"

    def get_plate_resonance(self, plate_name: str) -> float:
        """
        全量能量对撞因子 (0.0 - 2.0)
        计算逻辑：基础权重 (Rank) + 强度溢价 + 净额系数
        """
        if not self.hot_plates_map or not plate_name: return 1.0
        
        # 匹配板块数据 (支持模糊匹配，防止 V1 与 Kaipan 名称微差)
        p_data = None
        for name, data in self.hot_plates_map.items():
            if name in plate_name or plate_name in name:
                p_data = data
                break
        
        if not p_data: return 1.0
        
        # 1. 基础系数 (基于排名)
        rank_bonus = 1.2 if p_data['rank'] <= 3 else (1.1 if p_data['rank'] <= 10 else 1.0)
        
        # 2. 强度溢价 (参考: 3000 为极强阈值)
        strength_bonus = 0.0
        if p_data['strength'] > 3000: strength_bonus = 0.2
        elif p_data['strength'] > 1500: strength_bonus = 0.1
        
        # 3. 净额印证 (净额 > 5亿 为大单流入)
        net_amount_bonus = 0.0
        if p_data['net_amount'] > 5e8: net_amount_bonus = 0.3
        elif p_data['net_amount'] < 0: net_amount_bonus = -0.2 # 减分项：资金出逃 (警惕陷阱)
        
        return round(rank_bonus + strength_bonus + net_amount_bonus, 2)

    def compensate_amount_delta(self, symbol: str, current_amount: float) -> float:
        """
        2 分钟成交额补偿逻辑。
        """
        now = datetime.now()
        # 这里逻辑可以在 Orchestrator 中驱动，此处仅作为占位
        return current_amount # 暂返回原值，由于 Python 端的队列维护需要更高频的调度
