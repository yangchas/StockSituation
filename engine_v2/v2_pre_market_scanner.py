import os
import sys
import json
import asyncio
import logging
import pandas as pd
from datetime import datetime, timedelta

# 路径对齐
BASE_DIR = "d:/work/Go"
sys.path.append(BASE_DIR)

from web.services.tdengine_service import TDengineService
from engine_v2.v2_metadata_provider import MetadataProvider
import redis.asyncio as redis

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PreScanner")

class PreMarketScanner:
    """MarketEdge 空间换时间选股器 - 盘后/盘前执行"""
    
    def __init__(self, redis_url="redis://localhost:6379/0"):
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.td = TDengineService()
        self.meta = MetadataProvider(data_dir=os.path.join(BASE_DIR, "web", "data"))
        self.candidate_key = "market:alpha:candidates"
        self.s2p_key = "config:plate_mapping:s2p"

    async def scan_technical_candidates(self, target_date: str = None):
        """
        深度扫描技术面低位且具有爆发特征的个股
        """
        if not target_date:
            target_date = datetime.now().strftime('%Y-%m-%d')
            
        logger.info(f"🚀 开始全量技术面扫描: {target_date} (上帝视角预判模式)...")
        
        symbols = list(self.meta.stock_info.keys())
        candidates = []
        
        # 批量获取全场水位因素 (模拟环境暂用 SQL, 实战建议走超级表)
        # 获取 20 日均线乖离率 (bias_20) 和 获利盘比例 (profit_ratio)
        sql = f"SELECT symbol, LAST(bias_20), LAST(profit_ratio), LAST(vol_ratio) FROM daily_factors WHERE ts <= '{target_date} 23:59:59' GROUP BY symbol"
        
        try:
            loop = asyncio.get_event_loop()
            cursor = await loop.run_in_executor(None, self.td.execute_query, sql)
            if not cursor:
                logger.error("TDengine 因子库请求失败")
                return
            
            rows = cursor.fetchall()
            factors_map = {row[0]: (row[1], row[2], row[3]) for row in rows}
            
            for code in symbols:
                f = factors_map.get(code)
                if not f: continue
                
                bias_20, profit, vol_ratio = f
                
                # 漏斗 1: 位阶判定
                # 逻辑：价格偏离 20 日线不超过 5% (甚至水下) 且 获利盘比例在 30%-70% 之间 (非极度抛压也非过热)
                if bias_20 is not None and bias_20 < 0.05:
                    if profit is not None and 30 < profit < 85:
                        candidates.append({
                            "code": code,
                            "bias_20": bias_20,
                            "profit": profit,
                            "vol_ratio": vol_ratio
                        })
            
            # 存入 Redis
            if candidates:
                await self.redis.set(self.candidate_key, json.dumps(candidates, ensure_ascii=False))
                logger.info(f"✅ 成功锁定 {len(candidates)} 个技术面潜在种子股")
            
        except Exception as e:
            logger.error(f"扫描执行失败: {e}")

    async def sync_plate_mapping(self):
        """
        全量同步最新板块属性到 Redis (基于补涨挖掘需求)
        """
        logger.info("📡 正在同步全量板块题材映射...")
        # 此处复用 v2_plate_sync_offline 的核心逻辑
        # 限于篇幅，这里简述：调用 pykaipan 获取 5400 只个股题材并 hset 进入 config:plate_mapping:s2p
        from engine_v2.v2_plate_sync_offline import PlateSyncOffline
        sync = PlateSyncOffline()
        sync.redis = self.redis # 共享连接
        await sync.sync_all_stocks()

    async def run(self):
        # 1. 计算技术面
        await self.scan_technical_candidates()
        # 2. 同步板块 (如果距离上次同步已久)
        # await self.sync_plate_mapping()
        
if __name__ == "__main__":
    scanner = PreMarketScanner()
    asyncio.run(scanner.run())
