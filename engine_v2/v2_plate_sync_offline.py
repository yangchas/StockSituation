import os
import sys
import json
import time
import asyncio
import logging
from datetime import datetime

# 环境对齐
sys.path.append('/usr/local/lib/python3.9/site-packages')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PlateSync")

try:
    from pykaipan import pykaipan
except ImportError:
    from pykaipan import KaipanlaApi as pykaipan

class PlateSyncOffline:
    def __init__(self, redis_url="redis://localhost:6379/0"):
        import redis.asyncio as redis
        self.redis = redis.from_url(redis_url, decode_responses=True)
        self.s2p_key = "config:plate_mapping:s2p"

    async def sync_all_stocks(self, batch_size=50):
        """
        全量同步：针对当日竞价流水池中的 1000 只个股(存储在 top_amount 字段)，抓取开盘啦官方定义的最细题材(Reason)
        """
        # 1. 优先从当日竞价流的 top_amount 字段解析
        all_codes = []
        date_compact = "20260327"
        for tag in ["0925", "0924", "wencai"]:
            key = f"market:auction:{date_compact}:{tag}"
            raw = await self.redis.hget(key, "top_amount")
            if raw:
                try:
                    data = json.loads(raw)
                    all_codes = [str(item.get('symbol', item.get('code', '')))[-6:].zfill(6) for item in data]
                    if all_codes: break
                except: continue
        
        # 2. 如果竞价流为空，则从板块映射库保底
        if not all_codes:
            logger.warning("无法从 top_amount 获取列表，使用保底方案...")
            all_codes_raw = await self.redis.hkeys("config:plate_mapping:s2p")
            all_codes = [str(c)[-6:].zfill(6) for c in all_codes_raw if str(c).isdigit()]

        logger.info(f"🚀 准备同步竞价流水池中 {len(all_codes)} 只个股的题材原因...")

        count = 0
        skipped = 0
        
        # 为了防止被封 IP，采用小批量+休眠策略
        for i in range(0, len(all_codes), batch_size):
            batch = all_codes[i:i+batch_size]
            for code in batch:
                # 检查是否已有缓存 (可选：FORCE_SYNC 模式则不跳过)
                exist = await self.redis.hexists(self.s2p_key, code)
                if exist and os.getenv("FORCE_SYNC") != "1":
                    skipped += 1
                    continue
                
                try:
                    # 获取该股的具体涨停原因/题材定义 (动态探测方法名: getBanReasons 或 get_ban_reasons)
                    api_func = None
                    for name in ['getBanReasons', 'get_ban_reasons', 'getBanReason']:
                        if hasattr(pykaipan, name):
                            api_func = getattr(pykaipan, name)
                            break
                    
                    if not api_func:
                        logger.error("无法在 pykaipan 中找到题材获取接口")
                        break

                    res = api_func(code)
                    if res and isinstance(res, dict):
                        # 开盘啦 getBanReasons 统一返回 'List' 键
                        reason_list = res.get('List', [])
                        plates = []
                        for item in reason_list:
                            reason_text = item.get('Reason', '')
                            # 提取核心词: '存储芯片+华为；1. xxx' -> ['存储芯片', '华为']
                            if '；' in reason_text:
                                concepts_part = reason_text.split('；')[0]
                                concepts = concepts_part.split('+')
                                for c in concepts:
                                    c = c.strip()
                                    if c and c != "概念" and c not in plates:
                                        plates.append(c)
                            elif reason_text:
                                # 某些标的可能没分号，直接加
                                for p in reason_text.split('+'):
                                    p = p.strip()
                                    if p and p not in plates: plates.append(p)
                        
                        if plates:
                            await self.redis.hset(self.s2p_key, code, json.dumps(plates, ensure_ascii=False))
                            # logger.info(f"Synced {code}: {plates}")
                except Exception as e:
                    logger.error(f"Failed {code}: {e}")
                
                count += 1
                await asyncio.sleep(0.3) # 严格频率限制

            logger.info(f"进度: {i+batch_size}/{len(all_codes)} (跳过: {skipped})")
            await asyncio.sleep(1)

        logger.info("✅ 全量题材原因同步完成！")

if __name__ == "__main__":
    sync = PlateSyncOffline()
    asyncio.run(sync.sync_all_stocks())
