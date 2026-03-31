import asyncio
import aiohttp
import time
import logging
from typing import List, Dict, Optional, Any
from v2_data_service import SinaDataAdapter, KaipanlaDataAdapter, StandardSnapshot, StandardKLine
from v2_async_pipeline import RateLimitError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("V2Fetcher")

class UnifiedDataFetcher:
    """
    统一数据抓取器 - 支持探针、自动切源、清洗与增量获取
    """
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.sina = SinaDataAdapter()
        self.kaipan = KaipanlaDataAdapter()
        self.headers = {
            "Referer": "https://finance.sina.com.cn",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        
    async def fetch_sina_ticks(self, symbols: List[str]) -> List[StandardSnapshot]:
        """批量获取新浪实时行情"""
        url = f"https://hq.sinajs.cn/list={','.join(symbols)}"
        try:
            async with self.session.get(url, headers=self.headers, timeout=5) as resp:
                text = await resp.text()
                lines = text.strip().split("\n")
                results = []
                for line in lines:
                    snap = self.sina.format_snapshot(line)
                    if snap: results.append(snap)
                return results
        except Exception as e:
            logger.error(f"Sina Tick Fetch Error: {e}")
            return []

    async def fetch_sina_kline(self, symbol: str, resolution: str = "day", datalen: int = 1) -> List[StandardKLine]:
        """增量获取新浪/其它的 K 线数据"""
        scale = 240 if resolution == "day" else (5 if resolution == "5m" else 60)
        url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
        params = {"symbol": symbol, "scale": scale, "ma": 5, "datalen": datalen}
        try:
            async with self.session.get(url, params=params, headers=self.headers, timeout=5) as resp:
                if resp.status == 456:
                    raise RateLimitError(f"Sina 456 for {symbol}")
                raw_json = await resp.json()
                if not raw_json or not isinstance(raw_json, list):
                    return []
                return self.sina.format_kline(raw_json, resolution)
        except Exception as e:
            if isinstance(e, RateLimitError):
                raise e
            logger.debug(f"Sina KLine Fetch Error ({symbol}): {e}")
            return []

    async def probe_source(self, source_type: str) -> bool:
        """探针检测 - 如果探测失败则返回 False，触发逻辑层切换源"""
        test_symbol = "sh600000"
        if source_type == "sina":
            res = await self.fetch_sina_ticks([test_symbol])
            return len(res) > 0
        return False

async def test_fetcher():
    async with aiohttp.ClientSession() as session:
        fetcher = UnifiedDataFetcher(session)
        
        print("\n[Probe Test]")
        ok = await fetcher.probe_source("sina")
        print(f"Sina Source Health: {'✅ OK' if ok else '❌ FAIL'}")
        
        if ok:
            print("\n[Batch Tick Test]")
            snaps = await fetcher.fetch_sina_ticks(["sh600000", "sz000001"])
            for s in snaps:
                print(f" -> {s.code}: {s.price} ({s.change_pct}%)")
                
            print("\n[Incremental K-Line Test]")
            klines = await fetcher.fetch_sina_kline("sz000001", "5m", 2)
            if klines:
                print(f" -> 5m KLine Count: {len(klines)}, Latest: {klines[-1].dt}")

if __name__ == "__main__":
    asyncio.run(test_fetcher())
