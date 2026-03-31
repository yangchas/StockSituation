import asyncio
import logging
import json
from aioredis import from_url
from core.data_model import MarketDataVector
from core.indicator_service import IndicatorServiceVector
from modules.plate_analysis import PlateAnalysisVector

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MarketEdgeV2")

class MarketEdgeV2:
    def __init__(self, redis_url="redis://localhost:6379/0"):
        self.redis_url = redis_url
        self.dm = MarketDataVector(capacity=6000)
        self.isv = None
        self.pav = None
        self.redis = None

    async def initialize(self):
        self.redis = from_url(self.redis_url)
        self.isv = IndicatorServiceVector(self.redis, self.dm)
        self.pav = PlateAnalysisVector(self.dm)
        logger.info("🚀 MarketEdge V2 initialized with Vector Engine.")

    async def run_cycle(self, symbols: set):
        start_ts = asyncio.get_event_loop().time()
        
        # 1. High-Performance Hydration (O(N) batch)
        count = await self.isv.hydrate_market_matrix(symbols)
        
        # 2. Vectorized Plate Analysis (Matrix Multiplication)
        # This replaces the 15-second loop with a sub-millisecond Dot Product
        plate_spread = await self.pav.calculate_spread()
        
        # 3. Log results
        elapsed = (asyncio.get_event_loop().time() - start_ts) * 1000
        logger.info(f"✨ V2 Cycle Completed: {count} stocks processed in {elapsed:.2f}ms")
        if plate_spread:
            logger.info(f"🧭 Top Plates: {dict(list(plate_spread.items())[:3])}")

    async def start(self):
        await self.initialize()
        
        # Example dynamic symbols (in reality, these come from L4 candidate_pool task)
        # For POC, let's assume we monitor 500 stocks
        test_symbols = {f"{i:06d}" for i in range(1, 501)}
        self.dm.update_symbols(list(test_symbols))
        
        # Mocking plate mapping for POC
        mock_s2p = {s: ["plate_1", "plate_2"] for s in list(test_symbols)[:100]}
        self.pav.update_plate_mappings(mock_s2p)

        while True:
            await self.run_cycle(test_symbols)
            await asyncio.sleep(2)

if __name__ == "__main__":
    engine = MarketEdgeV2()
    asyncio.run(engine.start())
