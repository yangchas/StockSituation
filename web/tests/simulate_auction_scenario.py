
import asyncio
import sys
import os
import json
import logging
from datetime import datetime

# Add two levels up to path to allow importing 'web' package
# Add project root AND web directory to path to handle mixed imports
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
web_dir = os.path.join(root_dir, 'web')
sys.path.extend([root_dir, web_dir])

import aioredis
from web.market_edge_engine import MarketEdgeEngine
from web.services.advanced_indicators import OptimizedAdvancedTechnicalIndicators
from web.plate_updater import OptimizedPlateUpdater
from web.redis_storage import RedisStorageManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MockRedisStorage:
    def __init__(self, redis):
        self.redis = redis

async def run_simulation():
    """
    Simulate the Auction -> Open Verification flow.
    1. Inject Auction Data (9:25 expectation).
    2. Inject Open Data (9:30 reality).
    3. Run MarketEdgeEngine.calculate_open_scenario to detect gaps.
    """
    redis_url = "redis://localhost:6379/0"
    redis = await aioredis.from_url(redis_url, encoding='utf-8', decode_responses=True)
    
    date_str = "2026-02-09"
    # MarketEdgeEngine uses YYYYMMDD format
    date_compact = date_str.replace('-', '')
    auction_key = f"market:auction:{date_compact}:0925"
    
    logger.info(f"🚀 Starting Auction -> Open Simulation for {date_str} (Key: {auction_key})...")

    # --- Step 1: Simulate Auction Data (Expectation) ---
    logger.info("1️⃣  Simulating Auction Data (9:25)...")
    
    # 000001: Strong Auction (+6.0%)
    # 000002: Very Strong Auction (+9.0%) -> Candidate for Gap (High Open Low Go)
    # 000003: Moderate Auction (+3.0%)
    # 000004: Weak Auction (+1.0%) -> Opens Strong (+5.0%) -> Exceed Expectation!
    auction_data = [
        {"symbol": "000001", "name": "SimStrong", "amount": 10000000, "change_pct": 6.0},
        {"symbol": "000002", "name": "SimGap",    "amount": 20000000, "change_pct": 9.0}, # High Expectation, Low Real
        {"symbol": "000003", "name": "SimNormal", "amount": 5000000,  "change_pct": 3.0},
        {"symbol": "000004", "name": "SimExceed", "amount": 8000000,  "change_pct": 1.0}, # Low Expectation, High Real
    ]
    
    # Clean old data
    await redis.delete(auction_key)
    
    # Write to Redis as HASH with field 'top_amount'
    await redis.hset(auction_key, "top_amount", json.dumps(auction_data))
    logger.info(f"✅ Injected {len(auction_data)} auction targets into {auction_key}")
    
    # --- Step 2: Simulate Open Data (Reality) ---
    logger.info("2️⃣  Simulating Open Quote Data (9:31)...")
    
    # 000001: Opens Stronger (+7.5%) -> Confirmation
    # 000002: Opens Weak (+2.0%) -> REJECTION (Gap!)
    # 000003: Opens Same (+3.0%) -> Neutral
    # 000004: Opens Strong (+5.0%) -> Exceed (1.0 -> 5.0)
    
    stocks_reality = [
        {"code": "000001", "change": 7.5, "price": 10.75},
        {"code": "000002", "change": 2.0, "price": 20.40}, # 9.0 -> 2.0 is a 7% drop!
        {"code": "000003", "change": 3.0, "price": 15.45},
        {"code": "000004", "change": 5.0, "price": 8.88},  # 1.0 -> 5.0 is a 4% jump!
    ]
    
    for s in stocks_reality:
        key = f"stock:quote:{s['code']}"
        # MarketEdgeEngine reads `change_pct` field
        data = {
            "symbol": s['code'],
            "price": s['price'],
            "change_pct": s['change'],
            "amount": 10000000 # Dummy amount
        }
        await redis.hset(key, mapping=data)
        # Also clean up volatile data to ensure clean state if monitor was running?
        # No, MarketEdgeEngine reads stock:quote directly.
        
    logger.info("✅ Injected Open Quotes for targets.")

    # --- Step 3: Run Engine Logic ---
    logger.info("3️⃣  Running MarketEdgeEngine Verification...")
    
    # We need to mock dependencies
    # RedisStorageManager usually needs sync redis, but MarketEdgeEngine needs async redis.
    # We can pass None for components not used in calculate_open_scenario.
    # calculate_open_scenario uses: self.redis, self.advanced_indicators (for backup?)
    # Actually calculate_open_scenario reads stock:quote directly via self.redis OR self.redis_storage?
    # Let's check code... 
    # It uses: `current_price = await self.redis.hget(f"stock:quote:{code}", "price")`
    # It uses `self.redis` (aioredis).
    
    # So we can instantiate MarketEdgeEngine with just Redis!
    
    # Dummy mock for other services
    class DummyService:
        pass
    
    engine = MarketEdgeEngine(
        redis=redis,
        redis_storage=DummyService(),
        plate_updater=DummyService(),
        advanced_indicators=DummyService(),
        calendar=DummyService(),
        theme_ranker=DummyService()
    )
    
    # Force date
    engine.manual_date = date_str
    
    # Run the calculation
    await engine.calculate_open_scenario(date_str)
    
    logger.info("✅ Simulation Complete. Check the logs above for '⚠️ 竞价不及预期' or verification stats.")
    
    # Cleanup
    await redis.close()

if __name__ == "__main__":
    asyncio.run(run_simulation())
