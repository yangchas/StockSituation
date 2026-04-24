
import asyncio
import sys
import os
import logging

# Add two levels up to path to allow importing 'web' package
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
web_dir = os.path.join(root_dir, 'web')
sys.path.extend([root_dir, web_dir])

import aioredis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def cleanup_simulation():
    """
    Clean up simulation data from Redis to restore 'Real' state.
    """
    redis_url = "redis://localhost:6379/0"
    redis = await aioredis.from_url(redis_url, encoding='utf-8', decode_responses=True)
    
    date_str = "2026-02-09"
    date_compact = date_str.replace('-', '')
    
    keys_to_delete = [
        f"market:auction:{date_compact}:0925", # The simulated Auction Data
        f"market:open_scenario:{date_str}",    # The result of Open Scenario check
        f"rank:danger:{date_str}",             # The Danger List
        "stock:quote:000001",
        "stock:quote:000002",
        "stock:quote:000003",
        "stock:quote:000004",
    ]
    
    logger.info(f"🧹 Cleaning up simulation data for {date_str}...")
    
    count = 0
    for key in keys_to_delete:
        res = await redis.delete(key)
        if res > 0:
            logger.info(f"✅ Deleted: {key}")
            count += 1
        else:
            logger.info(f"⚪ Key not found (already clean): {key}")

    logger.info(f"🎉 Cleanup Complete. Removed {count} keys.")
    logger.info("ℹ️  You can now restart integrated_server.py for Real Replay mode.")
    logger.info("⚠️  Note: Real 'Auction Gap' verification requires real auction data in 'market:auction:YYYYMMDD:0925'. If missing, that specific check will be skipped.")

    await redis.close()

if __name__ == "__main__":
    asyncio.run(cleanup_simulation())
