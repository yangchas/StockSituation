import asyncio
import time
from typing import List, Dict, Any, Set
from .data_model import MarketDataVector

class IndicatorServiceVector:
    """
    Standardized Indicator Service for V2.
    Fetches raw Redis data and hydrates the DataModel matrix.
    """
    def __init__(self, redis_client, data_model: MarketDataVector):
        self.redis = redis_client
        self.dm = data_model

    async def hydrate_market_matrix(self, symbols: Set[str]):
        """
        P0: Fetch ALL required quotes in a single Redis Pipeline.
        Hydrates the unified matrix in O(N).
        """
        if not symbols: return
        
        # 1. Pipeline fetch
        pipe = self.redis.pipeline()
        sym_list = list(symbols)
        for s in sym_list:
            pipe.hgetall(f"stock:quote:{s}")
        
        raw_results = await pipe.execute()
        
        # 2. Batch hydrate
        # We transform bytes to dicts first (this is the bottleneck in Python)
        processed_map = {}
        for i, s in enumerate(sym_list):
            raw = raw_results[i]
            if not raw: continue
            
            # Fast decode - only what we need
            processed_map[s] = {
                k.decode('utf-8') if isinstance(k, bytes) else k: 
                v.decode('utf-8') if isinstance(v, bytes) else v 
                for k, v in raw.items()
            }
            
        # 3. Update Matrix
        self.dm.update_from_redis_quotes(processed_map)
        return len(processed_map)
