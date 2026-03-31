import time
import json
import logging
import numpy as np
from typing import Dict, Any, List, Optional

logger = logging.getLogger("MarketEdgeV2.Adapter")

class V2SentimentAdapter:
    def __init__(self, *args, **kwargs):
        print(f"DEBUG: V2SentimentAdapter.__init__ called with args={args}, kwargs={kwargs}")
        self.rust_engine = args[0] if args else kwargs.get('rust_engine')
        self.last_snapshot = {}
        
    def process_snapshot(self, snapshot: Dict[str, Any], today_str: str) -> Dict[str, Any]:
        """
        Translates raw Rust snapshot into V1-compatible business context and identifies extremes.
        """
        self.last_snapshot = snapshot
        
        # 1. Extract Sectors and Extremes (Metadata)
        sectors = snapshot.pop("_SECTORS_", {})
        extremes_meta = snapshot.pop("_EXTREMES_", {})
        
        # 2. Build V1-Compatible Quote Map and Indicators
        ctx_quote_map = {}
        cached_indicators = {}
        
        # Detected Extremes (Business Logic Level)
        detected_extremes = {
            "reversals": [],
            "auction_reversals": [],
            "top_turnover": extremes_meta.get("top_turnover", [])
        }
        
        for symbol, data in snapshot.items():
            if not isinstance(data, dict): continue
            
            # Extract basic metrics
            price = data.get("price", 0.0)
            speed_1m = data.get("speed_1m", 0.0)
            amount_2min = data.get("amount_2min", 0.0)
            
            # V1 Compatibility
            ctx_quote_map[symbol] = {
                "last": price,
                "amount_2min": amount_2min,
                # ... other fields filled as needed
            }
            cached_indicators[symbol] = {
                "speed_1m": speed_1m,
                "amount_2min": amount_2min,
            }
            
            # --- Extreme Behavior Detection ---
            max_p = data.get("max_p", 0.0)
            min_p = data.get("min_p", 0.0)
            p0920 = data.get("p0920", 0.0)
            p0924 = data.get("p0924", 0.0)
            p0925 = data.get("p0925", 0.0)
            
            # TODO: Add logic to fetch yesterday's close to calculate % properly
            # For now, we assume simple price comparison
            
            # 1. 天地板 (Limit-Up to Deep-Red) Detection (Simplified)
            if max_p > 0 and price < max_p * 0.90:  # Drop > 10% from high
                detected_extremes["reversals"].append({"code": symbol, "type": "T_TO_D", "drop": (price - max_p)/max_p})
                
            # 2. Auction Reversal (09:24 Deep Water -> 09:25 Rebound)
            if p0924 > 0 and p0925 > p0924 * 1.03: # Rebound > 3% in last minute of auction
                 detected_extremes["auction_reversals"].append({"code": symbol, "type": "AUCTION_REBOUND", "diff": (p0925 - p0924)/p0924})
        
        return {
            "quote_map": ctx_quote_map,
            "indicators": cached_indicators,
            "sectors": sectors,
            "extremes": detected_extremes
        }

    async def sync_to_redis(self, redis, processed_data: Dict[str, Any], today_str: str):
        """
        Broadcasts processed metrics to Redis for V1 Engine consumption.
        """
        pipe = redis.pipeline()
        
        # 1. Plate Spread Update (N_strong from Rust)
        details_key = f"rank:plate_spread:details:{today_str}"
        for pid, strong_count in processed_data["sectors"].items():
             # We deliver a minimal detail update here; V1 UI can pick it up
             detail = {
                 "ts": int(time.time() * 1000),
                 "id": pid,
                 "N_strong": strong_count,
                 "v2_heartbeat": True
             }
             pipe.hset(details_key, pid, json.dumps(detail, ensure_ascii=False))
             
        # 2. Extremes Broadcaster
        extremes_key = f"market:extremes:{today_str}"
        pipe.hset(extremes_key, "payload", json.dumps(processed_data["extremes"], ensure_ascii=False))
        
        await pipe.execute()
