import numpy as np
import time
from typing import List, Dict, Set, Any, Optional

class MarketDataVector:
    """
    Market Edge V2 Vectorized Data Model.
    Maps symbols to integer indices and stores data in optimized NumPy arrays.
    """
    def __init__(self, capacity: int = 6000):
        self.capacity = capacity
        self.symbols: List[str] = []
        self.symbol_to_idx: Dict[str, int] = {}
        
        # Core Matrix: [N_stocks, N_features]
        # 0: price, 1: change_pct, 2: amount_2min, 3: volume, 4: pre_close, 5: amount_today
        self.data = np.zeros((capacity, 10), dtype=np.float32)
        
        self.last_update_ts = 0.0
        self.current_count = 0

    def update_symbols(self, symbols: List[str]):
        """Rebuild symbol map (L4 task)"""
        self.symbols = sorted(list(set(symbols)))
        self.symbol_to_idx = {s: i for i, s in enumerate(self.symbols)}
        self.current_count = len(self.symbols)
        # Reset data for new symbols
        self.data.fill(0)

    def update_from_redis_quotes(self, quote_map: Dict[str, Dict[str, Any]]):
        """
        Batch update from Redis Hash maps.
        Converts string/mixed types to float32 matrix once.
        """
        for sym, q in quote_map.items():
            idx = self.symbol_to_idx.get(sym)
            if idx is None: continue
            
            # Fill predefined columns
            self.data[idx, 0] = float(q.get('price', 0) or 0)
            self.data[idx, 1] = float(q.get('change_pct', q.get('change', 0)) or 0)
            self.data[idx, 2] = float(q.get('amount_2min', 0) or 0)
            self.data[idx, 5] = float(q.get('amount', 0) or 0)
            self.data[idx, 4] = float(q.get('pre_close', 0) or 0)

        self.last_update_ts = time.time()

    @property
    def price(self): return self.data[:self.current_count, 0]
    
    @property
    def change_pct(self): return self.data[:self.current_count, 1]
    
    @property
    def amount_2min(self): return self.data[:self.current_count, 2]

    def get_strong_mask(self, min_change=1.0, min_amount_2min=5_000_000):
        """
        Vectorized boolean filtering.
        Returns a boolean array mask.
        """
        return (self.change_pct > min_change) & (self.amount_2min > min_amount_2min)

    def get_active_symbols(self, mask: np.ndarray) -> List[str]:
        """Convert mask back to string list if needed (slow path)"""
        indices = np.where(mask)[0]
        return [self.symbols[i] for i in indices]
