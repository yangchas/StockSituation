import numpy as np
from typing import Dict, List, Set, Any
from ..core.data_model import MarketDataVector

class PlateAnalysisVector:
    """
    Vectorized Plate Analysis.
    Uses matrix multiplication to aggregate stock metrics into plate metrics.
    """
    def __init__(self, data_model: MarketDataVector):
        self.dm = data_model
        # Matrix: [N_stocks, N_plates]
        self.plate_matrix = None
        self.plate_ids: List[str] = []
        self.plate_id_to_idx: Dict[str, int] = {}

    def update_plate_mappings(self, s2p: Dict[str, List[str]]):
        """Build the binary mapping matrix (L4 task)"""
        all_plates = sorted(list(set(p for plates in s2p.values() for p in plates)))
        self.plate_ids = all_plates
        self.plate_id_to_idx = {p: i for i, p in enumerate(all_plates)}
        
        N_s = self.dm.current_count
        N_p = len(all_plates)
        self.plate_matrix = np.zeros((N_s, N_p), dtype=np.float32)
        
        for symbol, plates in s2p.items():
            s_idx = self.dm.symbol_to_idx.get(symbol)
            if s_idx is None: continue
            for p in plates:
                p_idx = self.plate_id_to_idx.get(p)
                if p_idx is not None:
                    self.plate_matrix[s_idx, p_idx] = 1.0

    async def calculate_spread(self, strong_change=1.0, min_amount_2min=5_000_000) -> Dict[str, float]:
        """
        Matrix-based score calculation.
        """
        if self.plate_matrix is None: return {}
        
        # 1. Get binary masks
        strong_mask = (self.dm.change_pct > strong_change) & (self.dm.amount_2min > min_amount_2min)
        active_mask = (self.dm.amount_2min > 0) # Use any activity as denominator
        
        # 2. Matrix Multiplication: [N_plates, N_stocks] @ [N_stocks, 1] -> [N_plates, 1]
        plate_strong_counts = self.plate_matrix.T @ strong_mask.astype(np.float32)
        plate_active_total = self.plate_matrix.T @ active_mask.astype(np.float32)
        
        # 3. Calculate Ratios
        safe_total = np.maximum(1, plate_active_total)
        ratios = plate_strong_counts / safe_total
        
        # 4. Result Mapping (only return active ones)
        results = {}
        top_indices = np.where(ratios > 0.1)[0] # Threshold for logging
        for idx in top_indices:
            pid = self.plate_ids[idx]
            results[pid] = float(ratios[idx])
            
        return results
