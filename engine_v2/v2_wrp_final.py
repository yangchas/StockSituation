import logging
import time
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class RustMarketEngineAdapter:
    """
    Python-Side Bridge for the V2 Rust Core Matrix.
    
    This class handles the dynamic loading of the compiled PyO3 module
    and provides a safe, decoupled API for the existing MarketEdgeEngine.
    """
    
    def __init__(self, max_stocks: int = 5500):
        self.max_stocks = max_stocks
        self.engine = None
        self._load_binary()
        
    def _load_binary(self):
        """Attempts to load the compiled rust_engine .so/.pyd module."""
        try:
            import market_edge_v2_core
            self.engine = market_edge_v2_core.MarketEngine()
            logger.info("✅ 成功加载 V2 Rust 核心运算模块 (market_edge_v2_core)!")
        except ImportError as e:
            logger.critical(f"❌ 无法加载 Rust 底层模块: {e}")
            self.engine = None
            
    def register_symbols(self, symbols: list[str]):
        """盘前静态注入：注册所有的股票代码"""
        if not self.engine: return
        self.engine.register_symbols(symbols)
        logger.info(f"📊 已在 Rust 底层注册 {len(symbols)} 只证券结构体")

    def register_plate_mapping(self, plate_id: str, symbols: list[str]):
        """板块映射注入：注册板块与股票的归属关系"""
        if self.engine:
            self.engine.register_plate_mapping(plate_id, symbols)
        
    def push_tick_raw(self, symbol: str, price: float, amount: float, volume: float, time_str: str = "00:00:00", bid_amount: float = 0.0):
        """盘中极速 Tick 压入"""
        if self.engine:
            self.engine.push_tick(symbol, price, amount, volume, time_str, bid_amount)
            
    def get_snapshot(self) -> Dict[str, Any]:
        """获取全市场最新状态字典"""
        if not self.engine:
            return {}
        return self.engine.get_snapshot()

# 独立单例供其他模块安全导入
v2_core_bridge = RustMarketEngineAdapter()
