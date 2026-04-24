"""
懒加载数据获取器 LazyDataFetcher
基于SmartDataCache实现智能数据获取和缓存管理
"""

import asyncio
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from enum import Enum

# 导入SmartDataCache
from smart_data_cache import SmartDataCache, DataCategory

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('LazyDataFetcher')

class FocusLevel(Enum):
    """股票关注度级别"""
    HIGH = "high"        # 高关注
    MEDIUM = "medium"    # 中关注
    LOW = "low"          # 低关注
    BACKGROUND = "background"  # 后台

class DataType(Enum):
    """数据类型"""
    BASIC = "basic"          # 基础数据
    REAL_TIME = "real_time"  # 实时数据
    ALL = "all"              # 全部数据

# 刷新频率配置（单位：秒）
REFRESH_INTERVALS = {
    FocusLevel.HIGH: 30,         # 高关注：每30秒
    FocusLevel.MEDIUM: 120,      # 中关注：每2分钟
    FocusLevel.LOW: 300,         # 低关注：每5分钟
    FocusLevel.BACKGROUND: 1800  # 后台：每30分钟
}

class UnifiedMarketDataFetcher:
    """统一市场数据获取器（接口类）"""
    
    async def get_stock_basic_data(self, stock_code: str) -> Dict[str, Any]:
        """获取股票基础数据"""
        raise NotImplementedError
    
    async def get_stock_real_time_data(self, stock_code: str) -> Dict[str, Any]:
        """获取股票实时数据"""
        raise NotImplementedError
    
    async def get_hot_stocks(self) -> List[Dict[str, Any]]:
        """获取热榜股票"""
        raise NotImplementedError
    
    async def get_limit_up_stocks(self) -> List[Dict[str, Any]]:
        """获取涨停股票"""
        raise NotImplementedError
    
    async def batch_get_stock_data(self, stock_codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """批量获取股票数据"""
        raise NotImplementedError

class MockUnifiedMarketDataFetcher(UnifiedMarketDataFetcher):
    """模拟统一市场数据获取器"""
    
    async def get_stock_basic_data(self, stock_code: str) -> Dict[str, Any]:
        """模拟获取股票基础数据"""
        await asyncio.sleep(0.01)  # 模拟网络延迟
        return {
            'code': stock_code,
            'name': f'股票{stock_code}',
            'industry': '金融' if stock_code.startswith('00') else '科技',
            'market_value': 10000000000 + int(stock_code) % 1000 * 10000000,
            'pe_ratio': 15.0 + int(stock_code) % 10 * 0.5,
            'pb_ratio': 1.5 + int(stock_code) % 5 * 0.1,
            'update_time': datetime.now().isoformat()
        }
    
    async def get_stock_real_time_data(self, stock_code: str) -> Dict[str, Any]:
        """模拟获取股票实时数据"""
        await asyncio.sleep(0.02)  # 模拟网络延迟
        return {
            'code': stock_code,
            'price': 10.0 + int(stock_code) % 100 * 0.1,
            'change': 0.05 + int(stock_code) % 10 * 0.01,
            'change_percent': 0.5 + int(stock_code) % 5 * 0.1,
            'volume': 100000 + int(stock_code) % 1000 * 1000,
            'amount': 5000000 + int(stock_code) % 100 * 100000,
            'high': 12.0 + int(stock_code) % 100 * 0.1,
            'low': 9.5 + int(stock_code) % 100 * 0.1,
            'timestamp': datetime.now().isoformat()
        }
    
    async def get_hot_stocks(self) -> List[Dict[str, Any]]:
        """模拟获取热榜股票"""
        await asyncio.sleep(0.05)
        return [
            {'code': '000001', 'name': '平安银行', 'heat': 95},
            {'code': '600519', 'name': '贵州茅台', 'heat': 88},
            {'code': '000858', 'name': '五粮液', 'heat': 82},
            {'code': '002415', 'name': '海康威视', 'heat': 78},
            {'code': '601318', 'name': '中国平安', 'heat': 75}
        ] + [{'code': f'{i:06d}', 'name': f'股票{i:06d}', 'heat': 70 - i} for i in range(6, 101)]
    
    async def get_limit_up_stocks(self) -> List[Dict[str, Any]]:
        """模拟获取涨停股票"""
        await asyncio.sleep(0.03)
        return [
            {'code': '000001', 'name': '平安银行', 'limit_type': '涨停'},
            {'code': '002415', 'name': '海康威视', 'limit_type': '涨停'},
            {'code': '300059', 'name': '东方财富', 'limit_type': '涨停'}
        ]
    
    async def batch_get_stock_data(self, stock_codes: List[str]) -> Dict[str, Dict[str, Any]]:
        """模拟批量获取股票数据"""
        await asyncio.sleep(0.01 * len(stock_codes))  # 模拟批量延迟
        
        result = {}
        for code in stock_codes:
            basic_data = await self.get_stock_basic_data(code)
            real_time_data = await self.get_stock_real_time_data(code)
            
            # 合并数据
            result[code] = {
                **basic_data,
                **real_time_data,
                'update_time': datetime.now().isoformat()
            }
        
        return result

class LazyDataFetcher:
    """懒加载数据获取器"""
    
    def __init__(self, cache: SmartDataCache, base_fetcher: UnifiedMarketDataFetcher):
        """
        初始化懒加载数据获取器
        
        Args:
            cache: SmartDataCache实例
            base_fetcher: 基础数据获取器
        """
        self.cache = cache
        self.base_fetcher = base_fetcher
        
        # 关注度相关数据缓存
        self.hot_stocks_cache = []
        self.limit_up_stocks_cache = []
        self.stock_focus_levels = {}  # 股票关注度缓存
        
        # 监控列表（可以从配置文件或数据库加载）
        self.monitored_stocks = [
            '000001', '600519', '000858', '002415', '601318', '300059',
            '000002', '600036', '000333', '600887', '600276'
        ]
        
        # 统计数据
        self.stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'network_requests': 0,
            'last_update_time': datetime.now()
        }
        
        logger.info("LazyDataFetcher初始化完成")
    
    async def _get_stock_focus_level(self, stock_code: str) -> FocusLevel:
        """
        动态评估股票关注度
        
        Args:
            stock_code: 股票代码
            
        Returns:
            FocusLevel: 关注度级别
        """
        # 检查缓存
        if stock_code in self.stock_focus_levels:
            cached_level, cache_time = self.stock_focus_levels[stock_code]
            # 关注度评估结果缓存5分钟
            if (datetime.now() - cache_time).total_seconds() < 300:
                return cached_level
        
        # 获取最新数据
        await self._update_focus_data()
        
        # 判断逻辑
        focus_level = FocusLevel.BACKGROUND
        
        # 检查是否在监控列表
        if stock_code not in self.monitored_stocks:
            focus_level = FocusLevel.BACKGROUND
        else:
            # 高关注：热榜前50名 + 今日涨停股 + 成交额>10亿
            is_high_focus = (
                any(stock['code'] == stock_code for stock in self.hot_stocks_cache[:50]) or
                any(stock['code'] == stock_code for stock in self.limit_up_stocks_cache) or
                await self._is_high_turnover(stock_code)
            )
            
            if is_high_focus:
                focus_level = FocusLevel.HIGH
            else:
                # 中关注：热榜51-100名 + 昨日涨停股（这里简化处理）
                is_medium_focus = any(stock['code'] == stock_code for stock in self.hot_stocks_cache[50:100])
                
                if is_medium_focus:
                    focus_level = FocusLevel.MEDIUM
                else:
                    # 低关注：其他在监控列表的股票
                    focus_level = FocusLevel.LOW
        
        # 更新缓存
        self.stock_focus_levels[stock_code] = (focus_level, datetime.now())
        
        logger.debug(f"股票 {stock_code} 关注度评估为: {focus_level.value}")
        return focus_level
    
    async def _is_high_turnover(self, stock_code: str) -> bool:
        """判断股票是否高成交额（>10亿）"""
        try:
            # 获取实时数据判断成交额
            real_time_data = await self.get_stock_data(stock_code, DataType.REAL_TIME.value)
            if real_time_data and 'amount' in real_time_data:
                return real_time_data['amount'] > 1000000000  # 10亿
        except Exception as e:
            logger.warning(f"判断成交额失败: {e}")
        
        return False
    
    async def _update_focus_data(self) -> None:
        """更新关注度相关数据"""
        try:
            # 获取热榜数据（缓存30秒）
            self.hot_stocks_cache = await self.cache.get(
                key='hot_stocks_focus',
                fetch_func=self.base_fetcher.get_hot_stocks,
                category=DataCategory.HOT_LIST
            ) or []
            
            # 获取涨停股票数据（缓存10秒）
            self.limit_up_stocks_cache = await self.cache.get(
                key='limit_up_stocks_focus',
                fetch_func=self.base_fetcher.get_limit_up_stocks,
                category=DataCategory.LIMIT_UP_STATUS
            ) or []
            
        except Exception as e:
            logger.warning(f"更新关注度数据失败: {e}")
    
    async def _should_update(self, stock_code: str, data_type: str) -> bool:
        """
        判断是否应该更新数据
        
        Args:
            stock_code: 股票代码
            data_type: 数据类型
            
        Returns:
            bool: 是否应该更新
        """
        # 获取关注度级别
        focus_level = await self._get_stock_focus_level(stock_code)
        
        # 获取缓存键
        cache_key = self._get_cache_key(stock_code, data_type)
        
        # 检查缓存中是否有数据
        cached_data = await self.cache.get(cache_key)
        if cached_data is None:
            return True  # 没有缓存数据，需要更新
        
        # 检查数据新鲜度
        if 'update_time' in cached_data:
            try:
                update_time = datetime.fromisoformat(cached_data['update_time'])
                time_diff = (datetime.now() - update_time).total_seconds()
                
                # 根据关注度级别判断是否需要更新
                refresh_interval = REFRESH_INTERVALS[focus_level]
                
                if time_diff > refresh_interval:
                    logger.debug(f"数据已过期，需要更新: {stock_code} ({focus_level.value})")
                    return True
                else:
                    logger.debug(f"数据仍新鲜，使用缓存: {stock_code}")
                    return False
                    
            except Exception as e:
                logger.warning(f"检查数据新鲜度失败: {e}")
                return True
        
        return True  # 没有时间戳，需要更新
    
    def _get_cache_key(self, stock_code: str, data_type: str) -> str:
        """生成缓存键"""
        return f"{data_type}_{stock_code}"
    
    async def _fetch_with_retry(self, fetch_func: Callable, max_retries: int = 3) -> Any:
        """
        带重试机制的数据获取
        
        Args:
            fetch_func: 获取函数
            max_retries: 最大重试次数
            
        Returns:
            Any: 获取的数据
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    # 指数退避延迟
                    delay = min(2 ** attempt, 10)  # 最大10秒
                    logger.info(f"第{attempt + 1}次重试，延迟{delay}秒")
                    await asyncio.sleep(delay)
                
                result = await fetch_func()
                self.stats['network_requests'] += 1
                
                if attempt > 0:
                    logger.info(f"重试成功: 第{attempt + 1}次尝试")
                
                return result
                
            except Exception as e:
                last_error = e
                logger.warning(f"第{attempt + 1}次获取失败: {e}")
        
        # 所有重试都失败
        logger.error(f"数据获取失败，已达到最大重试次数{max_retries}: {last_error}")
        raise last_error
    
    async def get_stock_data(self, stock_code: str, data_type: str = 'all') -> Dict[str, Any]:
        """
        获取股票数据
        
        Args:
            stock_code: 股票代码
            data_type: 数据类型（basic/real_time/all）
            
        Returns:
            Dict[str, Any]: 股票数据
        """
        self.stats['total_requests'] += 1
        
        # 检查是否需要更新
        should_update = await self._should_update(stock_code, data_type)
        cache_key = self._get_cache_key(stock_code, data_type)
        
        if not should_update:
            # 使用缓存数据
            cached_data = await self.cache.get(cache_key)
            if cached_data:
                self.stats['cache_hits'] += 1
                logger.debug(f"缓存命中: {stock_code} ({data_type})")
                return cached_data
        
        # 需要从网络获取数据
        async def fetch_data():
            """获取数据的函数"""
            if data_type == DataType.BASIC.value:
                return await self.base_fetcher.get_stock_basic_data(stock_code)
            elif data_type == DataType.REAL_TIME.value:
                return await self.base_fetcher.get_stock_real_time_data(stock_code)
            else:  # all
                basic_data = await self.base_fetcher.get_stock_basic_data(stock_code)
                real_time_data = await self.base_fetcher.get_stock_real_time_data(stock_code)
                
                # 合并数据
                return {
                    **basic_data,
                    **real_time_data,
                    'update_time': datetime.now().isoformat()
                }
        
        # 带重试机制的数据获取
        data = await self._fetch_with_retry(fetch_data)
        
        if data:
            # 确定缓存分类
            focus_level = await self._get_stock_focus_level(stock_code)
            
            if focus_level == FocusLevel.HIGH:
                cache_category = DataCategory.REAL_TIME_PRICE
            elif focus_level == FocusLevel.MEDIUM:
                cache_category = DataCategory.BIG_ORDER_NET_AMOUNT
            elif focus_level == FocusLevel.LOW:
                cache_category = DataCategory.LIMIT_UP_STATUS
            else:
                cache_category = DataCategory.DEFAULT
            
            # 缓存数据
            await self.cache.set(cache_key, data, cache_category)
            
            logger.info(f"数据获取成功: {stock_code} ({data_type})")
        
        return data or {}
    
    async def batch_get(self, codes: List[str], data_type: str = 'basic') -> Dict[str, Dict[str, Any]]:
        """
        批量获取股票数据
        
        Args:
            codes: 股票代码列表
            data_type: 数据类型
            
        Returns:
            Dict[str, Dict[str, Any]]: 股票数据字典
        """
        result = {}
        
        # 分组处理：需要更新的和可以使用缓存的
        update_tasks = []
        cache_tasks = []
        
        for code in codes:
            should_update = await self._should_update(code, data_type)
            
            if should_update:
                update_tasks.append((code, data_type))
            else:
                cache_tasks.append((code, data_type))
        
        logger.info(f"批量获取: 需要更新{len(update_tasks)}只，缓存命中{len(cache_tasks)}只")
        
        # 处理缓存数据
        cache_results = await asyncio.gather(*[
            self.cache.get(self._get_cache_key(code, data_type)) for code, data_type in cache_tasks
        ])
        
        for (code, data_type), cached_data in zip(cache_tasks, cache_results):
            if cached_data:
                result[code] = cached_data
                self.stats['cache_hits'] += 1
        
        # 批量获取需要更新的数据
        if update_tasks:
            update_codes = [task[0] for task in update_tasks]
            
            async def batch_fetch():
                """批量获取函数"""
                if data_type == DataType.BASIC.value:
                    # 这里简化处理，实际应该根据data_type调用不同的批量方法
                    return await self.base_fetcher.batch_get_stock_data(update_codes)
                else:
                    return await self.base_fetcher.batch_get_stock_data(update_codes)
            
            batch_data = await self._fetch_with_retry(batch_fetch)
            
            # 缓存批量数据
            cache_tasks = []
            for code, stock_data in batch_data.items():
                if stock_data:
                    result[code] = stock_data
                    cache_key = self._get_cache_key(code, data_type)
                    
                    # 确定缓存分类
                    focus_level = await self._get_stock_focus_level(code)
                    
                    if focus_level == FocusLevel.HIGH:
                        cache_category = DataCategory.REAL_TIME_PRICE
                    elif focus_level == FocusLevel.MEDIUM:
                        cache_category = DataCategory.BIG_ORDER_NET_AMOUNT
                    elif focus_level == FocusLevel.LOW:
                        cache_category = DataCategory.LIMIT_UP_STATUS
                    else:
                        cache_category = DataCategory.DEFAULT
                    
                    cache_tasks.append(self.cache.set(cache_key, stock_data, cache_category))
            
            # 异步缓存数据
            if cache_tasks:
                await asyncio.gather(*cache_tasks)
        
        self.stats['total_requests'] += len(codes)
        
        return result
    
    async def prefetch_focus_stocks(self) -> None:
        """预加载焦点股票"""
        logger.info("开始预加载焦点股票")
        
        # 获取高关注度股票列表
        await self._update_focus_data()
        
        # 高关注度股票：热榜前50 + 涨停股
        high_focus_codes = set()
        
        # 热榜前50
        for stock in self.hot_stocks_cache[:50]:
            high_focus_codes.add(stock['code'])
        
        # 涨停股
        for stock in self.limit_up_stocks_cache:
            high_focus_codes.add(stock['code'])
        
        # 添加监控列表中的高成交额股票
        for code in self.monitored_stocks:
            if await self._is_high_turnover(code):
                high_focus_codes.add(code)
        
        high_focus_list = list(high_focus_codes)[:20]  # 限制预加载数量
        
        if high_focus_list:
            logger.info(f"预加载{len(high_focus_list)}只高关注度股票")
            
            # 批量预加载实时数据
            await self.batch_get(high_focus_list, DataType.REAL_TIME.value)
            
            logger.info("焦点股票预加载完成")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        hit_rate = (self.stats['cache_hits'] / self.stats['total_requests'] * 100) if self.stats['total_requests'] > 0 else 0
        
        return {
            **self.stats,
            'cache_hit_rate': f"{hit_rate:.2f}%",
            'monitored_stocks_count': len(self.monitored_stocks),
            'focus_levels_count': len(self.stock_focus_levels),
            'current_time': datetime.now().isoformat()
        }

# 使用示例
async def example_usage():
    """使用示例"""
    
    # 创建缓存和数据获取器
    cache = SmartDataCache()
    await cache.init_connections()
    
    base_fetcher = MockUnifiedMarketDataFetcher()
    lazy_fetcher = LazyDataFetcher(cache, base_fetcher)
    
    # 预加载焦点股票
    await lazy_fetcher.prefetch_focus_stocks()
    
    # 获取单个股票数据
    stock_data = await lazy_fetcher.get_stock_data('000001', 'all')
    print(f"获取的股票数据: {stock_data.get('name')} - {stock_data.get('price')}")
    
    # 批量获取数据
    codes = ['000001', '600519', '000858']
    batch_data = await lazy_fetcher.batch_get(codes, 'basic')
    print(f"批量获取数据量: {len(batch_data)}")
    
    # 获取统计信息
    stats = lazy_fetcher.get_stats()
    print(f"缓存命中率: {stats['cache_hit_rate']}")
    
    await cache.close()

if __name__ == "__main__":
    asyncio.run(example_usage())