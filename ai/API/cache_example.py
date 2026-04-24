"""
SmartDataCache 使用示例和测试
"""

import asyncio
import time
from datetime import datetime
from smart_data_cache import SmartDataCache, DataCategory

async def test_basic_operations():
    """测试基本操作"""
    print("=== 测试基本操作 ===")
    
    # 创建缓存实例（使用默认配置）
    cache = SmartDataCache()
    await cache.init_connections()
    
    # 测试数据
    test_data = {
        'code': '000001',
        'name': '平安银行',
        'price': 15.67,
        'timestamp': datetime.now().isoformat()
    }
    
    # 1. 测试设置缓存
    await cache.set('stock_000001', test_data, DataCategory.REAL_TIME_PRICE)
    print("✓ 数据设置成功")
    
    # 2. 测试获取缓存（应该命中）
    cached_data = await cache.get('stock_000001')
    print(f"✓ 缓存命中: {cached_data['name']} - {cached_data['price']}")
    
    # 3. 测试强制刷新
    async def fetch_new_data():
        return {
            'code': '000001',
            'name': '平安银行',
            'price': 15.70,  # 新价格
            'timestamp': datetime.now().isoformat()
        }
    
    fresh_data = await cache.get('stock_000001', fetch_func=fetch_new_data, force_fresh=True)
    print(f"✓ 强制刷新: {fresh_data['name']} - {fresh_data['price']}")
    
    # 4. 测试缓存失效
    await cache.invalidate('stock_000001')
    print("✓ 缓存失效成功")
    
    await cache.close()

async def test_different_categories():
    """测试不同数据类型"""
    print("\n=== 测试不同数据类型 ===")
    
    cache = SmartDataCache()
    await cache.init_connections()
    
    # 实时价格数据
    real_time_data = {
        'code': '600519',
        'name': '贵州茅台',
        'price': 1720.50,
        'volume': 12500,
        'timestamp': datetime.now().isoformat()
    }
    await cache.set('stock_600519', real_time_data, DataCategory.REAL_TIME_PRICE)
    print("✓ 实时价格数据缓存成功")
    
    # 热榜数据
    hot_list_data = {
        'timestamp': datetime.now().isoformat(),
        'stocks': [
            {'code': '000001', 'name': '平安银行', 'heat': 95},
            {'code': '600519', 'name': '贵州茅台', 'heat': 88},
            {'code': '000858', 'name': '五粮液', 'heat': 82}
        ]
    }
    await cache.set('hot_list_202501', hot_list_data, DataCategory.HOT_LIST)
    print("✓ 热榜数据缓存成功")
    
    # 涨停原因数据
    limit_up_reason = {
        'code': '002415',
        'name': '海康威视',
        'reason': '人工智能概念走强',
        'timestamp': datetime.now().isoformat()
    }
    await cache.set('limit_reason_002415', limit_up_reason, DataCategory.LIMIT_UP_REASON)
    print("✓ 涨停原因数据缓存成功")
    
    # 获取统计信息
    stats = await cache.get_stats()
    print(f"✓ 缓存统计: 内存大小={stats['memory_cache_size']}, Redis连接={stats['redis_connected']}")
    
    await cache.close()

async def test_performance():
    """测试性能"""
    print("\n=== 测试性能 ===")
    
    cache = SmartDataCache()
    await cache.init_connections()
    
    # 模拟批量数据获取
    stock_codes = [f'{i:06d}' for i in range(1, 101)]  # 000001 到 000100
    
    async def fetch_stock_data(code):
        # 模拟网络延迟
        await asyncio.sleep(0.01)
        return {
            'code': code,
            'name': f'股票{code}',
            'price': 10.0 + int(code) * 0.01,
            'timestamp': datetime.now().isoformat()
        }
    
    start_time = time.time()
    
    # 并发获取数据
    tasks = []
    for code in stock_codes:
        task = cache.get(f'stock_{code}', fetch_func=lambda c=code: fetch_stock_data(c))
        tasks.append(task)
    
    results = await asyncio.gather(*tasks)
    
    elapsed_time = time.time() - start_time
    print(f"✓ 批量获取100只股票数据耗时: {elapsed_time:.3f}秒")
    print(f"✓ 成功获取数据: {len([r for r in results if r is not None])}条")
    
    # 测试缓存命中性能
    start_time = time.time()
    
    cache_tasks = []
    for code in stock_codes:
        task = cache.get(f'stock_{code}')
        cache_tasks.append(task)
    
    cache_results = await asyncio.gather(*cache_tasks)
    
    cache_time = time.time() - start_time
    print(f"✓ 缓存命中获取100只股票数据耗时: {cache_time:.3f}秒")
    
    await cache.close()

async def test_error_handling():
    """测试错误处理"""
    print("\n=== 测试错误处理 ===")
    
    # 使用错误的连接配置
    cache = SmartDataCache(
        redis_host='invalid_host',  # 无效的主机
        tdengine_host='invalid_host'
    )
    
    await cache.init_connections()
    
    # 即使连接失败，内存缓存仍然可用
    test_data = {'test': 'data'}
    await cache.set('test_key', test_data)
    
    cached_data = await cache.get('test_key')
    print(f"✓ 内存缓存仍然可用: {cached_data}")
    
    # 测试获取函数失败的情况
    async def failing_fetch():
        raise Exception("模拟获取失败")
    
    try:
        result = await cache.get('failing_key', fetch_func=failing_fetch)
        print("✗ 应该抛出异常")
    except Exception as e:
        print(f"✓ 正确捕获异常: {e}")
    
    await cache.close()

async def test_real_world_scenario():
    """测试真实场景"""
    print("\n=== 测试真实场景 ===")
    
    cache = SmartDataCache()
    await cache.init_connections()
    
    # 模拟股票数据获取服务
    class StockService:
        def __init__(self, cache):
            self.cache = cache
        
        async def get_stock_real_time(self, code: str):
            """获取股票实时数据"""
            return await self.cache.get(
                key=f'real_time_{code}',
                fetch_func=lambda: self._fetch_real_time(code),
                category=DataCategory.REAL_TIME_PRICE
            )
        
        async def get_hot_list(self):
            """获取热榜数据"""
            return await self.cache.get(
                key='hot_list_current',
                fetch_func=self._fetch_hot_list,
                category=DataCategory.HOT_LIST
            )
        
        async def _fetch_real_time(self, code: str):
            """模拟获取实时数据"""
            await asyncio.sleep(0.05)  # 模拟网络延迟
            return {
                'code': code,
                'name': f'股票{code}',
                'price': 10.0 + int(code) % 100 * 0.5,
                'change': 0.1 + int(code) % 10 * 0.01,
                'volume': 10000 + int(code) % 1000 * 100,
                'timestamp': datetime.now().isoformat()
            }
        
        async def _fetch_hot_list(self):
            """模拟获取热榜数据"""
            await asyncio.sleep(0.1)  # 模拟网络延迟
            return {
                'timestamp': datetime.now().isoformat(),
                'stocks': [
                    {'code': '000001', 'name': '平安银行', 'heat': 95, 'reason': '金融科技'},
                    {'code': '600519', 'name': '贵州茅台', 'heat': 88, 'reason': '消费升级'},
                    {'code': '000858', 'name': '五粮液', 'heat': 82, 'reason': '白酒板块'}
                ]
            }
    
    service = StockService(cache)
    
    # 第一次获取（会调用fetch函数）
    start_time = time.time()
    stock_data = await service.get_stock_real_time('000001')
    first_time = time.time() - start_time
    print(f"✓ 第一次获取耗时: {first_time:.3f}秒")
    
    # 第二次获取（应该命中缓存）
    start_time = time.time()
    cached_stock_data = await service.get_stock_real_time('000001')
    cache_time = time.time() - start_time
    print(f"✓ 缓存命中耗时: {cache_time:.3f}秒")
    print(f"✓ 性能提升: {first_time/cache_time:.1f}x")
    
    # 获取热榜数据
    hot_list = await service.get_hot_list()
    print(f"✓ 热榜数据获取成功: {len(hot_list['stocks'])}只股票")
    
    await cache.close()

async def main():
    """主测试函数"""
    print("开始测试 SmartDataCache 三级缓存系统\n")
    
    try:
        await test_basic_operations()
        await test_different_categories()
        await test_performance()
        await test_error_handling()
        await test_real_world_scenario()
        
        print("\n🎉 所有测试完成！")
        
    except Exception as e:
        print(f"\n❌ 测试过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())