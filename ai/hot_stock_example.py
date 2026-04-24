"""
热榜数据获取使用示例
展示如何使用HotStockAPI类获取同花顺和东方财富的热榜数据
"""

import asyncio
from HotStockAPI import HotStockAPI, DataSource, sync_get_trending_stocks, sync_get_stocks_by_concept, sync_get_top_concepts


async def example_async_usage():
    """异步使用示例"""
    print("=== 异步使用示例 ===")
    
    # 1. 创建API实例
    ths_api = HotStockAPI(DataSource.THS)  # 同花顺热榜
    em_api = HotStockAPI("eastmoney")      # 东方财富热榜（字符串方式）
    
    # 2. 获取热榜数据
    print("\n1. 获取同花顺热榜前10只股票:")
    ths_stocks = await ths_api.get_trending_stocks(top_n=10)
    print(f"获取到 {len(ths_stocks)} 只股票")
    
    for i, stock in enumerate(ths_stocks, 1):
        print(f"  {i}. {stock['full_code']} {stock['name']} - "
              f"涨幅: {stock['rise_and_fall']:.2f}% - 热度: {stock['rate']}")
    
    # 3. 获取东方财富热榜
    print("\n2. 获取东方财富热榜前5只股票:")
    em_stocks = await em_api.get_trending_stocks(top_n=5)
    print(f"获取到 {len(em_stocks)} 只股票")
    
    for i, stock in enumerate(em_stocks, 1):
        print(f"  {i}. {stock['stock_code']} - 排名: {stock['current_rank']} - "
              f"变化: {stock['change']}")
    
    # 4. 按概念筛选股票（仅同花顺支持）
    print("\n3. 筛选'商业航天'概念股票:")
    concept_stocks = await ths_api.get_stocks_by_concept("商业航天", top_n=5)
    print(f"找到 {len(concept_stocks)} 只商业航天概念股")
    
    for stock in concept_stocks:
        print(f"  {stock['code']} {stock['name']}")
    
    # 5. 获取热门概念
    print("\n4. 获取热门概念标签:")
    top_concepts = await ths_api.get_top_concepts(top_n=8)
    
    for concept, count in top_concepts.items():
        print(f"  {concept}: {count}次")
    
    # 6. 查看数据源信息
    print("\n5. 数据源信息:")
    ths_info = ths_api.get_data_source_info()
    em_info = em_api.get_data_source_info()
    
    print(f"同花顺: {ths_info['description']}")
    print(f"  支持功能: {ths_info['supported_features']}")
    print(f"东方财富: {em_info['description']}")
    print(f"  支持功能: {em_info['supported_features']}")


def example_sync_usage():
    """同步使用示例"""
    print("\n=== 同步使用示例 ===")
    
    # 由于同步函数在异步环境中调用会有冲突，这里只展示同步函数的使用方法
    print("同步函数使用方法:")
    print("1. sync_get_trending_stocks(data_source, top_n=None, **kwargs)")
    print("2. sync_get_stocks_by_concept(concept, top_n=None)")
    print("3. sync_get_top_concepts(top_n=10)")
    print("\n注意: 同步函数应在非异步环境中使用，避免事件循环冲突")


def example_advanced_usage():
    """高级使用示例"""
    print("\n=== 高级使用示例 ===")
    
    # 由于同步函数在异步环境中调用会有冲突，这里只展示高级用法的思路
    print("高级用法思路:")
    print("1. 比较两个数据源的热榜差异")
    print("2. 数据分析和统计（平均涨幅、最大涨幅、涨幅分布等）")
    print("3. 热榜数据可视化")
    print("4. 定时获取热榜数据并存储")
    print("5. 热榜数据与股票基本面数据结合分析")
    print("\n注意: 这些功能需要在非异步环境中使用同步函数实现")


async def main():
    """主函数"""
    try:
        # 运行异步示例
        await example_async_usage()
        
        # 运行同步示例
        example_sync_usage()
        
        # 运行高级示例
        example_advanced_usage()
        
        print("\n=== 示例运行完成 ===")
        
    except Exception as e:
        print(f"示例运行出错: {str(e)}")


if __name__ == "__main__":
    # 运行主函数
    asyncio.run(main())