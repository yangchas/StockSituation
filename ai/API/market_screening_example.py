"""
市场压力位筛选示例 - 展示如何对市场个股进行压力位计算筛选
"""

import time
from market_pressure_screener import MarketPressureScreener


def example_basic_screening():
    """
    基础筛选示例
    """
    print("🎯 示例1: 基础市场筛选")
    print("=" * 80)
    
    # 创建筛选器
    screener = MarketPressureScreener(batch_size=5, delay=1.0)
    
    # 获取股票列表
    stock_codes = screener.get_stock_list("A股")
    print(f"📋 分析股票列表: {stock_codes}")
    
    # 执行筛选
    results = screener.screen_stocks_by_pressure(
        stock_codes=stock_codes,
        min_pattern_score=60,  # 最小形态评分60分
        max_pressure_distance=15.0,  # 压力位距离不超过15%
        require_breakout=True  # 需要突破信号
    )
    
    # 生成报告
    report = screener.generate_screening_report(results)
    print(report)
    
    return results


def example_custom_screening():
    """
    自定义筛选条件示例
    """
    print("\n🎯 示例2: 自定义筛选条件")
    print("=" * 80)
    
    screener = MarketPressureScreener()
    
    # 自定义股票列表
    custom_stocks = ["300433", "300059", "300750", "000001", "600036"]
    
    # 更严格的筛选条件
    results = screener.screen_stocks_by_pressure(
        stock_codes=custom_stocks,
        min_pattern_score=70,  # 更高的形态评分要求
        max_pressure_distance=10.0,  # 更近的压力位距离
        require_breakout=True
    )
    
    report = screener.generate_screening_report(results)
    print(report)
    
    return results


def example_different_markets():
    """
    不同市场筛选示例
    """
    print("\n🎯 示例3: 不同市场筛选")
    print("=" * 80)
    
    screener = MarketPressureScreener()
    
    # 分析不同市场
    markets = ["A股", "创业板", "科创板"]
    
    for market in markets:
        print(f"\n📊 分析{market}市场:")
        print("-" * 40)
        
        stock_codes = screener.get_stock_list(market)
        
        results = screener.screen_stocks_by_pressure(
            stock_codes=stock_codes,
            min_pattern_score=50,  # 降低要求以观察更多股票
            max_pressure_distance=20.0,
            require_breakout=False  # 不要求突破信号
        )
        
        qualified_count = len([r for r in results if r.get('meets_criteria', False)])
        print(f"   符合条件股票: {qualified_count}/{len(stock_codes)}")
        
        # 显示前3只符合条件的股票
        qualified_stocks = [r for r in results if r.get('meets_criteria', False)]
        qualified_stocks.sort(key=lambda x: x['pattern_score'], reverse=True)
        
        for i, stock in enumerate(qualified_stocks[:3], 1):
            print(f"   {i}. {stock['stock_code']} - 评分: {stock['pattern_score']}")


def example_single_stock_analysis():
    """
    单只股票详细分析示例
    """
    print("\n🎯 示例4: 单只股票详细分析")
    print("=" * 80)
    
    screener = MarketPressureScreener()
    
    # 分析单只股票
    stock_code = "300433"
    print(f"📊 详细分析股票: {stock_code}")
    
    result = screener.analyze_single_stock(stock_code)
    
    if result['success']:
        print(f"✅ 分析成功")
        print(f"   当前价格: {result['current_price']:.2f} 元")
        print(f"   形态评分: {result['pattern_score']}/100 ({result['pattern_rating']})")
        print(f"   突破信号: {result['breakout_signals']} 个")
        
        if 'pressure_distance' in result:
            print(f"   压力距离: {result['pressure_distance']:.1f}%")
            print(f"   最近压力位: {result['closest_pressure']:.2f} 元")
        
        if result['pressure_clusters']:
            print(f"   压力集群: {len(result['pressure_clusters'])} 个")
            for i, cluster in enumerate(result['pressure_clusters'], 1):
                prices = [p['price'] for p in cluster]
                print(f"     集群{i}: {min(prices):.2f} - {max(prices):.2f} 元")
    else:
        print(f"❌ 分析失败: {result.get('error', '未知错误')}")


def example_batch_analysis():
    """
    批量分析示例
    """
    print("\n🎯 示例5: 批量分析报告")
    print("=" * 80)
    
    screener = MarketPressureScreener(batch_size=3, delay=2.0)
    
    # 较大的股票列表
    large_stock_list = [
        "000001", "000002", "000858", "600036", "601318",
        "600519", "601888", "300059", "300433", "300750",
        "688001", "688008", "688009"
    ]
    
    print(f"📋 批量分析 {len(large_stock_list)} 只股票")
    print(f"   批量大小: {screener.batch_size}")
    print(f"   请求间隔: {screener.delay} 秒")
    
    start_time = time.time()
    
    results = screener.screen_stocks_by_pressure(
        stock_codes=large_stock_list,
        min_pattern_score=40,  # 较低的评分要求以观察更多结果
        max_pressure_distance=25.0,
        require_breakout=False
    )
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print(f"\n⏱️  分析耗时: {elapsed_time:.1f} 秒")
    
    # 统计结果
    successful_analysis = len([r for r in results if r['success']])
    qualified_stocks = len([r for r in results if r.get('meets_criteria', False)])
    
    print(f"📊 分析结果:")
    print(f"   成功分析: {successful_analysis}/{len(large_stock_list)}")
    print(f"   符合条件: {qualified_stocks}/{successful_analysis}")
    print(f"   成功率: {qualified_stocks/successful_analysis*100:.1f}%")
    
    # 显示评分分布
    scores = [r['pattern_score'] for r in results if r['success']]
    if scores:
        print(f"   平均评分: {sum(scores)/len(scores):.1f}")
        print(f"   最高评分: {max(scores)}")
        print(f"   最低评分: {min(scores)}")


def main():
    """
    主函数 - 运行所有示例
    """
    print("🚀 市场压力位筛选功能示例")
    print("=" * 80)
    print("本示例展示如何对市场个股进行压力位计算和筛选\n")
    
    # 运行所有示例
    example_basic_screening()
    example_custom_screening()
    example_different_markets()
    example_single_stock_analysis()
    example_batch_analysis()
    
    print("\n" + "=" * 80)
    print("✅ 所有示例运行完成!")
    print("\n💡 使用建议:")
    print("1. 调整 min_pattern_score 来控制筛选严格度")
    print("2. 设置 max_pressure_distance 来控制压力位距离")
    print("3. 使用 require_breakout 来要求突破信号")
    print("4. 通过 batch_size 和 delay 控制请求频率")
    print("5. 自定义股票列表来筛选特定板块或个股")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ 示例运行失败: {e}")
        import traceback
        traceback.print_exc()