"""
筹码区分布分析使用示例
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from chip_distribution_analyzer import ChipDistributionAnalyzer, format_chip_analysis


def example_single_stock_analysis():
    """示例1: 单只股票分析"""
    print("=== 示例1: 单只股票筹码分布分析 ===\n")
    
    # 创建分析器
    analyzer = ChipDistributionAnalyzer(price_bins=20)
    
    # 分析平安银行
    result = analyzer.analyze_stock('000001', 'day', 60)
    
    # 格式化输出
    print(format_chip_analysis(result))
    
    return result


def example_multiple_periods():
    """示例2: 多周期分析"""
    print("\n\n=== 示例2: 不同周期筹码分布对比 ===\n")
    
    analyzer = ChipDistributionAnalyzer(price_bins=15)
    
    # 分析不同周期的筹码分布
    periods = ['5min', '30min', 'day']
    
    for period in periods:
        print(f"\n--- {period}周期分析 ---")
        result = analyzer.analyze_stock('600519', period, 30)
        chip_data = result['chip_distribution']
        
        print(f"价格区间: {chip_data['price_range']['min']:.2f} - {chip_data['price_range']['max']:.2f}")
        print(f"平均成本: {chip_data['analysis']['avg_cost']:.2f}")
        
        # 筹码集中度
        conc = chip_data['analysis']['concentration']
        print(f"筹码集中区间: {conc['price_range']['range']:.2f}")


def example_stock_comparison():
    """示例3: 多股票比较"""
    print("\n\n=== 示例3: 多股票筹码分布比较 ===\n")
    
    analyzer = ChipDistributionAnalyzer()
    
    # 比较多只股票
    stocks = ['000001', '600519', '300063', '000858']
    
    comparison = analyzer.compare_multiple_stocks(stocks, 'day', 40)
    
    print("各股票平均成本对比:")
    for stock in stocks:
        if stock in comparison['individual_results']:
            result = comparison['individual_results'][stock]
            if 'error' not in result:
                chip_data = result['chip_distribution']
                print(f"  {stock}: {chip_data['analysis']['avg_cost']:.2f}")
    
    # 比较结果
    if comparison['comparison']:
        print("\n比较分析:")
        if 'avg_cost' in comparison['comparison']:
            avg_costs = comparison['comparison']['avg_cost']
            print(f"最低成本股票: {avg_costs['min_stock']}")
            print(f"最高成本股票: {avg_costs['max_stock']}")


def example_custom_parameters():
    """示例4: 自定义参数分析"""
    print("\n\n=== 示例4: 自定义参数分析 ===\n")
    
    # 不同价格区间数量对比
    print("不同价格区间数量对比:")
    for bins in [10, 20, 30]:
        analyzer = ChipDistributionAnalyzer(price_bins=bins)
        result = analyzer.analyze_stock('000001', 'day', 30)
        chip_data = result['chip_distribution']
        
        print(f"  {bins}个区间: 平均成本={chip_data['analysis']['avg_cost']:.2f}, "
              f"密集区数量={len(chip_data['analysis']['dense_areas'])}")
    
    # 成交量加权 vs 非加权
    print("\n成交量加权对比:")
    analyzer_weighted = ChipDistributionAnalyzer(volume_weight=True)
    analyzer_unweighted = ChipDistributionAnalyzer(volume_weight=False)
    
    result_weighted = analyzer_weighted.analyze_stock('600519', 'day', 25)
    result_unweighted = analyzer_unweighted.analyze_stock('600519', 'day', 25)
    
    cost_weighted = result_weighted['chip_distribution']['analysis']['avg_cost']
    cost_unweighted = result_unweighted['chip_distribution']['analysis']['avg_cost']
    
    print(f"  加权平均成本: {cost_weighted:.2f}")
    print(f"  非加权平均成本: {cost_unweighted:.2f}")
    print(f"  差异: {abs(cost_weighted - cost_unweighted):.2f}")


def example_profit_loss_analysis():
    """示例5: 盈亏分析"""
    print("\n\n=== 示例5: 盈亏分析 ===\n")
    
    analyzer = ChipDistributionAnalyzer()
    
    # 分析当前价格下的盈亏情况
    result = analyzer.analyze_stock('000858', 'day', 30)
    
    if result['chip_distribution']['analysis']['profit_loss']:
        pl = result['chip_distribution']['analysis']['profit_loss']
        
        print(f"当前价格: {result['current_price']}")
        print(f"盈利筹码比例: {pl['profit_ratio']*100:.1f}%")
        print(f"亏损筹码比例: {pl['loss_ratio']*100:.1f}%")
        print(f"平均盈利幅度: {pl['avg_profit_pct']:.1f}%")
        print(f"平均亏损幅度: {pl['avg_loss_pct']:.1f}%")
        
        # 判断筹码盈亏状态
        if pl['profit_ratio'] > 0.7:
            print("📈 大部分筹码处于盈利状态")
        elif pl['loss_ratio'] > 0.7:
            print("📉 大部分筹码处于亏损状态")
        else:
            print("⚖️  筹码盈亏分布相对均衡")


def example_advanced_analysis():
    """示例6: 高级分析功能"""
    print("\n\n=== 示例6: 高级分析功能 ===\n")
    
    analyzer = ChipDistributionAnalyzer(price_bins=25)
    
    # 分析特定股票
    result = analyzer.analyze_stock('300063', 'day', 50)
    chip_data = result['chip_distribution']
    analysis = chip_data['analysis']
    
    print("📊 详细分析报告:")
    print(f"股票: {result['stock_code']}")
    print(f"分析数据: {result['data_period']['data_points']}个交易日")
    
    # 成本分布
    cost_dist = analysis['cost_distribution']
    print(f"\n成本分布:")
    print(f"  25%分位数: {cost_dist['q25']:.2f}")
    print(f"  中位数: {cost_dist['q50']:.2f}")
    print(f"  75%分位数: {cost_dist['q75']:.2f}")
    print(f"  90%分位数: {cost_dist['q90']:.2f}")
    
    # 筹码密集区
    print(f"\n主要筹码密集区:")
    for i, area in enumerate(analysis['dense_areas'][:3], 1):
        print(f"  密集区{i}: {area['price_range']['start']:.2f}-{area['price_range']['end']:.2f} "
              f"({area['chip_percent']:.1f}%筹码)")
    
    # 筹码集中度分析
    conc = analysis['concentration']
    concentration_ratio = conc['price_range']['range'] / chip_data['price_range']['range'] * 100
    print(f"\n筹码集中度:")
    print(f"  70%筹码集中在 {conc['price_range']['range']:.2f} 的价格区间内")
    print(f"  占整个价格区间的 {concentration_ratio:.1f}%")


def main():
    """运行所有示例"""
    print("筹码区分布分析使用示例")
    print("=" * 50)
    
    examples = [
        example_single_stock_analysis,
        example_multiple_periods,
        example_stock_comparison,
        example_custom_parameters,
        example_profit_loss_analysis,
        example_advanced_analysis
    ]
    
    for example_func in examples:
        try:
            example_func()
            print("\n" + "="*50 + "\n")
        except Exception as e:
            print(f"示例执行失败: {e}")
            print("\n" + "="*50 + "\n")
    
    print("🎉 所有示例执行完成！")


if __name__ == "__main__":
    main()