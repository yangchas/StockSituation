"""
动态衰减模型筹码分析使用示例
展示基于换手率衰减的筹码分布计算方法
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from turnover_decay_analyzer import TurnoverDecayAnalyzer

def create_realistic_market_data():
    """创建更真实的股票市场数据"""
    
    def create_stock_kline(stock_code: str, base_price: float, volatility: float, days: int = 60):
        """创建单个股票的K线数据"""
        # 生成5分钟间隔的时间戳
        start_date = datetime.now() - timedelta(days=days)
        dates = pd.date_range(start_date, datetime.now(), freq='5min')
        
        # 模拟价格序列（带趋势的随机游走）
        np.random.seed(hash(stock_code) % 1000)
        prices = [base_price]
        
        for i in range(1, len(dates)):
            # 添加趋势和随机波动
            trend = np.random.choice([-0.00005, 0.00005, 0.0001])  # 随机趋势
            random_walk = np.random.normal(0, volatility)
            new_price = prices[-1] * (1 + trend + random_walk)
            prices.append(max(base_price * 0.5, new_price))  # 防止价格过低
        
        # 模拟成交额（与价格和交易活跃度相关）
        base_amount = base_price * 100000  # 基础成交额
        amounts = np.random.uniform(base_amount * 0.5, base_amount * 3, len(dates))
        
        # 添加交易活跃度变化
        for i in range(len(amounts)):
            hour = dates[i].hour
            minute = dates[i].minute
            
            # 模拟开盘和收盘活跃度
            if (hour == 9 and minute >= 30) or (hour == 10) or (hour == 14) or (hour == 15 and minute < 15):
                amounts[i] *= 2.0  # 交易活跃时段
            elif (hour == 11 and minute >= 30) or (hour == 13 and minute < 1):
                amounts[i] *= 0.3  # 午间休息
        
        return pd.DataFrame({
            'timestamp': dates,
            'close': prices,
            'amount': amounts
        })
    
    # 创建多只股票数据
    stocks_data = {
        '000001': {'base_price': 11.5, 'volatility': 0.008, 'name': '平安银行'},
        '000858': {'base_price': 85.0, 'volatility': 0.015, 'name': '五粮液'},
        '000002': {'base_price': 25.0, 'volatility': 0.012, 'name': '万科A'},
        '600519': {'base_price': 1600.0, 'volatility': 0.02, 'name': '贵州茅台'},
        '601318': {'base_price': 45.0, 'volatility': 0.01, 'name': '中国平安'},
    }
    
    kline_data = {}
    for code, info in stocks_data.items():
        kline_data[code] = create_stock_kline(code, info['base_price'], info['volatility'])
    
    return kline_data, stocks_data

def example_single_stock_analysis():
    """示例1：单只股票详细分析"""
    print("=== 示例1：单只股票详细分析 ===")
    
    # 创建分析器
    analyzer = TurnoverDecayAnalyzer(decay_factor=0.9, price_bins=40)
    
    # 获取数据
    kline_data, stocks_info = create_realistic_market_data()
    stock_code = '000001'
    
    # 分析单只股票
    current_price = kline_data[stock_code]['close'].iloc[-1]
    result = analyzer.analyze_chip_distribution(
        kline_data[stock_code], stock_code, current_price, days=30
    )
    
    print(f"股票: {stocks_info[stock_code]['name']}({stock_code})")
    print(f"当前价格: {result['current_price']:.2f}")
    print(f"分析天数: {result['analysis_days']}")
    print(f"总筹码量: {result['total_chips']:.4f}")
    print(f"筹码集中度: {result['concentration_ratio']:.2%}")
    
    print("\\n=== 涨停板区间分析 ===")
    limit_up = result['limit_up_analysis']
    for interval, data in limit_up.items():
        interval_names = {
            'current_to_1st': '当前价→一板',
            '1st_to_2nd': '一板→二板', 
            '2nd_to_3rd': '二板→三板'
        }
        print(f"  {interval_names[interval]}: {data['chip_ratio_percent']:.2f}% "
              f"(价格区间: {data['lower_bound']:.2f}-{data['upper_bound']:.2f})")
    
    print("\\n=== 压力评估 ===")
    pressure = result['pressure_assessment']
    for key, value in pressure.items():
        key_names = {
            '1st_limit_pressure': '一板压力',
            '2nd_limit_pressure': '二板压力',
            'overall_pressure': '总体压力',
            'suitable_for_limit_up': '适合涨停'
        }
        print(f"  {key_names[key]}: {value}")
    
    return result

def example_multiple_stock_comparison():
    """示例2：多股票比较分析"""
    print("\\n=== 示例2：多股票比较分析 ===")
    
    analyzer = TurnoverDecayAnalyzer()
    kline_data, stocks_info = create_realistic_market_data()
    
    # 准备多股票数据
    stock_data = {}
    for code in ['000001', '000858', '000002', '601318']:
        current_price = kline_data[code]['close'].iloc[-1]
        stock_data[code] = {
            'kline_data': kline_data[code],
            'current_price': current_price
        }
    
    # 比较多只股票
    comparison_result = analyzer.compare_multiple_stocks(stock_data, days=30)
    
    print("股票涨停适合度排名:")
    print("-" * 60)
    print(f"{'排名':<4} {'股票代码':<8} {'股票名称':<10} {'一板压力':<8} {'二板压力':<8} {'总体评估':<8} {'适合涨停':<8}")
    print("-" * 60)
    
    for i, (stock_code, result) in enumerate(comparison_result['sorted_by_suitability'], 1):
        pressure = result['pressure_assessment']
        stock_name = stocks_info[stock_code]['name']
        
        print(f"{i:<4} {stock_code:<8} {stock_name:<10} {pressure['1st_limit_pressure']:<8} "
              f"{pressure['2nd_limit_pressure']:<8} {pressure['overall_pressure']:<8} {pressure['suitable_for_limit_up']:<8}")
    
    return comparison_result

def example_parameter_sensitivity():
    """示例3：参数敏感性分析"""
    print("\\n=== 示例3：参数敏感性分析 ===")
    
    kline_data, stocks_info = create_realistic_market_data()
    stock_code = '000858'
    current_price = kline_data[stock_code]['close'].iloc[-1]
    
    print(f"分析股票: {stocks_info[stock_code]['name']}({stock_code})")
    print(f"当前价格: {current_price:.2f}")
    
    # 测试不同衰减因子
    print("\\n1. 不同衰减因子的影响:")
    print("-" * 50)
    print(f"{'衰减因子':<8} {'总筹码量':<12} {'一板筹码%':<10} {'二板筹码%':<10} {'适合涨停':<8}")
    print("-" * 50)
    
    decay_factors = [0.7, 0.8, 0.9, 0.95]
    for factor in decay_factors:
        analyzer = TurnoverDecayAnalyzer(decay_factor=factor)
        result = analyzer.analyze_chip_distribution(
            kline_data[stock_code], stock_code, current_price, 30
        )
        
        limit_up = result['limit_up_analysis']
        pressure = result['pressure_assessment']
        
        print(f"{factor:<8} {result['total_chips']:<12.4f} "
              f"{limit_up['1st_to_2nd']['chip_ratio_percent']:<10.2f} "
              f"{limit_up['2nd_to_3rd']['chip_ratio_percent']:<10.2f} "
              f"{pressure['suitable_for_limit_up']:<8}")
    
    # 测试不同价格分组数
    print("\\n2. 不同价格分组数的影响:")
    print("-" * 50)
    print(f"{'分组数':<8} {'筹码集中度':<12} {'计算时间(ms)':<12}")
    print("-" * 50)
    
    import time
    price_bins_list = [20, 40, 60, 80]
    
    for bins in price_bins_list:
        analyzer = TurnoverDecayAnalyzer(price_bins=bins)
        
        start_time = time.time()
        result = analyzer.analyze_chip_distribution(
            kline_data[stock_code], stock_code, current_price, 30
        )
        end_time = time.time()
        
        calc_time = (end_time - start_time) * 1000
        
        print(f"{bins:<8} {result['concentration_ratio']:<12.2%} {calc_time:<12.2f}")

def example_limit_up_strategy():
    """示例4：涨停板策略应用"""
    print("\\n=== 示例4：涨停板策略应用 ===")
    
    analyzer = TurnoverDecayAnalyzer()
    kline_data, stocks_info = create_realistic_market_data()
    
    print("涨停板选股策略:")
    print("选股条件: 一板压力 < 5% 且 二板压力 < 3%")
    print("-" * 70)
    print(f"{'股票代码':<8} {'股票名称':<10} {'当前价格':<10} {'一板压力%':<10} {'二板压力%':<10} {'符合条件':<8}")
    print("-" * 70)
    
    suitable_stocks = []
    
    for stock_code in kline_data.keys():
        current_price = kline_data[stock_code]['close'].iloc[-1]
        
        try:
            result = analyzer.analyze_chip_distribution(
                kline_data[stock_code], stock_code, current_price, 30
            )
            
            limit_up = result['limit_up_analysis']
            pressure_1st = limit_up['1st_to_2nd']['chip_ratio_percent']
            pressure_2nd = limit_up['2nd_to_3rd']['chip_ratio_percent']
            
            is_suitable = pressure_1st < 5 and pressure_2nd < 3
            
            print(f"{stock_code:<8} {stocks_info[stock_code]['name']:<10} "
                  f"{current_price:<10.2f} {pressure_1st:<10.2f} {pressure_2nd:<10.2f} {is_suitable:<8}")
            
            if is_suitable:
                suitable_stocks.append({
                    'code': stock_code,
                    'name': stocks_info[stock_code]['name'],
                    'price': current_price,
                    'pressure_1st': pressure_1st,
                    'pressure_2nd': pressure_2nd
                })
                
        except Exception as e:
            print(f"{stock_code:<8} {stocks_info[stock_code]['name']:<10} 分析失败: {e}")
    
    print("-" * 70)
    print(f"符合选股条件的股票数量: {len(suitable_stocks)}")
    
    if suitable_stocks:
        print("\\n推荐关注股票:")
        for stock in suitable_stocks:
            print(f"  {stock['name']}({stock['code']}) - 当前价: {stock['price']:.2f}")

def example_advanced_analysis():
    """示例5：高级分析功能"""
    print("\\n=== 示例5：高级分析功能 ===")
    
    # 创建自定义分析器
    analyzer = TurnoverDecayAnalyzer(
        decay_factor=0.85,
        price_bins=50,
        min_turnover_threshold=0.0005
    )
    
    kline_data, stocks_info = create_realistic_market_data()
    stock_code = '600519'  # 贵州茅台
    current_price = kline_data[stock_code]['close'].iloc[-1]
    
    # 详细分析
    result = analyzer.analyze_chip_distribution(
        kline_data[stock_code], stock_code, current_price, 45
    )
    
    print(f"高级分析 - {stocks_info[stock_code]['name']}({stock_code})")
    print(f"当前价格: {result['current_price']:.2f}")
    
    # 筹码分布可视化（文本形式）
    print("\\n筹码分布概览:")
    chip_distribution = result['chip_distribution']
    price_ranges = result['price_ranges']
    
    # 找到筹码最集中的5个价格区间
    top_indices = np.argsort(chip_distribution)[-5:][::-1]
    
    print("筹码最集中的价格区间:")
    for i, idx in enumerate(top_indices, 1):
        if idx < len(price_ranges) - 1:
            price_start = price_ranges[idx]
            price_end = price_ranges[idx + 1] if idx + 1 < len(price_ranges) else price_ranges[idx] * 1.02
            chip_ratio = chip_distribution[idx] / result['total_chips'] * 100
            
            print(f"  第{i}名: {price_start:.2f}-{price_end:.2f} ({chip_ratio:.2f}%)")
    
    # 剩余筹码分析
    print("\\n剩余筹码时间衰减分析:")
    remaining_chips = result['remaining_chips']
    print(f"初始筹码: {remaining_chips[0]:.4f}")
    print(f"最终筹码: {remaining_chips[-1]:.4f}")
    print(f"衰减比例: {(1 - remaining_chips[-1]/remaining_chips[0])*100:.2f}%")

def main():
    """主函数 - 运行所有示例"""
    print("动态衰减模型筹码分析使用示例")
    print("=" * 70)
    
    # 运行所有示例
    example_single_stock_analysis()
    example_multiple_stock_comparison()
    example_parameter_sensitivity()
    example_limit_up_strategy()
    example_advanced_analysis()
    
    print("\\n" + "=" * 70)
    print("所有示例运行完成！")
    print("\\n模型特点总结:")
    print("1. 基于换手率的时间衰减模型")
    print("2. 专门为涨停板分析设计")
    print("3. 支持多股票比较和参数调优")
    print("4. 提供涨停压力评估和选股策略")

if __name__ == "__main__":
    main()