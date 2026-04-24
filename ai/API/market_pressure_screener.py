"""
市场压力位筛选器 - 对市场个股进行压力位分析筛选
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from typing import List, Dict, Any
import sys
import os

# 添加上级目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from StockBreakoutAnalyzer import StockBreakoutAnalyzer


class MarketPressureScreener:
    """
    市场压力位筛选器
    """
    
    def __init__(self, batch_size: int = 10, delay: float = 0.5):
        """
        初始化筛选器
        
        Args:
            batch_size: 批量处理股票数量
            delay: 请求间隔时间（秒）
        """
        self.batch_size = batch_size
        self.delay = delay
        self.analyzer_cache = {}
    
    def get_stock_list(self, market_type: str = "A股") -> List[str]:
        """
        获取股票列表
        
        Args:
            market_type: 市场类型（A股、创业板、科创板等）
            
        Returns:
            股票代码列表
        """
        # 这里可以扩展为从不同数据源获取股票列表
        # 目前使用示例股票列表
        
        if market_type == "A股":
            # 示例A股股票列表
            return [
                "000001", "000002", "000858", "600036", "601318",
                "600519", "601888", "300059", "300433", "300750"
            ]
        elif market_type == "创业板":
            return ["300433", "300059", "300750", "300014", "300015"]
        elif market_type == "科创板":
            return ["688001", "688008", "688009", "688019", "688029"]
        else:
            return ["000001", "000002", "000858", "600036", "300433"]
    
    def analyze_single_stock(self, stock_code: str, days: int = 100) -> Dict[str, Any]:
        """
        分析单只股票的压力位情况
        
        Args:
            stock_code: 股票代码
            days: 分析周期
            
        Returns:
            分析结果字典
        """
        try:
            # 获取股票数据
            from test_stock_breakout_analyzer import get_real_stock_data
            data = get_real_stock_data(stock_code, days)
            
            if data is None or len(data) < 20:
                return {
                    'stock_code': stock_code,
                    'success': False,
                    'error': '数据获取失败或数据量不足'
                }
            
            # 创建分析器
            analyzer = StockBreakoutAnalyzer(data)
            
            # 运行针对性分析
            result = analyzer.run_targeted_analysis(stock_code)
            
            if not result:
                return {
                    'stock_code': stock_code,
                    'success': False,
                    'error': '分析失败'
                }
            
            # 提取关键信息
            pattern_info = result['progressive_pattern']
            
            return {
                'stock_code': stock_code,
                'success': True,
                'current_price': result['current_price'],
                'pattern_detected': pattern_info['pattern_detected'],
                'pattern_score': pattern_info['pattern_score'],
                'pattern_rating': pattern_info['pattern_rating'],
                'pressure_clusters': pattern_info.get('pressure_clusters', []),
                'breakout_signals': len(analyzer.detect_breakouts()),
                'analysis_date': result['current_date']
            }
            
        except Exception as e:
            return {
                'stock_code': stock_code,
                'success': False,
                'error': str(e)
            }
    
    def screen_stocks_by_pressure(self, stock_codes: List[str], 
                                 min_pattern_score: int = 60,
                                 max_pressure_distance: float = 10.0,
                                 require_breakout: bool = True) -> List[Dict[str, Any]]:
        """
        根据压力位条件筛选股票
        
        Args:
            stock_codes: 股票代码列表
            min_pattern_score: 最小形态评分
            max_pressure_distance: 最大压力位距离（%）
            require_breakout: 是否需要突破信号
            
        Returns:
            筛选结果列表
        """
        results = []
        
        print(f"🎯 开始筛选 {len(stock_codes)} 只股票...")
        print("=" * 80)
        
        for i, stock_code in enumerate(stock_codes, 1):
            print(f"\n📊 分析第 {i}/{len(stock_codes)} 只股票: {stock_code}")
            
            # 分析单只股票
            result = self.analyze_single_stock(stock_code)
            
            if not result['success']:
                print(f"   ❌ 分析失败: {result.get('error', '未知错误')}")
                continue
            
            # 检查筛选条件
            meets_criteria = True
            reasons = []
            
            # 条件1: 形态评分
            if result['pattern_score'] < min_pattern_score:
                meets_criteria = False
                reasons.append(f"形态评分不足 ({result['pattern_score']} < {min_pattern_score})")
            
            # 条件2: 突破信号
            if require_breakout and result['breakout_signals'] == 0:
                meets_criteria = False
                reasons.append("无突破信号")
            
            # 条件3: 压力位距离
            if result['pressure_clusters']:
                # 计算最近压力位的距离
                current_price = result['current_price']
                all_pressure_prices = []
                
                for cluster in result['pressure_clusters']:
                    for pressure_point in cluster:
                        all_pressure_prices.append(pressure_point['price'])
                
                if all_pressure_prices:
                    closest_pressure = min(all_pressure_prices, key=lambda x: abs(x - current_price))
                    pressure_distance = (closest_pressure - current_price) / current_price * 100
                    
                    if pressure_distance > max_pressure_distance:
                        meets_criteria = False
                        reasons.append(f"压力位距离过远 ({pressure_distance:.1f}% > {max_pressure_distance}%)")
                    
                    result['closest_pressure'] = closest_pressure
                    result['pressure_distance'] = pressure_distance
            
            # 记录结果
            result['meets_criteria'] = meets_criteria
            result['rejection_reasons'] = reasons if not meets_criteria else []
            
            if meets_criteria:
                print(f"   ✅ 符合筛选条件")
                print(f"      形态评分: {result['pattern_score']}/100")
                print(f"      突破信号: {result['breakout_signals']} 个")
                if 'pressure_distance' in result:
                    print(f"      最近压力位: {result['closest_pressure']:.2f} (距离: {result['pressure_distance']:.1f}%)")
                results.append(result)
            else:
                print(f"   ❌ 不符合条件: {', '.join(reasons)}")
            
            # 添加延迟避免请求过快
            if i < len(stock_codes):
                time.sleep(self.delay)
        
        return results
    
    def generate_screening_report(self, results: List[Dict[str, Any]]) -> str:
        """
        生成筛选报告
        
        Args:
            results: 筛选结果列表
            
        Returns:
            报告字符串
        """
        if not results:
            return "❌ 未找到符合筛选条件的股票"
        
        report = "=" * 80 + "\n"
        report += "🎯 市场压力位筛选报告\n"
        report += "=" * 80 + "\n\n"
        
        report += f"📊 筛选结果统计:\n"
        report += f"   分析股票数量: {len([r for r in results if r['success']])}\n"
        report += f"   符合条件股票: {len([r for r in results if r.get('meets_criteria', False)])}\n"
        report += f"   筛选成功率: {len([r for r in results if r.get('meets_criteria', False)]) / len([r for r in results if r['success']]) * 100:.1f}%\n\n"
        
        # 符合条件的股票详情
        qualified_stocks = [r for r in results if r.get('meets_criteria', False)]
        
        if qualified_stocks:
            report += "🏆 符合条件的股票:\n"
            report += "-" * 60 + "\n"
            
            # 按形态评分排序
            qualified_stocks.sort(key=lambda x: x['pattern_score'], reverse=True)
            
            for i, stock in enumerate(qualified_stocks, 1):
                report += f"{i}. {stock['stock_code']}\n"
                report += f"   当前价格: {stock['current_price']:.2f} 元\n"
                report += f"   形态评分: {stock['pattern_score']}/100 ({stock['pattern_rating']})\n"
                report += f"   突破信号: {stock['breakout_signals']} 个\n"
                if 'pressure_distance' in stock:
                    report += f"   压力距离: {stock['pressure_distance']:.1f}% (最近: {stock['closest_pressure']:.2f})\n"
                
                # 压力集群信息
                if stock['pressure_clusters']:
                    report += f"   压力集群: {len(stock['pressure_clusters'])} 个\n"
                    for j, cluster in enumerate(stock['pressure_clusters'][:2], 1):
                        prices = [p['price'] for p in cluster]
                        report += f"      集群{j}: {min(prices):.2f} - {max(prices):.2f} 元\n"
                report += "\n"
        
        # 分析建议
        report += "💡 投资建议:\n"
        report += "-" * 60 + "\n"
        
        if qualified_stocks:
            report += "✅ 建议重点关注以下股票:\n"
            for stock in qualified_stocks[:3]:  # 只显示前3只
                report += f"   • {stock['stock_code']}: "
                if stock['pattern_score'] >= 80:
                    report += "强势突破形态，可积极关注\n"
                elif stock['pattern_score'] >= 60:
                    report += "良好突破形态，可适度关注\n"
                else:
                    report += "突破形态初步形成，需谨慎观察\n"
        else:
            report += "⚠️  当前市场无明显突破机会，建议观望或降低筛选标准\n"
        
        report += "\n" + "=" * 80
        
        return report


def run_market_screening():
    """
    运行市场筛选示例
    """
    print("🚀 启动市场压力位筛选器")
    print("=" * 80)
    
    # 创建筛选器
    screener = MarketPressureScreener(batch_size=5, delay=1.0)
    
    # 获取股票列表
    stock_codes = screener.get_stock_list("A股")
    print(f"📋 获取到 {len(stock_codes)} 只股票进行分析")
    
    # 设置筛选条件
    min_score = 60  # 最小形态评分
    max_distance = 15.0  # 最大压力位距离
    require_breakout = True  # 需要突破信号
    
    print(f"🎯 筛选条件:")
    print(f"   最小形态评分: {min_score}")
    print(f"   最大压力位距离: {max_distance}%")
    print(f"   需要突破信号: {'是' if require_breakout else '否'}")
    print()
    
    # 执行筛选
    results = screener.screen_stocks_by_pressure(
        stock_codes=stock_codes,
        min_pattern_score=min_score,
        max_pressure_distance=max_distance,
        require_breakout=require_breakout
    )
    
    # 生成报告
    report = screener.generate_screening_report(results)
    
    print("\n" + "=" * 80)
    print("📋 筛选报告")
    print("=" * 80)
    print(report)
    
    return results, report


if __name__ == "__main__":
    try:
        results, report = run_market_screening()
        print("\n✅ 市场筛选完成!")
    except Exception as e:
        print(f"❌ 筛选过程中出现错误: {e}")
        import traceback
        traceback.print_exc()