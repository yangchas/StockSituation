"""
个股筹码区分布分析工具
基于K线数据计算筹码集中度、成本分布等指标

筹码区分布计算原理:
1. 将价格区间划分为多个档次
2. 统计每个价格区间的累计成交量
3. 计算筹码集中度、平均成本等指标
4. 识别主要筹码密集区
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from sina_kline_api import SinaKLineAPI


class ChipDistributionAnalyzer:
    """筹码区分布分析器"""
    
    def __init__(self, price_bins: int = 20, volume_weight: bool = True):
        """
        初始化分析器
        
        Args:
            price_bins: 价格区间数量
            volume_weight: 是否使用成交量加权
        """
        self.price_bins = price_bins
        self.volume_weight = volume_weight
        self.kline_api = SinaKLineAPI()
    
    def calculate_chip_distribution(self, 
                                  kline_data: List[Dict], 
                                  current_price: Optional[float] = None) -> Dict:
        """
        计算筹码区分布
        
        Args:
            kline_data: K线数据列表
            current_price: 当前价格（用于计算盈亏比例）
            
        Returns:
            Dict: 筹码分布分析结果
        """
        if not kline_data:
            return {}
        
        # 提取价格和成交量数据
        prices = []
        volumes = []
        
        for item in kline_data:
            # 使用成交均价（(高+低+收)/3）作为成本价格
            avg_price = (item['high'] + item['low'] + item['close']) / 3
            prices.append(avg_price)
            volumes.append(item['volume'])
        
        prices = np.array(prices)
        volumes = np.array(volumes)
        
        # 计算价格范围
        price_min = np.min(prices)
        price_max = np.max(prices)
        price_range = price_max - price_min
        
        # 划分价格区间
        bin_edges = np.linspace(price_min, price_max, self.price_bins + 1)
        
        # 计算每个区间的筹码量（成交量）
        if self.volume_weight:
            # 成交量加权
            chip_counts, _ = np.histogram(prices, bins=bin_edges, weights=volumes)
        else:
            # 简单计数
            chip_counts, _ = np.histogram(prices, bins=bin_edges)
        
        # 计算每个区间的价格中点
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
        
        # 计算总筹码量
        total_chips = np.sum(chip_counts)
        
        # 计算筹码分布比例
        chip_ratios = chip_counts / total_chips if total_chips > 0 else np.zeros_like(chip_counts)
        
        # 计算主要指标
        analysis_result = self._calculate_metrics(
            bin_centers, chip_counts, chip_ratios, current_price
        )
        
        # 构建详细结果
        result = {
            'price_bins': bin_centers.tolist(),
            'chip_counts': chip_counts.tolist(),
            'chip_ratios': chip_ratios.tolist(),
            'total_chips': total_chips,
            'price_range': {
                'min': float(price_min),
                'max': float(price_max),
                'range': float(price_range)
            },
            'analysis': analysis_result
        }
        
        return result
    
    def _calculate_metrics(self, 
                          bin_centers: np.ndarray, 
                          chip_counts: np.ndarray,
                          chip_ratios: np.ndarray,
                          current_price: Optional[float]) -> Dict:
        """计算筹码分布指标"""
        
        # 平均成本（处理权重为0的情况）
        if np.sum(chip_counts) > 0:
            avg_cost = np.average(bin_centers, weights=chip_counts)
        else:
            avg_cost = np.mean(bin_centers) if len(bin_centers) > 0 else 0
        
        # 筹码集中度（前30%筹码的价格区间）
        concentration = self._calculate_concentration(bin_centers, chip_ratios)
        
        # 主要密集区识别
        dense_areas = self._identify_dense_areas(bin_centers, chip_ratios)
        
        # 成本分布统计
        cost_distribution = self._calculate_cost_distribution(bin_centers, chip_ratios)
        
        # 盈亏分析（如果有当前价格）
        profit_loss = {}
        if current_price is not None:
            profit_loss = self._calculate_profit_loss(bin_centers, chip_ratios, current_price)
        
        return {
            'avg_cost': float(avg_cost),
            'concentration': concentration,
            'dense_areas': dense_areas,
            'cost_distribution': cost_distribution,
            'profit_loss': profit_loss
        }
    
    def _calculate_concentration(self, 
                                bin_centers: np.ndarray, 
                                chip_ratios: np.ndarray, 
                                threshold: float = 0.7) -> Dict:
        """计算筹码集中度"""
        # 按筹码比例排序
        sorted_indices = np.argsort(chip_ratios)[::-1]
        sorted_ratios = chip_ratios[sorted_indices]
        sorted_prices = bin_centers[sorted_indices]
        
        # 计算累计比例
        cumulative_ratio = np.cumsum(sorted_ratios)
        
        # 找到达到阈值的索引
        threshold_index = np.searchsorted(cumulative_ratio, threshold)
        
        if threshold_index < len(sorted_prices):
            concentration_prices = sorted_prices[:threshold_index + 1]
            concentration_range = {
                'min': float(np.min(concentration_prices)),
                'max': float(np.max(concentration_prices)),
                'range': float(np.max(concentration_prices) - np.min(concentration_prices))
            }
        else:
            concentration_range = {'min': 0, 'max': 0, 'range': 0}
        
        return {
            'threshold': threshold,
            'price_range': concentration_range,
            'ratio_at_threshold': float(cumulative_ratio[min(threshold_index, len(cumulative_ratio)-1)])
        }
    
    def _identify_dense_areas(self, 
                             bin_centers: np.ndarray, 
                             chip_ratios: np.ndarray, 
                             min_ratio: float = 0.05) -> List[Dict]:
        """识别主要筹码密集区"""
        dense_areas = []
        
        # 找到超过最小比例的区间
        dense_indices = np.where(chip_ratios >= min_ratio)[0]
        
        # 合并相邻的密集区间
        if len(dense_indices) > 0:
            current_area = {
                'start_index': dense_indices[0],
                'end_index': dense_indices[0],
                'total_ratio': chip_ratios[dense_indices[0]]
            }
            
            for i in range(1, len(dense_indices)):
                if dense_indices[i] == dense_indices[i-1] + 1:
                    # 相邻区间，合并
                    current_area['end_index'] = dense_indices[i]
                    current_area['total_ratio'] += chip_ratios[dense_indices[i]]
                else:
                    # 新区间开始
                    dense_areas.append(current_area)
                    current_area = {
                        'start_index': dense_indices[i],
                        'end_index': dense_indices[i],
                        'total_ratio': chip_ratios[dense_indices[i]]
                    }
            
            dense_areas.append(current_area)
        
        # 转换为价格信息
        result_areas = []
        for area in dense_areas:
            start_price = bin_centers[area['start_index']]
            end_price = bin_centers[area['end_index']]
            
            result_areas.append({
                'price_range': {
                    'start': float(start_price),
                    'end': float(end_price),
                    'width': float(end_price - start_price)
                },
                'chip_ratio': float(area['total_ratio']),
                'chip_percent': float(area['total_ratio'] * 100)
            })
        
        # 按筹码比例排序
        result_areas.sort(key=lambda x: x['chip_ratio'], reverse=True)
        
        return result_areas
    
    def _calculate_cost_distribution(self, 
                                   bin_centers: np.ndarray, 
                                   chip_ratios: np.ndarray) -> Dict:
        """计算成本分布统计"""
        # 计算分位数
        cumulative_ratio = np.cumsum(chip_ratios)
        
        def find_quantile(q: float) -> float:
            """找到指定分位数的价格"""
            idx = np.searchsorted(cumulative_ratio, q)
            if idx < len(bin_centers):
                return float(bin_centers[idx])
            return float(bin_centers[-1])
        
        return {
            'q25': find_quantile(0.25),  # 25%分位数
            'q50': find_quantile(0.50),  # 中位数
            'q75': find_quantile(0.75),  # 75%分位数
            'q90': find_quantile(0.90)   # 90%分位数
        }
    
    def _calculate_profit_loss(self, 
                              bin_centers: np.ndarray, 
                              chip_ratios: np.ndarray, 
                              current_price: float) -> Dict:
        """计算盈亏分析"""
        # 计算盈利和亏损的筹码比例
        profit_ratio = np.sum(chip_ratios[bin_centers < current_price])
        loss_ratio = np.sum(chip_ratios[bin_centers > current_price])
        break_even_ratio = np.sum(chip_ratios[bin_centers == current_price])
        
        # 计算平均盈亏幅度
        profit_prices = bin_centers[bin_centers < current_price]
        profit_ratios = chip_ratios[bin_centers < current_price]
        
        if len(profit_prices) > 0 and np.sum(profit_ratios) > 0:
            avg_profit_pct = np.average((current_price - profit_prices) / profit_prices * 100, 
                                      weights=profit_ratios)
        else:
            avg_profit_pct = 0
        
        loss_prices = bin_centers[bin_centers > current_price]
        loss_ratios = chip_ratios[bin_centers > current_price]
        
        if len(loss_prices) > 0 and np.sum(loss_ratios) > 0:
            avg_loss_pct = np.average((loss_prices - current_price) / current_price * 100, 
                                    weights=loss_ratios)
        else:
            avg_loss_pct = 0
        
        return {
            'profit_ratio': float(profit_ratio),
            'loss_ratio': float(loss_ratio),
            'break_even_ratio': float(break_even_ratio),
            'avg_profit_pct': float(avg_profit_pct),
            'avg_loss_pct': float(avg_loss_pct)
        }
    
    def analyze_stock(self, 
                     stock_code: str, 
                     scale: str = 'day',
                     datalen: int = 100,
                     current_price: Optional[float] = None) -> Dict:
        """
        分析单只股票的筹码分布
        
        Args:
            stock_code: 股票代码
            scale: K线周期
            datalen: 数据条数
            current_price: 当前价格
            
        Returns:
            Dict: 完整的筹码分析结果
        """
        # 获取K线数据
        kline_data = self.kline_api.get_kline_data(stock_code, scale, 5, datalen)
        
        # 如果没有提供当前价格，使用最新收盘价
        if current_price is None and kline_data:
            current_price = kline_data[0]['close']
        
        # 计算筹码分布
        chip_distribution = self.calculate_chip_distribution(kline_data, current_price)
        
        # 添加基本信息
        result = {
            'stock_code': stock_code,
            'scale': scale,
            'datalen': datalen,
            'current_price': current_price,
            'data_period': {
                'start': kline_data[-1]['datetime'] if kline_data else None,
                'end': kline_data[0]['datetime'] if kline_data else None,
                'data_points': len(kline_data)
            },
            'chip_distribution': chip_distribution
        }
        
        return result
    
    def compare_multiple_stocks(self, 
                               stock_codes: List[str], 
                               scale: str = 'day',
                               datalen: int = 100) -> Dict:
        """
        比较多只股票的筹码分布
        
        Args:
            stock_codes: 股票代码列表
            scale: K线周期
            datalen: 数据条数
            
        Returns:
            Dict: 多股票比较结果
        """
        results = {}
        
        for stock_code in stock_codes:
            try:
                analysis = self.analyze_stock(stock_code, scale, datalen)
                results[stock_code] = analysis
            except Exception as e:
                print(f"分析股票 {stock_code} 失败: {e}")
                results[stock_code] = {'error': str(e)}
        
        # 计算比较指标
        comparison = self._calculate_comparison(results)
        
        return {
            'individual_results': results,
            'comparison': comparison
        }
    
    def _calculate_comparison(self, results: Dict) -> Dict:
        """计算多股票比较指标"""
        valid_results = {k: v for k, v in results.items() if 'error' not in v}
        
        if not valid_results:
            return {}
        
        comparison = {}
        
        # 平均成本比较
        avg_costs = {}
        for stock, result in valid_results.items():
            if 'chip_distribution' in result and 'analysis' in result['chip_distribution']:
                avg_costs[stock] = result['chip_distribution']['analysis']['avg_cost']
        
        if avg_costs:
            comparison['avg_cost'] = {
                'min_stock': min(avg_costs, key=avg_costs.get),
                'max_stock': max(avg_costs, key=avg_costs.get),
                'values': avg_costs
            }
        
        # 筹码集中度比较
        concentrations = {}
        for stock, result in valid_results.items():
            if 'chip_distribution' in result and 'analysis' in result['chip_distribution']:
                conc = result['chip_distribution']['analysis']['concentration']
                concentrations[stock] = conc['price_range']['range']
        
        if concentrations:
            comparison['concentration'] = {
                'most_concentrated': min(concentrations, key=concentrations.get),
                'least_concentrated': max(concentrations, key=concentrations.get),
                'values': concentrations
            }
        
        return comparison


def format_chip_analysis(result: Dict) -> str:
    """格式化筹码分析结果"""
    if 'error' in result:
        return f"错误: {result['error']}"
    
    output = []
    output.append(f"=== {result['stock_code']} 筹码分布分析 ===")
    output.append(f"分析周期: {result['data_period']['start']} 到 {result['data_period']['end']}")
    output.append(f"当前价格: {result['current_price']}")
    
    chip_data = result['chip_distribution']
    analysis = chip_data['analysis']
    
    output.append(f"\n📊 基本指标:")
    output.append(f"平均成本: {analysis['avg_cost']:.2f}")
    output.append(f"价格区间: {chip_data['price_range']['min']:.2f} - {chip_data['price_range']['max']:.2f}")
    
    output.append(f"\n🎯 筹码集中度:")
    conc = analysis['concentration']
    output.append(f"70%筹码集中在: {conc['price_range']['min']:.2f} - {conc['price_range']['max']:.2f}")
    output.append(f"集中区间宽度: {conc['price_range']['range']:.2f}")
    
    output.append(f"\n📈 主要密集区:")
    for i, area in enumerate(analysis['dense_areas'][:3], 1):
        output.append(f"密集区{i}: {area['price_range']['start']:.2f}-{area['price_range']['end']:.2f} "
                     f"({area['chip_percent']:.1f}%筹码)")
    
    if analysis['profit_loss']:
        pl = analysis['profit_loss']
        output.append(f"\n💰 盈亏分析:")
        output.append(f"盈利筹码: {pl['profit_ratio']*100:.1f}%")
        output.append(f"亏损筹码: {pl['loss_ratio']*100:.1f}%")
        output.append(f"平均盈利: {pl['avg_profit_pct']:.1f}%")
        output.append(f"平均亏损: {pl['avg_loss_pct']:.1f}%")
    
    return '\n'.join(output)


def main():
    """使用示例"""
    analyzer = ChipDistributionAnalyzer(price_bins=20)
    
    # 示例1: 分析单只股票
    print("=== 示例1: 分析平安银行筹码分布 ===")
    result = analyzer.analyze_stock('000001', 'day', 100)
    print(format_chip_analysis(result))
    
    # 示例2: 比较多只股票
    print("\n\n=== 示例2: 比较多只股票筹码分布 ===")
    stocks = ['000001', '600519', '300063']
    comparison = analyzer.compare_multiple_stocks(stocks, 'day', 50)
    
    for stock in stocks:
        if stock in comparison['individual_results']:
            print(f"\n--- {stock} ---")
            print(format_chip_analysis(comparison['individual_results'][stock]))


if __name__ == "__main__":
    main()