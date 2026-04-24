"""
基于动态衰减模型的筹码分布分析工具
核心逻辑：通过换手率衰减计算剩余筹码比例

模型特点：
1. 使用5分钟K线数据，按日汇总
2. 基于换手率计算每日剩余筹码
3. 支持涨停板区间分析
4. 提供筹码压力评估
"""

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging

class TurnoverDecayAnalyzer:
    """基于换手率衰减的筹码分布分析器"""
    
    def __init__(self, 
                 market_cap_data: Optional[Dict] = None,
                 decay_factor: float = 0.9,
                 price_bins: int = 50,
                 min_turnover_threshold: float = 0.001):
        """
        初始化分析器
        
        Args:
            market_cap_data: 股票流通市值数据 {股票代码: 流通市值}
            decay_factor: 衰减因子，控制筹码衰减速度
            price_bins: 价格分组数量
            min_turnover_threshold: 最小换手率阈值
        """
        self.market_cap_data = market_cap_data or {}
        self.decay_factor = decay_factor
        self.price_bins = price_bins
        self.min_turnover_threshold = min_turnover_threshold
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """设置日志"""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def process_5min_kline_data(self, kline_data: pd.DataFrame) -> pd.DataFrame:
        """
        处理5分钟K线数据，计算每日换手率
        
        Args:
            kline_data: 包含['timestamp', 'close', 'amount']的DataFrame
            
        Returns:
            按日汇总的换手率数据
        """
        try:
            # 确保时间戳格式正确
            kline_data['date'] = pd.to_datetime(kline_data['timestamp']).dt.date
            
            # 按日汇总成交额
            daily_data = kline_data.groupby('date').agg({
                'close': 'last',
                'amount': 'sum'
            }).reset_index()
            
            # 计算换手率
            daily_data['turnover_rate'] = self._calculate_turnover_rate(
                daily_data['amount'], daily_data['close']
            )
            
            return daily_data
            
        except Exception as e:
            self.logger.error(f"处理5分钟K线数据失败: {e}")
            raise
    
    def _calculate_turnover_rate(self, 
                                daily_amount: pd.Series, 
                                close_price: pd.Series) -> pd.Series:
        """
        计算日换手率
        
        Args:
            daily_amount: 日成交额
            close_price: 收盘价
            
        Returns:
            换手率序列
        """
        # 这里需要流通市值数据，如果没有则使用默认值
        # 实际应用中应该从外部获取准确的流通市值
        default_market_cap = 1e10  # 默认100亿流通市值
        
        turnover_rates = []
        for i, (amount, close) in enumerate(zip(daily_amount, close_price)):
            # 计算换手率：成交额 / (收盘价 * 流通股本)
            # 这里简化处理，实际应该使用准确的流通市值
            market_cap = self.market_cap_data.get(str(i), default_market_cap)
            turnover_rate = amount / market_cap if market_cap > 0 else 0
            turnover_rates.append(turnover_rate)
        
        return pd.Series(turnover_rates)
    
    def calculate_remaining_chips(self, 
                                 turnover_rates: pd.Series, 
                                 days: int = 60) -> np.ndarray:
        """
        计算剩余筹码比例
        
        Args:
            turnover_rates: 换手率序列
            days: 计算天数
            
        Returns:
            剩余筹码比例数组
        """
        n = len(turnover_rates)
        if n < days:
            self.logger.warning(f"数据天数({n})少于要求天数({days})")
            days = n
        
        # 取最近days天的数据
        recent_turnover = turnover_rates.iloc[-days:].values
        
        # 计算剩余筹码比例
        remaining_chips = np.ones(days)
        
        for i in range(1, days):
            # 当日换手率 × 历史剩余筹码乘积 × 衰减因子
            remain_his = recent_turnover[i] * np.prod(remaining_chips[:i]) * self.decay_factor
            remaining_chips[i] = max(0, min(1, remain_his))
        
        return remaining_chips
    
    def analyze_chip_distribution(self, 
                                 kline_data: pd.DataFrame,
                                 stock_code: str,
                                 current_price: float,
                                 days: int = 60) -> Dict:
        """
        分析筹码分布
        
        Args:
            kline_data: 5分钟K线数据
            stock_code: 股票代码
            current_price: 当前价格
            days: 分析天数
            
        Returns:
            筹码分布分析结果
        """
        try:
            # 处理K线数据
            daily_data = self.process_5min_kline_data(kline_data)
            
            # 计算剩余筹码
            remaining_chips = self.calculate_remaining_chips(
                daily_data['turnover_rate'], days
            )
            
            # 价格分组统计
            price_ranges, chip_distribution = self._group_by_price(
                daily_data, remaining_chips, current_price
            )
            
            # 涨停板区间分析
            limit_up_analysis = self._analyze_limit_up_intervals(
                price_ranges, chip_distribution, current_price
            )
            
            # 筹码压力评估
            pressure_assessment = self._assess_chip_pressure(limit_up_analysis)
            
            return {
                'stock_code': stock_code,
                'current_price': current_price,
                'analysis_days': days,
                'price_ranges': price_ranges,
                'chip_distribution': chip_distribution,
                'remaining_chips': remaining_chips.tolist(),
                'limit_up_analysis': limit_up_analysis,
                'pressure_assessment': pressure_assessment,
                'total_chips': np.sum(chip_distribution),
                'concentration_ratio': self._calculate_concentration_ratio(chip_distribution)
            }
            
        except Exception as e:
            self.logger.error(f"分析筹码分布失败: {e}")
            raise
    
    def _group_by_price(self, 
                       daily_data: pd.DataFrame,
                       remaining_chips: np.ndarray,
                       current_price: float) -> Tuple[List, np.ndarray]:
        """按价格分组统计筹码量"""
        # 确定价格范围
        min_price = daily_data['close'].min()
        max_price = daily_data['close'].max()
        price_range = max(max_price - min_price, current_price * 0.1)  # 确保有足够范围
        
        # 创建价格区间
        price_bins = np.linspace(min_price, max_price, self.price_bins)
        
        # 统计每个价格区间的筹码量
        chip_counts = np.zeros(self.price_bins)
        
        for i, (_, row) in enumerate(daily_data.iterrows()):
            if i < len(remaining_chips):
                price = row['close']
                chip_weight = remaining_chips[i]
                
                # 找到对应的价格区间
                bin_idx = np.digitize(price, price_bins) - 1
                if 0 <= bin_idx < self.price_bins:
                    chip_counts[bin_idx] += chip_weight
        
        return price_bins.tolist(), chip_counts
    
    def _analyze_limit_up_intervals(self,
                                   price_ranges: List[float],
                                   chip_distribution: np.ndarray,
                                   current_price: float) -> Dict:
        """分析涨停板区间筹码分布"""
        # 计算涨停板价格区间
        limit_up_1 = current_price * 1.1      # 一板
        limit_up_2 = current_price * 1.21     # 二板
        limit_up_3 = current_price * 1.331    # 三板
        
        # 计算各区间筹码比例
        intervals = {
            'current_to_1st': (current_price, limit_up_1),
            '1st_to_2nd': (limit_up_1, limit_up_2),
            '2nd_to_3rd': (limit_up_2, limit_up_3)
        }
        
        interval_chips = {}
        total_chips = np.sum(chip_distribution)
        
        for interval_name, (lower, upper) in intervals.items():
            # 找到在区间内的价格分组
            mask = (np.array(price_ranges) >= lower) & (np.array(price_ranges) <= upper)
            interval_chip = np.sum(chip_distribution[mask])
            interval_ratio = interval_chip / total_chips if total_chips > 0 else 0
            
            interval_chips[interval_name] = {
                'lower_bound': lower,
                'upper_bound': upper,
                'chip_amount': interval_chip,
                'chip_ratio': interval_ratio,
                'chip_ratio_percent': interval_ratio * 100
            }
        
        return interval_chips
    
    def _assess_chip_pressure(self, limit_up_analysis: Dict) -> Dict:
        """评估筹码压力"""
        pressure_1st = limit_up_analysis['1st_to_2nd']['chip_ratio_percent']
        pressure_2nd = limit_up_analysis['2nd_to_3rd']['chip_ratio_percent']
        
        # 压力评估标准
        assessment = {
            '1st_limit_pressure': '低' if pressure_1st < 5 else '中' if pressure_1st < 10 else '高',
            '2nd_limit_pressure': '低' if pressure_2nd < 3 else '中' if pressure_2nd < 7 else '高',
            'overall_pressure': '低' if pressure_1st < 5 and pressure_2nd < 3 else '中' if pressure_1st < 10 and pressure_2nd < 7 else '高',
            'suitable_for_limit_up': pressure_1st < 5 and pressure_2nd < 3
        }
        
        return assessment
    
    def _calculate_concentration_ratio(self, chip_distribution: np.ndarray) -> float:
        """计算筹码集中度"""
        if len(chip_distribution) == 0:
            return 0
        
        total_chips = np.sum(chip_distribution)
        if total_chips == 0:
            return 0
        
        # 计算前30%价格区间的筹码占比
        sorted_indices = np.argsort(chip_distribution)[::-1]
        top_30_count = int(len(chip_distribution) * 0.3)
        top_30_chips = np.sum(chip_distribution[sorted_indices[:top_30_count]])
        
        return top_30_chips / total_chips
    
    def compare_multiple_stocks(self, 
                               stock_data: Dict[str, Dict],
                               days: int = 60) -> Dict:
        """
        比较多只股票的筹码分布
        
        Args:
            stock_data: {股票代码: {'kline_data': DataFrame, 'current_price': float}}
            days: 分析天数
            
        Returns:
            多股票比较结果
        """
        comparison_results = {}
        
        for stock_code, data in stock_data.items():
            try:
                result = self.analyze_chip_distribution(
                    data['kline_data'], stock_code, data['current_price'], days
                )
                comparison_results[stock_code] = result
            except Exception as e:
                self.logger.error(f"分析股票{stock_code}失败: {e}")
                comparison_results[stock_code] = {'error': str(e)}
        
        # 排序：按涨停板适合度排序
        sorted_stocks = sorted(
            [(code, result) for code, result in comparison_results.items() 
             if 'error' not in result],
            key=lambda x: x[1]['pressure_assessment']['suitable_for_limit_up'],
            reverse=True
        )
        
        return {
            'comparison_results': comparison_results,
            'sorted_by_suitability': sorted_stocks,
            'total_stocks_analyzed': len(comparison_results)
        }


def create_sample_data() -> pd.DataFrame:
    """创建示例5分钟K线数据"""
    dates = pd.date_range('2024-01-01', '2024-03-01', freq='5min')
    
    # 模拟价格数据（随机游走）
    np.random.seed(42)
    prices = [10.0]
    for _ in range(len(dates) - 1):
        change = np.random.normal(0, 0.01)
        new_price = max(0.1, prices[-1] * (1 + change))
        prices.append(new_price)
    
    # 模拟成交额数据
    amounts = np.random.uniform(1000000, 5000000, len(dates))
    
    return pd.DataFrame({
        'timestamp': dates,
        'close': prices,
        'amount': amounts
    })


if __name__ == "__main__":
    # 使用示例
    analyzer = TurnoverDecayAnalyzer()
    
    # 创建示例数据
    sample_data = create_sample_data()
    
    # 分析单只股票
    result = analyzer.analyze_chip_distribution(
        sample_data, '000001', current_price=11.5, days=30
    )
    
    print("=== 动态衰减模型筹码分析结果 ===")
    print(f"股票代码: {result['stock_code']}")
    print(f"当前价格: {result['current_price']}")
    print(f"总筹码量: {result['total_chips']:.4f}")
    print(f"筹码集中度: {result['concentration_ratio']:.2%}")
    
    print("\\n=== 涨停板区间分析 ===")
    for interval, data in result['limit_up_analysis'].items():
        print(f"{interval}: {data['chip_ratio_percent']:.2f}%")
    
    print("\\n=== 压力评估 ===")
    for key, value in result['pressure_assessment'].items():
        print(f"{key}: {value}")