#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强版通用突破形态识别系统 - 修正版V5
修复压力位检测问题，确保能正确识别集群压力位，并改进筹码区计算，捕捉放量高点，替换MA为BOLL支撑，添加长阳收盘支撑，添加横盘区检测
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import warnings
from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from scipy import stats

warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.figsize'] = [12, 8]
plt.rcParams['figure.dpi'] = 100
plt.rcParams['font.size'] = 10
class StockBreakoutAnalyzer:
    """
    修正版突破形态检测器 V5 - 改进压力检测捕捉放量高点，替换MA为BOLL支撑，添加长阳收盘支撑，添加横盘区检测
    """
    
    def __init__(self, data: pd.DataFrame):
        self.data = self._prepare_data(data.copy())
        self.pressure_levels = []
        self.pressure_clusters = []
        self.breakout_patterns = []
        self.pattern_evaluation = {}
        self.support_levels = []
        
        print(f"✅ 修正版检测器初始化完成")
        print(f"   数据周期: {len(self.data)} 天")
        print(f"   最新价格: {self.data['close'].iloc[-1]:.2f}")
        print(f"   最新高点: {self.data['high'].iloc[-1]:.2f}")
        print(f"   价格范围: {self.data['low'].min():.2f} - {self.data['high'].max():.2f}")
    
    def _prepare_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """准备数据 - 修复非交易日空白问题"""
        required_columns = ['open', 'high', 'low', 'close']
        for col in required_columns:
            if col not in data.columns:
                raise ValueError(f"数据缺少必要列: {col}")
        
        if 'turnover' not in data.columns and 'volume' in data.columns:
            data['turnover'] = data['volume'] * data['close']
        elif 'turnover' not in data.columns:
            data['turnover'] = 1000000
        
        # 确保索引是连续的日期（填充非交易日）
        if not pd.api.types.is_datetime64_any_dtype(data.index):
            try:
                data.index = pd.to_datetime(data.index)
            except:
                # 修复时间戳运算兼容性问题
                end_date = pd.Timestamp.now()
                start_date = end_date - pd.Timedelta(days=len(data)-1)
                data.index = pd.date_range(start=start_date, end=end_date, freq='D')
        
        data = data.sort_index()
        
        # 填充非交易日 - 使用前一天的收盘价填充
        date_range = pd.date_range(start=data.index[0], end=data.index[-1], freq='B')
        data = data.reindex(date_range)
        
        # 向前填充缺失的OHLC数据
        for col in ['open', 'high', 'low', 'close']:
            data[col] = data[col].fillna(method='ffill')
        
        # 成交量数据特殊处理，非交易日成交量为0
        data['turnover'] = data['turnover'].fillna(0)
        
        # 重置索引名称为日期
        data.index.name = 'date'
        
        return data
    
    def detect_pressure_levels_v5(self, 
                                current_price: float = None,
                                lookback_days: int = 60,
                                decay_rate: float = 0.01) -> List[Dict]:
        """
        修正版压力位检测 V5 - 只检测上方压力位，改进筹码区为动态衰减加权分布，捕捉放量高点，强制捕捉全局最高点
        """
        print("🔍 修正版压力位检测V5...")
        
        if current_price is None:
            current_price = self.data['close'].iloc[-1]
        
        data_len = len(self.data)
        
        # 方法1：使用滚动窗口检测上方高点，优先放量
        all_highs = []
        
        # 全局最高点
        max_high_idx = self.data['high'].tail(lookback_days).idxmax()
        max_high_price = self.data['high'].loc[max_high_idx]
        if max_high_price > current_price:
            # 修复时间戳运算兼容性问题
            idxmax_position = self.data.tail(lookback_days)['high'].idxmax()
            i = data_len - lookback_days + self.data.index.get_loc(idxmax_position)  # 绝对索引
            # 修复索引越界问题
            if i < 0 or i >= data_len:
                # 跳过无效索引，使用默认值
                volume_ratio = 1
            else:
                start_idx = max(0, i-10)
                if start_idx >= i:  # 确保切片有效
                    avg_volume = self.data['turnover'].iloc[i]  # 如果无法计算平均值，使用当前值
                else:
                    avg_volume = self.data['turnover'].iloc[start_idx:i].mean()
                volume_ratio = self.data['turnover'].iloc[i] / avg_volume if avg_volume > 0 else 1
            test_count = sum(1 for j in range(max(0, i-30), min(data_len, i+30)) if abs(self.data['high'].iloc[j] - max_high_price) / max_high_price < 0.02)
            if volume_ratio > 1.5:
                test_count += 2  # 放量加分
            else:
                test_count += 1
            distance_pct = (max_high_price - current_price) / current_price * 100
            category = 'near' if distance_pct < 10 else 'far'
            all_highs.append({
                'date': max_high_idx,
                'price': max_high_price,
                'close': self.data['close'].loc[max_high_idx],
                'volume_ratio': volume_ratio,
                'test_count': test_count,
                'days_ago': data_len - 1 - i,
                'distance_to_current_pct': distance_pct,
                'window': 'global_max',
                'category': category
            })
        
        # 使用更宽的窗口来捕捉所有潜在阻力位
        for window in [5, 10, 15, 20]:
            for i in range(window, min(data_len, lookback_days) - window):
                current_high_price = self.data['high'].iloc[i]
                if current_high_price <= current_price:  # 只检测上方
                    continue
                
                current_date = self.data.index[i]
                
                # 检查是否是窗口内高点
                left_max = self.data['high'].iloc[max(0, i-window):i].max()
                right_max = self.data['high'].iloc[i+1:min(data_len, i+window+1)].max()
                
                # 更宽松的判断条件：是局部高点即可
                is_local_high = (current_high_price >= left_max * 0.98 and 
                                current_high_price >= right_max * 0.98)
                
                # 计算成交量情况
                if i >= 10:
                    avg_volume = self.data['turnover'].iloc[max(0, i-10):i].mean()
                    volume_ratio = self.data['turnover'].iloc[i] / avg_volume if avg_volume > 0 else 1
                else:
                    volume_ratio = 1
                
                if is_local_high or volume_ratio > 1.2:  # 放宽放量阈值
                    # 计算该价格被测试的次数
                    test_count = 0
                    for j in range(max(0, i-30), min(data_len, i+30)):
                        if (self.data['high'].iloc[j] >= current_high_price * 0.98 and 
                            self.data['high'].iloc[j] <= current_high_price * 1.02):
                            test_count += 1
                    
                    if volume_ratio > 1.2:  # 放量加分
                        test_count += 1
                    
                    # 计算距离当前价的百分比
                    distance_pct = (current_high_price - current_price) / current_price * 100
                    
                    # 分类近远期
                    category = 'near' if distance_pct < 10 else 'far'
                    
                    all_highs.append({
                        'date': current_date,
                        'price': current_high_price,
                        'close': self.data['close'].iloc[i],
                        'volume_ratio': volume_ratio,
                        'test_count': test_count,
                        'days_ago': data_len - 1 - i,
                        'distance_to_current_pct': distance_pct,
                        'window': window,
                        'category': category
                    })
        
        # 方法2：检测重要的历史阻力位（基于价格显著回调，只上方）
        for i in range(20, min(data_len, lookback_days)):
            high_price = self.data['high'].iloc[i]
            if high_price <= current_price:
                continue
            
            # 检查后续是否有显著回落
            future_days = min(20, data_len - i - 1)
            if future_days > 5:
                future_min = self.data['low'].iloc[i+1:i+future_days+1].min()
                decline_pct = (high_price - future_min) / high_price * 100
                
                if decline_pct > 10:  # 回落超过10%
                    # 计算成交量情况
                    if i >= 10:
                        avg_volume = self.data['turnover'].iloc[max(0, i-10):i].mean()
                        volume_ratio = self.data['turnover'].iloc[i] / avg_volume if avg_volume > 0 else 1
                    else:
                        volume_ratio = 1
                    
                    # 检查这个高点是否被多次测试
                    test_count = 0
                    for j in range(max(0, i-30), min(data_len, i+30)):
                        if (self.data['high'].iloc[j] >= high_price * 0.98 and 
                            self.data['high'].iloc[j] <= high_price * 1.02):
                            test_count += 1
                    
                    if volume_ratio > 1.2:
                        test_count += 1
                    
                    if test_count >= 1:  # 放宽条件
                        distance_pct = (high_price - current_price) / current_price * 100
                        category = 'near' if distance_pct < 10 else 'far'
                        all_highs.append({
                            'date': self.data.index[i],
                            'price': high_price,
                            'close': self.data['close'].iloc[i],
                            'volume_ratio': volume_ratio,
                            'test_count': test_count,
                            'days_ago': data_len - 1 - i,
                            'distance_to_current_pct': distance_pct,
                            'window': 'history_resistance',
                            'category': category
                        })
        
        # 方法3：检测成交密集区形成的压力位（动态衰减加权筹码分布）
        recent_data = self.data.tail(min(lookback_days, data_len))
        
        price_samples = []
        weights = []
        
        for idx in range(len(recent_data)):
            low_price = recent_data['low'].iloc[idx]
            high_price = recent_data['high'].iloc[idx]
            turnover = recent_data['turnover'].iloc[idx]
            days_ago = len(recent_data) - 1 - idx
            weight = turnover * np.exp(-decay_rate * days_ago)
            
            if turnover > 0:
                num_samples = int(turnover / recent_data['turnover'].mean() * 5)
                num_samples = max(1, min(100, num_samples))
                samples = np.linspace(low_price, high_price, num_samples)
                price_samples.extend(samples)
                weights.extend([weight / num_samples] * num_samples)
        
        if price_samples:
            price_samples = np.array(price_samples)
            if len(price_samples) > 0:
                kde = stats.gaussian_kde(price_samples, weights=weights)
                price_range = np.linspace(price_samples.min(), price_samples.max(), 200)
                density = kde(price_range)
                
                peaks = []
                for i in range(1, len(density)-1):
                    if density[i] > density[i-1] and density[i] > density[i+1]:
                        peaks.append({
                            'price': price_range[i],
                            'density': density[i]
                        })
                
                peaks.sort(key=lambda x: x['density'], reverse=True)
                for i, peak in enumerate(peaks[:5]):
                    if peak['price'] > current_price:
                        distance_pct = (peak['price'] - current_price) / current_price * 100
                        if abs(distance_pct) > 30:
                            continue
                        category = 'near' if distance_pct < 10 else 'far'
                        all_highs.append({
                            'date': recent_data.index[-1],
                            'price': peak['price'],
                            'close': peak['price'],
                            'volume_ratio': 1.0,
                            'test_count': 2,
                            'days_ago': 0,
                            'distance_to_current_pct': distance_pct,
                            'window': 'volume_density',
                            'category': category
                        })
        
        # 方法4：横盘放量试盘压力位
        std_20 = self.data['close'].rolling(20).std()
        mean_std = std_20.mean()
        high_vol_mask = self.data['turnover'] > self.data['turnover'].rolling(20).mean() * 1.5
        low_vol_mask = std_20 < mean_std * 0.5
        horizontal_high_vol = self.data[low_vol_mask & high_vol_mask].tail(lookback_days)
        
        for idx in horizontal_high_vol.index:
            i = self.data.index.get_loc(idx)
            high_price = self.data['high'].loc[idx]
            if high_price <= current_price:
                continue
            test_count = sum(1 for j in range(max(0, i-30), min(data_len, i+30)) if abs(self.data['high'].iloc[j] - high_price) / high_price < 0.02)
            volume_ratio = self.data['turnover'].loc[idx] / self.data['turnover'].iloc[max(0, i-10):i].mean()
            if volume_ratio > 1.2:
                test_count += 1
            distance_pct = (high_price - current_price) / current_price * 100
            category = 'near' if distance_pct < 10 else 'far'
            all_highs.append({
                'date': idx,
                'price': high_price,
                'close': self.data['close'].loc[idx],
                'volume_ratio': volume_ratio,
                'test_count': test_count,
                'days_ago': data_len - 1 - i,
                'distance_to_current_pct': distance_pct,
                'window': 'horizontal_high_vol',
                'category': category
            })
        
        # 去重和合并
        if all_highs:
            all_highs.sort(key=lambda x: x['price'])
            filtered_highs = []
            for high in all_highs:
                if not filtered_highs:
                    filtered_highs.append(high)
                else:
                    last_high = filtered_highs[-1]
                    price_diff = abs(high['price'] - last_high['price']) / last_high['price'] * 100
                    
                    if price_diff <= 2.0:
                        if high['test_count'] > last_high['test_count']:
                            filtered_highs[-1] = high
                    else:
                        filtered_highs.append(high)
        
        final_highs = [high for high in filtered_highs if high['test_count'] >= 1]
        
        final_highs.sort(key=lambda x: x['price'], reverse=True)
        
        self.pressure_levels = final_highs
        
        print(f"✅ 检测到{len(final_highs)}个潜在压力位")
        
        if final_highs:
            print("   主要压力位:")
            for i, high in enumerate(final_highs[:10], 1):
                strength = "强" if high['test_count'] >= 3 else "中" if high['test_count'] >= 2 else "弱"
                print(f"     {i}. {high['price']:.2f} "
                      f"(测试:{high['test_count']}次, 强度:{strength}, "
                      f"位置:上方{high['distance_to_current_pct']:.1f}%, {high['category']})")
        
        return final_highs
    
    def cluster_pressure_levels_v5(self, 
                                  max_gap_pct: float = None,
                                  min_cluster_size: int = 1) -> List[Dict]:
        """
        修正版压力位聚类 V5 - 允许单点集群
        """
        print("📊 修正版压力位聚类V5...")
        
        if not self.pressure_levels:
            self.pressure_levels = self.detect_pressure_levels_v5()
        
        if len(self.pressure_levels) < 1:
            print(f"   压力位数量不足: {len(self.pressure_levels)}")
            self.pressure_clusters = []
            return []
        
        # 自适应聚类参数
        prices = [h['price'] for h in self.pressure_levels]
        price_range = max(prices) - min(prices)
        price_median = np.median(prices)
        
        if max_gap_pct is None:
            if price_range / price_median > 0.3:
                max_gap_pct = 6.0
            elif price_range / price_median > 0.15:
                max_gap_pct = 4.0
            else:
                max_gap_pct = 3.0
        
        print(f"   聚类参数: 最大差距={max_gap_pct}%, 最小集群大小={min_cluster_size}")
        
        # 使用DBSCAN风格的聚类算法
        sorted_levels = sorted(self.pressure_levels, key=lambda x: x['price'])
        
        clusters = []
        current_cluster = []
        
        for level in sorted_levels:
            if not current_cluster:
                current_cluster.append(level)
            else:
                cluster_prices = [l['price'] for l in current_cluster]
                cluster_avg = np.mean(cluster_prices)
                price_gap = abs(level['price'] - cluster_avg) / cluster_avg * 100
                
                if price_gap <= max_gap_pct:
                    current_cluster.append(level)
                else:
                    if len(current_cluster) >= min_cluster_size:
                        clusters.append(current_cluster.copy())
                    current_cluster = [level]
        
        if len(current_cluster) >= min_cluster_size:
            clusters.append(current_cluster)
        
        # 处理单个重要压力位
        all_clustered_indices = [sorted_levels.index(level) for cluster in clusters for level in cluster]
        for i, level in enumerate(sorted_levels):
            if i not in all_clustered_indices and level['test_count'] >= 1:  # 进一步放宽
                clusters.append([level])
        
        # 计算集群统计
        clustered_results = []
        for i, cluster in enumerate(clusters):
            prices = [l['price'] for l in cluster]
            test_counts = [l['test_count'] for l in cluster]
            dates = [l['date'] for l in cluster]
            categories = [l['category'] for l in cluster]
            
            avg_test_count = np.mean(test_counts)
            strength = '强' if avg_test_count >= 3 else '中' if avg_test_count >= 2 else '弱'
            strength_score = 3 if strength == '强' else 2 if strength == '中' else 1
            
            price_range_pct = (max(prices) - min(prices)) / np.mean(prices) * 100 if len(prices)>1 else 0
            density = '密集' if price_range_pct <= 5 else '中等' if price_range_pct <= 10 else '分散'
            
            category = max(set(categories), key=categories.count)
            
            cluster_info = {
                'cluster_id': i + 1,
                'levels': cluster,
                'min_price': min(prices),
                'max_price': max(prices),
                'avg_price': np.mean(prices),
                'median_price': np.median(prices),
                'price_range_pct': price_range_pct,
                'density': density,
                'earliest_date': min(dates),
                'latest_date': max(dates),
                'level_count': len(cluster),
                'avg_test_count': avg_test_count,
                'resistance_strength': strength,
                'strength_score': strength_score,
                'is_single': len(cluster) == 1,
                'category': category
            }
            
            if len(cluster) == 1:
                cluster_info['description'] = f"重要独立压力位 (测试{test_counts[0]}次, {category})"
            else:
                cluster_info['description'] = f"压力集群 (密度:{density}, {category})"
            
            clustered_results.append(cluster_info)
        
        # 按强度排序
        clustered_results.sort(key=lambda x: (-x['strength_score'], -x['level_count']))
        
        self.pressure_clusters = clustered_results
        
        print(f"✅ 识别到{len(clustered_results)}个压力集群/位")
        
        for cluster in clustered_results:
            strength_icon = "🟥" if cluster['resistance_strength'] == '强' else "🟧" if cluster['resistance_strength'] == '中' else "🟨"
            if cluster['is_single']:
                level = cluster['levels'][0]
                print(f"   {strength_icon} 独立压力位{cluster['cluster_id']}: {level['price']:.2f} "
                      f"(测试:{level['test_count']}次, 强度:{cluster['resistance_strength']}, {cluster['category']})")
            else:
                print(f"   {strength_icon} 集群{cluster['cluster_id']}: "
                      f"{cluster['min_price']:.2f}-{cluster['max_price']:.2f} "
                      f"(密度:{cluster['density']}, {cluster['level_count']}个点, 强度:{cluster['resistance_strength']}, {cluster['category']})")
        
        return clustered_results
    
    def detect_horizontal_zones(self, lookback_days=60, std_threshold=0.5, vol_threshold=1.5):
        """
        检测横盘放量区，作为支撑/压力
        """
        recent_data = self.data.tail(lookback_days)
        std_20 = recent_data['close'].rolling(20).std()
        mean_std = std_20.mean()
        high_vol_mask = recent_data['turnover'] > recent_data['turnover'].rolling(20).mean() * vol_threshold
        low_vol_mask = std_20 < mean_std * std_threshold
        horizontal_periods = recent_data[low_vol_mask]
        
        zones = []
        if not horizontal_periods.empty:
            # 聚类横盘期
            current_zone_start = horizontal_periods.index[0]
            current_zone_high = horizontal_periods['high'].iloc[0]
            current_zone_low = horizontal_periods['low'].iloc[0]
            current_vol = 0
            
            for idx in horizontal_periods.index[1:]:
                if (idx - current_zone_start).days <= 5:  # 连续期
                    current_zone_high = max(current_zone_high, horizontal_periods['high'].loc[idx])
                    current_zone_low = min(current_zone_low, horizontal_periods['low'].loc[idx])
                    if high_vol_mask.loc[idx]:
                        current_vol += 1
                else:
                    if current_vol > 0:  # 有放量
                        zones.append({
                            'start': current_zone_start,
                            'end': previous_idx,
                            'high': current_zone_high,
                            'low': current_zone_low,
                            'vol_count': current_vol,
                            'type': 'horizontal_zone'
                        })
                    current_zone_start = idx
                    current_zone_high = horizontal_periods['high'].loc[idx]
                    current_zone_low = horizontal_periods['low'].loc[idx]
                    current_vol = 1 if high_vol_mask.loc[idx] else 0
                previous_idx = idx
            
            if current_vol > 0:
                zones.append({
                    'start': current_zone_start,
                    'end': previous_idx,
                    'high': current_zone_high,
                    'low': current_zone_low,
                    'vol_count': current_vol,
                    'type': 'horizontal_zone'
                })
        
        return zones
    
    def calculate_support_levels_v5(self, decay_rate: float = 0.01) -> List[Dict]:
        """
        计算支撑位 V5 - 替换MA为BOLL下轨和中轨（距离过滤），添加长阳收盘支撑，添加横盘区下轨支撑
        """
        print("📉 计算支撑位V5...")
        
        current_price = self.data['close'].iloc[-1]
        
        supports = []
        
        # 1. 斐波那契回调支撑（基于近期波段）
        if len(self.data) >= 20:
            recent_high = self.data['high'].iloc[-30:].max()
            recent_low = self.data['low'].iloc[-30:].min()
            
            price_range = recent_high - recent_low
            fib_levels = {
                '23.6%': recent_high - price_range * 0.236,
                '38.2%': recent_high - price_range * 0.382,
                '50.0%': recent_high - price_range * 0.500,
                '61.8%': recent_high - price_range * 0.618,
                '78.6%': recent_high - price_range * 0.786
            }
            
            for level, price in fib_levels.items():
                if price < current_price:
                    distance_pct = (current_price - price) / current_price * 100
                    strength = '强' if level in ['38.2%', '50.0%'] else '中等'
                    category = 'near' if distance_pct < 10 else 'far'
                    supports.append({
                        'price': price,
                        'type': f'斐波那契{level}',
                        'strength': strength,
                        'distance_pct': distance_pct,
                        'priority': 1 if strength == '强' else 2,
                        'category': category
                    })
        
        # 2. BOLL支撑（中轨强，下轨若近加）
        boll_period = 20
        boll_std = 2
        if len(self.data) >= boll_period:
            rolling_mean = self.data['close'].rolling(boll_period).mean()
            rolling_std = self.data['close'].rolling(boll_period).std()
            boll_mid = rolling_mean.iloc[-1]
            boll_lower = rolling_mean.iloc[-1] - (rolling_std.iloc[-1] * boll_std)
            
            # 中轨
            if boll_mid < current_price:
                distance_pct = (current_price - boll_mid) / current_price * 100
                category = 'near' if distance_pct < 10 else 'far'
                supports.append({
                    'price': boll_mid,
                    'type': f'BOLL中轨({boll_period})',
                    'strength': '强',
                    'distance_pct': distance_pct,
                    'priority': 1,
                    'category': category
                })
            
            # 下轨若距离<15%加
            if boll_lower < current_price:
                distance_pct = (current_price - boll_lower) / current_price * 100
                if distance_pct < 15:
                    category = 'near' if distance_pct < 10 else 'far'
                    supports.append({
                        'price': boll_lower,
                        'type': f'BOLL下轨({boll_period})',
                        'strength': '中等',
                        'distance_pct': distance_pct,
                        'priority': 2,
                        'category': category
                    })
        
        # 3. 近期低点支撑
        recent_lows = []
        lookback = min(30, len(self.data))
        
        for i in range(len(self.data)-lookback, len(self.data)-5):
            low_price = self.data['low'].iloc[i]
            if low_price >= current_price:  # 只下方
                continue
            date = self.data.index[i]
            
            # 检查是否是局部低点
            if i >= 5:
                left_min = self.data['low'].iloc[max(0, i-5):i].min()
                right_min = self.data['low'].iloc[i+1:min(len(self.data), i+6)].min()
                
                if low_price <= left_min and low_price <= right_min:
                    # 检查这个低点是否被测试过
                    test_count = 0
                    for j in range(max(0, i-10), min(len(self.data), i+11)):
                        if (self.data['low'].iloc[j] <= low_price * 1.02 and 
                            self.data['low'].iloc[j] >= low_price * 0.98):
                            test_count += 1
                    
                    if test_count >= 2:
                        distance_pct = (current_price - low_price) / current_price * 100
                        category = 'near' if distance_pct < 10 else 'far'
                        recent_lows.append({
                            'price': low_price,
                            'date': date,
                            'type': '近期低点',
                            'strength': '中等',
                            'test_count': test_count,
                            'distance_pct': distance_pct,
                            'priority': 2,
                            'category': category
                        })
        
        supports.extend(recent_lows)
        
        # 4. 重要整数位支撑
        integer_levels = []
        price_min = self.data['low'].min()
        price_max = self.data['high'].max()
        
        min_int = int(np.floor(price_min))
        max_int = int(np.ceil(price_max))
        
        for int_level in range(min_int, max_int + 1):
            if int_level < current_price:
                mask = (self.data['low'] <= int_level * 1.02) & (self.data['high'] >= int_level * 0.98)
                test_count = mask.sum()
                
                if test_count >= 3:
                    distance_pct = (current_price - int_level) / current_price * 100
                    category = 'near' if distance_pct < 10 else 'far'
                    integer_levels.append({
                        'price': int_level,
                        'type': '整数位支撑',
                        'strength': '中等',
                        'test_count': test_count,
                        'distance_pct': distance_pct,
                        'priority': 3,
                        'category': category
                    })
        
        supports.extend(integer_levels)
        
        # 5. 加权筹码密集区作为支撑（下方峰值）
        lookback_days = 60
        recent_data = self.data.tail(min(lookback_days, len(self.data)))
        
        price_samples = []
        weights = []
        
        for idx in range(len(recent_data)):
            low_price = recent_data['low'].iloc[idx]
            high_price = recent_data['high'].iloc[idx]
            turnover = recent_data['turnover'].iloc[idx]
            days_ago = len(recent_data) - 1 - idx
            weight = turnover * np.exp(-decay_rate * days_ago)
            
            if turnover > 0:
                num_samples = int(turnover / recent_data['turnover'].mean() * 5)
                num_samples = max(1, min(100, num_samples))
                samples = np.linspace(low_price, high_price, num_samples)
                price_samples.extend(samples)
                weights.extend([weight / num_samples] * num_samples)
        
        if price_samples:
            price_samples = np.array(price_samples)
            if len(price_samples) > 0:
                kde = stats.gaussian_kde(price_samples, weights=weights)
                price_range = np.linspace(price_samples.min(), price_samples.max(), 200)
                density = kde(price_range)
                
                peaks = []
                for i in range(1, len(density)-1):
                    if density[i] > density[i-1] and density[i] > density[i+1]:
                        peaks.append({
                            'price': price_range[i],
                            'density': density[i]
                        })
                
                peaks.sort(key=lambda x: x['density'], reverse=True)
                for i, peak in enumerate(peaks[:5]):
                    if peak['price'] < current_price:  # 只下方作为支撑
                        distance_pct = (current_price - peak['price']) / current_price * 100
                        if distance_pct > 30:
                            continue
                        category = 'near' if distance_pct < 10 else 'far'
                        supports.append({
                            'price': peak['price'],
                            'type': '筹码密集区',
                            'strength': '强' if peak['density'] > np.mean(density) * 2 else '中等',
                            'distance_pct': distance_pct,
                            'priority': 1,
                            'category': category
                        })
        
        # 6. 长阳收盘支撑 (放宽阈值)
        long_yang_supports = []
        recent_data = self.data.tail(60)
        for i in range(1, len(recent_data)):
            close = recent_data['close'].iloc[i]
            prev_close = recent_data['close'].iloc[i-1]
            open_p = recent_data['open'].iloc[i]
            volume = recent_data['turnover'].iloc[i]
            date = recent_data.index[i]
            
            change_pct = (close - prev_close) / prev_close * 100
            if change_pct > 3 and close > open_p:  # 放宽到>3%
                if i >= 10:
                    avg_volume = recent_data['turnover'].iloc[max(0, i-10):i].mean()
                    volume_ratio = volume / avg_volume if avg_volume > 0 else 1
                else:
                    volume_ratio = 1
                
                if volume_ratio > 1.0 and close < current_price:  # 放宽vol
                    distance_pct = (current_price - close) / current_price * 100
                    category = 'near' if distance_pct < 10 else 'far'
                    long_yang_supports.append({
                        'price': close,
                        'date': date,
                        'type': '长阳收盘',
                        'strength': '中等',
                        'distance_pct': distance_pct,
                        'priority': 2,
                        'category': category
                    })
        
        supports.extend(long_yang_supports)
        
        # 7. 横盘放量下轨支撑
        horizontal_zones = self.detect_horizontal_zones()
        for zone in horizontal_zones:
            low_price = zone['low']
            if low_price < current_price:
                distance_pct = (current_price - low_price) / current_price * 100
                strength = '强' if zone['vol_count'] >= 3 else '中等'
                category = 'near' if distance_pct < 10 else 'far'
                supports.append({
                    'price': low_price,
                    'type': '横盘下轨',
                    'strength': strength,
                    'distance_pct': distance_pct,
                    'priority': 1 if strength == '强' else 2,
                    'category': category
                })
        
        # 合并相近的支撑位（1.5%以内）
        merged_supports = []
        for support in sorted(supports, key=lambda x: x['price'], reverse=True):
            if not merged_supports:
                merged_supports.append(support)
            else:
                last_support = merged_supports[-1]
                price_diff = abs(support['price'] - last_support['price']) / last_support['price'] * 100
                
                if price_diff <= 1.5:
                    if support['priority'] < last_support['priority']:
                        merged_supports[-1] = support
                    elif support['priority'] == last_support['priority']:
                        if support['distance_pct'] < last_support['distance_pct']:
                            merged_supports[-1] = support
                else:
                    merged_supports.append(support)
        
        # 按优先级和距离排序
        merged_supports.sort(key=lambda x: (x['priority'], x['distance_pct']))
        
        # 只保留距离合理的支撑位（50%以内）
        filtered_supports = [s for s in merged_supports if s['distance_pct'] <= 50]
        
        self.support_levels = filtered_supports
        
        print(f"✅ 计算到{len(filtered_supports)}个支撑位")
        
        if filtered_supports:
            print("   主要支撑位:")
            for i, support in enumerate(filtered_supports[:8], 1):
                strength_icon = "🟩" if support['strength'] == '强' else "🟨" if support['strength'] == '中等' else "⬜"
                print(f"     {strength_icon} {i}. {support['price']:.2f} - {support['type']} "
                      f"(距离:{support['distance_pct']:.1f}%, 强度:{support['strength']}, {support['category']})")
        
        return filtered_supports
    
    def analyze_breakouts_v5(self, 
                           days_to_analyze: int = 30,
                           min_breakout_pct: float = 3.0) -> Dict:
        """
        分析突破 V5
        """
        print(f"🔍 分析最近{days_to_analyze}天的突破V5...")
        
        if not self.pressure_clusters:
            self.pressure_clusters = self.cluster_pressure_levels_v5()
        
        current_date = self.data.index[-1]
        current_price = self.data['close'].iloc[-1]
        current_high = self.data['high'].iloc[-1]
        
        recent_start = current_date - timedelta(days=days_to_analyze)
        recent_data = self.data[self.data.index >= recent_start]
        
        big_up_days = []
        valid_breakout_days = []
        
        for i in range(1, len(recent_data)):
            date = recent_data.index[i]
            close = recent_data['close'].iloc[i]
            prev_close = recent_data['close'].iloc[i-1]
            high = recent_data['high'].iloc[i]
            low = recent_data['low'].iloc[i]
            volume = recent_data['turnover'].iloc[i]
            
            daily_change = (close - prev_close) / prev_close * 100
            is_big_up = daily_change > 3.0
            
            if is_big_up:
                if i >= 10:
                    avg_volume = recent_data['turnover'].iloc[max(0, i-10):i].mean()
                    volume_ratio = volume / avg_volume if avg_volume > 0 else 1
                else:
                    volume_ratio = 1
                
                broken_clusters = []
                for cluster in self.pressure_clusters:
                    if close > cluster['avg_price'] and high > cluster['avg_price'] * (1 + min_breakout_pct/100):
                        broken_clusters.append(cluster['cluster_id'])
                
                amplitude = (high - low) / prev_close * 100
                
                day_info = {
                    'date': date,
                    'close': close,
                    'high': high,
                    'low': low,
                    'open': recent_data['open'].iloc[i],
                    'daily_change_pct': daily_change,
                    'volume_ratio': volume_ratio,
                    'amplitude_pct': amplitude,
                    'broken_clusters': broken_clusters,
                    'is_breakout': len(broken_clusters) > 0,
                    'is_valid_breakout': False
                }
                
                big_up_days.append(day_info)
                
                if day_info['is_breakout']:
                    break_date = date
                    break_idx = self.data.index.get_loc(break_date)
                    future_days = min(3, len(self.data) - break_idx - 1)
                    
                    if future_days >= 2:
                        future_closes = [self.data['close'].iloc[break_idx + j] for j in range(1, future_days+1)]
                        all_above = all([fc > cluster['avg_price'] * 0.99 for fc in future_closes])
                        if all_above:
                            day_info['is_valid_breakout'] = True
                    else:
                        if volume_ratio > 1.2 and amplitude > 4:  # 放宽
                            day_info['is_valid_breakout'] = True
                    
                    if day_info['is_valid_breakout']:
                        valid_breakout_days.append(day_info)
        
        # 连续上涨序列
        consecutive_up = []
        temp_sequence = []
        
        for i in range(1, len(recent_data)):
            close = recent_data['close'].iloc[i]
            prev_close = recent_data['close'].iloc[i-1]
            
            if close > prev_close:
                if not temp_sequence:
                    temp_sequence = [i-1, i]
                else:
                    temp_sequence.append(i)
            else:
                if len(temp_sequence) >= 3:
                    consecutive_up.append(temp_sequence.copy())
                temp_sequence = []
        
        if len(temp_sequence) >= 3:
            consecutive_up.append(temp_sequence)
        
        result = {
            'current_status': {
                'date': current_date,
                'price': current_price,
                'high': current_high,
                'low': self.data['low'].iloc[-1],
                'days_analyzed': days_to_analyze
            },
            'big_up_days': big_up_days,
            'valid_breakout_days': valid_breakout_days,
            'consecutive_up_sequences': consecutive_up,
            'pressure_clusters': self.pressure_clusters,
            'support_levels': self.support_levels if hasattr(self, 'support_levels') else []
        }
        
        print(f"✅ 突破分析完成")
        print(f"   发现{len(big_up_days)}个大涨交易日，{len(valid_breakout_days)}个有效突破")
        
        if valid_breakout_days:
            print("   最近有效突破:")
            for day in valid_breakout_days[-3:]:
                print(f"     ✅ {day['date'].date()}: 涨幅{day['daily_change_pct']:.1f}%, "
                      f"突破集群{day['broken_clusters']}")
        
        return result
    
    def evaluate_pattern_v5(self, analysis_result: Dict = None) -> Dict:
        """
        形态评估 V5
        """
        print("📈 形态评估V5...")
        
        if analysis_result is None:
            analysis_result = self.analyze_breakouts_v5()
        
        pattern_score = 0
        pattern_indicators = []
        
        current_price = self.data['close'].iloc[-1]
        current_high = self.data['high'].iloc[-1]
        
        # 1. 压力集群分析 (0-25分，只上方)
        pressure_clusters = analysis_result.get('pressure_clusters', [])
        if pressure_clusters:
            strong_clusters = [c for c in pressure_clusters if c['resistance_strength'] == '强']
            medium_clusters = [c for c in pressure_clusters if c['resistance_strength'] == '中']
            
            if strong_clusters:
                pattern_score += min(len(strong_clusters) * 6, 15)
                pattern_indicators.append(f"🎯 发现{len(strong_clusters)}个强压力集群 ({len([c for c in strong_clusters if c['category']=='near'])} near)")
            
            if medium_clusters:
                pattern_score += min(len(medium_clusters) * 3, 10)
                pattern_indicators.append(f"📊 发现{len(medium_clusters)}个中等压力集群 ({len([c for c in medium_clusters if c['category']=='near'])} near)")
            
            dense_clusters = [c for c in pressure_clusters if c['density'] == '密集']
            if dense_clusters:
                pattern_score += 5
                pattern_indicators.append(f"📈 {len(dense_clusters)}个密集压力区")
        else:
            pattern_indicators.append("⚠️  无压力集群")
        
        # 2. 有效突破分析 (0-30分)
        valid_breakout_days = analysis_result.get('valid_breakout_days', [])
        if valid_breakout_days:
            pattern_score += min(len(valid_breakout_days) * 10, 25)
            pattern_indicators.append(f"✅ {len(valid_breakout_days)}次有效突破")
            
            strong_breakouts = [d for d in valid_breakout_days if d['daily_change_pct'] > 7]
            if strong_breakouts:
                pattern_score += 5
                pattern_indicators.append(f"⚡ {len(strong_breakouts)}次强势突破(>7%)")
        else:
            big_up_days = analysis_result.get('big_up_days', [])
            if big_up_days:
                pattern_score += min(len(big_up_days) * 2, 10)
                pattern_indicators.append(f"📊 {len(big_up_days)}次大涨(>3%)")
            else:
                pattern_indicators.append("❌ 无显著上涨")
        
        # 3. 当前位置分析 (0-20分)
        if pressure_clusters:
            clusters_near = sum(1 for c in pressure_clusters if abs((current_price - c['avg_price']) / c['avg_price'] * 100) <= 3)
            if clusters_near > 0:
                pattern_score += 5
                pattern_indicators.append(f"📍 接近{clusters_near}个压力区")
        
        # 4. 支撑位分析 (0-15分)
        support_levels = analysis_result.get('support_levels', [])
        if support_levels:
            strong_supports = [s for s in support_levels if s['strength'] == '强']
            
            if strong_supports:
                pattern_score += min(len(strong_supports) * 3, 9)
                pattern_indicators.append(f"🛡️  {len(strong_supports)}个强支撑位 ({len([s for s in strong_supports if s['category']=='near'])} near)")
            
            near_supports = [s for s in support_levels if s['distance_pct'] <= 10]
            if near_supports:
                pattern_score += 6
                nearest = min(near_supports, key=lambda x: x['distance_pct'])
                pattern_indicators.append(f"📉 近支撑在{nearest['price']:.2f}(距离:{nearest['distance_pct']:.1f}%)")
        
        # 5. 技术面分析 (0-10分)
        if len(self.data) >= 20:
            ma20 = self.data['close'].rolling(20).mean().iloc[-1]
            ma10 = self.data['close'].rolling(10).mean().iloc[-1]
            ma5 = self.data['close'].rolling(5).mean().iloc[-1]
            
            if ma5 > ma10 > ma20 and current_price > ma5:
                pattern_score += 8
                pattern_indicators.append(f"📊 均线多头排列(5>10>20)")
            elif current_price > ma20:
                pattern_score += 4
                pattern_indicators.append(f"📈 站上20日均线")
        
        pattern_score = min(100, max(0, pattern_score))
        
        if pattern_score >= 75:
            pattern_rating = "强势突破"
            pattern_detected = True
        elif pattern_score >= 60:
            pattern_rating = "有效突破"
            pattern_detected = True
        elif pattern_score >= 45:
            pattern_rating = "尝试突破"
            pattern_detected = True
        elif pattern_score >= 30:
            pattern_rating = "潜在突破"
            pattern_detected = False
        else:
            pattern_rating = "无突破信号"
            pattern_detected = False
        
        evaluation = {
            'pattern_detected': pattern_detected,
            'pattern_score': pattern_score,
            'pattern_rating': pattern_rating,
            'indicators': pattern_indicators,
            'current_price': current_price,
            'current_high': current_high,
            'support_levels': support_levels,
            'pressure_clusters': pressure_clusters
        }
        
        self.pattern_evaluation = evaluation
        
        print(f"✅ 形态评估完成")
        print(f"   综合评分: {pattern_score}/100 ({pattern_rating})")
        print(f"   形态检测: {'✅ 符合突破形态' if pattern_detected else '⚠️  观察中' if pattern_score >= 30 else '❌ 不符合'}")
        
        return evaluation
    
    def detect_complete_pattern_v5(self) -> Dict:
        """
        完整版突破形态检测 V5
        """
        print("=" * 70)
        print("🚀 完整版突破形态检测开始 V5")
        print("=" * 70)
        
        current_date = self.data.index[-1]
        current_price = self.data['close'].iloc[-1]
        current_high = self.data['high'].iloc[-1]
        
        print(f"📊 股票状态:")
        print(f"   分析日期: {current_date.date()}")
        print(f"   当前价格: {current_price:.2f}")
        print(f"   当前高点: {current_high:.2f}")
        print(f"   数据周期: {len(self.data)}天")
        
        # 1. 检测压力位
        self.detect_pressure_levels_v5(current_price, lookback_days=60)
        
        # 2. 聚类压力位
        self.cluster_pressure_levels_v5()
        
        # 3. 计算支撑位
        self.calculate_support_levels_v5()
        
        # 4. 分析突破
        analysis_result = self.analyze_breakouts_v5(days_to_analyze=30)
        
        # 5. 评估形态
        evaluation = self.evaluate_pattern_v5(analysis_result)
        
        # 6. 构建结果
        final_result = {
            **analysis_result,
            **evaluation,
            'stock_info': {
                'current_date': current_date,
                'current_price': current_price,
                'current_high': current_high,
                'data_length': len(self.data)
            }
        }
        
        print("\n" + "=" * 70)
        print("📊 完整版分析总结 V5")
        print("=" * 70)
        print(f"压力集群: {len(self.pressure_clusters)}个")
        print(f"支撑位: {len(self.support_levels)}个")
        print(f"形态评分: {evaluation['pattern_score']}/100")
        print(f"形态评级: {evaluation['pattern_rating']}")
        print(f"形态检测: {'✅ 符合突破形态' if evaluation['pattern_detected'] else '⚠️  观察中' if evaluation['pattern_score'] >= 30 else '❌ 不符合'}")
        
        if evaluation['pattern_score'] >= 30:
            print(f"\n🎯 关键特征:")
            for indicator in evaluation['indicators'][:6]:
                print(f"   • {indicator}")
        
        print("=" * 70)
        
        return final_result
    
    def plot_analysis_v5(self, analysis_result: Dict = None, save_path: str = None) -> plt.Figure:
        """
        绘制分析图 V5
        """
        print("🎨 绘制分析图V5...")
        
        if analysis_result is None:
            analysis_result = self.detect_complete_pattern_v5()
        
        fig = plt.figure(figsize=(18, 14))
        gs = fig.add_gridspec(4, 1, height_ratios=[3, 1, 1, 1], hspace=0.1)
        
        ax1 = fig.add_subplot(gs[0])
        
        plot_days = min(60, len(self.data))
        plot_data = self.data.tail(plot_days)
        x_indices = np.arange(len(plot_data))
        dates = plot_data.index
        
        # 绘制K线
        for i in range(len(plot_data)):
            open_p = plot_data['open'].iloc[i]
            close = plot_data['close'].iloc[i]
            color = 'red' if close >= open_p else 'green'
            
            ax1.plot([x_indices[i], x_indices[i]], 
                    [plot_data['low'].iloc[i], plot_data['high'].iloc[i]], 
                    color=color, linewidth=1.5, alpha=0.8)
            
            width = 0.6
            rect = Rectangle(
                (x_indices[i] - width/2, min(open_p, close)),
                width,
                abs(close - open_p),
                facecolor=color,
                edgecolor=color,
                alpha=0.7
            )
            ax1.add_patch(rect)
        
        # 移动平均线
        if len(plot_data) >= 5:
            ma5 = plot_data['close'].rolling(5).mean()
            ma10 = plot_data['close'].rolling(10).mean()
            ma20 = plot_data['close'].rolling(20).mean()
            
            ax1.plot(x_indices, ma5.values, 'orange', linewidth=1.5, alpha=0.7, label='MA5')
            ax1.plot(x_indices, ma10.values, 'blue', linewidth=1.5, alpha=0.7, label='MA10')
            ax1.plot(x_indices, ma20.values, 'purple', linewidth=1.5, alpha=0.7, label='MA20')
        
        # 标记压力集群 (上方)
        pressure_clusters = analysis_result.get('pressure_clusters', [])
        for cluster in pressure_clusters:
            color = 'darkred' if cluster['category'] == 'near' else 'red'
            alpha = 0.3 if cluster['resistance_strength'] == '强' else 0.2
            label = f"{cluster['description']}"
            
            if cluster['is_single']:
                ax1.axhline(y=cluster['avg_price'], color=color, linestyle='--', linewidth=2, alpha=0.7)
                ax1.text(x_indices[-1], cluster['avg_price'], label, color=color, fontsize=10)
            else:
                ax1.axhspan(cluster['min_price'], cluster['max_price'], alpha=alpha, color=color)
                ax1.axhline(y=cluster['avg_price'], color=color, linestyle='--', linewidth=1.5, alpha=0.6)
                ax1.text(x_indices[-1], cluster['avg_price'], label, color=color, fontsize=10)
        
        # 标记支撑位 (下方)
        support_levels = analysis_result.get('support_levels', [])
        for i, support in enumerate(support_levels[:5]):
            color = 'darkgreen' if support['category'] == 'near' else 'green'
            linestyle = '-' if support['strength'] == '强' else '--'
            linewidth = 2 if support['strength'] == '强' else 1.5
            alpha = 0.7 if support['strength'] == '强' else 0.5
            
            ax1.axhline(y=support['price'], color=color, linestyle=linestyle, 
                       linewidth=linewidth, alpha=alpha)
            ax1.text(x_indices[0], support['price'], 
                    f"支撑{i+1}: {support['price']:.2f} ({support['category']})", 
                    color=color, fontsize=9)
        
        # 当前价格
        current_price = self.data['close'].iloc[-1]
        ax1.axhline(y=current_price, color='blue', linestyle='-', linewidth=2, alpha=0.5)
        ax1.text(x_indices[-1], current_price, f" 当前价: {current_price:.2f}", color='blue', fontsize=12)
        
        ax1.set_title('价格走势与压力支撑分析 V5', fontsize=16, fontweight='bold', pad=20)
        ax1.set_ylabel('价格', fontsize=12)
        ax1.legend(loc='upper left', fontsize=9)
        ax1.grid(True, alpha=0.2)
        
        # x轴
        if len(dates) > 10:
            step = max(1, len(dates) // 10)
            tick_indices = np.arange(0, len(dates), step)
            tick_labels = [dates[i].strftime('%m-%d') for i in tick_indices]
            ax1.set_xticks(tick_indices)
            ax1.set_xticklabels(tick_labels, rotation=45)
        else:
            ax1.set_xticks(x_indices)
            ax1.set_xticklabels([d.strftime('%m-%d') for d in dates], rotation=45)
        
        # 成交量
        ax2 = fig.add_subplot(gs[1], sharex=ax1)
        colors = ['red' if c >= o else 'green' for c, o in zip(plot_data['close'], plot_data['open'])]
        ax2.bar(x_indices, plot_data['turnover'].values/1e8, width=0.6, color=colors, alpha=0.7)
        ax2.set_title('成交量分析', fontsize=14)
        ax2.set_ylabel('成交量 (亿)', fontsize=12)
        ax2.grid(True, alpha=0.2)
        
        # 涨幅
        ax3 = fig.add_subplot(gs[2], sharex=ax1)
        daily_changes = plot_data['close'].pct_change() * 100
        colors_change = ['red' if change >= 0 else 'green' for change in daily_changes]
        ax3.bar(x_indices[1:], daily_changes.values[1:], width=0.6, color=colors_change[1:], alpha=0.7)
        ax3.axhline(y=0, color='black', linewidth=0.5)
        ax3.axhline(y=5, color='orange', linestyle='--', alpha=0.6, label='大涨线(5%)')
        ax3.axhline(y=10, color='red', linestyle='--', alpha=0.6, label='暴涨线(10%)')
        ax3.set_title('每日涨幅分析', fontsize=14)
        ax3.set_ylabel('涨幅 (%)', fontsize=12)
        ax3.legend(loc='upper left', fontsize=9)
        ax3.grid(True, alpha=0.2)
        
        # RSI
        ax4 = fig.add_subplot(gs[3], sharex=ax1)
        if len(plot_data) >= 14:
            delta = plot_data['close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs))
            ax4.plot(x_indices[14:], rsi.values[14:], 'purple', linewidth=2, label='RSI(14)')
            ax4.axhline(y=70, color='red', linestyle='--', alpha=0.5, label='超买(70)')
            ax4.axhline(y=30, color='green', linestyle='--', alpha=0.5, label='超卖(30)')
            ax4.axhline(y=50, color='gray', linestyle='-', alpha=0.3)
        ax4.set_title('技术指标 (RSI)', fontsize=14)
        ax4.set_xlabel('日期', fontsize=12)
        ax4.set_ylabel('RSI', fontsize=12)
        ax4.set_ylim(0, 100)
        ax4.legend(loc='upper left', fontsize=9)
        ax4.grid(True, alpha=0.2)
        
        # 形态评分
        pattern_score = analysis_result.get('pattern_score', 0)
        pattern_rating = analysis_result.get('pattern_rating', '未知')
        pattern_detected = analysis_result.get('pattern_detected', False)
        
        color = 'darkgreen' if pattern_score >= 75 else 'green' if pattern_score >= 60 else 'orange' if pattern_score >= 45 else 'goldenrod' if pattern_score >= 30 else 'red'
        facecolor = 'lightgreen' if pattern_score >= 75 else 'lightyellow' if pattern_score >= 60 else 'wheat' if pattern_score >= 45 else 'linen' if pattern_score >= 30 else 'mistyrose'
        
        fig.text(0.02, 0.98, 
                f"形态评分: {pattern_score}/100\n评级: {pattern_rating}\n状态: {'✅突破' if pattern_detected else '⚠️观察' if pattern_score >= 30 else '❌无突破'}", 
                fontsize=12, fontweight='bold', color=color,
                bbox=dict(boxstyle="round,pad=0.5", facecolor=facecolor, alpha=0.9),
                verticalalignment='top')
        
        plt.tight_layout()
       
        if save_path:
            import os
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            fig.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"✅ 分析图已保存到: {save_path}")
        else:
            import os
            os.makedirs('output', exist_ok=True)
            save_path = f'output/stock_analysis_v5_{self.data.index[-1].date()}.png'
            fig.savefig(save_path, dpi=150, bbox_inches='tight')
            print(f"✅ 分析图已保存到: {save_path}")
        
        print("✅ 分析图绘制完成")
        plt.show()
        return fig
    
    def generate_report_v5(self, analysis_result: Dict = None, stock_code: str = "股票") -> str:
        """
        生成报告 V5 - 添加买卖建议基于近点位
        """
        print("📋 生成详细报告V5...")
        
        if analysis_result is None:
            analysis_result = self.detect_complete_pattern_v5()
        
        current_date = self.data.index[-1]
        current_price = self.data['close'].iloc[-1]
        
        pattern_score = analysis_result.get('pattern_score', 0)
        pattern_rating = analysis_result.get('pattern_rating', '未知')
        pattern_detected = analysis_result.get('pattern_detected', False)
        
        report = "=" * 80 + "\n"
        report += f"突破形态分析报告 V5 - {stock_code}\n"
        report += "=" * 80 + "\n\n"
        
        report += f"📅 分析时间: {current_date}\n"
        report += f"💰 当前价格: {current_price:.2f}\n"
        report += f"📊 形态评分: {pattern_score}/100\n"
        report += f"⭐ 形态评级: {pattern_rating}\n"
        report += f"🔍 形态检测: {'✅ 符合突破形态' if pattern_detected else '⚠️  观察中' if pattern_score >= 30 else '❌ 不符合'}\n\n"
        
        # 压力集群分析
        pressure_clusters = analysis_result.get('pressure_clusters', [])
        report += "【🎯 压力集群分析】\n"
        report += "-" * 60 + "\n"
        
        if pressure_clusters:
            report += f"识别到{len(pressure_clusters)}个压力集群/位:\n\n"
            near_pressures = [c for c in pressure_clusters if c['category'] == 'near']
            far_pressures = [c for c in pressure_clusters if c['category'] == 'far']
            
            report += "近期压力:\n"
            for cluster in near_pressures:
                if cluster['is_single']:
                    level = cluster['levels'][0]
                    report += f"独立压力位{cluster['cluster_id']}: {level['price']:.2f} ({cluster['category']})\n"
                else:
                    report += f"压力集群{cluster['cluster_id']}: {cluster['min_price']:.2f} - {cluster['max_price']:.2f} ({cluster['category']})\n"
            
            report += "\n远期压力:\n"
            for cluster in far_pressures:
                if cluster['is_single']:
                    level = cluster['levels'][0]
                    report += f"独立压力位{cluster['cluster_id']}: {level['price']:.2f} ({cluster['category']})\n"
                else:
                    report += f"压力集群{cluster['cluster_id']}: {cluster['min_price']:.2f} - {cluster['max_price']:.2f} ({cluster['category']})\n"
        else:
            report += "未识别到明显的压力集群\n\n"
        
        # 支撑位分析
        support_levels = analysis_result.get('support_levels', [])
        report += "【📉 支撑位分析】\n"
        report += "-" * 60 + "\n"
        
        if support_levels:
            report += f"识别到{len(support_levels)}个支撑位:\n\n"
            near_supports = [s for s in support_levels if s['category'] == 'near']
            far_supports = [s for s in support_levels if s['category'] == 'far']
            
            report += "近期支撑:\n"
            for i, support in enumerate(near_supports[:5], 1):
                report += f"支撑{i}: {support['price']:.2f} ({support['category']})\n"
                report += f"  类型: {support['type']}\n"
                report += f"  强度: {support['strength']}\n"
                report += f"  距离当前价: {support['distance_pct']:.1f}%\n\n"
            
            report += "远期支撑:\n"
            for i, support in enumerate(far_supports[:5], 1):
                report += f"支撑{i}: {support['price']:.2f} ({support['category']})\n"
                report += f"  类型: {support['type']}\n"
                report += f"  强度: {support['strength']}\n"
                report += f"  距离当前价: {support['distance_pct']:.1f}%\n\n"
        else:
            report += "未识别到有效的支撑位\n\n"
        
        # 形态强度指标
        indicators = analysis_result.get('indicators', [])
        report += "【📊 形态强度指标】\n"
        report += "-" * 60 + "\n"
        
        if indicators:
            for indicator in indicators:
                report += f"• {indicator}\n"
        else:
            report += "无显著形态指标\n"
        
        report += "\n"
        
        # 操作建议 - 添加买卖预案
        report += "【🎯 操作建议】\n"
        report += "-" * 60 + "\n"
        
        near_pressure = min([c['avg_price'] for c in near_pressures] + [float('inf')]) if near_pressures else float('inf')
        near_support = max([s['price'] for s in near_supports] + [0]) if near_supports else 0
        
        if pattern_detected:
            if pattern_score >= 75:
                report += "🚀 强势突破形态，建议操作:\n\n"
                report += "1. 🔥 积极买入或持有\n"
                report += "2. 📍 关注上方压力位突破情况\n"
                report += "3. 🛡️  设置止损在强支撑下方\n"
            elif pattern_score >= 60:
                report += "📈 有效突破形态，建议操作:\n\n"
                report += "1. 📊 可以谨慎参与\n"
                report += "2. 🔍 等待回踩确认\n"
                report += "3. ⚠️  控制仓位，设置止损\n"
            else:
                report += "⚠️  尝试突破形态，建议操作:\n\n"
                report += "1. 👀 继续观察\n"
                report += "2. 💡 小仓位试探\n"
                report += "3. 📉 严格止损\n"
        elif pattern_score >= 30:
            report += "⏳ 潜在突破形态，建议操作:\n\n"
            report += "1. 🔄 保持关注\n"
            report += "2. 📊 等待明确信号\n"
            report += "3. ⚠️  暂不参与\n"
        else:
            report += "❌ 无突破形态，建议操作:\n\n"
            report += "1. 🕐 继续观察\n"
            report += "2. 📊 关注基本面\n"
            report += "3. ⚠️  谨慎操作\n"
        
        report += "\n买卖预案（基于近点位）:\n"
        if near_pressure < float('inf'):
            report += f" - 若强势突破{near_pressure:.2f}，可买入，目标远压力\n"
        if near_support > 0:
            report += f" - 若跌破{near_support:.2f}，变盘卖出，止损\n"
        else:
            report += " - 无近点位，观察趋势\n"
        
        report += "\n📌 风险提示:\n"
        report += "  1. 技术分析仅供参考\n"
        report += "  2. 结合其他指标综合分析\n"
        report += "  3. 投资有风险，入市需谨慎\n"
        
        report += "\n" + "=" * 80 + "\n"
        report += "报告生成完毕 - 突破形态分析系统 V5\n"
        report += "=" * 80
        
        print("✅ 详细报告生成完成")
        
        return report
# 测试函数
def test_v5_with_real_data(data):
    """
    使用真实数据测试V5版本
    """
    print("🧪 测试V5版本分析器...")
    
    analyzer = StockBreakoutAnalyzer(data)
    
    # 运行完整分析
    result = analyzer.detect_complete_pattern_v5()
    
    # 生成报告
    report = analyzer.generate_report_v5(result, "300433")
    print(report)
    
    # 绘制图表
    fig = analyzer.plot_analysis_v5(result, save_path='output/300433_v5_analysis.png')
    fig.show()
    return analyzer, result, fig
# 实时行情数据获取函数
def get_real_stock_data(stock_code='300433', days=100, realtime=False):
    """
    获取真实股票数据 - 使用新浪财经API
    
    Args:
        stock_code: 股票代码
        days: 获取数据天数（最近days天）
        realtime: 是否包含实时数据
    
    Returns:
        pd.DataFrame: 包含OHLC和成交量的数据
    """
    if realtime:
        print(f"📊 正在获取 {stock_code} 实时数据 + 历史 {days} 天数据...")
    else:
        print(f"📊 正在获取 {stock_code} 最近 {days} 天的真实数据...")
    
    try:
        # 导入新浪财经API
        import sys
        import os
        
        # 添加API目录到路径
        api_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'API')
        if api_dir not in sys.path:
            sys.path.insert(0, api_dir)
        
        from sina_kline_api import SinaKLineAPI
        
        # 创建API实例
        api = SinaKLineAPI(timeout=30)
        
        # 获取日线数据
        kline_data = api.get_kline_data(stock_code, scale='day', ma=5, datalen=days)
        
        if not kline_data:
            raise Exception(f"未获取到 {stock_code} 的数据")
        
        # 转换为DataFrame
        df = api.to_dataframe(kline_data)
        
        # 数据清洗和格式化
        # 确保数据按时间正序排列（最旧的在前，最新的在后）
        df = df.sort_index(ascending=True)
        
        # 重命名列以匹配StockBreakoutAnalyzer的期望格式
        df = df.rename(columns={
            'open': 'open',
            'high': 'high', 
            'low': 'low',
            'close': 'close',
            'volume': 'turnover'  # 注意：新浪返回的是成交量，需要转换为成交额
        })
        
        # 将成交量转换为成交额（单位：万元）
        # 假设每手100股，转换为股数
        df['turnover'] = df['turnover'] * 100
        df['turnover'] = df['turnover'] * df['close'] / 1e4  # 转换为成交额（万元）
        
        # 只保留需要的列
        df = df[['open', 'high', 'low', 'close', 'turnover']]
        
        # 如果启用实时数据，尝试获取最新价格
        if realtime:
            try:
                # 获取实时数据（分钟级别）
                realtime_data = api.get_kline_data(stock_code, scale='5min', ma=5, datalen=10)
                if realtime_data:
                    realtime_df = api.to_dataframe(realtime_data)
                    if not realtime_df.empty:
                        latest_price = realtime_df['close'].iloc[-1]
                        latest_time = realtime_df.index[-1]
                        print(f"📈 实时数据: {latest_time} 最新价格: {latest_price:.2f} 元")
            except Exception as e:
                print(f"⚠️  实时数据获取失败: {e}")
        
        print(f"✅ 成功获取 {len(df)} 天数据")
        print(f"   数据范围: {df.index[0]} 到 {df.index[-1]}")
        print(f"   最新价格: {df['close'].iloc[-1]:.2f} 元")
        
        return df
        
    except ImportError:
        print("⚠️  无法导入新浪财经API，请确保sina_kline_api.py在API目录下")
        return create_fallback_data(days)
    except Exception as e:
        print(f"❌ 获取真实数据失败: {e}")
        print("📊 将使用模拟数据进行备用...")
        return create_fallback_data(days)

# 实时数据更新函数
def update_stock_data(analyzer, stock_code='300433', realtime=False):
    """
    更新股票数据到最新状态
    
    Args:
        analyzer: StockBreakoutAnalyzer实例
        stock_code: 股票代码
        realtime: 是否包含实时数据
    
    Returns:
        bool: 更新是否成功
    """
    print(f"🔄 正在更新 {stock_code} 数据...")
    
    try:
        # 获取最新数据（默认获取最近30天）
        new_data = get_real_stock_data(stock_code, days=100, realtime=realtime)
        
        # 更新分析器数据
        analyzer.data = new_data
        analyzer.data_valid = True
        
        # 重新验证数据
        analyzer._validate_data()
        
        print("✅ 数据更新成功")
        return True
        
    except Exception as e:
        print(f"❌ 数据更新失败: {e}")
        return False

def create_fallback_data(days=100):
    """
    创建备用模拟数据
    
    Args:
        days: 数据天数
    
    Returns:
        pd.DataFrame: 模拟的OHLC数据
    """
    import pandas as pd
    import numpy as np
    
    print("📊 创建模拟数据作为备用...")
    
    # 创建日期索引
    dates = pd.date_range('2025-08-01', periods=days, freq='B')  # 工作日
    np.random.seed(42)
    
    # 模拟价格走势：先震荡，后突破上涨
    base_price = 30
    noise = np.random.randn(days) * 1.5
    
    # 趋势部分
    trend = np.zeros(days)
    trend[days//2:] = np.linspace(0, 15, days - days//2)  # 后半段上涨趋势
    
    prices = base_price + trend + noise.cumsum() * 0.1
    prices = np.maximum(prices, 25)  # 确保价格为正
    
    # 生成OHLC数据
    data = pd.DataFrame(index=dates)
    data['open'] = prices * (1 + np.random.randn(days) * 0.01)
    data['high'] = data['open'] * (1 + np.random.rand(days) * 0.03)
    data['low'] = data['open'] * (1 - np.random.rand(days) * 0.03)
    data['close'] = prices
    data['turnover'] = np.random.rand(days) * 1e9 + 5e8  # 模拟成交量
    
    # 创建几个明显的阻力位
    resistance_points = [35.0, 38.0, 40.0, 42.0]
    for i, resistance in enumerate(resistance_points):
        idx = np.argmin(np.abs(data['high'].values - resistance))
        if idx < len(data) - 5:
            # 在阻力位附近创建高点
            for j in range(3):
                if idx + j < len(data):
                    data.loc[data.index[idx + j], 'high'] = resistance + np.random.rand() * 0.5
    
    print(f"📊 模拟数据创建完成: {len(data)} 天")
    print(f"   最新价格: {data['close'].iloc[-1]:.2f} 元")
    
    return data

# 完整分析流程函数
def run_complete_analysis(stock_code='300433', days=100, save_chart=True, realtime=False):
    """
    运行完整的突破形态分析流程
    
    Args:
        stock_code: 股票代码
        days: 分析天数
        save_chart: 是否保存图表
        realtime: 是否包含实时数据
    
    Returns:
        tuple: (分析器实例, 分析结果, 图表对象)
    """
    print("🚀 开始完整突破形态分析流程")
    print("=" * 80)
    
    # 1. 获取真实数据
    data = get_real_stock_data(stock_code, days, realtime=realtime)
    
    # 2. 创建分析器
    analyzer = StockBreakoutAnalyzer(data)
    
    # 3. 运行完整分析
    result = analyzer.detect_complete_pattern_v5()
    
    # 4. 生成报告
    report = analyzer.generate_report_v5(result, stock_code)
    print(report)
    
    # 5. 绘制图表
    if save_chart:
        chart_path = f'output/{stock_code}_breakout_analysis_v5.png'
        fig = analyzer.plot_analysis_v5(result, save_path=chart_path)
    else:
        fig = analyzer.plot_analysis_v5(result)
    
    print("✅ 完整分析流程完成！")
    
    return analyzer, result, fig
# 批量分析函数
def batch_analysis(stock_codes, days=100, save_chart=True, realtime=False):
    """
    批量分析多个股票
    
    Args:
        stock_codes: 股票代码列表
        days: 分析天数
        save_chart: 是否保存图表
        realtime: 是否包含实时数据
    
    Returns:
        dict: 各股票的分析结果
    """
    print("🚀 开始批量股票分析")
    print("=" * 80)
    
    results = {}
    
    for i, stock_code in enumerate(stock_codes, 1):
        print(f"\n📊 分析第{i}/{len(stock_codes)}个股票: {stock_code}")
        print("-" * 50)
        
        try:
            # 运行完整分析
            analyzer, result, fig = run_complete_analysis(
                stock_code=stock_code,
                days=days,
                save_chart=save_chart,
                realtime=realtime
            )
            
            results[stock_code] = {
                'analyzer': analyzer,
                'result': result,
                'fig': fig,
                'status': 'success'
            }
            
            print(f"✅ {stock_code} 分析完成")
            
        except Exception as e:
            print(f"❌ {stock_code} 分析失败: {e}")
            results[stock_code] = {
                'status': 'failed',
                'error': str(e)
            }
    
    # 生成批量分析报告
    print("\n" + "=" * 80)
    print("📊 批量分析汇总报告")
    print("=" * 80)
    
    success_count = sum(1 for r in results.values() if r['status'] == 'success')
    failed_count = len(stock_codes) - success_count
    
    print(f"✅ 成功分析: {success_count} 个股票")
    print(f"❌ 分析失败: {failed_count} 个股票")
    
    if success_count > 0:
        print("\n📈 成功分析股票详情:")
        for stock_code, result_data in results.items():
            if result_data['status'] == 'success':
                result = result_data['result']
                pattern_score = result.get('pattern_score', 0)
                pattern_rating = result.get('pattern_rating', '未知')
                print(f"   {stock_code}: 形态评分 {pattern_score}/100 - {pattern_rating}")
    
    if failed_count > 0:
        print("\n📉 分析失败股票:")
        for stock_code, result_data in results.items():
            if result_data['status'] == 'failed':
                print(f"   {stock_code}: {result_data['error']}")
    
    print("\n✅ 批量分析完成！")
    
    return results
# 命令行参数解析
def parse_arguments():
    """
    解析命令行参数
    
    Returns:
        argparse.Namespace: 解析后的参数对象
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='修正版突破形态识别系统 V5 - 真实行情数据集成版',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  python StockBreakoutAnalyzer.py                    # 默认分析300433，100天
  python StockBreakoutAnalyzer.py -s 600519 -d 200  # 分析600519，200天
  python StockBreakoutAnalyzer.py -s 000001 -d 50 -n # 不保存图表
  python StockBreakoutAnalyzer.py -h                 # 显示帮助信息
        '''
    )
    
    parser.add_argument(
        '-s', '--stock', 
        type=str, 
        default='300433',
        help='股票代码 (默认: 300433)'
    )
    
    parser.add_argument(
        '-d', '--days', 
        type=int, 
        default=100,
        help='分析天数 (默认: 100)'
    )
    
    parser.add_argument(
        '-n', '--no-save', 
        action='store_true',
        help='不保存图表文件'
    )
    
    parser.add_argument(
        '-v', '--verbose', 
        action='store_true',
        help='详细输出模式'
    )
    
    parser.add_argument(
        '-r', '--realtime', 
        action='store_true',
        help='包含实时数据（分钟级别）'
    )
    
    parser.add_argument(
        '-b', '--batch', 
        type=str,
        help='批量分析股票代码，用逗号分隔，如：000001,600519,300433'
    )
    
    return parser.parse_args()

# 主函数
def main():
    """主函数 - 支持命令行参数"""
    args = parse_arguments()
    
    print("修正版突破形态识别系统 V5 - 真实行情数据集成版")
    print("=" * 80)
    print("主要功能:")
    print("1. ✅ 集成新浪财经真实行情数据")
    print("2. ✅ 支持自定义股票代码和分析天数")
    print("3. ✅ 完整的突破形态分析流程")
    print("4. ✅ 自动生成详细报告和图表")
    print("5. ✅ 模拟数据备用机制")
    print("=" * 80)
    
    print(f"\n📊 分析配置:")
    print(f"   股票代码: {args.stock}")
    print(f"   分析天数: {args.days}")
    print(f"   保存图表: {'否' if args.no_save else '是'}")
    print(f"   实时数据: {'是' if args.realtime else '否'}")
    
    # 检查批量分析模式
    if args.batch:
        stock_codes = [code.strip() for code in args.batch.split(',')]
        print(f"   批量分析: {len(stock_codes)} 个股票")
        print("=" * 80)
        
        try:
            # 运行批量分析
            results = batch_analysis(
                stock_codes=stock_codes,
                days=args.days,
                save_chart=not args.no_save,
                realtime=args.realtime
            )
            
        except KeyboardInterrupt:
            print("\n❌ 用户中断批量分析")
        except Exception as e:
            print(f"\n❌ 批量分析过程中出现错误: {e}")
            print("请检查网络连接或API配置")
            if args.verbose:
                import traceback
                traceback.print_exc()
    else:
        print(f"   批量分析: 否")
        print("=" * 80)
        
        try:
            # 运行单股票分析
            analyzer, result, fig = run_complete_analysis(
                stock_code=args.stock, 
                days=args.days, 
                save_chart=not args.no_save,
                realtime=args.realtime
            )
            
            print("\n✅ 分析完成！")
            
            if args.verbose:
                print("\n📊 详细分析结果:")
                print(f"   压力集群数量: {len(result.get('pressure_clusters', []))}")
                print(f"   支撑位数量: {len(result.get('support_levels', []))}")
                print(f"   形态评分: {result.get('pattern_score', 0):.1f}")
                print(f"   突破状态: {result.get('breakout_status', '未知')}")
            
        except KeyboardInterrupt:
            print("\n❌ 用户中断分析")
        except Exception as e:
            print(f"\n❌ 分析过程中出现错误: {e}")
            print("请检查网络连接或API配置")
            if args.verbose:
                import traceback
                traceback.print_exc()
    
    print("\n📋 使用方法:")
    print("1. 直接运行: python StockBreakoutAnalyzer.py")
    print("2. 自定义分析: python StockBreakoutAnalyzer.py -s 股票代码 -d 天数")
    print("3. 批量分析: python StockBreakoutAnalyzer.py -b 000001,600519,300433")
    print("4. 不保存图表: python StockBreakoutAnalyzer.py -n")
    print("5. 详细模式: python StockBreakoutAnalyzer.py -v")
    print("6. 实时数据: python StockBreakoutAnalyzer.py -r")
if __name__ == "__main__":
    main()