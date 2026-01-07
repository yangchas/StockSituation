import numpy as np
import csv
import json
import os
import time
import random
import asyncio
from collections import defaultdict
from typing import Dict, List, Set
import logging

from redis_storage import RedisStorageManager

logger = logging.getLogger(__name__)

class LazyPlateUpdater:
    """
    优化版板块更新器 - 基于变化的增量更新
    """
    
    def __init__(self, plate_file: str, relation_file: str, redis_url: str = "redis://localhost:6379"):
        self.plate_file = plate_file
        self.relation_file = relation_file
        self.redis_storage = RedisStorageManager(redis_url)
        
        # 加载基础数据
        self.load_plate_data()
        self.load_stock_relations()
        self.build_plate_hierarchy()
        
        # 初始化主流板块的指标计算结构
        self.initialize_main_plate_structures()
        
        # 增量更新相关
        self.last_stock_data = {}  # 记录上次股票数据 {stock_id: data}
        self.dirty_plates = set()  # 标记需要更新的板块
        self.last_refresh_time = 0  # 上次刷新时间
        
        logger.info(f"🚀 优化版板块更新器初始化完成: {len(self.main_plates)}个主板块, {len(self.all_plates)}个总板块")
    
    def load_plate_data(self):
        """加载板块数据"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.all_plates = {}
        self.main_plates = {}
        self.plate_name_to_id = {}
        
        plate_path = os.path.join(script_dir, self.plate_file)
        with open(plate_path, 'r', encoding='gbk') as f:
            reader = csv.DictReader(f)
            for row in reader:
                plate_id = row['id']
                plate_name = row['name']
                market_cap = float(row['流通值']) if row['流通值'] else 0.0
                
                inner_data = []
                inner_str = row.get('inner', '').strip()
                if inner_str and inner_str != '[]':
                    try:
                        inner_str = inner_str.replace("'", '"')
                        inner_list = json.loads(inner_str)
                        inner_data = inner_list
                    except json.JSONDecodeError as e:
                        logger.warning(f"解析inner字段失败 {plate_id}: {e}")
                
                self.all_plates[plate_id] = {
                    'name': plate_name,
                    'market_cap': market_cap,
                    'inner': inner_data,
                    'type': 'main'
                }
                self.plate_name_to_id[plate_name] = plate_id
                self.main_plates[plate_id] = self.all_plates[plate_id]
                
                for inner_item in inner_data:
                    if isinstance(inner_item, list) and len(inner_item) >= 2:
                        sub_plate_id = inner_item[0]
                        sub_plate_name = inner_item[1]
                        
                        if sub_plate_id not in self.all_plates:
                            self.all_plates[sub_plate_id] = {
                                'name': sub_plate_name,
                                'market_cap': 0,
                                'inner': [],
                                'type': 'sub',
                                'parent': plate_id
                            }
                            self.plate_name_to_id[sub_plate_name] = sub_plate_id
    
    def load_stock_relations(self):
        """加载个股-板块关系"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.stock_to_plates = defaultdict(list)
        self.plate_to_stocks = defaultdict(list)
        
        relation_path = os.path.join(script_dir, self.relation_file)
        
        try:
            with open(relation_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)
                
                row_count = 0
                valid_count = 0
                
                for row in reader:
                    row_count += 1
                    if len(row) >= 2:
                        plate_id, stock_id = row[0], row[1]
                        
                        if plate_id in self.all_plates:
                            self.stock_to_plates[stock_id].append(plate_id)
                            self.plate_to_stocks[plate_id].append(stock_id)
                            valid_count += 1
                        else:
                            if row_count <= 10:
                                logger.warning(f"⚠️ 忽略无效板块ID: {plate_id} (股票: {stock_id})")
                
                logger.info(f"📈 加载个股关系: {row_count}行数据, {valid_count}个有效关系")
                logger.info(f"📊 股票-板块映射: {len(self.stock_to_plates)}只股票, {len(self.plate_to_stocks)}个板块有股票")
                
        except FileNotFoundError:
            logger.error(f"❌ 个股板块关系文件不存在: {relation_path}")
        except Exception as e:
            logger.error(f"❌ 加载个股关系失败: {e}")
    
    def build_plate_hierarchy(self):
        """构建板块层级关系"""
        self.plate_hierarchy = {}
        
        for plate_id, plate_info in self.main_plates.items():
            inner_data = plate_info.get('inner', [])
            if inner_data:
                self.plate_hierarchy[plate_id] = []
                for inner_item in inner_data:
                    if isinstance(inner_item, list) and len(inner_item) >= 2:
                        sub_plate_id = inner_item[0]
                        self.plate_hierarchy[plate_id].append(sub_plate_id)
    
    def initialize_main_plate_structures(self):
        """只为主流板块初始化计算结构"""
        self.main_plate_ids = list(self.main_plates.keys())
        self.main_plate_to_idx = {pid: i for i, pid in enumerate(self.main_plate_ids)}
        
        n_main_plates = len(self.main_plate_ids)
        self.main_section_sum_change = np.zeros(n_main_plates, dtype=np.float32)
        self.main_section_total_volume = np.zeros(n_main_plates, dtype=np.int32)
        self.main_section_total_large_net = np.zeros(n_main_plates, dtype=np.int32)
        self.main_section_rise_count = np.zeros(n_main_plates, dtype=np.int16)
        self.main_section_fall_count = np.zeros(n_main_plates, dtype=np.int16)
        
        self.stock_current_change = {}
        self.stock_current_volume = {}
        self.stock_current_large_net = {}
        
        self.main_plate_stock_count = np.zeros(n_main_plates, dtype=np.int16)
        for stock_id, plate_list in self.stock_to_plates.items():
            for plate_id in plate_list:
                if plate_id in self.main_plate_to_idx:
                    idx = self.main_plate_to_idx[plate_id]
                    self.main_plate_stock_count[idx] += 1
    
    def refresh_stock_data_from_redis(self) -> int:
        """
        增量刷新股票数据 - 只更新有变化的股票
        返回有变化的股票数量
        """
        refresh_start = time.time()
        changed_stocks = 0
        
        # 重置脏板块标记
        self.dirty_plates.clear()
        
        # 检查是否是第一次运行，需要初始化板块数据
        is_first_run = len(self.last_stock_data) == 0
        
        for stock_id in self.stock_to_plates.keys():
            try:
                current_data = self.redis_storage.get_stock_data(stock_id)
                if not current_data:
                    continue
                    
                # 检查数据是否变化
                last_data = self.last_stock_data.get(stock_id, {})
                if self._has_stock_changed(last_data, current_data) or is_first_run:
                    changed_stocks += 1
                    # 标记相关板块为脏
                    for plate_id in self.stock_to_plates[stock_id]:
                        self.dirty_plates.add(plate_id)
                    
                    # 更新股票当前状态
                    self._update_stock_metrics(stock_id, current_data, last_data)
                
                self.last_stock_data[stock_id] = current_data
                    
            except Exception as e:
                logger.warning(f"⚠️ 处理股票数据失败 {stock_id}: {e}")
        
        # 如果是第一次运行，强制更新所有板块
        if is_first_run and self.dirty_plates:
            # 初始化板块指标数组
            self._initialize_plate_metrics_from_redis()
            logger.info(f"🎯 首次运行: 初始化 {len(self.dirty_plates)} 个板块数据")
        
        # 只有有变化时才更新Redis
        if self.dirty_plates:
            self._update_dirty_plates_to_redis()
        
        refresh_time = (time.time() - refresh_start) * 1000
        if changed_stocks > 0:
            logger.info(f"🔄 增量刷新完成: {changed_stocks}只股票变化, {len(self.dirty_plates)}个板块更新, 耗时: {refresh_time:.2f}ms")
        
        self.last_refresh_time = time.time()
        return changed_stocks

    def _initialize_plate_metrics_from_redis(self):
        """从Redis中初始化板块指标数据"""
        logger.info("🔄 从Redis初始化板块指标数据...")
        
        # 重置所有板块指标
        n_main_plates = len(self.main_plate_ids)
        self.main_section_sum_change = np.zeros(n_main_plates, dtype=np.float32)
        self.main_section_total_volume = np.zeros(n_main_plates, dtype=np.int32)
        self.main_section_total_large_net = np.zeros(n_main_plates, dtype=np.int32)
        self.main_section_rise_count = np.zeros(n_main_plates, dtype=np.int16)
        self.main_section_fall_count = np.zeros(n_main_plates, dtype=np.int16)
        self.main_plate_stock_count = np.zeros(n_main_plates, dtype=np.int16)  # 重置股票计数
        
        # 遍历所有股票，从Redis获取当前数据并初始化板块指标
        for stock_id, plate_list in self.stock_to_plates.items():
            try:
                current_data = self.redis_storage.get_stock_data(stock_id)
                if not current_data:
                    continue
                    
                # 只更新主流板块
                main_plate_indices = []
                for plate_id in plate_list:
                    if plate_id in self.main_plate_to_idx:
                        main_plate_indices.append(self.main_plate_to_idx[plate_id])
                
                if not main_plate_indices:
                    continue
                    
                main_plate_indices = np.array(main_plate_indices, dtype=np.int32)
                
                # 获取当前值
                change = current_data.get('change_pct', 0.0)
                volume = current_data.get('volume', 0)
                large_net = current_data.get('large_net', 0)
                
                # 初始化板块指标
                self.main_section_sum_change[main_plate_indices] += change
                self.main_section_total_volume[main_plate_indices] += volume
                self.main_section_total_large_net[main_plate_indices] += large_net
                
                # 初始化股票计数
                for idx in main_plate_indices:
                    self.main_plate_stock_count[idx] += 1
                
                # 初始化涨跌计数
                for idx in main_plate_indices:
                    if change > 0:
                        self.main_section_rise_count[idx] += 1
                    elif change < 0:
                        self.main_section_fall_count[idx] += 1
                        
            except Exception as e:
                logger.warning(f"⚠️ 初始化股票数据失败 {stock_id}: {e}")
        
        logger.info("✅ 板块指标数据初始化完成")

    def _has_stock_changed(self, old_data: Dict, new_data: Dict) -> bool:
        """检查股票数据是否发生有意义的变化"""
        if not old_data:
            return True
            
        # 只关注关键字段的变化
        key_fields = ['change_pct', 'volume', 'large_net', 'price']
        for field in key_fields:
            old_val = old_data.get(field, 0)
            new_val = new_data.get(field, 0)
            
            if field == 'change_pct' and abs(new_val - old_val) > 0.01:  # 变化超过0.01%
                return True
            elif field == 'volume' and abs(new_val - old_val) > new_val * 0.05:  # 成交量变化5%
                return True
            elif field == 'large_net' and abs(new_val - old_val) > 10000:  # 大单净额变化超过1万
                return True
                
        return False
    
    def _update_stock_metrics(self, stock_id: str, current_data: Dict, last_data: Dict):
        """更新股票指标到板块计算"""
        plate_list = self.stock_to_plates[stock_id]
        
        # 只更新主流板块
        main_plate_indices = []
        for plate_id in plate_list:
            if plate_id in self.main_plate_to_idx:
                main_plate_indices.append(self.main_plate_to_idx[plate_id])
        
        if not main_plate_indices:
            return
            
        main_plate_indices = np.array(main_plate_indices, dtype=np.int32)
        
        # 获取新旧值
        new_change = current_data.get('change_pct', 0.0)
        old_change = last_data.get('change_pct', 0.0)
        delta_change = new_change - old_change
        
        new_volume = current_data.get('volume', 0)
        old_volume = last_data.get('volume', 0)
        delta_volume = new_volume - old_volume
        
        new_large_net = current_data.get('large_net', 0)
        old_large_net = last_data.get('large_net', 0)
        delta_large_net = new_large_net - old_large_net
        
        # 更新板块指标
        self.main_section_sum_change[main_plate_indices] += delta_change
        self.main_section_total_volume[main_plate_indices] += delta_volume
        self.main_section_total_large_net[main_plate_indices] += delta_large_net
        
        # 更新涨跌计数
        for idx in main_plate_indices:
            # 先移除旧的涨跌状态
            if old_change > 0:
                self.main_section_rise_count[idx] -= 1
            elif old_change < 0:
                self.main_section_fall_count[idx] -= 1
            
            # 添加新的涨跌状态
            if new_change > 0:
                self.main_section_rise_count[idx] += 1
            elif new_change < 0:
                self.main_section_fall_count[idx] += 1
        
        # 更新当前状态
        self.stock_current_change[stock_id] = new_change
        self.stock_current_volume[stock_id] = new_volume
        self.stock_current_large_net[stock_id] = new_large_net
    
    def _update_dirty_plates_to_redis(self):
        """将脏板块指标更新到Redis"""
        plate_metrics = {}
        
        for plate_id in self.dirty_plates:
            if plate_id in self.main_plate_to_idx:
                # 主流板块从预计算数组获取
                idx = self.main_plate_to_idx[plate_id]
                count = self.main_plate_stock_count[idx]
                
                if count > 0:
                    metrics = {
                        "change_pct": float(self.main_section_sum_change[idx] / count),
                        "total_volume": int(self.main_section_total_volume[idx]),
                        "total_large_net": int(self.main_section_total_large_net[idx]),
                        "rise_count": int(self.main_section_rise_count[idx]),
                        "fall_count": int(self.main_section_fall_count[idx]),
                        "stock_count": int(count),
                        "timestamp": int(time.time())
                    }
                else:
                    metrics = {
                        "change_pct": 0.0,
                        "total_volume": 0,
                        "total_large_net": 0,
                        "rise_count": 0,
                        "fall_count": 0,
                        "stock_count": 0,
                        "timestamp": int(time.time())
                    }
            else:
                # 非主流板块实时计算
                metrics = self._calculate_plate_metrics_lazy(plate_id)
                if metrics:
                    metrics = {
                        "change_pct": metrics.get('change_pct', 0),
                        "total_volume": metrics.get('total_volume', 0),
                        "total_large_net": metrics.get('total_large_net', 0),
                        "rise_count": metrics.get('rise_count', 0),
                        "fall_count": metrics.get('fall_count', 0),
                        "stock_count": metrics.get('stock_count', 0),
                        "timestamp": int(time.time())
                    }
            
            if metrics:
                plate_metrics[plate_id] = metrics
        
        # 批量更新到Redis
        if plate_metrics:
            self.redis_storage.batch_update_plates(plate_metrics)
            logger.debug(f"💾 更新 {len(plate_metrics)} 个脏板块到Redis")
    
    def get_plate_metrics(self, plate_id: str) -> Dict:
        """获取板块指标 - 懒加载版本"""
        # 首先尝试从Redis获取
        plate_data = self.redis_storage.get_plate_data(plate_id)
        
        if plate_data and self._is_plate_data_valid(plate_data):
            return self._format_plate_metrics(plate_id, plate_data)
        
        # Redis中没有有效数据，实时计算
        return self._calculate_plate_metrics_lazy(plate_id)

    def _is_plate_data_valid(self, plate_data: Dict) -> bool:
        """检查板块数据是否有效"""
        try:
            timestamp = plate_data.get('timestamp', 0)
            current_time = int(time.time())
            
            # 如果数据超过60秒，认为已过期
            if current_time - timestamp > 60:
                return False
            
            required_fields = ['change_pct', 'total_volume', 'rise_count', 'fall_count']
            for field in required_fields:
                if field not in plate_data:
                    return False
            
            return True
        except Exception:
            return False
    
    def _format_plate_metrics(self, plate_id: str, plate_data: Dict) -> Dict:
        """格式化板块指标数据"""
        plate_info = self.all_plates.get(plate_id, {})
        return {
            'id': plate_id,
            'name': plate_info.get('name', '未知板块'),
            'change_pct': plate_data.get('change_pct', 0.0),
            'total_volume': plate_data.get('total_volume', 0),
            'total_large_net': plate_data.get('total_large_net', 0),
            'rise_count': plate_data.get('rise_count', 0),
            'fall_count': plate_data.get('fall_count', 0),
            'stock_count': plate_data.get('stock_count', 0),
            'type': plate_info.get('type', 'unknown'),
            'market_cap': plate_info.get('market_cap', 0),
            'timestamp': plate_data.get('timestamp', int(time.time()))
        }
    
    def _calculate_plate_metrics_lazy(self, plate_id: str) -> Dict:
        """懒加载计算板块指标"""
        plate_info = self.all_plates.get(plate_id)
        if not plate_info:
            return None
        
        stock_ids = self.plate_to_stocks.get(plate_id, [])
        stock_count = len(stock_ids)
        
        if stock_count == 0:
            metrics = {
                'id': plate_id,
                'name': plate_info['name'],
                'change_pct': 0.0,
                'total_volume': 0,
                'total_large_net': 0,
                'rise_count': 0,
                'fall_count': 0,
                'stock_count': 0,
                'type': plate_info['type'],
                'market_cap': plate_info.get('market_cap', 0),
                'timestamp': int(time.time())
            }
        else:
            total_change = 0.0
            total_volume = 0
            total_large_net = 0
            rise_count = 0
            fall_count = 0
            valid_stock_count = 0
            
            for stock_id in stock_ids:
                stock_data = self.redis_storage.get_stock_data(stock_id)
                if not stock_data:
                    continue
                    
                change = stock_data.get('change_pct', 0.0)
                volume = stock_data.get('volume', 0)
                large_net = stock_data.get('large_net', 0)
                
                total_change += change
                total_volume += volume
                total_large_net += large_net
                
                if change > 0:
                    rise_count += 1
                elif change < 0:
                    fall_count += 1
                    
                valid_stock_count += 1
            
            if valid_stock_count > 0:
                avg_change = total_change / valid_stock_count
            else:
                avg_change = 0.0
            
            metrics = {
                'id': plate_id,
                'name': plate_info['name'],
                'change_pct': avg_change,
                'total_volume': total_volume,
                'total_large_net': total_large_net,
                'rise_count': rise_count,
                'fall_count': fall_count,
                'stock_count': valid_stock_count,
                'type': plate_info['type'],
                'market_cap': plate_info.get('market_cap', 0),
                'timestamp': int(time.time())
            }
        
        # 缓存到Redis
        redis_metrics = {
            "change_pct": metrics['change_pct'],
            "total_volume": metrics['total_volume'],
            "total_large_net": metrics['total_large_net'],
            "rise_count": metrics['rise_count'],
            "fall_count": metrics['fall_count'],
            "stock_count": metrics['stock_count'],
            "timestamp": metrics['timestamp']
        }
        self.redis_storage.update_plate_metrics(plate_id, redis_metrics)
        
        logger.debug(f"🔄 懒加载计算板块: {plate_info['name']} ({metrics['stock_count']}只有效股票)")
        return metrics
    
    def get_all_plate_metrics(self) -> List[Dict]:
        """获取所有板块指标"""
        all_metrics = []
        
        for plate_id in self.all_plates:
            metrics = self.get_plate_metrics(plate_id)
            if metrics:
                all_metrics.append(metrics)
        
        return all_metrics
    
    def get_main_plates_metrics(self) -> List[Dict]:
        """获取主流板块指标"""
        main_metrics = []
        for plate_id in self.main_plate_ids:
            metrics = self.get_plate_metrics(plate_id)
            if metrics:
                main_metrics.append(metrics)
        
        return main_metrics
    
    def get_sub_plates_metrics(self, main_plate_name: str) -> List[Dict]:
        """获取子板块指标 - 懒加载版本"""
        main_plate_id = self.plate_name_to_id.get(main_plate_name)
        if not main_plate_id:
            logger.warning(f"❌ 未找到主板块: {main_plate_name}")
            return []
        
        sub_plate_ids = self.plate_hierarchy.get(main_plate_id, [])
        sub_metrics = []
        
        for sub_plate_id in sub_plate_ids:
            metrics = self.get_plate_metrics(sub_plate_id)
            if metrics:
                sub_metrics.append(metrics)
        
        return sub_metrics
    
    def get_plate_stocks(self, plate_id: str) -> List[Dict]:
        """获取板块个股数据 - 从Redis获取实际数据，包含高级指标"""
        stock_ids = self.plate_to_stocks.get(plate_id, [])
        
        if not stock_ids:
            return []
        
        stocks = []
        found_count = 0
        
        # 批量获取股票基础数据
        for stock_id in stock_ids:
            stock_data = self.redis_storage.get_stock_data(stock_id)
            
            if stock_data:
                # 获取高级指标
                advanced_indicators = self.redis_storage.get_stock_advanced_indicators(stock_id)
                
                formatted_stock = {
                    'code': stock_id,
                    'name': stock_data.get('name', f"股票{stock_id}"),
                    'change_pct': stock_data.get('change_pct', 0.0),
                    'price': stock_data.get('price', 0.0),
                    'volume': stock_data.get('volume', 0),
                    'market_cap': stock_data.get('market_cap', 0),
                    'large_net': stock_data.get('large_net', 0),
                    'timestamp': stock_data.get('timestamp', 0),
                    # 添加高级指标
                    'advanced_indicators': advanced_indicators
                }
                stocks.append(formatted_stock)
                found_count += 1
        
        # 按涨跌幅排序
        stocks.sort(key=lambda x: x.get('change_pct', 0), reverse=True)
        
        logger.debug(f"📊 获取板块 {plate_id} 个股: {found_count}/{len(stock_ids)} 只股票, 包含高级指标: {len([s for s in stocks if s.get('advanced_indicators')])} 只")
        return stocks
    
    def get_plate_hierarchy(self):
        """获取板块层级关系"""
        return self.plate_hierarchy, list(self.main_plates.keys())
    
    def debug_plate_stocks(self, plate_id: str):
        """调试板块个股数据状态"""
        logger.info(f"🔍 调试板块 {plate_id} 的个股数据")
        
        plate_info = self.all_plates.get(plate_id)
        if not plate_info:
            logger.error(f"❌ 板块 {plate_id} 不存在")
            return
        
        logger.info(f"📋 板块信息: {plate_info['name']} (类型: {plate_info['type']})")
        
        stock_ids = self.plate_to_stocks.get(plate_id, [])
        logger.info(f"📦 个股关系: {len(stock_ids)} 只股票")
        
        sample_stocks = stock_ids[:5]
        for stock_id in sample_stocks:
            stock_data = self.redis_storage.get_stock_data(stock_id)
            if stock_data:
                logger.info(f"✅ 股票 {stock_id}: 有Redis数据 (涨跌幅: {stock_data.get('change_pct', 0):.2%})")
            else:
                logger.info(f"❌ 股票 {stock_id}: 无Redis数据")
        
        stocks = self.get_plate_stocks(plate_id)
        logger.info(f"📊 最终返回: {len(stocks)} 只股票数据")
class OptimizedPlateUpdater(LazyPlateUpdater):
    """优化版板块更新器 - 批量获取数据"""
    
    def __init__(self, plate_file: str, relation_file: str, redis_url: str = "redis://localhost:6379"):
        super().__init__(plate_file, relation_file, redis_url)
        self.all_stocks_cache = {}  # 缓存所有个股数据
        self.last_all_stocks_update = 0
        self.cache_ttl = 5  # 缓存5秒
    
    def refresh_all_stocks_data(self) -> Dict[str, Dict]:
        """批量刷新所有个股数据"""
        current_time = time.time()
        
        # 检查缓存是否有效
        if (current_time - self.last_all_stocks_update) < self.cache_ttl and self.all_stocks_cache:
            return self.all_stocks_cache
        
        # 检查是否是第一次运行，需要初始化板块数据
        is_first_run = len(self.last_stock_data) == 0
        
        # 获取所有股票代码
        all_stock_ids = list(self.stock_to_plates.keys())
        
        if not all_stock_ids:
            return {}
        
        # 批量获取所有股票数据
        all_stocks_data = {}
        batch_size = 500  # 分批处理，避免内存过大
        
        for i in range(0, len(all_stock_ids), batch_size):
            batch_ids = all_stock_ids[i:i + batch_size]
            
            # 批量获取基础数据
            for stock_id in batch_ids:
                stock_data = self.redis_storage.get_stock_data(stock_id)
                if stock_data:
                    # 格式化数据为basic和advanced结构，并转换数据类型
                    formatted_data = {
                        'basic': {
                            'name': stock_data.get('name', f'股票{stock_id}'),
                            'price': float(stock_data.get('price', 0.0)),
                            'change_pct': float(stock_data.get('change_pct', 0.0)),
                            'volume': int(stock_data.get('volume', 0)),
                            'market_cap': int(stock_data.get('market_cap', 0)),
                            'large_net': int(stock_data.get('large_net', 0)),
                            'timestamp': int(stock_data.get('timestamp', int(time.time())))
                        },
                        'advanced': {
                            'change_rate_1min': float(stock_data.get('change_rate_1min', 0.0)),
                            'amount_2min': int(stock_data.get('amount_2min', 0))
                        }
                    }
                    all_stocks_data[stock_id] = formatted_data
                    
                    # 如果是第一次运行，标记相关板块为脏
                    if is_first_run:
                        for plate_id in self.stock_to_plates[stock_id]:
                            self.dirty_plates.add(plate_id)
                    
                    # 更新股票当前数据
                    self.last_stock_data[stock_id] = formatted_data
        
        self.all_stocks_cache = all_stocks_data
        self.last_all_stocks_update = current_time
        
        # 如果是第一次运行，强制更新所有板块
        if is_first_run and self.dirty_plates:
            # 初始化板块指标数组
            self._initialize_plate_metrics_from_redis()
            logger.info(f"🎯 首次运行: 初始化 {len(self.dirty_plates)} 个板块数据")
            
            # 更新Redis中的板块数据
            self._update_dirty_plates_to_redis()
        
        logger.debug(f"🔄 批量刷新 {len(all_stocks_data)} 只股票数据")
        return all_stocks_data
    
    def get_all_plate_metrics_optimized(self) -> List[Dict]:
        """优化版获取所有板块指标"""
        # 先批量获取所有个股数据
        all_stocks_data = self.refresh_all_stocks_data()
        
        # 初始化板块统计
        plate_stats = {}
        for plate_id in self.all_plates:
            plate_stats[plate_id] = {
                'total_change': 0.0,
                'total_volume': 0,
                'total_large_net': 0,
                'rise_count': 0,
                'fall_count': 0,
                'stock_count': 0,
                'valid_stocks': 0,
                # 高级指标
                'change_rates_1min': [],
                'amounts_2min': []
            }
        
        # 遍历所有个股，累加到对应板块
        for stock_id, stock_data in all_stocks_data.items():
            basic_data = stock_data.get('basic', {})
            advanced_data = stock_data.get('advanced', {})
            
            # 获取该股票所属的所有板块
            plate_ids = self.stock_to_plates.get(stock_id, [])
            
            for plate_id in plate_ids:
                if plate_id not in plate_stats:
                    continue
                    
                stats = plate_stats[plate_id]
                
                # 基础指标
                change_pct = basic_data.get('change_pct', 0.0)
                volume = basic_data.get('volume', 0)
                large_net = basic_data.get('large_net', 0)
                
                stats['total_change'] += change_pct
                stats['total_volume'] += volume
                stats['total_large_net'] += large_net
                stats['stock_count'] += 1
                
                if change_pct > 0:
                    stats['rise_count'] += 1
                elif change_pct < 0:
                    stats['fall_count'] += 1
                
                # 高级指标
                change_rate_1min = advanced_data.get('change_rate_1min')
                amount_2min = advanced_data.get('amount_2min')
                
                if change_rate_1min is not None:
                    stats['change_rates_1min'].append(change_rate_1min)
                    stats['valid_stocks'] += 1
                
                if amount_2min is not None:
                    stats['amounts_2min'].append(amount_2min)
        
        # 计算最终板块指标
        plate_metrics = []
        for plate_id, stats in plate_stats.items():
            plate_info = self.all_plates.get(plate_id, {})
            
            # 计算平均涨跌幅
            if stats['stock_count'] > 0:
                avg_change = stats['total_change'] / stats['stock_count']
            else:
                avg_change = 0.0
            
            # 计算高级指标
            if stats['change_rates_1min']:
                avg_change_rate_1min = np.mean(stats['change_rates_1min'])
                total_amount_2min = np.sum(stats['amounts_2min'])
            else:
                avg_change_rate_1min = 0.0
                total_amount_2min = 0
            
            metrics = {
                'id': plate_id,
                'name': plate_info.get('name', '未知板块'),
                'change_pct': avg_change,
                'total_volume': stats['total_volume'],
                'total_large_net': stats['total_large_net'],
                'rise_count': stats['rise_count'],
                'fall_count': stats['fall_count'],
                'stock_count': stats['stock_count'],
                'type': plate_info.get('type', 'unknown'),
                'market_cap': plate_info.get('market_cap', 0),
                'timestamp': int(time.time()),
                'advanced_indicators': {
                    'avg_change_rate_1min': round(avg_change_rate_1min, 4),
                    'total_amount_2min': round(total_amount_2min, 2),
                    'valid_stocks': stats['valid_stocks'],
                    'data_source': 'batch_optimized'
                }
            }
            plate_metrics.append(metrics)
        
        logger.info(f"📊 优化计算完成: {len(plate_metrics)} 个板块")
        return plate_metrics
    
    def get_plate_stocks_optimized(self, plate_id: str) -> List[Dict]:
        """优化版获取板块个股数据"""
        # 使用缓存数据
        all_stocks_data = self.refresh_all_stocks_data()
        stock_ids = self.plate_to_stocks.get(plate_id, [])
        
        stocks = []
        for stock_id in stock_ids:
            if stock_id in all_stocks_data:
                stock_data = all_stocks_data[stock_id]
                basic_data = stock_data.get('basic', {})
                advanced_data = stock_data.get('advanced', {})
                
                formatted_stock = {
                    'code': stock_id,
                    'name': basic_data.get('name', f"股票{stock_id}"),
                    'change_pct': basic_data.get('change_pct', 0.0),
                    'price': basic_data.get('price', 0.0),
                    'volume': basic_data.get('volume', 0),
                    'market_cap': basic_data.get('market_cap', 0),
                    'large_net': basic_data.get('large_net', 0),
                    'timestamp': basic_data.get('timestamp', 0),
                    'advanced_indicators': advanced_data
                }
                stocks.append(formatted_stock)
        
        # 按涨跌幅排序
        stocks.sort(key=lambda x: x.get('change_pct', 0), reverse=True)
        
        logger.debug(f"📊 优化获取板块 {plate_id} 个股: {len(stocks)} 只股票")
        return stocks
class OptimizedEnhancedPlateUpdater(OptimizedPlateUpdater):
    """优化版增强板块更新器 - 整合高级指标到列表更新"""
    
    def __init__(self, plate_csv_path: str, stock_plate_csv_path: str, 
                 redis_storage: RedisStorageManager):
        super().__init__(plate_csv_path, stock_plate_csv_path)
        self.redis_storage = redis_storage
        self.plate_advanced_cache = {}  # 板块高级指标缓存
        
    def get_all_plate_metrics_with_integrated_advanced(self) -> List[Dict]:
        """获取所有板块指标（整合高级指标）- 使用Redis聚合"""
        try:
            # 1. 批量获取所有个股数据
            all_stocks_data = self.refresh_all_stocks_data()
            
            # 2. 初始化板块统计结构
            plate_stats = {}
            for plate_id in self.all_plates:
                plate_stats[plate_id] = {
                    'total_change': 0.0,
                    'total_volume': 0,
                    'total_large_net': 0,
                    'rise_count': 0,
                    'fall_count': 0,
                    'stock_count': 0,
                    'valid_stocks': 0,
                    # 高级指标聚合
                    'change_rates_1min': [],
                    'amounts_2min': [],
                    'total_amount_2min': 0
                }
            
            # 3. 遍历所有个股，聚合到板块
            for stock_id, stock_data in all_stocks_data.items():
                basic_data = stock_data.get('basic', {})
                advanced_data = stock_data.get('advanced', {})
                
                # 获取该股票所属的所有板块
                plate_ids = self.stock_to_plates.get(stock_id, [])
                
                for plate_id in plate_ids:
                    if plate_id not in plate_stats:
                        continue
                        
                    stats = plate_stats[plate_id]
                    
                    # 基础指标聚合
                    change_pct = basic_data.get('change_pct', 0.0)
                    volume = basic_data.get('volume', 0)
                    large_net = basic_data.get('large_net', 0)
                    
                    stats['total_change'] += change_pct
                    stats['total_volume'] += volume
                    stats['total_large_net'] += large_net
                    stats['stock_count'] += 1
                    
                    if change_pct > 0:
                        stats['rise_count'] += 1
                    elif change_pct < 0:
                        stats['fall_count'] += 1
                    
                    # 高级指标聚合
                    change_rate_1min = advanced_data.get('change_rate_1min')
                    amount_2min = advanced_data.get('amount_2min')
                    
                    if change_rate_1min is not None:
                        stats['change_rates_1min'].append(change_rate_1min)
                        stats['valid_stocks'] += 1
                    
                    if amount_2min is not None:
                        stats['amounts_2min'].append(amount_2min)
                        stats['total_amount_2min'] += amount_2min
            
            # 4. 计算最终板块指标（包含高级指标）
            plate_metrics = []
            for plate_id, stats in plate_stats.items():
                plate_info = self.all_plates.get(plate_id, {})
                
                # 计算平均涨跌幅
                if stats['stock_count'] > 0:
                    avg_change = stats['total_change'] / stats['stock_count']
                else:
                    avg_change = 0.0
                
                # 计算高级指标
                advanced_indicators = self._calculate_plate_advanced_from_stats(stats)
                
                # 构建完整的板块指标
                metrics = {
                    'id': plate_id,
                    'name': plate_info.get('name', '未知板块'),
                    'change_pct': round(avg_change, 4),
                    'total_volume': stats['total_volume'],
                    'total_large_net': stats['total_large_net'],
                    'rise_count': stats['rise_count'],
                    'fall_count': stats['fall_count'],
                    'stock_count': stats['stock_count'],
                    'type': plate_info.get('type', 'unknown'),
                    'market_cap': plate_info.get('market_cap', 0),
                    'timestamp': int(time.time()),
                    # 直接整合高级指标
                    'change_rate_1min': advanced_indicators.get('avg_change_rate_1min', 0),
                    'total_amount_2min': advanced_indicators.get('total_amount_2min', 0),
                    'valid_stocks': advanced_indicators.get('valid_stocks', 0),
                    'data_source': 'integrated'
                }
                plate_metrics.append(metrics)
            
            # 5. 批量更新到Redis（一次性）
            self._batch_update_plate_metrics_to_redis(plate_metrics)
            
            logger.debug(f"📊 整合计算完成: {len(plate_metrics)} 个板块")
            return plate_metrics
            
        except Exception as e:
            logger.error(f"❌ 整合计算板块指标失败: {e}")
            return []
    
    def _calculate_plate_advanced_from_stats(self, stats: Dict) -> Dict:
        """从统计数据计算板块高级指标"""
        if not stats['change_rates_1min']:
            return {
                'avg_change_rate_1min': 0.0,
                'total_amount_2min': 0,
                'valid_stocks': 0
            }
        
        return {
            'avg_change_rate_1min': round(np.mean(stats['change_rates_1min']), 4),
            'total_amount_2min': round(stats['total_amount_2min'], 2),
            'valid_stocks': stats['valid_stocks']
        }
    
    def _batch_update_plate_metrics_to_redis(self, plate_metrics: List[Dict]):
        """批量更新板块指标到Redis"""
        try:
            pipeline = self.redis_storage.redis.pipeline()
            
            for metrics in plate_metrics:
                # 存储完整指标（包含高级指标）
                key = f"plate:metrics:{metrics['id']}"
                pipeline.hset(key, 'change_pct', str(metrics['change_pct']))
                pipeline.hset(key, 'total_volume', str(metrics['total_volume']))
                pipeline.hset(key, 'total_large_net', str(metrics['total_large_net']))
                pipeline.hset(key, 'rise_count', str(metrics['rise_count']))
                pipeline.hset(key, 'fall_count', str(metrics['fall_count']))
                pipeline.hset(key, 'stock_count', str(metrics['stock_count']))
                pipeline.hset(key, 'change_rate_1min', str(metrics.get('change_rate_1min', 0)))
                pipeline.hset(key, 'total_amount_2min', str(metrics.get('total_amount_2min', 0)))
                pipeline.hset(key, 'timestamp', str(metrics['timestamp']))
                pipeline.expire(key, 30)  # 30秒过期
            
            pipeline.execute()
            logger.debug(f"💾 批量更新 {len(plate_metrics)} 个板块指标到Redis")
            
        except Exception as e:
            logger.error(f"❌ 批量更新Redis失败: {e}")
class PlateDataSimulator:
    """板块数据模拟器 - 适配优化版本"""
    
    def __init__(self, plate_updater, update_interval=3):
        self.plate_updater = plate_updater
        self.update_interval = update_interval
        self.running = False
        self.all_stock_ids = list(plate_updater.stock_to_plates.keys())
        logger.info(f"🎯 优化版模拟器初始化: {len(self.all_stock_ids)}只股票")

    async def start_simulation(self):
        """开始模拟数据更新"""
        self.running = True
        tick = 0
        
        try:
            while self.running:
                tick += 1
                
                stock_updates = self.generate_stock_updates()
                self.plate_updater.update_stocks(stock_updates)
                
                if tick % 10 == 0:
                    logger.info(f"🔄 第 {tick} 轮更新完成, {len(stock_updates)}只股票")
                    self.print_sample_metrics()
                
                await asyncio.sleep(self.update_interval)
                
        except Exception as e:
            logger.error(f"❌ 模拟器错误: {e}")
    
    def generate_stock_updates(self):
        """生成股票更新数据"""
        stock_updates = {}
        
        for stock_id in self.all_stock_ids:
            change = self.generate_realistic_change()
            volume = random.randint(10000, 1000000)
            large_net = random.randint(-50000, 50000)
            price = random.uniform(5, 100)
            market_cap = random.randint(100000000, 10000000000)
            
            stock_name = self._generate_stock_name(stock_id)
            
            stock_updates[stock_id] = {
                'change': change,
                'volume': volume,
                'large_net': large_net,
                'price': price,
                'market_cap': market_cap,
                'name': stock_name
            }
        
        return stock_updates
    
    def _generate_stock_name(self, stock_id: str) -> str:
        if stock_id.startswith('6'):
            return f"沪市股票{stock_id}"
        elif stock_id.startswith('0'):
            return f"深市股票{stock_id}" 
        elif stock_id.startswith('3'):
            return f"创业板股票{stock_id}"
        else:
            return f"股票{stock_id}"
    
    def generate_realistic_change(self):
        """生成更真实的涨跌幅"""
        if random.random() < 0.9:
            return round(random.uniform(-0.03, 0.03), 4)
        else:
            return round(random.uniform(-0.08, 0.08), 4)
    
    def print_sample_metrics(self):
        """打印示例板块指标"""
        sample_plates = ['801159', '801045', '801313', '801584']
        
        for plate_id in sample_plates:
            metrics = self.plate_updater.get_plate_metrics(plate_id)
            if metrics:
                logger.info(f"📊 {metrics['name']}: {metrics['change_pct']:+.2%} "
                          f"↑{metrics['rise_count']}↓{metrics['fall_count']} "
                          f"成交{metrics['total_volume']/10000:.0f}万")
    
    def stop(self):
        """停止模拟"""
        self.running = False
