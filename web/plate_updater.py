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
    基于懒加载的板块更新器 - 只在需要时计算子板块数据
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
        
        logger.info(f"🚀 懒加载板块更新器初始化完成: {len(self.main_plates)}个主板块, {len(self.all_plates)}个总板块")
    
    def load_plate_data(self):
        """加载板块数据"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.all_plates = {}  # 所有板块 {id: {name, market_cap, type}}
        self.main_plates = {}  # 主流板块
        self.plate_name_to_id = {}
        
        plate_path = os.path.join(script_dir, self.plate_file)
        with open(plate_path, 'r', encoding='gbk') as f:
            reader = csv.DictReader(f)
            for row in reader:
                plate_id = row['id']
                plate_name = row['name']
                market_cap = float(row['流通值']) if row['流通值'] else 0.0
                
                # 解析inner字段
                inner_data = []
                inner_str = row.get('inner', '').strip()
                if inner_str and inner_str != '[]':
                    try:
                        inner_str = inner_str.replace("'", '"')
                        inner_list = json.loads(inner_str)
                        inner_data = inner_list
                    except json.JSONDecodeError as e:
                        logger.warning(f"解析inner字段失败 {plate_id}: {e}")
                
                # 存储板块信息
                self.all_plates[plate_id] = {
                    'name': plate_name,
                    'market_cap': market_cap,
                    'inner': inner_data,
                    'type': 'main'
                }
                self.plate_name_to_id[plate_name] = plate_id
                self.main_plates[plate_id] = self.all_plates[plate_id]
                
                # 添加子板块到all_plates
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
        """加载个股-板块关系 - 修复版本"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        self.stock_to_plates = defaultdict(list)  # 股票 -> 板块列表
        self.plate_to_stocks = defaultdict(list)  # 板块 -> 股票列表
        
        relation_path = os.path.join(script_dir, self.relation_file)
        
        try:
            with open(relation_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader)  # 跳过表头
                
                row_count = 0
                valid_count = 0
                
                for row in reader:
                    row_count += 1
                    if len(row) >= 2:
                        plate_id, stock_id = row[0], row[1]
                        
                        # 只添加在all_plates中的板块关系
                        if plate_id in self.all_plates:
                            self.stock_to_plates[stock_id].append(plate_id)
                            self.plate_to_stocks[plate_id].append(stock_id)
                            valid_count += 1
                        else:
                            # 记录无效的板块ID，用于调试
                            if row_count <= 10:  # 只记录前10个无效ID避免日志过多
                                logger.warning(f"⚠️ 忽略无效板块ID: {plate_id} (股票: {stock_id})")
                
                logger.info(f"📈 加载个股关系: {row_count}行数据, {valid_count}个有效关系")
                logger.info(f"📊 股票-板块映射: {len(self.stock_to_plates)}只股票, {len(self.plate_to_stocks)}个板块有股票")
                
                # 统计各板块的股票数量分布
                stock_counts = [len(stocks) for stocks in self.plate_to_stocks.values()]
                if stock_counts:
                    logger.info(f"📋 板块股票数量统计: 平均{sum(stock_counts)/len(stock_counts):.1f}只, "
                              f"最多{max(stock_counts)}只, 最少{min(stock_counts)}只")
                    
        except FileNotFoundError:
            logger.error(f"❌ 个股板块关系文件不存在: {relation_path}")
        except Exception as e:
            logger.error(f"❌ 加载个股关系失败: {e}")
    
    def build_plate_hierarchy(self):
        """构建板块层级关系"""
        self.plate_hierarchy = {}  # 主板块ID -> 子板块ID列表
        
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
        
        # 主流板块指标数组
        n_main_plates = len(self.main_plate_ids)
        self.main_section_sum_change = np.zeros(n_main_plates, dtype=np.float32)
        self.main_section_total_volume = np.zeros(n_main_plates, dtype=np.int32)
        self.main_section_total_large_net = np.zeros(n_main_plates, dtype=np.int32)
        self.main_section_rise_count = np.zeros(n_main_plates, dtype=np.int16)
        self.main_section_fall_count = np.zeros(n_main_plates, dtype=np.int16)
        
        # 股票当前状态
        self.stock_current_change = {}
        self.stock_current_volume = {}
        self.stock_current_large_net = {}
        
        # 预计算主流板块的成分股数量
        self.main_plate_stock_count = np.zeros(n_main_plates, dtype=np.int16)
        for stock_id, plate_list in self.stock_to_plates.items():
            for plate_id in plate_list:
                if plate_id in self.main_plate_to_idx:
                    idx = self.main_plate_to_idx[plate_id]
                    self.main_plate_stock_count[idx] += 1
    def refresh_stock_data_from_redis(self):
        """
        从Redis刷新所有股票数据并更新板块指标 - 修复版本
        """
        refresh_start = time.time()
        
        # 重置板块指标
        n_main_plates = len(self.main_plate_ids)
        self.main_section_sum_change = np.zeros(n_main_plates, dtype=np.float32)
        self.main_section_total_volume = np.zeros(n_main_plates, dtype=np.int32)
        self.main_section_total_large_net = np.zeros(n_main_plates, dtype=np.int32)
        self.main_section_rise_count = np.zeros(n_main_plates, dtype=np.int16)
        self.main_section_fall_count = np.zeros(n_main_plates, dtype=np.int16)
        
        # 清空当前股票状态
        self.stock_current_change = {}
        self.stock_current_volume = {}
        self.stock_current_large_net = {}
        
        processed_count = 0
        valid_stock_count = 0
        error_count = 0
        
        # 遍历所有已知的股票
        for stock_id in self.stock_to_plates.keys():
            try:
                # 从Redis获取股票数据
                stock_data = self.redis_storage.get_stock_data(stock_id)
                
                if not stock_data:
                    continue
                    
                # 获取股票所属的所有板块
                plate_list = self.stock_to_plates[stock_id]
                
                # 只更新主流板块
                main_plate_indices = []
                for plate_id in plate_list:
                    if plate_id in self.main_plate_to_idx:
                        main_plate_indices.append(self.main_plate_to_idx[plate_id])
                
                if not main_plate_indices:
                    continue
                    
                main_plate_indices = np.array(main_plate_indices, dtype=np.int32)
                
                # 获取股票指标
                change = stock_data.get('change_pct', 0.0)
                volume = stock_data.get('volume', 0)
                large_net = stock_data.get('large_net', 0)
                
                # 更新股票当前状态
                self.stock_current_change[stock_id] = change
                self.stock_current_volume[stock_id] = volume
                self.stock_current_large_net[stock_id] = large_net
                
                # 更新板块指标
                self.main_section_sum_change[main_plate_indices] += change
                self.main_section_total_volume[main_plate_indices] += volume
                self.main_section_total_large_net[main_plate_indices] += large_net
                
                # 更新涨跌计数
                for idx in main_plate_indices:
                    if change > 0:
                        self.main_section_rise_count[idx] += 1
                    elif change < 0:
                        self.main_section_fall_count[idx] += 1
                
                processed_count += 1
                if stock_data.get('price', 0) > 0:  # 有有效价格的股票
                    valid_stock_count += 1
                    
            except Exception as e:
                error_count += 1
                logger.warning(f"⚠️ 处理股票数据失败 {stock_id}: {e}")
        
        # 更新主流板块数据到Redis
        self._update_main_plates_to_redis()
        
        refresh_time = (time.time() - refresh_start) * 1000
        logger.info(f"🔄 从Redis刷新 {processed_count}只股票数据, 其中{valid_stock_count}只有效, {error_count}只错误, 耗时: {refresh_time:.2f}ms")
    def update_stocks(self, stock_updates: Dict[str, Dict]):
        """
        更新股票数据 - 修复版本，确保股票数据更新到Redis
        """
        update_start = time.time()
        
        # 准备批量更新Redis的股票数据
        redis_stock_updates = {}
        
        for stock_id, update_data in stock_updates.items():
            if stock_id not in self.stock_to_plates:
                continue
                
            # 获取股票所属的所有板块
            plate_list = self.stock_to_plates[stock_id]
            
            # 只更新主流板块
            main_plate_indices = []
            for plate_id in plate_list:
                if plate_id in self.main_plate_to_idx:
                    main_plate_indices.append(self.main_plate_to_idx[plate_id])
            
            if not main_plate_indices:
                continue
                
            main_plate_indices = np.array(main_plate_indices, dtype=np.int32)
            
            # 更新指标
            new_change = update_data['change']
            old_change = self.stock_current_change.get(stock_id, 0.0)
            delta_change = new_change - old_change
            self.stock_current_change[stock_id] = new_change
            
            new_volume = update_data.get('volume', 0)
            old_volume = self.stock_current_volume.get(stock_id, 0)
            delta_volume = new_volume - old_volume
            self.stock_current_volume[stock_id] = new_volume
            
            new_large_net = update_data.get('large_net', 0)
            old_large_net = self.stock_current_large_net.get(stock_id, 0)
            delta_large_net = new_large_net - old_large_net
            self.stock_current_large_net[stock_id] = new_large_net
            
            # 批量更新主流板块指标
            self.main_section_sum_change[main_plate_indices] += delta_change
            self.main_section_total_volume[main_plate_indices] += delta_volume
            self.main_section_total_large_net[main_plate_indices] += delta_large_net
            
            # 更新涨跌计数
            for idx in main_plate_indices:
                # 先移除旧的涨跌状态
                old_change_for_plate = old_change
                if old_change_for_plate > 0:
                    self.main_section_rise_count[idx] -= 1
                elif old_change_for_plate < 0:
                    self.main_section_fall_count[idx] -= 1
                
                # 添加新的涨跌状态
                if new_change > 0:
                    self.main_section_rise_count[idx] += 1
                elif new_change < 0:
                    self.main_section_fall_count[idx] += 1
            
            # 准备Redis股票更新数据
            redis_stock_updates[stock_id] = {
                "price": update_data.get('price', random.uniform(5, 100)),
                "change_pct": new_change,
                "volume": new_volume,
                "timestamp": int(time.time()),
                "name": update_data.get('name', f"股票{stock_id}"),
                "market_cap": update_data.get('market_cap', random.randint(100000000, 10000000000)),
                "plates": plate_list  # 股票所属的所有板块
            }
        
        # 批量更新股票数据到Redis
        if redis_stock_updates:
            self.redis_storage.batch_update_stocks(redis_stock_updates)
            logger.info(f"💾 更新 {len(redis_stock_updates)} 只股票数据到Redis")
        
        # 更新主流板块数据到Redis
        self._update_main_plates_to_redis()
        
        update_time = (time.time() - update_start) * 1000
        logger.debug(f"⚡ 更新 {len(stock_updates)}只股票, 耗时: {update_time:.2f}ms")
    
    def _update_main_plates_to_redis(self):
        """将主流板块指标更新到Redis"""
        plate_metrics = {}
        
        for plate_id in self.main_plate_ids:
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
            
            plate_metrics[plate_id] = metrics
        
        # 批量更新到Redis
        self.redis_storage.batch_update_plates(plate_metrics)
    
    def get_plate_metrics(self, plate_id: str) -> Dict:
        """获取板块指标 - 改进的懒加载版本"""
        # 首先尝试从Redis获取
        plate_data = self.redis_storage.get_plate_data(plate_id)
        
        # 检查Redis中的数据是否有效
        if plate_data and self._is_plate_data_valid(plate_data):
            return plate_data
        
        # Redis中没有有效数据，实时计算
        return self._calculate_plate_metrics_lazy(plate_id)

    def _is_plate_data_valid(self, plate_data: Dict) -> bool:
        """检查板块数据是否有效"""
        try:
            timestamp = plate_data.get('timestamp', 0)
            current_time = int(time.time())
            
            # 如果数据超过30秒，认为已过期
            if current_time - timestamp > 30:
                return False
            
            # 检查必要字段是否存在
            required_fields = ['change_pct', 'total_volume', 'rise_count', 'fall_count']
            for field in required_fields:
                if field not in plate_data:
                    return False
            
            return True
        except Exception:
            return False
    def _calculate_plate_metrics_lazy(self, plate_id: str) -> Dict:
        """懒加载计算板块指标 - 改进版本"""
        plate_info = self.all_plates.get(plate_id)
        if not plate_info:
            return None
        
        # 获取板块的股票列表
        stock_ids = self.plate_to_stocks.get(plate_id, [])
        stock_count = len(stock_ids)
        
        if stock_count == 0:
            # 没有股票，返回基础信息
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
            # 实时计算板块指标
            total_change = 0.0
            total_volume = 0
            total_large_net = 0
            rise_count = 0
            fall_count = 0
            valid_stock_count = 0
            
            for stock_id in stock_ids:
                # 从Redis获取股票的当前状态（确保是最新数据）
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
            
            # 计算平均涨跌幅（只使用有效股票）
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
        self.redis_storage.update_plate_metrics(plate_id, metrics)
        
        logger.info(f"🔄 懒加载计算板块: {plate_info['name']} ({metrics['stock_count']}只有效股票)")
        return metrics
    
    def get_all_plate_metrics(self) -> List[Dict]:
        """获取所有板块指标 - 修复版本，包含主板块和子板块"""
        all_metrics = []
        
        # 获取所有板块（包括主板块和子板块）
        for plate_id in self.all_plates:
            metrics = self.get_plate_metrics(plate_id)
            if metrics:
                all_metrics.append(metrics)
        
        logger.info(f"📊 获取所有板块指标: {len(all_metrics)}个板块 (主板块: {len(self.main_plates)}, 子板块: {len(all_metrics) - len(self.main_plates)})")
        return all_metrics
    
    def get_main_plates_metrics(self) -> List[Dict]:
        """获取主流板块指标"""
        main_metrics = []
        for plate_id in self.main_plate_ids:
            metrics = self.get_plate_metrics(plate_id)
            if metrics:
                main_metrics.append(metrics)
        
        logger.info(f"🏆 获取主流板块指标: {len(main_metrics)}个主板块")
        return main_metrics
    
    def get_sub_plates_metrics(self, main_plate_name: str) -> List[Dict]:
        """获取子板块指标 - 懒加载版本"""
        logger.info(f"🔍 懒加载获取主板块 '{main_plate_name}' 的子板块")
        
        # 找到主板块ID
        main_plate_id = self.plate_name_to_id.get(main_plate_name)
        if not main_plate_id:
            logger.warning(f"❌ 未找到主板块: {main_plate_name}")
            return []
        
        # 获取子板块ID列表
        sub_plate_ids = self.plate_hierarchy.get(main_plate_id, [])
        logger.info(f"📦 找到 {len(sub_plate_ids)} 个子板块")
        
        sub_metrics = []
        for sub_plate_id in sub_plate_ids:
            # 懒加载计算子板块指标
            metrics = self.get_plate_metrics(sub_plate_id)
            if metrics:
                sub_metrics.append(metrics)
                logger.info(f"✅ 懒加载子板块: {metrics['name']} ({metrics['stock_count']}只股票)")
            else:
                logger.warning(f"❌ 子板块计算失败: {sub_plate_id}")
        
        logger.info(f"📋 主板块 '{main_plate_name}' 共有 {len(sub_metrics)} 个子板块")
        return sub_metrics
    
    def get_plate_stocks(self, plate_id: str) -> List[Dict]:
        """获取板块个股数据 - 从Redis获取实际数据"""
        logger.info(f"📊 获取板块 {plate_id} 的个股数据")
        
        # 1. 从个股板块关系获取该板块的所有股票ID
        stock_ids = self.plate_to_stocks.get(plate_id, [])
        logger.info(f"📦 板块 {plate_id} 包含 {len(stock_ids)} 只股票")
        
        if not stock_ids:
            logger.warning(f"⚠️ 板块 {plate_id} 没有找到股票")
            return []
        
        stocks = []
        found_count = 0
        not_found_count = 0
        
        # 2. 从Redis获取每只股票的详情（C++端存储的实际数据）
        for stock_id in stock_ids:  # 不再限制数量，获取所有股票
            stock_data = self.redis_storage.get_stock_data(stock_id)
            
            if stock_data:
                # 确保数据结构符合前端要求
                formatted_stock = {
                    'code': stock_id,  # 使用实际的股票ID
                    'name': stock_data.get('name', f"股票{stock_id}"),
                    'change_pct': stock_data.get('change_pct', 0.0),
                    'price': stock_data.get('price', 0.0),
                    'volume': stock_data.get('volume', 0),
                    'market_cap': stock_data.get('market_cap', 0),
                    'large_net': stock_data.get('large_net', 0),
                    'timestamp': stock_data.get('timestamp', 0)
                }
                stocks.append(formatted_stock)
                found_count += 1
            else:
                # Redis中没有找到该股票的数据
                logger.debug(f"⚠️ Redis中未找到股票 {stock_id} 的数据")
                not_found_count += 1
        
        # 按涨跌幅排序
        stocks.sort(key=lambda x: x.get('change_pct', 0), reverse=True)
        
        logger.info(f"✅ 获取个股数据完成: 找到 {found_count} 只, 未找到 {not_found_count} 只")
        return stocks
    
    def _create_simulated_stock_data(self, stock_id: str, plate_id: str) -> Dict:
        """创建模拟股票数据"""
        plate_info = self.all_plates.get(plate_id, {})
        plate_name = plate_info.get('name', '未知板块')
        
        return {
            'code': stock_id,
            'name': f"{plate_name}个股{stock_id[-4:]}",  # 使用股票ID后4位作为标识
            'change_pct': round(random.uniform(-0.05, 0.05), 4),
            'price': round(random.uniform(5, 100), 2),
            'volume': random.randint(1000000, 100000000),
            'market_cap': random.randint(100000000, 10000000000)
        }
    
    def get_plate_hierarchy(self):
        """获取板块层级关系"""
        return self.plate_hierarchy, list(self.main_plates.keys())
    
    def debug_plate_stocks(self, plate_id: str):
        """调试板块个股数据状态"""
        logger.info(f"🔍 调试板块 {plate_id} 的个股数据")
        
        # 检查板块是否存在
        plate_info = self.all_plates.get(plate_id)
        if not plate_info:
            logger.error(f"❌ 板块 {plate_id} 不存在")
            return
        
        logger.info(f"📋 板块信息: {plate_info['name']} (类型: {plate_info['type']})")
        
        # 检查股票关系
        stock_ids = self.plate_to_stocks.get(plate_id, [])
        logger.info(f"📦 个股关系: {len(stock_ids)} 只股票")
        
        # 检查前5只股票的Redis数据状态
        sample_stocks = stock_ids[:5]
        for stock_id in sample_stocks:
            stock_data = self.redis_storage.get_stock_data(stock_id)
            if stock_data:
                logger.info(f"✅ 股票 {stock_id}: 有Redis数据 (涨跌幅: {stock_data.get('change_pct', 0):.2%})")
            else:
                logger.info(f"❌ 股票 {stock_id}: 无Redis数据")
        
        # 测试获取个股数据
        stocks = self.get_plate_stocks(plate_id)
        logger.info(f"📊 最终返回: {len(stocks)} 只股票数据")


class PlateDataSimulator:
    """板块数据模拟器 - 适配懒加载版本"""
    
    def __init__(self, plate_updater, update_interval=3):
        self.plate_updater = plate_updater
        self.update_interval = update_interval
        self.running = False
        
        # 获取所有股票ID
        self.all_stock_ids = list(plate_updater.stock_to_plates.keys())
        logger.info(f"🎯 懒加载模拟器初始化: {len(self.all_stock_ids)}只股票")

    async def start_refreshing(self):
        """开始定期从Redis刷新数据"""
        self.running = True
        tick = 0
        
        try:
            while self.running:
                tick += 1
                
                # 从Redis刷新所有股票数据
                self.plate_updater.refresh_stock_data_from_redis()
                
                # 每10次刷新打印一次状态
                if tick % 10 == 0:
                    logger.info(f"🔄 第 {tick} 轮数据刷新完成")
                    self.print_sample_metrics()
                
                await asyncio.sleep(self.refresh_interval)
                
        except Exception as e:
            logger.error(f"❌ 数据刷新器错误: {e}")
    async def start_simulation(self):
        """开始模拟数据更新"""
        self.running = True
        tick = 0
        
        try:
            while self.running:
                tick += 1
                
                # 生成股票更新数据
                stock_updates = self.generate_stock_updates()
                
                # 批量更新
                self.plate_updater.update_stocks(stock_updates)
                
                # 每10次更新打印一次状态
                if tick % 10 == 0:
                    logger.info(f"🔄 第 {tick} 轮更新完成, {len(stock_updates)}只股票")
                    self.print_sample_metrics()
                
                await asyncio.sleep(self.update_interval)
                
        except Exception as e:
            logger.error(f"❌ 模拟器错误: {e}")
    
    def generate_stock_updates(self):
        """生成股票更新数据 - 修复版本，生成更真实的数据"""
        stock_updates = {}
        
        for stock_id in self.all_stock_ids:
            # 模拟更真实的涨跌幅分布
            change = self.generate_realistic_change()
            volume = random.randint(10000, 1000000)
            large_net = random.randint(-50000, 50000)
            price = random.uniform(5, 100)
            market_cap = random.randint(100000000, 10000000000)
            
            # 根据股票ID生成更有意义的名称
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
        """根据股票ID生成股票名称"""
        # 简单的命名规则，实际应用中应该从数据库或其他数据源获取
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