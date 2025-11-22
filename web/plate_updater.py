import numpy as np
import csv
import json
import os
import time
import random
import asyncio
from collections import defaultdict
from typing import Dict, List
import logging

from redis_storage import RedisStorageManager

logger = logging.getLogger(__name__)

class RedisPlateUpdater:
    """
    集成Redis存储的板块更新器
    """
    
    def __init__(self, plate_file: str, relation_file: str, redis_url: str = "redis://localhost:6379"):
        self.plate_file = plate_file
        self.relation_file = relation_file
        self.redis_storage = RedisStorageManager(redis_url)
        
        self.load_data()
        self.build_optimized_structures()
        self.initialize_metrics()
        self.initialize_redis_data()
    
    def load_data(self):
        """加载板块和关系数据"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 加载板块数据
        self.plates = {}
        self.plate_hierarchy = {}
        self.plate_name_to_id = {}
        
        plate_path = os.path.join(script_dir, self.plate_file)
        with open(plate_path, 'r', encoding='gbk') as f:
            reader = csv.DictReader(f)
            for row in reader:
                plate_id = row['id']
                plate_name = row['name']
                market_cap = float(row['流通值']) if row['流通值'] else 0.0
                
                self.plate_name_to_id[plate_name] = plate_id
                
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
                
                self.plates[plate_id] = {
                    'name': plate_name,
                    'market_cap': market_cap,
                    'inner': inner_data
                }
                
                # 构建层级关系
                if inner_data:
                    self.plate_hierarchy[plate_id] = []
                    for inner_item in inner_data:
                        if isinstance(inner_item, list) and len(inner_item) >= 2:
                            sub_plate_id = inner_item[0]
                            self.plate_hierarchy[plate_id].append(sub_plate_id)
        
        # 加载个股-板块关系
        self.stock_plate_relations = defaultdict(list)
        self.plate_stock_relations = defaultdict(list)
        
        relation_path = os.path.join(script_dir, self.relation_file)
        with open(relation_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                if len(row) >= 2:
                    plate_id, stock_id = row[0], row[1]
                    self.stock_plate_relations[stock_id].append(plate_id)
                    self.plate_stock_relations[plate_id].append(stock_id)
        
        logger.info(f"📊 加载数据: {len(self.plates)}板块, {len(self.stock_plate_relations)}股票")
    
    def build_optimized_structures(self):
        """构建优化数据结构"""
        self.plate_ids = list(self.plates.keys())
        self.plate_to_idx = {pid: i for i, pid in enumerate(self.plate_ids)}
        
        self.stock_to_plate_indices = {}
        for stock_id, plate_list in self.stock_plate_relations.items():
            indices = [self.plate_to_idx[pid] for pid in plate_list if pid in self.plate_to_idx]
            if indices:
                self.stock_to_plate_indices[stock_id] = np.array(indices, dtype=np.int32)
    
    def initialize_metrics(self):
        """初始化指标数组"""
        n_plates = len(self.plate_ids)
        
        self.section_sum_change = np.zeros(n_plates, dtype=np.float32)  # 使用float32节省内存
        self.section_total_volume = np.zeros(n_plates, dtype=np.int32)
        self.section_total_large_net = np.zeros(n_plates, dtype=np.int32)
        self.section_rise_count = np.zeros(n_plates, dtype=np.int16)    # 使用int16
        self.section_fall_count = np.zeros(n_plates, dtype=np.int16)
        
        self.stock_current_change = {}
        self.stock_current_volume = {}
        self.stock_current_large_net = {}
        
        # 预计算成分股数量
        self.plate_stock_count = np.zeros(n_plates, dtype=np.int16)
        for stock_id, plate_list in self.stock_plate_relations.items():
            for plate_id in plate_list:
                if plate_id in self.plate_to_idx:
                    idx = self.plate_to_idx[plate_id]
                    self.plate_stock_count[idx] += 1
    
    def initialize_redis_data(self):
        """初始化Redis中的数据"""
        # 存储板块基础信息
        plate_infos = {}
        for plate_id, plate_info in self.plates.items():
            plate_type = "main" if plate_info['inner'] else "sub"
            plate_infos[plate_id] = {
                "name": plate_info['name'],
                "type": plate_type,
                "market_cap": plate_info['market_cap']
            }
        
        # 存储板块层级关系
        self.redis_storage.initialize_plate_hierarchy(self.plate_hierarchy)
        
        # 批量更新板块基础信息
        self.redis_storage.batch_update_plates({}, plate_infos)
        
        logger.info("✅ Redis数据初始化完成")
    
    def update_stocks(self, stock_updates: Dict[str, Dict]):
        """
        更新股票数据并同步到Redis
        
        Args:
            stock_updates: {stock_id: {change, volume, large_net, name, market_cap}}
        """
        update_start = time.time()
        
        # 准备批量更新Redis的数据
        redis_stock_updates = {}
        
        for stock_id, update_data in stock_updates.items():
            if stock_id not in self.stock_to_plate_indices:
                continue
                
            plate_indices = self.stock_to_plate_indices[stock_id]
            
            # 更新内存中的指标
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
            
            # 批量更新板块指标
            self.section_sum_change[plate_indices] += delta_change
            self.section_total_volume[plate_indices] += delta_volume
            self.section_total_large_net[plate_indices] += delta_large_net
            
            # 修复：正确的涨跌计数更新方式
            # 对于每个板块索引，根据涨跌情况更新计数
            for idx in plate_indices:
                # 先移除旧的涨跌状态
                old_change_for_plate = old_change
                if old_change_for_plate > 0:
                    self.section_rise_count[idx] -= 1
                elif old_change_for_plate < 0:
                    self.section_fall_count[idx] -= 1
                
                # 添加新的涨跌状态
                if new_change > 0:
                    self.section_rise_count[idx] += 1
                elif new_change < 0:
                    self.section_fall_count[idx] += 1
            
            # 准备Redis更新数据
            redis_stock_updates[stock_id] = {
                "price": update_data.get('price', 0),
                "change_pct": new_change,
                "volume": new_volume,
                "timestamp": int(time.time()),
                "name": update_data.get('name', ''),
                "market_cap": update_data.get('market_cap', 0),
                "plates": self.stock_plate_relations.get(stock_id, [])
            }
        
        # 批量更新股票数据到Redis
        if redis_stock_updates:
            self.redis_storage.batch_update_stocks(redis_stock_updates)
        
        # 更新板块指标到Redis
        self._update_plate_metrics_to_redis()
        
        update_time = (time.time() - update_start) * 1000
        logger.debug(f"⚡ 更新 {len(stock_updates)}只股票, 耗时: {update_time:.2f}ms")
    
    def _update_plate_metrics_to_redis(self):
        """将板块指标更新到Redis"""
        plate_metrics = {}
        
        for plate_id in self.plate_ids:
            idx = self.plate_to_idx[plate_id]
            count = self.plate_stock_count[idx]
            
            if count > 0:
                metrics = {
                    "change_pct": float(self.section_sum_change[idx] / count),
                    "total_volume": int(self.section_total_volume[idx]),
                    "total_large_net": int(self.section_total_large_net[idx]),
                    "rise_count": int(self.section_rise_count[idx]),
                    "fall_count": int(self.section_fall_count[idx]),
                    "stock_count": int(count),
                    "timestamp": int(time.time())
                }
                plate_metrics[plate_id] = metrics
        
        # 批量更新到Redis
        self.redis_storage.batch_update_plates(plate_metrics)
    
    def get_plate_metrics(self, plate_id: str) -> Dict:
        """获取板块指标（从Redis）"""
        return self.redis_storage.get_plate_data(plate_id)
    
    def get_all_plate_metrics(self) -> List[Dict]:
        """获取所有板块指标（从Redis）"""
        return self.redis_storage.get_all_plate_metrics()
    
    def get_main_plates_metrics(self) -> List[Dict]:
        """获取主流板块指标（从Redis）"""
        return self.redis_storage.get_main_plates()
    
    def get_sub_plates_metrics(self, main_plate_name: str) -> List[Dict]:
        """获取子板块指标（从Redis）"""
        main_plate_id = self.plate_name_to_id.get(main_plate_name)
        print(main_plate_id)
        if not main_plate_id:
            return []
        return self.redis_storage.get_sub_plates(main_plate_id)
    
    def get_plate_stocks(self, plate_id: str) -> List[Dict]:
        """获取板块个股数据（从Redis）"""
        return self.redis_storage.get_plate_stocks(plate_id)
    
    def get_plate_hierarchy(self):
        """获取板块层级关系"""
        return self.plate_hierarchy, list(self.plate_hierarchy.keys())


class PlateDataSimulator:
    """板块数据模拟器 - 适配RedisPlateUpdater"""
    
    def __init__(self, plate_updater, update_interval=3):
        self.plate_updater = plate_updater
        self.update_interval = update_interval
        self.running = False
        
        # 获取所有股票ID
        self.all_stock_ids = list(plate_updater.stock_plate_relations.keys())
        logger.info(f"🎯 模拟器初始化: {len(self.all_stock_ids)}只股票, 更新间隔: {self.update_interval}秒")
    
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
        """生成股票更新数据"""
        stock_updates = {}
        
        for stock_id in self.all_stock_ids:
            # 模拟更真实的涨跌幅分布（大部分小幅波动，少数大幅波动）
            change = self.generate_realistic_change()
            volume = random.randint(10000, 1000000)  # 成交量
            large_net = random.randint(-50000, 50000)  # 大单净额
            price = random.uniform(5, 100)  # 价格
            market_cap = random.randint(100000000, 10000000000)  # 市值
            
            stock_updates[stock_id] = {
                'change': change,
                'volume': volume,
                'large_net': large_net,
                'price': price,
                'market_cap': market_cap,
                'name': f"股票{stock_id}"
            }
        
        return stock_updates
    
    def generate_realistic_change(self):
        """生成更真实的涨跌幅"""
        # 90%的股票在±3%之间，10%的股票可能大幅波动
        if random.random() < 0.9:
            return round(random.uniform(-0.03, 0.03), 4)
        else:
            return round(random.uniform(-0.08, 0.08), 4)
    
    def print_sample_metrics(self):
        """打印示例板块指标"""
        sample_plates = ['801159', '801045', '801313', '801584']  # 机器人、医药、金融、数字经济
        
        for plate_id in sample_plates:
            metrics = self.plate_updater.get_plate_metrics(plate_id)
            if metrics:
                logger.info(f"📊 {metrics['name']}: {metrics['change_pct']:+.2%} "
                          f"↑{metrics['rise_count']}↓{metrics['fall_count']} "
                          f"成交{metrics['total_volume']/10000:.0f}万")
    
    def stop(self):
        """停止模拟"""
        self.running = False