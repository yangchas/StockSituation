import numpy as np
import csv
import json
import os
import time
import random
from collections import defaultdict

class OptimizedPlateUpdater:
    """
    高效个股-板块多对多映射系统（支持全量模拟更新）
    """
    
    def __init__(self, plate_file, relation_file):
        self.load_data(plate_file, relation_file)
        self.build_optimized_structures()
        self.initialize_metrics()
    
    def load_data(self, plate_file, relation_file):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        plate_path = os.path.join(script_dir, plate_file)
        relation_path = os.path.join(script_dir, relation_file)
        
        # 加载板块数据
        self.plates = {}
        with open(plate_path, 'r', encoding='gbk') as f:
            reader = csv.DictReader(f)
            for row in reader:
                inner = json.loads(row['inner'].replace("'", '"')) if row['inner'] else []
                self.plates[row['id']] = {
                    'name': row['name'], 
                    'market_cap': float(row['流通值']), 
                    'inner': inner
                }
        
        # 加载个股-板块关系
        self.stock_plate_relations = defaultdict(list)
        with open(relation_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)
            for plate_id, stock_id in reader:
                self.stock_plate_relations[stock_id].append(plate_id)
    
    def build_optimized_structures(self):
        self.plate_ids = list(self.plates.keys())
        self.plate_to_idx = {pid: i for i, pid in enumerate(self.plate_ids)}
        
        self.stock_to_plate_indices = {}
        for stock_id, plate_list in self.stock_plate_relations.items():
            indices = [self.plate_to_idx[pid] for pid in plate_list if pid in self.plate_to_idx]
            if indices:
                self.stock_to_plate_indices[stock_id] = np.array(indices, dtype=np.int32)
    
    def initialize_metrics(self):
        n_plates = len(self.plate_ids)
        self.section_sum_change = np.zeros(n_plates, dtype=np.float64)
        self.section_total_volume = np.zeros(n_plates, dtype=np.float64)
        self.section_total_large_net = np.zeros(n_plates, dtype=np.float64)
        self.stock_current_change = {}
        
        # 预计算成分股数量
        self.plate_stock_count = np.zeros(n_plates, dtype=np.int32)
        for stock_id, plate_list in self.stock_plate_relations.items():
            for plate_id in plate_list:
                if plate_id in self.plate_to_idx:
                    idx = self.plate_to_idx[plate_id]
                    self.plate_stock_count[idx] += 1
    
    def update_stocks(self, stock_updates):
        """高效批量更新"""
        for stock_id, update_data in stock_updates.items():
            if stock_id not in self.stock_to_plate_indices:
                continue
                
            plate_indices = self.stock_to_plate_indices[stock_id]
            new_change = update_data['change']
            old_change = self.stock_current_change.get(stock_id, 0.0)
            delta_change = new_change - old_change
            self.stock_current_change[stock_id] = new_change
            
            self.section_sum_change[plate_indices] += delta_change
            self.section_total_volume[plate_indices] += update_data.get('volume', 0)
            self.section_total_large_net[plate_indices] += update_data.get('large_net', 0)
    
    def get_plate_metrics(self, plate_id):
        if plate_id not in self.plate_to_idx:
            return None
        idx = self.plate_to_idx[plate_id]
        count = self.plate_stock_count[idx]
        return {
            'name': self.plates[plate_id]['name'],
            'change_pct': self.section_sum_change[idx] / count if count > 0 else 0.0,
            'total_volume': self.section_total_volume[idx],
            'total_large_net': self.section_total_large_net[idx],
            'stock_count': int(count)
        }

# ==================== 模拟全量更新逻辑 ====================

def simulate_full_update(updater, interval=3):
    """
    模拟全量更新：定时为所有个股生成随机涨幅，并更新板块
    
    Args:
        updater: OptimizedPlateUpdater 实例
        interval: 更新间隔（秒）
    """
    # 获取所有唯一股票ID（来自个股-板块关系）
    all_stock_ids = list(updater.stock_plate_relations.keys())
    print(f"检测到 {len(all_stock_ids)} 只股票，开始模拟全量更新（每{interval}秒一次）...")
    
    tick = 0
    try:
        while True:
            tick += 1
            print(f"\n--- 第 {tick} 轮全量更新 ---")
            
            # 为所有股票生成模拟数据
            stock_updates = {}
            for stock_id in all_stock_ids:
                # 模拟涨跌幅：-5% 到 +5%
                change = round(random.uniform(-0.05, 0.05), 4)
                volume = random.randint(1000, 100000)      # 成交量
                large_net = random.randint(-5000, 5000)    # 大单净额
                
                stock_updates[stock_id] = {
                    'change': change,
                    'volume': volume,
                    'large_net': large_net
                }
            
            # 批量更新所有股票
            start_time = time.time()
            updater.update_stocks(stock_updates)
            update_time = time.time() - start_time
            
            print(f"✅ 全量更新完成 | 股票数: {len(all_stock_ids)} | 耗时: {update_time*1000:.2f}ms")
            
            # 示例：查询几个热门板块
            sample_plates = ['801159', '801045', '801313']  # 替换为你关心的板块ID
            for plate_id in sample_plates:
                metrics = updater.get_plate_metrics(plate_id)
                if metrics:
                    print(f"📊 板块 {plate_id} ({metrics['name']}): "
                          f"平均涨幅 {metrics['change_pct']:.2%}, "
                          f"成分股 {metrics['stock_count']}只")
            
            # 等待下一轮
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n⏹️  模拟更新已停止")

# ==================== 使用示例 ====================

if __name__ == "__main__":
    # 初始化系统
    updater = OptimizedPlateUpdater('data/板块.csv', 'data/个股板块.csv')
    
    # 启动全量模拟更新（每3秒一次）
    simulate_full_update(updater, interval=3)