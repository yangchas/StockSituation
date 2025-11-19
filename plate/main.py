import numpy as np
import csv
import json
import os
from collections import defaultdict

class OptimizedPlateUpdater:
    def __init__(self, plate_file, relation_file):
        self.load_data(plate_file, relation_file)
        self.build_optimized_structures()
        self.initialize_metrics()
    
    def load_data(self, plate_file, relation_file):
        # 获取脚本所在目录
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 构建绝对路径
        plate_path = os.path.join(script_dir, plate_file)
        relation_path = os.path.join(script_dir, relation_file)
        
        # 加载板块数据 (使用GBK编码)
        self.plates = {}
        with open(plate_path, 'r', encoding='gbk') as f:
            reader = csv.DictReader(f)
            for row in reader:
                inner = json.loads(row['inner'].replace("'", '"'))
                self.plates[row['id']] = {
                    'name': row['name'], 
                    'market_cap': float(row['流通值']), 
                    'inner': inner
                }
        
        # 加载个股-板块关系 (使用UTF-8编码)
        self.stock_plate_relations = defaultdict(list)
        with open(relation_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # 跳过标题行
            for plate_id, stock_id in reader:
                self.stock_plate_relations[stock_id].append(plate_id)
    
    def build_optimized_structures(self):
        # 板块ID到索引映射
        self.plate_ids = list(self.plates.keys())
        self.plate_to_idx = {pid: i for i, pid in enumerate(self.plate_ids)}
        
        # 股票到板块索引列表（预计算）
        self.stock_to_plate_indices = {}
        for stock_id, plate_list in self.stock_plate_relations.items():
            indices = [self.plate_to_idx[pid] for pid in plate_list if pid in self.plate_to_idx]
            self.stock_to_plate_indices[stock_id] = np.array(indices, dtype=np.int32)
    
    def initialize_metrics(self):
        n_plates = len(self.plate_ids)
        self.section_sum_change = np.zeros(n_plates, dtype=np.float64)
        self.section_total_volume = np.zeros(n_plates, dtype=np.float64)
        self.section_total_large_net = np.zeros(n_plates, dtype=np.float64)
        self.stock_current_change = {}
        
        # ✅ 新增：预计算每个板块的成分股数量（关键优化！）
        self.plate_stock_count = np.zeros(n_plates, dtype=np.int32)
        for stock_id, plate_list in self.stock_plate_relations.items():
            for plate_id in plate_list:
                if plate_id in self.plate_to_idx:
                    idx = self.plate_to_idx[plate_id]
                    self.plate_stock_count[idx] += 1
    
    def update_stocks(self, stock_updates):
        """高效更新函数 - 无冗余循环"""
        for stock_id, update_data in stock_updates.items():
            if stock_id not in self.stock_to_plate_indices:
                continue
                
            plate_indices = self.stock_to_plate_indices[stock_id]
            new_change = update_data['change']
            old_change = self.stock_current_change.get(stock_id, 0.0)
            delta_change = new_change - old_change
            self.stock_current_change[stock_id] = new_change
            
            # 向量化更新
            self.section_sum_change[plate_indices] += delta_change
            self.section_total_volume[plate_indices] += update_data.get('volume', 0)
            self.section_total_large_net[plate_indices] += update_data.get('large_net', 0)
    
    def get_plate_metrics(self, plate_id):
        """获取指定板块的指标 - O(1) 时间复杂度"""
        if plate_id not in self.plate_to_idx:
            return None
        idx = self.plate_to_idx[plate_id]
        count = self.plate_stock_count[idx]  # ✅ 使用预计算值，O(1)查询
        
        return {
            'change_pct': self.section_sum_change[idx] / count if count > 0 else 0.0,
            'total_volume': self.section_total_volume[idx],
            'total_large_net': self.section_total_large_net[idx],
            'stock_count': int(count)  # ✅ 可选：返回成分股数量
        }

# 使用示例 - 使用正确的相对路径
updater = OptimizedPlateUpdater('data/板块.csv', 'data/个股板块.csv')

# 模拟股票更新
stock_updates = {
    '002520': {'change': 0.02, 'volume': 10000, 'large_net': 5000},
    '688585': {'change': -0.01, 'volume': 8000, 'large_net': -2000},
    '301261': {'change': 0.015, 'volume': 12000, 'large_net': 3000}
}

updater.update_stocks(stock_updates)

# 获取板块指标
metrics = updater.get_plate_metrics('801159')
print(f"机器人概念板块指标: {metrics}")

# ✅ 性能测试：高频查询
import time
start_time = time.time()
for _ in range(1000):  # 模拟1000次查询
    metrics = updater.get_plate_metrics('801159')
end_time = time.time()
print(f"1000次查询耗时: {end_time - start_time:.4f}秒")
print(f"单次查询平均耗时: {(end_time - start_time) * 1000:.4f}毫秒")