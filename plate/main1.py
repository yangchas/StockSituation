import numpy as np
import csv
import json
import os
from collections import defaultdict

class OptimizedPlateUpdater:
    """
    高效个股-板块多对多映射系统
    - 支持5000+股票，100+板块，2.5万+关系
    - 每tick毫秒级更新，O(1)查询
    - 统一处理所有板块（包括inner子板块）
    - 预计算+向量化，极致性能优化
    """
    
    def __init__(self, plate_file, relation_file):
        """
        初始化板块更新器
        
        Args:
            plate_file: 板块CSV文件路径
            relation_file: 个股-板块关系CSV文件路径
        """
        self.load_data(plate_file, relation_file)
        self.build_optimized_structures()
        self.initialize_metrics()
    
    def load_data(self, plate_file, relation_file):
        """加载板块数据和个股-板块关系"""
        # 获取脚本所在目录，构建绝对路径
        script_dir = os.path.dirname(os.path.abspath(__file__))
        plate_path = os.path.join(script_dir, plate_file)
        relation_path = os.path.join(script_dir, relation_file)
        
        # 加载板块数据 (使用GBK编码)
        self.plates = {}
        with open(plate_path, 'r', encoding='gbk') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # 解析inner字段（用于展示层级关系，不影响更新逻辑）
                inner = json.loads(row['inner'].replace("'", '"')) if row['inner'] else []
                self.plates[row['id']] = {
                    'name': row['name'], 
                    'market_cap': float(row['流通值']), 
                    'inner': inner  # 仅用于前端展示
                }
        
        # 加载个股-板块关系 (使用UTF-8编码)
        self.stock_plate_relations = defaultdict(list)
        with open(relation_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # 跳过标题行
            for plate_id, stock_id in reader:
                self.stock_plate_relations[stock_id].append(plate_id)
    
    def build_optimized_structures(self):
        """
        构建优化的数据结构
        - 板块ID到连续索引映射
        - 股票到板块索引列表（预计算，避免运行时循环）
        """
        # 板块ID到索引映射（用于numpy数组索引）
        self.plate_ids = list(self.plates.keys())
        self.plate_to_idx = {pid: i for i, pid in enumerate(self.plate_ids)}
        
        # 股票到板块索引列表（预计算，O(1)查询）
        self.stock_to_plate_indices = {}
        for stock_id, plate_list in self.stock_plate_relations.items():
            indices = [self.plate_to_idx[pid] for pid in plate_list if pid in self.plate_to_idx]
            if indices:  # 只存储有关联的股票
                self.stock_to_plate_indices[stock_id] = np.array(indices, dtype=np.int32)
    
    def initialize_metrics(self):
        """
        初始化指标数组（numpy向量化存储）
        - 预计算每个板块的成分股数量，避免实时遍历
        """
        n_plates = len(self.plate_ids)
        
        # 板块指标数组（numpy向量化，支持O(1)批量更新）
        self.section_sum_change = np.zeros(n_plates, dtype=np.float64)      # 涨跌幅累加
        self.section_total_volume = np.zeros(n_plates, dtype=np.float64)    # 总成交量
        self.section_total_large_net = np.zeros(n_plates, dtype=np.float64) # 总大单净额
        
        # 股票状态缓存
        self.stock_current_change = {}
        
        # ✅ 预计算每个板块的成分股数量（关键优化！）
        self.plate_stock_count = np.zeros(n_plates, dtype=np.int32)
        for stock_id, plate_list in self.stock_plate_relations.items():
            for plate_id in plate_list:
                if plate_id in self.plate_to_idx:
                    idx = self.plate_to_idx[plate_id]
                    self.plate_stock_count[idx] += 1
    
    def update_stocks(self, stock_updates):
        """
        高效批量更新股票数据
        
        Args:
            stock_updates: dict {stock_id: {'change': 0.02, 'volume': 1000, 'large_net': 500}}
        """
        for stock_id, update_data in stock_updates.items():
            if stock_id not in self.stock_to_plate_indices:
                continue  # 跳过无关股票
                
            # 获取该股票关联的板块索引
            plate_indices = self.stock_to_plate_indices[stock_id]
            
            # 计算涨跌幅增量（避免全量重新计算）
            new_change = update_data['change']
            old_change = self.stock_current_change.get(stock_id, 0.0)
            delta_change = new_change - old_change
            self.stock_current_change[stock_id] = new_change
            
            # ✅ 向量化更新：单行代码更新所有关联板块
            self.section_sum_change[plate_indices] += delta_change
            self.section_total_volume[plate_indices] += update_data.get('volume', 0)
            self.section_total_large_net[plate_indices] += update_data.get('large_net', 0)
    
    def get_plate_metrics(self, plate_id):
        """
        获取指定板块的实时指标 - O(1)查询时间复杂度
        
        Args:
            plate_id: 板块ID
            
        Returns:
            dict: 板块指标 {'change_pct': 0.02, 'total_volume': 100000, ...}
        """
        if plate_id not in self.plate_to_idx:
            return None
            
        idx = self.plate_to_idx[plate_id]
        count = self.plate_stock_count[idx]
        
        return {
            'name': self.plates[plate_id]['name'],
            'change_pct': self.section_sum_change[idx] / count if count > 0 else 0.0,
            'total_volume': self.section_total_volume[idx],
            'total_large_net': self.section_total_large_net[idx],
            'stock_count': int(count),
            'market_cap': self.plates[plate_id]['market_cap']
        }
    
    def get_multiple_plate_metrics(self, plate_ids):
        """
        批量获取多个板块指标
        
        Args:
            plate_ids: 板块ID列表
            
        Returns:
            dict: {plate_id: metrics_dict}
        """
        results = {}
        for plate_id in plate_ids:
            results[plate_id] = self.get_plate_metrics(plate_id)
        return results
    
    def get_top_plates_by_change(self, top_n=10):
        """
        获取涨跌幅排名前N的板块
        
        Args:
            top_n: 返回前N个板块
            
        Returns:
            list: [(plate_id, change_pct), ...]
        """
        # 计算平均涨跌幅
        avg_changes = []
        for i, plate_id in enumerate(self.plate_ids):
            count = self.plate_stock_count[i]
            if count > 0:
                avg_change = self.section_sum_change[i] / count
                avg_changes.append((plate_id, avg_change))
        
        # 按涨跌幅排序
        avg_changes.sort(key=lambda x: abs(x[1]), reverse=True)
        return avg_changes[:top_n]
    
    def get_system_status(self):
        """获取系统状态信息"""
        return {
            'total_plates': len(self.plate_ids),
            'total_stocks': len(self.stock_to_plate_indices),
            'total_relations': sum(len(indices) for indices in self.stock_to_plate_indices.values()),
            'avg_plates_per_stock': np.mean([len(indices) for indices in self.stock_to_plate_indices.values()]) if self.stock_to_plate_indices else 0
        }

# 使用示例
def main():
    """演示系统使用方法"""
    # 初始化系统
    updater = OptimizedPlateUpdater('data/板块.csv', 'data/个股板块.csv')
    
    # 模拟股票实时更新（每3秒一次）
    stock_updates = {
        '002520': {'change': 0.02, 'volume': 10000, 'large_net': 5000},
        '688585': {'change': -0.01, 'volume': 8000, 'large_net': -2000},
        '301261': {'change': 0.015, 'volume': 12000, 'large_net': 3000},
        '605018': {'change': 0.005, 'volume': 5000, 'large_net': 1000}
    }
    
    # 批量更新股票数据
    updater.update_stocks(stock_updates)
    
    # 获取单个板块指标
    metrics = updater.get_plate_metrics('801159')
    print(f"机器人概念板块指标: {metrics}")
    
    # 批量获取多个板块指标
    plate_metrics = updater.get_multiple_plate_metrics(['801159', '801313', '801045'])
    print(f"多个板块指标: {plate_metrics}")
    
    # 获取涨跌幅排名前5的板块
    top_plates = updater.get_top_plates_by_change(5)
    print(f"涨跌幅前5板块: {top_plates}")
    
    # 系统状态
    status = updater.get_system_status()
    print(f"系统状态: {status}")

if __name__ == "__main__":
    main()