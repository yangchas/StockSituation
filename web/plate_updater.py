import numpy as np
import csv
import json
import os
import time
import random
import asyncio
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OptimizedPlateUpdater:
    """
    高效个股-板块多对多映射系统（支持全量模拟更新）
    """
    
    def __init__(self, plate_file, relation_file, concept_file=None):
        self.plate_file = plate_file
        self.relation_file = relation_file
        self.concept_file = concept_file
        self.load_data()
        self.build_optimized_structures()
        self.initialize_metrics()
        # 初始化模拟个股数据
        self.initialize_mock_stocks()
    
    def load_data(self):
        """加载所有数据"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 加载板块数据
        self.plates = {}
        self.plate_hierarchy = {}  # 主流板块 -> 子板块映射
        self.main_plates = []      # 主流板块列表
        self.plate_name_to_id = {}  # 板块名称到ID的映射
        
        plate_path = os.path.join(script_dir, self.plate_file)
        with open(plate_path, 'r', encoding='gbk') as f:
            reader = csv.DictReader(f)
            for row in reader:
                plate_id = row['id']
                plate_name = row['name']
                market_cap = float(row['流通值']) if row['流通值'] else 0.0
                
                # 保存名称到ID的映射
                self.plate_name_to_id[plate_name] = plate_id
                
                # 解析inner字段
                inner_data = []
                inner_str = row.get('inner', '').strip()
                if inner_str and inner_str != '[]':
                    try:
                        # 将单引号转换为双引号
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
                
                # 如果有子板块，则认为是主流板块
                if inner_data:
                    self.main_plates.append(plate_name)
                    self.plate_hierarchy[plate_name] = []
                    
                    for inner_item in inner_data:
                        if isinstance(inner_item, list) and len(inner_item) >= 2:
                            sub_plate_id = inner_item[0]
                            sub_plate_name = inner_item[1]
                            self.plate_hierarchy[plate_name].append({
                                'code': sub_plate_id,
                                'name': sub_plate_name
                            })
        
        logger.info(f"📊 加载板块数据: {len(self.plates)}个板块, {len(self.main_plates)}个主流板块")
        
        # 加载个股-板块关系
        self.stock_plate_relations = defaultdict(list)
        self.plate_stock_relations = defaultdict(list)  # 新增：板块到个股的映射
        relation_path = os.path.join(script_dir, self.relation_file)
        with open(relation_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # 跳过表头
            for row in reader:
                if len(row) >= 2:
                    plate_id, stock_id = row[0], row[1]
                    self.stock_plate_relations[stock_id].append(plate_id)
                    self.plate_stock_relations[plate_id].append(stock_id)  # 新增：建立反向映射
        
        logger.info(f"📈 加载个股关系: {len(self.stock_plate_relations)}只股票")
        
        # 加载概念数据（如果有）
        if self.concept_file:
            self.concepts = {}
            concept_path = os.path.join(script_dir, self.concept_file)
            try:
                with open(concept_path, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        self.concepts[row['id']] = row['name']
                logger.info(f"📚 加载概念数据: {len(self.concepts)}个概念")
            except Exception as e:
                logger.warning(f"加载概念数据失败: {e}")
    
    def build_optimized_structures(self):
        """构建优化数据结构"""
        self.plate_ids = list(self.plates.keys())
        self.plate_to_idx = {pid: i for i, pid in enumerate(self.plate_ids)}
        
        # 构建股票到板块索引的映射
        self.stock_to_plate_indices = {}
        for stock_id, plate_list in self.stock_plate_relations.items():
            indices = [self.plate_to_idx[pid] for pid in plate_list if pid in self.plate_to_idx]
            if indices:
                self.stock_to_plate_indices[stock_id] = np.array(indices, dtype=np.int32)
        
        logger.info(f"⚡ 优化结构构建完成: {len(self.stock_to_plate_indices)}只有效股票")
    
    def initialize_metrics(self):
        """初始化指标"""
        n_plates = len(self.plate_ids)
        
        # 核心指标数组
        self.section_sum_change = np.zeros(n_plates, dtype=np.float64)
        self.section_total_volume = np.zeros(n_plates, dtype=np.float64)
        self.section_total_large_net = np.zeros(n_plates, dtype=np.float64)
        self.section_rise_count = np.zeros(n_plates, dtype=np.int32)
        self.section_fall_count = np.zeros(n_plates, dtype=np.int32)
        
        # 当前股票状态
        self.stock_current_change = {}
        self.stock_current_volume = {}
        self.stock_current_large_net = {}
        
        # 预计算成分股数量
        self.plate_stock_count = np.zeros(n_plates, dtype=np.int32)
        for stock_id, plate_list in self.stock_plate_relations.items():
            for plate_id in plate_list:
                if plate_id in self.plate_to_idx:
                    idx = self.plate_to_idx[plate_id]
                    self.plate_stock_count[idx] += 1
        
        logger.info(f"📐 指标初始化完成: {n_plates}个板块")
    
    def initialize_mock_stocks(self):
        """初始化模拟个股数据"""
        self.mock_stocks_data = {}
        
        # 为每个板块生成模拟个股数据
        for plate_id in self.plate_ids:
            stock_count = self.plate_stock_count[self.plate_to_idx[plate_id]]
            if stock_count > 0:
                stocks = []
                # 使用实际的股票ID
                actual_stock_ids = self.plate_stock_relations.get(plate_id, [])
                for i, stock_id in enumerate(actual_stock_ids[:10]):  # 每个板块最多10只个股
                    stock_code = stock_id
                    stock_name = f"{self.plates[plate_id]['name']}个股{i+1}"
                    
                    stocks.append({
                        'code': stock_code,
                        'name': stock_name,
                        'change_pct': round(random.uniform(-0.05, 0.05), 4),
                        'price': round(random.uniform(5, 100), 2),
                        'volume': random.randint(1000000, 100000000),
                        'market_cap': random.randint(100000000, 10000000000)
                    })
                
                self.mock_stocks_data[plate_id] = stocks
        
        logger.info(f"📈 初始化模拟个股数据: {len(self.mock_stocks_data)}个板块")
    
    def update_stocks(self, stock_updates):
        """
        高效批量更新股票数据
        stock_updates: {stock_id: {'change': float, 'volume': int, 'large_net': int}}
        """
        update_start = time.time()
        
        for stock_id, update_data in stock_updates.items():
            if stock_id not in self.stock_to_plate_indices:
                continue
                
            plate_indices = self.stock_to_plate_indices[stock_id]
            
            # 更新涨跌幅
            new_change = update_data['change']
            old_change = self.stock_current_change.get(stock_id, 0.0)
            delta_change = new_change - old_change
            self.stock_current_change[stock_id] = new_change
            
            # 更新成交量
            new_volume = update_data.get('volume', 0)
            old_volume = self.stock_current_volume.get(stock_id, 0)
            delta_volume = new_volume - old_volume
            self.stock_current_volume[stock_id] = new_volume
            
            # 更新大单净额
            new_large_net = update_data.get('large_net', 0)
            old_large_net = self.stock_current_large_net.get(stock_id, 0)
            delta_large_net = new_large_net - old_large_net
            self.stock_current_large_net[stock_id] = new_large_net
            
            # 批量更新板块指标
            self.section_sum_change[plate_indices] += delta_change
            self.section_total_volume[plate_indices] += delta_volume
            self.section_total_large_net[plate_indices] += delta_large_net
            
            # 更新涨跌计数
            if new_change > 0:
                self.section_rise_count[plate_indices] += 1
            elif new_change < 0:
                self.section_fall_count[plate_indices] += 1
        
        update_time = (time.time() - update_start) * 1000
        if len(stock_updates) > 1000:
            logger.debug(f"⚡ 批量更新 {len(stock_updates)}只股票, 耗时: {update_time:.2f}ms")
    
    def get_plate_metrics(self, plate_id):
        """获取单个板块指标"""
        if plate_id not in self.plate_to_idx:
            return None
            
        idx = self.plate_to_idx[plate_id]
        count = self.plate_stock_count[idx]
        
        if count == 0:
            return None
            
        plate_info = self.plates[plate_id]
        change_pct = self.section_sum_change[idx] / count if count > 0 else 0
        
        return {
            'id': plate_id,
            'name': plate_info['name'],
            'change_pct': change_pct,
            'total_volume': self.section_total_volume[idx],
            'total_large_net': self.section_total_large_net[idx],
            'stock_count': int(count),
            'rise_count': int(self.section_rise_count[idx]),
            'fall_count': int(self.section_fall_count[idx]),
            'market_cap': plate_info['market_cap'],
            'type': 'main' if plate_info['inner'] else 'sub'
        }
    
    def get_all_plate_metrics(self):
        """获取所有板块指标"""
        all_metrics = []
        for plate_id in self.plate_ids:
            metrics = self.get_plate_metrics(plate_id)
            if metrics:
                all_metrics.append(metrics)
        return all_metrics
    
    def get_main_plates_metrics(self):
        """获取主流板块指标"""
        main_metrics = []
        for plate_name in self.main_plates:
            # 找到对应的plate_id
            for plate_id, plate_info in self.plates.items():
                if plate_info['name'] == plate_name:
                    metrics = self.get_plate_metrics(plate_id)
                    if metrics:
                        main_metrics.append(metrics)
                    break
        return main_metrics
    
    def get_sub_plates_metrics(self, main_plate_name):
        """获取指定主流板块的子板块指标 - 修复版本"""
        logger.info(f"🔍 查找主板块 '{main_plate_name}' 的子板块")
        
        # 首先通过名称找到主板块ID
        main_plate_id = self.plate_name_to_id.get(main_plate_name)
        if not main_plate_id:
            logger.warning(f"❌ 未找到主板块: {main_plate_name}")
            return []
        
        # 获取主板块的子板块信息
        main_plate_info = self.plates.get(main_plate_id)
        if not main_plate_info:
            logger.warning(f"❌ 未找到主板块信息: {main_plate_id}")
            return []
        
        sub_plates_data = main_plate_info.get('inner', [])
        sub_metrics = []
        
        for sub_plate_data in sub_plates_data:
            if isinstance(sub_plate_data, list) and len(sub_plate_data) >= 2:
                sub_plate_id = sub_plate_data[0]
                sub_plate_name = sub_plate_data[1]
                
                # 获取子板块的指标
                metrics = self.get_plate_metrics(sub_plate_id)
                if metrics:
                    sub_metrics.append(metrics)
                    logger.info(f"✅ 找到子板块: {sub_plate_name} ({sub_plate_id})")
                else:
                    logger.warning(f"❌ 子板块无指标数据: {sub_plate_name} ({sub_plate_id})")
        
        logger.info(f"📋 主板块 '{main_plate_name}' 共有 {len(sub_metrics)} 个子板块")
        return sub_metrics
    
    def get_plate_stocks(self, plate_id):
        """获取板块个股数据 - 修复版本"""
        logger.info(f"📊 获取板块 {plate_id} 的个股数据")
        
        # 检查是否有模拟数据
        if plate_id in self.mock_stocks_data:
            stocks = self.mock_stocks_data[plate_id]
            logger.info(f"✅ 返回模拟个股数据: {len(stocks)} 只股票")
            return stocks
        
        # 如果没有模拟数据，尝试从实际关系生成
        actual_stock_ids = self.plate_stock_relations.get(plate_id, [])
        stocks = []
        
        for i, stock_id in enumerate(actual_stock_ids[:20]):  # 限制返回数量
            stock_data = {
                'code': stock_id,
                'name': f"股票{stock_id}",
                'change_pct': round(random.uniform(-0.05, 0.05), 4),
                'price': round(random.uniform(5, 100), 2),
                'volume': random.randint(1000000, 100000000),
                'market_cap': random.randint(100000000, 10000000000)
            }
            stocks.append(stock_data)
        
        logger.info(f"✅ 生成个股数据: {len(stocks)} 只股票")
        return stocks
    
    def get_plate_hierarchy(self):
        """获取板块层级关系"""
        return self.plate_hierarchy, self.main_plates

class PlateDataSimulator:
    """板块数据模拟器"""
    
    def __init__(self, plate_updater, update_interval=3):
        self.plate_updater = plate_updater
        self.update_interval = update_interval
        self.running = False
        
        # 获取所有股票ID
        self.all_stock_ids = list(plate_updater.stock_plate_relations.keys())
        logger.info(f"🎯 模拟器初始化: {len(self.all_stock_ids)}只股票")
    
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
            
            stock_updates[stock_id] = {
                'change': change,
                'volume': volume,
                'large_net': large_net
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

# 测试函数
def test_plate_updater():
    """测试板块更新器"""
    logger.info("🧪 测试板块更新器...")
    
    # 初始化
    updater = OptimizedPlateUpdater('data/板块.csv', 'data/个股板块.csv', 'data/概念.csv')
    
    # 测试子板块查找
    logger.info("🔍 测试子板块查找...")
    sub_plates = updater.get_sub_plates_metrics('机器人概念')
    logger.info(f"机器人概念子板块: {len(sub_plates)}个")
    for sub in sub_plates[:3]:  # 显示前3个
        logger.info(f"  - {sub['name']}: {sub['change_pct']:+.2%}")
    
    # 测试个股数据
    logger.info("📊 测试个股数据...")
    stocks = updater.get_plate_stocks('801159')
    logger.info(f"机器人概念个股: {len(stocks)}只")
    for stock in stocks[:3]:  # 显示前3个
        logger.info(f"  - {stock['name']}({stock['code']}): {stock['change_pct']:+.2%}")
    
    # 生成测试数据
    test_updates = {}
    sample_stocks = list(updater.stock_plate_relations.keys())[:10]  # 取前10只股票测试
    
    for stock_id in sample_stocks:
        test_updates[stock_id] = {
            'change': random.uniform(-0.05, 0.05),
            'volume': random.randint(10000, 100000),
            'large_net': random.randint(-5000, 5000)
        }
    
    # 更新测试
    updater.update_stocks(test_updates)
    
    # 检查结果
    logger.info("✅ 测试更新完成")
    
    # 打印板块层级
    hierarchy, main_plates = updater.get_plate_hierarchy()
    logger.info(f"🏗️ 板块层级: {len(main_plates)}个主流板块")
    for main_plate in main_plates[:3]:  # 显示前3个
        sub_count = len(hierarchy[main_plate])
        logger.info(f"  📁 {main_plate}: {sub_count}个子板块")
    
    # 打印板块指标
    all_metrics = updater.get_all_plate_metrics()
    logger.info(f"📈 总板块数: {len(all_metrics)}")
    
    for metrics in all_metrics[:5]:  # 显示前5个
        logger.info(f"  📊 {metrics['name']}: {metrics['change_pct']:+.2%}")

if __name__ == "__main__":
    test_plate_updater()