import asyncio
import json
import pandas as pd
import numpy as np
from datetime import datetime
import redis.asyncio as redis
from typing import Dict, List, Optional
import os

class PlateDataService:
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis = None
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.plate_hierarchy = {}
        
        # 板块数据结构
        self.plate_keys = {
            'main_plates': 'plate:main_list',
            'plate_metrics': 'plate:metrics:',
            'plate_stocks': 'plate:stocks:',
            'sub_plates': 'plate:sub:',
            'plate_hierarchy': 'plate:hierarchy'
        }
    
    async def connect_redis(self):
        """连接Redis"""
        self.redis = await redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=0,
            decode_responses=True,
            encoding='utf-8'
        )
        await self.redis.ping()
        print("✅ Redis连接成功")
        return self.redis
    
    def load_concept_data(self, concept_file: str):
        """加载概念板块数据"""
        try:
            if not os.path.exists(concept_file):
                print(f"⚠️ 概念文件不存在: {concept_file}，使用模拟数据")
                return self.create_mock_hierarchy()
                
            # 读取概念板块文件
            df = pd.read_csv(concept_file, encoding='gbk')
            print(f"📊 加载概念数据: {len(df)} 行")
            
            # 构建板块层级关系
            plate_hierarchy = {}
            
            for _, row in df.iterrows():
                main_plate = row.get('一级分类', '默认分类')
                sub_plate = row.get('板块名称', '未知板块')
                plate_code = row.get('板块代码', f"concept_{sub_plate}")
                
                if main_plate not in plate_hierarchy:
                    plate_hierarchy[main_plate] = []
                
                plate_hierarchy[main_plate].append({
                    'code': plate_code,
                    'name': sub_plate,
                    'main_plate': main_plate
                })
            
            self.plate_hierarchy = plate_hierarchy
            print(f"✅ 加载板块数据完成: {len(plate_hierarchy)}个主流板块")
            return plate_hierarchy
            
        except Exception as e:
            print(f"❌ 加载概念数据失败: {e}")
            return self.create_mock_hierarchy()
    
    def create_mock_hierarchy(self):
        """创建模拟板块层级"""
        self.plate_hierarchy = {
            "科技": [
                {'code': 'tech_1', 'name': '半导体', 'main_plate': '科技'},
                {'code': 'tech_2', 'name': '人工智能', 'main_plate': '科技'},
                {'code': 'tech_3', 'name': '5G通信', 'main_plate': '科技'},
                {'code': 'tech_4', 'name': '芯片', 'main_plate': '科技'}
            ],
            "医药": [
                {'code': 'medical_1', 'name': '创新药', 'main_plate': '医药'},
                {'code': 'medical_2', 'name': '医疗器械', 'main_plate': '医药'},
                {'code': 'medical_3', 'name': '中药', 'main_plate': '医药'},
                {'code': 'medical_4', 'name': '生物制药', 'main_plate': '医药'}
            ],
            "消费": [
                {'code': 'consume_1', 'name': '白酒', 'main_plate': '消费'},
                {'code': 'consume_2', 'name': '食品饮料', 'main_plate': '消费'},
                {'code': 'consume_3', 'name': '家电', 'main_plate': '消费'},
                {'code': 'consume_4', 'name': '零售', 'main_plate': '消费'}
            ],
            "新能源": [
                {'code': 'energy_1', 'name': '锂电池', 'main_plate': '新能源'},
                {'code': 'energy_2', 'name': '光伏', 'main_plate': '新能源'},
                {'code': 'energy_3', 'name': '风电', 'main_plate': '新能源'},
                {'code': 'energy_4', 'name': '储能', 'main_plate': '新能源'}
            ],
            "金融": [
                {'code': 'finance_1', 'name': '银行', 'main_plate': '金融'},
                {'code': 'finance_2', 'name': '保险', 'main_plate': '金融'},
                {'code': 'finance_3', 'name': '证券', 'main_plate': '金融'},
                {'code': 'finance_4', 'name': '互联网金融', 'main_plate': '金融'}
            ]
        }
        print("✅ 创建模拟板块层级完成")
        return self.plate_hierarchy
    
    async def update_plate_metrics(self):
        """更新板块指标数据 - 生成模拟数据"""
        try:
            timestamp = int(datetime.now().timestamp() * 1000)
            plate_data = {}
            
            # 为每个主流板块和子板块生成数据
            for main_plate, sub_plates in self.plate_hierarchy.items():
                # 主流板块数据
                main_plate_code = f"main_{main_plate}"
                plate_data[main_plate_code] = {
                    'name': main_plate,
                    'change_pct': round(np.random.uniform(-0.05, 0.05), 4),
                    'turnover': np.random.randint(10000000, 1000000000),
                    'main_net': np.random.randint(-50000000, 50000000),
                    'stock_count': np.random.randint(50, 300),
                    'rise_count': np.random.randint(0, 50),
                    'fall_count': np.random.randint(0, 50),
                    'type': 'main',
                    'timestamp': timestamp,
                    'update_time': datetime.now().isoformat()
                }
                
                # 子板块数据
                for sub_plate in sub_plates:
                    plate_data[sub_plate['code']] = {
                        'name': sub_plate['name'],
                        'main_plate': main_plate,
                        'change_pct': round(np.random.uniform(-0.08, 0.08), 4),
                        'turnover': np.random.randint(1000000, 100000000),
                        'main_net': np.random.randint(-10000000, 10000000),
                        'stock_count': np.random.randint(10, 100),
                        'rise_count': np.random.randint(0, 20),
                        'fall_count': np.random.randint(0, 20),
                        'type': 'sub',
                        'timestamp': timestamp,
                        'update_time': datetime.now().isoformat()
                    }
            
            # 存储到Redis
            for plate_code, metrics in plate_data.items():
                await self.redis.set(
                    f"{self.plate_keys['plate_metrics']}{plate_code}",
                    json.dumps(metrics, ensure_ascii=False)
                )
                
                # 更新排序集合
                await self._update_sorted_sets(plate_code, metrics)
            
            # 存储板块层级关系
            await self.redis.set(
                self.plate_keys['plate_hierarchy'],
                json.dumps(self.plate_hierarchy, ensure_ascii=False)
            )
            
            # 存储主流板块列表
            main_plates = list(self.plate_hierarchy.keys())
            await self.redis.sadd(
                self.plate_keys['main_plates'],
                *main_plates
            )
            
            print(f"✅ 更新板块指标: {len(plate_data)}个板块")
            return plate_data
            
        except Exception as e:
            print(f"❌ 更新板块指标失败: {e}")
            return {}
    
    async def _update_sorted_sets(self, plate_code: str, metrics: Dict):
        """更新排序集合"""
        try:
            # 按涨跌幅排序
            if 'change_pct' in metrics:
                await self.redis.zadd(
                    'plate:sort:change_pct',
                    {plate_code: metrics['change_pct']}
                )
            
            # 按成交额排序
            if 'turnover' in metrics:
                await self.redis.zadd(
                    'plate:sort:turnover',
                    {plate_code: metrics['turnover']}
                )
            
            # 按主力净额排序
            if 'main_net' in metrics:
                await self.redis.zadd(
                    'plate:sort:main_net',
                    {plate_code: metrics['main_net']}
                )
        except Exception as e:
            print(f"❌ 更新排序集合失败: {e}")
    
    async def get_main_plates(self) -> List[str]:
        """获取主流板块列表"""
        try:
            return list(await self.redis.smembers(self.plate_keys['main_plates']))
        except:
            return list(self.plate_hierarchy.keys())
    
    async def get_plate_hierarchy(self) -> Dict:
        """获取板块层级关系"""
        try:
            data = await self.redis.get(self.plate_keys['plate_hierarchy'])
            return json.loads(data) if data else self.plate_hierarchy
        except:
            return self.plate_hierarchy
    
    async def get_plate_metrics(self, plate_code: str) -> Optional[Dict]:
        """获取板块指标"""
        try:
            data = await self.redis.get(f"{self.plate_keys['plate_metrics']}{plate_code}")
            return json.loads(data) if data else None
        except:
            return None
    
    async def get_sorted_plates(self, sort_by: str, desc: bool = True, limit: int = 50) -> List[Dict]:
        """获取排序后的板块列表"""
        try:
            sort_key = f'plate:sort:{sort_by}'
            
            if desc:
                plate_codes = await self.redis.zrevrange(sort_key, 0, limit - 1)
            else:
                plate_codes = await self.redis.zrange(sort_key, 0, limit - 1)
            
            result = []
            for plate_code in plate_codes:
                metrics = await self.get_plate_metrics(plate_code)
                if metrics:
                    metrics['code'] = plate_code
                    result.append(metrics)
            
            return result
        except Exception as e:
            print(f"❌ 获取排序板块失败: {e}")
            return []