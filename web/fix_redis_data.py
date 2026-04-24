#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复Redis中板块基础信息和股票-板块关系缺失的问题
"""

import redis
import json
import time
import logging
from typing import Dict, List

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedisDataFixer:
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        
    def fix_plate_info_and_relations(self):
        """修复板块基础信息和股票-板块关系"""
        logger.info("🔧 开始修复Redis数据...")
        
        # 1. 检查当前状态
        plate_metrics_keys = self.redis.keys("plate:metrics:*")
        logger.info(f"📊 当前板块数据数量: {len(plate_metrics_keys)}")
        
        if not plate_metrics_keys:
            logger.error("❌ 没有找到板块数据，请先启动集成服务器")
            return False
        
        # 2. 从plate_updater获取板块基础信息
        plate_infos = self._get_plate_infos_from_updater()
        if not plate_infos:
            logger.error("❌ 无法从plate_updater获取板块基础信息")
            return False
        
        # 3. 存储板块基础信息到Redis
        self._store_plate_infos(plate_infos)
        
        # 4. 存储股票-板块关系到Redis
        self._store_stock_plate_relations()
        
        logger.info("✅ Redis数据修复完成")
        return True
    
    def _get_plate_infos_from_updater(self) -> Dict[str, Dict]:
        """从plate_updater获取板块基础信息"""
        try:
            # 导入plate_updater模块
            import sys
            import os
            sys.path.append(os.path.dirname(__file__))
            
            from plate_updater import OptimizedPlateUpdater
            
            # 创建plate_updater实例
            updater = OptimizedPlateUpdater(
                plate_file="data/板块.csv",
                relation_file="data/个股板块.csv"
            )
            
            # 提取板块基础信息
            plate_infos = {}
            for plate_id, plate_info in updater.all_plates.items():
                plate_infos[plate_id] = {
                    'name': plate_info.get('name', ''),
                    'type': plate_info.get('type', 'sub'),
                    'market_cap': plate_info.get('market_cap', 0)
                }
                
                # 如果是子板块，添加父板块信息
                if 'parent' in plate_info:
                    plate_infos[plate_id]['parent'] = plate_info['parent']
            
            logger.info(f"📋 获取到 {len(plate_infos)} 个板块基础信息")
            return plate_infos
            
        except Exception as e:
            logger.error(f"❌ 获取板块基础信息失败: {e}")
            return {}
    
    def _store_plate_infos(self, plate_infos: Dict[str, Dict]):
        """存储板块基础信息到Redis"""
        pipeline = self.redis.pipeline()
        
        for plate_id, info in plate_infos.items():
            info_key = f"plate:info:{plate_id}"
            
            # 构造基础信息数据
            info_data = {
                "n": info.get("name", ""),
                "t": info.get("type", "sub"),
                "m": int(info.get("market_cap", 0))
            }
            
            # 如果是子板块，添加父板块信息
            if "parent" in info:
                info_data["p"] = info["parent"]
            
            # 存储到Redis - 使用传统的field-value方式
            for field, value in info_data.items():
                pipeline.hset(info_key, field, str(value))
            pipeline.expire(info_key, 86400 * 7)  # 7天过期
        
        # 执行批量操作
        pipeline.execute()
        logger.info(f"💾 存储 {len(plate_infos)} 个板块基础信息到Redis")
    
    def _store_stock_plate_relations(self):
        """存储股票-板块关系到Redis"""
        try:
            # 导入plate_updater模块
            import sys
            import os
            sys.path.append(os.path.dirname(__file__))
            
            from plate_updater import OptimizedPlateUpdater
            
            # 创建plate_updater实例
            updater = OptimizedPlateUpdater(
                plate_file="data/板块.csv",
                relation_file="data/个股板块.csv"
            )
            
            pipeline = self.redis.pipeline()
            relation_count = 0
            
            # 存储股票-板块关系
            for stock_id, plate_list in updater.stock_to_plates.items():
                plates_key = f"stock_plates:{stock_id}"
                
                # 先删除旧的，再添加新的
                pipeline.delete(plates_key)
                if plate_list:
                    pipeline.sadd(plates_key, *plate_list)
                    pipeline.expire(plates_key, 86400 * 7)  # 7天过期
                    relation_count += len(plate_list)
            
            # 执行批量操作
            pipeline.execute()
            logger.info(f"🔗 存储 {relation_count} 个股票-板块关系到Redis")
            
        except Exception as e:
            logger.error(f"❌ 存储股票-板块关系失败: {e}")
    
    def check_fix_result(self):
        """检查修复结果"""
        logger.info("🔍 检查修复结果...")
        
        # 检查板块基础信息
        plate_info_keys = self.redis.keys("plate:info:*")
        logger.info(f"🏷️ 板块基础信息数量: {len(plate_info_keys)}")
        
        # 检查股票-板块关系
        stock_plate_keys = self.redis.keys("stock_plates:*")
        logger.info(f"🔗 股票-板块关系数量: {len(stock_plate_keys)}")
        
        # 显示部分数据
        if plate_info_keys:
            sample_key = plate_info_keys[0]
            plate_info = self.redis.hgetall(sample_key)
            logger.info(f"📋 示例板块信息 {sample_key}: {plate_info}")
        
        if stock_plate_keys:
            sample_key = stock_plate_keys[0]
            stock_plates = self.redis.smembers(sample_key)
            logger.info(f"📊 示例股票-板块关系 {sample_key}: {list(stock_plates)[:3]}...")

def main():
    """主函数"""
    fixer = RedisDataFixer()
    
    # 检查当前状态
    print("\n📊 修复前状态:")
    plate_metrics_keys = fixer.redis.keys("plate:metrics:*")
    plate_info_keys = fixer.redis.keys("plate:info:*")
    stock_plate_keys = fixer.redis.keys("stock_plates:*")
    
    print(f"  板块数据: {len(plate_metrics_keys)} 条")
    print(f"  板块基础信息: {len(plate_info_keys)} 条")
    print(f"  股票-板块关系: {len(stock_plate_keys)} 条")
    
    # 执行修复
    if fixer.fix_plate_info_and_relations():
        # 检查修复结果
        print("\n✅ 修复后状态:")
        fixer.check_fix_result()
    else:
        print("❌ 修复失败")

if __name__ == "__main__":
    main()