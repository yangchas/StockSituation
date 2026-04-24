
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List

try:
    from web.services.tdengine_service import TDengineService
    from web.redis_storage import RedisStorageManager
except ImportError:
    import sys
    import os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
    from web.services.tdengine_service import TDengineService
    from web.redis_storage import RedisStorageManager, REDIS_UNIT_FACTOR


logger = logging.getLogger(__name__)

class OptimizedAdvancedTechnicalIndicators:
    """优化版高级技术指标服务 - 减少重复字段，降低计算频率"""
    
    def __init__(self, tdengine_service, redis_storage):
        self.tdengine = tdengine_service
        self.redis_storage = redis_storage
        self.calculated_indicators = {}  # 缓存已计算的指标
        self.last_calculation_time = {}  # 记录上次计算时间
        
    def get_stock_advanced_indicators_optimized(self, symbol: str, force_recalc: bool = False) -> Dict:
        """获取个股的高级技术指标 - 优化版本"""
        try:
            # 检查缓存和是否需要重新计算
            cache_key = f"advanced_indicators_{symbol}"
            current_time = time.time()
            
            # 如果不在强制重算且缓存有效（5秒内），直接返回缓存
            if not force_recalc and symbol in self.calculated_indicators:
                last_time = self.last_calculation_time.get(symbol, 0)
                if current_time - last_time < 5:  # 5秒缓存
                    return self.calculated_indicators[symbol]
            
            # 从Redis获取基础数据
            stock_data = self.redis_storage.get_stock_data(symbol)
            if not stock_data:
                return {}
            
            # 只计算必要的核心指标，避免重复字段
            indicators = {
                # 基础字段（直接从Redis获取，确保转换为数字类型）
                'price': float(stock_data.get('price', 0)),
                'change_pct': float(stock_data.get('change_pct', 0)),
                'volume': float(stock_data.get('volume', 0)),
                'amount': float(stock_data.get('amount', 0)),
                
                # 核心高级指标（避免重复计算）
                'change_rate_1min': self._calculate_change_rate_1min(symbol),
                'amount_2min': self._calculate_amount_2min(symbol),
                
                # 从Redis直接获取的大单净额（确保转换为数字类型）
                'large_net': float(stock_data.get('large_net', 0)),
                
                # 元数据
                'timestamp': current_time,
                'update_count': 1  # 用于跟踪更新频率
            }
            
            # 更新缓存
            # 限制内存缓存大小，防止长期运行内存溢出
            if len(self.calculated_indicators) > 2000:
                # 简单清理：删除前20%的旧数据
                keys_to_remove = list(self.calculated_indicators.keys())[:400]
                for k in keys_to_remove:
                    self.calculated_indicators.pop(k, None)
                    self.last_calculation_time.pop(k, None)

            self.calculated_indicators[symbol] = indicators
            self.last_calculation_time[symbol] = current_time
            
            # 存储到Redis（短期缓存）
            self.redis_storage.store_data(
                cache_key, indicators, expire_seconds=10
            )
            
            return indicators
            
        except Exception as e:
            logger.error(f"❌ 获取个股高级指标失败 {symbol}: {e}")
            return {}
    
    def _calculate_change_rate_1min(self, symbol: str) -> float:
        """计算1分钟涨速 - 优化版"""
        try:
            # 从TDengine获取最近2分钟的收盘价，按时间倒序排列
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=2)
            
            # 获取最近的两个价格点
            sql = f"""
            SELECT lp as price
            FROM stock_data 
            WHERE symbol = '{symbol}' 
                AND ts >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
                AND ts <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY ts DESC
            LIMIT 2
            """
            
            cursor = self.tdengine.execute_query(sql)
            if not cursor:
                return 0.0
            
            rows = cursor.fetchall()
            if not rows or len(rows) < 2:
                return 0.0
            
            # 计算涨速
            if rows[0][0] and rows[1][0] and rows[1][0] > 0:
                return ((rows[0][0] - rows[1][0]) / rows[1][0]) * 100
            
            return 0.0
            
        except Exception:
            return 0.0
    
    def _calculate_amount_2min(self, symbol: str) -> float:
        """计算2分钟成交额 - 优化版"""
        try:
            # 直接从Redis获取最近的数据，避免频繁查询数据库
            cache_key = f"amount_2min_{symbol}"
            cached = self.redis_storage.get_data(cache_key)
            
            if cached:
                return float(cached)
            
            # 必要时从TDengine计算
            end_time = datetime.now()
            start_time = end_time - timedelta(minutes=2)
            
            sql = f"""
            SELECT SUM(a) as total_amount
            FROM stock_data 
            WHERE symbol = '{symbol}' 
                AND ts >= '{start_time.strftime('%Y-%m-%d %H:%M:%S')}'
                AND ts <= '{end_time.strftime('%Y-%m-%d %H:%M:%S')}'
            """
            
            cursor = self.tdengine.execute_query(sql)
            if not cursor:
                return 0.0
            
            rows = cursor.fetchall()
            if rows and rows[0][0]:
                amount = float(rows[0][0])
                # 缓存结果
                self.redis_storage.store_data(cache_key, amount, expire_seconds=60)
                return amount
            
            return 0.0
            
        except Exception:
            return 0.0
    
    def _get_effective_ref_time(self, ref_time: datetime) -> datetime:
        """如果当前时间不在交易时段，回退到最近的交易时段末尾"""
        t = ref_time.strftime("%H:%M")
        if t < "09:30":
            # 开盘前，用前一天的 15:00
            yesterday = ref_time - timedelta(days=1)
            return yesterday.replace(hour=15, minute=0, second=0, microsecond=0)
        elif "11:35" < t < "13:00":
            # 午休，用今天 11:35
            return ref_time.replace(hour=11, minute=35, second=0, microsecond=0)
        elif t > "15:05":
            # 收盘后，用今天 15:00 (适当延迟几分钟确保数据入库)
            return ref_time.replace(hour=15, minute=0, second=0, microsecond=0)
        return ref_time  # 盘中，使用当前时间

    def _batch_fetch_td_indicators(self, symbols: List[str], ref_time: datetime) -> Dict[str, Dict]:
        """[特色查询重构] 利用 PARTITION BY 和 INTERVAL 实现全量时间切片获取 (P2 Power Query)"""
        results = {}
        if not symbols:
            return results

        # 获取有效参考时间
        eff_ref_time = self._get_effective_ref_time(ref_time)
        
        # 定义 2 分钟窗口（包含 2 个 1 分钟区间）
        end_str = eff_ref_time.strftime('%Y-%m-%d %H:%M:%S')
        start_2m = (eff_ref_time - timedelta(minutes=2)).strftime('%Y-%m-%d %H:%M:%S')

        try:
            # 核心：使用时序特色查询。
            # PARTITION BY symbol: 底层并行扫描每个股票序列
            # INTERVAL(1m): 自动切分为 1 分钟窗口计算，FILL(PREV) 填充心跳缺失
            sql = f"""
            SELECT _wstart, symbol, LAST(a) - FIRST(a) as amt, FIRST(lp) as f_p, LAST(lp) as l_p 
            FROM stock_data 
            WHERE ts >= '{start_2m}' AND ts <= '{end_str}'
            PARTITION BY symbol 
            INTERVAL(1m) 
            FILL(PREV)
            """
            
            cursor = self.tdengine.execute_query(sql)
            if cursor:
                rows = cursor.fetchall()
                
                # 内存聚合：将窗口数据合并为最终指标
                temp_data = {} # {symbol: [row1, row2]}
                for row in rows:
                    sym = row[1]
                    temp_data.setdefault(sym, []).append(row)
                
                for sym, windows in temp_data.items():
                    # 按时间轴排序（_wstart）
                    windows.sort(key=lambda x: x[0])
                    
                    # 1. 2分钟成交额 = 所有窗口成交额累加
                    amt_2min = sum(w[2] for w in windows if w[2] is not None)
                    
                    # 2. 1分钟涨速 = 最后一个有效窗口的涨跌幅
                    # 取最后 1 个窗口（即最新的 1 分钟）
                    last_w = windows[-1]
                    f_p, l_p = last_w[3], last_w[4]
                    rate = 0.0
                    if f_p and f_p > 0 and l_p:
                        rate = ((l_p - f_p) / f_p) * 100
                        
                    results[sym] = {
                        'amount_2min': float(amt_2min / 10000.0), # 转为万元
                        'change_rate_1min': round(rate, 4)
                    }
                
                if not results and symbols:
                    logger.warning(f"⚠️ [amount_2min] TDengine 查询窗口 {start_2m} ~ {end_str} 返回空结果")
            return results
        except Exception as e:
            logger.error(f"❌ TDengine 特色分片查询失败: {e}")
            return results

    def _sync_indicators_to_redis_batch(self, td_results: Dict[str, Dict]):
        """将 TD 计算结果批量同步回 Redis 实时哈希表 (P2 Sync)"""
        try:
            # 获取采集端单位因子，确保转换逻辑闭环
            # 假设采集端存入为万元，我们需要同步写入对应的字符串
            pipeline = self.redis_storage.redis.pipeline()
            
            # 使用与 RedisStorageManager 一致的单位转换逻辑 (万元 = 元 / 10000)
            WAN_UNIT = 10000.0
            
            for symbol, metrics in td_results.items():
                key = f"stock:quote:{symbol}"
                update_map = {}
                if 'amount_2min' in metrics:
                    update_map['amount_2min'] = str(metrics['amount_2min']) # 输入已是万元
                if 'change_rate_1min' in metrics:
                    update_map['change_rate_1min'] = str(metrics['change_rate_1min'])
                
                if update_map:
                    pipeline.hset(key, mapping=update_map)
            
            pipeline.execute()
        except Exception as e:
            logger.error(f"❌ 批量同步 Redis 指标失败: {e}")

    def batch_get_stocks_advanced_indicators_optimized(self, symbols: List[str]) -> Dict[str, Dict]:
        """批量获取个股高级指标 - 内存优化版 (减少对象分配)
        
        优化说明:
        1. 只做一次 Redis Pipeline 读取 (不再写回后再读)
        2. TDengine 指标直接合并到内存结果中
        3. 使用持久化缓存 _batch_results_cache 避免每周期重建 dict
        """
        if not symbols:
            return {}
            
        try:
            # 确保持久化缓存存在
            if not hasattr(self, '_batch_results_cache'):
                self._batch_results_cache = {}

            # 🚀【CPU 优化】2 秒全局缓存，防止多个并发任务触发重复计算
            if hasattr(self, '_last_batch_results') and hasattr(self, '_last_batch_time'):
                if (datetime.now() - self._last_batch_time).total_seconds() < 2.0:
                    # 如果请求的 symbols 是上一次缓存的子集，直接返回缓存中的对应项
                    return self._last_batch_results

            current_time_dt = datetime.now()
            current_ms = time.time() * 1000
            
            # 1. 一次 Pipeline 从 Redis 获取全量基础数据
            pipeline = self.redis_storage.redis.pipeline()
            for symbol in symbols:
                pipeline.hgetall(f"stock:quote:{symbol}")
            redis_results = pipeline.execute()
            
            # 2. 预处理 Redis 结果，识别需要同步的股票代码
            missing_symbols = []
            standardized_results = {}
            
            for i, symbol in enumerate(symbols):
                raw_stock_data = redis_results[i]
                if not raw_stock_data:
                    missing_symbols.append(symbol)
                    continue
                
                # 标准化 Redis 原始数据 (从 Redis Hash 转换为 Python Dict 并处理单位)
                std = self.redis_storage._standardize_stock_quote(raw_stock_data, symbol)
                standardized_results[symbol] = std
                
                # 3. 核心指标获取 (由 C++ 推送，Python 端仅做标准化读取)
                # 不再检查缺失并回补 TDengine，因为 t1 是实时权威来源
                standardized_results[symbol] = std

            # 3. 按需从 TDengine 聚合高频指标 (仅对缺失或过期的股票)
            td_metrics = {}
            if missing_symbols:
                # logger.debug(f"🔍 [Indicator Opt] 对 {len(missing_symbols)}/{len(symbols)} 只股票触发 TDengine 补全")
                td_metrics = self._batch_fetch_td_indicators(missing_symbols, current_time_dt)
                # 同步回 Redis (异步)
                if td_metrics:
                    self._sync_indicators_to_redis_batch(td_metrics)
            
            # 4. 在内存中合并并返回结果
            results = self._batch_results_cache
            for symbol in symbols:
                # 获取 Redis 基础数据 (可能为空)
                std = standardized_results.get(symbol, {})
                
                # 合并 TDengine 兜底数据 (如果有)
                td = td_metrics.get(symbol)
                if td:
                    std.update(td)
                
                if not std: continue

                # 复用已存在的字典对象，减少 GC 压力
                existing = results.get(symbol)
                if existing is not None:
                    existing.update(std)
                else:
                    results[symbol] = std
            
            # 缓存本次批量结果
            self._last_batch_results = results
            self._last_batch_time = current_time_dt
            return results
            
        except Exception as e:
            logger.error(f"❌ 批量获取高级指标全流程失败: {e}")
            return {}

