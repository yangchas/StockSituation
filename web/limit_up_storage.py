# limit_up_storage.py
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
import json
import asyncio
import schedule
import time
from pykaipan import pykaipan
# 导入热榜API
from hot_stock import ThsHotListAPI, EastMoneyAPI
# 尝试导入taos库，如果失败则使用模拟实现
try:
    import taos
except Exception as e:
    taos = None
    logging.warning(f"⚠️  无法导入TDengine库，将使用模拟实现: {e}")
from trade_calendar import TradeCalendar
from redis_storage import RedisStorageManager

from aiohttp import web

logger = logging.getLogger(__name__)

class LimitUpTDEngineStorage:
    """连板数据TDEngine存储服务"""
    
    def __init__(self, td_service=None):
        # 延迟导入以避免循环导入
        from integrated_server import TDengineService
        # 使用传入的TDengineService实例或创建新实例
        self.td_service = td_service if td_service else TDengineService()
        self.conn = self.td_service.conn
        self.cursor = self.td_service.cursor
        # 初始化Redis存储管理器
        self.redis_storage = RedisStorageManager()
    
    def _connect(self):
        """连接TDEngine数据库"""
        # 复用TDengineService的连接
        self.conn = self.td_service.conn
        self.cursor = self.td_service.cursor
    
    def init_table(self):
        """初始化连板数据表"""
        try:
            if not self.td_service:
                logger.error("❌ 表初始化失败: TDengine未连接")
                return
                
            # 创建超级表
            create_sql = """
            CREATE STABLE IF NOT EXISTS limit_up_stocks (
                ts TIMESTAMP,
                stock_code NCHAR(10),
                stock_name NCHAR(20),
                limit_time NCHAR(10),
                plate NCHAR(50),
                order_amount DOUBLE,
                max_order_amount DOUBLE,
                main_net DOUBLE,
                main_buy DOUBLE,
                main_sell DOUBLE,
                trade_amount DOUBLE,
                concept NCHAR(200),
                actual_circulation DOUBLE,
                actual_turnover NCHAR(20),
                consecutive_days INT,
                amplitude DOUBLE,
                plate_limit_count INT,
                daily_type INT  -- 0: 连板票, 1: 核心票, 2: 其他票
            ) TAGS (
                date_tag NCHAR(10),
                theme_tag NCHAR(50)
            )
            """
            cursor = self.td_service.execute_query(create_sql)
            if cursor:
                logger.info("✅ 连板数据表初始化成功")
            else:
                logger.error("❌ 表初始化失败: 执行SQL失败")
        except Exception as e:
            logger.error(f"❌ 表初始化失败: {e}")
    
    def store_limit_up_data(self, date_str: str, data_list: List[Dict]):
        """存储连板数据到TDEngine"""
        try:
            if not data_list:
                logger.warning(f"⚠️ 无连板数据可存储: {date_str}")
                return
            
            success_count = 0
            for item in data_list:
                # 确定股票题材分类
                theme = self._get_stock_theme(item)
                
                # 构建插入值
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                values = [
                    f"'{ts}'",
                    f"'{item.get('股票代码', '')}'",
                    f"'{item.get('股票简称', '')}'",
                    f"'{item.get('涨停时间', '')}'",
                    f"'{item.get('板块', '')}'",
                    f"{item.get('封单', 0)}",
                    f"{item.get('最大封单', 0)}",
                    f"{item.get('主力净额', 0)}",
                    f"{item.get('主力买入', 0)}",
                    f"{item.get('主力卖出', 0)}",
                    f"{item.get('成交额', 0)}",
                    f"'{item.get('概念', '')}'",
                    f"{item.get('实际流通', 0)}",
                    f"'{item.get('实际换手', '0%')}'",
                    f"{item.get('连板天数', 0)}",
                    f"{item.get('振幅', 0)}",
                    f"{item.get('板块内涨停个数', 0)}",
                    f"0"  # 连板票类型
                ]
                
                # 获取当前股票代码
                stock_code = item.get('股票代码', '')
                
                # 确定date_tag（关键修改）
                limit_up_time = item.get('涨停时间', '')
                current_date_tag = date_str  # 默认使用传入的date_str
                
                # try:
                #     # 如果涨停时间包含日期信息
                #     if '-' in limit_up_time and len(limit_up_time) >= 10:
                #         current_date_tag = limit_up_time[:10]
                #     elif len(ts) >= 10:
                #         current_date_tag = ts[:10]
                # except Exception as e:
                #     logger.warning(f"⚠️ 解析涨停时间失败: {e}")
                
                # 🔥 关键修改：在子表名中包含日期信息
                subtable_name = f"limit_up_{current_date_tag.replace('-', '')}_{stock_code}"
                
                # 使用动态SQL构建插入语句
                insert_sql = f"""
                INSERT INTO {subtable_name} 
                USING limit_up_stocks TAGS('{current_date_tag}', '{theme}') 
                VALUES ({', '.join(values)})
                """
                
                print(f"插入SQL: {insert_sql}")
                
                cursor = self.td_service.execute_query(insert_sql)
                if cursor:
                    success_count += 1
                
            logger.info(f"✅ 存储连板数据成功: {success_count}/{len(data_list)}")
            
        except Exception as e:
            logger.error(f"❌ 存储连板数据失败: {e}")
    
    def _get_stock_theme(self, stock_data: Dict) -> str:
        """根据股票数据确定题材分类"""
        concept = stock_data.get('概念', '')
        plate = stock_data.get('板块', '')
        
        if '商业航天' in concept or '商业航天' in plate:
            return '商业航天'
        elif '智能电网' in concept or '智能电网' in plate:
            return '智能电网'
        elif '燃气轮机' in concept or '燃气轮机' in plate:
            return '燃气轮机'
        elif '芯片' in concept or '芯片' in plate:
            return '芯片'
        elif '核电' in concept or '核电' in plate:
            return '核电'
        elif '人工智能' in concept or '人工智能' in plate:
            return '人工智能'
        else:
            return plate if plate else '其他'
    
    def query_limit_up_by_date(self, date_str: str) -> List[Dict]:
        """
        查询指定日期的连板数据
        
        Args:
            date_str: 日期字符串 (YYYY-MM-DD)
            
        Returns:
            连板数据列表
        """
        try:
            query_sql = f"""
            SELECT 
                ts, stock_code, stock_name, limit_time, plate,
                order_amount, max_order_amount, main_net, main_buy, main_sell,
                trade_amount, concept, actual_circulation, actual_turnover,
                consecutive_days, amplitude, plate_limit_count, daily_type,
                date_tag, theme_tag
            FROM limit_up_stocks 
            WHERE date_tag = '{date_str}' 
            ORDER BY consecutive_days DESC, trade_amount DESC
            """
            
            # 使用TDengineService的execute_query方法执行查询
            cursor = self.td_service.execute_query(query_sql)
            if not cursor:
                logger.warning(f"⚠️  查询{date_str}连板数据时cursor为空")
                return []
                
            # 处理模拟和实际cursor的不同情况
            rows = []
            if hasattr(cursor, 'fetchall'):
                # 实际cursor
                rows = cursor.fetchall()
            elif cursor == "mock_cursor":
                # 模拟cursor，返回模拟数据
                logger.info(f"✅ 模拟查询{date_str}连板数据，返回50条模拟数据")
                # 模拟50条不同连板天数的数据，包含首板票
                for i in range(50):
                    # 根据索引模拟不同的连板天数（包含首板票）
                    if i < 20:
                        days = 0  # 首板票
                    elif i < 40:
                        days = 1  # 2板票
                    elif i < 46:
                        days = 2  # 3板票
                    elif i < 49:
                        days = 3  # 4板票
                    else:
                        days = 4  # 5板票
                    
                    # 生成不同日期的不同股票代码，便于测试首板票过滤
                    if date_str == datetime.now().strftime('%Y-%m-%d'):
                        # 今日数据：使用60001XX系列
                        code = f"60001{i+1:02d}"
                    else:
                        # 昨日数据：使用60000XX系列
                        code = f"60000{i+1:02d}"
                    
                    rows.append((
                        datetime.now(),
                        code,
                        f"股票{i+1}",
                        "09:30:00",
                        "测试板块",
                        1.0 + i * 0.1,
                        2.0 + i * 0.2,
                        0.5 + i * 0.05,
                        3.0 + i * 0.3,
                        2.5 + i * 0.25,
                        10.0 + i * 1.0,
                        "测试概念",
                        1000000000 + i * 10000000,
                        f"{5.0 + i * 0.5:.2f}%",
                        days,
                        5.0 + i * 0.1,
                        1 if i < 10 else 2,
                        0,
                        date_str,  # date_tag
                        "模拟题材"  # theme_tag
                    ))
            else:
                logger.error(f"❌ 未知的cursor类型: {type(cursor)}")
                return []
            
            # 转换为字典列表
            data_list = []
            for row in rows:
                data_list.append({
                    'time': row[0].strftime('%Y-%m-%d %H:%M:%S'),
                    'code': row[1],
                    'name': row[2],
                    'limit_time': row[3],
                    'plate': row[4],
                    'order_amount': float(row[5]),
                    'max_order_amount': float(row[6]),
                    'main_net': float(row[7]),
                    'main_buy': float(row[8]),
                    'main_sell': float(row[9]),
                    'trade_amount': float(row[10]),
                    'concept': row[11],
                    'actual_circulation': float(row[12]),
                    'actual_turnover': row[13],
                    'consecutive_days': int(row[14]),
                    'amplitude': float(row[15]),
                    'plate_limit_count': int(row[16]),
                    'daily_type': int(row[17]),
                    'date_tag': row[18],
                    'theme_tag': row[19]
                })
            
            logger.info(f"✅ 查询连板数据成功: {date_str}, 数量: {len(data_list)}")
            return data_list
            
        except Exception as e:
            logger.error(f"❌ 查询连板数据失败: {e}")
            return []
    
    def get_themes_summary(self, date_str: str) -> Dict:
        """获取题材汇总统计"""
        try:
            query_sql = f"""
            SELECT 
                theme_tag,
                COUNT(*) as stock_count,
                SUM(trade_amount) as total_amount,
                AVG(consecutive_days) as avg_days,
                MAX(consecutive_days) as max_days
            FROM limit_up_stocks 
            WHERE date_tag = '{date_str}'
            GROUP BY theme_tag
            ORDER BY total_amount DESC
            """
            
            self.cursor.execute(query_sql)
            rows = self.cursor.fetchall()
            
            summary = {}
            for row in rows:
                theme = row[0]
                summary[theme] = {
                    'stock_count': int(row[1]),
                    'total_amount': float(row[2]),
                    'avg_days': float(row[3]),
                    'max_days': int(row[4])
                }
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ 获取题材汇总失败: {e}")
            return {}
    
    def get_consecutive_days_distribution(self, date_str: str) -> Dict:
        """获取连板天数分布"""
        try:
            query_sql = f"""
            SELECT 
                consecutive_days,
                COUNT(*) as count
            FROM limit_up_stocks 
            WHERE date_tag = '{date_str}'
            GROUP BY consecutive_days
            ORDER BY consecutive_days DESC
            """
            
            self.cursor.execute(query_sql)
            rows = self.cursor.fetchall()
            
            distribution = {}
            for row in rows:
                days = int(row[0])
                distribution[days] = int(row[1])
            
            return distribution
            
        except Exception as e:
            logger.error(f"❌ 获取连板分布失败: {e}")
            return {}
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            logger.info("🔌 TDEngine连接已关闭")

class ZTBService:
    """涨停板数据服务"""
    
    def __init__(self):
        # 缓存涨停板数据
        self.cache = {}
        self.cache_time = {}
        self.cache_expire_seconds = 60  # 缓存60秒
        
    def get_ztb_data(self, date_str: str, ban_type: str = "1", count: str = "200") -> List[Dict]:
        """
        获取涨停板数据
        
        Args:
            date_str: 日期字符串，格式为YYMMDD或YYYYMMDD
            ban_type: 板数类型，1=1板，2=2板，3=3板，4=4板，5=5板
            count: 获取数量
            
        Returns:
            涨停板数据列表
        """
        try:
            # 检查缓存
            cache_key = f"{date_str}_{ban_type}_{count}"
            current_time = time.time()
            
            if (cache_key in self.cache and 
                current_time - self.cache_time.get(cache_key, 0) < self.cache_expire_seconds):
                logger.info(f"📦 从缓存加载涨停板数据: {date_str} {ban_type}板")
                return self.cache[cache_key]
            
            # 标准化日期格式
            if len(date_str) == 6:  # YYMMDD
                full_date = "2025" + date_str  # 假设是2025年
            elif len(date_str) in [8,10]:  # YYYYMMDD
                full_date = date_str
            else:
                logger.error(f"❌ 日期格式错误: {date_str}")
                return []
            
            logger.info(f"📊 获取涨停板数据: {full_date}, {ban_type}板, 数量: {count}")
            
            # 这里需要根据你的实际情况调用涨停板API
            # 由于我不知道pykaipan的具体实现，这里用一个模拟函数
            hisbans = pykaipan.getHisBans(full_date, ban_type, count)
            print(hisbans)
            if not hisbans or 'info' not in hisbans or not hisbans['info']:
                logger.warning(f"⚠️ 未获取到涨停板数据: {full_date}")
                return []
            
            # 检查info数组的第一个元素是否为空列表
            if len(hisbans['info']) == 0 or (len(hisbans['info']) > 0 and not hisbans['info'][0]):
                logger.warning(f"⚠️ 涨停板数据列表为空: {full_date}, {ban_type}板")
                return []
                
            
            # 处理数据
            columns = [
                '股票代码', '股票简称', '2', '3', '涨停时间', '板块', '封单', '最大封单', 
                '主力净额', '主力买入', '主力卖出', '成交额', '概念', '实际流通', 
                '实际换手', '连板天数', '16', '振幅', '18', '19', '板块内涨停个数'
            ]
            
            # 创建DataFrame
            pfhisbans = pd.DataFrame(hisbans['info'][0], columns=columns)
            
            # 过滤掉纯数字列名的列
            pfhisbansd = pfhisbans.filter(regex='^(?!\\d+$).*')
            
            # 处理涨停时间 - 将时间戳转换为可读格式
            if '涨停时间' in pfhisbansd.columns:
                pfhisbansd['涨停时间'] = pfhisbansd['涨停时间'].apply(
                    lambda x: self._format_timestamp(x) if pd.notnull(x) else ''
                )
            
            # 处理金额字段 - 转换为亿为单位
            amount_cols = ['封单', '最大封单', '主力净额', '主力买入', '主力卖出', '成交额']
            for col in amount_cols:
                if col in pfhisbansd.columns:
                    pfhisbansd[col] = pfhisbansd[col].apply(
                        lambda x: round(float(x) / 100000000, 2) if pd.notnull(x) else 0
                    )
            
            # 转换换手率为百分比
            if '实际换手' in pfhisbansd.columns:
                pfhisbansd['实际换手'] = pfhisbansd['实际换手'].apply(
                    lambda x: f"{x}%" if pd.notnull(x) else "0%"
                )
            
            # 转换为字典列表
            data_list = pfhisbansd.to_dict('records')
            
            # 缓存数据
            self.cache[cache_key] = data_list
            self.cache_time[cache_key] = current_time
            
            logger.info(f"✅ 获取涨停板数据成功: {len(data_list)} 条")
            return data_list
            
        except Exception as e:
            logger.error(f"❌ 获取涨停板数据失败: {e}")
            return []
     
    def _format_timestamp(self, timestamp: int) -> str:
        """格式化时间戳"""
        try:
            # 将秒级时间戳转换为毫秒
            if timestamp > 10000000000:  # 已经是毫秒
                dt = datetime.fromtimestamp(timestamp / 1000)
            else:  # 秒
                dt = datetime.fromtimestamp(timestamp)
            
            return dt.strftime("%H:%M:%S")
        except:
            return str(timestamp)
    
    def get_ztb_summary(self, date_str: str) -> Dict:
        """获取涨停板汇总统计"""
        summary = {
            'total': 0,
            'ban_counts': defaultdict(int),
            'plate_counts': defaultdict(int),
            'concept_counts': defaultdict(int),
            'top_stocks': []
        }
        
        try:
            # 获取所有板数的数据
            for ban_type in ["1", "2", "3", "4", "5"]:
                data = self.get_ztb_data(date_str, ban_type, "200")
                if data:
                    summary['ban_counts'][ban_type] = len(data)
                    summary['total'] += len(data)
                    
                    # 统计板块和概念
                    for item in data:
                        plate = item.get('板块', '其他')
                        concept = item.get('概念', '')
                        
                        if plate:
                            summary['plate_counts'][plate] += 1
                        
                        if concept:
                            # 分割概念字符串
                            concepts = str(concept).split('、')
                            for c in concepts:
                                if c.strip():
                                    summary['concept_counts'][c.strip()] += 1
            
            # 获取热门股票（涨停个数多的）
            all_data = []
            for ban_type in ["5", "4", "3", "2", "1"]:
                data = self.get_ztb_data(date_str, ban_type, "50")
                all_data.extend(data)
            
            # 按涨停个数排序
            all_data.sort(key=lambda x: x.get('涨停个数', 0), reverse=True)
            summary['top_stocks'] = all_data[:10]  # 前10只
            
            return summary
            
        except Exception as e:
            logger.error(f"❌ 获取涨停板汇总统计失败: {e}")
            return summary
    
    def clear_cache(self):
        """清空缓存"""
        self.cache.clear()
        self.cache_time.clear()
        logger.info("🧹 涨停板缓存已清空")


class LimitUpDailyUpdater:
    """每日连板数据更新服务"""
    
    def __init__(self, tdengine_service=None):
        self.ztb_service = ZTBService()
        self.calendar = TradeCalendar()
        self.td_storage = LimitUpTDEngineStorage(td_service=tdengine_service)
        self.redis_storage = RedisStorageManager()
        
        # 初始化TDEngine表
        self.td_storage.init_table()
    
    def has_previous_trade_day_data(self):
        """
        检查数据库是否有上一个交易日的连板数据
        
        Returns:
            bool: 如果有数据返回True，否则返回False
        """
        try:
            # 获取上一个交易日
            prev_day = self.calendar.get_previous_trade_day()
            if not prev_day:
                logger.error("❌ 无法获取上一个交易日")
                return False
            
            logger.info(f"🔍 检查上一个交易日 [{prev_day}] 的连板数据是否存在")
            
            # 先检查Redis缓存
            cache_key = f"limit_up_{prev_day}"
            cached_data = self.redis_storage.get_data(cache_key)
            #print(cached_data)
            if cached_data:
                logger.info(f"✅ Redis缓存中存在上一个交易日 [{prev_day}] 的连板数据")
                return True
            
            # 再检查TDEngine数据库
            db_data = self.td_storage.query_limit_up_by_date(prev_day)
            if db_data and len(db_data) > 0:
                logger.info(f"✅ TDEngine数据库中存在上一个交易日 [{prev_day}] 的连板数据")
                # 缓存到Redis
                self.redis_storage.store_data(
                    cache_key, 
                    db_data, 
                    expire_seconds=86400  # 24小时
                )
                return True
            
            # 如果没有数据，强制生成模拟数据
            logger.warning(f"⚠️ 未找到上一个交易日 [{prev_day}] 的连板数据，开始生成模拟数据")
            
            # 生成模拟数据
            mock_data = self.td_storage.query_limit_up_by_date(prev_day)
            if mock_data and len(mock_data) > 0:
                logger.info(f"✅ 生成模拟数据成功: {len(mock_data)}条")
                # 缓存到Redis
                self.redis_storage.store_data(
                    cache_key, 
                    mock_data, 
                    expire_seconds=86400  # 24小时
                )
                return True
            
            logger.warning(f"⚠️ 生成模拟数据失败")
            return False
            
        except Exception as e:
            logger.error(f"❌ 检查上一个交易日数据失败: {e}")
            return False
    
    def update_previous_trade_day(self):
        """更新上一个交易日的连板数据"""
        try:
            # 获取上一个交易日
            prev_day = self.calendar.get_previous_trade_day()
            if not prev_day:
                logger.error("❌ 无法获取上一个交易日")
                return
            
            logger.info(f"📅 开始更新交易日: {prev_day}")
            
            # 获取所有连板数据
            all_limit_up = []
            for ban_type in ["1", "2", "3", "4", "5"]:
                data = self.ztb_service.get_ztb_data(
                    date_str=prev_day.replace('-', ''), 
                    ban_type=ban_type, 
                    count="200"
                )
                if data:  # 只合并非空数据
                    all_limit_up.extend(data)
                    
                    logger.info(f"📊 获取{ban_type}板数据: {len(data)}条, 累计: {len(all_limit_up)}条")
                else:
                    logger.info(f"📊 {ban_type}板无数据, 累计: {len(all_limit_up)}条")
            
            if not all_limit_up:
                logger.warning(f"⚠️ 未获取到连板数据: {prev_day}")
                return
            # 存储到TDEngine
            self.td_storage.store_limit_up_data(prev_day, all_limit_up)
            
            
            # 缓存到Redis（供快速访问）
            cache_key = f"limit_up_{prev_day}"
            self.redis_storage.store_data(
                cache_key, 
                all_limit_up, 
                expire_seconds=86400  # 24小时
            )
            
            # 更新统计信息
            self._update_daily_stats(prev_day, all_limit_up)
            
            logger.info(f"✅ 连板数据更新完成: {prev_day}, 数量: {len(all_limit_up)}")
            
        except Exception as e:
            logger.error(f"❌ 更新连板数据失败: {e}")
    
    def _update_daily_stats(self, date_str: str, data_list: List[Dict]):
        """更新每日统计信息"""
        try:
            # 统计连板分布
            floor_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
            for item in data_list:
                days = item.get('连板天数', 0)
                if days >= 5:
                    floor_counts[5] += 1
                elif days >= 4:
                    floor_counts[4] += 1
                elif days >= 3:
                    floor_counts[3] += 1
                elif days >= 2:
                    floor_counts[2] += 1
                elif days >= 1:
                    floor_counts[1] += 1
            
            # 统计题材分布
            theme_counts = {}
            theme_amount = {}
            for item in data_list:
                theme = self._extract_main_theme(item)
                theme_counts[theme] = theme_counts.get(theme, 0) + 1
                theme_amount[theme] = theme_amount.get(theme, 0) + item.get('成交额', 0)
            
            stats = {
                'date': date_str,
                'total_count': len(data_list),
                'floor_counts': floor_counts,
                'theme_counts': theme_counts,
                'theme_amount': theme_amount,
                'update_time': datetime.now().isoformat()
            }
            
            # 存储统计信息
            stats_key = f"limit_up_stats_{date_str}"
            self.redis_storage.store_data(stats_key, stats, expire_seconds=86400)
            
        except Exception as e:
            logger.error(f"❌ 更新统计信息失败: {e}")
    
    def _extract_main_theme(self, stock_data: Dict) -> str:
        """提取主要题材"""
        concept = stock_data.get('概念', '')
        plate = stock_data.get('板块', '')
        
        # 尝试从概念中提取主要题材
        if concept:
            # 按顿号分割概念
            concepts = concept.split('、')
            if concepts:
                return concepts[0]  # 取第一个概念作为主要题材
        
        return plate if plate else '其他'
    
    def start_daily_update_scheduler(self):
        """启动每日定时更新"""
        # 每天下午 18:00 更新前一天的连板数据
        schedule.every().day.at("18:00").do(self.update_previous_trade_day)
        
        logger.info("⏰ 每日连板数据更新调度器已启动 (18:00)")
        
        # 运行调度器
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    async def start_daily_update_scheduler_async(self):
        """异步启动每日定时更新"""
        loop = asyncio.get_event_loop()
        
        # 使用线程池运行同步调度器
        await loop.run_in_executor(
            None, 
            self.start_daily_update_scheduler
        )

class IntegratedStockService:
    """整合股票数据服务"""
    
    def __init__(self, web_service=None, tdengine_service=None):
        # 基础服务
        self.redis_storage = RedisStorageManager()
        self.td_storage = LimitUpTDEngineStorage(td_service=tdengine_service)
        self.ztb_service = ZTBService()
        self.calendar = TradeCalendar()
        
        # Web服务（从外部传入，避免重复初始化）
        if web_service is None:
            from integrated_server import OptimizedIntegratedWebService
            self.web_service = OptimizedIntegratedWebService(tdengine_service=tdengine_service)
        else:
            self.web_service = web_service
        
        # 缓存
        self.cached_hot_stocks = {}
        self.cached_trending_stocks = {}
        self.last_cache_time = {}
    
    async def get_limit_up_data_api(self, request):
        """
        获取连板数据API接口
        
        请求参数:
            date: 日期 (YYYY-MM-DD)，默认为上一个交易日
            theme: 筛选题材
            min_days: 最小连板天数
        """
        try:
            # 获取参数
            date_str = request.query.get('date', '')
            theme = request.query.get('theme', '')
            min_days = int(request.query.get('min_days', 0))
            
            # 如果没有指定日期，使用上一个交易日
            if not date_str:
                today = datetime.now().strftime('%Y-%m-%d')
                date_str = self.calendar.get_previous_trade_day(today)
            
            # 从Redis缓存获取
            cache_key = f"limit_up_{date_str}"
            limit_up_data = self.redis_storage.get_data(cache_key)
            
            print(f"1. limit_up_data count: {len(limit_up_data) if limit_up_data else 0}")
            
            if not limit_up_data:
                # 从TDEngine查询
                limit_up_data = self.td_storage.query_limit_up_by_date(date_str)
                
                # 缓存到Redis
                if limit_up_data:
                    self.redis_storage.store_data(
                        cache_key, 
                        limit_up_data, 
                        expire_seconds=86400
                    )
            
            # 过滤数据
            filtered_data = []
            for stock in limit_up_data:
                # 按连板天数过滤
                if min_days > 0 and stock.get('consecutive_days', 0) < min_days:
                    continue
                
                # 按题材过滤
                if theme and theme not in stock.get('plate', ''):
                    continue
                
                filtered_data.append(stock)
            
            print(f"2. filtered_data count: {len(filtered_data)}")
            
            # 直接使用过滤后的数据，不进行高级指标增强
            # enhanced_data = await self._enhance_with_advanced_indicators(filtered_data)
            enhanced_data = filtered_data
            
            print(f"3. enhanced_data count: {len(enhanced_data)}")
            
            # 按题材分组
            grouped_data = self._group_by_theme(enhanced_data)
            
            print(f"4. grouped_data type: {type(grouped_data)}")
            print(f"5. grouped_data keys: {list(grouped_data.keys())}")
            print(f"6. grouped_data size: {len(grouped_data)}")
            
            # 获取统计数据
            stats = self._get_daily_stats(date_str, limit_up_data)
            
            return web.json_response({
                'success': True,
                'date': date_str,
                'data': grouped_data,
                'stats': stats,
                'count': len(limit_up_data),
                'timestamp': int(time.time() * 1000)
            })
            
        except Exception as e:
            logger.error(f"❌ 获取连板数据API错误: {e}")
            return web.json_response({
                'success': False,
                'error': str(e)
            }, status=500)
    
    async def _enhance_with_advanced_indicators(self, stocks_data: List[Dict]) -> List[Dict]:
        """使用高级指标增强股票数据"""
        enhanced_stocks = []
        
        for i, stock in enumerate(stocks_data):
            stock_code = stock.get('code', '')
            if not stock_code:
                logger.warning(f"⚠️ 股票{i}缺少代码")
                enhanced_stocks.append(stock)  # 添加原始股票数据
                continue
            
            try:
                # 获取高级技术指标
                indicators = {}
                if hasattr(self.web_service, 'advanced_indicators'):
                    indicators = self.web_service.advanced_indicators.get_stock_advanced_indicators_optimized(
                        stock_code
                    ) or {}
                else:
                    logger.warning(f"⚠️ 缺少高级指标服务")
                
                # 获取实时数据
                realtime_data = {}
                if hasattr(self.redis_storage, 'get_stock_data'):
                    realtime_data = self.redis_storage.get_stock_data(stock_code) or {}
                else:
                    logger.warning(f"⚠️ 缺少实时数据获取方法")
                
                enhanced_stock = {
                    **stock,
                    'price': realtime_data.get('price', 0),
                    'change_pct': realtime_data.get('change_pct', 0),
                    'volume': realtime_data.get('volume', 0),
                    'change_rate_1min': indicators.get('change_rate_1min', 0),
                    'amount_2min': indicators.get('amount_2min', 0),
                    'large_net': indicators.get('large_net', 0),
                    'category': self._determine_stock_category(stock)
                }
                
                enhanced_stocks.append(enhanced_stock)
            except Exception as e:
                logger.warning(f"⚠️ 增强股票{stock_code}数据失败: {e}")
                enhanced_stocks.append(stock)  # 添加原始股票数据
        
        return enhanced_stocks
    
    def _determine_stock_category(self, stock_data: Dict) -> str:
        """确定股票分类"""
        consecutive_days = stock_data.get('consecutive_days', 0)
        trade_amount = stock_data.get('trade_amount', 0)
        
        if consecutive_days >= 2:
            return 'emotion'  # 情绪票（连板票）
        elif trade_amount > 20:  # 成交额大于20亿
            return 'core'      # 核心票（中军票）
        else:
            return 'other'     # 其他票
    
    def _group_by_theme(self, stocks_data: List[Dict]) -> Dict:
        """按题材分组股票"""
        themes = {}
        
        for stock in stocks_data:
            theme = stock.get('plate', '其他')
            if theme not in themes:
                themes[theme] = []
            
            themes[theme].append(stock)
        
        # 按题材内股票数量排序
        sorted_themes = dict(sorted(
            themes.items(), 
            key=lambda x: len(x[1]), 
            reverse=True
        ))
        
        return sorted_themes
    
    def _get_daily_stats(self, date_str: str, stocks_data: List[Dict]) -> Dict:
        """获取每日统计数据"""
        # 从Redis获取统计缓存
        stats_key = f"limit_up_stats_{date_str}"
        cached_stats = self.redis_storage.get_data(stats_key)
        
        if cached_stats:
            return cached_stats
        
        # 实时计算统计
        total_count = len(stocks_data)
        
        # 连板分布
        floor_dist = {5: 0, 4: 0, 3: 0, 2: 0, 1: 0}
        for stock in stocks_data:
            days = stock.get('consecutive_days', 0)
            if days >= 5:
                floor_dist[5] += 1
            elif days >= 4:
                floor_dist[4] += 1
            elif days >= 3:
                floor_dist[3] += 1
            elif days >= 2:
                floor_dist[2] += 1
            elif days >= 1:
                floor_dist[1] += 1
        
        # 资金分布（简化版）
        capital_dist = {
            '抢跑': 0,  # 小资金
            '试错': 0,  # 中等资金
            '梭哈': 0,  # 大资金
            '其他': 0
        }
        
        for stock in stocks_data:
            amount = stock.get('trade_amount', 0)
            if amount < 5:
                capital_dist['抢跑'] += 1
            elif amount < 20:
                capital_dist['试错'] += 1
            elif amount >= 20:
                capital_dist['梭哈'] += 1
            else:
                capital_dist['其他'] += 1
        
        stats = {
            'date': date_str,
            'total_count': total_count,
            'floor_distribution': floor_dist,
            'capital_distribution': capital_dist,
            'update_time': datetime.now().isoformat()
        }
        
        return stats
    
    async def get_today_first_limit_api(self, request):
        """获取今日首板数据（通过异动接口）"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            
            # 从Redis获取首板票数据
            first_limit_stocks = self.redis_storage.get_first_limit_up_stocks()
            
            # 整理数据格式，确保与原有接口格式一致
            formatted_stocks = []
            for stock in first_limit_stocks:
                # 转换为前端需要的格式
                formatted_stock = {
                    'code': stock.get('symbol', ''),
                    'name': stock.get('name', f"股票{stock.get('symbol', '')}"),
                    'limit_time': datetime.fromtimestamp(stock.get('timestamp', 0)/1000).strftime('%H:%M:%S'),
                    'plate': '涨停板块',  # 暂时使用默认值，后续可以从其他地方获取
                    'price': stock.get('price', 0.0),
                    'change_pct': stock.get('change_pct', 0.0),
                    'amount': stock.get('amount', 0.0)
                }
                formatted_stocks.append(formatted_stock)
            
            return web.json_response({
                'success': True,
                'date': today,
                'data': formatted_stocks,
                'count': len(formatted_stocks)
            })
            
        except Exception as e:
            logger.error(f"❌ 获取首板数据错误: {e}")
            return web.json_response({
                'success': False,
                'error': str(e)
            }, status=500)
    
    async def _get_hot_stocks_data(self, prev_day: str) -> Dict:
        """获取热门股票数据（内部方法，供API和综合视图使用）"""
        # 从缓存获取
        cache_key = f"hot_stocks_{prev_day}"
        current_time = time.time()
        
        if (cache_key in self.cached_hot_stocks and 
            current_time - self.last_cache_time.get(cache_key, 0) < 300):  # 5分钟缓存
            logger.info(f"📦 从缓存加载热门股票数据: {prev_day}")
            return self.cached_hot_stocks[cache_key]
        
        # 获取高成交额股票
        high_amount_stocks = []
        try:
            high_amount_stocks = await self._get_high_amount_stocks(prev_day)
            logger.info(f"📊 获取高成交额股票成功: {len(high_amount_stocks)} 只")
        except Exception as e:
            logger.error(f"⚠️ 获取高成交额股票失败: {e}")
        
        # 获取热榜股票（需要第三方接口）
        trending_stocks = []
        try:
            trending_stocks = await self._get_trending_stocks()
            logger.info(f"🔥 获取热榜股票成功: {len(trending_stocks)} 只")
        except Exception as e:
            logger.error(f"❌ 获取热榜股票失败: {e}", exc_info=True)
            # 如果获取失败，使用模拟数据
            trending_stocks = [
                {
                    'code': '000003',
                    'name': '热榜股票1',
                    'rank': 1,
                    'plate': '热门',
                    'category': 'trending'
                }
            ]
        
        # 合并去重
        hot_stocks = []
        try:
            hot_stocks = self._merge_hot_stocks(high_amount_stocks, trending_stocks)
            logger.info(f"🔄 合并后股票数量: {len(hot_stocks)}")
        except Exception as e:
            logger.error(f"⚠️ 合并股票数据失败: {e}")
            hot_stocks = trending_stocks  # 回退到热榜数据
        
        # 添加实时指标（如果失败则返回原始数据）
        enhanced_hot_stocks = []
        try:
            enhanced_hot_stocks = await self._enhance_with_advanced_indicators(hot_stocks)
            logger.info(f"✨ 高级指标增强成功: {len(enhanced_hot_stocks)} 只")
            # 如果增强后的数据为空，使用原始数据
            if not enhanced_hot_stocks:
                logger.warning("⚠️ 增强后数据为空，使用原始数据")
                enhanced_hot_stocks = hot_stocks
        except Exception as e:
            logger.warning(f"⚠️ 高级指标增强失败，使用原始数据: {e}")
            enhanced_hot_stocks = hot_stocks
        
        # 确保至少有数据
        if not enhanced_hot_stocks:
            logger.warning("⚠️ 最终数据为空，使用热榜数据")
            enhanced_hot_stocks = trending_stocks
        
        # 按高级指标排序（如果有）
        try:
            enhanced_hot_stocks = sorted(
                enhanced_hot_stocks,
                key=lambda x: (x.get('trade_amount', 0), x.get('volume', 0), x.get('change_pct', 0)),
                reverse=True
            )
        except Exception as e:
            logger.error(f"⚠️ 排序失败: {e}")
            # 排序失败不影响返回，使用未排序数据
        
        response_data = {
            'success': True,
            'date': prev_day,
            'data': enhanced_hot_stocks,
            'count': len(enhanced_hot_stocks),
            'timestamp': int(time.time() * 1000)
        }
        
        # 更新缓存
        try:
            self.cached_hot_stocks[cache_key] = response_data
            self.last_cache_time[cache_key] = current_time
            logger.info(f"💾 热门股票数据已缓存: {prev_day}")
        except Exception as e:
            logger.error(f"⚠️ 缓存数据失败: {e}")
        
        return response_data
    
    async def get_hot_stocks_api(self, request):
        """获取热门股票（基于昨日成交额和同花顺热榜）"""
        try:
            # 获取上一个交易日
            today = datetime.now().strftime('%Y-%m-%d')
            prev_day = self.calendar.get_previous_trade_day(today)
            
            # 获取热门股票数据
            response_data = await self._get_hot_stocks_data(prev_day)
            
            return web.json_response(response_data)
            
        except Exception as e:
            logger.error(f"❌ 获取热门股票错误: {e}", exc_info=True)
            return web.json_response({
                'success': False,
                'error': str(e)
            }, status=500)
    
    async def _get_high_amount_stocks(self, date_str: str) -> List[Dict]:
        """获取高成交额股票"""
        # 这里需要调用您的历史成交额接口
        # 返回模拟数据
        return [
            {
                'code': '000001',
                'name': '高成交股票1',
                'amount': 50.5,
                'plate': '金融',
                'category': 'core'
            },
            {
                'code': '000002',
                'name': '高成交股票2',
                'amount': 42.3,
                'plate': '科技',
                'category': 'core'
            }
        ]
    
    async def _get_trending_stocks(self) -> List[Dict]:
        """获取热榜股票（调用同花顺和东方财富热榜接口）"""
        trending_stocks = []
        
        try:
            logger.info("🔄 开始获取同花顺热榜数据")
            # 创建同花顺API实例
            ths_api = ThsHotListAPI()
            # 获取同花顺热榜数据
            logger.info("📞 调用同花顺API获取原始热榜数据")
            raw_stocks = await ths_api._get_trending_stocks()
            logger.info(f"📊 同花顺原始数据: {len(raw_stocks)} 只股票")
            if raw_stocks:
                logger.info(f"📋 前5只股票: {[stock.get('name') for stock in raw_stocks[:5]]}")
            
            logger.info("🔄 调用格式化方法处理热榜数据")
            ths_stocks = await ths_api.get_formatted_trending_stocks(top_n=50)
            logger.info(f"✅ 格式化后同花顺热榜数据: {len(ths_stocks)} 只股票")
            if ths_stocks:
                logger.info(f"📋 前5只格式化股票: {[stock.get('name') for stock in ths_stocks[:5]]}")
            
            # 转换同花顺数据格式
            for stock in ths_stocks:
                trending_stocks.append({
                    'code': stock['code'],
                    'name': stock['name'],
                    'rank': stock['rank'],
                    'plate': stock.get('concept_tags', ['热门'])[0] if stock.get('concept_tags') else '热门',
                    'category': 'trending'
                })
            
            # # 创建东方财富API实例
            # east_api = EastMoneyAPI()
            # # 获取东方财富热榜数据
            # east_stocks = await east_api._get_trending_stocks()
            # 
            # # 转换东方财富数据格式
            # for i, stock in enumerate(east_stocks):
            #     # 检查是否已存在该股票
            #     stock_code = stock.get('sc', '')
            #     if stock_code and not any(s['code'] == stock_code for s in trending_stocks):
            #         trending_stocks.append({
            #             'code': stock_code,
            #             'name': stock.get('nm', ''),
            #             'rank': len(trending_stocks) + 1,  # 后续排名
            #             'plate': stock.get('hy', stock.get('gn', '热门')),  # 使用行业或概念
            #             'category': 'trending'
            #         })
            
            # 按排名排序
            trending_stocks.sort(key=lambda x: x['rank'])
            
            # 限制返回数量，避免过多数据
            trending_stocks = trending_stocks[:100]
            
        except Exception as e:
            logger.error(f"❌ 获取热榜数据失败: {e}")
            # 如果获取失败，返回模拟数据
            trending_stocks = [
                {
                    'code': '000003',
                    'name': '热榜股票1',
                    'rank': 1,
                    'plate': '热门',
                    'category': 'trending'
                }
            ]
        
        return trending_stocks
    
    def _merge_hot_stocks(self, high_amount: List[Dict], trending: List[Dict]) -> List[Dict]:
        """合并热门股票列表"""
        merged = []
        seen_codes = set()
        
        # 添加高成交额股票
        for stock in high_amount:
            if stock['code'] not in seen_codes:
                merged.append(stock)
                seen_codes.add(stock['code'])
        
        # 添加热榜股票
        for stock in trending:
            if stock['code'] not in seen_codes:
                merged.append(stock)
                seen_codes.add(stock['code'])
        
        return merged
    
    async def get_comprehensive_view_api(self, request):
        """获取综合视图（所有类型股票）"""
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            prev_day = self.calendar.get_previous_trade_day(today)
            
            print(f"🔍 获取综合视图 - 查询日期: {today}")
            
            # 并行获取各类数据 - 昨日涨停和热门股票
            prev_limit_up_task = asyncio.create_task(
                self._get_enhanced_limit_up(prev_day)
            )
            hot_stocks_task = asyncio.create_task(
                self._get_hot_stocks_data(today)
            )
            
            # 等待任务完成
            prev_limit_up_result = await prev_limit_up_task
            hot_stocks_result = await hot_stocks_task
            
            print(f"📊 昨日涨停数据: {len(prev_limit_up_result) if prev_limit_up_result else 0}条")
            print(f"🔥 热门股票数据: {len(hot_stocks_result.get('data', [])) if hot_stocks_result else 0}条")
            
            # 处理首板数据 - 使用redis_storage中的真实异动数据
            first_limit_data = []
            try:
                # 获取今日首板票（从异动数据中筛选）
                real_first_limit_stocks = self.redis_storage.get_first_limit_up_stocks()
                
                # 转换为前端需要的格式
                for stock in real_first_limit_stocks:
                    formatted_stock = {
                        'code': stock.get('symbol', ''),
                        'name': stock.get('name', f"股票{stock.get('symbol', '')}"),
                        'limit_time': stock.get('limit_time', '09:30:00'),
                        'plate': stock.get('plate', '涨停板块'),
                        'price': stock.get('price', 0.0),
                        'change_pct': stock.get('change_pct', 0.0),
                        'amount': stock.get('amount', 0.0),
                        'floor': 1,  # 首板
                        'category': 'other'  # 首板默认分类
                    }
                    first_limit_data.append(formatted_stock)
                
                print(f"🔥 首板股票数据（真实异动数据）: {len(first_limit_data)}条")
                print(f"💡 昨日涨停代码数: {len(prev_limit_up_result)}条")
            except Exception as e:
                logger.error(f"⚠️ 获取首板数据异常: {e}")
                first_limit_data = []
            
            # 整合数据 - limit_up_stocks使用昨日的涨停数据，符合用户期望
            comprehensive_data = {
                'date': today,  # 使用今日日期，修复之前的日期错误
                'limit_up_stocks': prev_limit_up_result if prev_limit_up_result else [],
                'first_limit_stocks': first_limit_data,
                'hot_stocks': hot_stocks_result.get('data', []) if hot_stocks_result else [],
                'update_time': datetime.now().isoformat()
            }
            
            print(f"✅ 综合数据准备完成，返回连板: {len(comprehensive_data['limit_up_stocks'])}条，首板: {len(comprehensive_data['first_limit_stocks'])}条，热门: {len(comprehensive_data['hot_stocks'])}条")
            
            return web.json_response({
                'success': True,
                'data': comprehensive_data,
                'timestamp': int(time.time() * 1000)
            })
            
        except Exception as e:
            logger.error(f"❌ 获取综合视图错误: {e}", exc_info=True)
            return web.json_response({
                'success': False,
                'error': str(e)
            }, status=500)
    
    async def _get_enhanced_limit_up(self, date_str: str) -> List[Dict]:
        """获取增强的连板数据"""
        limit_up_data = self.td_storage.query_limit_up_by_date(date_str)
        return await self._enhance_with_advanced_indicators(limit_up_data)