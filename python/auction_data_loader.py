import asyncio
import json
import redis
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import sys

# --- 动态路径配置，确保能引用到项目其他模块 ---
# 将项目根目录（假设是Go/）添加到sys.path
# 你可能需要根据你的实际运行环境调整这个路径
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, '..')) # 假设此文件在 Go/python/ 目录下
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'ai', 'API'))
sys.path.insert(0, os.path.join(project_root, 'web'))

# --- 依赖项导入 ---
try:
    from ai.API.api import UnifiedMarketDataFetcher
    from web.integrated_server import TDengineService, TradingCalendarService # 复用已有的服务
except ImportError as e:
    print(f"[ERROR] 无法导入依赖模块，请检查sys.path设置和模块是否存在: {e}")
    # 在无法导入时提供一个优雅的退出或降级方案
    UnifiedMarketDataFetcher = None
    TDengineService = None
    TradingCalendarService = None

# --- 日志配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 核心服务类 ---

class AuctionSnapshotService:
    """ 竞价快照服务：负责从Redis读取实时快照，从TDengine读取历史快照 """
    def __init__(self, redis_config: dict, tdengine_config: dict):
        if not TradingCalendarService:
            raise ImportError("TradingCalendarService 未成功导入")
            
        self.redis_client = redis.Redis(**redis_config, decode_responses=True)
        self.tdengine_service = TDengineService(**tdengine_config)
        self.calendar_service = TradingCalendarService()
        logger.info("竞价快照服务已初始化")

    def get_latest_auction_snapshot(self, trade_date: str = None) -> dict:
        """ 从Redis获取指定交易日最新的竞价快照 """
        if not trade_date:
            trade_date = datetime.now().strftime("%Y%m%d")

        try:
            latest_key = f"market:auction:{trade_date}:latest"
            latest_tag_info = self.redis_client.hgetall(latest_key)
            
            if not latest_tag_info or 'tag' not in latest_tag_info:
                logger.warning(f"在Redis中未找到 {trade_date} 的最新竞价快照指针: {latest_key}")
                return {}

            tag = latest_tag_info['tag']
            snapshot_key = f"market:auction:{trade_date}:{tag}"
            snapshot_data = self.redis_client.hgetall(snapshot_key)

            if not snapshot_data:
                logger.warning(f"根据指针未找到快照数据: {snapshot_key}")
                return {}

            # 反序列化JSON字段
            for field in ['summary', 'top_amount', 'meta']:
                if field in snapshot_data:
                    snapshot_data[field] = json.loads(snapshot_data[field])
            
            logger.info(f"成功从Redis获取最新竞价快照 (tag: {tag})")
            return snapshot_data
        except Exception as e:
            logger.error(f"从Redis获取最新竞价快照失败: {e}")
            return {}

    def get_historical_auction_snapshot(self, trade_date: str, tag: str = '0925') -> dict:
        """ 从TDengine获取指定历史交易日的竞价快照（用于对比） """
        summary_query = f"SELECT * FROM market_auction_summary WHERE trade_date = '{trade_date}' AND tag = '{tag}' LIMIT 1"
        top_amount_query = f"SELECT * FROM market_auction_top_amount WHERE trade_date = '{trade_date}' AND tag = '{tag}'"

        try:
            summary_cursor = self.tdengine_service.execute_query(summary_query)
            top_amount_cursor = self.tdengine_service.execute_query(top_amount_query)

            if not summary_cursor or not top_amount_cursor:
                logger.warning(f"查询TDengine失败，无法获取 {trade_date} 的历史快照")
                return {}

            # 处理汇总数据
            summary_data = {}
            summary_fields = [field[0] for field in summary_cursor.description]
            summary_row = summary_cursor.fetchone()
            if summary_row:
                summary_data = dict(zip(summary_fields, summary_row))

            # 处理TopN数据
            top_amount_data = []
            top_amount_fields = [field[0] for field in top_amount_cursor.description]
            top_amount_rows = top_amount_cursor.fetchall()
            for row in top_amount_rows:
                top_amount_data.append(dict(zip(top_amount_fields, row)))

            logger.info(f"成功从TDengine获取 {trade_date} (tag: {tag}) 的历史竞价快照")
            return {
                'summary': summary_data,
                'top_amount': top_amount_data
            }
        except Exception as e:
            logger.error(f"从TDengine获取历史竞价快照失败: {e}")
            return {}

    def get_comparison_snapshots(self) -> dict:
        """ 获取用于对比预期差的快照：今日最新 vs 昨日0925 """
        today_str = datetime.now().strftime('%Y-%m-%d')
        yesterday_trade_date = self.calendar_service.get_previous_trading_day(today_str)
        yesterday_trade_date_yyyymmdd = yesterday_trade_date.replace('-', '')

        latest_today = self.get_latest_auction_snapshot()
        historical_yesterday = self.get_historical_auction_snapshot(yesterday_trade_date_yyyymmdd, '0925')

        return {
            'today': latest_today,
            'yesterday': historical_yesterday
        }

class LimitUpBaselineService:
    """ 连板/首板基准服务：负责从问财获取每日基准数据 """
    def __init__(self, wencai_cookie: str):
        if not UnifiedMarketDataFetcher:
            raise ImportError("UnifiedMarketDataFetcher 未成功导入")
        self.fetcher = UnifiedMarketDataFetcher(wencai_cookie=wencai_cookie)
        logger.info("连板/首板基准服务已初始化")

    async def fetch_daily_baselines(self) -> dict:
        """ 获取并处理当天的涨停、连板、首板基准数据 """
        try:
            # 并行获取数据
            limitup_task = self.fetcher.get_wencai_limitup_with_lb_days()
            first_limitup_task = self.fetcher.get_wencai_first_limit()
            
            df_limitup, df_first_limitup = await asyncio.gather(limitup_task, first_limitup_task)

            # --- 数据清洗与结构化 ---
            # 1. 处理所有涨停（包含连板天数）
            limitup_map = {}
            if not df_limitup.empty and 'code6' in df_limitup.columns and 'lb_days' in df_limitup.columns:
                # 确保 lb_days 是整数，无法转换的为1（问财对首板可能返回NaN）
                df_limitup['lb_days'] = pd.to_numeric(df_limitup['lb_days'], errors='coerce').fillna(1).astype(int)
                limitup_map = df_limitup.set_index('code6')['lb_days'].to_dict()

            # 2. 处理首板
            first_limitup_set = set()
            if not df_first_limitup.empty and 'code6' in df_first_limitup.columns:
                first_limitup_set = set(df_first_limitup['code6'])

            # 3. 补充：确保所有首板的连板天数都为1
            for code in first_limitup_set:
                if code not in limitup_map or limitup_map[code] != 1:
                    limitup_map[code] = 1
            
            logger.info(f"成功从问财获取基准数据：总涨停 {len(limitup_map)} 只，其中首板 {len(first_limitup_set)} 只")

            return {
                'limitup_map': limitup_map, # {code6: lb_days}
                'first_limitup_set': first_limitup_set, # {code6, ...}
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"获取连板/首板基准数据失败: {e}")
            return {}

# --- 主执行入口（用于独立测试） ---

async def main():
    """ 用于独立测试此模块的功能 """
    # --- 配置 (建议从配置文件或环境变量加载) ---
    REDIS_CONFIG = {
        'host': 'localhost',
        'port': 6379,
        'db': 0
    }
    TDENGINE_CONFIG = {
        'host': 'localhost',
        'port': 6030,
        'user': 'root',
        'password': 'taosdata',
        'database': 'market_data1'
    }
    # 你的问财Cookie
    WENCAI_COOKIE = "your_wencai_cookie_here"

    # --- 测试竞价快照服务 ---
    logger.info("\n--- 正在测试 AuctionSnapshotService ---")
    auction_service = AuctionSnapshotService(REDIS_CONFIG, TDENGINE_CONFIG)
    
    # 1. 获取最新快照
    latest_snapshot = auction_service.get_latest_auction_snapshot()
    if latest_snapshot:
        print(f"最新快照 (summary): {latest_snapshot.get('summary')}")
        print(f"最新快照 (top 5 amount): {latest_snapshot.get('top_amount', [])[:5]}")

    # 2. 获取对比快照
    comparison_data = auction_service.get_comparison_snapshots()
    if comparison_data.get('today') and comparison_data.get('yesterday'):
        print("\n成功获取用于对比的今昨快照数据。")
        # 在这里可以加入预期差计算逻辑

    # --- 测试连板/首板基准服务 ---
    if WENCAI_COOKIE != "your_wencai_cookie_here":
        logger.info("\n--- 正在测试 LimitUpBaselineService ---")
        baseline_service = LimitUpBaselineService(wencai_cookie=WENCAI_COOKIE)
        baselines = await baseline_service.fetch_daily_baselines()
        if baselines:
            print(f"连板天数 (前5): {dict(list(baselines.get('limitup_map', {}).items())[:5])}")
            print(f"首板集合 (前5): {list(baselines.get('first_limitup_set', set())[:5])}")
    else:
        logger.warning("\n请在脚本中配置你的WENCAI_COOKIE以测试连板/首板基准服务")

if __name__ == '__main__':
    # 确保在Windows上asyncio可以正常运行
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
