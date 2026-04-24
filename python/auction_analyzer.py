#!/usr/bin/env python3
import logging
import time
import json
import asyncio
from datetime import datetime, time as dt_time
from typing import Dict, Any, List, Optional
import redis
import taos
import pandas as pd
from collections import defaultdict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] - %(levelname)s - %(message)s"
)
logger = logging.getLogger("AuctionAnalysisConsumer")

class EnhancedAuctionAnalysisEngine:
    """增强版竞价分析引擎 - 二次验证异动"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.redis_client = None
        self.tdengine_conn = None
        
        # 分析阈值
        self.min_total_amount = config.get("min_total_amount", 500000)  # 最低总金额50万
        self.min_single_order = config.get("min_single_order", 100000)  # 最低单笔金额10万
        self.max_withdrawal_rate = config.get("max_withdrawal_rate", 0.8)  # 最大撤单率80%
        self.volume_ratio_threshold = config.get("volume_ratio_threshold", 3.0)  # 量比阈值
        self.price_change_threshold = config.get("price_change_threshold", 0.03)  # 价格变化阈值3%
        
        # 初始化连接
        self._init_connections()
    
    def _init_connections(self):
        """初始化Redis和TDengine连接"""
        try:
            # Redis连接
            self.redis_client = redis.Redis(
                host=self.config.get('redis_host', 'localhost'),
                port=self.config.get('redis_port', 6379),
                db=self.config.get('redis_db', 0),
                decode_responses=True
            )
            
            # TDengine连接
            self.tdengine_conn = taos.connect(
                host=self.config.get('tdengine_host', 'localhost'),
                port=self.config.get('tdengine_port', 6030),
                user=self.config.get('tdengine_user', 'root'),
                password=self.config.get('tdengine_password', 'taosdata')
            )
            self.tdengine_conn.execute("USE market_data")
            
            logger.info("Redis和TDengine连接初始化成功")
        except Exception as e:
            logger.error(f"连接初始化失败: {e}")
            raise
    
    def get_volatile_stocks(self) -> List[Dict[str, Any]]:
        """从异动池获取异动股票"""
        try:
            # 获取最近10分钟的异动股票
            cutoff_time = int(time.time() * 1000) - 10 * 60 * 1000
            volatile_data = self.redis_client.zrangebyscore(
                self.config['volatile_pool_key'],
                cutoff_time,
                int(time.time() * 1000)
            )
            
            stocks = []
            for data_str in volatile_data:
                try:
                    stock_data = json.loads(data_str)
                    stocks.append(stock_data)
                except json.JSONDecodeError:
                    continue
            
            # 按符号去重，保留最新的
            unique_stocks = {}
            for stock in stocks:
                symbol = stock['symbol']
                if symbol not in unique_stocks or stock['detect_time'] > unique_stocks[symbol]['detect_time']:
                    unique_stocks[symbol] = stock
            
            return list(unique_stocks.values())
        except Exception as e:
            logger.error(f"获取异动股票失败: {e}")
            return []
    
    def pre_filter_stocks(self, stocks: List[Dict[str, Any]]) -> List[str]:
        """预过滤股票，排除明显不符合条件的"""
        filtered_symbols = []
        
        for stock in stocks:
            symbol = stock['symbol']
            
            try:
                # 从TDengine查询该股票竞价阶段的总成交额
                total_amount = self._get_auction_total_amount(symbol)
                
                # 过滤条件1: 总成交额过低
                if total_amount < self.min_total_amount:
                    logger.debug(f"过滤 {symbol}: 总成交额过低 {total_amount}")
                    continue
                
                # 过滤条件2: 试盘行为（9:20前检测到的异动）
                if self._is_trial_behavior(symbol, stock):
                    logger.debug(f"过滤 {symbol}: 试盘行为")
                    continue
                
                filtered_symbols.append(symbol)
                
            except Exception as e:
                logger.error(f"预过滤股票 {symbol} 失败: {e}")
                continue
        
        return filtered_symbols
    
    def _is_trial_behavior(self, symbol: str, stock_data: Dict[str, Any]) -> bool:
        """判断是否为试盘行为"""
        try:
            # 如果在9:20之前检测到异动，需要进一步验证
            detect_timestamp = stock_data.get('timestamp', 0)
            detect_time = datetime.fromtimestamp(detect_timestamp / 1000).time()
            
            if detect_time < dt_time(9, 20):
                # 检查9:20之后的成交量确认
                return self._check_trial_confirmation(symbol, detect_timestamp)
            
            return False
            
        except Exception as e:
            logger.error(f"判断试盘行为失败 {symbol}: {e}")
            return False
    
    def _check_trial_confirmation(self, symbol: str, detect_timestamp: int) -> bool:
        """检查试盘确认 - 9:20后是否有持续的大单"""
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            query_sql = f"""
                SELECT tss, lp, a, v, 
                       ap1, bp1, av1, bv1
                FROM t_{self._sanitize_symbol(symbol)} 
                WHERE tss >= '{today} 09:20:00.000' AND tss <= '{today} 09:25:00.000'
                ORDER BY tss
            """
            
            result = self.tdengine_conn.query(query_sql)
            rows = result.fetch_all()
            
            if len(rows) < 3:  # 数据点太少，无法判断
                return True  # 保守策略，认为是试盘
            
            # 分析9:20后的大单情况
            large_order_count = 0
            for row in rows:
                timestamp = int(row[0].timestamp() * 1000)
                last_price = row[1]
                amount = row[2]
                volume = row[3]
                ask_price = row[4]
                bid_price = row[5]
                ask_volume = row[6]
                bid_volume = row[7]
                
                # 检查是否有大单
                bid_order_value = bid_price * bid_volume * 100
                ask_order_value = ask_price * ask_volume * 100
                
                if bid_order_value >= self.min_single_order or ask_order_value >= self.min_single_order:
                    large_order_count += 1
            
            # 如果9:20后大单数量很少，认为是试盘
            return large_order_count < 2
            
        except Exception as e:
            logger.error(f"检查试盘确认失败 {symbol}: {e}")
            return True  # 出错时保守判断为试盘
    
    def _get_auction_total_amount(self, symbol: str) -> float:
        """获取竞价阶段总成交额"""
        try:
            # 获取当天竞价阶段数据
            today = datetime.now().strftime("%Y-%m-%d")
            query_sql = f"""
                SELECT SUM(cast(a as double)) as total_amount 
                FROM t_{self._sanitize_symbol(symbol)} 
                WHERE tss >= '{today} 09:15:00.000' AND tss <= '{today} 09:25:00.000'
            """
            
            result = self.tdengine_conn.query(query_sql)
            rows = result.fetch_all()
            
            return rows[0][0] if rows and rows[0][0] else 0
            
        except Exception as e:
            logger.error(f"查询 {symbol} 成交额失败: {e}")
            return 0
    
    def analyze_stock(self, symbol: str) -> Dict[str, Any]:
        """分析单个股票的竞价数据 - 二次详细分析"""
        try:
            # 获取竞价阶段完整数据
            auction_data = self._get_auction_data(symbol)
            if not auction_data:
                return {'significant': False, 'reason': '无竞价数据'}
            
            # 计算关键指标
            indicators = self._calculate_indicators(symbol, auction_data)
            
            # 综合判断
            return self._comprehensive_judgment(symbol, indicators)
            
        except Exception as e:
            logger.error(f"分析股票 {symbol} 失败: {e}")
            return {'significant': False, 'reason': f'分析失败: {str(e)}'}
    
    def _calculate_indicators(self, symbol: str, auction_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """计算关键指标"""
        if not auction_data:
            return {}
        
        # 基础指标
        first_tick = auction_data[0]
        last_tick = auction_data[-1]
        
        # 昨日收盘价
        previous_close = first_tick.get('close', 0)
        
        # 当前价格
        current_price = last_tick.get('last_price', 0)
        
        # 计算涨跌幅
        price_change_pct = 0
        if previous_close > 0:
            price_change_pct = (current_price - previous_close) / previous_close
        
        # 计算总成交额和成交量
        total_amount = sum(tick.get('amount', 0) for tick in auction_data)
        total_volume = sum(tick.get('volume', 0) for tick in auction_data)
        
        # 计算量比（相对于前5分钟平均）
        avg_volume_5min = self._get_avg_volume_5min(symbol)
        volume_ratio = total_volume / (avg_volume_5min + 1e-6) if avg_volume_5min > 0 else 1.0
        
        # 分析大单
        large_order_analysis = self._analyze_large_orders(auction_data)
        
        # 分析撤单
        withdrawal_analysis = self._analyze_withdrawals(auction_data)
        
        return {
            'symbol': symbol,
            'previous_close': previous_close,
            'current_price': current_price,
            'price_change_pct': price_change_pct,
            'total_amount': total_amount,
            'total_volume': total_volume,
            'volume_ratio': volume_ratio,
            'large_orders': large_order_analysis,
            'withdrawals': withdrawal_analysis
        }
    
    def _get_avg_volume_5min(self, symbol: str) -> float:
        """获取前5分钟平均成交量（用于计算量比）"""
        try:
            # 这里简化处理，使用固定值或从历史数据获取
            # 实际应用中应该从历史数据计算前5日同时段的平均成交量
            return 1000000  # 默认100万股
        except Exception as e:
            logger.error(f"获取平均成交量失败 {symbol}: {e}")
            return 1000000
    
    def _analyze_large_orders(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """分析大单行为"""
        large_orders = []
        total_large_order_value = 0
        
        for i in range(1, len(data)):
            current = data[i]
            prev = data[i-1]
            
            # 分析买单大单
            if (len(current['bid_volumes']) > 0 and len(prev['bid_volumes']) > 0 and
                current['bid_prices'][0] == prev['bid_prices'][0]):
                delta_bv1 = current['bid_volumes'][0] - prev['bid_volumes'][0]
                if delta_bv1 > 0:
                    order_value = delta_bv1 * current['bid_prices'][0] * 100
                    if order_value >= self.min_single_order:
                        large_orders.append({
                            'direction': 'buy',
                            'value': order_value,
                            'timestamp': current['timestamp']
                        })
                        total_large_order_value += order_value
            
            # 分析卖单大单
            if (len(current['ask_volumes']) > 0 and len(prev['ask_volumes']) > 0 and
                current['ask_prices'][0] == prev['ask_prices'][0]):
                delta_av1 = current['ask_volumes'][0] - prev['ask_volumes'][0]
                if delta_av1 > 0:
                    order_value = delta_av1 * current['ask_prices'][0] * 100
                    if order_value >= self.min_single_order:
                        large_orders.append({
                            'direction': 'sell',
                            'value': order_value,
                            'timestamp': current['timestamp']
                        })
                        total_large_order_value += order_value
        
        buy_orders = [o for o in large_orders if o['direction'] == 'buy']
        sell_orders = [o for o in large_orders if o['direction'] == 'sell']
        
        return {
            'large_orders': large_orders,
            'buy_orders': buy_orders,
            'sell_orders': sell_orders,
            'total_large_order_value': total_large_order_value,
            'buy_order_count': len(buy_orders),
            'sell_order_count': len(sell_orders)
        }
    
    def _analyze_withdrawals(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """分析撤单行为"""
        if len(data) < 2:
            return {'total_withdrawal': 0, 'withdrawal_rate': 0}
        
        total_withdrawal = 0
        total_early_amount = 0
        
        for i in range(1, len(data)):
            current = data[i]
            prev = data[i-1]
            
            # 只在试盘阶段分析撤单
            current_time = datetime.fromtimestamp(current['timestamp'] / 1000).time()
            if current_time >= dt_time(9, 20):
                continue
            
            # 分析卖单撤单
            if (len(current['ask_volumes']) > 0 and len(prev['ask_volumes']) > 0 and
                current['ask_prices'][0] == prev['ask_prices'][0]):
                delta_av1 = current['ask_volumes'][0] - prev['ask_volumes'][0]
                if delta_av1 < 0:
                    withdrawal_value = abs(delta_av1) * current['ask_prices'][0] * 100
                    total_withdrawal += withdrawal_value
            
            # 分析买单撤单
            if (len(current['bid_volumes']) > 0 and len(prev['bid_volumes']) > 0 and
                current['bid_prices'][0] == prev['bid_prices'][0]):
                delta_bv1 = current['bid_volumes'][0] - prev['bid_volumes'][0]
                if delta_bv1 < 0:
                    withdrawal_value = abs(delta_bv1) * current['bid_prices'][0] * 100
                    total_withdrawal += withdrawal_value
            
            # 累计试盘阶段成交额
            if current_time < dt_time(9, 20):
                total_early_amount += current.get('amount', 0)
        
        withdrawal_rate = total_withdrawal / (total_early_amount + 1e-6)
        
        return {
            'total_withdrawal': total_withdrawal,
            'withdrawal_rate': withdrawal_rate
        }
    
    def _comprehensive_judgment(self, symbol: str, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """综合判断异动真实性"""
        price_change_pct = indicators.get('price_change_pct', 0)
        volume_ratio = indicators.get('volume_ratio', 0)
        total_amount = indicators.get('total_amount', 0)
        withdrawal_rate = indicators.get('withdrawals', {}).get('withdrawal_rate', 0)
        buy_order_count = indicators.get('large_orders', {}).get('buy_order_count', 0)
        sell_order_count = indicators.get('large_orders', {}).get('sell_order_count', 0)
        
        # 判断条件
        conditions = []
        
        # 条件1: 价格变化显著
        if abs(price_change_pct) >= self.price_change_threshold:
            conditions.append(f"涨跌幅:{price_change_pct:.2%}")
        
        # 条件2: 量比显著
        if volume_ratio >= self.volume_ratio_threshold:
            conditions.append(f"量比:{volume_ratio:.2f}")
        
        # 条件3: 大单方向明确
        total_large_orders = buy_order_count + sell_order_count
        if total_large_orders >= 3:  # 至少3笔大单
            if buy_order_count > sell_order_count * 1.5:
                conditions.append(f"买单主导:{buy_order_count}买/{sell_order_count}卖")
            elif sell_order_count > buy_order_count * 1.5:
                conditions.append(f"卖单主导:{buy_order_count}买/{sell_order_count}卖")
            else:
                conditions.append(f"多空均衡:{buy_order_count}买/{sell_order_count}卖")
        
        # 条件4: 撤单率正常
        if withdrawal_rate <= self.max_withdrawal_rate:
            conditions.append(f"撤单率正常:{withdrawal_rate:.2%}")
        else:
            conditions.append(f"撤单率过高:{withdrawal_rate:.2%}")
        
        # 综合判断
        is_significant = (len(conditions) >= 2 and  # 至少满足2个条件
                         total_amount >= self.min_total_amount)
        
        if is_significant:
            direction = "看涨" if price_change_pct > 0 else "看跌"
            reason = f"{direction} - " + "，".join(conditions)
            
            return {
                'significant': True,
                'symbol': symbol,
                'direction': direction,
                'reason': reason,
                'price_change_pct': price_change_pct,
                'volume_ratio': volume_ratio,
                'total_amount': total_amount,
                'withdrawal_rate': withdrawal_rate,
                'buy_order_count': buy_order_count,
                'sell_order_count': sell_order_count,
                'analysis_time': int(time.time() * 1000)
            }
        else:
            return {
                'significant': False,
                'reason': f"条件不足: {', '.join(conditions)}",
                'symbol': symbol,
                'price_change_pct': price_change_pct,
                'volume_ratio': volume_ratio,
                'total_amount': total_amount
            }
    
    def _get_auction_data(self, symbol: str) -> List[Dict[str, Any]]:
        """获取竞价阶段数据"""
        try:
            today = datetime.now().strftime("%Y-%m-%d")
            query_sql = f"""
                SELECT tss, lp, o, h, l, lc, a, v, 
                       ap1, ap2, ap3, ap4, ap5,
                       bp1, bp2, bp3, bp4, bp5,
                       av1, av2, av3, av4, av5,
                       bv1, bv2, bv3, bv4, bv5
                FROM t_{self._sanitize_symbol(symbol)} 
                WHERE tss >= '{today} 09:15:00.000' AND tss <= '{today} 09:25:00.000'
                ORDER BY tss
            """
            
            result = self.tdengine_conn.query(query_sql)
            rows = result.fetch_all()
            
            data = []
            for row in rows:
                data.append({
                    'timestamp': int(row[0].timestamp() * 1000),
                    'last_price': row[1],
                    'open': row[2],
                    'high': row[3],
                    'low': row[4],
                    'close': row[5],  # 昨日收盘价
                    'amount': row[6],
                    'volume': row[7],
                    'ask_prices': [row[8], row[9], row[10], row[11], row[12]],
                    'bid_prices': [row[13], row[14], row[15], row[16], row[17]],
                    'ask_volumes': [row[18], row[19], row[20], row[21], row[22]],
                    'bid_volumes': [row[23], row[24], row[25], row[26], row[27]]
                })
            
            return data
            
        except Exception as e:
            logger.error(f"获取 {symbol} 竞价数据失败: {e}")
            return []
    
    def _sanitize_symbol(self, symbol):
        """清理符号名称"""
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', symbol)
        if sanitized and sanitized[0].isdigit():
            sanitized = 's_' + sanitized
        return sanitized
    
    def store_analysis_result(self, result: Dict[str, Any]):
        """存储分析结果"""
        try:
            if not result.get('significant', False):
                return
            
            # 创建分析结果表
            create_sql = """
                CREATE TABLE IF NOT EXISTS market_data.auction_analysis 
                (tss TIMESTAMP, symbol BINARY(20), direction BINARY(10), 
                 reason BINARY(200), price_change_pct FLOAT, volume_ratio FLOAT,
                 total_amount FLOAT, withdrawal_rate FLOAT, 
                 buy_order_count INT, sell_order_count INT)
                TAGS (market BINARY(10))
            """
            self.tdengine_conn.execute(create_sql)
            
            # 插入分析结果
            symbol = result['symbol']
            ts_str = datetime.fromtimestamp(result['analysis_time'] / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            
            insert_sql = f"""
                INSERT INTO t_auction_{self._sanitize_symbol(symbol)} 
                USING market_data.auction_analysis TAGS ('{symbol[:2]}') 
                VALUES ('{ts_str}', '{symbol}', '{result['direction']}', 
                '{result['reason']}', {result['price_change_pct']}, 
                {result['volume_ratio']}, {result['total_amount']}, 
                {result['withdrawal_rate']}, {result['buy_order_count']}, 
                {result['sell_order_count']})
            """
            self.tdengine_conn.execute(insert_sql)
            
            logger.info(f"存储分析结果: {symbol} - {result['reason']}")
            
        except Exception as e:
            logger.error(f"存储分析结果失败: {e}")
    
    def run_analysis_cycle(self):
        """运行一个分析周期"""
        try:
            # 获取异动股票
            volatile_stocks = self.get_volatile_stocks()
            if not volatile_stocks:
                return
            
            logger.info(f"发现 {len(volatile_stocks)} 支异动股票")
            
            # 预过滤
            filtered_symbols = self.pre_filter_stocks(volatile_stocks)
            logger.info(f"预过滤后剩余 {len(filtered_symbols)} 支股票")
            
            # 详细分析
            significant_count = 0
            for symbol in filtered_symbols:
                try:
                    result = self.analyze_stock(symbol)
                    if result.get('significant', False):
                        logger.info(f"重要异动: {symbol} - {result['reason']}")
                        self.store_analysis_result(result)
                        significant_count += 1
                    else:
                        logger.debug(f"非重要异动: {symbol} - {result.get('reason', '未知')}")
                except Exception as e:
                    logger.error(f"分析股票 {symbol} 失败: {e}")
                    continue
            
            if significant_count > 0:
                logger.info(f"本轮分析发现 {significant_count} 支重要异动股票")
                    
        except Exception as e:
            logger.error(f"分析周期执行失败: {e}")
    
    def close(self):
        """关闭连接"""
        if self.tdengine_conn:
            self.tdengine_conn.close()
        if self.redis_client:
            self.redis_client.close()
        logger.info("竞价分析引擎已关闭")

def main():
    """主函数"""
    config = {
        'redis_host': 'localhost',
        'redis_port': 6379,
        'redis_db': 0,
        'tdengine_host': 'localhost',
        'tdengine_port': 6030,
        'tdengine_user': 'root',
        'tdengine_password': 'taosdata',
        'volatile_pool_key': 'stock:volatile_pool',
        
        # 分析参数
        'min_total_amount': 500000,
        'min_single_order': 100000,
        'max_withdrawal_rate': 0.8,
        'volume_ratio_threshold': 3.0,
        'price_change_threshold': 0.03,
    }
    
    analysis_engine = EnhancedAuctionAnalysisEngine(config)
    
    try:
        logger.info("增强版竞价分析消费者启动成功")
        
        while True:
            # 检查是否为竞价时间段
            current_time = datetime.now().time()
            if dt_time(9, 15) <= current_time <= dt_time(9, 25):
                analysis_engine.run_analysis_cycle()
                # 竞价阶段每10秒分析一次
                time.sleep(10)
            else:
                # 非竞价阶段每分钟检查一次
                time.sleep(60)
                
    except KeyboardInterrupt:
        logger.info("接收到中断信号，程序退出")
    except Exception as e:
        logger.error(f"程序异常: {e}")
    finally:
        analysis_engine.close()

if __name__ == '__main__':
    main()