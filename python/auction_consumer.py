#!/usr/bin/env python3
import logging
import multiprocessing
import sys
import time
import os
import json
import struct
import zlib
import signal
import argparse
from multiprocessing import Process, Queue
from queue import Empty
from typing import List, Dict, Any
from datetime import datetime, time as dt_time
import pika
import taos
import redis

# 导入生成的 protobuf 模块
import schema_pb2

# 配置日志
logging.basicConfig(
    stream=sys.stdout, 
    level=logging.INFO, 
    format="%(asctime)s [%(name)s] - %(levelname)s - %(message)s"
)
logger = logging.getLogger("TDengineConsumer")

# 全局配置
CONFIG = {
    'rabbitmq_uri': 'amqp://admin:admin@localhost:5672/',
    'queue_name': 'stream2',
    'consumer_tag': 'tdengine-consumer',
    'batch_size': 500,
    'worker_count': 4,
    'buffer_size': 50000,
    'flush_timeout': 3.0,
    'verbose': True,
    'shutdown_timeout': 30,
    
    # Redis配置
    'redis_host': 'localhost',
    'redis_port': 6379,
    'redis_db': 0,
    
    # 异动池配置
    'volatile_pool_key': 'stock:volatile_pool',
    'volatile_expire': 300,  # 异动池数据过期时间(秒)
    
    # 异动检测阈值
    'price_change_threshold': 0.02,  # 价格变化阈值2%
    'volume_ratio_threshold': 3.0,   # 量比阈值
    'min_amount_threshold': 1000000, # 最小成交额阈值100万
}

# 完成消息标记
_DONE_MESSAGE = '__DONE__'

class MessageHeader:
    def __init__(self, proto_version, compression, batch_id, record_count, original_size, compressed_size, timestamp):
        self.proto_version = proto_version
        self.compression = compression
        self.batch_id = batch_id
        self.record_count = record_count
        self.original_size = original_size
        self.compressed_size = compressed_size
        self.timestamp = timestamp

    @classmethod
    def from_dict(cls, data):
        return cls(
            proto_version=data.get('proto_version', ''),
            compression=data.get('compression', ''),
            batch_id=data.get('batch_id', ''),
            record_count=data.get('record_count', 0),
            original_size=data.get('original_size', 0),
            compressed_size=data.get('compressed_size', 0),
            timestamp=data.get('timestamp', 0)
        )

class VolatilityDetector:
    """异动检测器"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
        self.volatile_pool_key = CONFIG['volatile_pool_key']
        self.price_threshold = CONFIG['price_change_threshold']
        self.volume_threshold = CONFIG['volume_ratio_threshold']
        self.amount_threshold = CONFIG['min_amount_threshold']
        
        # 缓存最近的价格和成交量
        self.price_cache = {}
        self.volume_cache = {}
        self.last_update_time = {}
    
    def is_auction_time(self, timestamp: int) -> bool:
        """检查是否为竞价时间"""
        dt = datetime.fromtimestamp(timestamp / 1000)
        current_time = dt.time()
        return True
        return dt_time(9, 15) <= current_time <= dt_time(9, 25)
    
    def detect_volatility(self, symbol: str, tick_data: Dict[str, Any]) -> bool:
        """检测股票异动 - 简化逻辑：只检测大单和涨跌幅"""
        timestamp = tick_data['timestamp']
        
        # 只在竞价时间段检测
        if not self.is_auction_time(timestamp):
            return False
        
        current_price = tick_data.get('last_price', 0)
        previous_close = tick_data.get('close', 0)  # lc是昨日收盘价
        
        # # 缓存昨日收盘价
        # if symbol not in self.previous_close_cache and previous_close > 0:
        #     self.previous_close_cache[symbol] = previous_close
        
        # 检测大单
        has_large_order = self._detect_large_orders(symbol, tick_data)
        
        # 检测涨跌幅（基于昨日收盘价）
        # has_price_change = self._detect_price_change(symbol, current_price)
        price_change = abs(current_price - previous_close) / previous_close
        has_price_change= price_change >= self.price_threshold
        # 只要满足大单或涨跌幅条件就加入异动池
        is_volatile = has_large_order or has_price_change
        
        if is_volatile:
            dt = datetime.fromtimestamp(timestamp / 1000)
            reason = "大单" if has_large_order else "涨跌幅"
            logger.info(f"{dt.strftime('%H:%M:%S')} 检测到异动: {symbol}, 原因: {reason}")
            self._add_to_volatile_pool(symbol, tick_data, reason)
        
        return is_volatile
    
    def _detect_large_orders(self, symbol: str, tick_data: Dict[str, Any]) -> bool:
        """检测大单"""
        try:
            # 检查买一档大单
            bid_price_1 = tick_data.get('bid_prices', [0])[0] if len(tick_data.get('bid_prices', [])) > 0 else 0
            bid_volume_1 = tick_data.get('bid_volumes', [0])[0] if len(tick_data.get('bid_volumes', [])) > 0 else 0
            
            # 检查卖一档大单
            ask_price_1 = tick_data.get('ask_prices', [0])[0] if len(tick_data.get('ask_prices', [])) > 0 else 0
            ask_volume_1 = tick_data.get('ask_volumes', [0])[0] if len(tick_data.get('ask_volumes', [])) > 0 else 0
            
            # 计算大单金额
            bid_order_value = bid_price_1 * bid_volume_1 * 100  # 转换为元
            ask_order_value = ask_price_1 * ask_volume_1 * 100  # 转换为元
            
            # 检测是否有大单
            has_large_bid = bid_order_value >= self.large_order_threshold
            has_large_ask = ask_order_value >= self.large_order_threshold
            
            return has_large_bid or has_large_ask
            
        except Exception as e:
            logger.error(f"检测大单失败 {symbol}: {e}")
            return False
    
    def _detect_price_change(self, symbol: str, current_price: float) -> bool:
        """检测涨跌幅"""
        try:
            if symbol not in self.previous_close_cache:
                return False
            
            previous_close = self.previous_close_cache[symbol]
            if previous_close <= 0:
                return False
            
            # 计算涨跌幅
            price_change = abs(current_price - previous_close) / previous_close
            
            return price_change >= self.price_threshold
            
        except Exception as e:
            logger.error(f"检测涨跌幅失败 {symbol}: {e}")
            return False
    
    def _add_to_volatile_pool(self, symbol: str, tick_data: Dict[str, Any], reason: str):
        """添加到异动池"""
        try:
            volatile_data = {
                'symbol': symbol,
                'timestamp': tick_data['timestamp'],
                'price': tick_data.get('last_price', 0),
                'volume': tick_data.get('volume', 0),
                'amount': tick_data.get('amount', 0),
                'reason': reason,
                'detect_time': int(time.time() * 1000),
                'is_trial_period': self.is_trial_period(tick_data['timestamp'])
            }
            
            # 使用Redis有序集合，按检测时间排序
            self.redis.zadd(
                self.volatile_pool_key,
                {json.dumps(volatile_data): volatile_data['detect_time']}
            )
            
            # 设置过期时间
            self.redis.expire(self.volatile_pool_key, CONFIG['volatile_expire'])
            
        except Exception as e:
            logger.error(f"添加到异动池失败 {symbol}: {e}")
    
    def cleanup_old_data(self):
        """清理过期数据"""
        try:
            # 移除过期数据
            cutoff_time = int(time.time() * 1000) - 3600 * 1000
            self.redis.zremrangebyscore(self.volatile_pool_key, 0, cutoff_time)
        except Exception as e:
            logger.error(f"清理异动池数据失败: {e}")

def get_tdengine_connection():
    """获取 TDengine 连接"""
    try:
        host = os.environ.get("TDENGINE_HOST", "localhost")
        port = int(os.environ.get("TDENGINE_PORT", "6030"))
        user = os.environ.get("TDENGINE_USER", "root")
        password = os.environ.get("TDENGINE_PASSWORD", "taosdata")
        database = os.environ.get("TDENGINE_DATABASE", "market_data")
        
        conn = taos.connect(host=host, port=port, user=user, password=password)
        conn.execute(f"USE {database}")
        
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to TDengine: {e}")
        raise


class TDengineWriterWithVolatility:
    """TDengine写入器 + 异动检测"""
    
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.conn = None
        self.redis_client = None
        self.volatility_detector = None
        self.log = logging.getLogger(f"TDengineWriter-{worker_id}")
        
        # 初始化Redis连接
        self._init_redis()
    
    def _init_redis(self):
        """初始化Redis连接"""
        try:
            self.redis_client = redis.Redis(
                host=CONFIG['redis_host'],
                port=CONFIG['redis_port'],
                db=CONFIG['redis_db'],
                decode_responses=True
            )
            self.volatility_detector = VolatilityDetector(self.redis_client)
            self.log.info("Redis连接和异动检测器初始化成功")
        except Exception as e:
            self.log.error(f"Redis初始化失败: {e}")
    
    def connect(self):
        """连接 TDengine"""
        if self.conn is None:
            self.conn = get_tdengine_connection()
            self.log.info("Connected to TDengine")
    
    def close(self):
        """关闭连接"""
        if self.conn:
            self.conn.close()
            self.conn = None
        if self.redis_client:
            self.redis_client.close()
        self.log.info("连接已关闭")
    
    def create_table_if_not_exists(self, symbol, exchange, market):
        """创建子表（如果不存在）"""
        sanitized_symbol = self.sanitize_symbol(symbol)
        table_name = f"t_{sanitized_symbol}"
        
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} 
            USING market_data.stock_data 
            TAGS ('{symbol}', '{exchange}', '{market}')
        """
        
        try:
            self.conn.execute(create_sql)
            return table_name
        except Exception as e:
            self.log.error(f"Failed to create table for symbol {symbol}: {e}")
            raise
    
    def sanitize_symbol(self, symbol):
        """清理符号名称"""
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', symbol)
        if sanitized and sanitized[0].isdigit():
            sanitized = 's_' + sanitized
        return sanitized
    
    def format_timestamp(self, timestamp):
        """格式化时间戳为 TDengine 接受的格式"""
        dt = datetime.fromtimestamp(timestamp / 1000)
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    def protobuf_to_dict(self, record):
        """将protobuf记录转换为字典"""
        return {
            'symbol': record.symbol,
            'timestamp': record.tss,
            'last_price': record.lp or 0,
            'open': record.o or 0,
            'high': record.h or 0,
            'low': record.l or 0,
            'close': record.lc or 0,
            'volume': record.v or 0,
            'amount': record.a or 0,
            'ask_prices': [record.ap1, record.ap2, record.ap3, record.ap4, record.ap5],
            'bid_prices': [record.bp1, record.bp2, record.bp3, record.bp4, record.bp5],
            'ask_volumes': [record.av1, record.av2, record.av3, record.av4, record.av5],
            'bid_volumes': [record.bv1, record.bv2, record.bv3, record.bv4, record.bv5]
        }
    
    def insert_records(self, records):
        """插入记录到 TDengine 并进行异动检测"""
        if not records:
            return 0
        
        start_time = time.time()
        
        # 按符号分组
        records_by_symbol = {}
        for record in records:
            symbol = record.symbol
            if symbol not in records_by_symbol:
                records_by_symbol[symbol] = []
            records_by_symbol[symbol].append(record)
        
        total_inserted = 0
        
        for symbol, symbol_records in records_by_symbol.items():
            if not symbol_records:
                continue
                
            try:
                first_record = symbol_records[0]
                exchange = first_record.exchange or ""
                market = first_record.market or ""
                
                # 创建表
                table_name = self.create_table_if_not_exists(symbol, exchange, market)
                
                # 构建插入 SQL
                values = []
                
                for record in symbol_records:
                    ts_str = self.format_timestamp(record.tss)
                    
                    value_str = (
                        f"('{ts_str}', {record.lp or 0}, {record.o or 0}, {record.h or 0}, {record.l or 0}, "
                        f"{record.lc or 0}, {record.a or 0}, {record.v or 0}, {record.p or 0}, "
                        f"{record.ap1 or 0}, {record.ap2 or 0}, {record.ap3 or 0}, {record.ap4 or 0}, {record.ap5 or 0}, "
                        f"{record.bp1 or 0}, {record.bp2 or 0}, {record.bp3 or 0}, {record.bp4 or 0}, {record.bp5 or 0}, "
                        f"{record.av1 or 0}, {record.av2 or 0}, {record.av3 or 0}, {record.av4 or 0}, {record.av5 or 0}, "
                        f"{record.bv1 or 0}, {record.bv2 or 0}, {record.bv3 or 0}, {record.bv4 or 0}, {record.bv5 or 0})"
                    )
                    values.append(value_str)
                    
                    # 异动检测 - 使用简化版检测器
                    if self.volatility_detector:
                        tick_data = self.protobuf_to_dict(record)
                        self.volatility_detector.detect_volatility(symbol, tick_data)
                
                # 执行插入
                insert_sql = f"INSERT INTO {table_name} VALUES {' '.join(values)}"
                self.conn.execute(insert_sql)
                total_inserted += len(symbol_records)
                
                if CONFIG['verbose']:
                    self.log.debug(f"Inserted {len(symbol_records)} records for symbol {symbol}")
                    
            except Exception as e:
                self.log.error(f"Failed to insert records for symbol {symbol}: {e}")
        
        if total_inserted > 1 and CONFIG['verbose']:
            duration = time.time() - start_time
            self.log.info(f"Worker {self.worker_id}: Inserted {total_inserted} records in {duration:.3f}s")
        
        return total_inserted


def parse_message_format(body):
    """解析消息格式"""
    if len(body) < 4:
        raise ValueError(f"Message body too short: {len(body)} bytes")
    
    header_len = struct.unpack('>I', body[:4])[0]
    
    if len(body) < 4 + header_len:
        raise ValueError(f"Message body too short for header: have {len(body)}, need {4 + header_len}")
    
    header_json = body[4:4+header_len].decode('utf-8')
    header_data = json.loads(header_json)
    header = MessageHeader.from_dict(header_data)
    
    proto_data = body[4+header_len:]
    
    return header, proto_data

def decompress_zlib(data):
    """解压缩 zlib 数据"""
    return zlib.decompress(data)

def process_message(body):
    """处理单个消息"""
    try:
        header, proto_data = parse_message_format(body)
        
        data_request = schema_pb2.DataRequest()
        data_request.ParseFromString(proto_data)
        
        if header.compression in ['GZIP', 'ZLIB']:
            batch_bytes = decompress_zlib(data_request.compressed_data)
        elif header.compression in ['NONE', '']:
            batch_bytes = data_request.compressed_data
        else:
            raise ValueError(f"Unsupported compression: {header.compression}")
        
        data_batch = schema_pb2.DataBatch()
        data_batch.ParseFromString(batch_bytes)
        
        return data_batch.records, len(data_batch.records)
        
    except Exception as e:
        logger.error(f"Failed to process message: {e}")
        return [], 0

def run_rabbitmq_consumer(task_queues: List[Queue], shutdown_event):
    """运行 RabbitMQ 消费者任务"""
    log = logging.getLogger("RabbitMQConsumer")
    connection = None
    channel = None
    
    try:
        # 连接 RabbitMQ
        log.info(f"Connecting to RabbitMQ: {CONFIG['rabbitmq_uri']}")
        parameters = pika.URLParameters(CONFIG['rabbitmq_uri'])
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        
        # 声明队列
        log.info(f"Declaring queue: {CONFIG['queue_name']}")
        channel.queue_declare(
            queue=CONFIG['queue_name'],
            durable=True,
            arguments={'x-max-length': 100000}
        )
        
        # 设置 QoS
        channel.basic_qos(prefetch_count=10)
        
        # 开始消费
        unique_tag = f"{CONFIG['consumer_tag']}-{int(time.time())}"
        log.info(f"Starting consumer with tag: {unique_tag}")
        
        message_count = 0
        record_count = 0
        start_time = time.time()
        last_report = start_time
        
        def callback(ch, method, properties, body):
            nonlocal message_count, record_count, last_report
            
            # 检查是否应该关闭
            if shutdown_event.is_set():
                ch.stop()
                return
                
            message_count += 1
            
            try:
                records, records_processed = process_message(body)
                record_count += records_processed
                
                # 分发记录到工作队列
                for record in records:
                    # 根据符号哈希选择队列
                    queue_index = hash(record.symbol) % len(task_queues)
                    
                    # 如果队列已满，等待一段时间
                    while task_queues[queue_index].full() and not shutdown_event.is_set():
                        time.sleep(0.1)
                    
                    # 如果正在关闭，不再添加新消息
                    if shutdown_event.is_set():
                        log.info("Shutdown in progress, skipping new messages")
                        break
                        
                    task_queues[queue_index].put(record)
                
                # 定期报告
                now = time.time()
                if now - last_report > 10.0:  # 每10秒报告一次
                    elapsed = now - start_time
                    msg_rate = message_count / elapsed
                    record_rate = record_count / elapsed
                    log.info(
                        f"Processed {message_count} messages, {record_count} records "
                        f"({msg_rate:.2f} msg/sec, {record_rate:.2f} records/sec)"
                    )
                    last_report = now
                
                if CONFIG['verbose'] and message_count % 1000 == 0:
                    log.info(f"Received message {message_count}")
                    
            except Exception as e:
                log.error(f"Failed to process message {message_count}: {e}")
        
        # 开始消费
        channel.basic_consume(
            queue=CONFIG['queue_name'],
            on_message_callback=callback,
            auto_ack=True,
            consumer_tag=unique_tag
        )
        
        log.info("RabbitMQ consumer started successfully")
        
        # 使用非阻塞方式消费，以便检查关闭事件
        while not shutdown_event.is_set():
            connection.process_data_events(time_limit=1)  # 处理1秒的数据事件
            
        log.info("RabbitMQ consumer stopping due to shutdown event")
        
    except KeyboardInterrupt:
        log.info("Consumer interrupted by user")
    except Exception as e:
        log.error(f"RabbitMQ consumer error: {e}")
        raise
    finally:
        # 发送完成消息
        for i, queue in enumerate(task_queues):
            try:
                # 非阻塞方式发送完成消息
                if not queue.full():
                    queue.put_nowait(_DONE_MESSAGE)
                    log.info(f"Sent DONE message to queue {i}")
                else:
                    log.warning(f"Queue {i} is full, cannot send DONE message")
            except Exception as e:
                log.error(f"Failed to send DONE message to queue {i}: {e}")
            
        # 关闭连接
        if channel and channel.is_open:
            try:
                channel.close()
            except Exception as e:
                log.error(f"Error closing channel: {e}")
        if connection and connection.is_open:
            try:
                connection.close()
            except Exception as e:
                log.error(f"Error closing connection: {e}")
        log.info("RabbitMQ consumer stopped")

def run_write_task(task_id: int, queue: Queue, done_queue: Queue, shutdown_event):
    """运行写入任务 - 包含异动检测"""
    log = logging.getLogger(f"WriteTask-{task_id}")
    writer = TDengineWriterWithVolatility(task_id)
    
    # 在子进程中忽略 SIGINT 信号
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    try:
        writer.connect()
        
        batch = []
        last_flush = time.time()
        received_done = False
        force_flush = False
        
        while True:
            # 检查是否应该退出
            if shutdown_event.is_set() and received_done and len(batch) == 0:
                try:
                    record = queue.get_nowait()
                    if record == _DONE_MESSAGE:
                        continue
                    batch.append(record)
                    continue
                except Empty:
                    log.info("Queue is empty, exiting...")
                    break
            
            # 强制刷新条件
            current_time = time.time()
            should_flush = (batch and (
                len(batch) >= CONFIG['batch_size'] or 
                current_time - last_flush >= CONFIG['flush_timeout'] or
                force_flush or
                (shutdown_event.is_set() and len(batch) > 0)
            ))
            
            if should_flush:
                try:
                    writer.insert_records(batch)
                    batch = []
                    last_flush = current_time
                    force_flush = False
                    
                    # 定期清理异动池旧数据
                    if writer.volatility_detector and current_time % 300 < 1:  # 每5分钟清理一次
                        writer.volatility_detector.cleanup_old_data()
                        
                except Exception as e:
                    log.error(f"Failed to insert batch: {e}")
                    try:
                        writer.close()
                        writer.connect()
                    except Exception as conn_error:
                        log.error(f"Failed to reconnect: {conn_error}")
                        time.sleep(1)
            
            # 从队列获取记录
            try:
                timeout = 0.1
                if shutdown_event.is_set():
                    timeout = 1
                
                record = queue.get(timeout=timeout)
                if record == _DONE_MESSAGE:
                    log.info("Received done message, will process remaining messages...")
                    received_done = True
                    force_flush = True
                    continue
                batch.append(record)
                    
            except Empty:
                if shutdown_event.is_set() and received_done:
                    if len(batch) == 0:
                        log.info("No more messages, exiting...")
                        break
                    else:
                        force_flush = True
                continue
            except Exception as e:
                log.error(f"Error getting from queue: {e}")
                continue
        
        # 最终强制处理剩余记录
        final_batch_processed = 0
        if batch:
            log.info(f"Final flush: Processing {len(batch)} remaining records in batch")
            try:
                writer.insert_records(batch)
                final_batch_processed = len(batch)
                batch = []
            except Exception as e:
                log.error(f"Failed to insert final batch: {e}")
        
        # 最后检查队列中是否还有剩余消息
        remaining_messages = []
        try:
            while True:
                record = queue.get_nowait()
                if record != _DONE_MESSAGE:
                    remaining_messages.append(record)
        except Empty:
            pass
        
        if remaining_messages:
            log.info(f"Final queue drain: Processing {len(remaining_messages)} remaining records from queue")
            try:
                writer.insert_records(remaining_messages)
                final_batch_processed += len(remaining_messages)
            except Exception as e:
                log.error(f"Failed to insert remaining queue messages: {e}")
        
        if final_batch_processed > 0:
            log.info(f"Worker {task_id}: Successfully processed {final_batch_processed} final records during shutdown")
            
        done_queue.put(_DONE_MESSAGE)
        
    except Exception as e:
        log.error(f"Write task failed: {e}")
        try:
            done_queue.put(_DONE_MESSAGE)
        except:
            pass
        raise
    finally:
        writer.close()
        log.info(f"Write task {task_id} finished")

def run_monitor_process(done_queue: Queue, shutdown_event):
    """运行监控进程"""
    log = logging.getLogger("Monitor")
    
    # 在子进程中忽略 SIGINT 信号
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    
    try:
        conn = get_tdengine_connection()
        
        def get_record_count():
            """获取总记录数"""
            try:
                result = conn.query("SELECT COUNT(*) FROM market_data.stock_data")
                rows = result.fetch_all()
                return rows[0][0] if rows else 0
            except Exception as e:
                log.error(f"Failed to get record count: {e}")
                return 0
        
        last_count = get_record_count()
        last_time = time.time()
        done_count = 0
        
        while not (shutdown_event.is_set() and done_count >= CONFIG['worker_count']):
            # 检查是否所有写入任务都已完成
            try:
                while True:
                    done_queue.get_nowait()
                    done_count += 1
                    log.info(f"Write task completed ({done_count}/{CONFIG['worker_count']})")
                    if done_count >= CONFIG['worker_count']:
                        log.info("All write tasks completed")
                        break
            except Empty:
                pass
            
            # 每10秒报告一次，但在关闭时更频繁检查
            if shutdown_event.is_set():
                time.sleep(1)  # 关闭时每秒检查一次
            else:
                time.sleep(10)
            
            # 只有在有活动时才报告统计信息
            if not shutdown_event.is_set() or done_count < CONFIG['worker_count']:
                current_count = get_record_count()
                current_time = time.time()
                
                if current_time > last_time:
                    speed = (current_count - last_count) / (current_time - last_time)
                    log.info(f"Total records: {current_count}, Speed: {speed:.2f} records/sec")
                    
                    last_count = current_count
                    last_time = current_time
                
    except Exception as e:
        log.error(f"Monitor error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
        log.info("Monitor process finished")

def check_remaining_messages(task_queues):
    """检查并报告剩余未处理的消息"""
    log = logging.getLogger("ShutdownChecker")
    total_remaining = 0
    
    for i, queue in enumerate(task_queues):
        queue_size = queue.qsize()
        if queue_size > 0:
            log.warning(f"Queue {i} has {queue_size} remaining messages")
            total_remaining += queue_size
    
    if total_remaining > 0:
        log.warning(f"Total {total_remaining} messages remaining in queues")
    else:
        log.info("All queues are empty")
        
    return total_remaining

def force_process_remaining_messages(task_queues):
    """强制处理剩余的消息"""
    log = logging.getLogger("ForceProcessor")
    total_processed = 0
    
    try:
        writer = TDengineWriterWithVolatility(-1)  # 特殊ID用于强制处理
        writer.connect()
        
        for i, queue in enumerate(task_queues):
            messages = []
            # 清空队列
            try:
                while True:
                    record = queue.get_nowait()
                    if record != _DONE_MESSAGE:  # 跳过完成消息
                        messages.append(record)
            except Empty:
                pass
            
            if messages:
                log.info(f"Force processing {len(messages)} messages from queue {i}")
                try:
                    processed = writer.insert_records(messages)
                    total_processed += processed
                    log.info(f"Force processed {processed} messages from queue {i}")
                except Exception as e:
                    log.error(f"Failed to force process messages from queue {i}: {e}")
        
        writer.close()
        
    except Exception as e:
        log.error(f"Error in force processing: {e}")
    
    return total_processed

def initialize_database():
    """初始化数据库和表"""
    log = logging.getLogger("DatabaseInitializer")
    
    try:
        conn = get_tdengine_connection()
        
        # 创建数据库
        conn.execute("CREATE DATABASE IF NOT EXISTS market_data")
        conn.execute("USE market_data")
        
        # 创建超级表
        create_stable_sql = """
        CREATE STABLE IF NOT EXISTS stock_data (
            ts TIMESTAMP,
            lp FLOAT, o FLOAT, h FLOAT, l FLOAT, lc FLOAT, a FLOAT,
            v BIGINT, p BIGINT,
            ap1 FLOAT, ap2 FLOAT, ap3 FLOAT, ap4 FLOAT, ap5 FLOAT,
            bp1 FLOAT, bp2 FLOAT, bp3 FLOAT, bp4 FLOAT, bp5 FLOAT,
            av1 BIGINT, av2 BIGINT, av3 BIGINT, av4 BIGINT, av5 BIGINT,
            bv1 BIGINT, bv2 BIGINT, bv3 BIGINT, bv4 BIGINT, bv5 BIGINT
        ) TAGS (symbol BINARY(20), exchange BINARY(10), market BINARY(10))
        """
        
        conn.execute(create_stable_sql)
        log.info("Database and tables initialized successfully")
        
    except Exception as e:
        log.error(f"Failed to initialize database: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

def signal_handler(signum, frame, shutdown_event):
    """信号处理函数"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()

def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='TDengine RabbitMQ Consumer with Volatility Detection')
    parser.add_argument('--uri', default=CONFIG['rabbitmq_uri'], help='RabbitMQ URI')
    parser.add_argument('--queue', default=CONFIG['queue_name'], help='RabbitMQ queue name')
    parser.add_argument('--consumer-tag', default=CONFIG['consumer_tag'], help='Consumer tag')
    parser.add_argument('--batch-size', type=int, default=CONFIG['batch_size'], help='Batch size')
    parser.add_argument('--workers', type=int, default=CONFIG['worker_count'], help='Number of workers')
    parser.add_argument('--buffer-size', type=int, default=CONFIG['buffer_size'], help='Buffer size')
    parser.add_argument('--shutdown-timeout', type=int, default=CONFIG['shutdown_timeout'], help='Shutdown timeout in seconds')
    parser.add_argument('--verbose', action='store_true', default=CONFIG['verbose'], help='Verbose output')
    parser.add_argument('--redis-host', default=CONFIG['redis_host'], help='Redis host')
    parser.add_argument('--redis-port', type=int, default=CONFIG['redis_port'], help='Redis port')
    parser.add_argument('--redis-db', type=int, default=CONFIG['redis_db'], help='Redis database')
    
    args = parser.parse_args()
    
    # 更新配置
    CONFIG.update(vars(args))
    
    logger.info(
        f"Starting TDengine consumer with volatility detection: "
        f"queue={CONFIG['queue_name']}, "
        f"workers={CONFIG['worker_count']}, "
        f"redis={CONFIG['redis_host']}:{CONFIG['redis_port']}"
    )
    
    # 创建关闭事件
    shutdown_event = multiprocessing.Event()
    
    # 设置信号处理
    def handler(signum, frame):
        signal_handler(signum, frame, shutdown_event)
    
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    
    # 初始化数据库
    initialize_database()
    
    # 创建进程间通信队列
    task_queues = [Queue(maxsize=CONFIG['buffer_size']) for _ in range(CONFIG['worker_count'])]
    done_queue = Queue()
    
    # 创建监控进程
    monitor_process = Process(target=run_monitor_process, args=(done_queue, shutdown_event))
    monitor_process.start()
    logger.info(f"Monitor process started with PID {monitor_process.pid}")
    
    # 创建写入进程
    write_processes = []
    for i in range(CONFIG['worker_count']):
        p = Process(target=run_write_task, args=(i, task_queues[i], done_queue, shutdown_event))
        p.start()
        write_processes.append(p)
        logger.info(f"Write task {i} started with PID {p.pid}")
    
    # 运行 RabbitMQ 消费者（在主进程中）
    try:
        run_rabbitmq_consumer(task_queues, shutdown_event)
    except Exception as e:
        logger.error(f"RabbitMQ consumer failed: {e}")
        shutdown_event.set()
    finally:
        # 设置关闭事件，确保所有进程知道要关闭
        shutdown_event.set()
        
        # 等待所有进程完成，但有超时
        logger.info(f"Waiting for processes to finish (timeout: {CONFIG['shutdown_timeout']}s)...")
        
        # 首先检查剩余消息
        initial_remaining = check_remaining_messages(task_queues)
        
        # 等待写入进程完成
        start_wait_time = time.time()
        for i, p in enumerate(write_processes):
            timeout_remaining = max(0, CONFIG['shutdown_timeout'] - (time.time() - start_wait_time))
            if timeout_remaining > 0:
                p.join(timeout=timeout_remaining)
                if p.is_alive():
                    logger.warning(f"Write process {i} (PID {p.pid}) did not finish in time, terminating...")
                    p.terminate()
            else:
                logger.warning(f"No time remaining, terminating write process {i}")
                p.terminate()
        
        # 等待监控进程完成
        timeout_remaining = max(0, CONFIG['shutdown_timeout'] - (time.time() - start_wait_time))
        if timeout_remaining > 0:
            monitor_process.join(timeout=timeout_remaining)
            if monitor_process.is_alive():
                logger.warning("Monitor process did not finish in time, terminating...")
                monitor_process.terminate()
        else:
            monitor_process.terminate()
        
        # 最终检查剩余消息
        final_remaining = check_remaining_messages(task_queues)
        
        # 如果有剩余消息，强制处理
        if final_remaining > 0:
            logger.warning(f"Found {final_remaining} remaining messages, attempting force processing...")
            forced_processed = force_process_remaining_messages(task_queues)
            if forced_processed > 0:
                logger.info(f"Successfully force processed {forced_processed} messages")
            
            # 最后再次检查
            final_check = check_remaining_messages(task_queues)
            if final_check > 0:
                logger.error(f"WARNING: {final_check} messages were lost during shutdown!")
            else:
                logger.info("All messages successfully processed (including forced processing)")
        else:
            logger.info("All messages processed successfully")
        
        logger.info("All processes finished")

if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    main()