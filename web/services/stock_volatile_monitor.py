
import aioredis
import asyncio
import json
import time
import logging
import base64
from datetime import datetime
from typing import Set, Dict, List, Optional

try:
    from web.redis_storage import RedisStorageManager
    from web.trade_calendar import TradeCalendar
except ImportError:
    import sys
    import os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
    from web.redis_storage import RedisStorageManager
    from web.trade_calendar import TradeCalendar

logger = logging.getLogger(__name__)

class StockVolatileMonitor:
    def __init__(self):
        self.redis = None
        self.redis_storage = RedisStorageManager()  # 复用现有Redis缓存工具，读取昨日涨停集合等
        self.connections: Set = set()  # 移除类型注解以兼容两种WebSocket类型
        self.volatile_pool_key = "stock:volatile_pool"  # 修正键名，从真正的异动池获取数据
        self.first_limit_key = "stock:first_limit_up"  # 严格首板票存储键名
        self.last_check_timestamp = 0
        self.monitoring_active = False
        self.calendar = TradeCalendar()  # 避免在监控循环中重复创建
        self.prev_day_cache = None
        self.yesterday_limit_set_cache = set()
        
    async def connect_redis(self):
        """连接Redis"""
        try:
            self.redis = await aioredis.from_url(
                "redis://localhost:6379/0",
                encoding='utf-8',
                decode_responses=True,
                max_connections=10
            )
            
            # 测试连接
            await self.redis.ping()
            logger.info("✅ Redis连接成功")
        except Exception as e:
            logger.error(f"❌ Redis连接失败: {e}")
            raise
    
    async def check_key_exists(self):
        """检查Redis键是否存在"""
        try:
            exists = await self.redis.exists(self.volatile_pool_key)
            if exists:
                key_type = await self.redis.type(self.volatile_pool_key)
                count = await self.redis.zcard(self.volatile_pool_key)
                # if count > 0:  # 只在有数据时记录
                    # logger.info(f"✅ 找到键: {self.volatile_pool_key}, 类型: {key_type}, 数据量: {count}")
                return True
            if not exists:
                logger.debug(f"⚠️ Redis键不存在: {self.volatile_pool_key}")
                return False
        except Exception as e:
            logger.error(f"❌ 检查Redis键失败: {e}")
            return False
    
    async def init_last_check_timestamp(self):
        """初始化最后检查时间戳"""
        try:
            exists = await self.redis.exists(self.volatile_pool_key)
            if not exists:
                logger.warning("Redis键不存在，使用当前时间")
                self.last_check_timestamp = int(time.time() * 1000)
                return
            
            # 获取有序集合中最后一条数据
            last_items = await self.redis.zrevrange(
                self.volatile_pool_key, 0, 0, withscores=True
            )
            
            if last_items:
                last_data_str, last_timestamp = last_items[0]
                self.last_check_timestamp = int(last_timestamp)
                logger.info(f"⏰ 初始化为最后一条消息的时间戳: {self.last_check_timestamp}")
                
                try:
                    if isinstance(last_data_str, bytes):
                        last_data_str = last_data_str.decode('utf-8', errors='ignore')
                    last_data = json.loads(last_data_str)
                    symbol = last_data.get('symbol', '未知')
                    reason = last_data.get('reason', '')
                    logger.info(f"📊 最后一条数据: {symbol} - {reason}")
                except Exception as e:
                    logger.warning(f"解析最后一条数据失败: {e}")
            else:
                self.last_check_timestamp = int(time.time() * 1000)
                logger.info(f"⏰ Redis键为空，使用当前时间戳: {self.last_check_timestamp}")
                
        except Exception as e:
            logger.error(f"❌ 初始化最后检查时间戳失败: {e}")
            self.last_check_timestamp = int(time.time() * 1000)
    
    async def monitor_volatile_stocks(self):
        """监控股票异动数据"""
        logger.info("🔍 开始监控股票异动数据...")
        
        check_count = 0
        consecutive_missing_count = 0
        
        while True:
            try:
                check_count += 1
                
                # 检查Redis键是否存在
                key_exists = await self.check_key_exists()
                
                if not key_exists:
                    consecutive_missing_count += 1
                    self.monitoring_active = False
                    
                    if consecutive_missing_count <= 3:
                        wait_time = 5
                    elif consecutive_missing_count <= 10:
                        wait_time = 10
                    else:
                        wait_time = 30
                    
                    if consecutive_missing_count % 5 == 0:
                        now_time = datetime.now().time()
                        if now_time < datetime.strptime("09:30", "%H:%M").time():
                            if consecutive_missing_count % 30 == 0:
                                logger.warning(f"⚠️ 盘前等待异动数据源就绪 (连续缺失: {consecutive_missing_count})")
                        else:
                            # 降低日志级别：在行情淡季或非交易时段，volatile_pool 不存在是正常现象
                            logger.debug(f"ℹ️ Redis键不存在，等待 {wait_time} 秒后重试 (连续缺失: {consecutive_missing_count})")
                    
                    # 通知客户端监控暂停
                    if self.connections and consecutive_missing_count == 1:
                        await self.broadcast_system_message("监控暂停：数据源暂时不可用，正在重连...")
                    
                    await asyncio.sleep(wait_time)
                    continue
                
                # 键存在时的处理逻辑
                if not self.monitoring_active:
                    logger.info("✅ Redis键已恢复，重新开始监控")
                    self.monitoring_active = True
                    consecutive_missing_count = 0
                    await self.init_last_check_timestamp()
                    
                    # 通知客户端监控恢复
                    if self.connections:
                        await self.broadcast_system_message("监控恢复：数据源已连接")
                
                # 正常监控逻辑
                if check_count % 120 == 0:
                    logger.info("🔄 监控运行中...")
                
                current_timestamp = int(time.time() * 1000)
                
                # 获取新数据
                new_data = await self.redis.zrangebyscore(
                    self.volatile_pool_key,
                    min=self.last_check_timestamp + 1,
                    max=current_timestamp,
                    withscores=True
                )
                
                if new_data:
                    logger.info(f"🎯 发现 {len(new_data)} 条新异动数据")
                    
                    for data_str, score in new_data:
                        try:
                            if isinstance(data_str, bytes):
                                data_str = data_str.decode('utf-8', errors='ignore')
                            
                            data = json.loads(data_str)
                            # print(data)
                            data['timestamp'] = int(score)
                            
                            # 🟢 FEED MARKET ENGINE: Sync volatile data to stock:quote:{symbol}
                            # This allows MarketEdgeEngine to see price/amount changes during replay
                            try:
                                s_symbol = str(data.get('symbol')).strip()
                                s_price = data.get('price', 0)
                                s_change = data.get('change', 0)
                                if isinstance(s_change, str):
                                    s_change = s_change.replace('%', '')
                                s_amount = data.get('amount', 0)
                                
                                # Use amount_5min as proxy for amount_2min if available, else amount
                                s_amount_2min = data.get('amount_5min', s_amount) 
                                
                                quote_update = {
                                    "symbol": s_symbol,
                                    "price": s_price,
                                    "change_pct": s_change,
                                    "amount": s_amount,
                                    "amount_2min": s_amount_2min,
                                    "timestamp": int(score)
                                }
                                
                                # Normalize Change Pct (Ratio -> Percentage)
                                try:
                                    val = float(s_change)
                                    if abs(val) < 0.5 and val != 0:
                                        s_change = val * 100.0
                                        quote_update["change_pct"] = s_change
                                        data['change'] = s_change
                                except:
                                    pass

                                await self.redis.hset(f"stock:quote:{s_symbol}", mapping=quote_update)
                                await self.redis.expire(f"stock:quote:{s_symbol}", 86400)
                            except Exception as e:
                                logger.error(f"Failed to sync quote for {data.get('symbol')}: {e}")

                            # Log with display name if available
                            alert_msg = self.format_volatile_alert(data)
                            display_name = alert_msg.get('name', '')
                            log_text = f"{alert_msg['symbol']}"
                            if display_name:
                                 log_text += f"({display_name})"
                            log_text += f" - {alert_msg['action_text']}"
                            
                            # logger.info(f"📢 广播: {log_text}")
                            
                            # Broadcast via WS
                            if self.connections:
                                disconnected = []
                                msg_json = json.dumps(alert_msg, ensure_ascii=False)
                                for ws in self.connections:
                                    try:
                                        if hasattr(ws, 'send_str'):
                                            await ws.send_str(msg_json)
                                        else:
                                            await ws.send(msg_json)
                                    except:
                                        disconnected.append(ws)
                                for ws in disconnected:
                                    self.connections.remove(ws)
                            
                            # 检查是否为涨停票，如果是则推送首板票
                            change = data.get('change', 0)
                            try:
                                if isinstance(change, str):
                                    # 去除百分号并转换为浮点数
                                    change = float(change.rstrip('%'))
                                elif not isinstance(change, (int, float)):
                                    change = 0
                            except ValueError:
                                change = 0
                            
                            # 根据股票前缀判断涨停阈值
                            symbol = data.get('symbol', '')
                            limit_up_threshold = 9.8 if symbol.startswith(('60', '00')) else 19.8
                            
                            # 如果是涨停票，按“严格首板”规则写入 Redis：涨停且不在昨日涨停集合
                            if change >= limit_up_threshold:
                                symbol = str(symbol).strip()

                                # 计算昨日交易日（复用初始化的交易日历，避免循环内重复创建）
                                today_str = datetime.now().strftime('%Y-%m-%d')
                                prev_day = self.calendar.get_previous_trade_day(today_str)

                                # 昨日涨停集合：优先读取 limit_up_{prev_day}（连板数据日更缓存），并fallback到综合视图缓存
                                # 这里做一层缓存，避免每条异动都去读Redis
                                if self.prev_day_cache != prev_day:
                                    self.prev_day_cache = prev_day
                                    self.yesterday_limit_set_cache = set()
                                    try:
                                        # 1) 主优先：limit_up_{prev_day}
                                        key_limit_up = f"limit_up_{prev_day}"
                                        prev_limit_up_result = self.redis_storage.get_data(key_limit_up)

                                        # 2) fallback：cache:comprehensive:prev_limit_up:{prev_day}
                                        if not prev_limit_up_result:
                                            key_prev_limit_up = f"cache:comprehensive:prev_limit_up:{prev_day}"
                                            prev_limit_up_result = self.redis_storage.get_data(key_prev_limit_up)

                                        if prev_limit_up_result:
                                            for item in prev_limit_up_result:
                                                if isinstance(item, dict):
                                                    code = str(item.get('code', '') or item.get('股票代码', '')).strip()
                                                    if code:
                                                        self.yesterday_limit_set_cache.add(code)
                                    except Exception:
                                        # 如果读取失败，保守为空集合
                                        self.yesterday_limit_set_cache = set()

                                yesterday_limit_set = self.yesterday_limit_set_cache

                                # 严格首板：不在昨日涨停集合
                                if symbol in yesterday_limit_set:
                                    continue

                                first_limit_data = data.copy()
                                first_limit_data['type'] = 'first_limit'
                                first_limit_data['change_pct'] = change  # 确保change_pct字段存在

                                # 广播首板警报
                                await self.broadcast_first_limit_alert(first_limit_data)

                                # 写入 Redis 的 stock:first_limit_up（严格首板池）
                                try:
                                    await self.redis.zadd(self.first_limit_key, {
                                        json.dumps(first_limit_data): int(time.time())
                                    })
                                    await self.redis.expire(self.first_limit_key, 24 * 60 * 60)
                                    logger.info(f"✅ 成功写入Redis首板数据: {first_limit_data.get('symbol')} ({change}%)")
                                except Exception as e:
                                    logger.error(f"❌ 写入Redis首板数据失败: {e}")
                            
                            # 更新最后检查时间戳
                            if int(score) > self.last_check_timestamp:
                                self.last_check_timestamp = int(score)
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ JSON解析错误: {e}, 数据: {data_str[:100]}...")
                        except Exception as e:
                            logger.error(f"❌ 处理数据错误: {e}")
                    
                    logger.info(f"⏰ 最后检查时间戳更新为: {self.last_check_timestamp}")
                    await asyncio.sleep(0.5)
                else:
                    count = await self.redis.zcard(self.volatile_pool_key)
                    if count > 0:
                        await asyncio.sleep(2)
                    else:
                        await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ 监控异动数据错误: {e}")
                self.monitoring_active = False
                await asyncio.sleep(5)
    
    async def broadcast_system_message(self, message: str):
        """广播系统消息到所有客户端"""
        system_message = {
            'type': 'system',
            'message': message,
            'timestamp': int(time.time() * 1000)
        }
        
        if self.connections:
            disconnected = []
            for ws in self.connections:
                try:
                    if hasattr(ws, 'send_str'):  # aiohttp WebSocket
                        await ws.send_str(json.dumps(system_message, ensure_ascii=False))
                    else:  # websockets库
                        await ws.send(json.dumps(system_message, ensure_ascii=False))
                except:
                    disconnected.append(ws)
            
            for ws in disconnected:
                self.connections.remove(ws)
    
    async def broadcast_volatile_alert(self, data: Dict):
        """广播异动警报到所有客户端"""
        alert_message = self.format_volatile_alert(data)
        
        logger.info(f"📢 广播: {alert_message['symbol']} - {alert_message['action_text']}")
        
        if self.connections:
            disconnected = []
            for ws in self.connections:
                try:
                    if hasattr(ws, 'send_str'):  # aiohttp WebSocket
                        await ws.send_str(json.dumps(alert_message, ensure_ascii=False))
                    else:  # websockets库
                        await ws.send(json.dumps(alert_message, ensure_ascii=False))
                except:
                    disconnected.append(ws)
            
            for ws in disconnected:
                self.connections.remove(ws)
    
    async def broadcast_first_limit_alert(self, data: Dict):
        """广播首板票警报到所有客户端"""
        alert_message = self.format_first_limit_alert(data)
        
        logger.info(f"📢 首板广播: {alert_message['code']} - {alert_message['name']}")
        
        # 构建符合前端期望的first_limit消息格式
        ws_message = {
            'type': 'first_limit',
            'payload': alert_message
        }
        
        # 同时也广播incremental_update消息，保持兼容性
        stock_id = alert_message['code']
        incremental_data = {
            stock_id: {
                'name': alert_message['name'],
                'price': alert_message['price'],
                'change_pct': alert_message['change_pct'],
                'amount': alert_message['trade_amount'],
                'change_rate_1min': alert_message.get('change_rate_1min', 0),
                'plate': alert_message['plate'],
                'is_first_limit': True  # 添加首板标识
            }
        }
        
        incremental_ws_message = {
            'type': 'incremental_update',
            'data': incremental_data,
            'timestamp': int(time.time())
        }
        
        if self.connections:
            disconnected = []
            for ws in self.connections:
                try:
                    # 发送first_limit消息（符合前端期望）
                    if hasattr(ws, 'send_str'):  # aiohttp WebSocket
                        await ws.send_str(json.dumps(ws_message, ensure_ascii=False))
                    else:  # websockets库
                        await ws.send(json.dumps(ws_message, ensure_ascii=False))
                    
                    # 发送incremental_update消息（保持兼容性）
                    if hasattr(ws, 'send_str'):  # aiohttp WebSocket
                        await ws.send_str(json.dumps(incremental_ws_message, ensure_ascii=False))
                    else:  # websockets库
                        await ws.send(json.dumps(incremental_ws_message, ensure_ascii=False))
                except:
                    disconnected.append(ws)
            
            for ws in disconnected:
                self.connections.remove(ws)
    
    def format_first_limit_alert(self, data: Dict) -> Dict:
        """格式化首板票警报消息"""
        code = data.get('symbol', '')  # 使用code字段，而不是symbol
        name = data.get('name', '')
        change = data.get('change', 0)
        price = data.get('price', 0)
        reason = data.get('reason', '')
        timestamp = data.get('timestamp', 0)
        trade_amount = data.get('amount', 0)
        plate = data.get('plate', '')
        
        if isinstance(reason, bytes):
            reason = reason.decode('utf-8', errors='ignore')
        
        # 转换时间戳
        try:
            dt = time.localtime(timestamp / 1000)
            display_time = time.strftime("%H:%M:%S", dt)
        except:
            display_time = time.strftime("%H:%M:%S")
        
        # 确保change是数字类型，并且转换为百分比形式
        if isinstance(change, str):
            # 如果是字符串，去掉百分号并转换为数字
            change_pct = float(change.replace('%', ''))
        else:
            # 如果已经是数字，转换为百分比（乘以100）
            change_pct = float(change) * 100 if isinstance(change, (int, float)) else 0
        
        return {
            'code': code,  # 前端使用code字段
            'name': name,
            'price': float(price),
            'change': str(change),
            'change_pct': change_pct,  # 前端需要的涨跌幅百分比
            'reason': reason,
            'action_text': "首板涨停",
            'alert_level': "high",
            'color_class': "limit_up",
            'timestamp': timestamp,
            'display_time': display_time,
            'trade_amount': float(trade_amount),  # 成交额
            'plate': plate,  # 所属板块
            'change_rate_1min': data.get('change_rate_1min', 0)  # 1分钟涨速
        }
    
    def format_volatile_alert(self, data: Dict) -> Dict:
        """格式化异动警报消息"""
        symbol = data.get('symbol', '')
        name_b64 = data.get('name_b64', '')
        if isinstance(name_b64, str):
            name_b64 = name_b64.encode('utf-8')
        name = base64.b64decode(name_b64).decode('utf-8', errors='ignore')
        change = data.get('change', '')
        amount = data.get('amount', '')
        reason = data.get('reason', '')
        strength = data.get('strength', 0)
        price = data.get('price', 0)
        change_5min = data.get('change_5min', 0)
        large_net_5min = data.get('large_net_5min', 0)
        amount_5min = data.get('amount_5min', 0)
        timestamp = data.get('timestamp', 0)
        
        if isinstance(reason, bytes):
            reason = reason.decode('utf-8', errors='ignore')
        
        # 根据强度确定警报级别
        if strength >= 8:
            alert_level = "high"
            level_text = "强烈异动"
        elif strength >= 5:
            alert_level = "medium" 
            level_text = "中度异动"
        else:
            alert_level = "low"
            level_text = "轻微异动"
        
        # 生成显示文本
        if "封单" in reason or "Top" in reason:
            action_text = "封单异动"
            color_class = "breakthrough"
        elif "Amount" in reason or "amount" in reason:
            action_text = "成交额异动"
            color_class = "large-order"
        else:
            action_text = reason
            color_class = "normal"
        
        # 转换时间戳
        try:
            dt = time.localtime(timestamp / 1000)
            display_time = time.strftime("%H:%M:%S", dt)
        except:
            display_time = time.strftime("%H:%M:%S")
        
        return {
            'type': 'volatile_alert',
            'symbol': symbol,
            'name' : name,
            'price': float(price),
            'amount':amount,
            'change': change,
            'reason': reason,
            'action_text': action_text,
            'level_text': level_text,
            'alert_level': alert_level,
            'color_class': color_class,
            'strength': int(strength),
            'change_5min': float(change_5min),
            'large_net_5min': float(large_net_5min),
            'amount_5min': float(amount_5min),
            'timestamp': timestamp,
            'display_time': display_time
        }
    
    async def get_recent_volatiles(self, count: int = 50):
        """获取最近的异动数据"""
        try:
            exists = await self.redis.exists(self.volatile_pool_key)
            if not exists:
                logger.warning("Redis键不存在，无法获取历史数据")
                return []
            
            recent_data = await self.redis.zrevrange(
                self.volatile_pool_key, 0, count - 1, withscores=False
            )
            
            volatiles = []
            for data_str in recent_data:
                try:
                    if isinstance(data_str, bytes):
                        data_str = data_str.decode('utf-8', errors='ignore')
                    data = json.loads(data_str)
                    formatted = self.format_volatile_alert(data)
                    volatiles.append(formatted)
                except Exception as e:
                    logger.error(f"解析历史数据错误: {e}")
                    continue
            
            logger.info(f"📚 加载 {len(volatiles)} 条历史异动数据")
            return volatiles
        except Exception as e:
            logger.error(f"❌ 获取历史异动数据错误: {e}")
            return []
    
    async def handler(self, websocket):
        """处理WebSocket连接 - 兼容aiohttp和websockets"""
        self.connections.add(websocket)
        logger.info(f"🔗 客户端连接，当前连接数: {len(self.connections)}")
        
        try:
            # 发送欢迎消息
            if hasattr(websocket, 'send_str'):  # aiohttp WebSocket
                await websocket.send_str(json.dumps({
                    'type': 'system',
                    'message': '连接成功，开始接收股票异动数据...',
                    'monitoring_active': self.monitoring_active
                }))
            else:  # websockets库
                await websocket.send(json.dumps({
                    'type': 'system',
                    'message': '连接成功，开始接收股票异动数据...',
                    'monitoring_active': self.monitoring_active
                }))
            
            # 发送历史数据
            recent_volatiles = await self.get_recent_volatiles(30)
            for volatile in recent_volatiles:
                if hasattr(websocket, 'send_str'):  # aiohttp WebSocket
                    await websocket.send_str(json.dumps(volatile, ensure_ascii=False))
                else:  # websockets库
                    await websocket.send(json.dumps(volatile, ensure_ascii=False))
            
            # 保持连接
            async for message in websocket:
                if hasattr(websocket, 'send_str'):  # aiohttp WebSocket
                    if message.type == web.WSMsgType.TEXT:
                        logger.info(f"📨 收到客户端消息: {message.data}")
                else:  # websockets库
                    logger.info(f"📨 收到客户端消息: {message}")
                    
        except Exception as e:
            logger.error(f"❌ 处理客户端消息错误: {e}")
        finally:
            self.connections.remove(websocket)
            logger.info(f"🔌 客户端断开，当前连接数: {len(self.connections)}")
