#!/usr/bin/env python3
import asyncio
import websockets
import redis.asyncio as redis
import json
import time
import os
from typing import Dict, Set
import logging
from aiohttp import web

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StockVolatileMonitor:
    def __init__(self):
        self.redis = None
        self.connections: Set = set()  # 移除类型注解以兼容两种WebSocket类型
        self.volatile_pool_key = "stock:volatile_pool"
        self.last_check_timestamp = 0
        self.monitoring_active = False
        
    async def connect_redis(self):
        """连接Redis"""
        self.redis = await redis.Redis(
            host='localhost', 
            port=6379, 
            db=0, 
            decode_responses=True,
            encoding='utf-8',
            max_connections=10
        )
        
        # 测试连接
        try:
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
                if count > 0:  # 只在有数据时记录
                    logger.info(f"✅ 找到键: {self.volatile_pool_key}, 类型: {key_type}, 数据量: {count}")
                return True
            else:
                logger.warning(f"⚠️ Redis键不存在: {self.volatile_pool_key}")
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
                        logger.warning(f"⚠️ Redis键不存在，等待 {wait_time} 秒后重试 (连续缺失: {consecutive_missing_count})")
                    
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
                            data['timestamp'] = int(score)
                            await self.broadcast_volatile_alert(data)
                            
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
    
    def format_volatile_alert(self, data: Dict) -> Dict:
        """格式化异动警报消息"""
        symbol = data.get('symbol', '')
        name = data.get('name', '')
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

# HTTP 路由处理
async def handle_index(request):
    """处理首页请求"""
    try:
        with open('html/index.html', 'r', encoding='utf-8') as f:
            content = f.read()
        return web.Response(text=content, content_type='text/html')
    except FileNotFoundError:
        return web.Response(text='index.html not found', status=404)

async def handle_yidong(request):
    """处理异动页面请求"""
    try:
        with open('html/yidong.html', 'r', encoding='utf-8') as f:
            content = f.read()
        return web.Response(text=content, content_type='text/html')
    except FileNotFoundError:
        return web.Response(text='yidong.html not found', status=404)

async def handle_static(request):
    """处理静态文件请求"""
    path = request.match_info.get('path', '')
    if '..' in path:
        return web.Response(text='Forbidden', status=403)
    
    file_path = os.path.join('html', path)
    if os.path.isfile(file_path):
        return web.FileResponse(file_path)
    else:
        return web.Response(text='File not found', status=404)

async def health_check(request):
    """健康检查接口"""
    return web.json_response({
        'status': 'healthy',
        'timestamp': time.time(),
        'monitoring_active': monitor.monitoring_active,
        'connections': len(monitor.connections)
    })

async def handle_websocket(request):
    """处理WebSocket连接 - aiohttp版本"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    # 调用原来的handler逻辑
    await monitor.handler(ws)
    
    return ws

async def main():
    global monitor
    monitor = StockVolatileMonitor()
    
    try:
        await monitor.connect_redis()
    except Exception as e:
        logger.error(f"❌ 启动失败: {e}")
        return
    
    # 创建HTTP应用
    app = web.Application()
    
    # 添加路由
    app.router.add_get('/', handle_index)
    app.router.add_get('/index', handle_index)
    app.router.add_get('/yidong', handle_yidong)
    app.router.add_get('/health', health_check)
    app.router.add_get('/static/{path:.*}', handle_static)
    app.router.add_get('/ws', handle_websocket)  # WebSocket路由
    
    # 启动服务器
    port = 8080
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    logger.info(f"🚀 服务器已启动在端口 {port}")
    logger.info(f"🌐 HTTP服务: http://localhost:{port}/")
    logger.info(f"📄 首页: http://localhost:{port}/")
    logger.info(f"📈 异动监控: http://localhost:{port}/yidong")
    logger.info(f"🔌 WebSocket: ws://localhost:{port}/ws")
    logger.info(f"❤️ 健康检查: http://localhost:{port}/health")
    logger.info("📈 短线精灵服务运行中...")
    
    # 启动监控任务
    asyncio.create_task(monitor.monitor_volatile_stocks())
    
    # 永久运行
    await asyncio.Future()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    asyncio.run(main())