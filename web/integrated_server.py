#!/usr/bin/env python3
import asyncio
import websockets
import json
import time
import os
from typing import Dict, Set, List
import logging
from aiohttp import web

# 修改导入路径
from plate_updater import LazyPlateUpdater, PlateDataSimulator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IntegratedWebService:
    def __init__(self):
        # 修改：使用LazyPlateUpdater替代原来的PlateUpdater
        self.plate_updater = LazyPlateUpdater(
            'data/板块.csv', 
            'data/个股板块.csv'
        )
        
        # 修改：使用新的PlateDataSimulator
        self.data_simulator = PlateDataSimulator(self.plate_updater, update_interval=10)
        
        # WebSocket连接管理
        self.plate_connections: Set = set()
        self.volatile_connections: Set = set()
        self.stock_connections: Dict[str, Set] = {}  # 新增：个股订阅连接 {plate_id: set(connections)}
        
        # 更新统计
        self.update_count = 0
    
    async def start_services(self):
        """启动所有服务"""
        # 启动数据模拟
        asyncio.create_task(self.data_simulator.start_simulation())
        
        # 启动板块数据广播
        asyncio.create_task(self.broadcast_plate_updates())
        
        # 新增：启动个股数据广播
        asyncio.create_task(self.broadcast_stock_updates())
        
        logger.info("🚀 所有服务已启动")
    
    async def broadcast_plate_updates(self):
        """定期广播板块更新"""
        while True:
            try:
                if self.plate_connections:
                    # 获取最新数据 - 现在从Redis获取
                    all_metrics = self.plate_updater.get_all_plate_metrics()
                    main_metrics = self.plate_updater.get_main_plates_metrics()
                    
                    update_msg = {
                        'type': 'plate_update',
                        'data': {
                            'all_plates': all_metrics,
                            'main_plates': main_metrics
                        },
                        'timestamp': int(time.time() * 1000),
                        'update_count': self.update_count
                    }
                    
                    # 广播给所有客户端
                    await self.broadcast_to_connections(update_msg, self.plate_connections)
                    
                    self.update_count += 1
                    
                    if self.update_count % 30 == 0:  # 每30次更新记录一次
                        logger.info(f"📤 广播板块更新 #{self.update_count}, 客户端: {len(self.plate_connections)}")
                
                await asyncio.sleep(self.data_simulator.update_interval)  # 1秒广播一次
                
            except Exception as e:
                logger.error(f"❌ 广播板块更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_stock_updates(self):
        """定期广播个股更新"""
        while True:
            try:
                if self.stock_connections:
                    current_time = int(time.time() * 1000)
                    
                    # 遍历所有被订阅的板块
                    for plate_id, connections in list(self.stock_connections.items()):
                        if not connections:
                            continue
                        
                        # 获取该板块的最新个股数据
                        stocks = self.plate_updater.get_plate_stocks(plate_id)
                        
                        # 构建更新消息
                        update_msg = {
                            'type': 'stock_update',
                            'plate_id': plate_id,
                            'data': stocks,
                            'timestamp': current_time
                        }
                        
                        # 广播给订阅该板块的所有客户端
                        await self.broadcast_to_connections(update_msg, connections)
                    
                    # 每5秒记录一次日志
                    if int(time.time()) % 5 == 0:
                        active_subscriptions = sum(len(conns) for conns in self.stock_connections.values())
                        logger.info(f"📤 广播个股更新, 活跃订阅: {active_subscriptions}个连接")
                
                await asyncio.sleep(3)  # 3秒更新一次
                
            except Exception as e:
                logger.error(f"❌ 广播个股更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_to_connections(self, message: Dict, connections: Set):
        """向连接集合广播消息"""
        if not connections:
            return
            
        disconnected = []
        for ws in connections:
            try:
                await ws.send_str(json.dumps(message, ensure_ascii=False))
            except:
                disconnected.append(ws)
        
        for ws in disconnected:
            connections.remove(ws)
    
    async def handle_plate_websocket(self, request):
        """处理板块数据WebSocket连接"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.plate_connections.add(ws)
        logger.info(f"🔗 板块客户端连接, 总数: {len(self.plate_connections)}")
        
        try:
            # 发送初始数据 - 现在从Redis获取
            hierarchy, main_plates = self.plate_updater.get_plate_hierarchy()
            all_metrics = self.plate_updater.get_all_plate_metrics()
            main_metrics = self.plate_updater.get_main_plates_metrics()
            
            init_data = {
                'type': 'plate_init',
                'data': {
                    'hierarchy': hierarchy,
                    'main_plates': main_plates,
                    'all_plates': all_metrics,
                    'main_plates_metrics': main_metrics
                },
                'timestamp': int(time.time() * 1000)
            }
            
            await ws.send_str(json.dumps(init_data, ensure_ascii=False))
            
            # 处理客户端消息
            async for msg in ws:
                try:
                    if msg.type == web.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        await self.handle_plate_message(data, ws)
                    elif msg.type == web.WSMsgType.ERROR:
                        logger.error(f"WebSocket错误: {ws.exception()}")
                except json.JSONDecodeError as e:
                    logger.error(f"❌ 解析消息失败: {e}")
                    
        except Exception as e:
            logger.error(f"❌ 板块WebSocket错误: {e}")
        finally:
            # 连接关闭时清理所有订阅
            self.plate_connections.remove(ws)
            for plate_id in list(self.stock_connections.keys()):
                if ws in self.stock_connections[plate_id]:
                    self.stock_connections[plate_id].remove(ws)
                    if not self.stock_connections[plate_id]:
                        del self.stock_connections[plate_id]
            
            logger.info(f"🔌 板块客户端断开, 总数: {len(self.plate_connections)}")
        
        return ws
    
    async def handle_plate_message(self, data: Dict, websocket):
        """处理板块相关消息"""
        msg_type = data.get('type')
        logger.info(f"📨 收到消息类型: {msg_type}")
        
        if msg_type == 'get_sorted_plates':
            sort_by = data.get('sort_by', 'change_pct')
            plate_type = data.get('plate_type', 'all')  # all, main, sub
            
            if plate_type == 'main':
                plates_data = self.plate_updater.get_main_plates_metrics()
            else:
                plates_data = self.plate_updater.get_all_plate_metrics()
            
            # 排序
            if sort_by == 'change_pct':
                sorted_plates = sorted(plates_data, key=lambda x: x['change_pct'], reverse=True)
            elif sort_by == 'total_volume':
                sorted_plates = sorted(plates_data, key=lambda x: x['total_volume'], reverse=True)
            elif sort_by == 'total_large_net':
                sorted_plates = sorted(plates_data, key=lambda x: x['total_large_net'], reverse=True)
            elif sort_by == 'rise_count':
                sorted_plates = sorted(plates_data, key=lambda x: x['rise_count'], reverse=True)
            else:
                sorted_plates = plates_data
            
            response = {
                'type': 'sorted_plates',
                'data': sorted_plates[:100],  # 限制数量
                'sort_by': sort_by,
                'plate_type': plate_type,
                'timestamp': int(time.time() * 1000)
            }
            
        elif msg_type == 'get_sub_plates':
            main_plate_name = data.get('main_plate')
            logger.info(f"🔍 获取子板块: {main_plate_name}")
            
            sub_plates = self.plate_updater.get_sub_plates_metrics(main_plate_name)
            logger.info(f"📋 找到子板块: {len(sub_plates)}个")
            
            response = {
                'type': 'sub_plates',
                'main_plate': main_plate_name,
                'data': sub_plates,
                'timestamp': int(time.time() * 1000)
            }
        
        elif msg_type == 'get_plate_stocks':
            plate_id = data.get('plate_id')
            logger.info(f"📊 获取板块个股: {plate_id}")
            
            stocks = self.plate_updater.get_plate_stocks(plate_id)
            logger.info(f"📈 找到个股: {len(stocks)}只")
            
            response = {
                'type': 'plate_stocks',
                'plate_id': plate_id,
                'data': stocks,
                'timestamp': int(time.time() * 1000)
            }
        
        elif msg_type == 'get_plate_detail':
            plate_id = data.get('plate_id')
            metrics = self.plate_updater.get_plate_metrics(plate_id)
            
            response = {
                'type': 'plate_detail',
                'data': metrics,
                'timestamp': int(time.time() * 1000)
            }
        
        # 新增：个股订阅消息处理
        elif msg_type == 'subscribe_stocks':
            plate_id = data.get('plate_id')
            action = data.get('action', 'subscribe')  # subscribe 或 unsubscribe
            
            if action == 'subscribe':
                # 订阅个股更新
                if plate_id not in self.stock_connections:
                    self.stock_connections[plate_id] = set()
                self.stock_connections[plate_id].add(websocket)
                logger.info(f"✅ 客户端订阅个股更新: {plate_id}, 当前订阅数: {len(self.stock_connections[plate_id])}")
                
                response = {
                    'type': 'subscribe_result',
                    'plate_id': plate_id,
                    'action': 'subscribed',
                    'message': f'已订阅 {plate_id} 的个股更新'
                }
            else:
                # 取消订阅
                if plate_id in self.stock_connections and websocket in self.stock_connections[plate_id]:
                    self.stock_connections[plate_id].remove(websocket)
                    logger.info(f"❌ 客户端取消订阅个股更新: {plate_id}, 剩余订阅数: {len(self.stock_connections[plate_id])}")
                    
                    # 如果该板块没有订阅者了，清理空集合
                    if not self.stock_connections[plate_id]:
                        del self.stock_connections[plate_id]
                
                response = {
                    'type': 'subscribe_result',
                    'plate_id': plate_id,
                    'action': 'unsubscribed',
                    'message': f'已取消订阅 {plate_id} 的个股更新'
                }
        
        else:
            response = {
                'type': 'error',
                'message': f'未知消息类型: {msg_type}'
            }
        
        # 发送响应
        await websocket.send_str(json.dumps(response, ensure_ascii=False))

# HTTP路由处理
async def handle_bankuai(request):
    """板块监控页面"""
    try:
        with open('html/bankuai.html', 'r', encoding='utf-8') as f:
            content = f.read()
        return web.Response(text=content, content_type='text/html')
    except FileNotFoundError:
        # 提供简单页面
        html = """
        <!DOCTYPE html>
        <html>
        <head><title>板块监控</title></head>
        <body>
            <h1>板块监控系统</h1>
            <p>请确保 bankuai.html 文件存在</p>
            <p>实时板块数据将通过WebSocket推送</p>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')

async def handle_plate_websocket(request):
    """板块WebSocket"""
    return await service.handle_plate_websocket(request)

async def plate_api(request):
    """板块数据API"""
    try:
        query_type = request.query.get('type', 'all_plates')
        
        if query_type == 'all_plates':
            data = service.plate_updater.get_all_plate_metrics()
        elif query_type == 'main_plates':
            data = service.plate_updater.get_main_plates_metrics()
        elif query_type == 'hierarchy':
            hierarchy, main_plates = service.plate_updater.get_plate_hierarchy()
            data = {'hierarchy': hierarchy, 'main_plates': main_plates}
        else:
            return web.json_response({'error': '未知查询类型'}, status=400)
        
        return web.json_response({
            'data': data,
            'timestamp': int(time.time() * 1000)
        })
        
    except Exception as e:
        logger.error(f"❌ 板块API错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def health_check(request):
    """健康检查"""
    return web.json_response({
        'status': 'healthy',
        'plate_connections': len(service.plate_connections),
        'update_count': service.update_count,
        'stock_count': len(service.plate_updater.stock_to_plates),
        'plate_count': len(service.plate_updater.all_plates)
    })

# Redis状态检查
async def redis_status(request):
    """Redis状态检查"""
    try:
        from redis_storage import RedisStorageManager
        storage = RedisStorageManager()
        memory_info = storage.get_memory_info()
        
        return web.json_response({
            'status': 'healthy',
            'redis_memory': memory_info
        })
    except Exception as e:
        return web.json_response({
            'status': 'error',
            'error': str(e)
        }, status=500)

# 调试接口 - 个股数据状态
async def debug_plate_stocks_api(request):
    """调试板块个股API"""
    try:
        plate_id = request.query.get('plate_id', '')
        
        if not plate_id:
            return web.json_response({'error': '请提供plate_id参数'}, status=400)
        
        # 调用调试方法
        service.plate_updater.debug_plate_stocks(plate_id)
        
        # 获取实际的个股数据
        stocks = service.plate_updater.get_plate_stocks(plate_id)
        
        return web.json_response({
            'plate_id': plate_id,
            'stock_count': len(stocks),
            'stocks_sample': stocks[:5]  # 返回前5只作为样本
        })
        
    except Exception as e:
        logger.error(f"❌ 调试接口错误: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def main():
    global service
    service = IntegratedWebService()
    
    # 启动后台服务
    await service.start_services()
    
    # 创建HTTP应用
    app = web.Application()
    
    # 添加路由
    app.router.add_get('/', handle_bankuai)
    app.router.add_get('/bankuai', handle_bankuai)
    app.router.add_get('/ws/plate', handle_plate_websocket)
    app.router.add_get('/api/plate', plate_api)
    app.router.add_get('/health', health_check)
    app.router.add_get('/redis-status', redis_status)
    app.router.add_get('/debug/plate-stocks', debug_plate_stocks_api)  # 新增调试接口
    
    # 启动服务器
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    
    logger.info("🚀 集成Web服务已启动")
    logger.info("🌐 http://localhost:8080/bankuai - 板块监控")
    logger.info("🔌 ws://localhost:8080/ws/plate - 板块WebSocket")
    logger.info("📊 http://localhost:8080/api/plate - 板块API")
    logger.info("❤️ http://localhost:8080/health - 健康检查")
    logger.info("💾 http://localhost:8080/redis-status - Redis状态")
    logger.info("🐛 http://localhost:8080/debug/plate-stocks?plate_id=801159 - 个股调试")
    
    # 永久运行
    await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ 服务已停止")