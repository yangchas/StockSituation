#!/usr/bin/env python3
import asyncio
import websockets
import json
import time
import os
from typing import Dict, Set, List
import logging
from aiohttp import web
from plate_updater import OptimizedPlateUpdater, PlateDataSimulator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class IntegratedWebService:
    def __init__(self):
        # 初始化板块更新器
        self.plate_updater = OptimizedPlateUpdater(
            'data/板块.csv', 
            'data/个股板块.csv', 
            'data/概念.csv'
        )
        
        # 初始化数据模拟器
        self.data_simulator = PlateDataSimulator(self.plate_updater, update_interval=2)
        
        # WebSocket连接管理
        self.plate_connections: Set = set()
        self.volatile_connections: Set = set()
        
        # 更新统计
        self.update_count = 0
    
    async def start_services(self):
        """启动所有服务"""
        # 启动数据模拟
        asyncio.create_task(self.data_simulator.start_simulation())
        
        # 启动板块数据广播
        asyncio.create_task(self.broadcast_plate_updates())
        
        logger.info("🚀 所有服务已启动")
    
    async def broadcast_plate_updates(self):
        """定期广播板块更新"""
        while True:
            try:
                if self.plate_connections:
                    # 获取最新数据
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
                
                await asyncio.sleep(1)  # 1秒广播一次
                
            except Exception as e:
                logger.error(f"❌ 广播板块更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_to_connections(self, message: Dict, connections: Set):
        """向连接集合广播消息"""
        if not connections:
            return
            
        disconnected = []
        for ws in connections:
            try:
                if hasattr(ws, 'send_str'):  # aiohttp WebSocket
                    await ws.send_str(json.dumps(message, ensure_ascii=False))
                else:  # websockets库
                    await ws.send(json.dumps(message, ensure_ascii=False))
            except:
                disconnected.append(ws)
        
        for ws in disconnected:
            connections.remove(ws)
    
    async def handle_plate_websocket(self, websocket):
        """处理板块数据WebSocket连接"""
        self.plate_connections.add(websocket)
        logger.info(f"🔗 板块客户端连接, 总数: {len(self.plate_connections)}")
        
        try:
            # 发送初始数据
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
            
            if hasattr(websocket, 'send_str'):
                await websocket.send_str(json.dumps(init_data, ensure_ascii=False))
            else:
                await websocket.send(json.dumps(init_data, ensure_ascii=False))
            
            # 处理客户端消息
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.handle_plate_message(data, websocket)
                except json.JSONDecodeError as e:
                    logger.error(f"❌ 解析消息失败: {e}")
                    
        except Exception as e:
            logger.error(f"❌ 板块WebSocket错误: {e}")
        finally:
            self.plate_connections.remove(websocket)
            logger.info(f"🔌 板块客户端断开, 总数: {len(self.plate_connections)}")
    
    async def handle_plate_message(self, data: Dict, websocket):
        """处理板块相关消息"""
        msg_type = data.get('type')
        
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
            sub_plates = self.plate_updater.get_sub_plates_metrics(main_plate_name)
            
            response = {
                'type': 'sub_plates',
                'main_plate': main_plate_name,
                'data': sub_plates,
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
        
        else:
            response = {
                'type': 'error',
                'message': f'未知消息类型: {msg_type}'
            }
        
        # 发送响应
        if hasattr(websocket, 'send_str'):
            await websocket.send_str(json.dumps(response, ensure_ascii=False))
        else:
            await websocket.send(json.dumps(response, ensure_ascii=False))

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
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    await service.handle_plate_websocket(ws)
    return ws

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
        'stock_count': len(service.plate_updater.stock_plate_relations),
        'plate_count': len(service.plate_updater.plates)
    })

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
    
    # 永久运行
    await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ 服务已停止")