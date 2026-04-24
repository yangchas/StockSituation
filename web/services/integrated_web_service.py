
import logging
import asyncio
import time
import json
import base64
import os
from typing import Set, Dict, List, Optional
from aiohttp import web

try:
    from web.plate_updater import OptimizedEnhancedPlateUpdater
    from web.trade_calendar import TradeCalendar
    from web.redis_storage import RedisStorageManager
    from web.services.tdengine_service import TDengineService
    from web.services.advanced_indicators import OptimizedAdvancedTechnicalIndicators
    from web.services.stock_kline_service import StockKLineService
    from web.services.f10_service import F10DataService
except ImportError:
    import sys
    import os
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
    from web.plate_updater import OptimizedEnhancedPlateUpdater
    from web.trade_calendar import TradeCalendar
    from web.redis_storage import RedisStorageManager
    from web.services.tdengine_service import TDengineService
    from web.services.advanced_indicators import OptimizedAdvancedTechnicalIndicators
    from web.services.stock_kline_service import StockKLineService
    from web.services.f10_service import F10DataService

logger = logging.getLogger(__name__)

class OptimizedIntegratedWebService:
    """优化版集成Web服务 - 单例模式"""
    _instance = None
    
    def __new__(cls, tdengine_service=None):
        if cls._instance is None:
            cls._instance = super(OptimizedIntegratedWebService, cls).__new__(cls)
            # 初始化Redis存储
            cls._instance.redis_storage = RedisStorageManager()
            
            # 使用优化后的板块更新器（直接集成高级指标）
            cls._instance.plate_updater = OptimizedEnhancedPlateUpdater(
                'data/板块.csv', 
                'data/个股板块.csv',
                cls._instance.redis_storage
            )
            
            # 使用优化后的高级指标服务
            # 使用外部传入的TDengineService实例或创建新实例
            cls._instance.tdengine = tdengine_service if tdengine_service else TDengineService()
            cls._instance.advanced_indicators = OptimizedAdvancedTechnicalIndicators(
                cls._instance.tdengine,  # 使用同一个TDengineService实例
                cls._instance.redis_storage
            )
            
            # WebSocket连接管理
            cls._instance.plate_connections: Set = set()
            cls._instance.plate_data_connections: Set = set()  # 新增：板块数据更新专用连接
            cls._instance.stock_connections: Dict[str, Set] = {}  # 个股订阅连接 {plate_id: set(connections)}
            cls._instance.aiohttp_subscriptions = {} # Excel页面订阅 {websocket: {'stocks': [], 'last_data': {}}}
            
            # 新增：K线服务 - 传递同一个TDengineService实例
            cls._instance.kline_service = StockKLineService(cls._instance.tdengine)
            # 新增：F10数据服务（支持环境变量覆盖路径）
            f10_csv_path = os.environ.get('F10_CSV_PATH', 'data/f10.csv')
            cls._instance.f10_service = F10DataService(f10_csv_path)

            # 交易日历（供 MarketEdgeEngine 等复用）
            cls._instance.calendar = TradeCalendar()
            
            # 更新统计
            cls._instance.update_count = 0
            cls._instance.cached_plate_metrics = []  # 缓存的板块指标
        return cls._instance
    
    def __init__(self, tdengine_service=None):
        # 单例模式下，__init__可能会被多次调用，所以什么都不做
        pass
    
    async def start_optimized_services(self):
        """启动优化后的服务"""
        # 启动板块数据更新
        asyncio.create_task(self.refresh_plate_data_optimized())
        
        # 启动板块数据广播
        asyncio.create_task(self.broadcast_plate_updates_optimized())
        
        # 启动板块数据更新专用广播
        asyncio.create_task(self.broadcast_plate_data_updates())
        
        # 启动个股数据广播
        asyncio.create_task(self.broadcast_stock_updates_optimized())

        logger.info("🚀 优化版服务已启动")
    
    async def refresh_plate_data_optimized(self):
        """优化版板块数据刷新"""
        while True:
            try:
                # 使用整合计算获取板块数据（包含高级指标）
                plate_metrics = self.plate_updater.get_all_plate_metrics_with_integrated_advanced()
                
                # 缓存到内存供快速访问
                self.cached_plate_metrics = plate_metrics
                
                # 每5秒刷新一次
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ 刷新板块数据失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_plate_updates_optimized(self):
        """优化版广播板块更新"""
        while True:
            try:
                if self.plate_connections:
                    # 使用缓存数据或实时获取
                    all_metrics = self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()
                    
                    # 筛选主板块
                    main_metrics = [m for m in all_metrics if m.get('type') == 'main']
                    
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
                    await self.broadcast_to_connections(update_msg, set(self.plate_connections))
                    
                    self.update_count += 1
                    
                    if self.update_count % 10 == 0:  # 每10次更新记录一次
                        logger.info(f"📤 广播板块更新 #{self.update_count}, 客户端: {len(self.plate_connections)}, 板块数: {len(all_metrics)}")
                
                await asyncio.sleep(3)  # 3秒广播一次
                
            except Exception as e:
                logger.error(f"❌ 广播板块更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_plate_data_updates(self):
        """广播板块数据更新（专门的WebSocket）"""
        while True:
            try:
                await asyncio.sleep(1)  # 1秒更新一次
                
                if self.plate_data_connections:
                    # 获取最新板块数据（包含高级指标）
                    all_plates = self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()
                    main_plates = [p for p in all_plates if p.get('type') == 'main']
                    
                    update_msg = {
                        'type': 'plate_data_update',
                        'timestamp': int(time.time() * 1000),
                        'data': {
                            'all_plates': all_plates,
                            'main_plates': main_plates
                        }
                    }
                    
                    # 广播给所有订阅的客户端
                    await self.broadcast_to_connections(update_msg, set(self.plate_data_connections))
                    
            except Exception as e:
                logger.error(f"❌ 广播板块数据更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_stock_updates_optimized(self):
        """优化版个股数据广播"""
        while True:
            try:
                all_indicators_dict = {}
                
                if self.stock_connections:
                    current_time = int(time.time() * 1000)
                    
                    # 获取所有活跃股票
                    active_stocks = self._get_active_stocks()
                    if active_stocks:
                        # 使用优化后的批量获取方法
                        indicators_dict = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(active_stocks)
                        all_indicators_dict = indicators_dict.copy()
                        
                        # 按板块分组广播
                        for plate_id, connections in self.stock_connections.items():
                            if connections and indicators_dict:
                                # 构建优化后的消息
                                update_msg = self._build_optimized_stock_update(plate_id, indicators_dict)
                                await self.broadcast_to_connections(update_msg, set(connections))
                    
                    # 每3秒记录一次日志
                    if int(time.time()) % 5 == 0:
                        active_subscriptions = sum(len(conns) for conns in self.stock_connections.values())
                        logger.info(f"📤 广播个股更新, 活跃订阅: {active_subscriptions}个连接")
                
                # 处理Excel页面的股票订阅
                if self.aiohttp_subscriptions:
                    # 获取所有订阅的股票ID
                    all_subscribed_stocks = []
                    for subscription in self.aiohttp_subscriptions.values():
                        all_subscribed_stocks.extend(subscription['stocks'])
                    
                    # 去重
                    all_subscribed_stocks = list(set(all_subscribed_stocks))
                    
                    if all_subscribed_stocks:
                        # 批量获取所有订阅股票的最新数据
                        subscribed_indicators = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(all_subscribed_stocks)
                        all_indicators_dict.update(subscribed_indicators)
                        
                        # 向订阅客户端推送更新
                        await self.broadcast_stock_updates_to_subscribers(subscribed_indicators)
                
                await asyncio.sleep(3)  # 3秒更新一次
                
            except Exception as e:
                logger.error(f"❌ 广播个股更新失败: {e}")
                await asyncio.sleep(5)
    
    async def broadcast_stock_updates_to_subscribers(self, updated_stocks):
        """广播股票更新到所有订阅了这些股票的客户端"""
        
        if not self.aiohttp_subscriptions or not updated_stocks:
            return
        
        # 按客户端分组需要推送的更新
        client_updates = {}
        
        for ws, subscription_info in self.aiohttp_subscriptions.items():
            subscribed_stocks = subscription_info['stocks']
            last_data = subscription_info['last_data']
            
            # 找出该客户端订阅的股票中有更新的部分
            client_update = {}
            for stock_id, new_data in updated_stocks.items():
                if stock_id in subscribed_stocks:
                    # 检查是否有实质性变化
                    old_data = last_data.get(stock_id, {})
                    
                    # 只推送有变化的数据
                    changed = False
                    for key in ['change_rate_1min', 'change_pct', 'amount_2min']:
                        # 确保数据是数字类型
                        new_value = float(new_data.get(key, 0))
                        old_value = float(old_data.get(key, 0))
                        if abs(new_value - old_value) > 0.01:
                            changed = True
                            break
                    
                    if changed:
                        client_update[stock_id] = new_data
            
            if client_update:
                client_updates[ws] = client_update
        
        # 推送更新给各个客户端
        for ws, update_data in client_updates.items():
            try:
                # 发送增量更新
                await ws.send_str(json.dumps({
                    'type': 'incremental_update',
                    'data': update_data,
                    'timestamp': int(time.time())
                }))
                
                # 更新客户端的最后数据记录
                if ws in self.aiohttp_subscriptions:
                    subscription_info = self.aiohttp_subscriptions[ws]
                    subscription_info['last_data'].update(update_data)
                    
            except Exception as e:
                logger.error(f"❌ 向Excel客户端推送更新出错: {e}")
                # 移除无效连接
                if ws in self.aiohttp_subscriptions:
                    del self.aiohttp_subscriptions[ws]
    
    def _get_active_stocks(self) -> List[str]:
        """获取活跃股票列表"""
        active_stocks = []
        for plate_id in self.stock_connections.keys():
            stocks = self.plate_updater.plate_to_stocks.get(plate_id, [])
            active_stocks.extend(stocks)
        
        # 去重
        return list(set(active_stocks))
    
    def _build_optimized_stock_update(self, plate_id: str, indicators_dict: Dict[str, Dict]) -> Dict:
        """构建优化后的个股更新消息"""
        # 获取该板块的股票
        stock_ids = self.plate_updater.plate_to_stocks.get(plate_id, [])
        
        stocks_data = []
        for stock_id in stock_ids:
            if stock_id in indicators_dict:
                indicators = indicators_dict[stock_id]
                
                # 获取股票基础信息
                stock_data = self.redis_storage.get_stock_data(stock_id) or {}
                
                # 构建完整的股票数据
                stock_info = {
                    'code': stock_id,
                    'name': stock_data.get('name', f"股票{stock_id}"),
                    'change_pct': indicators.get('change_pct', 0),
                    'price': indicators.get('price', 0),
                    'volume': indicators.get('volume', 0),
                    'market_cap': stock_data.get('market_cap', 0),
                    'large_net': indicators.get('large_net', 0),
                    'timestamp': indicators.get('timestamp', 0),
                    # 高级指标
                    'change_rate_1min': indicators.get('change_rate_1min', 0),
                    'amount_2min': indicators.get('amount_2min', 0)
                }
                stocks_data.append(stock_info)
        
        return {
            'type': 'stock_update',
            'plate_id': plate_id,
            'data': stocks_data,
            'timestamp': int(time.time() * 1000)
        }
    
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
            # 发送初始数据
            hierarchy, main_plates = self.plate_updater.get_plate_hierarchy()
            all_metrics = self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()
            main_metrics = [m for m in all_metrics if m.get('type') == 'main']
            
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
    
    async def handle_plate_data_websocket(self, request):
        """处理板块数据WebSocket连接（专门用于实时更新）"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        self.plate_data_connections.add(ws)
        logger.info(f"🔗 板块数据更新客户端连接, 总数: {len(self.plate_data_connections)}")
        
        try:
            # 发送初始数据
            init_data = {
                'type': 'plate_data_init',
                'timestamp': int(time.time() * 1000),
                'data': {
                    'all_plates': self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced(),
                    'main_plates': [p for p in (self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()) if p.get('type') == 'main']
                }
            }
            await ws.send_str(json.dumps(init_data, ensure_ascii=False))
            
            # 保持连接
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    if data.get('type') == 'ping':
                        await ws.send_str(json.dumps({'type': 'pong'}))
                elif msg.type == web.WSMsgType.ERROR:
                    break
                    
        except Exception as e:
            logger.error(f"❌ 板块数据WebSocket错误: {e}")
        finally:
            self.plate_data_connections.remove(ws)
            logger.info(f"🔌 板块数据更新客户端断开, 总数: {len(self.plate_data_connections)}")
        
        return ws
    
    async def handle_plate_message(self, data: Dict, websocket):
        """处理板块相关消息"""
        msg_type = data.get('type')
        logger.info(f"📨 收到消息类型: {msg_type}")
        
        if msg_type == 'get_sorted_plates':
            sort_by = data.get('sort_by', 'change_pct')
            plate_type = data.get('plate_type', 'all')  # all, main, sub
            
            if plate_type == 'main':
                plates_data = [p for p in (self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()) if p.get('type') == 'main']
            else:
                plates_data = self.cached_plate_metrics or self.plate_updater.get_all_plate_metrics_with_integrated_advanced()
            
            # 排序
            if sort_by == 'change_pct':
                sorted_plates = sorted(plates_data, key=lambda x: x['change_pct'], reverse=True)
            elif sort_by == 'total_volume':
                sorted_plates = sorted(plates_data, key=lambda x: x['total_volume'], reverse=True)
            elif sort_by == 'total_large_net':
                sorted_plates = sorted(plates_data, key=lambda x: x['total_large_net'], reverse=True)
            elif sort_by == 'rise_count':
                sorted_plates = sorted(plates_data, key=lambda x: x['rise_count'], reverse=True)
            elif sort_by == 'change_rate_1min':
                sorted_plates = sorted(plates_data, key=lambda x: x.get('change_rate_1min', 0), reverse=True)
            elif sort_by == 'total_amount_2min':
                sorted_plates = sorted(plates_data, key=lambda x: x.get('total_amount_2min', 0), reverse=True)
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
            
            # 使用优化后的批量获取高级指标
            stock_codes = [stock.get('code') for stock in stocks if stock.get('code')]
            if stock_codes:
                try:
                    # 使用优化后的批量获取方法
                    advanced_indicators_dict = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(stock_codes)
                    
                    # 将高级指标合并到个股数据中
                    for stock in stocks:
                        stock_code = stock.get('code')
                        if stock_code in advanced_indicators_dict:
                            indicators = advanced_indicators_dict[stock_code]
                            stock['advanced_indicators'] = {
                                'change_rate_1min': indicators.get('change_rate_1min', 0),
                                'amount_2min': indicators.get('amount_2min', 0),
                                'timestamp': int(time.time() * 1000)
                            }
                            logger.debug(f"✅ 已合并股票 {stock_code} 的高级指标")
                except Exception as e:
                    logger.error(f"❌ 批量获取高级指标失败: {e}")
            
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
        
        # 新增：获取全部个股数据（支持差异更新）
        elif msg_type == 'get_all_stocks':
            client_timestamp = data.get('last_update', 0)  # 客户端上次更新时间
            force_full = data.get('force_full', False)    # 是否强制全量更新
            
            logger.info(f"📊 请求全部个股数据, 客户端时间戳: {client_timestamp}, 强制全量: {force_full}")
            
            # 获取服务器端最新数据
            all_stocks_data = self.plate_updater.refresh_all_stocks_data()
            
            if force_full or client_timestamp == 0:
                # 全量更新模式
                response = {
                    'type': 'all_stocks',
                    'data': all_stocks_data,
                    'update_type': 'full',
                    'timestamp': int(time.time() * 1000),
                    'count': len(all_stocks_data)
                }
                logger.info(f"📤 全量推送全部个股数据: {len(all_stocks_data)} 只股票")
            else:
                # 差异更新模式 - 只返回有变化的股票
                changed_stocks = {}
                
                for stock_id, stock_data in all_stocks_data.items():
                    # 检查股票数据是否有变化（基于时间戳或关键字段变化）
                    if self._has_stock_changed(stock_id, stock_data, client_timestamp):
                        changed_stocks[stock_id] = stock_data
                
                if changed_stocks:
                    response = {
                        'type': 'all_stocks',
                        'data': changed_stocks,
                        'update_type': 'delta',
                        'timestamp': int(time.time() * 1000),
                        'count': len(changed_stocks),
                        'total_count': len(all_stocks_data)
                    }
                    logger.info(f"📤 差异推送个股数据: {len(changed_stocks)} 只有变化的股票")
                else:
                    # 没有变化，只返回时间戳确认
                    response = {
                        'type': 'all_stocks',
                        'update_type': 'no_change',
                        'timestamp': int(time.time() * 1000),
                        'message': '没有数据变化'
                    }
                    logger.info("📤 个股数据无变化，返回确认")
        
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
    
    def _has_stock_changed(self, stock_id: str, stock_data: Dict, client_timestamp: int) -> bool:
        """检查股票数据是否有变化（支持差异更新）"""
        try:
            # 检查时间戳变化
            stock_timestamp = stock_data.get('basic', {}).get('timestamp', 0)
            if stock_timestamp > client_timestamp:
                return True
            
            # 检查关键字段变化（涨跌幅、价格、成交量等）
            # 从Redis获取上次的数据进行比较
            cache_key = f"stock:last_sent:{stock_id}"
            last_sent_data = self.redis_storage.get_data(cache_key)
            
            if not last_sent_data:
                # 没有历史数据，视为有变化
                self.redis_storage.store_data(cache_key, stock_data, expire_seconds=300)  # 缓存5分钟
                return True
            
            # 比较关键字段
            key_fields = ['change_pct', 'price', 'volume', 'large_net']
            
            for field in key_fields:
                current_value = stock_data.get('basic', {}).get(field, 0)
                last_value = last_sent_data.get('basic', {}).get(field, 0)
                
                # 对于数值字段，检查是否有显著变化
                if isinstance(current_value, (int, float)) and isinstance(last_value, (int, float)):
                    if abs(current_value - last_value) > 0.001:  # 微小变化阈值
                        self.redis_storage.store_data(cache_key, stock_data, expire_seconds=300)
                        return True
            
            # 检查高级指标变化
            current_advanced = stock_data.get('advanced', {})
            last_advanced = last_sent_data.get('advanced', {})
            
            advanced_fields = ['change_rate_1min', 'amount_2min']
            for field in advanced_fields:
                current_val = current_advanced.get(field, 0)
                last_val = last_advanced.get(field, 0)
                
                if abs(current_val - last_val) > 0.01:  # 高级指标变化阈值
                    self.redis_storage.store_data(cache_key, stock_data, expire_seconds=300)
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ 检查股票变化失败 {stock_id}: {e}")
            return True  # 出错时保守地返回有变化
    
    async def handle_stock_kline_api(self, request):
        """处理个股K线数据API请求"""
        try:
            # 获取查询参数
            code = request.query.get('code', '')
            frequency = request.query.get('frequency', 'd')  # d, 5, 60, w
            start_date = request.query.get('start_date', '')
            end_date = request.query.get('end_date', '')
            
            if not code:
                return web.json_response({'error': '股票代码不能为空'}, status=400)
            
            # 验证频率参数
            valid_frequencies = ['1','5', '60', 'd', 'w']
            if frequency not in valid_frequencies:
                return web.json_response({'error': f'频率参数必须是: {valid_frequencies}'}, status=400)
            
            logger.info(f"📈 请求K线数据: {code}, 频率: {frequency}")
            
            # 获取K线数据
            kline_data = await asyncio.get_event_loop().run_in_executor(
                None, self.kline_service.fetch_kline_data, code, frequency, start_date, end_date
            )
            
            # 计算技术指标
            indicators = self.kline_service.calculate_technical_indicators(kline_data)
            
            response_data = {
                'code': code,
                'frequency': frequency,
                'data': kline_data,
                'indicators': indicators,
                'count': len(kline_data),
                'timestamp': int(time.time() * 1000)
            }
            
            return web.json_response(response_data)
            
        except Exception as e:
            logger.error(f"❌ K线API错误: {e}")
            return web.json_response({'error': str(e)}, status=500)
            
    async def handle_stock_subscription_websocket(self, request):
        """处理股票订阅WebSocket连接 - 支持订阅特定股票列表"""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        # 初始化订阅信息
        subscription_info = {
            'stocks': [],
            'last_data': {}
        }
        self.aiohttp_subscriptions[ws] = subscription_info
        
        logger.info(f"🔗 Excel页面客户端连接，当前连接数: {len(self.aiohttp_subscriptions)}")
        
        try:
            # 发送欢迎消息
            await ws.send_str(json.dumps({
                'type': 'system',
                'message': '连接成功，等待股票订阅列表...',
                'timestamp': int(time.time())
            }))
            
            # 处理客户端消息
            async for message in ws:
                if message.type == web.WSMsgType.TEXT:
                    try:
                        data = json.loads(message.data)
                        
                        if data.get('type') == 'subscribe':
                            logger.info(f"📨 收到Excel页面客户端消息: 处理订阅请求")
                            # 处理订阅请求
                            stock_list = data.get('stocks', [])
                            subscription_info['stocks'] = stock_list
                            
                            # 全量推送订阅股票的数据
                            if stock_list:
                                logger.info(f"📤 全量推送 {len(stock_list)} 只股票数据")
                                
                                try:
                                    # 获取所有订阅股票的高级指标
                                    if hasattr(self, 'advanced_indicators'):
                                        indicators_dict = self.advanced_indicators.batch_get_stocks_advanced_indicators_optimized(stock_list)
                                        
                                        logger.info(f"📊 获取到的股票指标数据: {len(indicators_dict)}")
                                        
                                        # 全量推送数据
                                        await ws.send_str(json.dumps({
                                            'type': 'full_update',
                                            'data': indicators_dict,
                                            'timestamp': int(time.time())
                                        }))
                                        
                                        # 更新最后数据记录
                                        subscription_info['last_data'] = indicators_dict
                                    else:
                                        logger.error("❌ 高级指标服务不可用")
                                        await ws.send_str(json.dumps({
                                            'type': 'error',
                                            'message': '高级指标服务不可用',
                                            'timestamp': int(time.time())
                                        }))
                                except Exception as e:
                                    logger.error(f"❌ 处理订阅请求失败: {e}")
                                    await ws.send_str(json.dumps({
                                        'type': 'error',
                                        'message': f'处理订阅请求失败: {str(e)}',
                                        'timestamp': int(time.time())
                                    }))
                            else:
                                await ws.send_str(json.dumps({
                                    'type': 'system',
                                    'message': '未提供股票列表，等待更新...',
                                    'timestamp': int(time.time())
                                }))
                        
                        elif data.get('type') == 'ping':
                            # 处理心跳请求
                            await ws.send_str(json.dumps({
                                'type': 'pong',
                                'timestamp': int(time.time())
                            }))
                        
                    except json.JSONDecodeError:
                        logger.error(f"❌ 解析客户端消息失败: {message.data}")
                        await ws.send_str(json.dumps({
                            'type': 'error',
                            'message': '消息格式错误',
                            'timestamp': int(time.time())
                        }))
                elif message.type == web.WSMsgType.CLOSE:
                    logger.info("🔌 客户端主动关闭连接")
                    break
                elif message.type == web.WSMsgType.ERROR:
                    logger.error(f"❌ WebSocket连接错误: {ws.exception()}")
                    break
        
        finally:
            # 清理连接
            if ws in self.aiohttp_subscriptions:
                del self.aiohttp_subscriptions[ws]
            logger.info(f"🔗 Excel页面客户端断开连接，当前连接数: {len(self.aiohttp_subscriptions)}")
        
        return ws
