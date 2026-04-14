import aiohttp
import asyncio
import logging
import redis.asyncio as redis
from web.services.tdengine_service import TDengineService

logger = logging.getLogger("InfraProvider")

class InfraProvider:
    """MarketEdge V32.0 基础设施单例容器 - 确保全引擎共享单一连接池且 Event Loop 对齐"""
    _redis_instance = None
    _td_instance = None
    _session_instance = None
    _last_loop = None

    @classmethod
    async def get_redis(cls, host='127.0.0.1', db=0):
        current_loop = asyncio.get_running_loop()
        
        if cls._redis_instance is None or cls._last_loop != current_loop:
            if cls._redis_instance:
                logger.info("🔄 [Infra] 检测到 Event Loop 变更，正在重置 Redis 连接池...")
                try: await cls._redis_instance.connection_pool.disconnect()
                except: pass
            
            logger.info(f"📡 [Infra] 建立全局唯一 Redis 连接 (Loop-ID: {id(current_loop)})")
            cls._redis_instance = redis.from_url(
                f"redis://{host}", db=db, decode_responses=True,
                socket_keepalive=True, retry_on_timeout=True
            )
            cls._last_loop = current_loop
        return cls._redis_instance

    @classmethod
    async def get_session(cls):
        current_loop = asyncio.get_running_loop()
        if cls._session_instance is None or cls._session_instance.closed or cls._last_loop != current_loop:
            if cls._session_instance and not cls._session_instance.closed:
                logger.info("🔄 [Infra] 检测到 Event Loop 变更，物理关闭旧 Session...")
                await cls._session_instance.close()
            
            logger.info(f"📡 [Infra] 建立全局唯一 aiohttp Session (Loop-ID: {id(current_loop)})")
            # 🚀 [V32.0] 强制使用 TCPConnector 禁止重用连接导致的未关闭警告
            connector = aiohttp.TCPConnector(limit=100, force_close=True)
            cls._session_instance = aiohttp.ClientSession(connector=connector)
            cls._last_loop = current_loop
        return cls._session_instance

    @classmethod
    def get_tdengine(cls):
        if cls._td_instance is None:
            logger.info("📡 [Infra] 建立全局唯一 TDengine 服务实例")
            cls._td_instance = TDengineService()
        return cls._td_instance

# 全局便捷访问点
async def get_global_redis(): return await InfraProvider.get_redis()
async def get_global_session(): return await InfraProvider.get_session()
def get_global_tdengine(): return InfraProvider.get_tdengine()

if __name__ == "__main__":
    target = r"d:\work\Go\engine_v2\v2_infra_provider.py"
    # 自我覆盖逻辑 (用于外部调用)
    pass
