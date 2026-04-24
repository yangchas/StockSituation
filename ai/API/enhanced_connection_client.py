"""
增强连接客户端 - 针对垃圾服务器的多种连接优化策略
"""

import asyncio
import aiohttp
import time
from typing import List, Dict, Any, Optional, Union
import json
from dataclasses import dataclass
import logging
from enum import Enum

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ConnectionStrategy(Enum):
    """连接策略枚举"""
    STANDARD = "standard"  # 标准连接
    FAST_TIMEOUT = "fast_timeout"  # 快速超时重试
    PERSISTENT = "persistent"  # 持久连接
    AGGRESSIVE = "aggressive"  # 激进重试
    BACKUP = "backup"  # 备用服务器


@dataclass
class RequestConfig:
    """增强请求配置类"""
    url: str
    method: str = "POST"
    headers: Optional[Dict[str, str]] = None
    data: Optional[Dict[str, Any]] = None
    json_data: Optional[Dict[str, Any]] = None
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    connection_strategy: ConnectionStrategy = ConnectionStrategy.STANDARD
    backup_urls: Optional[List[str]] = None  # 备用URL列表
    

@dataclass
class ResponseResult:
    """响应结果类"""
    success: bool
    status_code: Optional[int] = None
    content: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    error: Optional[str] = None
    execution_time: float = 0.0
    retry_count: int = 0
    used_url: Optional[str] = None  # 实际使用的URL
    strategy: Optional[str] = None  # 使用的策略


class EnhancedConnectionClient:
    """增强连接客户端 - 针对垃圾服务器的优化"""
    
    def __init__(self, 
                 max_concurrent: int = 10, 
                 default_timeout: float = 30.0,
                 connection_pool_size: int = 100,
                 keepalive_timeout: int = 30):
        """
        初始化增强客户端
        
        Args:
            max_concurrent: 最大并发数
            default_timeout: 默认超时时间（秒）
            connection_pool_size: 连接池大小
            keepalive_timeout: 保持连接超时时间
        """
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        self.connection_pool_size = connection_pool_size
        self.keepalive_timeout = keepalive_timeout
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 连接统计
        self.connection_stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'timeout_requests': 0,
            'total_retries': 0,
            'strategy_usage': {}
        }
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        # 创建优化的连接器
        connector = aiohttp.TCPConnector(
            limit=self.connection_pool_size,
            limit_per_host=self.max_concurrent,
            keepalive_timeout=self.keepalive_timeout,
            enable_cleanup_closed=True,
            use_dns_cache=True,  # 启用DNS缓存
            ttl_dns_cache=300,   # DNS缓存TTL 5分钟
            force_close=False,   # 不强制关闭连接
        )
        
        # 创建超时配置
        timeout = aiohttp.ClientTimeout(
            total=self.default_timeout,
            connect=10.0,  # 连接超时10秒
            sock_connect=10.0,  # socket连接超时10秒
            sock_read=20.0  # socket读取超时20秒
        )
        
        # 创建会话
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive'
            },
            cookie_jar=aiohttp.CookieJar(unsafe=True)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        if self.session:
            await self.session.close()
        
        # 输出连接统计
        self._log_connection_stats()
    
    def _get_strategy_config(self, strategy: ConnectionStrategy) -> Dict[str, Any]:
        """获取策略配置"""
        configs = {
            ConnectionStrategy.STANDARD: {
                'timeout_factor': 1.0,
                'retry_delay_factor': 1.0,
                'max_retries_factor': 1.0,
                'description': '标准连接策略'
            },
            ConnectionStrategy.FAST_TIMEOUT: {
                'timeout_factor': 0.5,  # 更短的超时时间
                'retry_delay_factor': 0.3,  # 更短的重试延迟
                'max_retries_factor': 2.0,  # 更多的重试次数
                'description': '快速超时重试策略 - 适合响应慢的服务器'
            },
            ConnectionStrategy.PERSISTENT: {
                'timeout_factor': 2.0,  # 更长的超时时间
                'retry_delay_factor': 2.0,  # 更长的重试延迟
                'max_retries_factor': 1.5,  # 中等重试次数
                'description': '持久连接策略 - 适合不稳定但可恢复的服务器'
            },
            ConnectionStrategy.AGGRESSIVE: {
                'timeout_factor': 0.3,  # 非常短的超时时间
                'retry_delay_factor': 0.1,  # 非常短的重试延迟
                'max_retries_factor': 3.0,  # 大量重试次数
                'description': '激进重试策略 - 适合频繁超时的服务器'
            }
        }
        return configs.get(strategy, configs[ConnectionStrategy.STANDARD])
    
    async def make_request(self, config: RequestConfig) -> ResponseResult:
        """
        执行单个请求 - 支持多种连接策略
        
        Args:
            config: 增强请求配置
            
        Returns:
            响应结果
        """
        self.connection_stats['total_requests'] += 1
        
        # 记录策略使用情况
        strategy_name = config.connection_strategy.value
        if strategy_name not in self.connection_stats['strategy_usage']:
            self.connection_stats['strategy_usage'][strategy_name] = 0
        self.connection_stats['strategy_usage'][strategy_name] += 1
        
        # 获取策略配置
        strategy_config = self._get_strategy_config(config.connection_strategy)
        
        # 应用策略调整
        adjusted_timeout = config.timeout * strategy_config['timeout_factor']
        adjusted_retry_delay = config.retry_delay * strategy_config['retry_delay_factor']
        adjusted_max_retries = int(config.max_retries * strategy_config['max_retries_factor'])
        
        logger.info(f"🔧 使用策略: {strategy_config['description']}")
        logger.info(f"   调整后超时: {adjusted_timeout:.1f}s, 重试延迟: {adjusted_retry_delay:.1f}s, 最大重试: {adjusted_max_retries}")
        
        # 如果有备用URL，尝试备用策略
        if config.backup_urls and config.connection_strategy == ConnectionStrategy.BACKUP:
            return await self._try_backup_urls(config, adjusted_timeout, adjusted_max_retries, adjusted_retry_delay)
        
        # 标准请求流程
        return await self._execute_single_request(
            config.url, config, adjusted_timeout, adjusted_max_retries, adjusted_retry_delay
        )
    
    async def _execute_single_request(self, 
                                    url: str, 
                                    config: RequestConfig, 
                                    timeout: float, 
                                    max_retries: int, 
                                    retry_delay: float) -> ResponseResult:
        """执行单个URL的请求"""
        start_time = time.time()
        retry_count = 0
        
        while retry_count <= max_retries:
            try:
                # 准备请求参数
                request_kwargs = {
                    'method': config.method,
                    'url': url,
                    'headers': config.headers or {},
                    'timeout': aiohttp.ClientTimeout(total=timeout)
                }
                
                # 添加数据
                if config.data:
                    request_kwargs['data'] = config.data
                elif config.json_data:
                    request_kwargs['json'] = config.json_data
                
                # 执行请求
                async with self.session.request(**request_kwargs) as response:
                    content = await response.text()
                    
                    # 检查响应状态码
                    if 200 <= response.status < 400:
                        # 成功响应
                        result = ResponseResult(
                            success=True,
                            status_code=response.status,
                            content=content,
                            headers=dict(response.headers),
                            execution_time=time.time() - start_time,
                            retry_count=retry_count,
                            used_url=url,
                            strategy=config.connection_strategy.value
                        )
                        
                        self.connection_stats['successful_requests'] += 1
                        self._log_success_response(url, result)
                        return result
                    else:
                        # 服务器错误
                        error_msg = f"服务器错误: {response.status}"
                        
                        if 500 <= response.status < 600:
                            logger.warning(f"服务器错误: {url} - 状态码: {response.status} - 重试 {retry_count + 1}/{max_retries}")
                        else:
                            # 4xx错误，不重试
                            result = ResponseResult(
                                success=False,
                                status_code=response.status,
                                content=content,
                                headers=dict(response.headers),
                                error=error_msg,
                                execution_time=time.time() - start_time,
                                retry_count=retry_count,
                                used_url=url,
                                strategy=config.connection_strategy.value
                            )
                            self.connection_stats['failed_requests'] += 1
                            return result
                    
            except asyncio.TimeoutError:
                error_msg = f"请求超时 (超时时间: {timeout}s)"
                logger.warning(f"请求超时: {url} - 重试 {retry_count + 1}/{max_retries}")
                self.connection_stats['timeout_requests'] += 1
                
            except aiohttp.ClientError as e:
                error_msg = f"客户端错误: {str(e)}"
                logger.warning(f"客户端错误: {url} - {str(e)} - 重试 {retry_count + 1}/{max_retries}")
                
            except Exception as e:
                error_msg = f"未知错误: {str(e)}"
                logger.error(f"未知错误: {url} - {str(e)}")
                break
            
            # 重试逻辑
            retry_count += 1
            self.connection_stats['total_retries'] += 1
            
            if retry_count <= max_retries:
                delay = self._calculate_retry_delay(retry_delay, retry_count, error_msg)
                logger.info(f"等待 {delay:.1f}秒后重试...")
                await asyncio.sleep(delay)
            else:
                break
        
        # 所有重试都失败
        self.connection_stats['failed_requests'] += 1
        return ResponseResult(
            success=False,
            error=error_msg,
            execution_time=time.time() - start_time,
            retry_count=retry_count,
            used_url=url,
            strategy=config.connection_strategy.value
        )
    
    async def _try_backup_urls(self, 
                             config: RequestConfig, 
                             timeout: float, 
                             max_retries: int, 
                             retry_delay: float) -> ResponseResult:
        """尝试备用URL策略"""
        all_urls = [config.url] + (config.backup_urls or [])
        
        logger.info(f"🔄 备用URL策略: 尝试 {len(all_urls)} 个URL")
        
        for i, url in enumerate(all_urls):
            logger.info(f"   尝试URL {i+1}/{len(all_urls)}: {url}")
            
            result = await self._execute_single_request(url, config, timeout, max_retries, retry_delay)
            
            if result.success:
                logger.info(f"✅ 备用URL {i+1} 成功!")
                return result
            else:
                logger.warning(f"❌ 备用URL {i+1} 失败: {result.error}")
        
        # 所有备用URL都失败
        return ResponseResult(
            success=False,
            error=f"所有 {len(all_urls)} 个备用URL都失败",
            execution_time=0.0,
            retry_count=0
        )
    
    def _calculate_retry_delay(self, base_delay: float, retry_count: int, error_msg: str) -> float:
        """计算智能重试延迟时间"""
        delay = base_delay * (2 ** (retry_count - 1))
        
        if "超时" in error_msg:
            delay *= 1.5
        elif "连接" in error_msg.lower():
            delay *= 1.2
        
        max_delay = 60.0
        return min(delay, max_delay)
    
    def _log_success_response(self, url: str, result: ResponseResult):
        """记录成功响应的详细信息"""
        logger.info(f"✅ 请求成功: {url}")
        logger.info(f"   状态码: {result.status_code}")
        logger.info(f"   耗时: {result.execution_time:.2f}s")
        logger.info(f"   重试次数: {result.retry_count}")
        logger.info(f"   策略: {result.strategy}")
        logger.info(f"   内容长度: {len(result.content)} 字节")
        
        if result.content:
            preview_length = min(500, len(result.content))
            content_preview = result.content[:preview_length]
            if len(result.content) > preview_length:
                content_preview += "..."
            logger.info(f"   内容预览: {content_preview}")
    
    def _log_connection_stats(self):
        """输出连接统计信息"""
        logger.info("📊 连接统计信息:")
        logger.info(f"   总请求数: {self.connection_stats['total_requests']}")
        logger.info(f"   成功请求: {self.connection_stats['successful_requests']}")
        logger.info(f"   失败请求: {self.connection_stats['failed_requests']}")
        logger.info(f"   超时请求: {self.connection_stats['timeout_requests']}")
        logger.info(f"   总重试次数: {self.connection_stats['total_retries']}")
        
        if self.connection_stats['strategy_usage']:
            logger.info("   策略使用情况:")
            for strategy, count in self.connection_stats['strategy_usage'].items():
                logger.info(f"     {strategy}: {count} 次")


# 测试函数
async def test_enhanced_connection():
    """测试增强连接功能"""
    print("🚀 测试增强连接客户端")
    print("=" * 80)
    
    # 创建华夏银行API配置
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json;charset=utf-8',
        'Cookie': 'BIGipServerpool_mcm_web_4443_v6=!GO6PTwst5dRM6uFDZAWKcNsf5P88uREKUb5G+zYLOgDGSlT8CyoXBN+aZyrgagX++5Mymat4A7lEXFN265aokLZ8c74rQze39EylEaFJ; BIGipServerpool_mcm_web_4443=!Pq84hmEo1DJcK+VDZAWKcNsf5P88uYUOrx+hvvrc5PmqpV18HangNU0B/SslRF50+BpseEkiyUyWdzI=',
        'Host': 'mcm.hxb.com.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    json_data = {
        "head_jsessionid": "",
        "h5_channel_id": "",
        "head_area_code": "110000",
        "head_iv": "0000000000000000",
        "head_osnumber": "72c86cea-83dd-65f6-6826-77cf2ee19990",
        "system_id": "hxmark",
        "request_time": "4FD5A59F2A991063382DAF7F546731F9",
        "head_trans_code": "100104",
        "head_mac": "FAD0F4FE769D5F837334920A3F2EF399"
    }
    
    # 测试不同策略
    strategies = [
        (ConnectionStrategy.STANDARD, "标准策略"),
        (ConnectionStrategy.FAST_TIMEOUT, "快速超时策略"),
        (ConnectionStrategy.PERSISTENT, "持久连接策略"),
        (ConnectionStrategy.AGGRESSIVE, "激进重试策略")
    ]
    
    async with EnhancedConnectionClient(max_concurrent=3, default_timeout=15.0) as client:
        for strategy, description in strategies:
            print(f"\n🔧 测试策略: {description}")
            print("-" * 40)
            
            config = RequestConfig(
                url="https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json",
                method="POST",
                headers=headers,
                json_data=json_data,
                timeout=10.0,
                max_retries=2,
                connection_strategy=strategy
            )
            
            result = await client.make_request(config)
            
            print(f"📊 结果: {'成功' if result.success else '失败'}")
            print(f"   状态码: {result.status_code}")
            print(f"   耗时: {result.execution_time:.2f}s")
            print(f"   重试次数: {result.retry_count}")
            
            if not result.success:
                print(f"   错误: {result.error}")
    
    print("\n✅ 增强连接测试完成!")


async def main():
    """主函数"""
    await test_enhanced_connection()


if __name__ == "__main__":
    asyncio.run(main())