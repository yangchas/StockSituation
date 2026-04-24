"""
并行请求客户端 - 支持超时连接和并行请求
"""

import asyncio
import aiohttp
import time
from typing import List, Dict, Any, Optional
import json
from dataclasses import dataclass
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class RequestConfig:
    """请求配置类"""
    url: str
    method: str = "POST"
    headers: Optional[Dict[str, str]] = None
    data: Optional[Dict[str, Any]] = None
    json_data: Optional[Dict[str, Any]] = None
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0


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


class ParallelRequestClient:
    """并行请求客户端"""
    
    def __init__(self, max_concurrent: int = 10, default_timeout: float = 30.0):
        """
        初始化客户端
        
        Args:
            max_concurrent: 最大并发数
            default_timeout: 默认超时时间（秒）
        """
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        # 创建连接器，配置连接池和超时
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent,
            limit_per_host=self.max_concurrent,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        
        # 创建超时配置
        timeout = aiohttp.ClientTimeout(total=self.default_timeout)
        
        # 创建会话
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器退出"""
        if self.session:
            await self.session.close()
    
    async def make_request(self, config: RequestConfig) -> ResponseResult:
        """
        执行单个请求 - 成功时返回内容，失败时重新请求
        
        Args:
            config: 请求配置
            
        Returns:
            响应结果
        """
        start_time = time.time()
        retry_count = 0
        
        while retry_count <= config.max_retries:
            try:
                # 准备请求参数
                request_kwargs = {
                    'method': config.method,
                    'url': config.url,
                    'headers': config.headers or {},
                    'timeout': aiohttp.ClientTimeout(total=config.timeout)
                }
                
                # 添加数据
                if config.data:
                    request_kwargs['data'] = config.data
                elif config.json_data:
                    request_kwargs['json'] = config.json_data
                
                # 执行请求
                async with self.session.request(**request_kwargs) as response:
                    content = await response.text()
                    
                    # 检查响应状态码，决定是否需要重试
                    if 200 <= response.status < 400:
                        # 成功响应，立即返回内容并输出详细信息
                        result = ResponseResult(
                            success=True,
                            status_code=response.status,
                            content=content,
                            headers=dict(response.headers),
                            execution_time=time.time() - start_time,
                            retry_count=retry_count
                        )
                        
                        # 输出成功请求的详细信息
                        logger.info(f"✅ 请求成功: {config.url}")
                        logger.info(f"   状态码: {response.status}")
                        logger.info(f"   耗时: {result.execution_time:.2f}s")
                        logger.info(f"   重试次数: {retry_count}")
                        logger.info(f"   内容长度: {len(content)} 字节")
                        
                        # 输出内容预览（限制长度避免日志过长）
                        if content:
                            preview_length = min(500, len(content))
                            content_preview = content[:preview_length]
                            if len(content) > preview_length:
                                content_preview += "..."
                            
                            logger.info(f"   内容预览: {content_preview}")
                        
                        # 输出响应头信息
                        if response.headers:
                            logger.info(f"   响应头数量: {len(response.headers)}")
                            # 输出重要的响应头
                            important_headers = ['content-type', 'content-length', 'server', 'date']
                            for header in important_headers:
                                if header in response.headers:
                                    logger.info(f"     {header}: {response.headers[header]}")
                        
                        return result
                    else:
                        # 服务器错误，根据状态码决定是否重试
                        error_msg = f"服务器错误: {response.status}"
                        
                        # 5xx错误通常可以重试，4xx错误通常不需要重试
                        if 500 <= response.status < 600:
                            logger.warning(f"服务器错误: {config.url} - 状态码: {response.status} - 重试 {retry_count + 1}/{config.max_retries}")
                        else:
                            # 4xx错误，不重试直接返回
                            result = ResponseResult(
                                success=False,
                                status_code=response.status,
                                content=content,
                                headers=dict(response.headers),
                                error=error_msg,
                                execution_time=time.time() - start_time,
                                retry_count=retry_count
                            )
                            logger.warning(f"客户端错误: {config.url} - 状态码: {response.status}")
                            return result
                    
            except asyncio.TimeoutError:
                error_msg = f"请求超时 (超时时间: {config.timeout}s)"
                logger.warning(f"请求超时: {config.url} - 重试 {retry_count + 1}/{config.max_retries}")
                
            except aiohttp.ClientError as e:
                error_msg = f"客户端错误: {str(e)}"
                logger.warning(f"客户端错误: {config.url} - {str(e)} - 重试 {retry_count + 1}/{config.max_retries}")
                
            except Exception as e:
                error_msg = f"未知错误: {str(e)}"
                logger.error(f"未知错误: {config.url} - {str(e)}")
                break
            
            # 重试逻辑 - 只有在需要重试的情况下才执行
            retry_count += 1
            if retry_count <= config.max_retries:
                # 智能重试延迟 - 根据错误类型调整延迟时间
                delay = self._calculate_retry_delay(config.retry_delay, retry_count, error_msg)
                logger.info(f"等待 {delay:.1f}秒后重试...")
                await asyncio.sleep(delay)
            else:
                break
        
        # 所有重试都失败
        return ResponseResult(
            success=False,
            error=error_msg,
            execution_time=time.time() - start_time,
            retry_count=retry_count
        )
    
    def _calculate_retry_delay(self, base_delay: float, retry_count: int, error_msg: str) -> float:
        """
        计算智能重试延迟时间
        
        Args:
            base_delay: 基础延迟时间
            retry_count: 当前重试次数
            error_msg: 错误信息
            
        Returns:
            重试延迟时间
        """
        # 指数退避基础延迟
        delay = base_delay * (2 ** (retry_count - 1))
        
        # 根据错误类型调整延迟
        if "超时" in error_msg:
            # 超时错误，增加延迟时间
            delay *= 1.5
        elif "连接" in error_msg.lower():
            # 连接错误，中等延迟
            delay *= 1.2
        
        # 限制最大延迟时间
        max_delay = 60.0  # 最大60秒
        return min(delay, max_delay)
    
    async def make_parallel_requests(self, configs: List[RequestConfig]) -> List[ResponseResult]:
        """
        执行并行请求
        
        Args:
            configs: 请求配置列表
            
        Returns:
            响应结果列表
        """
        if not self.session:
            raise RuntimeError("客户端未初始化，请使用 async with 语句")
        
        # 使用信号量控制并发数
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def bounded_request(config: RequestConfig) -> ResponseResult:
            async with semaphore:
                return await self.make_request(config)
        
        # 执行所有请求
        tasks = [bounded_request(config) for config in configs]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 处理异常结果
        final_results = []
        for result in results:
            if isinstance(result, Exception):
                final_results.append(ResponseResult(
                    success=False,
                    error=f"任务执行异常: {str(result)}",
                    execution_time=0.0
                ))
            else:
                final_results.append(result)
        
        return final_results


def create_hxb_request_config() -> RequestConfig:
    """
    创建华夏银行API请求配置
    
    Returns:
        请求配置对象
    """
    headers = {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=utf-8',
        'Cookie': 'BIGipServerpool_mcm_web_4443_v6=!GO6PTwst5dRM6uFDZAWKcNsf5P88uREKUb5G+zYLOgDGSlT8CyoXBN+aZyrgagX++5Mymat4A7lEXFN265aokLZ8c74rQze39EylEaFJ; BIGipServerpool_mcm_web_4443=!Pq84hmEo1DJcK+VDZAWKcNsf5P88uYUOrx+hvvrc5PmqpV18HangNU0B/SslRF50+BpseEkiyUyWdzI=',
        'Host': 'mcm.hxb.com.cn',
        'Origin': 'https://mcm.hxb.com.cn',
        'Referer': 'https://mcm.hxb.com.cn/p/coin/orderAdd.html',
        'Sec-Ch-Ua': '"Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0'
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
    
    return RequestConfig(
        url="https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json",
        method="POST",
        headers=headers,
        json_data=json_data,
        timeout=10.0,  # 10秒超时
        max_retries=2,  # 最多重试2次
        retry_delay=2.0  # 重试延迟2秒
    )


async def test_single_request():
    """测试单个请求"""
    print("🚀 测试单个请求")
    print("=" * 80)
    
    config = create_hxb_request_config()
    
    async with ParallelRequestClient() as client:
        result = await client.make_request(config)
        
        print(f"📊 请求结果:")
        print(f"   成功: {result.success}")
        print(f"   状态码: {result.status_code}")
        print(f"   耗时: {result.execution_time:.2f}s")
        print(f"   重试次数: {result.retry_count}")
        
        if result.success:
            print(f"   响应内容长度: {len(result.content) if result.content else 0}")
            
            # 详细输出成功请求的内容
            if result.content:
                print(f"\n   📄 响应内容详情:")
                print(f"     完整内容: {result.content}")
                
                # 尝试解析JSON内容
                try:
                    import json
                    json_data = json.loads(result.content)
                    print(f"   🎯 JSON解析结果:")
                    print(f"     数据类型: {type(json_data)}")
                    
                    if isinstance(json_data, dict):
                        print(f"     字段数量: {len(json_data)}")
                        for key, value in json_data.items():
                            value_preview = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                            print(f"     {key}: {value_preview}")
                    elif isinstance(json_data, list):
                        print(f"     数组长度: {len(json_data)}")
                        for i, item in enumerate(json_data[:3]):  # 只显示前3个
                            item_preview = str(item)[:100] + "..." if len(str(item)) > 100 else str(item)
                            print(f"     [{i}]: {item_preview}")
                        if len(json_data) > 3:
                            print(f"     ... 还有 {len(json_data) - 3} 个元素")
                except json.JSONDecodeError:
                    print(f"   📝 文本内容 (非JSON): {result.content[:500]}")
                    if len(result.content) > 500:
                        print(f"      ... (内容过长，已截断)")
            
            # 输出响应头信息
            if result.headers:
                print(f"\n   📋 响应头信息:")
                print(f"     头信息数量: {len(result.headers)}")
                important_headers = ['content-type', 'content-length', 'server', 'date', 'cache-control']
                for header in important_headers:
                    if header in result.headers:
                        print(f"     {header}: {result.headers[header]}")
        else:
            print(f"   错误信息: {result.error}")
        
        print()


async def test_parallel_requests():
    """测试并行请求"""
    print("\n🚀 测试并行请求 (3个并发)")
    print("=" * 80)
    
    # 创建3个相同的请求配置
    configs = [create_hxb_request_config() for _ in range(3)]
    
    async with ParallelRequestClient(max_concurrent=3) as client:
        start_time = time.time()
        results = await client.make_parallel_requests(configs)
        total_time = time.time() - start_time
        
        print(f"📊 并行请求完成")
        print(f"   总耗时: {total_time:.2f}s")
        print(f"   请求数量: {len(results)}")
        
        success_count = sum(1 for r in results if r.success)
        failure_count = len(results) - success_count
        
        print(f"   成功: {success_count}")
        print(f"   失败: {failure_count}")
        
        for i, result in enumerate(results, 1):
            print(f"\n  请求 {i}:")
            print(f"     成功: {result.success}")
            print(f"     状态码: {result.status_code}")
            print(f"     耗时: {result.execution_time:.2f}s")
            print(f"     重试: {result.retry_count}")
            
            if not result.success:
                print(f"     错误: {result.error}")


async def benchmark_requests():
    """性能基准测试"""
    print("\n🚀 性能基准测试 (10个请求，最大并发5)")
    print("=" * 80)
    
    configs = [create_hxb_request_config() for _ in range(10)]
    
    async with ParallelRequestClient(max_concurrent=5, default_timeout=15.0) as client:
        start_time = time.time()
        results = await client.make_parallel_requests(configs)
        total_time = time.time() - start_time
        
        # 统计信息
        success_count = sum(1 for r in results if r.success)
        total_retries = sum(r.retry_count for r in results)
        avg_time = sum(r.execution_time for r in results) / len(results)
        
        print(f"📊 基准测试结果:")
        print(f"   总请求数: {len(results)}")
        print(f"   成功请求: {success_count}")
        print(f"   失败请求: {len(results) - success_count}")
        print(f"   总重试次数: {total_retries}")
        print(f"   总耗时: {total_time:.2f}s")
        print(f"   平均请求时间: {avg_time:.2f}s")
        print(f"   并发效率: {len(results)/total_time:.2f} 请求/秒")


async def main():
    """主函数"""
    print("🔧 并行请求客户端测试")
    print("=" * 80)
    
    try:
        # 测试单个请求
        await test_single_request()
        
        # 测试并行请求
        await test_parallel_requests()
        
        # 性能基准测试
        await benchmark_requests()
        
        print("\n✅ 所有测试完成!")
        
    except Exception as e:
        print(f"❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())