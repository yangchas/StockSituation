"""
成功请求内容输出演示
演示成功请求时的详细内容输出功能
"""

import asyncio
import time
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass
from aiohttp import ClientSession, ClientTimeout
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class RequestConfig:
    """请求配置"""
    url: str
    method: str = "GET"
    headers: Optional[Dict[str, str]] = None
    json_data: Optional[Dict[str, Any]] = None
    timeout: float = 10.0
    max_retries: int = 1
    retry_delay: float = 2.0


@dataclass
class ResponseResult:
    """响应结果"""
    success: bool
    status_code: Optional[int] = None
    content: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    execution_time: float = 0.0
    retry_count: int = 0
    error: Optional[str] = None


class DemoRequestClient:
    """演示用请求客户端"""
    
    def __init__(self, max_concurrent: int = 5, default_timeout: float = 10.0):
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        self.session: Optional[ClientSession] = None
    
    async def __aenter__(self):
        """异步上下文管理器入口"""
        timeout = ClientTimeout(total=self.default_timeout)
        self.session = ClientSession(timeout=timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self.session:
            await self.session.close()
    
    async def make_request(self, config: RequestConfig) -> ResponseResult:
        """
        执行单个请求
        
        Args:
            config: 请求配置
            
        Returns:
            响应结果
        """
        if not self.session:
            raise RuntimeError("客户端未初始化")
        
        start_time = time.time()
        
        # 模拟成功响应
        success_response = {
            "code": "200",
            "message": "操作成功",
            "data": {
                "user_id": "123456789",
                "user_name": "张三",
                "verify_status": "已认证",
                "balance": "10000.00",
                "last_login_time": "2024-01-15 10:30:00",
                "account_info": {
                    "account_number": "6222021001123456789",
                    "bank_name": "华夏银行",
                    "branch": "北京分行"
                },
                "transaction_history": [
                    {"date": "2024-01-10", "amount": "500.00", "type": "收入"},
                    {"date": "2024-01-08", "amount": "-200.00", "type": "支出"},
                    {"date": "2024-01-05", "amount": "1000.00", "type": "收入"}
                ]
            },
            "timestamp": "2024-01-15 14:25:30"
        }
        
        # 模拟响应内容
        content = json.dumps(success_response, ensure_ascii=False, indent=2)
        
        # 模拟响应头
        headers = {
            'content-type': 'application/json; charset=utf-8',
            'content-length': str(len(content)),
            'server': 'nginx/1.18.0',
            'date': 'Mon, 15 Jan 2024 14:25:30 GMT',
            'cache-control': 'no-cache',
            'x-powered-by': 'Express'
        }
        
        # 创建成功结果
        result = ResponseResult(
            success=True,
            status_code=200,
            content=content,
            headers=headers,
            execution_time=time.time() - start_time,
            retry_count=0
        )
        
        # 输出成功请求的详细信息
        self._log_success_response(config.url, result)
        
        return result
    
    def _log_success_response(self, url: str, result: ResponseResult):
        """记录成功响应的详细信息"""
        logger.info(f"✅ 请求成功: {url}")
        logger.info(f"   状态码: {result.status_code}")
        logger.info(f"   耗时: {result.execution_time:.2f}s")
        logger.info(f"   重试次数: {result.retry_count}")
        logger.info(f"   内容长度: {len(result.content)} 字节")
        
        # 输出内容预览
        if result.content:
            preview_length = min(500, len(result.content))
            content_preview = result.content[:preview_length]
            if len(result.content) > preview_length:
                content_preview += "..."
            
            logger.info(f"   内容预览: {content_preview}")
        
        # 输出响应头信息
        if result.headers:
            logger.info(f"   响应头数量: {len(result.headers)}")
            important_headers = ['content-type', 'content-length', 'server', 'date', 'cache-control']
            for header in important_headers:
                if header in result.headers:
                    logger.info(f"     {header}: {result.headers[header]}")


async def demo_success_response():
    """演示成功响应内容输出"""
    print("🚀 演示成功请求的内容输出")
    print("=" * 80)
    
    # 创建请求配置
    config = RequestConfig(
        url="https://api.example.com/success",
        method="POST",
        headers={
            'Content-Type': 'application/json',
            'User-Agent': 'DemoClient/1.0'
        },
        json_data={"action": "verify"},
        timeout=10.0
    )
    
    async with DemoRequestClient() as client:
        result = await client.make_request(config)
        
        print(f"\n📊 请求结果详情:")
        print(f"   成功: {result.success}")
        print(f"   状态码: {result.status_code}")
        print(f"   耗时: {result.execution_time:.2f}s")
        print(f"   重试次数: {result.retry_count}")
        
        if result.success:
            print(f"   响应内容长度: {len(result.content) if result.content else 0}")
            
            # 详细输出成功请求的内容
            if result.content:
                print(f"\n   📄 响应内容详情:")
                print(f"     完整内容:\n{result.content}")
                
                # 解析JSON内容
                try:
                    json_data = json.loads(result.content)
                    print(f"\n   🎯 JSON解析结果:")
                    print(f"     数据类型: {type(json_data)}")
                    
                    if isinstance(json_data, dict):
                        print(f"     字段数量: {len(json_data)}")
                        for key, value in json_data.items():
                            if key == 'data' and isinstance(value, dict):
                                print(f"     {key}:")
                                for sub_key, sub_value in value.items():
                                    if isinstance(sub_value, list):
                                        print(f"       {sub_key} (数组长度 {len(sub_value)}):")
                                        for i, item in enumerate(sub_value[:2]):
                                            print(f"         [{i}]: {item}")
                                        if len(sub_value) > 2:
                                            print(f"         ... 还有 {len(sub_value) - 2} 个元素")
                                    else:
                                        print(f"       {sub_key}: {sub_value}")
                            else:
                                print(f"     {key}: {value}")
                except json.JSONDecodeError:
                    print(f"   📝 文本内容 (非JSON): {result.content[:500]}")
            
            # 输出响应头信息
            if result.headers:
                print(f"\n   📋 响应头信息:")
                print(f"     头信息数量: {len(result.headers)}")
                for header, value in result.headers.items():
                    print(f"     {header}: {value}")
        
        print("\n" + "=" * 80)
        print("✅ 演示完成!")


async def demo_parallel_requests():
    """演示并行请求的成功内容输出"""
    print("\n🚀 演示并行请求的成功内容输出")
    print("=" * 80)
    
    # 创建多个请求配置
    configs = [
        RequestConfig(
            url=f"https://api.example.com/request/{i}",
            method="GET",
            headers={'User-Agent': 'DemoClient/1.0'}
        ) for i in range(3)
    ]
    
    async with DemoRequestClient(max_concurrent=3) as client:
        start_time = time.time()
        
        # 执行并行请求
        tasks = [client.make_request(config) for config in configs]
        results = await asyncio.gather(*tasks)
        
        total_time = time.time() - start_time
        
        print(f"📊 并行请求完成")
        print(f"   总耗时: {total_time:.2f}s")
        print(f"   请求数量: {len(results)}")
        print(f"   成功请求: {sum(1 for r in results if r.success)}")
        
        for i, result in enumerate(results, 1):
            print(f"\n  请求 {i} 详情:")
            print(f"     成功: {result.success}")
            print(f"     状态码: {result.status_code}")
            print(f"     耗时: {result.execution_time:.2f}s")
            
            if result.success and result.content:
                try:
                    json_data = json.loads(result.content)
                    print(f"     响应数据: code={json_data.get('code', 'N/A')}, message={json_data.get('message', 'N/A')}")
                except:
                    print(f"     响应内容长度: {len(result.content)}")
        
        print("\n" + "=" * 80)
        print("✅ 并行请求演示完成!")


async def main():
    """主函数"""
    print("🔧 成功请求内容输出演示")
    print("=" * 80)
    
    try:
        # 演示单个成功响应
        await demo_success_response()
        
        # 演示并行请求
        await demo_parallel_requests()
        
        print("\n🎉 所有演示完成!")
        
    except Exception as e:
        print(f"❌ 演示失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())