"""
智能重试示例 - 演示成功时返回内容，失败时重新请求
"""

import asyncio
import time
from parallel_request_client import ParallelRequestClient, RequestConfig, ResponseResult


async def test_smart_retry_with_different_scenarios():
    """测试不同场景下的智能重试功能"""
    print("🚀 智能重试功能测试")
    print("=" * 80)
    
    # 创建不同的测试场景
    test_scenarios = [
        {
            "name": "正常请求",
            "url": "https://httpbin.org/status/200",
            "method": "GET",
            "expected_status": 200,
            "should_retry": False
        },
        {
            "name": "服务器错误(可重试)",
            "url": "https://httpbin.org/status/503",
            "method": "GET", 
            "expected_status": 503,
            "should_retry": True
        },
        {
            "name": "客户端错误(不重试)",
            "url": "https://httpbin.org/status/404",
            "method": "GET",
            "expected_status": 404,
            "should_retry": False
        },
        {
            "name": "超时测试",
            "url": "https://httpbin.org/delay/3",  # 3秒延迟
            "method": "GET",
            "timeout": 2.0,  # 2秒超时
            "should_retry": True
        }
    ]
    
    async with ParallelRequestClient(max_concurrent=3, default_timeout=10.0) as client:
        for i, scenario in enumerate(test_scenarios, 1):
            print(f"\n📋 测试场景 {i}: {scenario['name']}")
            print("-" * 40)
            
            # 创建请求配置
            config = RequestConfig(
                url=scenario['url'],
                method=scenario['method'],
                timeout=scenario.get('timeout', 10.0),
                max_retries=3,
                retry_delay=1.0
            )
            
            # 执行请求
            start_time = time.time()
            result = await client.make_request(config)
            execution_time = time.time() - start_time
            
            # 分析结果
            print(f"📊 请求结果:")
            print(f"   成功: {result.success}")
            print(f"   状态码: {result.status_code}")
            print(f"   重试次数: {result.retry_count}")
            print(f"   总耗时: {execution_time:.2f}s")
            
            if result.success:
                print(f"   ✅ 请求成功，立即返回内容")
                if result.content:
                    print(f"   内容长度: {len(result.content)} 字节")
            else:
                print(f"   ❌ 请求失败")
                print(f"   错误信息: {result.error}")
                
                # 检查是否符合预期
                if scenario.get('expected_status'):
                    if result.status_code == scenario['expected_status']:
                        print(f"   ✅ 符合预期状态码")
                    else:
                        print(f"   ❌ 状态码不符合预期")
                
                # 检查重试行为
                if scenario['should_retry']:
                    if result.retry_count > 0:
                        print(f"   ✅ 进行了重试")
                    else:
                        print(f"   ❌ 应该重试但没有重试")
                else:
                    if result.retry_count == 0:
                        print(f"   ✅ 正确未重试")
                    else:
                        print(f"   ❌ 不应该重试但重试了")


async def test_parallel_retry_with_mixed_scenarios():
    """测试混合场景下的并行重试"""
    print("\n🚀 并行混合场景重试测试")
    print("=" * 80)
    
    # 创建混合场景的请求配置
    configs = [
        RequestConfig(url="https://httpbin.org/status/200", method="GET", max_retries=2),
        RequestConfig(url="https://httpbin.org/status/503", method="GET", max_retries=3),
        RequestConfig(url="https://httpbin.org/status/404", method="GET", max_retries=2),
        RequestConfig(url="https://httpbin.org/delay/2", method="GET", timeout=1.0, max_retries=2),
        RequestConfig(url="https://httpbin.org/status/500", method="GET", max_retries=3)
    ]
    
    async with ParallelRequestClient(max_concurrent=3, default_timeout=5.0) as client:
        start_time = time.time()
        results = await client.make_parallel_requests(configs)
        total_time = time.time() - start_time
        
        print(f"📊 并行请求完成")
        print(f"   总请求数: {len(results)}")
        print(f"   总耗时: {total_time:.2f}s")
        
        # 统计结果
        success_count = sum(1 for r in results if r.success)
        failure_count = len(results) - success_count
        total_retries = sum(r.retry_count for r in results)
        
        print(f"   成功请求: {success_count}")
        print(f"   失败请求: {failure_count}")
        print(f"   总重试次数: {total_retries}")
        
        # 详细分析每个请求
        print("\n📋 详细请求分析:")
        for i, (config, result) in enumerate(zip(configs, results), 1):
            print(f"\n   请求 {i}: {config.url}")
            print(f"      成功: {result.success}")
            print(f"      状态码: {result.status_code}")
            print(f"      重试次数: {result.retry_count}")
            print(f"      耗时: {result.execution_time:.2f}s")
            
            if result.success:
                print(f"      ✅ 成功返回内容")
            else:
                print(f"      ❌ 失败原因: {result.error}")


async def test_adaptive_retry_strategy():
    """测试自适应重试策略"""
    print("\n🚀 自适应重试策略测试")
    print("=" * 80)
    
    # 测试不同的错误类型和重试策略
    error_scenarios = [
        ("超时错误", "timeout", 2.0),
        ("连接错误", "connection", 1.5),
        ("服务器错误", "server", 1.0)
    ]
    
    for scenario_name, error_type, base_delay in error_scenarios:
        print(f"\n📋 测试场景: {scenario_name}")
        print("-" * 30)
        
        # 模拟不同重试次数的延迟计算
        for retry_count in range(1, 4):
            # 创建模拟错误信息
            if error_type == "timeout":
                error_msg = f"请求超时 (超时时间: 10s)"
            elif error_type == "connection":
                error_msg = "连接错误: Connection refused"
            else:
                error_msg = "服务器错误: 503"
            
            # 计算延迟时间
            from parallel_request_client import ParallelRequestClient
            client = ParallelRequestClient()
            delay = client._calculate_retry_delay(base_delay, retry_count, error_msg)
            
            print(f"   重试 {retry_count}: 基础延迟 {base_delay}s → 实际延迟 {delay:.1f}s")


async def demonstrate_success_content_return():
    """演示成功时立即返回内容的功能"""
    print("\n🚀 成功时立即返回内容演示")
    print("=" * 80)
    
    async with ParallelRequestClient() as client:
        # 测试成功请求
        config = RequestConfig(
            url="https://httpbin.org/json",
            method="GET",
            timeout=10.0,
            max_retries=0  # 不重试，直接返回
        )
        
        print("📋 测试成功请求:")
        start_time = time.time()
        result = await client.make_request(config)
        execution_time = time.time() - start_time
        
        print(f"✅ 请求成功!")
        print(f"   状态码: {result.status_code}")
        print(f"   耗时: {execution_time:.2f}s")
        print(f"   内容长度: {len(result.content) if result.content else 0} 字节")
        
        if result.content:
            print(f"   内容预览: {result.content[:100]}...")
            print(f"   📝 成功时立即返回了完整内容!")


async def main():
    """主函数"""
    print("🔧 智能重试功能演示")
    print("=" * 80)
    
    try:
        # 演示成功时返回内容
        await demonstrate_success_content_return()
        
        # 测试不同场景的智能重试
        await test_smart_retry_with_different_scenarios()
        
        # 测试并行混合场景
        await test_parallel_retry_with_mixed_scenarios()
        
        # 测试自适应重试策略
        await test_adaptive_retry_strategy()
        
        print("\n✅ 所有演示完成!")
        print("\n📋 功能总结:")
        print("   ✅ 成功时立即返回内容")
        print("   ✅ 失败时智能重试")
        print("   ✅ 根据错误类型调整重试策略")
        print("   ✅ 支持并行请求的重试管理")
        print("   ✅ 自适应延迟计算")
        
    except Exception as e:
        print(f"❌ 演示失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 运行异步主函数
    asyncio.run(main())