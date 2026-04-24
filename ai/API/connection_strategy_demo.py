"""
连接策略演示 - 针对垃圾服务器的优化方案
"""

import asyncio
import time
from enhanced_connection_client import EnhancedConnectionClient, RequestConfig, ConnectionStrategy


async def demo_standard_strategy():
    """演示标准策略"""
    print("\n🔧 1. 标准连接策略")
    print("=" * 50)
    print("特点: 平衡的超时和重试设置")
    print("适用: 一般服务器，响应时间稳定")
    
    async with EnhancedConnectionClient() as client:
        config = RequestConfig(
            url="https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json",
            method="POST",
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json;charset=utf-8',
                'Cookie': 'BIGipServerpool_mcm_web_4443_v6=!GO6PTwst5dRM6uFDZAWKcNsf5P88uREKUb5G+zYLOgDGSlT8CyoXBN+aZyrgagX++5Mymat4A7lEXFN265aokLZ8c74rQze39EylEaFJ; BIGipServerpool_mcm_web_4443=!Pq84hmEo1DJcK+VDZAWKcNsf5P88uYUOrx+hvvrc5PmqpV18HangNU0B/SslRF50+BpseEkiyUyWdzI=',
                'Host': 'mcm.hxb.com.cn',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            json_data={
                "head_jsessionid": "",
                "h5_channel_id": "",
                "head_area_code": "110000",
                "head_iv": "0000000000000000",
                "head_osnumber": "72c86cea-83dd-65f6-6826-77cf2ee19990",
                "system_id": "hxmark",
                "request_time": "4FD5A59F2A991063382DAF7F546731F9",
                "head_trans_code": "100104",
                "head_mac": "FAD0F4FE769D5F837334920A3F2EF399"
            },
            timeout=15.0,
            max_retries=3,
            connection_strategy=ConnectionStrategy.STANDARD
        )
        
        result = await client.make_request(config)
        print(f"结果: {'✅ 成功' if result.success else '❌ 失败'}")
        print(f"状态码: {result.status_code}")
        print(f"耗时: {result.execution_time:.2f}s")
        print(f"重试次数: {result.retry_count}")


async def demo_fast_timeout_strategy():
    """演示快速超时策略"""
    print("\n⚡ 2. 快速超时策略")
    print("=" * 50)
    print("特点: 更短的超时时间，更快的重试")
    print("适用: 响应慢但偶尔能成功的服务器")
    
    async with EnhancedConnectionClient() as client:
        config = RequestConfig(
            url="https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json",
            method="POST",
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json;charset=utf-8',
                'Cookie': 'BIGipServerpool_mcm_web_4443_v6=!GO6PTwst5dRM6uFDZAWKcNsf5P88uREKUb5G+zYLOgDGSlT8CyoXBN+aZyrgagX++5Mymat4A7lEXFN265aokLZ8c74rQze39EylEaFJ; BIGipServerpool_mcm_web_4443=!Pq84hmEo1DJcK+VDZAWKcNsf5P88uYUOrx+hvvrc5PmqpV18HangNU0B/SslRF50+BpseEkiyUyWdzI=',
                'Host': 'mcm.hxb.com.cn',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            json_data={
                "head_jsessionid": "",
                "h5_channel_id": "",
                "head_area_code": "110000",
                "head_iv": "0000000000000000",
                "head_osnumber": "72c86cea-83dd-65f6-6826-77cf2ee19990",
                "system_id": "hxmark",
                "request_time": "4FD5A59F2A991063382DAF7F546731F9",
                "head_trans_code": "100104",
                "head_mac": "FAD0F4FE769D5F837334920A3F2EF399"
            },
            timeout=8.0,  # 更短的超时时间
            max_retries=5,  # 更多的重试次数
            connection_strategy=ConnectionStrategy.FAST_TIMEOUT
        )
        
        result = await client.make_request(config)
        print(f"结果: {'✅ 成功' if result.success else '❌ 失败'}")
        print(f"状态码: {result.status_code}")
        print(f"耗时: {result.execution_time:.2f}s")
        print(f"重试次数: {result.retry_count}")


async def demo_persistent_strategy():
    """演示持久连接策略"""
    print("\n🔗 3. 持久连接策略")
    print("=" * 50)
    print("特点: 更长的超时时间，保持连接")
    print("适用: 不稳定但可恢复的服务器")
    
    async with EnhancedConnectionClient(keepalive_timeout=60) as client:  # 更长的保持连接时间
        config = RequestConfig(
            url="https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json",
            method="POST",
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json;charset=utf-8',
                'Cookie': 'BIGipServerpool_mcm_web_4443_v6=!GO6PTwst5dRM6uFDZAWKcNsf5P88uREKUb5G+zYLOgDGSlT8CyoXBN+aZyrgagX++5Mymat4A7lEXFN265aokLZ8c74rQze39EylEaFJ; BIGipServerpool_mcm_web_4443=!Pq84hmEo1DJcK+VDZAWKcNsf5P88uYUOrx+hvvrc5PmqpV18HangNU0B/SslRF50+BpseEkiyUyWdzI=',
                'Host': "mcm.hxb.com.cn",
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            json_data={
                "head_jsessionid": "",
                "h5_channel_id": "",
                "head_area_code": "110000",
                "head_iv": "0000000000000000",
                "head_osnumber": "72c86cea-83dd-65f6-6826-77cf2ee19990",
                "system_id": "hxmark",
                "request_time": "4FD5A59F2A991063382DAF7F546731F9",
                "head_trans_code": "100104",
                "head_mac": "FAD0F4FE769D5F837334920A3F2EF399"
            },
            timeout=30.0,  # 更长的超时时间
            max_retries=4,  # 中等重试次数
            connection_strategy=ConnectionStrategy.PERSISTENT
        )
        
        result = await client.make_request(config)
        print(f"结果: {'✅ 成功' if result.success else '❌ 失败'}")
        print(f"状态码: {result.status_code}")
        print(f"耗时: {result.execution_time:.2f}s")
        print(f"重试次数: {result.retry_count}")


async def demo_aggressive_strategy():
    """演示激进重试策略"""
    print("\n💥 4. 激进重试策略")
    print("=" * 50)
    print("特点: 非常短的超时，大量重试")
    print("适用: 频繁超时的垃圾服务器")
    
    async with EnhancedConnectionClient() as client:
        config = RequestConfig(
            url="https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json",
            method="POST",
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json;charset=utf-8',
                'Cookie': 'BIGipServerpool_mcm_web_4443_v6=!GO6PTwst5dRM6uFDZAWKcNsf5P88uREKUb5G+zYLOgDGSlT8CyoXBN+aZyrgagX++5Mymat4A7lEXFN265aokLZ8c74rQze39EylEaFJ; BIGipServerpool_mcm_web_4443=!Pq84hmEo1DJcK+VDZAWKcNsf5P88uYUOrx+hvvrc5PmqpV18HangNU0B/SslRF50+BpseEkiyUyWdzI=',
                'Host': "mcm.hxb.com.cn",
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            json_data={
                "head_jsessionid": "",
                "h5_channel_id": "",
                "head_area_code": "110000",
                "head_iv": "0000000000000000",
                "head_osnumber": "72c86cea-83dd-65f6-6826-77cf2ee19990",
                "system_id": "hxmark",
                "request_time": "4FD5A59F2A991063382DAF7F546731F9",
                "head_trans_code": "100104",
                "head_mac": "FAD0F4FE769D5F837334920A3F2EF399"
            },
            timeout=5.0,   # 非常短的超时时间
            max_retries=3,  # 合理重试次数
            connection_strategy=ConnectionStrategy.AGGRESSIVE
        )
        
        result = await client.make_request(config)
        print(f"结果: {'✅ 成功' if result.success else '❌ 失败'}")
        print(f"状态码: {result.status_code}")
        print(f"耗时: {result.execution_time:.2f}s")
        print(f"重试次数: {result.retry_count}")


async def demo_backup_strategy():
    """演示备用服务器策略"""
    print("\n🔄 5. 备用服务器策略")
    print("=" * 50)
    print("特点: 自动切换到备用服务器")
    print("适用: 主服务器经常宕机的情况")
    
    async with EnhancedConnectionClient() as client:
        config = RequestConfig(
            url="https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json",
            method="POST",
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json;charset=utf-8',
                'Cookie': 'BIGipServerpool_mcm_web_4443_v6=!GO6PTwst5dRM6uFDZAWKcNsf5P88uREKUb5G+zYLOgDGSlT8CyoXBN+aZyrgagX++5Mymat4A7lEXFN265aokLZ8c74rQze39EylEaFJ; BIGipServerpool_mcm_web_4443=!Pq84hmEo1DJcK+VDZAWKcNsf5P88uYUOrx+hvvrc5PmqpV18HangNU0B/SslRF50+BpseEkiyUyWdzI=',
                'Host': "mcm.hxb.com.cn",
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            },
            json_data={
                "head_jsessionid": "",
                "h5_channel_id": "",
                "head_area_code": "110000",
                "head_iv": "0000000000000000",
                "head_osnumber": "72c86cea-83dd-65f6-6826-77cf2ee19990",
                "system_id": "hxmark",
                "request_time": "4FD5A59F2A991063382DAF7F546731F9",
                "head_trans_code": "100104",
                "head_mac": "FAD0F4FE769D5F837334920A3F2EF399"
            },
            timeout=10.0,
            max_retries=2,
            connection_strategy=ConnectionStrategy.BACKUP,
            backup_urls=[
                "https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json",
                "https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json"  # 实际应用中应该是不同的备用URL
            ]
        )
        
        result = await client.make_request(config)
        print(f"结果: {'✅ 成功' if result.success else '❌ 失败'}")
        print(f"状态码: {result.status_code}")
        print(f"耗时: {result.execution_time:.2f}s")
        print(f"重试次数: {result.retry_count}")
        print(f"使用的URL: {result.used_url}")


async def demo_parallel_strategies():
    """演示并行使用不同策略"""
    print("\n🚀 6. 并行策略测试")
    print("=" * 50)
    print("特点: 同时测试多种策略，选择最佳方案")
    
    strategies = [
        (ConnectionStrategy.STANDARD, "标准策略"),
        (ConnectionStrategy.FAST_TIMEOUT, "快速超时"),
        (ConnectionStrategy.PERSISTENT, "持久连接"),
        (ConnectionStrategy.AGGRESSIVE, "激进重试")
    ]
    
    async with EnhancedConnectionClient(max_concurrent=4) as client:
        tasks = []
        
        for strategy, name in strategies:
            config = RequestConfig(
                url="https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json",
                method="POST",
                headers={
                    'Accept': 'application/json',
                    'Content-Type': 'application/json;charset=utf-8',
                    'Cookie': 'BIGipServerpool_mcm_web_4443_v6=!GO6PTwst5dRM6uFDZAWKcNsf5P88uREKUb5G+zYLOgDGSlT8CyoXBN+aZyrgagX++5Mymat4A7lEXFN265aokLZ8c74rQze39EylEaFJ; BIGipServerpool_mcm_web_4443=!Pq84hmEo1DJcK+VDZAWKcNsf5P88uYUOrx+hvvrc5PmqpV18HangNU0B/SslRF50+BpseEkiyUyWdzI=',
                    'Host': "mcm.hxb.com.cn",
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                },
                json_data={
                    "head_jsessionid": "",
                    "h5_channel_id": "",
                    "head_area_code": "110000",
                    "head_iv": "0000000000000000",
                    "head_osnumber": "72c86cea-83dd-65f6-6826-77cf2ee19990",
                    "system_id": "hxmark",
                    "request_time": "4FD5A59F2A991063382DAF7F546731F9",
                    "head_trans_code": "100104",
                    "head_mac": "FAD0F4FE769D5F837334920A3F2EF399"
                },
                timeout=10.0,
                max_retries=3,
                connection_strategy=strategy
            )
            
            tasks.append((name, client.make_request(config)))
        
        # 并行执行
        results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
        
        # 输出结果
        for (name, _), result in zip(tasks, results):
            if isinstance(result, Exception):
                print(f"{name}: ❌ 异常 - {str(result)}")
            else:
                print(f"{name}: {'✅ 成功' if result.success else '❌ 失败'} - 耗时: {result.execution_time:.2f}s, 重试: {result.retry_count}")


async def main():
    """主演示函数"""
    print("🔧 连接策略优化演示")
    print("=" * 80)
    print("针对垃圾服务器的多种连接优化方案")
    print()
    
    # 演示各种策略
    await demo_standard_strategy()
    await demo_fast_timeout_strategy()
    await demo_persistent_strategy()
    await demo_aggressive_strategy()
    await demo_backup_strategy()
    await demo_parallel_strategies()
    
    print("\n" + "=" * 80)
    print("✅ 连接策略演示完成!")
    print("\n📋 策略选择建议:")
    print("• 标准策略: 一般服务器")
    print("• 快速超时: 响应慢的服务器") 
    print("• 持久连接: 不稳定但可恢复的服务器")
    print("• 激进重试: 频繁超时的垃圾服务器")
    print("• 备用策略: 主服务器经常宕机的情况")


if __name__ == "__main__":
    asyncio.run(main())