"""
实用连接优化器 - 针对垃圾服务器的实际优化方案
"""

import asyncio
import time
import json
from enhanced_connection_client import EnhancedConnectionClient, RequestConfig, ConnectionStrategy


class ServerPerformanceAnalyzer:
    """服务器性能分析器"""
    
    def __init__(self):
        self.performance_data = {
            'response_times': [],
            'success_rate': 0.0,
            'timeout_rate': 0.0,
            'error_rate': 0.0,
            'avg_response_time': 0.0,
            'max_response_time': 0.0
        }
    
    def analyze_server_performance(self, results):
        """分析服务器性能"""
        total_requests = len(results)
        successful_requests = sum(1 for r in results if r.success)
        timeout_requests = sum(1 for r in results if not r.success and '超时' in r.error)
        error_requests = total_requests - successful_requests - timeout_requests
        
        response_times = [r.execution_time for r in results if r.success]
        
        self.performance_data.update({
            'response_times': response_times,
            'success_rate': successful_requests / total_requests if total_requests > 0 else 0.0,
            'timeout_rate': timeout_requests / total_requests if total_requests > 0 else 0.0,
            'error_rate': error_requests / total_requests if total_requests > 0 else 0.0,
            'avg_response_time': sum(response_times) / len(response_times) if response_times else 0.0,
            'max_response_time': max(response_times) if response_times else 0.0
        })
    
    def recommend_strategy(self):
        """推荐最佳连接策略"""
        success_rate = self.performance_data['success_rate']
        timeout_rate = self.performance_data['timeout_rate']
        avg_response_time = self.performance_data['avg_response_time']
        
        if success_rate > 0.8 and avg_response_time < 5.0:
            return ConnectionStrategy.STANDARD, "服务器性能良好，使用标准策略"
        elif timeout_rate > 0.5:
            return ConnectionStrategy.AGGRESSIVE, "服务器频繁超时，使用激进重试策略"
        elif avg_response_time > 10.0:
            return ConnectionStrategy.FAST_TIMEOUT, "服务器响应慢，使用快速超时策略"
        elif success_rate < 0.5:
            return ConnectionStrategy.PERSISTENT, "服务器不稳定，使用持久连接策略"
        else:
            return ConnectionStrategy.STANDARD, "默认使用标准策略"


async def test_server_performance(url: str, headers: dict, json_data: dict, num_requests: int = 5):
    """测试服务器性能"""
    print(f"🔍 正在测试服务器性能: {url}")
    print(f"   测试请求数: {num_requests}")
    
    results = []
    
    async with EnhancedConnectionClient(max_concurrent=3) as client:
        for i in range(num_requests):
            print(f"   进度: {i+1}/{num_requests}")
            
            config = RequestConfig(
                url=url,
                method="POST",
                headers=headers,
                json_data=json_data,
                timeout=15.0,
                max_retries=2,
                connection_strategy=ConnectionStrategy.STANDARD
            )
            
            result = await client.make_request(config)
            results.append(result)
            
            if result.success:
                print(f"      ✅ 成功 - 耗时: {result.execution_time:.2f}s")
            else:
                print(f"      ❌ 失败 - 错误: {result.error}")
    
    return results


async def optimize_connection_for_bad_server():
    """针对垃圾服务器的连接优化"""
    print("🔧 垃圾服务器连接优化方案")
    print("=" * 80)
    
    # 华夏银行API配置
    url = "https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json"
    
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json;charset=utf-8',
        'Cookie': 'BIGipServerpool_mcm_web_4443_v6=!GO6PTwst5dRM6uFDZAWKcNsf5P88uREKUb5G+zYLOgDGSlT8CyoXBN+aZyrgagX++5Mymat4A7lEXFN265aokLZ8c74rQze39EylEaFJ; BIGipServerpool_mcm_web_4443=!Pq84hmEo1DJcK+VDZAWKcNsf5P88uYUOrx+hvvrc5PmqpV18HangNU0B/SslRF50+BpseEkiyUyWdzI=',
        'Host': "mcm.hxb.com.cn",
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
    
    # 步骤1: 性能测试
    print("\n📊 步骤1: 服务器性能测试")
    print("-" * 40)
    
    results = await test_server_performance(url, headers, json_data, num_requests=3)
    
    # 步骤2: 性能分析
    print("\n📊 步骤2: 性能分析")
    print("-" * 40)
    
    analyzer = ServerPerformanceAnalyzer()
    analyzer.analyze_server_performance(results)
    
    performance = analyzer.performance_data
    print(f"✅ 成功率: {performance['success_rate']:.1%}")
    print(f"⏰ 平均响应时间: {performance['avg_response_time']:.2f}s")
    print(f"❌ 超时率: {performance['timeout_rate']:.1%}")
    print(f"⚠️  错误率: {performance['error_rate']:.1%}")
    
    # 步骤3: 策略推荐
    print("\n📊 步骤3: 优化策略推荐")
    print("-" * 40)
    
    recommended_strategy, reason = analyzer.recommend_strategy()
    print(f"🎯 推荐策略: {recommended_strategy.value}")
    print(f"📝 原因: {reason}")
    
    # 步骤4: 应用优化策略
    print("\n📊 步骤4: 应用优化策略")
    print("-" * 40)
    
    # 根据推荐策略调整参数
    strategy_configs = {
        ConnectionStrategy.STANDARD: {
            'timeout': 15.0,
            'max_retries': 3,
            'description': '标准连接策略'
        },
        ConnectionStrategy.FAST_TIMEOUT: {
            'timeout': 8.0,
            'max_retries': 5,
            'description': '快速超时策略'
        },
        ConnectionStrategy.PERSISTENT: {
            'timeout': 30.0,
            'max_retries': 4,
            'description': '持久连接策略'
        },
        ConnectionStrategy.AGGRESSIVE: {
            'timeout': 5.0,
            'max_retries': 6,
            'description': '激进重试策略'
        }
    }
    
    config = strategy_configs[recommended_strategy]
    
    print(f"🔧 应用配置:")
    print(f"   策略: {config['description']}")
    print(f"   超时时间: {config['timeout']}s")
    print(f"   最大重试次数: {config['max_retries']}")
    
    # 步骤5: 验证优化效果
    print("\n📊 步骤5: 验证优化效果")
    print("-" * 40)
    
    async with EnhancedConnectionClient(
        max_concurrent=2, 
        default_timeout=config['timeout'],
        keepalive_timeout=60
    ) as client:
        
        optimized_config = RequestConfig(
            url=url,
            method="POST",
            headers=headers,
            json_data=json_data,
            timeout=config['timeout'],
            max_retries=config['max_retries'],
            connection_strategy=recommended_strategy
        )
        
        print("🚀 执行优化后的请求...")
        result = await client.make_request(optimized_config)
        
        print(f"📊 优化结果:")
        print(f"   {'✅ 成功' if result.success else '❌ 失败'}")
        print(f"   状态码: {result.status_code}")
        print(f"   耗时: {result.execution_time:.2f}s")
        print(f"   重试次数: {result.retry_count}")
        
        if result.success and result.content:
            try:
                content_json = json.loads(result.content)
                print(f"   业务结果: {content_json.get('succ', '未知')}")
                print(f"   错误代码: {content_json.get('head_ret_code', '无')}")
                print(f"   错误信息: {content_json.get('head_ret_msg', '无')}")
            except:
                print(f"   响应内容: {result.content[:200]}...")
    
    # 步骤6: 提供优化建议
    print("\n📊 步骤6: 进一步优化建议")
    print("-" * 40)
    
    if performance['success_rate'] < 0.3:
        print("⚠️  服务器性能极差，建议:")
        print("   • 使用备用服务器策略")
        print("   • 增加重试次数到8-10次")
        print("   • 设置更短的超时时间(3-5秒)")
        print("   • 考虑使用CDN或代理服务器")
    elif performance['timeout_rate'] > 0.3:
        print("⚠️  服务器频繁超时，建议:")
        print("   • 使用快速超时策略")
        print("   • 增加并发连接数")
        print("   • 优化网络连接设置")
    elif performance['avg_response_time'] > 10.0:
        print("⚠️  服务器响应慢，建议:")
        print("   • 使用持久连接策略")
        print("   • 减少请求数据量")
        print("   • 启用连接复用")
    else:
        print("✅ 服务器性能可接受，当前策略已优化")
    
    print("\n" + "=" * 80)
    print("✅ 连接优化完成!")


async def compare_strategies():
    """比较不同策略的效果"""
    print("\n🔬 策略效果对比测试")
    print("=" * 80)
    
    url = "https://mcm.hxb.com.cn/market-gateway-api/market-api/app/userverify100104.json"
    
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json;charset=utf-8',
        'Cookie': 'BIGipServerpool_mcm_web_4443_v6=!GO6PTwst5dRM6uFDZAWKcNsf5P88uREKUb5G+zYLOgDGSlT8CyoXBN+aZyrgagX++5Mymat4A7lEXFN265aokLZ8c74rQze39EylEaFJ; BIGipServerpool_mcm_web_4443=!Pq84hmEo1DJcK+VDZAWKcNsf5P88uYUOrx+hvvrc5PmqpV18HangNU0B/SslRF50+BpseEkiyUyWdzI=',
        'Host': "mcm.hxb.com.cn",
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
    
    strategies = [
        (ConnectionStrategy.STANDARD, "标准策略"),
        (ConnectionStrategy.FAST_TIMEOUT, "快速超时"),
        (ConnectionStrategy.PERSISTENT, "持久连接"),
        (ConnectionStrategy.AGGRESSIVE, "激进重试")
    ]
    
    results = {}
    
    for strategy, name in strategies:
        print(f"\n🔧 测试策略: {name}")
        
        async with EnhancedConnectionClient() as client:
            config = RequestConfig(
                url=url,
                method="POST",
                headers=headers,
                json_data=json_data,
                timeout=10.0,
                max_retries=3,
                connection_strategy=strategy
            )
            
            start_time = time.time()
            result = await client.make_request(config)
            execution_time = time.time() - start_time
            
            results[name] = {
                'success': result.success,
                'execution_time': execution_time,
                'retry_count': result.retry_count,
                'status_code': result.status_code
            }
            
            print(f"   结果: {'✅ 成功' if result.success else '❌ 失败'}")
            print(f"   耗时: {execution_time:.2f}s")
            print(f"   重试: {result.retry_count}次")
    
    # 输出对比结果
    print("\n📊 策略对比结果:")
    print("-" * 40)
    
    for name, data in results.items():
        status = "✅ 成功" if data['success'] else "❌ 失败"
        print(f"{name:12} | {status:8} | 耗时: {data['execution_time']:5.2f}s | 重试: {data['retry_count']}次")


async def main():
    """主函数"""
    print("🔧 垃圾服务器连接优化工具")
    print("=" * 80)
    
    # 执行优化流程
    await optimize_connection_for_bad_server()
    
    # 比较不同策略
    await compare_strategies()
    
    print("\n" + "=" * 80)
    print("✅ 所有测试完成!")
    print("\n💡 使用建议:")
    print("• 对于响应慢的服务器: 使用快速超时策略")
    print("• 对于频繁超时的服务器: 使用激进重试策略") 
    print("• 对于不稳定的服务器: 使用持久连接策略")
    print("• 对于性能良好的服务器: 使用标准策略")


if __name__ == "__main__":
    asyncio.run(main())