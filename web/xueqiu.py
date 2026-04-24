import aiohttp
import asyncio
from typing import Dict, Optional, Tuple
import json
import time
import random
from urllib.parse import quote


class XueQiuCookieManager:
    """雪球Cookie管理器"""
    
    def __init__(self):
        self.base_url = "https://xueqiu.com"
        self.session = None
        self.cookies = {}
        self.last_refresh_time = 0
        self.refresh_interval = 3600  # 1小时刷新一次
        
    def _generate_random_params(self) -> str:
        """生成随机参数，模拟浏览器首次访问"""
        # 模拟实际浏览器访问时的随机参数
        timestamp = int(time.time() * 1000)
        random_str = f"{timestamp}-{random.randint(100000, 999999)}"
        encoded_str = quote(random_str)
        return f"?md5__1038={encoded_str}"
    
    async def _create_session(self):
        """创建aiohttp会话"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
    
    async def close(self):
        """关闭会话"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def get_initial_cookies(self) -> Dict[str, str]:
        """
        获取初始cookie（模拟首次访问）
        
        Returns:
            Dict[str, str]: cookie字典
        """
        await self._create_session()
        
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Host": "xueqiu.com",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
        }
        
        try:
            # 生成随机参数访问首页
            params = self._generate_random_params()
            url = f"{self.base_url}/{params}"
            
            async with self.session.get(
                url,
                headers=headers,
                allow_redirects=True,
                timeout=aiohttp.ClientTimeout(total=15)
            ) as response:
                
                # 获取响应中的cookie
                response_cookies = response.cookies
                cookie_dict = {}
                
                for key, cookie in response_cookies.items():
                    cookie_dict[key] = cookie.value
                
                # 更新缓存
                self.cookies.update(cookie_dict)
                self.last_refresh_time = time.time()
                
                # 如果cookie不完整，尝试通过其他API获取
                if 'xq_a_token' not in cookie_dict:
                    await self._get_api_cookies()
                
                return self.cookies
                
        except Exception as e:
            print(f"获取初始cookie失败: {str(e)}")
            return {}
    
    async def _get_api_cookies(self):
        """通过API请求获取更完整的cookie"""
        try:
            # 尝试访问热股榜API
            headers = {
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "Connection": "keep-alive",
                "Host": "stock.xueqiu.com",
                "Referer": "https://xueqiu.com/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
            }
            
            # 使用现有cookie
            if self.cookies:
                headers["Cookie"] = self._build_cookie_string(self.cookies)
            
            url = "https://stock.xueqiu.com/v5/stock/hot_stock/list.json"
            params = {
                "size": "8",
                "_type": "10",
                "type": "10",
                "include": "1"
            }
            
            async with self.session.get(
                url,
                headers=headers,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                
                # 更新cookie
                response_cookies = response.cookies
                for key, cookie in response_cookies.items():
                    self.cookies[key] = cookie.value
                
        except Exception as e:
            print(f"通过API获取cookie失败: {str(e)}")
    
    def _build_cookie_string(self, cookie_dict: Dict[str, str]) -> str:
        """构建cookie字符串"""
        return "; ".join([f"{key}={value}" for key, value in cookie_dict.items()])
    
    def get_cookie_string(self) -> str:
        """获取cookie字符串"""
        return self._build_cookie_string(self.cookies)
    
    def should_refresh(self) -> bool:
        """判断是否需要刷新cookie"""
        current_time = time.time()
        return (current_time - self.last_refresh_time) > self.refresh_interval
    
    async def refresh_cookies(self) -> bool:
        """刷新cookie"""
        try:
            await self.get_initial_cookies()
            return True
        except Exception as e:
            print(f"刷新cookie失败: {str(e)}")
            return False


class AutoXueQiuHotStockAPI:
    """自动处理cookie的雪球热门股票API"""
    
    def __init__(self):
        self.cookie_manager = XueQiuCookieManager()
        self.base_url = "https://stock.xueqiu.com/v5/stock/hot_stock/list.json"
        self.headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "Connection": "keep-alive",
            "Host": "stock.xueqiu.com",
            "Referer": "https://xueqiu.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0"
        }
    
    async def initialize(self):
        """初始化，获取初始cookie"""
        await self.cookie_manager.get_initial_cookies()
    
    async def close(self):
        """关闭资源"""
        await self.cookie_manager.close()
    
    async def _get_trending_stocks_with_retry(
        self, 
        size: int = 20,
        max_retries: int = 2
    ) -> Tuple[Optional[list], bool]:
        """
        带重试机制的获取热门股票
        
        Returns:
            Tuple[Optional[list], bool]: (股票数据, 是否需要刷新cookie)
        """
        retry_count = 0
        need_refresh = False
        
        while retry_count <= max_retries:
            try:
                # 检查是否需要刷新cookie
                if retry_count > 0 or self.cookie_manager.should_refresh():
                    await self.cookie_manager.refresh_cookies()
                
                # 构建请求
                params = {
                    "size": size,
                    "_type": "10",
                    "type": "10",
                    "include": "1"
                }
                
                headers = self.headers.copy()
                headers["Cookie"] = self.cookie_manager.get_cookie_string()
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        self.base_url,
                        headers=headers,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        
                        if response.status == 403:
                            # Cookie失效，需要刷新
                            need_refresh = True
                            retry_count += 1
                            continue
                        
                        if response.status != 200:
                            raise Exception(f"请求失败，状态码: {response.status}{await response.text()}")
                        
                        data = await response.json()
                        
                        # 检查返回数据
                        error_code = data.get("error_code")
                        if error_code is not None and error_code != 0:
                            if error_code == 40000:  # 常见的token失效错误码
                                need_refresh = True
                                retry_count += 1
                                continue
                            else:
                                error_description = data.get("error_description", "未知错误")
                                raise Exception(f"接口返回错误: {error_description} (错误码: {error_code})")
                        
                        # 成功获取数据
                        stocks_data = data.get("data", {}).get("items", [])
                        return stocks_data, False
                        
            except aiohttp.ClientError as e:
                print(f"网络请求错误: {str(e)}")
                retry_count += 1
                if retry_count > max_retries:
                    return None, False
                
            except Exception as e:
                print(f"获取数据失败: {str(e)}")
                retry_count += 1
                if retry_count > max_retries:
                    return None, False
        
        return None, need_refresh
    
    async def get_trending_stocks(self, size: int = 20) -> Optional[list]:
        """
        获取热门股票数据（自动处理cookie）
        
        Args:
            size: 返回的股票数量
            
        Returns:
            Optional[list]: 股票数据列表，失败返回None
        """
        # 首次运行需要初始化
        if not self.cookie_manager.cookies:
            await self.initialize()
        
        stocks, need_refresh = await self._get_trending_stocks_with_retry(size)
        
        # 如果需要刷新cookie，尝试刷新后重试
        if need_refresh:
            print("检测到cookie失效，正在刷新...")
            if await self.cookie_manager.refresh_cookies():
                stocks, _ = await self._get_trending_stocks_with_retry(size)
        
        return stocks
    
    async def monitor_and_refresh(self, interval: int = 1800):
        """
        监控并自动刷新cookie
        
        Args:
            interval: 检查间隔（秒）
        """
        while True:
            try:
                await asyncio.sleep(interval)
                
                if self.cookie_manager.should_refresh():
                    print(f"自动刷新cookie...")
                    success = await self.cookie_manager.refresh_cookies()
                    if success:
                        print("Cookie刷新成功")
                    else:
                        print("Cookie刷新失败")
                        
            except Exception as e:
                print(f"监控任务出错: {str(e)}")
    
    def save_cookies_to_file(self, filepath: str = "xueqiu_cookies.json"):
        """保存cookies到文件"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump({
                    'cookies': self.cookie_manager.cookies,
                    'last_refresh': self.cookie_manager.last_refresh_time
                }, f, ensure_ascii=False, indent=2)
            print(f"Cookies已保存到: {filepath}")
        except Exception as e:
            print(f"保存cookies失败: {str(e)}")
    
    def load_cookies_from_file(self, filepath: str = "xueqiu_cookies.json") -> bool:
        """从文件加载cookies"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.cookie_manager.cookies = data.get('cookies', {})
                self.cookie_manager.last_refresh_time = data.get('last_refresh', 0)
            print(f"Cookies已从文件加载: {filepath}")
            return True
        except FileNotFoundError:
            print(f"Cookie文件不存在: {filepath}")
            return False
        except Exception as e:
            print(f"加载cookies失败: {str(e)}")
            return False


# 使用示例
async def main():
    # 创建API实例
    api = AutoXueQiuHotStockAPI()
    
    try:
        # 尝试从文件加载cookie
        if not api.load_cookies_from_file():
            print("未找到cookie文件，开始初始化...")
            await api.initialize()
        
        # 获取热门股票数据
        print("获取热门股票数据...")
        stocks = await api.get_trending_stocks(size=10)
        
        if stocks:
            print(f"成功获取到 {len(stocks)} 个热门股票")
            for i, stock in enumerate(stocks[:5], 1):
                print(f"{i}. {stock.get('symbol')} {stock.get('name')} - 价格: {stock.get('current')}")
        else:
            print("获取热门股票数据失败")
        
        # 保存cookie到文件
        api.save_cookies_to_file()
        
        # 启动cookie监控（在实际应用中，可以放到后台运行）
        # asyncio.create_task(api.monitor_and_refresh(interval=600))  # 每10分钟检查一次
        
    except Exception as e:
        print(f"运行出错: {str(e)}")
    
    finally:
        # 关闭资源
        await api.close()


# 定期刷新cookie的后台任务
async def background_refresh_task(api: AutoXueQiuHotStockAPI, interval: int = 3600):
    """
    后台定期刷新cookie的任务
    
    Args:
        api: API实例
        interval: 刷新间隔（秒）
    """
    while True:
        try:
            await asyncio.sleep(interval)
            print(f"[后台任务] 检查并刷新cookie...")
            if await api.cookie_manager.refresh_cookies():
                print("[后台任务] Cookie刷新成功")
                # 保存更新后的cookie
                api.save_cookies_to_file()
            else:
                print("[后台任务] Cookie刷新失败")
                
        except Exception as e:
            print(f"[后台任务] 出错: {str(e)}")


# 完整的应用示例
async def full_example():
    """完整的使用示例"""
    api = AutoXueQiuHotStockAPI()
    
    try:
        # 1. 初始化
        print("=== 1. 初始化 ===")
        await api.initialize()
        
        # 2. 获取数据
        print("\n=== 2. 获取热门股票 ===")
        stocks = await api.get_trending_stocks(size=5)
        if stocks:
            print("热门股票:")
            for stock in stocks:
                print(f"  {stock.get('symbol')} {stock.get('name')} "
                      f"- 当前价: {stock.get('current')} "
                      f"- 涨跌幅: {stock.get('percent'):+.2f}%")
        
        # 3. 保存cookie
        print("\n=== 3. 保存Cookie ===")
        api.save_cookies_to_file()
        
        # 4. 启动后台刷新任务（在实际应用中）
        # print("\n=== 4. 启动后台刷新任务 ===")
        # refresh_task = asyncio.create_task(background_refresh_task(api, interval=1800))
        
        # 5. 模拟后续请求
        print("\n=== 5. 模拟后续请求 ===")
        await asyncio.sleep(2)
        stocks = await api.get_trending_stocks(size=3)
        if stocks:
            print("再次获取热门股票成功")
        
        # 等待一段时间，让后台任务运行（在实际应用中）
        # await asyncio.sleep(3600)
        # refresh_task.cancel()
        
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"程序出错: {str(e)}")
    finally:
        await api.close()


if __name__ == "__main__":
    # 简单示例
    print("简单示例:")
    asyncio.run(main())
    
    # 完整示例（取消注释以运行）
    # print("\n\n完整示例:")
    # asyncio.run(full_example())