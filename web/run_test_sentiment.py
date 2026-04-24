import paramiko

def run():
    host = '115.190.156.240'
    port = 22
    user = 'root'
    password = 'Chao123+'
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, port, user, password)
        sftp = ssh.open_sftp()
        
        test_script = """
import asyncio
import aioredis
import sys
sys.path.append('/root/work')
from web.market_edge_engine import MarketEdgeEngine
from web.redis_storage import RedisStorageManager

class MockService:
    pass

class MockCalendar:
    def get_previous_trade_day(self, date_str=None):
        return "2026-02-24"

async def main():
    redis = await aioredis.from_url("redis://localhost:6379/0", encoding='utf-8', decode_responses=True)
    redis_storage = RedisStorageManager()
    calendar = MockCalendar()
    
    engine = MarketEdgeEngine(
        redis=redis,
        redis_storage=redis_storage,
        limit_up_updater=MockService(),
        calendar=calendar,
        advanced_indicators=MockService(),
        theme_ranker=MockService()
    )
    
    print("Dependencies injected. Initializing engine...")
    if hasattr(engine, 'init'):
        if hasattr(engine.init, '__await__'):
            await engine.init()
        else:
            engine.init()
        
    date_str = "20260225"
    print(f"Calculating sentiment for {date_str}...")
    await engine.calculate_sentiment(date_str)
    
    print("Fetching Result from Redis...")
    res = await redis.hgetall(f"market:sentiment:{date_str}")
    for k, v in res.items():
        print(f"  {k}: {v}")
        
    await redis.close()

if __name__ == '__main__':
    asyncio.run(main())
"""
        sftp.open('/root/work/web/test_sentiment_cycle.py', 'w').write(test_script)
        
        stdin, stdout, stderr = ssh.exec_command('cd /root/work/web && python -W ignore test_sentiment_cycle.py')
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        print("Output:\n", out)
        if err:
            print("Error:\n", err)
            
        sftp.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    run()
