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
import warnings
warnings.filterwarnings("ignore")

import asyncio
import aioredis
import traceback
import sys
sys.path.append('/root/work/web')
from market_edge_engine import MarketEdgeEngine
from redis_storage import RedisStorageManager

class MockCalendar:
    def get_previous_trade_day(self, date_str=None):
        return "2026-02-24"

async def main():
    try:
        redis = await aioredis.from_url("redis://localhost:6379/0", encoding='utf-8', decode_responses=True)
        redis_storage = RedisStorageManager()
        calendar = MockCalendar()
        
        engine = MarketEdgeEngine(
            redis=redis,
            redis_storage=redis_storage,
            plate_updater=None,
            calendar=calendar,
            advanced_indicators=None,
            theme_ranker=None
        )
        if hasattr(engine, 'init'):
            if hasattr(engine.init, '__await__'):
                await engine.init()
            else:
                engine.init()
        
        print("Calculating...")
        await engine.calculate_sentiment("20260225")
        res = await redis.hgetall("market:sentiment:20260225")
        
        print("RESULT_START")
        for k, v in res.items():
             print(f"{k}: {v}")
        print("RESULT_END")
        
        await redis.close()
    except Exception as e:
        print("TRACEBACK_START")
        traceback.print_exc()
        print("TRACEBACK_END")

if __name__ == '__main__':
    asyncio.run(main())
"""
        sftp.open('/root/work/web/test_sentiment_cycle_clean.py', 'w').write(test_script)
        
        stdin, stdout, stderr = ssh.exec_command('cd /root/work/web && python test_sentiment_cycle_clean.py')
        
        out = stdout.read().decode('utf-8')
        err = stderr.read().decode('utf-8')
        
        print("--- STDOUT ---")
        print(out)
        print("--- STDERR ---")
        print(err)
            
        sftp.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    run()
