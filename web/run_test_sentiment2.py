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
        sftp.put("market_edge_engine.py", "/root/work/web/market_edge_engine.py")
        
        test_script = """
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
            limit_up_updater=None,
            calendar=calendar,
            advanced_indicators=None,
            theme_ranker=None
        )
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
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())
"""
        sftp.open('/root/work/web/test_sentiment_cycle_clean.py', 'w').write(test_script)
        
        stdin, stdout, stderr = ssh.exec_command('cd /root/work/web && python -W ignore test_sentiment_cycle_clean.py')
        
        lines = stdout.read().decode('utf-8').splitlines()
        is_result = False
        for line in lines:
            if line == "RESULT_START":
                is_result = True
                continue
            if line == "RESULT_END":
                is_result = False
                continue
            if is_result:
                print(line)
                
        err = stderr.read().decode('utf-8').strip()
        if err and "Blowfish" not in err:
            print(err)
            
        sftp.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    run()
