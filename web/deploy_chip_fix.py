"""
部署修复文件到服务器并测试筹码峰计算(10只样本)
"""
import paramiko
import os

HOST = '115.190.156.240'
USER = 'root'
PASSWORD = 'Chao123+'

files_to_deploy = [
    ('d:/work/Go/web/services/chip_batch_runner.py', '/root/work/web/services/chip_batch_runner.py'),
    ('d:/work/Go/web/services/advanced_indicators.py', '/root/work/web/services/advanced_indicators.py'),
    ('d:/work/Go/web/redis_storage.py', '/root/work/web/redis_storage.py'),
    ('d:/work/Go/web/services/stock_kline_service.py', '/root/work/web/services/stock_kline_service.py'),
    ('d:/work/Go/web/plate_updater.py', '/root/work/web/plate_updater.py'),
    ('d:/work/Go/web/market_edge_engine.py', '/root/work/web/market_edge_engine.py'),
]

def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, 22, USER, PASSWORD, timeout=10)
    sftp = ssh.open_sftp()

    # 1. 部署文件
    print("=== 部署文件 ===")
    for local, remote in files_to_deploy:
        try:
            sftp.put(local, remote)
            print(f"  ✅ {os.path.basename(local)}")
        except Exception as e:
            print(f"  ❌ {os.path.basename(local)}: {e}")
    sftp.close()

    # 2. 验证 import
    print("\n=== 验证 import ===")
    _, stdout, stderr = ssh.exec_command(
        'cd /root/work && python3 -c "from web.services.chip_batch_runner import ChipBatchRunner; print(\'OK\')" 2>&1',
        timeout=15
    )
    print(f"  chip_batch_runner: {stdout.read().decode().strip()}")

    # 3. 小规模测试 (10只股票)
    print("\n=== 小规模测试 (10只样本) ===")
    test_script = '''
import sys
sys.path.insert(0, '/root/work')
from web.services.chip_batch_runner import ChipBatchRunner
import baostock as bs

runner = ChipBatchRunner()
runner.load_f10_market_cap()

# 登录baostock
lg = bs.login()
print(f"baostock login: {lg.error_code}")

test_codes = ['000001', '600519', '300750', '002594', '000858', '601318', '600036', '000333', '002475', '300308']
start_date = '2025-09-01'  # ~半年
end_date = '2026-02-26'

for code in test_codes:
    df = runner.fetch_kline_baostock(code, start_date, end_date)
    if df is not None and len(df) > 0:
        peak = runner.calculate_chip_peak(df)
        extra = runner.calculate_extra_factors(code, df)
        cap = extra.get('real_market_cap', 0)
        print(f"  {code}: K线{len(df)}条 | 筹码峰={peak.get('peak_price','-')} | 5日涨幅={extra.get('change_pct_5d','-')}% | 换手={extra.get('avg_turnover_5d','-')}% | 涨停={extra.get('limit_up_days_5','-')} | 市值={cap}亿")
    else:
        print(f"  {code}: 无数据")

bs.logout()
print("OK")
'''
    _, stdout, stderr = ssh.exec_command(
        f'cd /root/work && PYTHONPATH=/root/work python3 -c "{test_script}" 2>&1',
        timeout=120
    )
    output = stdout.read().decode('utf-8', errors='replace')
    print(output)
    
    ssh.close()
    print("Done.")

if __name__ == "__main__":
    main()
