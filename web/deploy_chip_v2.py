"""部署增强版并重新测试"""
import paramiko
import os

HOST = '115.190.156.240'
USER = 'root'
PASSWORD = 'Chao123+'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, 22, USER, PASSWORD, timeout=10)
sftp = ssh.open_sftp()

# 部署
for local, remote in [
    ('d:/work/Go/web/services/chip_batch_runner.py', '/root/work/web/services/chip_batch_runner.py'),
    ('d:/work/Go/web/market_edge_engine.py', '/root/work/web/market_edge_engine.py'),
]:
    sftp.put(local, remote)
    print(f"✅ {os.path.basename(local)}")
sftp.close()

# 增强版测试
test_script = r"""
import sys
sys.path.insert(0, '/root/work')
from web.services.chip_batch_runner import ChipBatchRunner
import baostock as bs

runner = ChipBatchRunner()
runner.load_f10_market_cap()
lg = bs.login()

test_codes = ['000001', '600519', '300750', '002594', '300308']
start_date = '2025-09-01'
end_date = '2026-02-26'

for code in test_codes:
    try:
        df = runner.fetch_kline_baostock(code, start_date, end_date)
        if df is not None and len(df) > 0:
            peak = runner.calculate_chip_peak(df)
            extra = runner.calculate_extra_factors(code, df)
            cap = extra.get('real_market_cap', 0)
            print(f"  {code}:")
            print(f"    K线={len(df)}条, 市值={cap}亿")
            print(f"    筹码峰: peak={peak.get('peak_price','-')}, 区间={peak.get('price_start','-')}~{peak.get('price_end','-')}")
            print(f"    占比={peak.get('chip_percent','-')}%, 均成本={peak.get('avg_cost','-')}")
            print(f"    集中度={peak.get('concentration','-')}, 密集区={peak.get('dense_area_count','-')}个")
            print(f"    盈利={peak.get('profit_ratio','-')}, 亏损={peak.get('loss_ratio','-')}")
            print(f"    5日涨幅={extra.get('change_pct_5d','-')}%, 换手={extra.get('avg_turnover_5d','-')}%")
    except Exception as e:
        print(f"  {code}: ERR {e}")

bs.logout()
print("\nDONE")
"""

sftp = ssh.open_sftp()
with sftp.open('/tmp/test_chip2.py', 'w') as f:
    f.write(test_script)
sftp.close()

_, stdout, _ = ssh.exec_command('cd /root/work && PYTHONPATH=/root/work python3 /tmp/test_chip2.py 2>&1', timeout=120)
print(stdout.read().decode('utf-8', errors='replace'))
ssh.close()
