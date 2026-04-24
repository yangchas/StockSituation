"""在服务器上设置 crontab: 每个交易日 18:30 自执行筹码峰批量计算"""
import paramiko

HOST = '115.190.156.240'
USER = 'root'
PASSWORD = 'Chao123+'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(HOST, 22, USER, PASSWORD, timeout=10)

def ssh_run(cmd, timeout=15):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    return stdout.read().decode('utf-8', errors='replace').strip()

# 1. 查看当前 crontab
print("=== 当前 crontab ===")
print(ssh_run('crontab -l 2>/dev/null || echo "(empty)"'))

# 2. 添加筹码峰定时任务
cron_line = '30 18 * * 1-5 cd /root/work && PYTHONPATH=/root/work /usr/bin/python3 -m web.services.chip_batch_runner >> /tmp/chip_batch.log 2>&1'
# 先检查是否已存在
existing = ssh_run('crontab -l 2>/dev/null || true')
if 'chip_batch_runner' in existing:
    print("\n✅ crontab 中已存在 chip_batch_runner 任务，跳过")
else:
    # 追加新任务
    cmd = f'(crontab -l 2>/dev/null; echo "{cron_line}") | crontab -'
    result = ssh_run(cmd)
    print(f"\n✅ 已添加 crontab 任务: {cron_line}")

# 3. 确认
print("\n=== 更新后的 crontab ===")
print(ssh_run('crontab -l 2>/dev/null'))

# 4. 手动先跑一次全量 (后台)
print("\n=== 触发首次全量计算 (后台) ===")
ssh_run('cd /root/work && nohup bash -c "PYTHONPATH=/root/work /usr/bin/python3 -m web.services.chip_batch_runner" > /tmp/chip_batch.log 2>&1 &')
print("  ✅ 已在后台启动, 日志: /tmp/chip_batch.log")
print("  预计耗时 15-30 分钟 (5000+只股票)")

ssh.close()
print("\nDone.")
