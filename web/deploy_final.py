import paramiko
import os
import sys
import io
import time

# Fix stdout encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

HOST = '115.190.156.240'
USER = 'root'
PASSWORD = 'Chao123+'

def deploy_and_restart():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(HOST, 22, USER, PASSWORD, timeout=10)
    
    # 1. Sync files
    sftp = ssh.open_sftp()
    
    # Paths relative to d:/work/Go
    files_to_sync = [
        ('d:/work/Go/web/market_edge_engine.py', '/root/work/web/market_edge_engine.py'),
        ('d:/work/Go/web/plate_updater.py', '/root/work/web/plate_updater.py'),
        ('d:/work/Go/web/services/trading_calendar_service.py', '/root/work/web/services/trading_calendar_service.py'),
        ('d:/work/Go/web/integrated_server.py', '/root/work/web/integrated_server.py'),
    ]
    
    for local, remote in files_to_sync:
        if os.path.exists(local):
            sftp.put(local, remote)
            print(f"OK: Uploaded {local} to {remote}")
        else:
            print(f"WARN: Local file not found: {local}")
            
    sftp.close()
    
    # 2. Restart server
    print("Stopping existing integrated_server.py...")
    ssh.exec_command("pkill -9 -f integrated_server.py")
    
    time.sleep(2)
    
    print("Starting integrated_server.py...")
    ssh.exec_command("cd /root/work/web && nohup python3 integrated_server.py > nohup.out 2>&1 &")
    
    time.sleep(3)
    stdin, stdout, stderr = ssh.exec_command("ps -ef | grep integrated_server.py | grep -v grep")
    print("PS Output:")
    print(stdout.read().decode('utf-8'))
    
    print("All tasks completed.")
    ssh.close()

if __name__ == "__main__":
    deploy_and_restart()
