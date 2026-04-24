import paramiko
import json

def remote_redis_probe():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    print(f"--- 🚀 正在 SSH 登录服务器 {host} ---")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        print("✅ SSH 连接成功")
        
        # 探测命令集
        commands = [
            "redis-cli keys '*auction:20260331*'",
            "redis-cli type market:auction:20260331:latest",
            "redis-cli hkeys market:auction:20260331:latest",
            "redis-cli hget market:auction:20260331:latest top_amount | head -c 200",
            "redis-cli hget market:auction:20260331:0925 top_amount | head -c 200"
        ]
        
        for cmd in commands:
            print(f"\n[Remote CMD] {cmd}")
            stdin, stdout, stderr = ssh.exec_command(cmd)
            out = stdout.read().decode().strip()
            err = stderr.read().decode().strip()
            
            if out: print(f"Result:\n{out}")
            if err: print(f"Error:\n{err}")
            
    except Exception as e:
        print(f"❌ SSH Probe Error: {e}")
    finally:
        ssh.close()

remote_redis_probe()
