import paramiko

def final_redis_audit():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        print(f"--- 📡 Redis 物理存证探测 (paramiko) ---")
        
        date_sh = "20260331"
        cmds = [
            f"redis-cli type market:auction:{date_sh}:latest",
            f"redis-cli hkeys market:auction:{date_sh}:latest | head -n 5",
            f"redis-cli dbsize"
        ]
        
        for cmd in cmds:
            print(f"\n[Remote CMD] {cmd}")
            stdin, stdout, stderr = ssh.exec_command(cmd)
            print(stdout.read().decode())
            
    except Exception as e:
        print(f"❌ Audit Error: {e}")
    finally:
        ssh.close()

final_redis_audit()
