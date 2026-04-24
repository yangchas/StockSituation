import paramiko

def diagnose_and_force_kill():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        print(f"--- 🩺 正在深度诊断服务器逻辑执行情况 ---")
        
        # 1. 核验代码内容
        cmd_check = "grep -n 'auction_synced_date' /root/work/engine_v2/v2_orc_final.py"
        stdin, stdout, stderr = ssh.exec_command(cmd_check)
        output = stdout.read().decode()
        if output:
            print("✅ 远程代码核验：状态锁逻辑已存在于磁盘。")
            print(output)
        else:
            print("❌ 远程代码核验：未发现状态锁逻辑！请确认 SFTP 是否同步成功。")
            
        # 2. 检查多余进程
        cmd_ps = "ps -ef | grep v2_orc_final.py | grep -v grep"
        stdin, stdout, stderr = ssh.exec_command(cmd_ps)
        procs = stdout.read().decode().strip().split('\n')
        if len(procs) > 1:
            print(f"⚠️ 发现 {len(procs)} 个活跃进程！存在僵尸进程干扰的可能性极大。")
            for p in procs: print(f"  -> {p}")
            # 暴力清理
            print("🚀 正在执行全量进程净化...")
            ssh.exec_command("pkill -9 -f v2_orc_final.py")
        elif procs and procs[0]:
            print(f"✅ 仅发现一个活跃进程: {procs[0]}")
        else:
            print("🌑 未发现运行中的进程。")
            
    except Exception as e:
        print(f"❌ Diagnostic Error: {e}")
    finally:
        ssh.close()

diagnose_and_force_kill()
