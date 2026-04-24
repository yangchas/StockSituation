import paramiko

def final_config_audit():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        print(f"--- 🔍 配置文件物理审计 ---")
        
        # 精确获取库名称和目录结构
        cmds = [
            "cat /root/work/market_edge_v2_core/Cargo.toml | grep -E 'name|crate-type'",
            "ls -R /root/work/market_edge_v2_core/src",
            "ls -F /root/work/market_edge_v2_core/ | grep .py"
        ]
        
        for cmd in cmds:
            print(f"\n[Remote CMD] {cmd}")
            stdin, stdout, stderr = ssh.exec_command(cmd)
            print(stdout.read().decode())
            
    except Exception as e:
        print(f"❌ Audit Error: {e}")
    finally:
        ssh.close()

final_config_audit()
