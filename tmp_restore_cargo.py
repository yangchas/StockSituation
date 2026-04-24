import paramiko

def restore_cargo_toml():
    host = "115.190.156.240"
    port = 22
    user = "root"
    pw = "Chao123+"
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    try:
        ssh.connect(host, port, user, pw)
        print("--- 🛰️ 正在从服务器拉取 Cargo.toml ---")
        
        # 尝试不同路径，确保万无一失
        potential_paths = [
            "/root/work/engine_v2/Cargo.toml",
            "/root/work/market_edge_v2_core/Cargo.toml"
        ]
        
        content = ""
        for p in potential_paths:
            print(f"  Checking path: {p}")
            stdin, stdout, stderr = ssh.exec_command(f"cat {p}")
            res = stdout.read().decode()
            if res:
                content = res
                print(f"✅ Found Cargo.toml at {p}")
                break
        
        if content:
            local_path = r"d:\work\Go\engine_v2\Cargo.toml"
            with open(local_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"🎉 本地 Cargo.toml 重建成功: {local_path}")
        else:
            print("❌ 未能在服务器找到有效的 Cargo.toml")
            
    except Exception as e:
        print(f"❌ Restore Error: {e}")
    finally:
        ssh.close()

restore_cargo_toml()
