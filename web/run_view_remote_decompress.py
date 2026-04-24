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
        # We will just cat the lines from redis_storage.py that define compress/decompress
        cmd = "cd /root/work/web && grep -A 20 'def _decompress_data' redis_storage.py"
        stdin, stdout, stderr = ssh.exec_command(cmd)
        
        out = stdout.read().decode().strip()
        if out: print(f"Output:\n{out}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    run()
