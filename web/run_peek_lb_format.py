import paramiko

def run():
    host = '115.190.156.240'
    port = 22
    user = 'root'
    password = 'Chao123+'
    script_name = 'peek_lb_format.py'
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, port, user, password)
        sftp = ssh.open_sftp()
        sftp.put(script_name, f'/root/work/web/{script_name}')
        
        stdin, stdout, stderr = ssh.exec_command(f'cd /root/work/web && python -W ignore {script_name}')
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        if out: print(f"Output:\n{out}")
        if err: print(f"Error:\n{err}")
        sftp.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    run()
