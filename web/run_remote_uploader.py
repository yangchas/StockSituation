import paramiko

def run():
    host = '115.190.156.240'
    port = 22
    user = 'root'
    password = 'Chao123+'
    script_name = 'fast_plate_uploader.py'
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        print("Connecting to server...")
        ssh.connect(host, port, user, password)
        print("Connected! Uploading script...")
        
        sftp = ssh.open_sftp()
        sftp.put(script_name, f'/root/work/web/{script_name}')
        sftp.close()
        
        print("Executing script on server...")
        # Since python 3.9 might throw deprecation warnings, filter them
        stdin, stdout, stderr = ssh.exec_command(f'cd /root/work/web && python -W ignore {script_name}')
        
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        exit_status = stdout.channel.recv_exit_status()
        
        if out:
            print(f"Output:\n{out}")
        if err:
            print(f"Error:\n{err}")
        print(f"Exit code: {exit_status}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    run()
