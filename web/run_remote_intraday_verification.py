import paramiko
import os

def run():
    host = '115.190.156.240'
    port = 22
    user = 'root'
    password = 'Chao123+'
    
    script_name = 'remote_intraday_verification.py'
    report_name = 'intraday_verification_report.md'
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        print("Connecting to server...")
        ssh.connect(host, port, user, password)
        print("Connected! Uploading Phase 2 script...")
        
        sftp = ssh.open_sftp()
        sftp.put(script_name, f'/root/work/web/{script_name}')
        
        print("Executing Phase 2 script on server...")
        stdin, stdout, stderr = ssh.exec_command(f'cd /root/work/web && python -W ignore {script_name}')
        
        out = stdout.read().decode().strip()
        err = stderr.read().decode().strip()
        exit_status = stdout.channel.recv_exit_status()
        
        if out: print(f"Output:\n{out}")
        if err: print(f"Error:\n{err}")
        print(f"Exit code: {exit_status}")
            
        if exit_status == 0:
            print(f"Downloading {report_name}...")
            sftp.get(f'/root/work/web/{report_name}', report_name)
            print(f"Successfully downloaded {report_name}")
            
        sftp.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    run()
