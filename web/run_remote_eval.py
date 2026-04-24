import paramiko
import os
import time

def deploy_and_eval():
    host = '115.190.156.240'
    port = 22
    user = 'root'
    password = 'Chao123+'
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        print("Connecting to server...")
        ssh.connect(host, port, user, password)
        print("Connected! Uploading script...")
        
        sftp = ssh.open_sftp()
        local_script = 'd:/work/Go/web/remote_plate_eval.py'
        remote_script = '/tmp/remote_plate_eval.py'
        sftp.put(local_script, remote_script)
        
        print("Executing script on server... (may take a moment to query redis)")
        cmd = f'python3 {remote_script}'
        stdin, stdout, stderr = ssh.exec_command(cmd)
        
        # Wait for command
        exit_status = stdout.channel.recv_exit_status()
        out = stdout.read().decode('utf-8', errors='replace')
        err = stderr.read().decode('utf-8', errors='replace')
        
        if exit_status != 0:
            print(f"Error executing remote script (exited {exit_status}):")
            print(out)
            print("STDERR:")
            print(err)
            return
        else:
            print("Script executed successfully.")
            if out: print("STDOUT:", out)
        
        print("Downloading report...")
        remote_report = '/root/remote_report.md' # the script runs in ~ or depending on CWD
        # Actually paramiko exec runs in home so it's probably /root/remote_report.md
        # Let's just download it
        sftp.get('/root/remote_report.md', 'd:/work/Go/web/final_expectation_report.md')
        print("Report downloaded to d:/work/Go/web/final_expectation_report.md")
        
        sftp.close()
    except Exception as e:
        print(f"SSH/SFTP Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    deploy_and_eval()
