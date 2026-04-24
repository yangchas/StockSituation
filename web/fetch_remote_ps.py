import paramiko

host = '115.190.156.240'
port = 22
user = 'root'
password = 'Chao123+'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, user, password)
    stdin, stdout, stderr = ssh.exec_command('ps -eo pid,lstart,cmd | grep "[p]ython"')
    out = stdout.read().decode('utf-8').strip()
    
    print("=== REMOTE PYTHON PROCESSES ===")
    for line in out.splitlines():
        print(line)
        
finally:
    ssh.close()
