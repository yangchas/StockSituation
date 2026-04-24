import paramiko

host = '115.190.156.240'
port = 22
user = 'root'
password = 'Chao123+'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, user, password)
    stdin, stdout, stderr = ssh.exec_command('ps -ef | grep python')
    print("--- PS OUTPUT ---")
    print(stdout.read().decode('utf-8'))
    
    stdin, stdout, stderr = ssh.exec_command('cat /root/work/web/market_edge_engine.py | grep "def calculate_sentiment" -A 30 | grep "phase ="')
    print("--- CODE CHECK ---")
    print(stdout.read().decode('utf-8'))
    
finally:
    ssh.close()
