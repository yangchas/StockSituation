import paramiko

host = '115.190.156.240'
port = 22
user = 'root'
password = 'Chao123+'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, user, password)
    sftp = ssh.open_sftp()
    sftp.put('market_edge_engine.py', '/root/work/web/market_edge_engine.py')
    print('market_edge_engine.py synced successfully.')
    sftp.close()
except Exception as e:
    print(f'Error: {e}')
finally:
    ssh.close()
