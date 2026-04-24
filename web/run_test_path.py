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
        sftp = ssh.open_sftp()
        
        test_script = """
import sys
import os
sys.path.insert(0, '/root/work/web')
import market_edge_engine
print("Module path:", market_edge_engine.__file__)
import inspect
print("Init signature:")
print(inspect.signature(market_edge_engine.MarketEdgeEngine.__init__))
"""
        sftp.open('/root/work/web/test_path.py', 'w').write(test_script)
        
        stdin, stdout, stderr = ssh.exec_command('cd /root/work/web && python -W ignore test_path.py')
        
        out = stdout.read().decode('utf-8')
        print(out)
        
        sftp.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    run()
