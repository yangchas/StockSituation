import paramiko
import json
import ast

def fetch_auction_data():
    host = '115.190.156.240'
    port = 22
    user = 'root'
    password = 'Chao123+'
    
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        print("Connecting to server...")
        ssh.connect(host, port, user, password)
        print("Connected!")
        
        # Redis command to check keys
        print("Getting yesterday's (2026-02-25) auction snapshot latest tag...")
        stdin, stdout, stderr = ssh.exec_command('redis-cli -n 0 hget market:auction:20260225:latest tag')
        tag = stdout.read().decode().strip()
        print(f"Latest tag: {tag}")
        if not tag:
            tag = '0925'
        
        snap_key = f'market:auction:20260225:{tag}'
        print(f"Fetching from {snap_key}...")
        stdin, stdout, stderr = ssh.exec_command(f'redis-cli -n 0 hget {snap_key} top_amount')
        raw = stdout.read().decode().strip()
        
        if not raw:
            print("No top_amount found. Checking string fallback...")
            stdin, stdout, stderr = ssh.exec_command('redis-cli -n 0 get market:auction:20260225:0925')
            raw = stdout.read().decode().strip()
            
        if not raw:
            print("No data found in HASH or STRING format.")
            return

        try:
            # Parse json
            # redis-cli might add quotes or we can parse it as json / ast.literal_eval depending on format
            # Let's write the raw string to a local file
            import codecs
            with codecs.open('auction_20260225.json', 'w', 'utf-8') as f:
                f.write(raw)
            print(f"Successfully saved {len(raw)} bytes to auction_20260225.json")
            
        except Exception as e:
            print(f"Error parsing data: {e}")
            
    except Exception as e:
        print(f"SSH Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    fetch_auction_data()
