import paramiko
import json

def fetch_remote_data():
    host = '115.190.156.240'
    port = 22
    user = 'root'
    password = 'Chao123+'

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(host, port, user, password)
        
        # 1. List potential auction keys
        command = 'redis-cli keys "market:auction:20260323*"'
        stdin, stdout, stderr = ssh.exec_command(command)
        keys = stdout.read().decode('utf-8').splitlines()
        print(f"Found {len(keys)} auction keys on server for 2026-03-23")
        
        if not keys:
            # Try dashed format
            command = 'redis-cli keys "market:auction:2026-03-23*"'
            stdin, stdout, stderr = ssh.exec_command(command)
            keys = stdout.read().decode('utf-8').splitlines()
            print(f"Found {len(keys)} dashed auction keys on server")

        if keys:
            # Get sample data from the first key
            sample_key = keys[0]
            stdin, stdout, stderr = ssh.exec_command(f'redis-cli type {sample_key}')
            ktype = stdout.read().decode('utf-8').strip()
            print(f"Key: {sample_key}, Type: {ktype}")
            
            if ktype == 'hash':
                stdin, stdout, stderr = ssh.exec_command(f'redis-cli hkeys {sample_key}')
                fields = stdout.read().decode('utf-8').splitlines()
                print(f"Hash keys (top 10): {fields[:10]}")
                
                # Check for top_amount specifically
                if "top_amount" in fields:
                    print("✅ Found 'top_amount' field!")
                else:
                    print("❌ 'top_amount' field NOT found among keys.")
            
            # ... rest of the fetch logic

            
        # 2. Check if auction_summary exists
        command = 'redis-cli exists "market:auction_summary:2026-03-23"'
        stdin, stdout, stderr = ssh.exec_command(command)
        summary_exists = stdout.read().decode('utf-8').strip()
        print(f"market:auction_summary:2026-03-23 exists: {summary_exists}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        ssh.close()

if __name__ == "__main__":
    fetch_remote_data()
