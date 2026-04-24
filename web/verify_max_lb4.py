import sys
sys.path.append('/root/work/web')
from redis_storage import RedisStorageManager

def main():
    rsm = RedisStorageManager()
    
    # Notice we don't prepend 'cache:' because get_data already does it.
    # But limit_up_{date} keys might not use the cache prefix if it's set natively, let's see.
    # limit_up_storage.py did: self.redis_storage.store_data("limit_up_2026-02-24", all_limit_up, 86400)
    # the cache prefix is likely "cache:" so the final key is "cache:limit_up_2026-02-24"
    
    lb_list = rsm.get_data("limit_up_2026-02-24")
    if not lb_list:
        print("native get_data returned None.")
        # try without prefix?
        return
        
    print(f"Successfully loaded list of {len(lb_list)} items.")
    
    parsed_list = []
    for item in lb_list:
        if not isinstance(item, dict): continue
        lb = item.get("连板天数", item.get("lb_days", 0))
        try: lb = int(lb)
        except: lb = 1
        name = item.get("股票简称", item.get("name", "Unknown"))
        code = str(item.get("股票代码", item.get("code", "000000")))[:6]
        parsed_list.append((lb, code, name))
        
    parsed_list.sort(key=lambda x: x[0], reverse=True)
    
    print("=== Top Limit-Up Stocks for 2026-02-24 ===")
    for lb, code, name in parsed_list[:15]:
        print(f"[{lb} 连板] {code} {name}")
        
if __name__ == '__main__':
    main()
