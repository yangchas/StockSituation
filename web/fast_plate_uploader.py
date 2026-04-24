import redis
import json
import pandas as pd

def main():
    r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    
    # 1. Load Plate Names
    try:
        plates_df = pd.read_csv('/root/work/web/data/板块.csv', encoding='utf-8')
    except:
        plates_df = pd.read_csv('/root/work/web/data/板块.csv', encoding='gb18030')
        
    plate_id_to_name = {}
    for _, row in plates_df.iterrows():
        pid = str(row['id']).strip()
        pname = str(row['name']).strip()
        plate_id_to_name[pid] = pname

    # 2. Load Stock to Plate IDs
    try:
        stocks_df = pd.read_csv('/root/work/web/data/个股板块.csv', encoding='utf-8')
    except:
        stocks_df = pd.read_csv('/root/work/web/data/个股板块.csv', encoding='gb18030')
        
    stock_to_plates = {}
    for _, row in stocks_df.iterrows():
        pid = str(row['plateid']).strip()
        sid = str(row['stockid']).strip()
        if sid.endswith('.0'):
            sid = sid[:-2]
        sid = sid.zfill(6)
        
        pname = plate_id_to_name.get(pid)
        if pname:
            if sid not in stock_to_plates:
                stock_to_plates[sid] = []
            if pname not in stock_to_plates[sid]:
                stock_to_plates[sid].append(pname)

    # 3. Read existing from Redis to merge
    s2p_key = "config:plate_mapping:s2p"
    existing = r.hgetall(s2p_key)
    if existing:
        for k, v in existing.items():
            try:
                plates = json.loads(v)
                if k not in stock_to_plates:
                    stock_to_plates[k] = plates
                else:
                    for p in plates:
                        if p not in stock_to_plates[k]:
                            stock_to_plates[k].append(p)
            except:
                pass
                
    # 4. Save to Redis
    p = r.pipeline()
    for k, v in stock_to_plates.items():
        p.hset(s2p_key, k, json.dumps(v, ensure_ascii=False))
    p.execute()
    print(f"Successfully uploaded full market plate mappings to Redis config:plate_mapping:s2p. Total mapped: {len(stock_to_plates)}")

if __name__ == '__main__':
    main()
