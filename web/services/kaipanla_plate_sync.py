import sys
import os
import json
import time
import logging
from datetime import datetime
import redis

# Resolve paths dynamically
current_dir = os.path.dirname(os.path.abspath(__file__))
web_dir = os.path.dirname(current_dir)
project_root = os.path.dirname(web_dir)

if project_root not in sys.path:
    sys.path.append(project_root)
if os.path.join(project_root, 'ai') not in sys.path:
    sys.path.append(os.path.join(project_root, 'ai'))

try:
    # Try multiple import paths for StockAnalyzer
    try:
        from ai.API.StockAnalyzer import StockAnalyzer
    except ImportError:
        try:
            from API.StockAnalyzer import StockAnalyzer
        except ImportError:
            from StockAnalyzer import StockAnalyzer
except ImportError:
    StockAnalyzer = None
    logger.warning("StockAnalyzer could not be imported in KaipanlaPlateSync")

from web.trade_calendar import TradeCalendar

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KaipanlaPlateSync:
    """
    盘前/盘后独立执行的开盘啦板块同步服务。
    负责拉取近期游资炒作概念并全量刷新至 Redis。
    """
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        self.analyzer = StockAnalyzer()
        self.calendar = TradeCalendar()
        
    def sync_recent_plates(self, days: int = 1):
        logger.info(f"Starting Kaipanla API event-driven plate sync for the last {days} trading days...")
        
        # 获取最近 N 个交易日
        dates_to_fetch = []
        date_str = datetime.now().strftime('%Y-%m-%d')
        # 如果是交易日，且已经正式开盘 (如 09:30 后)，才拉取当日数据
        # 否则启动时拉取历史天数 (days) 不包含尚未产生的今日数据
        current_time_str = datetime.now().strftime('%H:%M')
        if self.calendar.is_trade_day(date_str) and current_time_str >= "09:32":
            dates_to_fetch.append(date_str)
            
        current = date_str
        while len(dates_to_fetch) < days:
            current = self.calendar.get_previous_trade_day(current)
            if not current:
                break
            dates_to_fetch.append(current)
                
        # 增量读取 Redis 历史数据：构成持久化"记忆池"
        s2p_key = "config:plate_mapping:s2p"
        info_key = "config:plate_mapping:info"
        
        stock_to_plates = {}
        plate_info = {}
        try:
            old_s2p = self.redis_client.hgetall(s2p_key)
            if old_s2p:
                for k, v in old_s2p.items():
                    try:
                        stock_to_plates[k] = json.loads(v)
                    except: pass
            
            old_info = self.redis_client.hgetall(info_key)
            if old_info:
                for k, v in old_info.items():
                    try:
                        plate_info[k] = json.loads(v)
                    except: pass
        except Exception as e:
            logger.warning(f"Failed to load old mappings from Redis: {e}")
        
        for d in dates_to_fetch:
            logger.info(f"Fetching limit-up events for {d}...")
            kpl_date = d.replace('-', '')
            today_kpl = datetime.now().strftime('%Y%m%d')
            
            if kpl_date == today_kpl:
                res = self.analyzer.get_bans()
            else:
                res = self.analyzer.get_his_bans(kpl_date)
                
            list_data = []
            if res and isinstance(res, dict):
                list_data = res.get('info') or res.get('List') or res.get('list') or []
                
            if not list_data:
                # 识别当前是否为交易日的早盘时间
                is_today = (kpl_date == datetime.now().strftime('%Y%m%d'))
                is_early = (datetime.now().strftime('%H:%M') < "09:32")
                
                msg = f"No limit-up data found for {kpl_date}. Response: {type(res)}"
                if is_today and is_early:
                    logger.info(f"☕ {msg} (Waiting for market session to populate data)")
                else:
                    res_keys = list(res.keys()) if isinstance(res, dict) else "N/A"
                    logger.warning(f"⚠️ {msg}. Response keys: {res_keys}")
                continue
                
            flat_rows = []
            for item in list_data:
                if isinstance(item, list) and len(item) > 0 and isinstance(item[0], list):
                    flat_rows.extend(item)
                else:
                    flat_rows.append(item)
                    
            for row in flat_rows:
                try:
                    code, name, main_concept, sub_concepts_str = "", "", "", ""
                    
                    if isinstance(row, list) and len(row) > 12:
                        code = str(row[0])
                        name = str(row[1])
                        main_concept = str(row[5])
                        sub_concepts_str = str(row[12])
                    elif isinstance(row, dict):
                        code = str(row.get('StockID', row.get('code', '')))
                        name = str(row.get('StockName', row.get('name', '')))
                        main_concept = str(row.get('PlateName', row.get('plate_name', '')))
                        sub_concepts_str = str(row.get('Reason', ''))
                        
                    if not code:
                        continue
                        
                    if len(code) > 6:
                        code = code[-6:]
                        
                    plates = []
                    if main_concept:
                        plates.append(main_concept)
                    if sub_concepts_str:
                        for sep in ['、', ',', '，']:
                            if sep in sub_concepts_str:
                                subs = [s.strip() for s in sub_concepts_str.split(sep) if s.strip()]
                                plates.extend(subs)
                                break
                        else:
                            plates.append(sub_concepts_str.strip())
                            
                    # 原生 ZSCode API 深度溯源事件概念层级！
                    try:
                        reason_res = self.analyzer.get_ban_reasons(code)
                        time.sleep(0.5) # Strictly avoid Kaipanla API limit during full sync
                        if reason_res and isinstance(reason_res, dict):
                            reason_list = reason_res.get('List', [])
                            for reason_item in reason_list:
                                reason_text = reason_item.get('Reason', '')
                                if '；' in reason_text:
                                    # Reason: 'AI视频+字节概念；1. AI视频... '
                                    concepts_part = reason_text.split('；')[0]
                                    deep_concepts = concepts_part.split('+')
                                    for dc in deep_concepts:
                                        dc = dc.strip()
                                        if dc and dc not in plates:
                                            plates.append(dc)
                    except Exception as e:
                        logger.warning(f"Failed to fetch detailed API event reasons for {code}: {e}")
                        
                    unique_plates = []
                    for p in plates:
                        if p == "概念": # 纯"概念"二字无意义，但我们要保留"字节概念"等
                            continue
                        if p and p not in unique_plates:
                            unique_plates.append(p)
                            
                    if not unique_plates:
                        continue
                        
                    for p in unique_plates:
                        if p not in plate_info:
                            plate_info[p] = {"name": p, "type": "main" if p == main_concept else "sub"}
                            
                    if code not in stock_to_plates:
                        stock_to_plates[code] = unique_plates
                    else:
                        existing = stock_to_plates[code]
                        for p in unique_plates:
                            if p not in existing:
                                existing.append(p)
                except Exception as e:
                    logger.error(f"Row parsing error on {d}: {e} | Row Data: {row}")
            
            time.sleep(1.5) # Daily loop limit protection
            
        logger.info(f"Cumulative memory: {len(stock_to_plates)} stocks with event-driven native layer mappings.")
        
        p = self.redis_client.pipeline()
        
        if stock_to_plates:
            s2p_dict = {k: json.dumps(v, ensure_ascii=False) for k, v in stock_to_plates.items()}
            for k, v in s2p_dict.items():
                p.hset(s2p_key, k, v)
                
        if plate_info:
            info_dict = {k: json.dumps(v, ensure_ascii=False) for k, v in plate_info.items()}
            for k, v in info_dict.items():
                p.hset(info_key, k, v)
                
        p.execute()
        logger.info("Successfully pushed incremental event mappings to Redis memory!")

    def sync_all_stocks_ban_reasons(self):
        import csv
        logger.info("Starting Kaipanla API full sync across all 5000+ individual stocks...")
        
        stocks_file = os.path.join(project_root, 'web', 'data', '个股板块.csv')
        stocks_set = set()
        try:
            with open(stocks_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)
                for row in reader:
                    if len(row) > 1:
                        code = str(row[1]).strip().zfill(6)
                        if code.startswith(('00', '30', '60', '68')) and len(code) == 6:
                            stocks_set.add(code)
        except Exception as e:
            logger.error(f"Failed to read valid stock list: {e}")
            return
            
        all_stocks = list(stocks_set)
        logger.info(f"Loaded {len(all_stocks)} valid A-share stocks for processing.")
        
        s2p_key = "config:plate_mapping:s2p"
        info_key = "config:plate_mapping:info"
        
        stock_to_plates = {}
        plate_info = {}
        try:
            old_s2p = self.redis_client.hgetall(s2p_key)
            if old_s2p:
                for k, v in old_s2p.items():
                    try: stock_to_plates[k] = json.loads(v)
                    except: pass
            
            old_info = self.redis_client.hgetall(info_key)
            if old_info:
                for k, v in old_info.items():
                    try: plate_info[k] = json.loads(v)
                    except: pass
        except Exception as e:
            logger.warning(f"Failed to load old mappings from Redis: {e}")
            
        count = 0
        skipped = 0
        p = self.redis_client.pipeline()
        for code in all_stocks:
            # 增加跳过逻辑：如果已经有概念映射，则跳过全量请求，支持断点续传
            if code in stock_to_plates and stock_to_plates[code]:
                skipped += 1
                continue

            count += 1
            if count % 100 == 0:
                logger.info(f"Processed {count}/{len(all_stocks)} stocks (skipped {skipped})...")
                p.execute()
                p = self.redis_client.pipeline()
                
            try:
                # 获取个股具体的涨停原因和题材（包含隐藏概念）
                reason_res = self.analyzer.get_ban_reasons(code)
                time.sleep(0.3) # Strictly avoid Kaipanla API limit per stock
                if reason_res and isinstance(reason_res, dict):
                    reason_list = reason_res.get('List', [])
                    if not reason_list:
                        continue
                        
                    plates = []
                    for reason_item in reason_list:
                        reason_text = reason_item.get('Reason', '')
                        if '；' in reason_text:
                            concepts_part = reason_text.split('；')[0]
                            deep_concepts = concepts_part.split('+')
                            for dc in deep_concepts:
                                dc = dc.strip()
                                if dc and dc != "概念" and dc not in plates:
                                    plates.append(dc)
                                    if dc not in plate_info:
                                        plate_info[dc] = {"name": dc, "type": "sub"}
                                        p.hset(info_key, dc, json.dumps(plate_info[dc], ensure_ascii=False))
                                        
                    if plates:
                        if code not in stock_to_plates:
                            stock_to_plates[code] = plates
                        else:
                            existing = stock_to_plates[code]
                            for dc in plates:
                                if dc not in existing:
                                    existing.append(dc)
                        p.hset(s2p_key, code, json.dumps(stock_to_plates[code], ensure_ascii=False))
                        # logger.info(f"(FullSync) Cached deep concepts for {code}: {plates}")
            except Exception as e:
                logger.error(f"Failed to process stock {code}: {e}")
                
        p.execute()
        
        # 写入全量同步完成的状态标志位
        sync_info = {
            "status": "completed",
            "last_full_sync": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_count": len(all_stocks),
            "valid_mappings": len(stock_to_plates)
        }
        self.redis_client.set("config:plate_mapping:full_sync_info", json.dumps(sync_info, ensure_ascii=False))
        logger.info(f"Successfully processed all {len(all_stocks)} stocks (processed {count}, skipped {skipped}) and updated Redis.")

if __name__ == "__main__":
    sync = KaipanlaPlateSync()
    sync.sync_all_stocks_ban_reasons()
    sync.sync_recent_plates(days=1)
