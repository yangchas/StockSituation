#!/usr/bin/env python3
"""
Redis 数据完整性诊断 + 竞价数据预期差分析
2026-02-27 交易日数据检查
"""
import redis
import json
import time
import sys
import os
from datetime import datetime
from collections import defaultdict

# 连接 Redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# 处理编码
if sys.stdout.encoding != 'utf-8':
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass

TODAY = "2026-02-27" # 根据用户之前的日志，这应该是最近的交易日
DATE_COMPACT = TODAY.replace("-", "")

print("=" * 80)
print(f"Redis Data Diagnosis - Date: {TODAY}")
print(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# =====================================================
# 1. Redis Connectivity & Base Info
# =====================================================
print("\n" + "─" * 60)
print("1. Redis Connectivity & Base Info")
print("-" * 60)
try:
    info = r.info("memory")
    print(f"  OK Redis connected")
    print(f"     used_memory_human: {info.get('used_memory_human', 'N/A')}")
    print(f"     used_memory_peak_human: {info.get('used_memory_peak_human', 'N/A')}")
    db_info = r.info("keyspace")
    for db_name, db_data in db_info.items():
        print(f"     {db_name}: keys={db_data.get('keys',0)}, expires={db_data.get('expires',0)}")
except Exception as e:
    print(f"  ❌ Redis 连接失败: {e}")
    sys.exit(1)

# =====================================================
# 2. Check stock:quote:* real-time data
print("\n" + "-" * 60)
print("2. Stock Real-time Data (stock:quote:*)")
print("-" * 60)

stock_keys = list(r.scan_iter(match="stock:quote:*", count=10000))
print(f"  Total keys: {len(stock_keys)}")

if stock_keys:
    # 抽样检查
    sample_keys = stock_keys[:5]
    valid_count = 0
    empty_name_count = 0
    stale_count = 0
    now_ts = int(time.time())
    
    all_stocks_data = {}
    for key in stock_keys:
        try:
            data = r.hgetall(key)
            code = key.replace("stock:quote:", "")
            if data:
                all_stocks_data[code] = data
                ts = int(data.get("timestamp", data.get("ts", 0)))
                name = data.get("name", "")
                if name:
                    valid_count += 1
                else:
                    empty_name_count += 1
                if ts > 0 and now_ts - ts > 3600:
                    stale_count += 1
        except:
            pass
    
    print(f"  Stocks with name: {valid_count}")
    print(f"  Stocks missing name: {empty_name_count}")
    print(f"  Stale data (>1h): {stale_count}")
    
    # Samples
    for key in sample_keys:
        data = r.hgetall(key)
        code = key.replace("stock:quote:", "")
        name = data.get("name", "?")
        price = data.get("price", data.get("cur_price", "?"))
        change_pct = data.get("change_pct", data.get("change", "?"))
        ts = data.get("timestamp", data.get("ts", "?"))
        print(f"    - {code} ({name}): price={price}, change={change_pct}%, ts={ts}")

# =====================================================
# 3. Check MarketEdgeEngine Core Keys
# =====================================================
print("\n" + "-" * 60)
print("3. MarketEdgeEngine Core Keys")
print("-" * 60)

engine_keys = [
    f"market:sentiment:{TODAY}",
    f"market:fear_greed:{TODAY}",
    f"market:herding:{TODAY}",
    f"market:resonance:{TODAY}",
    f"market:overview:{TODAY}",
    f"market:process_profile:{TODAY}",
    f"market:strategy_tags:{TODAY}",
    f"market:ab_arbitrage:{TODAY}",
    f"market:comfort_exit:{TODAY}",
    f"market:execution_policy:{TODAY}",
    f"market:operator_advice:{TODAY}",
    f"market:open_scenario:{TODAY}",
    f"market:plan:preopen:{TODAY}",
    f"market:plan:open_verify:{TODAY}",
    f"diag:expectation_eval:{TODAY}",
    f"diag:auction_source:{TODAY}",
]

for key in engine_keys:
    try:
        key_type = r.type(key)
        if key_type == "hash":
            data = r.hgetall(key)
            ts = data.get("ts", "?")
            fields = list(data.keys())
            if len(fields) > 6:
                fields_str = ", ".join(fields[:6]) + f"... (+{len(fields)-6})"
            else:
                fields_str = ", ".join(fields)
            print(f"  OK {key} | fields={len(data)} | ts={ts}")
            print(f"      keys: [{fields_str}]")
        elif key_type == "string":
            val = r.get(key)
            print(f"  OK {key} | type=string | len={len(val) if val else 0}")
        elif key_type == "none":
            print(f"  ERR {key} | Non-existent")
        else:
            print(f"  WARN {key} | type={key_type}")
    except Exception as e:
        print(f"  ERR {key} | Error: {e}")

# 4. Auction Data Check
print("\n" + "-" * 60)
print("4. Auction Data (market:auction:*)")
print("-" * 60)

auction_keys_to_check = [
    f"market:auction:{DATE_COMPACT}:0925",
    f"market:auction:{DATE_COMPACT}:0924",
    f"market:auction:{DATE_COMPACT}:0920",
    f"market:auction:{DATE_COMPACT}:latest",
    f"market:auction:{TODAY}:0925",
]

auction_items = None
auction_source = None

for key in auction_keys_to_check:
    try:
        key_type = r.type(key)
        if key_type == "hash":
            data = r.hgetall(key)
            fields = list(data.keys())
            print(f"  ✅ {key} | type=hash | fields={fields}")
            top_amount_raw = data.get("top_amount", "")
            if top_amount_raw:
                try:
                    items = json.loads(top_amount_raw)
                    print(f"      top_amount: {len(items)} 条记录")
                    if auction_items is None:
                        auction_items = items
                        auction_source = key
                except:
                    print(f"      top_amount: 解析失败")
        elif key_type == "string":
            val = r.get(key)
            try:
                items = json.loads(val)
                if isinstance(items, list):
                    print(f"  ✅ {key} | type=string | {len(items)} 条记录")
                    if auction_items is None:
                        auction_items = items
                        auction_source = key
                else:
                    print(f"  ✅ {key} | type=string | len={len(val)}")
            except:
                print(f"  ✅ {key} | type=string | len={len(val) if val else 0}")
        elif key_type == "none":
            print(f"  ❌ {key} | 不存在")
        else:
            print(f"  ⚠️  {key} | type={key_type}")
    except Exception as e:
        print(f"  ❌ {key} | 错误: {e}")

# Search for all auction keys
all_auction_keys = list(r.scan_iter(match="market:auction:*", count=1000))
if all_auction_keys:
    print(f"\n  Search all auction keys ({len(all_auction_keys)}):")
    for k in sorted(all_auction_keys):
        kt = r.type(k)
        if kt == "hash":
            d = r.hgetall(k)
            print(f"     {k} [hash] fields={list(d.keys())[:5]}")
        elif kt == "string":
            v = r.get(k)
            print(f"     {k} [string] len={len(v) if v else 0}")
        else:
            print(f"     {k} [{kt}]")

# =====================================================
# 5. 竞价数据预期差分析 (分板块)
# =====================================================
print("\n" + "=" * 80)
print("5️⃣  竞价预期差分析 (Expectation Gap Analysis)")
print("=" * 80)

if auction_items is None:
    print("  ⚠️  没有找到竞价数据，无法做预期差分析")
else:
    print(f"  数据来源: {auction_source}")
    print(f"  竞价记录数: {len(auction_items)}")
    
    # 加载板块映射
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    try:
        import csv
        script_dir = os.path.dirname(os.path.abspath(__file__))
        
        # 加载个股-板块关系
        stock_to_plates = defaultdict(list)
        plate_names = {}
        
        # 板块数据
        plate_path = os.path.join(script_dir, 'data/板块.csv')
        if os.path.exists(plate_path):
            with open(plate_path, 'r', encoding='gbk') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    plate_names[row['id']] = row['name']
        
        # 个股板块关系
        relation_path = os.path.join(script_dir, 'data/个股板块.csv')
        if os.path.exists(relation_path):
            with open(relation_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # skip header
                for row in reader:
                    if len(row) >= 2:
                        plate_id, stock_id = row[0], row[1]
                        if plate_id in plate_names:
                            stock_to_plates[stock_id].append(plate_id)
        
        print(f"  加载板块关系: {len(plate_names)} 个板块, {len(stock_to_plates)} 只股票")
    except Exception as e:
        print(f"  ⚠️  加载板块数据失败: {e}")
        stock_to_plates = {}
        plate_names = {}
    
    # 解析竞价数据
    parsed = []
    for it in auction_items:
        code = str(it.get("symbol", it.get("code", ""))).strip()
        if len(code) != 6:
            continue
        
        # 竞价涨幅
        auc_change = float(it.get("change_pct", it.get("auc", 0.0)))
        # 标准化: 如果是小数形式 (0.05) 而不是百分比 (5.0)
        if abs(auc_change) < 0.5 and abs(auc_change) > 0:
            auc_change *= 100
        
        auc_amount = float(it.get("auction_amount_yuan", it.get("auction_amount", it.get("amount", 0.0))))
        bid_amount = float(it.get("bid_amount_yuan", it.get("bid_amount", 0.0)))
        name = it.get("name", "")
        
        # 获取当前行情
        quote = r.hgetall(f"stock:quote:{code}") or {}
        cur_change = float(quote.get("change_pct", quote.get("change", 0.0)))
        cur_price = float(quote.get("price", quote.get("cur_price", 0.0)))
        stock_name = name or quote.get("name", code)
        
        gap = cur_change - auc_change
        
        parsed.append({
            "code": code,
            "name": stock_name,
            "auc": auc_change,
            "cur": cur_change,
            "gap": gap,
            "auc_amount": auc_amount,
            "bid_amount": bid_amount,
            "cur_price": cur_price,
            "plates": stock_to_plates.get(code, []),
        })
    
    print(f"  有效解析: {len(parsed)} 条")
    
    if parsed:
        # 整体统计
        print("\n  📊 整体统计:")
        avg_auc = sum(x["auc"] for x in parsed) / len(parsed)
        avg_cur = sum(x["cur"] for x in parsed) / len(parsed)
        avg_gap = sum(x["gap"] for x in parsed) / len(parsed)
        print(f"     平均竞价涨幅: {avg_auc:+.2f}%")
        print(f"     平均盘中涨幅: {avg_cur:+.2f}%")
        print(f"     平均预期差:   {avg_gap:+.2f}%")
        
        # 区间分布
        fade_high = [x for x in parsed if x["auc"] >= 5.0 and x["gap"] <= -4.0]
        hold_high = [x for x in parsed if x["auc"] >= 5.0 and x["gap"] > -4.0]
        rise_low  = [x for x in parsed if x["auc"] <= 1.0 and x["gap"] >= 3.0]
        fade_low  = [x for x in parsed if x["auc"] <= 1.0 and x["gap"] < 3.0]
        
        print(f"\n  📈 竞价高开(≥5%)回落的(差≤-4%): {len(fade_high)} 只")
        for x in sorted(fade_high, key=lambda x: x["gap"])[:10]:
            plates_str = ", ".join([plate_names.get(p, p) for p in x["plates"][:3]])
            print(f"     {x['code']} {x['name']:<8s} 竞价:{x['auc']:+6.2f}% → 盘中:{x['cur']:+6.2f}% 差:{x['gap']:+6.2f}% | {plates_str}")
        
        print(f"\n  📈 竞价高开(≥5%)坚守的: {len(hold_high)} 只")
        for x in sorted(hold_high, key=lambda x: x["cur"], reverse=True)[:10]:
            plates_str = ", ".join([plate_names.get(p, p) for p in x["plates"][:3]])
            print(f"     {x['code']} {x['name']:<8s} 竞价:{x['auc']:+6.2f}% → 盘中:{x['cur']:+6.2f}% 差:{x['gap']:+6.2f}% | {plates_str}")
        
        print(f"\n  📉 竞价低开(≤1%)逆袭的(差≥3%): {len(rise_low)} 只")
        for x in sorted(rise_low, key=lambda x: x["gap"], reverse=True)[:10]:
            plates_str = ", ".join([plate_names.get(p, p) for p in x["plates"][:3]])
            print(f"     {x['code']} {x['name']:<8s} 竞价:{x['auc']:+6.2f}% → 盘中:{x['cur']:+6.2f}% 差:{x['gap']:+6.2f}% | {plates_str}")
        
        # 极端开盘
        extreme_low = [x for x in parsed if x["auc"] <= -5.0]
        one_word_limit = [x for x in parsed if (x["auc"] >= 9.8 if not x["code"].startswith(("300","301","688","689")) else x["auc"] >= 19.8)]
        
        print(f"\n  🔥 一字涨停(竞价≈涨停): {len(one_word_limit)} 只")
        for x in sorted(one_word_limit, key=lambda x: x["auc_amount"], reverse=True)[:10]:
            plates_str = ", ".join([plate_names.get(p, p) for p in x["plates"][:3]])
            seal_ratio = x["bid_amount"] / max(1.0, x["auc_amount"]) * 100
            print(f"     {x['code']} {x['name']:<8s} 竞价:{x['auc']:+6.2f}% 盘中:{x['cur']:+6.2f}% 封单比:{seal_ratio:.0f}% 金额:{x['auc_amount']/10000:.0f}万 | {plates_str}")
        
        print(f"\n  ❄️ 极端低开(≤-5%): {len(extreme_low)} 只")
        for x in sorted(extreme_low, key=lambda x: x["gap"], reverse=True)[:10]:
            plates_str = ", ".join([plate_names.get(p, p) for p in x["plates"][:3]])
            print(f"     {x['code']} {x['name']:<8s} 竞价:{x['auc']:+6.2f}% → 盘中:{x['cur']:+6.2f}% 反弹:{x['gap']:+6.2f}% | {plates_str}")
        
        # =====================================================
        # 6. 分板块预期差分析
        # =====================================================
        print("\n" + "=" * 80)
        print("6️⃣  分板块预期差分析")
        print("=" * 80)
        
        plate_analysis = defaultdict(lambda: {"stocks": [], "auc_sum": 0, "cur_sum": 0, "gap_sum": 0, "count": 0})
        
        for x in parsed:
            for plate_id in x["plates"]:
                pa = plate_analysis[plate_id]
                pa["stocks"].append(x)
                pa["auc_sum"] += x["auc"]
                pa["cur_sum"] += x["cur"]
                pa["gap_sum"] += x["gap"]
                pa["count"] += 1
        
        # 筛选有足够样本的板块
        significant_plates = {pid: pa for pid, pa in plate_analysis.items() if pa["count"] >= 3}
        
        # 按预期差排序 (正 = 超预期, 负 = 低于预期)
        sorted_plates = sorted(
            significant_plates.items(),
            key=lambda kv: kv[1]["gap_sum"] / kv[1]["count"],
            reverse=True
        )
        
        print(f"\n  超预期板块 TOP 15 (竞价→盘中涨幅扩大最多):")
        print(f"  {'板块':<12s} {'样本':>4s} {'竞价均':>8s} {'盘中均':>8s} {'预期差':>8s}")
        print(f"  {'-'*52}")
        for pid, pa in sorted_plates[:15]:
            pname = plate_names.get(pid, pid)
            avg_a = pa["auc_sum"] / pa["count"]
            avg_c = pa["cur_sum"] / pa["count"]
            avg_g = pa["gap_sum"] / pa["count"]
            print(f"  {pname:<12s} {pa['count']:>4d} {avg_a:>+7.2f}% {avg_c:>+7.2f}% {avg_g:>+7.2f}%")
        
        print(f"\n  低于预期板块 TOP 15 (竞价→盘中涨幅回落最多):")
        print(f"  {'板块':<12s} {'样本':>4s} {'竞价均':>8s} {'盘中均':>8s} {'预期差':>8s}")
        print(f"  {'-'*52}")
        for pid, pa in sorted_plates[-15:]:
            pname = plate_names.get(pid, pid)
            avg_a = pa["auc_sum"] / pa["count"]
            avg_c = pa["cur_sum"] / pa["count"]
            avg_g = pa["gap_sum"] / pa["count"]
            print(f"  {pname:<12s} {pa['count']:>4d} {avg_a:>+7.2f}% {avg_c:>+7.2f}% {avg_g:>+7.2f}%")
        
        # 竞价金额前排分析
        print("\n" + "─" * 60)
        print("7️⃣  竞价金额前 30 名个股分析")
        print("─" * 60)
        top_amount = sorted(parsed, key=lambda x: x["auc_amount"], reverse=True)[:30]
        print(f"  {'代码':<8s} {'名称':<10s} {'竞价金额':>10s} {'竞价涨幅':>8s} {'盘中涨幅':>8s} {'预期差':>8s} {'板块'}")
        print(f"  {'-'*80}")
        for x in top_amount:
            plates_str = ", ".join([plate_names.get(p, p) for p in x["plates"][:2]])
            amt_str = f"{x['auc_amount']/10000:.0f}万" if x['auc_amount'] > 0 else "?"
            print(f"  {x['code']:<8s} {x['name']:<10s} {amt_str:>10s} {x['auc']:>+7.2f}% {x['cur']:>+7.2f}% {x['gap']:>+7.2f}% {plates_str}")

# =====================================================
# 8. 检查板块指标数据
# =====================================================
print("\n" + "─" * 60)
print("8️⃣  板块指标数据 (pm:*)")
print("─" * 60)

plate_metric_keys = list(r.scan_iter(match="pm:*", count=5000))
print(f"  板块指标 key 总数: {len(plate_metric_keys)}")
if plate_metric_keys:
    sample = plate_metric_keys[:3]
    for key in sample:
        data = r.hgetall(key)
        print(f"    📌 {key}: fields={list(data.keys())[:8]}")

# =====================================================
# 9. 检查涨停数据
# =====================================================
print("\n" + "─" * 60)
print("9️⃣  涨停相关数据")
print("─" * 60)

limit_up_patterns = [
    "limit_up:*",
    "ztb:*",
    "limit:*",
]
for pattern in limit_up_patterns:
    keys = list(r.scan_iter(match=pattern, count=1000))
    if keys:
        print(f"  {pattern}: {len(keys)} 个 key")
        for k in sorted(keys)[:5]:
            kt = r.type(k)
            if kt == "hash":
                d = r.hgetall(k)
                print(f"     {k} [hash] fields={len(d)}")
            elif kt == "string":
                v = r.get(k)
                try:
                    items = json.loads(v)
                    if isinstance(items, list):
                        print(f"     {k} [string/list] len={len(items)}")
                    else:
                        print(f"     {k} [string] len={len(v)}")
                except:
                    print(f"     {k} [string] len={len(v) if v else 0}")
            elif kt == "set":
                members = r.smembers(k)
                print(f"     {k} [set] size={len(members)}")
            else:
                print(f"     {k} [{kt}]")

print("Check finished.")
