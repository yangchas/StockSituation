"""
retro_today.py — MarketEdge 盘后复盘脚本 (Decision Logic V4.0)
=================================================================
功能:
  1. 拉取今日涨停全量数据 (pykaipan.getHisBans)
  2. 拉取今日热门板块       (pykaipan.getHisPlates)
  3. 拉取 09:25 竞价截面   (Redis → market:auction:YYYYMMDD:0925)
  4. 分析 1进2 晋级质量    (竞价溢价 vs 收盘连板)
  5. 综合复盘              (情绪、板块、竞价共振)

用法: python retro_today.py [YYYYMMDD]
"""

import sys
import json
import redis
import logging
from datetime import datetime
from collections import defaultdict

sys.path.append('/usr/local/lib/python3.9/site-packages')
sys.path.append('/root/work/web')
sys.path.append('/root/work/ai')

logging.basicConfig(level=logging.WARNING)

# ── 工具函数 ──────────────────────────────────────────────────

def color(text, code): return f"\033[{code}m{text}\033[0m"
def red(t):    return color(t, "91")
def green(t):  return color(t, "92")
def yellow(t): return color(t, "93")
def cyan(t):   return color(t, "96")
def bold(t):   return color(t, "1")

def pct_fmt(v, mul=1):
    v = float(v) * mul
    s = f"{v:+.2f}%"
    return green(s) if v > 0 else (red(s) if v < 0 else s)

def amt_fmt(v_yuan): return f"{float(v_yuan)/1e8:.2f}亿"

# ── 数据获取 ──────────────────────────────────────────────────

def fetch_limit_ups(date_str: str) -> list:
    """[V3.5 统一网关] 拉取全量涨停，采用 StockAnalyzer 统一逻辑"""
    from ai.API.StockAnalyzer import StockAnalyzer
    analyzer = StockAnalyzer()
    # 直接调用网关的黄金解析逻辑 (内部已处理 1-5 板、Index [15]/[12]、去重)
    return analyzer.get_history_bans_pool(date_str, max_ban=5)


def fetch_hot_plates(date_str: str) -> list:
    """拉取今日热门板块 Top10"""
    from pykaipan.pykaipan import getHisPlates
    try:
        res = getHisPlates(date=date_str)
        p_list = res.get('list', [])
        return [(str(p[1]), int(p[4])) for p in p_list[:10]]
    except:
        return []


def fetch_auction_map(date_compact: str) -> dict:
    """从 Redis 拉取 09:25 竞价截面，返回 code -> {change_pct, amount}"""
    r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    result = {}

    # 优先拉 top_amount 快速索引
    raw = r.hget(f"market:auction:{date_compact}:0925", "top_amount")
    if raw:
        try:
            for item in json.loads(raw):
                code = str(item.get('code', ''))[-6:].zfill(6)
                pct  = float(item.get('change_pct', 0))
                if abs(pct) > 1.0: pct /= 100.0
                result[code] = {"change_pct": pct, "amount": float(item.get('amount', item.get('auction_amount_yuan', 0)))}
        except: pass

    # 降级: 扫描全量 hash
    if not result:
        raw2 = r.hgetall(f"market:auction:{date_compact}:0925")
        if raw2:
            for k, v in raw2.items():
                if k == "top_amount": continue
                try:
                    item = json.loads(v)
                    code = str(item.get('code', k))[-6:].zfill(6)
                    pct  = float(item.get('change_pct', 0))
                    if abs(pct) > 1.0: pct /= 100.0
                    result[code] = {"change_pct": pct, "amount": float(item.get('auction_amount_yuan', 0))}
                except: pass

    return result


def fetch_yest_bans(date_str: str) -> dict:
    """拉取前一日涨停 (用于判断 1进2), 返回 code -> lb_days"""
    from pykaipan.pykaipan import getHisBans
    # 获取前交易日
    sys.path.append('/root/work/web')
    try:
        from trade_calendar import TradeCalendar
        cal = TradeCalendar()
        yest = cal.get_previous_trade_day(date_str)
    except:
        yest = date_str  # fallback

    result = {}
    for ban_lvl in ['1', '2', '3', '4', '5']:
        try:
            res = getHisBans(date=yest, ban=ban_lvl, size=200)
            for page in res.get('info', []):
                for rec in page:
                    if len(rec) < 16: continue
                    code = str(rec[0])[-6:].zfill(6)
                    lb   = int(rec[15]) if rec[15] else 1
                    result[code] = lb
        except: pass
    return result

# ── 分析逻辑 ──────────────────────────────────────────────────

def rate_seal_quality(seal_time: str) -> str:
    """根据封板时间评价竞价质量"""
    try:
        h, m, s = int(seal_time[:2]), int(seal_time[2:4]), int(seal_time[4:6]) if len(seal_time) >= 6 else 0
    except:
        return "?"
    mins = h * 60 + m
    if mins <= 9 * 60 + 30: return green("☆ 竞价封板")
    if mins <= 9 * 60 + 45: return green("★ 闪电封板")
    if mins <= 10 * 60 + 30: return yellow("▲ 上午封板")
    if mins <= 14 * 60:     return yellow("■ 午后封板")
    return red("▼ 尾盘封板")

def analyze(date_str: str):
    date_compact = date_str.replace('-', '')
    print(bold(f"\n{'='*65}"))
    print(bold(f"  📊 MarketEdge 盘后复盘  [{date_str}]"))
    print(bold(f"{'='*65}"))

    # 1. 拉数据
    print("\n⏳ 正在拉取数据...")
    today_bans  = fetch_limit_ups(date_str)
    hot_plates  = fetch_hot_plates(date_str)
    auction_map = fetch_auction_map(date_compact)
    yest_bans   = fetch_yest_bans(date_str)

    print(f"   今日涨停: {bold(str(len(today_bans)))} 只  |  热门板块: {len(hot_plates)} 条  |  竞价截面: {len(auction_map)} 只")

    if not today_bans:
        print(red("\n❌ 无法获取今日涨停数据，请检查 pykaipan 连接或日期是否为交易日。"))
        return

    # 2. 标记 1板/2板 等，并注入竞价数据
    for b in today_bans:
        code = b['code']
        b['yest_lb'] = yest_bans.get(code, 0)  # 昨日板数; 0 = 昨日未涨停
        auc = auction_map.get(code, {})
        b['auc_pct']  = auc.get('change_pct', 0.0)
        b['auc_amt']  = auc.get('amount', 0.0)
        b['is_1go2']  = (b['yest_lb'] == 1 and b['lb_days'] >= 2)  # 昨日1板今日成功晋级

    # ── 板块总览 ─────────────────────────────────────────────
    print(f"\n{cyan(bold('📂 今日热门板块 (来源: kaipan)'))} — 今日晋级率对照")
    print(f"   {'板块':<10} {'涨停数':<6}  {'板块内涨停个股'}")
    print(f"   {'-'*60}")
    plate_to_bans = defaultdict(list)
    for b in today_bans:
        plate_to_bans[b['plate']].append(b)

    for p_name, p_cnt in hot_plates[:8]:
        bans_in_plate = plate_to_bans.get(p_name, [])
        names = "、".join([b['name'] for b in bans_in_plate[:4]])
        cnt_str = green(str(p_cnt)) if p_cnt > 2 else str(p_cnt)
        print(f"   {p_name:<10} {cnt_str:<6}  {names or '暂无'}")

    # ── 1进2 晋级分析 ─────────────────────────────────────────
    went_to_2 = [b for b in today_bans if b['is_1go2']]
    # 昨日1板、今日未成功晋级 (掉在 1板 or 未涨停)
    failed_to_2 = [code for code, lb in yest_bans.items() if lb == 1 and code not in {b['code'] for b in today_bans}]

    total_1b = len(went_to_2) + len(failed_to_2)
    promo_rate = len(went_to_2) / total_1b * 100 if total_1b > 0 else 0

    print(f"\n{cyan(bold('🎯 1板 → 2板 晋级分析'))} — 共 {total_1b} 只昨日1板")
    print(f"   晋级成功: {green(str(len(went_to_2)))} 只  |  晋级失败: {red(str(len(failed_to_2)))} 只  |  晋级率: {bold(f'{promo_rate:.1f}%')}")
    print()

    # 按竞价涨幅排序展示成功者
    went_to_2.sort(key=lambda x: x['auc_pct'], reverse=True)
    print(f"   {'个股':<8} {'板块':<10} {'竞价涨幅':<10} {'竞价额':<8} {'封板质量':<14} {'昨日板'}")
    print(f"   {'-'*72}")
    for b in went_to_2:
        auc_pct_str = pct_fmt(b['auc_pct'])
        seal_q = rate_seal_quality(b['seal_time'].replace(':', '').replace(' ', ''))
        row = f"   {b['name']:<8} {b['plate']:<10} {auc_pct_str:<20} {amt_fmt(b['auc_amt']):<10} {seal_q:<24} {b['yest_lb']}B→{b['lb_days']}B"
        print(row)

    # 竞价共振分析
    if went_to_2:
        auc_corr = [b for b in went_to_2 if b['auc_pct'] > 0.05]
        print(f"\n   💡 竞价预示强度: {len(auc_corr)}/{len(went_to_2)} 只在竞价阶段 >5% (高预示)")
        avg_auc = sum(b['auc_pct'] for b in went_to_2) / len(went_to_2)
        print(f"   💡 平均竞价涨幅: {pct_fmt(avg_auc)}")

    # ── 多板段分析 ───────────────────────────────────────────
    higher_boards = [b for b in today_bans if b['lb_days'] >= 3]
    if higher_boards:
        print(f"\n{cyan(bold('👑 高板段详情 (≥3板)'))} — 共 {len(higher_boards)} 只")
        print(f"   {'个股':<8} {'板天':<5} {'板块':<12} {'竞价涨幅':<10} {'竞价额':<8} {'封板质量'}")
        print(f"   {'-'*72}")
        for b in sorted(higher_boards, key=lambda x: x['lb_days'], reverse=True):
            auc_pct_str = pct_fmt(b['auc_pct'])
            seal_q = rate_seal_quality(b['seal_time'].replace(':', '').replace(' ', ''))
            print(f"   {b['name']:<8} {b['lb_days']}B    {b['plate']:<12} {auc_pct_str:<20} {amt_fmt(b['auc_amt']):<10} {seal_q}")

    # ── 竞价强弱全局视角 ─────────────────────────────────────
    bans_with_auc = [b for b in today_bans if b['auc_pct'] != 0]
    if bans_with_auc:
        one_word = [b for b in bans_with_auc if b['auc_pct'] >= 0.095]
        strong_open = [b for b in bans_with_auc if 0.05 <= b['auc_pct'] < 0.095]
        weak_open = [b for b in bans_with_auc if b['auc_pct'] < 0.02]

        print(f"\n{cyan(bold('🔥 竞价动能复盘'))} — 覆盖 {len(bans_with_auc)}/{len(today_bans)} 只涨停")
        print(f"   一字竞价 (≥9.5%): {green(str(len(one_word)))} 只")
        print(f"   强势竞价 (5~9.5%): {green(str(len(strong_open)))} 只")
        print(f"   弱势竞价 (<2%):   {red(str(len(weak_open)))} 只")

        # Top3 竞价大单
        top_auc_amt = sorted(bans_with_auc, key=lambda x: x['auc_amt'], reverse=True)[:3]
        print(f"\n   💰 竞价资金前3:")
        for b in top_auc_amt:
            print(f"      {b['name']} ({b['plate']}) — {amt_fmt(b['auc_amt'])} | 竞价:{pct_fmt(b['auc_pct'])}")

    # ── 综合情绪评判 ─────────────────────────────────────────
    total = len(today_bans)
    high_boards = len([b for b in today_bans if b['lb_days'] >= 4])
    print(f"\n{cyan(bold('📊 综合情绪评判'))}")
    print(f"   总涨停: {total} 只  |  4板以上: {green(str(high_boards))} 只  |  1进2晋级率: {bold(f'{promo_rate:.1f}%')}")

    if promo_rate >= 60:
        verdict = green("✅ 赚钱效应强  → 明日可积极参与次日连板机会")
    elif promo_rate >= 40:
        verdict = yellow("⚖️ 分歧震荡    → 明日聚焦强势板块龙头，控制仓位")
    else:
        verdict = red("❌ 亏钱效应明显 → 明日以轻仓防守为主，等待修复信号")
    print(f"   🎯 综合判断: {verdict}")

    print(f"\n{bold('='*65)}\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        d = sys.argv[1]
        # 接受 YYYYMMDD 或 YYYY-MM-DD
        if len(d) == 8:
            d = f"{d[:4]}-{d[4:6]}-{d[6:]}"
    else:
        d = datetime.now().strftime("%Y-%m-%d")
    analyze(d)
