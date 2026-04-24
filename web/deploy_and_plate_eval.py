"""
2026-02-26 板块预期差分析 + 部署新代码
通过 SSH + 远程 redis-cli 批量获取数据，本地计算分析，输出 markdown 报告。
"""
import paramiko
import json
import codecs
import sys
import time

HOST = '115.190.156.240'
PORT = 22
USER = 'root'
PASSWORD = 'Chao123+'
DATE = '2026-02-26'
DATE_COMPACT = DATE.replace('-', '')


def norm_pct(v):
    try:
        x = float(v)
    except:
        return 0.0
    return x * 100.0 if abs(x) <= 1.0 else x


def safe_float(v, default=0.0):
    try:
        if v is None or v in ('', 'None', '(nil)'):
            return default
        return float(v)
    except:
        return default


def ssh_run(ssh, cmd, timeout=30):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    return stdout.read().decode('utf-8', errors='replace').strip()


def deploy(ssh):
    """上传新版 market_edge_engine.py"""
    print("="*60)
    print("  部署新版 market_edge_engine.py")
    print("="*60)
    sftp = ssh.open_sftp()
    sftp.put('market_edge_engine.py', '/root/work/web/market_edge_engine.py')
    sftp.close()
    print("  OK: 文件已上传到 /root/work/web/market_edge_engine.py")
    print("  注意：需要重启引擎进程才能生效")


def run_analysis(ssh):
    """板块预期差分析"""
    print("\n" + "="*60)
    print("  板块预期差分析 - 远程数据采集")
    print("="*60)

    # 1. 读取竞价数据
    print("  [1/4] 读取竞价快照...")
    raw = ssh_run(ssh, f'redis-cli -n 0 hget "market:auction:{DATE_COMPACT}:0925" top_amount')
    if not raw or raw == '(nil)':
        print("  ERROR: 竞价数据不存在")
        return
    items = json.loads(raw)
    print(f"  OK: {len(items)} 条竞价数据")

    # 2. 读取板块映射
    print("  [2/4] 读取板块映射...")
    plate_map_raw = ssh_run(ssh, 'redis-cli -n 0 hgetall "config:plate_mapping:s2p"', timeout=30)
    plate_map = {}
    if plate_map_raw and plate_map_raw != '(empty array)':
        lines = plate_map_raw.split('\n')
        for i in range(0, len(lines)-1, 2):
            code = lines[i].strip().strip('"')
            try:
                plist = json.loads(lines[i+1].strip().strip('"'))
                plate_map[code] = [p for p in plist if isinstance(p, str)]
            except:
                pass
    print(f"  OK: {len(plate_map)} 个股票的板块映射")

    # 3. 批量读取行情（用 redis-cli pipeline 模式）
    print("  [3/4] 批量读取收盘行情（通过管道）...")
    
    codes = []
    for it in items:
        code = str(it.get('symbol') or it.get('code') or '').strip()
        if len(code) == 6:
            codes.append(code)
    
    # 构造批量命令
    # 使用 redis-cli pipe 方式: 一次 SSH 执行多个 hget
    batch_size = 100
    quote_map = {}
    
    for batch_start in range(0, len(codes), batch_size):
        batch_codes = codes[batch_start:batch_start+batch_size]
        # 构造一条 redis-cli 命令获取多个key
        # 使用 bash -c 和 for loop
        redis_cmds = "; ".join([
            f'echo "---{c}---"; redis-cli -n 0 hget "stock:quote:{c}" change_pct; redis-cli -n 0 hget "stock:quote:{c}" name'
            for c in batch_codes
        ])
        out = ssh_run(ssh, f'bash -c \'{redis_cmds}\'', timeout=60)
        
        # 解析输出
        current_code = None
        values = []
        for line in out.split('\n'):
            line = line.strip()
            if line.startswith('---') and line.endswith('---'):
                if current_code and len(values) >= 2:
                    chg = values[0] if values[0] != '(nil)' else None
                    name = values[1] if values[1] != '(nil)' else ''
                    quote_map[current_code] = {'change_pct': chg, 'name': name}
                current_code = line.strip('-')
                values = []
            elif current_code is not None:
                values.append(line)
        # 最后一个
        if current_code and len(values) >= 2:
            chg = values[0] if values[0] != '(nil)' else None
            name = values[1] if values[1] != '(nil)' else ''
            quote_map[current_code] = {'change_pct': chg, 'name': name}
        
        progress = min(batch_start + batch_size, len(codes))
        sys.stdout.write(f"\r  进度: {progress}/{len(codes)}")
        sys.stdout.flush()
    
    print(f"\n  OK: 获取 {len(quote_map)} 只股票行情")

    # 4. 分析
    print("  [4/4] 计算板块预期差...")
    rows = []
    for it in items:
        if not isinstance(it, dict):
            continue
        code = str(it.get('symbol') or it.get('code') or '').strip()
        if len(code) != 6:
            continue

        auc = norm_pct(it.get('change_pct', it.get('bid_change_pct', 0)))
        bid = safe_float(it.get('bid_amount_yuan', it.get('bid_amount', 0)))
        aamt = safe_float(it.get('auction_amount_yuan', it.get('amount', 0)))

        q = quote_map.get(code, {})
        name = q.get('name', '') or it.get('name', code)
        cur_raw = q.get('change_pct')
        if cur_raw is not None:
            cur = norm_pct(cur_raw)
        else:
            cur = auc  # 无行情时用竞价替代

        gap = cur - auc

        plates = plate_map.get(code, [])
        safe_plates = [p for p in plates if isinstance(p, str) and '昨日' not in p]

        rows.append({
            'code': code,
            'name': name,
            'auc': auc,
            'cur': cur,
            'gap': gap,
            'aamt': aamt,
            'bid': bid,
            'plates': safe_plates,
        })

    rows.sort(key=lambda x: x['aamt'], reverse=True)

    # ── 生成 Markdown 报告 ──
    md = [f"# {DATE} 盘后竞价预期差分析 (按板块分类)\n\n"]
    md.append(f"共加载 {len(rows)} 只竞价样本股。\n")
    md.append("*涨跌幅已修正为真实百分比格式（10%为涨停）。预期差(Gap) = 收盘涨幅 - 竞价涨幅。*\n\n")

    # ── 强封单 ──
    strong_bids = [x for x in rows if x['bid'] > x['aamt'] * 1.5 and x['aamt'] > 10000000]
    strong_bids.sort(key=lambda x: x['bid'], reverse=True)
    md.append("## 💪 强封单 (封单>竞价额1.5倍 且 竞价额>1000万)\n")
    md.append("| 代码 | 名称 | 竞价% | 收盘% | 预期差 | 竞价(万) | 封单(万) | 板块 |\n")
    md.append("|---|---|---|---|---|---|---|---|\n")
    for x in strong_bids[:15]:
        plates_str = ",".join(x['plates'][:2]) or "未分类"
        md.append(f"| {x['code']} | {x['name']} | {x['auc']:.2f}% | {x['cur']:.2f}% | **{x['gap']:+.2f}%** | {x['aamt']/10000:.0f}万 | {x['bid']/10000:.0f}万 | {plates_str} |\n")

    # ── 高开兑现 & 高开回落 ──
    high_open = [x for x in rows if x['auc'] >= 5.0]
    if high_open:
        high_hold = [x for x in high_open if x['cur'] >= x['auc'] - 2.0]
        high_fade = [x for x in high_open if x['gap'] <= -4.0]
        md.append(f"\n## 🔥 高开(>=5%)兑现情况\n")
        md.append(f"高开总数: {len(high_open)} | 兑现(收盘接近竞价): {len(high_hold)} ({len(high_hold)/max(1,len(high_open))*100:.0f}%) | 回落(Gap<=-4%): {len(high_fade)} ({len(high_fade)/max(1,len(high_open))*100:.0f}%)\n\n")
        if high_fade:
            md.append("**高开回落 Top10:**\n")
            md.append("| 代码 | 名称 | 竞价% | 收盘% | 预期差 | 竞价额(万) | 板块 |\n")
            md.append("|---|---|---|---|---|---|---|\n")
            for x in sorted(high_fade, key=lambda y: y['gap'])[:10]:
                plates_str = ",".join(x['plates'][:2]) or "未分类"
                md.append(f"| {x['code']} | {x['name']} | {x['auc']:.2f}% | {x['cur']:.2f}% | **{x['gap']:+.2f}%** | {x['aamt']/10000:.0f}万 | {plates_str} |\n")

    # ── 低开反弹 ──
    deep_low = [x for x in rows if x['auc'] <= -3.0]
    if deep_low:
        rebound = [x for x in deep_low if x['gap'] >= 3.0]
        md.append(f"\n## 💎 低开(<=-3%)反弹情况\n")
        md.append(f"低开总数: {len(deep_low)} | 反弹(Gap>=3%): {len(rebound)} ({len(rebound)/max(1,len(deep_low))*100:.0f}%)\n\n")
        if rebound:
            md.append("**低开反弹 Top10:**\n")
            md.append("| 代码 | 名称 | 竞价% | 收盘% | 预期差 | 竞价额(万) | 板块 |\n")
            md.append("|---|---|---|---|---|---|---|\n")
            for x in sorted(rebound, key=lambda y: y['gap'], reverse=True)[:10]:
                plates_str = ",".join(x['plates'][:2]) or "未分类"
                md.append(f"| {x['code']} | {x['name']} | {x['auc']:.2f}% | {x['cur']:.2f}% | **{x['gap']:+.2f}%** | {x['aamt']/10000:.0f}万 | {plates_str} |\n")

    # ── 板块汇总 ──
    plate_stats = {}
    for x in rows:
        plates = x['plates'] if x['plates'] else ['未分类']
        primary = plates[0]
        if primary not in plate_stats:
            plate_stats[primary] = {'count': 0, 'gap_sum': 0.0, 'up': 0, 'down': 0, 'stocks': []}
        plate_stats[primary]['count'] += 1
        plate_stats[primary]['gap_sum'] += x['gap']
        if x['gap'] > 0:
            plate_stats[primary]['up'] += 1
        elif x['gap'] < 0:
            plate_stats[primary]['down'] += 1
        plate_stats[primary]['stocks'].append(x)

    valid_plates = []
    for p, stats in plate_stats.items():
        if stats['count'] >= 2:
            avg_gap = stats['gap_sum'] / stats['count']
            win_rate = stats['up'] / max(1, stats['count'])
            top_stocks = sorted(stats['stocks'], key=lambda x: x['gap'], reverse=True)[:3]
            top_str = "<br>".join([f"{s['name']}({s['gap']:+.1f}%)" for s in top_stocks])
            sorted_stocks = sorted(stats['stocks'], key=lambda x: x['aamt'], reverse=True)
            valid_plates.append({
                'plate': p, 'count': stats['count'], 'avg_gap': avg_gap,
                'win_rate': win_rate, 'top_str': top_str, 'all_stocks': sorted_stocks,
            })
    valid_plates.sort(key=lambda x: x['avg_gap'], reverse=True)

    md.append("\n## 📊 板块概览 (按平均预期差排序)\n")
    md.append("| 板块 | 股票数 | 平均预期差 | 正反馈率 | 领涨核心股 (日内Gap) |\n")
    md.append("|---|---|---|---|---|\n")
    for p in valid_plates:
        md.append(f"| {p['plate']} | {p['count']} | **{p['avg_gap']:+.2f}%** | {p['win_rate']:.0%} | {p['top_str']} |\n")

    # ── 板块明细（仅Top10和Bottom10） ──
    top_plates = valid_plates[:10]
    bot_plates = valid_plates[-10:] if len(valid_plates) > 10 else []

    md.append(f"\n## 🚀 预期差最高 Top10 板块明细\n")
    for p in top_plates:
        md.append(f"\n### {p['plate']} (共{p['count']}只, 平均预期差{p['avg_gap']:+.2f}%, 正反馈率{p['win_rate']:.0%})\n")
        md.append("| 代码 | 名称 | 竞价% | 收盘% | 预期差 | 竞价额(万) | 封单(万) |\n")
        md.append("|---|---|---|---|---|---|---|\n")
        for x in p['all_stocks']:
            md.append(f"| {x['code']} | {x['name']} | {x['auc']:.2f}% | {x['cur']:.2f}% | **{x['gap']:+.2f}%** | {x['aamt']/10000:.0f}万 | {x['bid']/10000:.0f}万 |\n")

    if bot_plates:
        md.append(f"\n## 💀 预期差最低 Bottom10 板块明细\n")
        for p in bot_plates:
            md.append(f"\n### {p['plate']} (共{p['count']}只, 平均预期差{p['avg_gap']:+.2f}%, 正反馈率{p['win_rate']:.0%})\n")
            md.append("| 代码 | 名称 | 竞价% | 收盘% | 预期差 | 竞价额(万) | 封单(万) |\n")
            md.append("|---|---|---|---|---|---|---|\n")
            for x in p['all_stocks']:
                md.append(f"| {x['code']} | {x['name']} | {x['auc']:.2f}% | {x['cur']:.2f}% | **{x['gap']:+.2f}%** | {x['aamt']/10000:.0f}万 | {x['bid']/10000:.0f}万 |\n")

    # ── 综合指标 ──
    total_fade = sum(1 for x in rows if x['auc'] >= 5.0 and x['gap'] <= -4.0)
    total_rise = sum(1 for x in rows if x['auc'] <= 1.0 and x['gap'] >= 3.0 and x['cur'] > 0)
    extreme_low = [x for x in rows if x['auc'] <= -8.0]
    extreme_rebound = [x for x in extreme_low if x['gap'] >= 5.0]

    md.append("\n## 📈 综合预期差指标\n")
    md.append(f"| 指标 | 值 |\n")
    md.append(f"|---|---|\n")
    md.append(f"| 总样本 | {len(rows)} |\n")
    md.append(f"| 高开回落(竞价>=5%,Gap<=-4%) | {total_fade} |\n")
    md.append(f"| 低位反弹(竞价<=1%,Gap>=3%) | {total_rise} |\n")
    md.append(f"| 极端低开(<=-8%) | {len(extreme_low)} |\n")
    md.append(f"| 极端低开反弹(Gap>=5%) | {len(extreme_rebound)} |\n")
    md.append(f"| 板块总数 | {len(valid_plates)} |\n")
    md.append(f"| 正向板块(平均Gap>0) | {sum(1 for p in valid_plates if p['avg_gap'] > 0)} |\n")
    md.append(f"| 负向板块(平均Gap<0) | {sum(1 for p in valid_plates if p['avg_gap'] < 0)} |\n")

    # 输出文件
    outfile = f'plate_eval_{DATE}.md'
    with codecs.open(outfile, 'w', 'utf-8') as f:
        f.writelines(md)
    print(f"\n  报告已输出: {outfile}")
    print(f"  板块数: {len(valid_plates)} | 样本: {len(rows)}")
    print(f"  预期差最高板块: {valid_plates[0]['plate']}({valid_plates[0]['avg_gap']:+.2f}%)" if valid_plates else "")
    print(f"  预期差最低板块: {valid_plates[-1]['plate']}({valid_plates[-1]['avg_gap']:+.2f}%)" if valid_plates else "")


def main():
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(HOST, PORT, USER, PASSWORD, timeout=10)
        print("SSH connected OK\n")
    except Exception as e:
        print(f"SSH failed: {e}")
        sys.exit(1)

    try:
        # 1. 部署新代码
        deploy(ssh)
        # 2. 板块预期差分析
        run_analysis(ssh)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        ssh.close()
        print("\nSSH closed.")


if __name__ == "__main__":
    main()
