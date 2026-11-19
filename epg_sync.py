#!/usr/bin/env python3
"""EPG 电子节目单抓取与合并
抓取多个来源的 EPG，去重合并后生成 guide.xml 与 epg_index.json
"""
import argparse
import asyncio
import aiohttp
import aiofiles
import gzip
import json
import re
import time
import os
import hashlib
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# ============================================================
#  配置（可由环境变量覆盖）
# ============================================================
EPG_DIR = Path(os.environ.get('EPG_DIR', 'output'))
EPG_CACHE_DIR = Path(os.environ.get('EPG_CACHE_DIR', 'epg_cache'))
MAX_AGE_DAYS = int(os.environ.get('MAX_AGE_DAYS', 14))      # 抓取回溯窗口（天）
FETCH_TIMEOUT = int(os.environ.get('EPG_FETCH_TIMEOUT', 60))
CONCURRENCY = int(os.environ.get('EPG_CONCURRENCY', 8))
MIN_PROGRAMS = int(os.environ.get('EPG_MIN_PROGRAMS', 3))    # 单频道最少节目数
DEDUPE_WINDOW_HOURS = int(os.environ.get('EPG_DEDUPE_WINDOW', 2))  # 去重时间窗
USER_AGENT = "Mozilla/5.0 (TVBox EPG Sync)"

SOURCES = [
    {
        "name": "iptv-org/china",
        "url": "https://gh-proxy.com/https://raw.githubusercontent.com/iptv-org/epg/master/epgs/China.epg.xml",
        "format": "xml",
        "priority": 1,
    },
    {
        "name": "iptv-org/hongkong",
        "url": "https://gh-proxy.com/https://raw.githubusercontent.com/iptv-org/epg/master/epgs/HongKong.epg.xml",
        "format": "xml",
        "priority": 2,
    },
    {
        "name": "iptv-org/guides/zh",
        "url": "https://gh-proxy.com/https://raw.githubusercontent.com/iptv-org/epg/master/guides/zh.xml.gz",
        "format": "gz",
        "priority": 2,
    },
    {
        "name": "Guovin/gd",
        "url": "https://gh-proxy.com/https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/epg/epg.xml",
        "format": "xml",
        "priority": 3,
    },
    {
        "name": "BurningC4/Chinese-IPTV",
        "url": "https://cdn.jsdelivr.net/gh/BurningC4/Chinese-IPTV@master/guide.xml",
        "format": "xml",
        "priority": 3,
    },
    {
        "name": "fanmingming",
        "url": "https://live.fanmingming.com/e.xml",
        "format": "xml",
        "priority": 4,
    },
]

# 频道名归一化映射（把 EPG 里的频道名对齐到聚合器的 tvg-id）
CHANNEL_ALIAS = {
    "CCTV-1综合": "CCTV1", "CCTV1综合": "CCTV1", "CCTV 1": "CCTV1",
    "CCTV-2财经": "CCTV2", "CCTV2财经": "CCTV2",
    "CCTV-3综艺": "CCTV3", "CCTV3综艺": "CCTV3",
    "CCTV-4中文国际": "CCTV4", "CCTV4中文国际": "CCTV4", "CCTV4国际": "CCTV4",
    "CCTV-5体育": "CCTV5", "CCTV5体育": "CCTV5", "CCTV5+体育赛事": "CCTV5+",
    "CCTV-6电影": "CCTV6", "CCTV6电影": "CCTV6",
    "CCTV-7军事农业": "CCTV7", "CCTV7军事农业": "CCTV7",
    "CCTV-8电视剧": "CCTV8", "CCTV8电视剧": "CCTV8",
    "CCTV-9纪录": "CCTV9", "CCTV9纪录": "CCTV9",
    "CCTV-10科教": "CCTV10", "CCTV10科教": "CCTV10",
    "CCTV-11戏曲": "CCTV11", "CCTV11戏曲": "CCTV11",
    "CCTV-12社会与法": "CCTV12", "CCTV12社会与法": "CCTV12",
    "CCTV-13新闻": "CCTV13", "CCTV13新闻": "CCTV13",
    "CCTV-14少儿": "CCTV14", "CCTV14少儿": "CCTV14",
    "CCTV-15音乐": "CCTV15", "CCTV15音乐": "CCTV15",
    "CCTV-16奥林匹克": "CCTV16", "CCTV16奥林匹克": "CCTV16",
    "CCTV-17农业农村": "CCTV17", "CCTV17农业农村": "CCTV17",
    "CCTV4K": "CCTV4K", "CCTV-4K": "CCTV4K",
    "CCTV8K": "CCTV8K", "CCTV-8K": "CCTV8K",
}

# ============================================================
#  解析
# ============================================================
def normalize_channel(name):
    if not name:
        return ''
    n = name.strip()
    n = re.sub(r'\s+', '', n)
    # 去掉分辨率/后缀
    n = re.sub(r'(?<=\d)([Kk]|[Pp]\d+).*$', '', n)
    n = re.sub(r'(HD|SD|4K|8K|FHD|高清|标清|HDTV).*$', '', n, flags=re.I)
    return CHANNEL_ALIAS.get(n, n)


async def fetch_one(session, src, sem):
    async with sem:
        url = src['url']
        cp = EPG_CACHE_DIR / (re.sub(r'[^\w]', '_', src['name']) + '.xml')
        age = _cache_age(cp)

        if age is not None and age < MAX_AGE_DAYS:
            print(f"  📦 [{src['name']}] 命中缓存 ({age:.1f}h 前)")
            return src, cp.read_text(encoding='utf-8', errors='ignore')

        t0 = time.monotonic()
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=FETCH_TIMEOUT),
                headers={"User-Agent": USER_AGENT},
            ) as r:
                if r.status != 200:
                    print(f"  ✗ [{src['name']}] HTTP {r.status}")
                    return src, None
                raw = await r.read()
                text = _decode(raw, src.get('format') == 'gz')
                if len(text) < 1000:
                    print(f"  ✗ [{src['name']}] 内容过短 {len(text)} 字节")
                    return src, None
                cp.parent.mkdir(parents=True, exist_ok=True)
                cp.write_text(text, encoding='utf-8')
                print(f"  ✓ [{src['name']}] {len(text)} 字节, {time.monotonic()-t0:.1f}s")
                return src, text
        except Exception as e:
            print(f"  ✗ [{src['name']}] {type(e).__name__}: {e}")
            return src, None


def _cache_age(cp):
    if not cp.exists():
        return None
    return (time.time() - cp.stat().st_mtime) / 3600.0


def _decode(raw, is_gz):
    if is_gz:
        try:
            return gzip.decompress(raw).decode('utf-8', errors='ignore')
        except Exception:
            pass
    for enc in ('utf-8', 'gbk', 'gb2312', 'latin-1'):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode('utf-8', errors='ignore')


def parse_epg_xml(text, source_name, priority):
    """解析 XMLTV 格式，返回 {channel_id: [programs...]}"""
    programs = defaultdict(list)
    channels = {}
    if not text:
        return channels, programs

    # 提取 display-name 映射
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        print(f"  ⚠️ [{source_name}] XML 解析失败: {e}")
        return channels, programs

    for ch in root.iter('channel'):
        cid = ch.get('id', '').strip()
        if not cid:
            continue
        name = ''
        for dn in ch.iter('display-name'):
            name = (dn.text or '').strip()
            if name:
                break
        if name:
            channels[cid] = name

    for prog in root.iter('programme'):
        cid = prog.get('channel', '').strip()
        start = prog.get('start', '').strip()
        stop = prog.get('stop', '').strip()
        title_el = prog.find('title')
        title = (title_el.text or '').strip() if title_el is not None else ''
        if not cid or not start or not title:
            continue
        programs[cid].append({
            'start': _normalize_time(start),
            'stop': _normalize_time(stop),
            'title': title,
            'source': source_name,
            'priority': priority,
        })

    return channels, programs


def _normalize_time(t):
    """把 XMLTV 时间格式归一化为 ISO 8601"""
    t = re.sub(r'[^\d\+-]', '', t)
    if len(t) >= 14:
        return t[:14]
    return t


# ============================================================
#  合并与去重
# ============================================================
def merge(all_data):
    """按优先级合并多个来源的 EPG"""
    # 频道名 → 节目
    merged = defaultdict(list)
    channel_names = {}

    # 按优先级排序
    items = sorted(all_data, key=lambda x: x['priority'])

    for src_name, priority, channels, programs in items:
        for cid, name in channels.items():
            norm = normalize_channel(name)
            if norm:
                channel_names[norm] = name

        for cid, progs in programs.items():
            # 尝试用 channel id 归一化
            norm_id = normalize_channel(cid)
            if norm_id in channel_names:
                norm = norm_id
            else:
                # 用 channel 里已知的 name 反查
                name = channels.get(cid, '')
                norm = normalize_channel(name) if name else ''
            if not norm:
                continue
            for p in progs:
                merged[norm].append(p)

    # 去重：同一频道、时间窗内相同标题只保留高优先级的
    result = {}
    for ch, progs in merged.items():
        progs.sort(key=lambda x: (x['start'], -x['priority']))
        deduped = []
        seen_keys = set()
        for p in progs:
            key = (p['start'][:8], _hour_bucket(p['start']))
            dedup_key = (key, p['title'])
            if dedup_key in seen_keys:
                continue
            seen_keys.add(dedup_key)
            deduped.append(p)
        if len(deduped) >= MIN_PROGRAMS:
            result[ch] = deduped

    return result, channel_names


def _hour_bucket(t):
    if len(t) < 10:
        return 0
    try:
        h = int(t[8:10])
        return h // DEDUPE_WINDOW_HOURS
    except Exception:
        return 0


# ============================================================
#  输出
# ============================================================
def write_xml(merged, channel_names):
    """生成标准 XMLTV 文件"""
    root = ET.Element('tv')
    root.set('generator-info-name', 'ysc-self-host epg_sync')

    now = datetime.utcnow()
    week_start = (now - timedelta(days=MAX_AGE_DAYS)).strftime('%Y%m%d%H%M%S') + ' +0000'
    week_end = (now + timedelta(days=2)).strftime('%Y%m%d%H%M%S') + ' +0000'

    for ch in sorted(merged.keys()):
        ce = ET.SubElement(root, 'channel')
        ce.set('id', ch)
        dn = ET.SubElement(ce, 'display-name')
        dn.text = channel_names.get(ch, ch)

    for ch, progs in merged.items():
        for p in progs:
            if p['start'] < week_start or p['start'] > week_end:
                continue
            pe = ET.SubElement(root, 'programme')
            pe.set('channel', ch)
            pe.set('start', p['start'] + ' +0000')
            if p.get('stop'):
                pe.set('stop', p['stop'] + ' +0000')
            te = ET.SubElement(pe, 'title')
            te.text = p['title']

    tree = ET.ElementTree(root)
    out = EPG_DIR / 'epg.xml'
    tree.write(out, encoding='utf-8', xml_declaration=True)
    return out, len(root.findall('programme')), len(root.findall('channel'))


def write_index(merged):
    """生成轻量索引 JSON，供前端快速查询"""
    index = {}
    for ch, progs in merged.items():
        today = datetime.utcnow().strftime('%Y%m%d')
        today_progs = [p for p in progs if p['start'].startswith(today)]
        if not today_progs:
            # 取最近一天的
            future = [p for p in progs if p['start'] > datetime.utcnow().strftime('%Y%m%d%H%M%S')]
            today_progs = sorted(future, key=lambda x: x['start'])[:12]
        index[ch] = [{'s': p['start'], 't': p['title']} for p in today_progs[:12]]

    out = EPG_DIR / 'epg_index.json'
    out.write_text(json.dumps(index, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
    return out, len(index)


# ============================================================
#  主流程
# ============================================================
async def main_async():
    EPG_DIR.mkdir(parents=True, exist_ok=True)
    EPG_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY * 2)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_one(session, src, sem) for src in SOURCES]
        results = await asyncio.gather(*tasks)

    all_data = []
    for src, text in results:
        if not text:
            continue
        channels, programs = parse_epg_xml(text, src['name'], src['priority'])
        print(f"  📺 [{src['name']}] 频道 {len(channels)} 个, 节目 {sum(len(v) for v in programs.values())} 条")
        all_data.append((src['name'], src['priority'], channels, programs))

    if not all_data:
        print("⚠️ 所有 EPG 源都抓取失败，跳过生成")
        return

    merged, channel_names = merge(all_data)
    print(f"\n🔀 合并完成：{len(merged)} 个频道有有效节目")

    xml_path, prog_count, ch_count = write_xml(merged, channel_names)
    print(f"  ✓ {xml_path} ({ch_count} 频道, {prog_count} 节目)")

    idx_path, idx_count = write_index(merged)
    print(f"  ✓ {idx_path} ({idx_count} 频道今日节目)")


def cleanup():
    """清理过旧缓存，并动态调整回溯窗口"""
    global MAX_AGE_DAYS                      # ← 放在函数最前
    cutoff = time.time() - MAX_AGE_DAYS * 86400
    removed = 0
    for cp in EPG_CACHE_DIR.glob('*.xml'):
        try:
            if cp.stat().st_mtime < cutoff:
                cp.unlink()
                removed += 1
        except Exception:
            pass
    if removed:
        print(f"🧹 清理旧缓存 {removed} 个文件")

    # 抓取成功率低时适当缩短窗口，避免空跑
    MAX_AGE_DAYS = max(7, int(MAX_AGE_DAYS * 0.9))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='EPG 电子节目单同步')
    parser.add_argument('--cleanup', action='store_true', help='仅清理旧缓存')
    args = parser.parse_args()

    if args.cleanup:
        cleanup()
    else:
        asyncio.run(main_async())
        cleanup()
