#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG 节目单同步脚本（v2）
- 多源抓取：iptv-org（主）+ Guovin（兜底）
- 过期过滤：丢弃超过 7 天的旧节目，避免 Guovin 那种过期 3 个月的数据污染
- 增量合并：同频道多源节目去重，取最新的一份
"""
import os
import sys
import gzip
import json
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

OUTPUT_DIR = "output"
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
EPG_OUT = os.path.join(OUTPUT_DIR, "epg.xml")
CACHE_FILE = os.path.join(CACHE_DIR, "epg_last.json")

# ── 过期阈值：超过这个天数的节目直接丢弃 ──
MAX_AGE_DAYS = 7

PROXIES = [
    "https://gh-proxy.com/",
    "https://gh-proxy.cc/",
    "https://gh-proxy.cn/",
    "https://gh.llkk.cc/",
    "https://hub.gitmirror.com/",
]

# ── 多源配置 ──
EPG_SOURCES = [
    {
        "name": "iptv-org China",
        "type": "iptv-org",
        "raw": "iptv-org/epg/master/epgs/China.epg.xml.gz",
        "active": True,
    },
    {
        "name": "iptv-org HongKong",
        "type": "iptv-org",
        "raw": "iptv-org/epg/master/epgs/HongKong.epg.xml.gz",
        "active": True,
    },
    {
        "name": "Guovin EPG (gd)",
        "type": "guovin",
        "raw": "Guovin/iptv-api/gd/output/epg/epg.xml",
        "active": True,
    },
]

CHANNEL_ALIASES = {
    "CCTV-1": ["cctv1", "cctv-1", "cctv 1", "央视一套", "中央电视台综合频道"],
    "CCTV-2": ["cctv2", "cctv-2", "cctv 2", "央视二套", "中央电视台财经频道"],
    "CCTV-3": ["cctv3", "cctv-3", "cctv 3", "央视三套", "中央电视台综艺频道"],
    "CCTV-4": ["cctv4", "cctv-4", "cctv 4", "cctv4中文国际", "中央电视台中文国际频道"],
    "CCTV-5": ["cctv5", "cctv-5", "cctv 5", "央视五套", "中央电视台体育频道"],
    "CCTV-5+": ["cctv5+", "cctv5plus", "cctv-5+", "央视五加"],
    "CCTV-6": ["cctv6", "cctv-6", "cctv 6", "央视六套", "中央电视台电影频道"],
    "CCTV-7": ["cctv7", "cctv-7", "cctv 7", "央视七套", "中央电视台国防军事频道"],
    "CCTV-8": ["cctv8", "cctv-8", "cctv 8", "央视八套", "中央电视台电视剧频道"],
    "CCTV-9": ["cctv9", "cctv-9", "cctv 9", "央视九套", "中央电视台纪录频道"],
    "CCTV-10": ["cctv10", "cctv-10", "cctv 10", "央视十套", "中央电视台科教频道"],
    "CCTV-11": ["cctv11", "cctv-11", "cctv 11", "央视十一套", "中央电视台戏曲频道"],
    "CCTV-12": ["cctv12", "cctv-12", "cctv 12", "央视十二套", "中央电视台社会与法频道"],
    "CCTV-13": ["cctv13", "cctv-13", "cctv 13", "央视十三套", "中央电视台新闻频道"],
    "CCTV-14": ["cctv14", "cctv-14", "cctv 14", "央视十四套", "中央电视台少儿频道"],
    "CCTV-15": ["cctv15", "cctv-15", "cctv 15", "央视十五套", "中央电视台音乐频道"],
    "CCTV-16": ["cctv16", "cctv-16", "cctv 16", "央视十六套", "中央电视台奥林匹克频道"],
    "CCTV-17": ["cctv17", "cctv-17", "cctv 17", "央视十七套", "中央电视台农业农村频道"],
}


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def now_cst():
    """当前时间（UTC+8）"""
    return datetime.now(timezone(timedelta(hours=8)))


def build_urls(raw_path):
    urls = [f"https://raw.githubusercontent.com/{raw_path}"]
    for p in PROXIES:
        urls.append(f"{p}https://raw.githubusercontent.com/{raw_path}")
    return urls


def fetch(url, timeout=90):
    """抓取 URL，默认超时 90 秒（iptv-org 的 gz 文件较大，需放宽超时）"""
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; EPG-Sync/2.0)",
            "Accept": "*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
    if url.endswith(".gz"):
        data = gzip.decompress(data)
    text = data.decode("utf-8", errors="replace")
    if len(text.strip()) < 200:
        raise ValueError(f"内容为空或过小({len(data)}字节)")
    return text


def parse_xmltv_time(raw):
    """解析 xmltv 时间格式 20260807001100 +0800 -> datetime"""
    if not raw:
        return None
    try:
        s = raw.strip()
        dt_str = s[:14]
        tz_str = s[15:].strip() if len(s) > 14 else "+0000"
        dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S")
        sign = 1 if tz_str[0] == '+' else -1
        hours = int(tz_str[1:3])
        minutes = int(tz_str[3:5]) if len(tz_str) >= 5 else 0
        tz = timezone(sign * timedelta(hours=hours, minutes=minutes))
        return dt.replace(tzinfo=tz)
    except Exception:
        return None


def is_fresh(start_raw, max_age_days=MAX_AGE_DAYS):
    """判断节目是否在有效时间窗口内（过去/未来 max_age_days 天内）"""
    dt = parse_xmltv_time(start_raw)
    if dt is None:
        return True  # 解析失败就保留，避免误杀
    now = now_cst()
    if dt.utcoffset() is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_cst = dt.astimezone(timezone(timedelta(hours=8)))
    delta = abs((now - dt_cst).total_seconds()) / 86400.0
    return delta <= max_age_days


def norm_channel(name):
    if not name:
        return ""
    name = name.strip()
    for k, aliases in CHANNEL_ALIASES.items():
        if name.lower() == k.lower():
            return k
        if name.lower() in [a.lower() for a in aliases]:
            return k
    return name


def parse_iptv_org_xml(text):
    """解析 iptv-org / 标准 xmltv 格式"""
    channels = {}
    root = ET.fromstring(text)

    channel_names = {}
    for ch in root.iter("channel"):
        cid = ch.get("id") or ""
        name = cid
        n = ch.find("display-name")
        if n is not None and n.text:
            name = n.text
        channel_names[cid] = name

    for prog in root.iter("programme"):
        cid = prog.get("channel") or ""
        if not cid:
            continue
        name = channel_names.get(cid, cid)
        norm = norm_channel(name)
        if not norm:
            continue

        start_raw = prog.get("start") or ""
        stop_raw = prog.get("stop") or ""

        # ── 过期过滤（核心）──
        if not is_fresh(start_raw):
            continue

        title_el = prog.find("title")
        title = ""
        if title_el is not None and title_el.text:
            title = title_el.text
        if not title:
            continue

        channels.setdefault(norm, [])
        channels[norm].append({
            "start_raw": start_raw,
            "stop_raw": stop_raw,
            "title": title,
        })

    return channels


def parse_guovin_xml(text):
    """Guovin 格式与 xmltv 标准相同，复用解析器"""
    return parse_iptv_org_xml(text)


def fetch_source(src):
    name = src["name"]
    parser = parse_iptv_org_xml if src["type"] == "iptv-org" else parse_guovin_xml
    tries = []
    for url in build_urls(src["raw"]):
        try:
            log(f"  ↳ 抓取 {name}: {url}")
            text = fetch(url)
            log(f"  ✓ {name}: 获取 {len(text)} 字符")
            channels = parser(text)
            c = len(channels)
            p = sum(len(v) for v in channels.values())
            log(f"  ✓ {name}: {c} 频道, {p} 条节目（已过滤过期）")
            return name, channels
        except Exception as e:
            tries.append((url, str(e)[:80]))
            log(f"  ✗ {name}: {str(e)[:80]}")
    raise RuntimeError(
        f"源 {name} 所有 URL 均失败: " + "; ".join(f"{u}→{e}" for u, e in tries)
    )


def merge_channels(all_channels_list):
    """多源合并：同频道去重，保留节目最多的那一份"""
    merged = {}
    for channels in all_channels_list:
        for ch, progs in channels.items():
            if ch not in merged or len(progs) > len(merged[ch]):
                merged[ch] = progs
    return merged


def load_cache():
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        log(f"✓ 已加载缓存: {len(data)} 频道")
        return data
    except Exception as e:
        log(f"⚠️ 缓存加载失败: {e}")
        return {}


def save_cache(channels):
    os.makedirs(CACHE_DIR, exist_ok=True)
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(channels, f, ensure_ascii=False)
        log(f"✓ 缓存已保存: {CACHE_FILE}")
    except Exception as e:
        log(f"⚠️ 缓存保存失败: {e}")


def write_epg(channels, out_path):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    total = sum(len(v) for v in channels.values())
    if total < 3:
        log("⚠️ 节目单为空(<3 频道)，跳过写入，保留昨日文件")
        return False

    tv = ET.Element("tv", {
        "generator-info-name": "epg_sync.py",
        "date": datetime.utcnow().strftime("%Y%m%d%H%M%S") + " +0000",
    })
    for ch_name in sorted(channels.keys()):
        progs = channels[ch_name]
        ch_el = ET.SubElement(tv, "channel", {"id": ch_name})
        dn = ET.SubElement(ch_el, "display-name")
        dn.text = ch_name
        for p in progs:
            attrs = {"channel": ch_name}
            if p.get("start_raw"):
                attrs["start"] = p["start_raw"]
            if p.get("stop_raw"):
                attrs["stop"] = p["stop_raw"]
            pe = ET.SubElement(tv, "programme", attrs)
            t = ET.SubElement(pe, "title")
            t.text = p["title"]

    tree = ET.ElementTree(tv)
    tree.write(out_path, encoding="utf-8", xml_declaration=True)
    log(f"✓ 已写入 {out_path}: {len(channels)} 频道, {total} 条节目")
    return True


def main():
    parser = argparse.ArgumentParser(description="EPG 节目单同步")
    parser.add_argument("--max-age", type=int, default=MAX_AGE_DAYS,
                        help=f"节目最大有效天数（默认 {MAX_AGE_DAYS}，超出视为过期丢弃）")
    parser.add_argument("--use-cache", action="store_true", help="强制使用缓存")
    parser.add_argument("--dry-run", action="store_true", help="只抓取不写文件")
    args = parser.parse_args()

    global MAX_AGE_DAYS
    MAX_AGE_DAYS = args.max_age

    log(f"EPG 同步开始  max_age={MAX_AGE_DAYS}天  use_cache={args.use_cache}")

    all_channels_list = []
    any_success = False

    if not args.use_cache:
        for src in EPG_SOURCES:
            if not src.get("active", True):
                log(f"  ⏭️  跳过 {src['name']}（未启用）")
                continue
            try:
                name, channels = fetch_source(src)
                any_success = True
                all_channels_list.append(channels)
            except Exception as e:
                log(f"  ✗ {e}")

    if not any_success or not all_channels_list:
        log("⚠️ 在线源未获取到数据，回退缓存")
        cached = load_cache()
        if cached:
            all_channels_list.append(cached)
        else:
            log("✗✗✗ 无可用节目数据，退出")
            sys.exit(1)

    merged = merge_channels(all_channels_list)
    save_cache(merged)

    if not args.dry_run:
        if not write_epg(merged, EPG_OUT):
            log("⚠️ 未写入（节目单为空）")
    else:
        total = sum(len(v) for v in merged.values())
        log(f"[dry-run] 共 {len(merged)} 频道, {total} 条节目, 不写文件")


if __name__ == "__main__":
    main()
