#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG 节目单同步脚本
- 抓取多来源 EPG（Guovin + iptv-org 全量），合并去重后写入 output/epg.xml 与 epg.json
- 时间统一归一化为北京时间 'YYYYMMDDHHMMSS +0800'
- 过滤已结束超过 EXPIRE_GRACE_HOURS 小时的过期节目
- 无有效节目的频道整条移除
"""
import os
import sys
import gzip
import json
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET

OUTPUT_DIR = "output"
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
EPG_XML_OUT = os.path.join(OUTPUT_DIR, "epg.xml")
EPG_JSON_OUT = os.path.join(OUTPUT_DIR, "epg.json")
CACHE_FILE = os.path.join(CACHE_DIR, "epg_last.json")

# 过期容差：节目结束时间早于 (现在 - 此值) 则丢弃
EXPIRE_GRACE_HOURS = 6
# 未来最多保留多少天（iptv-org 全量源含 7 天，超过的裁剪）
FUTURE_KEEP_DAYS = 3

PROXIES = [
    "https://gh-proxy.com/",
    "https://gh-proxy.cc/",
    "https://gh-proxy.cn/",
    "https://gh.llkk.cc/",
    "https://hub.gitmirror.com/",
]

# ── 来源定义 ────────────────────────────────────────────────
# type: guovin / iptvorg_xmltv / iptvorg_guide
EPG_SOURCES = [
    {
        "name": "Guovin EPG (gd)",
        "type": "guovin",
        "raw": "Guovin/iptv-api/gd/output/epg/epg.xml",
        "db": "Guovin/iptv-api/gd/output/result.m3u",
    },
    {
        "name": "Guovin EPG (master)",
        "type": "guovin",
        "raw": "Guovin/iptv-api/master/output/epg/epg.xml",
        "db": "Guovin/iptv-api/master/output/result.m3u",
    },
    {
        "name": "iptv-org epg.xml.gz (全量)",
        "type": "iptvorg_xmltv",
        "raw": "iptv-org/epg/master/epg.xml.gz",
    },
    {
        "name": "iptv-org guides/zh.xml.gz (中文)",
        "type": "iptvorg_guide",
        "raw": "iptv-org/epg/master/guides/zh.xml.gz",
    },
]

# ── 频道归一化 ──────────────────────────────────────────────
CHANNEL_ALIASES = {
    "CCTV-1": ["cctv1", "cctv-1", "cctv 1", "央视一套", "中央电视台综合频道"],
    "CCTV-2": ["cctv2", "cctv-2", "cctv 2", "央视二套", "中央电视台财经频道"],
    "CCTV-3": ["cctv3", "cctv-3", "cctv 3", "央视三套", "中央电视台综艺频道"],
    "CCTV-4": ["cctv4", "cctv-4", "cctv 4", "cctv4中文国际", "中央电视台中文国际频道"],
    "CCTV-5": ["cctv5", "cctv-5", "cctv 5", "央视五套", "中央电视台体育频道"],
    "CCTV-5+": ["cctv5+", "cctv5plus", "cctv-5+", "cctv-5+体育赛事", "cctv5+体育赛事", "cctv5+赛事", "央视五加", "央视体育赛事"],
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
    "CCTV4K": ["cctv4k", "cctv-4k", "cctv4 4k", "cctv-4k超高清", "cctv4k超高清", "cctv4 4k超高清", "中央4k超高清"],
    "CCTV8K": ["cctv8k", "cctv-8k", "cctv8 8k", "cctv-8k超高清", "cctv8k超高清", "cctv8 8k超高清", "中央8k超高清"],
}


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def beijing_now():
    """当前北京时间"""
    return datetime.utcnow() + timedelta(hours=8)


def parse_epg_time(raw):
    """把各种 XMLTV 时间写法解析为 datetime（北京时间）"""
    if not raw:
        return None
    s = raw.strip()
    m = None
    try:
        m = re_match(r'^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\s*([+-]\d{4})?', s)
    except Exception:
        pass
    if not m:
        s2 = s.replace("T", "").replace("Z", "+0000")
        m = re_match(r'^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\s*([+-]\d{4})?', s2)
    if not m:
        return None
    try:
        naive = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)),
                         int(m.group(4)), int(m.group(5)), int(m.group(6)))
    except Exception:
        return None
    tz = m.group(7)
    if tz and tz != "+0000":
        sign = 1 if tz[0] == "+" else -1
        oh, om = int(tz[1:3]), int(tz[3:5])
        naive = naive - timedelta(hours=sign * oh, minutes=sign * om)
    else:
        # 无时区视为 UTC
        pass
    # 转北京时间
    return naive + timedelta(hours=8)


def re_match(pattern, s):
    import re
    return re.match(pattern, s)


def fmt_bj(dt):
    """datetime → 'YYYYMMDDHHMMSS +0800'"""
    return dt.strftime("%Y%m%d%H%M%S") + " +0800"


def norm_channel(name):
    if not name:
        return ""
    name = name.strip()
    lower = name.lower()
    # 1) 精确匹配 key 或别名
    for k, aliases in CHANNEL_ALIASES.items():
        if lower == k.lower():
            return k
        if lower in [a.lower() for a in aliases]:
            return k
    # 2) 包含匹配：优先匹配更长的别名，避免 'CCTV-5+体育赛事' 被漏掉
    best = None
    for k, aliases in CHANNEL_ALIASES.items():
        for a in aliases:
            if len(a) > 2 and a.lower() in lower:
                if best is None or len(a) > len(best[1]):
                    best = (k, a)
    if best:
        return best[0]
    return name


# ── 抓取 ────────────────────────────────────────────────────
def build_urls(raw_path):
    urls = [f"https://raw.githubusercontent.com/{raw_path}"]
    for p in PROXIES:
        urls.append(f"{p}https://raw.githubusercontent.com/{raw_path}")
    return urls


def fetch(url, timeout=25):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; EPG-Sync/1.0)",
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


def fetch_source(src):
    """抓取一个源，返回 (name, type, channels_dict)
    channels_dict: {norm_name: [{start, stop, title}, ...]}
    """
    name = src["name"]
    stype = src["type"]
    tries = []
    for url in build_urls(src["raw"]):
        try:
            log(f"  ↳ 抓取 {name}: {url}")
            text = fetch(url)
            log(f"  ✓ {name}: 获取 {len(text)} 字符")
            if stype == "guovin":
                return name, stype, parse_guovin_xml(text)
            elif stype == "iptvorg_xmltv":
                return name, stype, parse_iptvorg_xmltv(text)
            elif stype == "iptvorg_guide":
                return name, stype, parse_iptvorg_guide(text)
        except Exception as e:
            tries.append((url, str(e)[:80]))
            log(f"  ✗ {name}: {str(e)[:80]}")
    if src.get("db"):
        for url in build_urls(src["db"]):
            try:
                log(f"  ↳ 兜底 {name} DB: {url}")
                fetch(url)
                log(f"  ✓ {name} (DB): 已获取(暂不解析)")
            except Exception as e:
                tries.append((url, str(e)[:80]))
    raise RuntimeError(
        f"源 {name} 所有 URL 均失败: " + "; ".join(f"{u}→{e}" for u, e in tries)
    )


# ── 解析器 ──────────────────────────────────────────────────
def parse_guovin_xml(text):
    """Guovin 格式：与 iptv-org XMLTV 结构一致"""
    return _parse_xmltv(text)


def parse_iptvorg_xmltv(text):
    """iptv-org epg.xml.gz 标准 XMLTV"""
    return _parse_xmltv(text)


def parse_iptvorg_guide(text):
    """iptv-org guides/zh.xml.gz 中文节目单"""
    return _parse_xmltv(text)


def _parse_xmltv(text):
    """通用 XMLTV 解析：按 channel display-name 归一化，节目按 start/stop/title 提取"""
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
        title_el = prog.find("title")
        title = ""
        if title_el is not None and title_el.text:
            title = title_el.text.strip()
        start_raw = prog.get("start") or ""
        stop_raw = prog.get("stop") or ""
        if not title:
            continue

        start_dt = parse_epg_time(start_raw)
        stop_dt = parse_epg_time(stop_raw)
        start_bj = fmt_bj(start_dt) if start_dt else ""
        stop_bj = fmt_bj(stop_dt) if stop_dt else ""

        channels.setdefault(norm, [])
        channels[norm].append({
            "start": start_bj,
            "stop": stop_bj,
            "title": title,
        })

    return channels


# ── 时间过滤 ────────────────────────────────────────────────
def filter_valid(channels):
    """
    1. 丢弃已结束超过 EXPIRE_GRACE_HOURS 小时的节目
    2. 丢弃结束时间超出未来 FUTURE_KEEP_DAYS 的节目
    3. 移除已无任何有效节目的频道
    返回新 dict
    """
    now = beijing_now()
    expire_before = now - timedelta(hours=EXPIRE_GRACE_HOURS)
    future_cutoff = now + timedelta(days=FUTURE_KEEP_DAYS)

    kept_total = 0
    dropped_expired = 0
    dropped_future = 0
    removed_channels = 0

    result = {}
    for ch, progs in channels.items():
        kept = []
        for p in progs:
            stop_dt = parse_epg_time(p.get("stop", ""))
            start_dt = parse_epg_time(p.get("start", ""))

            # 过期判定：有结束时间且早于容差线
            if stop_dt and stop_dt < expire_before:
                dropped_expired += 1
                continue
            # 未来过远：有开始时间且超出保留窗口
            if start_dt and start_dt > future_cutoff:
                dropped_future += 1
                continue
            kept.append(p)
        if kept:
            result[ch] = kept
            kept_total += len(kept)
        else:
            removed_channels += 1

    log(f"🕒 时间过滤: 保留 {kept_total} 条, "
        f"丢弃过期 {dropped_expired} 条, 丢弃过远未来 {dropped_future} 条, "
        f"移除空频道 {removed_channels} 个")
    return result


# ── 合并去重 ────────────────────────────────────────────────
def merge_all(fetched_list):
    """多源合并，按 start 排序，同频道同时间段去重"""
    merged = {}
    seen = set()
    for name, stype, channels in fetched_list:
        for ch, progs in channels.items():
            for p in progs:
                key = (ch, p.get("start", ""), p.get("title", ""))
                if key in seen:
                    continue
                seen.add(key)
                merged.setdefault(ch, [])
                merged[ch].append(p)

    # 每个频道按开始时间排序
    for ch in merged:
        merged[ch].sort(key=lambda x: x.get("start", ""))
    return merged


# ── 缓存 ────────────────────────────────────────────────────
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


# ── 写入 ─────────────────────────────────────────────────────
def write_epg(channels, out_path):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    total = sum(len(v) for v in channels.values())
    if total < 3:
        log("⚠️ 节目单为空(<3 频道)，跳过写入，保留昨日文件")
        return False

    now_bj = beijing_now().strftime("%Y%m%d%H%M%S") + " +0800"
    tv = ET.Element("tv", {
        "generator-info-name": "epg_sync.py",
        "date": now_bj,
    })
    for ch_name in sorted(channels.keys()):
        progs = channels[ch_name]
        ch_el = ET.SubElement(tv, "channel", {"id": ch_name})
        dn = ET.SubElement(ch_el, "display-name")
        dn.text = ch_name
        for p in progs:
            attrs = {"channel": ch_name}
            if p.get("start"):
                attrs["start"] = p["start"]
            if p.get("stop"):
                attrs["stop"] = p["stop"]
            pe = ET.SubElement(tv, "programme", attrs)
            t = ET.SubElement(pe, "title")
            t.text = p["title"]

    tree = ET.ElementTree(tv)
    tree.write(out_path, encoding="utf-8", xml_declaration=True)
    log(f"✓ 已写入 {out_path}: {len(channels)} 频道, {total} 条节目")
    return True


def write_epg_json(channels, out_path):
    total = sum(len(v) for v in channels.values())
    payload = {"channels": []}
    for ch_name in sorted(channels.keys()):
        progs = channels[ch_name]
        payload["channels"].append({
            "id": ch_name,
            "name": ch_name,
            "programs": [
                {"start": p.get("start", ""),
                 "stop": p.get("stop", ""),
                 "title": p["title"]}
                for p in progs
            ],
        })
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log(f"✓ 已写入 {out_path}: {len(channels)} 频道, {total} 条节目")
    return True


# ── 入口 ────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="EPG 节目单同步")
    parser.add_argument("--days", type=int, default=FUTURE_KEEP_DAYS,
                        help=f"未来保留天数(默认{FUTURE_KEEP_DAYS})")
    parser.add_argument("--use-cache", action="store_true", help="强制使用缓存")
    parser.add_argument("--dry-run", action="store_true", help="只抓取不写文件")
    args = parser.parse_args()

    if args.days != FUTURE_KEEP_DAYS:
        # 通过模块属性修改，避免 global 声明顺序问题
        globals()['FUTURE_KEEP_DAYS'] = args.days

    log(f"EPG 同步开始  过期容差={EXPIRE_GRACE_HOURS}h  未来保留={FUTURE_KEEP_DAYS}天")

    fetched_list = []
    any_success = False

    if not args.use_cache:
        for src in EPG_SOURCES:
            try:
                name, stype, channels = fetch_source(src)
                any_success = True
                c = len(channels)
                p = sum(len(v) for v in channels.values())
                log(f"  {name}: {c} 频道, {p} 条节目")
                fetched_list.append((name, stype, channels))
            except Exception as e:
                log(f"  ✗ {e}")

    if not any_success or not fetched_list:
        log("⚠️ 在线源未获取到数据，回退缓存")
        merged = load_cache()
    else:
        merged = merge_all(fetched_list)

    if not merged:
        log("✗✗✗ 无可用节目数据，退出")
        sys.exit(1)

    # 时间过滤（缓存回退的数据也过滤，避免过期内容复活）
    merged = filter_valid(merged)

    if not merged:
        log("⚠️ 过滤后无有效节目，回退昨日缓存")
        merged = load_cache()
        if not merged:
            log("✗✗✗ 无可用节目数据，退出")
            sys.exit(1)

    save_cache(merged)

    if not args.dry_run:
        if not write_epg(merged, EPG_XML_OUT):
            log("⚠️ XML 未写入（节目单为空）")
        else:
            write_epg_json(merged, EPG_JSON_OUT)
    else:
        log(f"[dry-run] 共 {len(merged)} 频道, 不写文件")


if __name__ == "__main__":
    main()
