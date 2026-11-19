#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG 节目单同步脚本
- 抓取 Guovin EPG，合并去重后写入 output/epg.xml
- 不做时间过滤，全部节目原样保留（先跑通链路）
- 抓取失败打印每个 URL 的具体原因
"""
import os
import sys
import gzip
import json
import argparse
import urllib.request
import urllib.error
from datetime import datetime
from xml.etree import ElementTree as ET

OUTPUT_DIR = "output"
CACHE_DIR = os.path.join(OUTPUT_DIR, "cache")
EPG_XML_OUT = os.path.join(OUTPUT_DIR, "epg.xml")
EPG_JSON_OUT = os.path.join(OUTPUT_DIR, "epg.json")
CACHE_FILE = os.path.join(CACHE_DIR, "epg_last.json")

PROXIES = [
    "https://gh-proxy.com/",
    "https://gh-proxy.cc/",
    "https://gh-proxy.cn/",
    "https://gh.llkk.cc/",
    "https://hub.gitmirror.com/",
]

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


def build_urls(raw_path):
    urls = [f"https://raw.githubusercontent.com/{raw_path}"]
    for p in PROXIES:
        urls.append(f"{p}https://raw.githubusercontent.com/{raw_path}")
    return urls


def fetch(url, timeout=20):
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


def parse_guovin_xml(text):
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
            title = title_el.text
        start_raw = prog.get("start") or ""
        stop_raw = prog.get("stop") or ""
        if not title:
            continue
        channels.setdefault(norm, [])
        channels[norm].append({
            "start_raw": start_raw,
            "stop_raw": stop_raw,
            "title": title,
        })

    return channels


def fetch_source(src):
    name = src["name"]
    tries = []
    for url in build_urls(src["raw"]):
        try:
            log(f"  ↳ 抓取 {name}: {url}")
            text = fetch(url)
            log(f"  ✓ {name}: 获取 {len(text)} 字符")
            return name, parse_guovin_xml(text)
        except Exception as e:
            tries.append((url, str(e)[:80]))
            log(f"  ✗ {name}: {str(e)[:80]}")
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


def write_epg_json(channels, out_path):
    """生成 JSON 格式 EPG（与 epg.xml 内容一致，供需 JSON 的场景使用）"""
    total = sum(len(v) for v in channels.values())
    payload = {"channels": []}
    for ch_name in sorted(channels.keys()):
        progs = channels[ch_name]
        payload["channels"].append({
            "id": ch_name,
            "name": ch_name,
            "programs": [
                {"start": p.get("start_raw", ""),
                 "stop": p.get("stop_raw", ""),
                 "title": p["title"]}
                for p in progs
            ],
        })
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log(f"✓ 已写入 {out_path}: {len(channels)} 频道, {total} 条节目")
    return True


def main():
    parser = argparse.ArgumentParser(description="EPG 节目单同步")
    parser.add_argument("--days", type=int, default=7, help="保留天数(本版本未启用过滤)")
    parser.add_argument("--use-cache", action="store_true", help="强制使用缓存")
    parser.add_argument("--dry-run", action="store_true", help="只抓取不写文件")
    args = parser.parse_args()

    log(f"EPG 同步开始  use_cache={args.use_cache}  (不做时间过滤)")

    merged = {}
    any_success = False

    if not args.use_cache:
        for src in EPG_SOURCES:
            try:
                name, channels = fetch_source(src)
                any_success = True
                c = len(channels)
                p = sum(len(v) for v in channels.values())
                log(f"  {name}: {c} 频道, {p} 条节目")
                for ch, progs in channels.items():
                    merged[ch] = progs
            except Exception as e:
                log(f"  ✗ {e}")

    if not any_success or not merged:
        log("⚠️ 在线源未获取到数据，回退缓存")
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
