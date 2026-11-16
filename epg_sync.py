#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
epg_sync.py —— 节目单(EPG)同步脚本
===============================
功能：从公开 xmltv 源抓取节目单，筛选匹配本地频道的节目，输出标准 xmltv 格式的 epg.xml。

数据源（按优先级，取到第一个可用的即停止）：
  1. Guovin/iptv-api (gd 分支) output/epg/epg.xml   ← 主源，已验证可用，52 频道/3378 节目
     https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/epg/epg.xml
  2. 同主源经 gh-proxy 中转（国内访问 GitHub 不稳定时的兜底）
  3. 其他备选源（可按需追加）

设计要点：
  - 每个源提供多个 URL 写法（直连 / 多个代理中转），自动选用第一个返回 200 的
  - gzip 自动识别解压
  - 所有源都失败时，回退使用上次成功的缓存（避免节目单变空）
  - 频道匹配采用"本地频道名优先"策略，最大限度保留 CCTV/卫视节目

用法：
  python epg_sync.py                 # 默认保留 7 天，写 output/epg.xml
  python epg_sync.py --days 3       # 只保留未来 3 天
  python epg_sync.py --dry-run      # 抓取解析但不写文件
  python epg_sync.py --use-cache    # 强制允许回退缓存
"""

import argparse
import gzip
import io
import os
import re
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree as ET

# ---------------------------------------------------------------------------
# 配置区
# ---------------------------------------------------------------------------

# 输出路径（相对仓库根目录）
OUTPUT_PATH = os.environ.get("EPG_OUTPUT", "output/epg.xml")
CACHE_PATH = os.environ.get("EPG_CACHE", "output/cache/epg_last.xml")
DEFAULT_KEEP_DAYS = 7

# 主源：Guovin/iptv-api gd 分支，已验证可用
#   频道命名：CCTV-1 / CCTV-2 ... 干净格式，与 aggregator.py 天然对齐
#   注意：路径是 output/epg/epg.xml，不是 output/epg.xml
GUOVIN_RAW = "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/epg/epg.xml"

# 代理中转（国内访问 GitHub raw 不稳定时尝试）
PROXY_HOSTS = [
    "https://gh-proxy.com/",
    "https://gh-proxy.cc/",
    "https://gh.llkk.cc/",
    "https://gh-proxy.net/",
]

# 备选源：每个源是一组 URL，按顺序尝试
EPG_SOURCES = [
    {
        "name": "Guovin EPG (gd)",
        "urls": [GUOVIN_RAW] + [p + "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/epg/epg.xml" for p in PROXY_HOSTS],
    },
]

# 本地频道名 → 别名正则（用于把 xmltv 里的频道 ID 映射到本地频道名）
# Guovin 源的频道 ID 本身就是 "CCTV-1" 这种干净格式，匹配率极高，这里主要做兜底
CHANNEL_ALIASES = {
    "CCTV-1": [r"^cctv[-_]?1$", r"^cctv\s*1$"],
    "CCTV-2": [r"^cctv[-_]?2$"],
    "CCTV-3": [r"^cctv[-_]?3$"],
    "CCTV-4": [r"^cctv[-_]?4$"],
    "CCTV-5": [r"^cctv[-_]?5$"],
    "CCTV-5+": [r"^cctv[-_]?5\+$", r"^cctv[-_]?5plus$"],
    "CCTV-6": [r"^cctv[-_]?6$"],
    "CCTV-7": [r"^cctv[-_]?7$"],
    "CCTV-8": [r"^cctv[-_]?8$"],
    "CCTV-9": [r"^cctv[-_]?9$"],
    "CCTV-10": [r"^cctv[-_]?10$"],
    "CCTV-11": [r"^cctv[-_]?11$"],
    "CCTV-12": [r"^cctv[-_]?12$"],
    "CCTV-13": [r"^cctv[-_]?13$"],
    "CCTV-14": [r"^cctv[-_]?14$"],
    "CCTV-15": [r"^cctv[-_]?15$"],
    "CCTV-16": [r"^cctv[-_]?16$"],
    "CCTV-17": [r"^cctv[-_]?17$"],
    "CCTV-4K": [r"^cctv[-_]?4k$"],
    "CCTV-8K": [r"^cctv[-_]?8k$"],
    "CGTN": [r"^cgtn$"],
    "CGTN-Record": [r"^cgtn[-_]?record$"],
}

# 请求超时（秒）
REQUEST_TIMEOUT = 30
# 最大重试次数
MAX_RETRIES = 2
# 缓存有效期（小时）
CACHE_TTL_HOURS = 24

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------

_log_level = "INFO"


def log(level, msg):
    if level == "DEBUG" and _log_level != "DEBUG":
        return
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"{ts} [{level}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# 抓取
# ---------------------------------------------------------------------------

def fetch_url(url, timeout=REQUEST_TIMEOUT):
    """抓取单个 URL，返回 bytes；失败返回 None。自动处理 gzip。"""
    last_err = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; epg-sync/1.0)",
                    "Accept-Encoding": "gzip, deflate",
                    "Accept": "*/*",
                },
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
                if not data:
                    last_err = "empty body"
                    continue
                # 自动解压 gzip
                ctype = resp.headers.get("Content-Encoding", "").lower()
                ctype2 = resp.headers.get("content-type", "").lower()
                if ctype == "gzip" or url.endswith(".gz") or "gzip" in ctype2:
                    try:
                        data = gzip.decompress(data)
                    except OSError:
                        pass  # 已经是明文
                return data
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if e.code in (404, 403):
                return None  # 确定不存在，不再重试
        except Exception as e:
            last_err = str(e)[:60]
        if attempt < MAX_RETRIES:
            time.sleep(1)
    log("WARNING", f"  ✗ {last_err}")
    return None


def fetch_source(src):
    """尝试一个源的所有 URL 写法，返回第一个成功的 bytes。"""
    for url in src["urls"]:
        log("INFO", f"  ↳ 抓取 {src['name']}: {url}")
        data = fetch_url(url)
        if data:
            log("INFO", f"  ✓ {src['name']}: 获取 {len(data)} 字节")
            return data
    return None


def fetch_all():
    """按顺序尝试所有源，返回第一个成功的 (源名, bytes)。"""
    for src in EPG_SOURCES:
        data = fetch_source(src)
        if data:
            return src["name"], data
    return None, None


# ---------------------------------------------------------------------------
# 解析
# ---------------------------------------------------------------------------

def _norm_time(s):
    """把 xmltv 时间字符串解析为带时区的 datetime（统一转 UTC）。"""
    if not s:
        return None
    s = s.strip()
    # 格式：YYYYMMDDHHMMSS +0800  或  YYYYMMDDHHMMSS
    m = re.match(r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\s*([+-]\d{4})?$", s)
    if not m:
        # 兜底尝试宽松解析
        m2 = re.match(r"^(\d{4})-?(\d{2})-?(\d{2})[T ](\d{2}):?(\d{2}):?(\d{2})", s)
        if not m2:
            return None
        y, mo, d, h, mi, se = (int(x) for x in m2.groups())
        offset = timezone.utc
    else:
        y, mo, d, h, mi, se = (int(x) for x in m.groups()[:6])
        tz_str = m.group(7)
        if tz_str:
            sign = 1 if tz_str[0] == "+" else -1
            oh = int(tz_str[1:3])
            om = int(tz_str[3:5])
            offset = timezone(sign * timedelta(hours=oh, minutes=om))
        else:
            offset = timezone.utc
    try:
        return datetime(y, mo, d, h, mi, se, tzinfo=offset).astimezone(timezone.utc)
    except ValueError:
        return None


def parse_epg(data):
    """解析 xmltv 字节流，返回 (channels: dict, programmes: list)。"""
    if not data:
        return {}, []
    try:
        root = ET.fromstring(data.decode("utf-8", errors="ignore"))
    except ET.ParseError as e:
        log("WARNING", f"  XML 解析失败: {e}")
        return {}, []

    channels = {}
    for ch in root.iter("channel"):
        cid = ch.get("id", "").strip()
        if not cid:
            continue
        dnames = [dn.text for dn in ch.iter("display-name") if dn.text]
        channels[cid] = dnames[0] if dnames else cid

    programmes = []
    for prog in root.iter("programme"):
        chan = prog.get("channel", "").strip()
        start = _norm_time(prog.get("start"))
        stop = _norm_time(prog.get("stop"))
        titles = [t.text for t in prog.iter("title") if t.text]
        title = titles[0] if titles else ""
        programmes.append({
            "channel": chan,
            "start": start,
            "stop": stop,
            "title": title,
            "elem": prog,
        })
    return channels, programmes


# ---------------------------------------------------------------------------
# 频道匹配
# ---------------------------------------------------------------------------

def build_alias_map(channels):
    """为每个 xmltv 频道 ID 计算它最可能对应的本地频道名。
    返回 dict: xmltv_channel_id -> local_name，未匹配为 None。"""
    result = {}
    for cid in channels:
        cid_l = cid.lower()
        matched = None
        for local, patterns in CHANNEL_ALIASES.items():
            for pat in patterns:
                if re.match(pat, cid_l):
                    matched = local
                    break
            if matched:
                break
        result[cid] = matched
    return result


def filter_programmes(programmes, alias_map, keep_unmatched=True):
    """按时间窗口筛选节目，并保留匹配本地频道的部分。"""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=DEFAULT_KEEP_DAYS)
    # 用命令行参数覆盖
    args = getattr(filter_programmes, "_args", None)
    if args is not None:
        cutoff = now - timedelta(days=args.days)

    matched_progs = []
    kept_unmatched = 0
    dropped_old = 0
    for p in programmes:
        if p["stop"] and p["stop"] < cutoff:
            dropped_old += 1
            continue
        local = alias_map.get(p["channel"])
        if local:
            p["channel_local"] = local
            matched_progs.append(p)
        elif keep_unmatched:
            kept_unmatched += 1
            matched_progs.append(p)
    log("DEBUG", f"  时间筛选丢弃 {dropped_old} 条旧节目；保留未匹配 {kept_unmatched} 条")
    return matched_progs


# ---------------------------------------------------------------------------
# 输出
# ---------------------------------------------------------------------------

def build_output(programmes, channels, alias_map):
    """重建 xmltv 输出。返回 ElementTree 根节点。"""
    tv = ET.Element("tv")
    tv.set("date", datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S +0000"))
    tv.set("generator-info-name", "epg_sync.py")
    # 收集用到的频道，保持顺序
    used = []
    for p in programmes:
        local = p.get("channel_local") or p["channel"]
        if local not in used:
            used.append(local)

    for local in used:
        ch = ET.SubElement(tv, "channel")
        ch.set("id", local)
        dn = ET.SubElement(ch, "display-name")
        dn.set("lang", "zh")
        dn.text = local
        # 附加 icon（若有）
        icon_url = None
        for cid, lname in alias_map.items():
            if lname == local:
                icon_url = f"https://epg.112114.xyz/logo/{local}.png"
                break
        if icon_url:
            icon = ET.SubElement(ch, "icon")
            icon.set("src", icon_url)

    for p in programmes:
        local = p.get("channel_local") or p["channel"]
        prog = ET.SubElement(tv, "programme")
        prog.set("channel", local)
        if p["start"]:
            prog.set("start", p["start"].strftime("%Y%m%d%H%M%S %z"))
        if p["stop"]:
            prog.set("stop", p["stop"].strftime("%Y%m%d%H%M%S %z"))
        if p["title"]:
            t = ET.SubElement(prog, "title")
            t.set("lang", "zh")
            t.text = p["title"]
    return tv


def save_epg(tree, path):
    """序列化并写文件，同时更新缓存。"""
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    body = ET.tostring(tree, encoding="utf-8", xml_declaration=True)
    with open(path, "wb") as f:
        f.write(body)
    # 缓存
    try:
        os.makedirs(os.path.dirname(CACHE_PATH) if os.path.dirname(CACHE_PATH) else ".", exist_ok=True)
        with open(CACHE_PATH, "wb") as f:
            f.write(body)
    except Exception as e:
        log("WARNING", f"  写缓存失败: {e}")


def load_cache():
    """读取缓存，返回 (ElementTree, 是否新鲜)。"""
    if not os.path.exists(CACHE_PATH):
        return None, False
    try:
        mtime = os.path.getmtime(CACHE_PATH)
        age_h = (time.time() - mtime) / 3600
        fresh = age_h <= CACHE_TTL_HOURS
        tree = ET.parse(CACHE_PATH)
        return tree, fresh
    except Exception:
        return None, False


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="EPG 节目单同步")
    parser.add_argument("--days", type=int, default=DEFAULT_KEEP_DAYS,
                        help=f"保留天数（默认 {DEFAULT_KEEP_DAYS}）")
    parser.add_argument("--dry-run", action="store_true", help="只抓取解析，不写文件")
    parser.add_argument("--use-cache", action="store_true", help="允许回退使用本地缓存")
    parser.add_argument("--debug", action="store_true", help="输出调试日志")
    parser.add_argument("--keep-unmatched", action="store_true", default=True,
                        help="保留未匹配频道（默认开启）")
    parser.add_argument("--no-keep-unmatched", action="store_false", dest="keep_unmatched",
                        help="丢弃未匹配频道")
    args = parser.parse_args()
    filter_programmes._args = args

    global _log_level
    if args.debug:
        _log_level = "DEBUG"

    log("INFO", "=" * 60)
    log("INFO", "EPG 同步开始")
    log("INFO", f"  保留天数: {args.days} 天")
    log("INFO", f"  输出路径: {os.path.abspath(OUTPUT_PATH)}")
    log("INFO", "=" * 60)

    # 1. 抓取
    src_name, data = fetch_all()
    if not data:
        log("ERROR", "所有 EPG 源均不可用")
        if args.use_cache:
            tree, fresh = load_cache()
            if tree is not None:
                log("WARNING", f"  回退使用本地缓存（新鲜度: {'新鲜' if fresh else '已过期'}）")
                if not args.dry_run:
                    save_epg(tree.getroot(), OUTPUT_PATH)
                log("INFO", "  已使用缓存输出 epg.xml")
                return
        log("ERROR", "无可用源且无有效缓存，退出")
        sys.exit(1)

    log("INFO", f"源 {src_name} 抓取成功，开始解析")

    # 2. 解析
    channels, programmes = parse_epg(data)
    log("INFO", f"  解析完成: {len(channels)} 个频道, {len(programmes)} 条节目")

    # 3. 频道匹配
    alias_map = build_alias_map(channels)
    matched = sum(1 for v in alias_map.values() if v)
    log("INFO", f"  频道匹配: {matched}/{len(channels)} 个可映射到本地频道")

    # 4. 筛选
    kept = filter_programmes(programmes, alias_map, args.keep_unmatched)
    log("INFO", f"  筛选后保留: {len(kept)} 条节目")

    # 5. 输出
    tree = build_output(kept, channels, alias_map)
    if args.dry_run:
        log("INFO", "dry-run 模式，不写文件")
        return
    save_epg(tree, OUTPUT_PATH)
    size = os.path.getsize(OUTPUT_PATH) if os.path.exists(OUTPUT_PATH) else 0
    log("INFO", f"✓ 已写入 {OUTPUT_PATH} ({size} 字节)")


if __name__ == "__main__":
    main()
