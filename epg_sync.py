#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
epg_sync.py —— EPG 节目单同步脚本
================================================================================
从第三方公开 xmltv 源抓取节目单，输出为播放器通用的 XMLTV 格式（output/epg.xml）。

设计要点：
  1. 数据源全部为第三方公开 xmltv 源，**不调用央视接口**（央视接口对云 IP 返回 403）。
  2. 支持多源轮询 + 单次超时，到点即跳过，不阻塞。
  3. 只保留与本地频道列表匹配的节目单，丢弃无关频道，控制体积。
  4. 带本地缓存 + 过期回退，单源失败不影响整体。
  5. 纯标准库实现（urllib），不引入第三方依赖，GitHub Actions 可直接跑。

使用方法：
  python scripts/epg_sync.py                # 全量同步
  python scripts/epg_sync.py --days 3      # 只保留最近 3 天
  python scripts/epg_sync.py --dry-run     # 只抓取和解析，不写文件

配合 .github/workflows/sync-epg.yml 每日凌晨自动运行。
"""

import sys
import os
import re
import json
import gzip
import logging
import argparse
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree as ET

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

# 第三方公开 xmltv 源（按优先级排序，前几个失败会回退到后面的）
# 这些都是 GitHub 上公开维护的节目单，不含央视官方接口
EPG_SOURCES = [
    {
        "name": "Guovin EPG",
        "url": "https://gh-proxy.com/https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/epg.xml",
        "gzip": False,
    },
    {
        "name": "iptv-org EPG China",
        "url": "https://gh-proxy.com/https://raw.githubusercontent.com/iptv-org/epg/master/epgs/China/epg.xml",
        "gzip": False,
    },
    {
        "name": "iptv-org EPG HongKong",
        "url": "https://gh-proxy.com/https://raw.githubusercontent.com/iptv-org/epg/master/epgs/HongKong/epg.xml",
        "gzip": False,
    },
    {
        "name": "xmltv",
        "url": "https://iptv-org.github.io/epg/guides/zh.xml.gz",
        "gzip": True,
    },
]

# 单次请求超时（秒）
FETCH_TIMEOUT = 30

# 输出路径
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
EPG_OUTPUT = OUTPUT_DIR / "epg.xml"
EPG_CACHE_DIR = OUTPUT_DIR / "cache" / "epg"

# 频道匹配映射：把本地频道名 → xmltv 里的 channel id
# 左侧是你在播放器里看到的频道名（含别名），右侧是正则，用于匹配 xmltv 的 channel id
CHANNEL_ALIASES = {
    "CCTV1":     [r"cctv1?", r"cctv-?1", r"cctv\s*1", r"cctv\s*综合"],
    "CCTV2":     [r"cctv2?", r"cctv-?2", r"cctv\s*2", r"cctv\s*财经"],
    "CCTV3":     [r"cctv3?", r"cctv-?3", r"cctv\s*3", r"cctv\s*综艺"],
    "CCTV4":     [r"cctv4?", r"cctv-?4", r"cctv\s*4", r"cctv\s*中文国际", r"cctv4?\s*asia", r"cctv4?\s*国际"],
    "CCTV5":     [r"cctv5?", r"cctv-?5", r"cctv\s*5", r"cctv\s*体育"],
    "CCTV5+":    [r"cctv5\+", r"cctv5\s*plus", r"cctv\s*5\+", r"cctv\s*体育.?赛"],
    "CCTV6":     [r"cctv6?", r"cctv-?6", r"cctv\s*6", r"cctv\s*电影"],
    "CCTV7":     [r"cctv7?", r"cctv-?7", r"cctv\s*7", r"cctv\s*军农", r"cctv\s*国防", r"cctv\s*军事"],
    "CCTV8":     [r"cctv8?", r"cctv-?8", r"cctv\s*8", r"cctv\s*电视剧"],
    "CCTV9":     [r"cctv9?", r"cctv-?9", r"cctv\s*9", r"cctv\s*纪录"],
    "CCTV10":    [r"cctv10?", r"cctv-?10", r"cctv\s*10", r"cctv\s*科教"],
    "CCTV11":    [r"cctv11?", r"cctv-?11", r"cctv\s*11", r"cctv\s*戏曲"],
    "CCTV12":    [r"cctv12?", r"cctv-?12", r"cctv\s*12", r"cctv\s*社会与法"],
    "CCTV13":    [r"cctv13?", r"cctv-?13", r"cctv\s*13", r"cctv\s*新闻"],
    "CCTV14":    [r"cctv14?", r"cctv-?14", r"cctv\s*14", r"cctv\s*少儿"],
    "CCTV15":    [r"cctv15?", r"cctv-?15", r"cctv\s*15", r"cctv\s*音乐"],
    "CCTV16":    [r"cctv16?", r"cctv-?16", r"cctv\s*16", r"cctv\s*奥运", r"cctv\s*奥林匹克"],
    "CCTV17":    [r"cctv17?", r"cctv-?17", r"cctv\s*17", r"cctv\s*农业农村"],
    "CCTV4K":    [r"cctv\s*4[kk]", r"cctv-?4[kk]", r"cctv\s*4k"],
    "CCTV8K":    [r"cctv\s*8[kk]", r"cctv-?8[kk]", r"cctv\s*8k"],
    "CCTV-5+":   [r"cctv5\+", r"cctv5\s*plus"],
    "CGTN":      [r"cgtn"],
    "CGTN-EN":   [r"cgtn\s*en", r"cgtn\s*english"],
    "BTV北京卫视": [r"btv", r"北京卫视", r"beijing"],
    "东方卫视":   [r"东方卫视", r"dragon\s*tv", r"dongfang"],
    "湖南卫视":   [r"湖南卫视", r"hunan\s*tv", r"hntv"],
    "浙江卫视":   [r"浙江卫视", r"zhejiang\s*tv", r"zjtv"],
    "江苏卫视":   [r"江苏卫视", r"jiangsu\s*tv", r"jstv"],
    "广东卫视":   [r"广东卫视", r"guangdong\s*tv", r"gdtv"],
    "山东卫视":   [r"山东卫视", r"shandong\s*tv", r"sdtv"],
    "安徽卫视":   [r"安徽卫视", r"anhuiv", r"ahtv"],
    "深圳卫视":   [r"深圳卫视", r"shenzhen\s*tv", r"sztv"],
    "凤凰卫视中文台": [r"凤凰卫视", r"phoenix", r"fenghuang"],
}

# 是否包含未匹配到的频道（True=全量输出，False=只输出匹配频道）
KEEP_UNMATCHED = True

# 日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("epg_sync")


# ---------------------------------------------------------------------------
# 抓取
# ---------------------------------------------------------------------------

def fetch_epg(source: dict) -> str | None:
    """抓取单个 EPG 源，返回 XML 文本。失败返回 None。"""
    name = source["name"]
    url = source["url"]
    is_gzip = source.get("gzip", False)
    log.info(f"  ↳ 抓取 {name}: {url}")
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; EPG-Sync/1.0)",
                "Accept": "application/xml,text/xml,*/*",
            },
        )
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as resp:
            data = resp.read()
        if is_gzip:
            data = gzip.decompress(data)
        text = data.decode("utf-8", errors="ignore")
        if "<tv>" not in text[:2000]:
            log.warning(f"  ✗ {name}: 返回内容不像 xmltv（未找到 <tv> 根节点）")
            return None
        log.info(f"  ✓ {name}: 抓取成功，{len(text)/1024:.1f} KB")
        return text
    except urllib.error.URLError as e:
        log.warning(f"  ✗ {name}: 网络错误 - {e.reason if hasattr(e, 'reason') else e}")
    except gzip.BadGzipFile:
        log.warning(f"  ✗ {name}: gzip 解压失败")
    except Exception as e:
        log.warning(f"  ✗ {name}: {type(e).__name__}: {e}")
    return None


def fetch_first_available() -> tuple[str | None, str | None]:
    """按顺序尝试所有源，返回 (xml_text, source_name)。"""
    for src in EPG_SOURCES:
        text = fetch_epg(src)
        if text:
            return text, src["name"]
    return None, None


# ---------------------------------------------------------------------------
# 解析与筛选
# ---------------------------------------------------------------------------

def parse_xmltv(xml_text: str) -> tuple[list, dict]:
    """解析 xmltv，返回 (channels_list, programs_by_channel_id)。"""
    root = ET.fromstring(xml_text)
    channels = []
    programs = {}
    for ch in root.findall("channel"):
        cid = ch.get("id", "")
        display_names = [dn.text or "" for dn in ch.findall("display-name")]
        icon = ch.find("icon")
        icon_src = icon.get("src", "") if icon is not None else ""
        channels.append({
            "id": cid,
            "display_names": display_names,
            "icon": icon_src,
        })
    for prog in root.findall("programme"):
        start = prog.get("start", "")
        stop = prog.get("stop", "")
        cid = prog.get("channel", "")
        title_el = prog.find("title")
        title = title_el.text or "" if title_el is not None else ""
        desc_el = prog.find("desc")
        desc = desc_el.text or "" if desc_el is not None else ""
        category_el = prog.find("category")
        category = category_el.text or "" if category_el is not None else ""
        programs.setdefault(cid, []).append({
            "start": start,
            "stop": stop,
            "title": title,
            "desc": desc,
            "category": category,
        })
    return channels, programs


def channel_matches(channel_id: str, display_names: list) -> bool:
    """判断某个 xmltv channel 是否匹配我们关心的频道。"""
    haystack = " ".join([channel_id] + display_names).lower()
    for local_name, patterns in CHANNEL_ALIASES.items():
        for pat in patterns:
            if re.search(pat, haystack, re.IGNORECASE):
                return True
    return False


def filter_by_time(programs: dict, days: int) -> dict:
    """只保留最近 days 天内的节目（含今天及之后）。"""
    now = datetime.now(timezone.utc)
    cutoff_start = now - timedelta(hours=6)          # 多留 6 小时缓冲
    cutoff_end = now + timedelta(days=days)
    result = {}
    for cid, progs in programs.items():
        kept = []
        for p in progs:
            try:
                start_dt = _parse_xmltv_time(p["start"])
            except Exception:
                kept.append(p)
                continue
            if cutoff_start <= start_dt <= cutoff_end:
                kept.append(p)
        if kept:
            result[cid] = kept
    return result


def _parse_xmltv_time(s: str) -> datetime:
    """解析 xmltv 时间格式，如 20261116060000 +0800。"""
    m = re.match(r"^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\s*([+-]\d{4})?$", s.strip())
    if not m:
        raise ValueError(f"无法解析时间: {s}")
    y, mo, d, h, mi, se = (int(x) for x in m.groups()[:6])
    tz_str = m.group(7) or "+0000"
    sign = 1 if tz_str[0] == "+" else -1
    tz_h = int(tz_str[1:3]) * sign
    tz_m = int(tz_str[3:5]) * sign
    tz = timezone(timedelta(hours=tz_h, minutes=tz_m))
    return datetime(y, mo, d, h, mi, se, tzinfo=tz)


# ---------------------------------------------------------------------------
# 输出
# ---------------------------------------------------------------------------

def build_filtered_xmltv(
    channels: list,
    programs: dict,
    keep_unmatched: bool,
) -> ET.Element:
    """构造筛选后的 xmltv 树。"""
    tv = ET.Element("tv")
    tv.set("generator-info-name", "epg_sync.py")
    tv.set("generator-info-url", "https://github.com/")

    matched_ids = set()
    selected_channels = []

    if keep_unmatched:
        # 保留所有有节目的频道
        selected_channels = channels
        matched_ids = {c["id"] for c in channels}
    else:
        # 只保留匹配 CHANNEL_ALIASES 的频道
        for ch in channels:
            if channel_matches(ch["id"], ch["display_names"]):
                selected_channels.append(ch)
                matched_ids.add(ch["id"])

    # 只输出有节目的频道
    selected_channels = [c for c in selected_channels if c["id"] in programs]

    for ch in selected_channels:
        el = ET.SubElement(tv, "channel")
        el.set("id", ch["id"])
        for dn in ch["display_names"]:
            dn_el = ET.SubElement(el, "display-name")
            dn_el.text = dn
        if ch["icon"]:
            icon_el = ET.SubElement(el, "icon")
            icon_el.set("src", ch["icon"])

    prog_count = 0
    for cid, progs in programs.items():
        if cid not in matched_ids:
            continue
        for p in progs:
            el = ET.SubElement(tv, "programme")
            el.set("start", p["start"])
            el.set("stop", p["stop"])
            el.set("channel", cid)
            t_el = ET.SubElement(el, "title")
            t_el.set("lang", "zh")
            t_el.text = p["title"]
            if p.get("category"):
                c_el = ET.SubElement(el, "category")
                c_el.set("lang", "zh")
                c_el.text = p["category"]
            if p.get("desc"):
                d_el = ET.SubElement(el, "desc")
                d_el.set("lang", "zh")
                d_el.text = p["desc"]
            prog_count += 1

    log.info(
        f"  输出频道 {len(selected_channels)} 个，节目条目 {prog_count} 条"
    )
    return tv


def write_epg(tv: ET.Element, output_path: Path) -> None:
    """写入 epg.xml（UTF-8，带声明头）。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tree = ET.ElementTree(tv)
    # 手动写声明头，确保 UTF-8 无 BOM
    with open(output_path, "wb") as f:
        f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        tree.write(f, encoding="utf-8", xml_declaration=False)
    size_kb = output_path.stat().st_size / 1024
    log.info(f"  ✓ 已写入 {output_path} ({size_kb:.1f} KB)")


# ---------------------------------------------------------------------------
# 缓存
# ---------------------------------------------------------------------------

def save_cache(xml_text: str, source_name: str) -> None:
    EPG_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    meta = {"source": source_name, "fetched_at": datetime.utcnow().isoformat()}
    (EPG_CACHE_DIR / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False), encoding="utf-8"
    )
    (EPG_CACHE_DIR / "epg.xml").write_text(xml_text, encoding="utf-8")


def load_cache() -> str | None:
    p = EPG_CACHE_DIR / "epg.xml"
    if p.exists():
        return p.read_text(encoding="utf-8")
    return None


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="EPG 节目单同步脚本")
    parser.add_argument("--days", type=int, default=7, help="保留最近 N 天的节目单（默认 7）")
    parser.add_argument("--dry-run", action="store_true", help="只抓取解析，不写文件")
    parser.add_argument("--use-cache", action="store_true", help="允许使用本地缓存")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("EPG 同步开始")
    log.info(f"  保留天数: {args.days} 天")
    log.info(f"  输出路径: {EPG_OUTPUT}")
    log.info("=" * 60)

    xml_text, source_name = fetch_first_available()

    if not xml_text:
        if args.use_cache:
            xml_text = load_cache()
            if xml_text:
                log.warning("  所有源抓取失败，回退本地缓存")
                source_name = "cache"
        if not xml_text:
            log.error("  ✗✗✗ 所有 EPG 源均不可用，且无缓存，退出")
            sys.exit(1)

    log.info(f"  解析 xmltv（来源: {source_name}）...")
    channels, programs = parse_xmltv(xml_text)
    log.info(f"  原始频道 {len(channels)} 个，节目条目 {sum(len(v) for v in programs.values())} 条")

    # 按时间筛选
    programs = filter_by_time(programs, args.days)

    # 筛选频道
    tv = build_filtered_xmltv(channels, programs, keep_unmatched=KEEP_UNMATCHED)

    if args.dry_run:
        log.info("  [dry-run] 不写入文件")
        sys.exit(0)

    write_epg(tv, EPG_OUTPUT)
    save_cache(xml_text, source_name)

    log.info("=" * 60)
    log.info("EPG 同步完成 ✓")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
