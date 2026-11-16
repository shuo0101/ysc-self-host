#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EPG 节目单同步脚本
=================
从多个公开 xmltv 源抓取 EPG，解析后输出标准 XMLTV 格式的 epg.xml。

设计要点
--------
1. 多源轮询：EPG_SOURCES 里每个源都提供「多种 URL 写法」（含/不含 gh-proxy、
   直连 raw、.gz/非 .gz），按列表顺序取第一个能成功抓取的 URL 即用。
2. 自动 gzip 识别：URL 以 .gz 结尾时自动解压。
3. 内置频道别名匹配：把 xmltv 里五花八门的频道 ID 映射到本地频道名。
4. 缓存 + 过期回退：所有源都挂了时，自动回退上次成功的 output/epg.xml，
   避免节目单变空。

用法
----
    python scripts/epg_sync.py                # 全量同步，保留 7 天
    python scripts/epg_sync.py --days 3       # 只保留 3 天
    python scripts/epg_sync.py --dry-run      # 只抓取解析，不写文件
    python scripts/epg_sync.py --use-cache    # 强制允许回退本地缓存
"""
import argparse
import gzip
import logging
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

# ──────────────────────────────────────────────
# 日志
# ──────────────────────────────────────────────
LOG = logging.getLogger("epg_sync")
LOG.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
_h = logging.StreamHandler(sys.stdout)
_h.setFormatter(_fmt)
LOG.addHandler(_h)

# ──────────────────────────────────────────────
# 配置
# ──────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.environ.get("EPG_OUTPUT_DIR", os.path.join(PROJECT_ROOT, "output"))
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "epg.xml")
CACHE_DIR = os.environ.get("EPG_CACHE_DIR", os.path.join(OUTPUT_DIR, "cache"))
CACHE_TTL_HOURS = 24  # 缓存有效期（小时）

KEEP_UNMATCHED = os.environ.get("EPG_KEEP_UNMATCHED", "true").lower() == "true"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# gh-proxy 中转站列表（可替换 / 增减）。留空即只用直连。
GH_PROXIES = [
    "https://gh-proxy.com/",
    "https://gh-proxy.cc/",
    "https://gh.llkk.cc/",
    "https://gh-proxy.net/",
]

# 每个源提供多种 URL 写法，按顺序尝试，第一个成功即用。
# 写法说明：
#   raw://...      → 直连 raw.githubusercontent.com（不套代理）
#   gh://...       → 套 GH_PROXIES 前缀
#   .gz 结尾       → 自动解压
EPG_SOURCES = [
    {
        "name": "Guovin EPG",
        "urls": [
            # 注意：Guovin/iptv-api 的 output 目录不一定有 epg.xml，
            # 优先尝试 gd 分支，其次 main；都失败则跳过
            "gh://raw.githubusercontent.com/Guovin/iptv-api/gd/output/epg.xml",
            "gh://raw.githubusercontent.com/Guovin/iptv-api/main/output/epg.xml",
            "gh://raw.githubusercontent.com/Guovin/iptv-api/master/output/epg.xml",
            "raw://raw.githubusercontent.com/Guovin/iptv-api/gd/output/epg.xml",
        ],
    },
    {
        "name": "iptv-org EPG China",
        "urls": [
            # 注意路径：epgs/ 下是 .gz 压缩包，文件名带 .epg
            "gh://raw.githubusercontent.com/iptv-org/epg/master/epgs/China.epg.xml.gz",
            "gh://raw.githubusercontent.com/iptv-org/epg/master/epgs/China.epg.xml",
            "raw://raw.githubusercontent.com/iptv-org/epg/master/epgs/China.epg.xml.gz",
        ],
    },
    {
        "name": "iptv-org EPG HongKong",
        "urls": [
            "gh://raw.githubusercontent.com/iptv-org/epg/master/epgs/HongKong.epg.xml.gz",
            "gh://raw.githubusercontent.com/iptv-org/epg/master/epgs/HongKong.epg.xml",
            "raw://raw.githubusercontent.com/iptv-org/epg/master/epgs/HongKong.epg.xml.gz",
        ],
    },
    {
        "name": "iptv-org guides zh",
        "urls": [
            # guides/ 下（不是 epgs/），且是 .gz
            "gh://raw.githubusercontent.com/iptv-org/epg/master/guides/zh.xml.gz",
            "raw://raw.githubusercontent.com/iptv-org/epg/master/guides/zh.xml.gz",
        ],
    },
    {
        "name": "iptv-org web zh",
        "urls": [
            # github.io 页面上的静态资源，不一定存在，仅作兜底
            "https://iptv-org.github.io/epg/guides/zh.xml.gz",
            "https://iptv-org.github.io/epg/epg.xml.gz",
        ],
    },
]

# 频道别名：把 xmltv 里各种写法的频道 ID / display-name 映射到本地频道名
CHANNEL_ALIASES = {
    # CCTV 综合
    "CCTV-1":    [r"cctv[- ]?1", r"cctv\s*1\s*\(\s*综合", r"^cctv1$", r"cctv-1.*综合", r"cctv1hd"],
    "CCTV-2":    [r"cctv[- ]?2", r"cctv\s*2\s*财经", r"^cctv2$"],
    "CCTV-3":    [r"cctv[- ]?3", r"cctv\s*3\s*综艺", r"^cctv3$"],
    "CCTV-4":    [r"cctv[- ]?4", r"cctv\s*4\s*中文国际", r"^cctv4$"],
    "CCTV-5":    [r"cctv[- ]?5", r"cctv\s*5\s*体育", r"^cctv5$"],
    "CCTV-5+":   [r"cctv[- ]?5\+", r"cctv5\+", r"cctv[- ]?5\s*plus", r"cctv5\s*plus", r"cctv5p"],
    "CCTV-6":    [r"cctv[- ]?6", r"cctv\s*6\s*电影", r"^cctv6$"],
    "CCTV-7":    [r"cctv[- ]?7", r"cctv\s*7\s*国防军事", r"^cctv7$"],
    "CCTV-8":    [r"cctv[- ]?8", r"cctv\s*8\s*电视剧", r"^cctv8$"],
    "CCTV-9":    [r"cctv[- ]?9", r"cctv\s*9\s*纪录", r"^cctv9$"],
    "CCTV-10":   [r"cctv[- ]?10", r"cctv\s*10\s*科教", r"^cctv10$"],
    "CCTV-11":   [r"cctv[- ]?11", r"cctv\s*11\s*戏曲", r"^cctv11$"],
    "CCTV-12":   [r"cctv[- ]?12", r"cctv\s*12\s*社会与法", r"^cctv12$"],
    "CCTV-13":   [r"cctv[- ]?13", r"cctv\s*13\s*新闻", r"^cctv13$", r"cctv[- ]?news"],
    "CCTV-14":   [r"cctv[- ]?14", r"cctv\s*14\s*少儿", r"^cctv14$"],
    "CCTV-15":   [r"cctv[- ]?15", r"cctv\s*15\s*音乐", r"^cctv15$"],
    "CCTV-16":   [r"cctv[- ]?16", r"cctv\s*16\s*奥林匹克", r"^cctv16$"],
    "CCTV-17":   [r"cctv[- ]?17", r"cctv\s*17\s*农业农村", r"^cctv17$"],
    "CCTV-4K":   [r"cctv[- ]?4[kK]", r"cctv\s*4\s*[kK]", r"cctv4k"],
    "CCTV-8K":   [r"cctv[- ]?8[kK]", r"cctv8k"],
    # 卫视
    "湖南卫视":   [r"hunan\s*tv", r"hntv", r"湖南"],
    "浙江卫视":   [r"zhejiang\s*tv", r"zjtv", r"浙江"],
    "江苏卫视":   [r"jiangsu\s*tv", r"jstv", r"江苏"],
    "东方卫视":   [r"dongfang\s*tv", r"smg", r"东方"],
    "北京卫视":   [r"beijing\s*tv", r"btv", r"北京"],
    "广东卫视":   [r"guangdong\s*tv", r"gd\s*tv", r"广东"],
    "深圳卫视":   [r"shenzhen\s*tv", r"sz\s*tv", r"深圳"],
    "安徽卫视":   [r"anhui\s*tv", r"ah\s*tv", r"安徽"],
    "湖北卫视":   [r"hubei\s*tv", r"hb\s*tv", r"湖北"],
    "山东卫视":   [r"shandong\s*tv", r"sd\s*tv", r"山东"],
    "四川卫视":   [r"sichuan\s*tv", r"sc\s*tv", r"四川"],
    "辽宁卫视":   [r"liaoning\s*tv", r"ln\s*tv", r"辽宁"],
    "重庆卫视":   [r"chongqing\s*tv", r"cq\s*tv", r"重庆"],
    "河南卫视":   [r"henan\s*tv", r"hn\s*tv", r"河南"],
    "河北卫视":   [r"hebei\s*tv", r"hebtv", r"河北"],
    "天津卫视":   [r"tianjin\s*tv", r"tj\s*tv", r"天津"],
    "黑龙江卫视": [r"heilongjiang\s*tv", r"hlj\s*tv", r"黑龙江"],
    "吉林卫视":   [r"jilin\s*tv", r"jl\s*tv", r"吉林"],
    "江西卫视":   [r"jiangxi\s*tv", r"jx\s*tv", r"江西"],
    "广西卫视":   [r"guangxi\s*tv", r"gx\s*tv", r"广西"],
    "贵州卫视":   [r"guizhou\s*tv", r"gz\s*tv", r"贵州"],
    "云南卫视":   [r"yunnan\s*tv", r"yn\s*tv", r"云南"],
    "陕西卫视":   [r"shanxi?\d?\s*tv", r"sn\s*tv", r"陕西"],
    "甘肃卫视":   [r"gansu\s*tv", r"gs\s*tv", r"甘肃"],
    "新疆卫视":   [r"xinjiang\s*tv", r"xj\s*tv", r"新疆"],
    "青海卫视":   [r"qinghai\s*tv", r"qh\s*tv", r"青海"],
    "宁夏卫视":   [r"ningxia\s*tv", r"nx\s*tv", r"宁夏"],
    "海南卫视":   [r"hainan\s*tv", r"hn\s*tv", r"海南"],
    "内蒙古卫视": [r"neimenggu\s*tv", r"nmg\s*tv", r"内蒙古"],
    "西藏卫视":   [r"xizang\s*tv", r"xz\s*tv", r"西藏"],
    # 港澳台 / 国际
    "凤凰卫视中文台": [r"phoenix\s*(chinese|中文)"],
    "TVB 翡翠台":   [r"tvb\s*jade", r"tvb\s*翡翠"],
    "CGTN":        [r"cgtn"],
    "CGTN Documentary": [r"cgtn\s*doc"],
}


# ──────────────────────────────────────────────
# URL 解析
# ──────────────────────────────────────────────
def resolve_urls(spec):
    """把 'raw://' / 'gh://' 写法展开成真实 URL 列表。"""
    out = []
    if spec.startswith("raw://"):
        out.append(spec[len("raw://"):])
    elif spec.startswith("gh://"):
        tail = spec[len("gh://"):]
        for p in GH_PROXIES:
            out.append(p.rstrip("/") + "/" + tail)
        out.append(tail)  # 最后兜底直连
    else:
        out.append(spec)
    return out


# ──────────────────────────────────────────────
# 网络抓取
# ──────────────────────────────────────────────
def fetch(url, timeout=30):
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout) as r:
        data = r.read()
    if url.lower().endswith(".gz"):
        data = gzip.decompress(data)
    return data.decode("utf-8", errors="ignore")


def fetch_source(src):
    """尝试一个源的所有 URL 写法，返回 (成功url, 文本) 或 (None, None)。"""
    for spec in src["urls"]:
        for url in resolve_urls(spec):
            try:
                LOG.info("  ↳ 抓取 %s: %s", src["name"], url)
                text = fetch(url)
                if text and len(text) > 1000:
                    LOG.info("  ✓ %s 成功 (%d 字节)", src["name"], len(text))
                    return url, text
                LOG.warning("  ✗ %s: 响应过短 (%d 字节)", src["name"], len(text))
            except HTTPError as e:
                LOG.warning("  ✗ %s: HTTP %s", src["name"], e.code)
            except (URLError, TimeoutError) as e:
                LOG.warning("  ✗ %s: %s", src["name"], e.reason if hasattr(e, "reason") else e)
            except Exception as e:  # noqa
                LOG.warning("  ✗ %s: %s", src["name"], type(e).__name__)
    return None, None


# ──────────────────────────────────────────────
# XMLTV 解析
# ──────────────────────────────────────────────
def norm(s):
    return re.sub(r"\s+", "", s).lower()


def match_channel(display_name, channel_id):
    """把 xmltv 的频道标识映射到本地频道名，返回本地名或 None。"""
    candidates = []
    if display_name:
        candidates.append(display_name)
    if channel_id:
        candidates.append(channel_id)
    for local, patterns in CHANNEL_ALIASES.items():
        for c in candidates:
            cn = norm(c)
            for pat in patterns:
                if re.search(pat, cn, re.IGNORECASE):
                    return local
    return None


def parse_epg(xml_text, cutoff, keep_unmatched):
    """解析 xmltv，返回 (channels, programmes) 两个 dict。"""
    ns = {"tv": "http://www.xmltv.org/xmltv"}
    root = ET.fromstring(xml_text)
    channels = {}       # local_name -> channel element
    programmes = {}     # local_name -> [programme elements]

    for ch in root.findall("tv:channel", ns):
        cid = ch.get("id", "")
        dn = ch.find("tv:display-name", ns)
        name = dn.text if dn is not None and dn.text else cid
        matched = match_channel(name, cid)
        if matched:
            channels.setdefault(matched, ch)
        elif keep_unmatched:
            channels.setdefault(name, ch)

    for prog in root.findall("tv:programme", ns):
        start = prog.get("start", "")
        try:
            dt = datetime.strptime(start[:14], "%Y%m%d%H%M%S")
            dt = dt.replace(tzinfo=timezone.utc).astimezone()
        except ValueError:
            continue
        if dt < cutoff:
            continue
        cid = prog.get("channel", "")
        dn_el = None
        for ch in root.findall("tv:channel", ns):
            if ch.get("id") == cid:
                d = ch.find("tv:display-name", ns)
                dn_el = d.text if d is not None and d.text else cid
                break
        if dn_el is None:
            dn_el = cid
        matched = match_channel(dn_el, cid)
        if matched:
            programmes.setdefault(matched, []).append(prog)
        elif keep_unmatched:
            programmes.setdefault(dn_el, []).append(prog)

    return channels, programmes


# ──────────────────────────────────────────────
# 输出
# ──────────────────────────────────────────────
def build_xml(channels, programmes):
    tv = ET.Element("tv", attrib={"generator-info-name": "epg_sync.py", "source-info-name": "xmltv"})
    for name, ch in channels.items():
        c = ET.SubElement(tv, "channel", attrib={"id": name})
        d = ET.SubElement(c, "display-name", attrib={"lang": "zh"})
        d.text = name
        if ch is not channels[name]:
            pass
    for name, progs in programmes.items():
        for prog in progs:
            p = ET.SubElement(tv, "programme", attrib={
                "start": prog.get("start", ""),
                "stop": prog.get("stop", ""),
                "channel": name,
            })
            for t in prog.findall("title"):
                tt = ET.SubElement(p, "title", attrib={"lang": t.get("lang", "zh")})
                tt.text = t.text or ""
            for d in prog.findall("desc"):
                dd = ET.SubElement(p, "desc", attrib={"lang": d.get("lang", "zh")})
                dd.text = d.text or ""
            for c in prog.findall("category"):
                cc = ET.SubElement(p, "category", attrib={"lang": c.get("lang", "zh")})
                cc.text = c.text or ""
    return tv


def write_output(tv, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tree = ET.ElementTree(tv)
    tree.write(path, encoding="utf-8", xml_declaration=True)
    size = os.path.getsize(path)
    LOG.info("  ✓ 写入 %s (%d 字节, %d 频道, %d 节目)",
             path, size, len(tv.findall("channel")), len(tv.findall("programme")))


# ──────────────────────────────────────────────
# 缓存
# ──────────────────────────────────────────────
def cache_path():
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, "epg_last.xml")


def save_cache(xml_bytes):
    with open(cache_path(), "wb") as f:
        f.write(xml_bytes)


def load_cache():
    p = cache_path()
    if not os.path.exists(p):
        return None
    age = (datetime.now() - datetime.fromtimestamp(os.path.getmtime(p))).total_seconds() / 3600
    if age > CACHE_TTL_HOURS:
        LOG.info("  ⚠ 缓存已过期 (%.1f 小时前)", age)
        return None
    LOG.info("  📦 使用本地缓存 (%.1f 小时前)", age)
    with open(p, "rb") as f:
        return f.read().decode("utf-8", errors="ignore")


# ──────────────────────────────────────────────
# 主流程
# ──────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="EPG 节目单同步脚本")
    ap.add_argument("--days", type=int, default=7, help="保留天数（默认 7）")
    ap.add_argument("--dry-run", action="store_true", help="只抓取解析，不写文件")
    ap.add_argument("--use-cache", action="store_true", help="允许回退本地缓存")
    ap.add_argument("--keep-unmatched", action="store_true",
                    default=KEEP_UNMATCHED, help="保留未匹配的频道")
    args = ap.parse_args()

    cutoff = datetime.now(timezone(tz=timedelta(hours=8))) - timedelta(days=1)

    LOG.info("=" * 60)
    LOG.info("EPG 同步开始")
    LOG.info("  保留天数: %d 天", args.days)
    LOG.info("  输出路径: %s", OUTPUT_FILE)
    LOG.info("=" * 60)

    raw = None
    for src in EPG_SOURCES:
        url, text = fetch_source(src)
        if text:
            raw = text
            break

    if not raw:
        LOG.error("✗✗✗ 所有 EPG 源均不可用")
        if args.use_cache:
            cached = load_cache()
            if cached:
                if not args.dry_run:
                    write_output(build_xml(*parse_epg(cached, cutoff, args.keep_unmatched)), OUTPUT_FILE)
                sys.exit(0)
        LOG.error("无可用源且无有效缓存，退出")
        sys.exit(1)

    channels, programmes = parse_epg(raw, cutoff, args.keep_unmatched)
    if not channels:
        LOG.error("✗✗✗ 解析结果为空（可能是格式不兼容或内容为空）")
        sys.exit(1)

    save_cache(raw.encode("utf-8"))
    tv = build_xml(channels, programmes)
    if not args.dry_run:
        write_output(tv, OUTPUT_FILE)
    LOG.info("EPG 同步完成 ✓")


if __name__ == "__main__":
    main()
