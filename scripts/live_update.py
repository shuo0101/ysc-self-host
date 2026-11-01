#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直播源自动更新：抓取 -> 探测测速 -> 去重 -> 4K权重排序 -> 输出带分组和台标的 m3u
"""
import json
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    HAVE_REQUESTS = True
except ImportError:
    HAVE_REQUESTS = False

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONF = os.path.join(ROOT, "config", "sources.json")
OUT = os.path.join(ROOT, "output")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 11; TV) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Accept": "*/*",
}

# 4K/高清权重：排序时优先保留清晰度高的线路
QUALITY_WEIGHT = [
    ("4K", 100), ("2160", 95), ("UHD", 90), ("超清", 85), ("蓝光", 80),
    ("1080", 70), ("蓝光", 65), ("高清", 50), ("HD", 50),
    ("720", 40), ("标清", 25), ("SD", 25),
]

# 分组优先级（越小越靠前）
GROUP_ORDER = ["央视频道", "卫视高清", "地方频道", "影视轮播", "体育赛事",
               "少儿动画", "纪录片", "4K专区", "网络频道", "其他频道"]


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def load_config():
    with open(CONF, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_text(url, timeout=15):
    if HAVE_REQUESTS:
        r = requests.get(url, timeout=timeout, headers=HEADERS)
        r.raise_for_status()
        # 兼容各种编码
        if r.encoding and r.encoding.lower() == "iso-8859-1":
            r.encoding = r.apparent_encoding or "utf-8"
        return r.text
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    for enc in ("utf-8", "gbk", "gb2312"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")


def parse_m3u(text):
    """解析 m3u，返回 [(name, url, group, logo, extra)]"""
    items = []
    group = "其他频道"
    name = "未知频道"
    extra = {}
    for line in text.splitlines():
        line = line.rstrip()
        if not line or line.startswith("#EXTM3U"):
            continue
        if line.startswith("#EXTINF"):
            extra = {}
            gm = re.search(r'group-title="([^"]*)"', line)
            if gm:
                group = gm.group(1).strip()
            nm = re.search(r'tvg-logo="([^"]*)"', line)
            if nm:
                extra["logo"] = nm.group(1).strip()
            tm = re.search(r'tvg-name="([^"]*)"', line)
            if tm:
                extra["tvg_name"] = tm.group(1).strip()
            # 频道名在最后一个逗号后
            cm = re.search(r",(.+)$", line)
            name = cm.group(1).strip() if cm else "未知频道"
            continue
        if line.startswith("#"):
            # 其他指令行（如 #EXTGRP）作为分组
            gm = re.search(r'#EXTGRP:(.+)', line)
            if gm:
                group = gm.group(1).strip()
            continue
        # 是 URL
        url = line.strip()
        if not url:
            continue
        items.append({
            "name": name, "url": url, "group": group,
            "logo": extra.get("logo", ""), "tvg_name": extra.get("tvg_name", name),
        })
        name = "未知频道"
        group = "其他频道"
        extra = {}
    return items


def parse_txt(text):
    """解析 txt 格式：频道名,url 或 频道名$url 每行一条"""
    items = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        sep = None
        for s in [",", "$", "#"]:
            if s in line:
                sep = s
                break
        if not sep:
            continue
        parts = line.split(sep, 1)
        name = parts[0].strip()
        url = parts[1].strip()
        if not name or not url.startswith("http"):
            continue
        items.append({"name": name, "url": url, "group": "其他频道",
                      "logo": "", "tvg_name": name})
    return items


def quality_score(name):
    """根据频道名/线路名计算清晰度权重"""
    s = 0
    up = name.upper()
    for kw, w in QUALITY_WEIGHT:
        if kw.upper() in up:
            s = max(s, w)
    return s


def is_valid_scheme(url):
    return url.startswith("http://") or url.startswith("https://")


def probe(item):
    """探测地址是否可访问，返回 (item, ok, elapsed)"""
    url = item["url"]
    if not is_valid_scheme(url):
        return (item, False, 0)
    start = time.time()
    try:
        if HAVE_REQUESTS:
            r = requests.get(url, timeout=6, headers=HEADERS, stream=True,
                             allow_redirects=True)
            elapsed = (time.time() - start) * 1000
            if r.status_code == 200:
                chunk = next(r.iter_content(4096), b"")
                if len(chunk) > 200:
                    return (item, True, elapsed)
            return (item, False, elapsed)
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=6) as resp:
            chunk = resp.read(1024)
            elapsed = (time.time() - start) * 1000
            return (item, len(chunk) > 100, elapsed)
    except Exception:
        elapsed = (time.time() - start) * 1000
        return (item, False, elapsed)


# CCTV 子频道细分映射：先按频道号/关键词判断
_CCTV_SUB = {
    "4K": "4K专区", "8K": "4K专区",
    "5": "体育赛事", "5+": "体育赛事", "体育": "体育赛事",
    "14": "少儿动画", "少儿": "少儿动画",
    "9": "纪录片", "纪录": "纪录片",
    "6": "影视轮播", "怀旧": "影视轮播",
    "11": "少儿动画",
    "15": "纪录片",
}


def classify_group(name):
    """根据频道名归类"""
    n = name
    up = n.upper()

    # 1. CCTV 子频道：按频道号细分
    m = re.search(r'CCTV\s*-?\s*(\d{1,2}\+?)', up)
    if m:
        num = m.group(1)
        # CCTV5+ 也算体育
        if num in ("5+", "5"):
            return "体育赛事"
        if num in _CCTV_SUB:
            return _CCTV_SUB[num]
        if num == "5":
            return "体育赛事"
        return "央视频道"

    if "CCTV" in up or "中央" in n or "央视" in n:
        # 带关键词的 CCTV 变体（无数字写法）
        for kw, grp in [("4K", "4K专区"), ("8K", "4K专区"),
                        ("体育", "体育赛事"), ("纪录", "纪录片"),
                        ("少儿", "少儿动画"), ("动画", "少儿动画"),
                        ("影视", "影视轮播"), ("怀旧", "影视轮播")]:
            if kw in n:
                return grp
        return "央视频道"

    # 2. 地方频道：省级行政区名 + 卫视/频道
    provinces = ["北京", "上海", "广东", "浙江", "江苏", "湖南", "四川",
                 "山东", "湖北", "河北", "河南", "安徽", "福建", "江西",
                 "辽宁", "天津", "重庆", "山西", "陕西", "云南", "广西",
                 "贵州", "甘肃", "新疆", "西藏", "内蒙", "吉林", "黑龙江",
                 "海南", "宁夏", "青海", "香港", "澳门", "台湾"]
    for p in provinces:
        if p in n:
            return "地方频道"

    # 3. 卫视高清（非省级但带"卫视"字样）
    if "卫视" in n:
        return "卫视高清"

    # 4. 其他关键词
    for kw, grp in [("4K", "4K专区"), ("2160", "4K专区"), ("8K", "4K专区"),
                    ("体育", "体育赛事"), ("足球", "体育赛事"),
                    ("NBA", "体育赛事"), ("赛事", "体育赛事"),
                    ("少儿", "少儿动画"), ("动画", "少儿动画"), ("卡通", "少儿动画"),
                    ("纪录", "纪录片"), ("纪实", "纪录片"),
                    ("影视", "影视轮播"), ("电影", "影视轮播"),
                    ("剧场", "影视轮播"), ("轮播", "影视轮播")]:
        if kw in n:
            return grp

    return "网络频道"


def build_group(items):
    """从多个源拉取，按 (组, 名) 去重，保留质量最好、最快的"""
    best = {}
    for it in items:
        key = (it["group"], it["name"])
        score = quality_score(it["name"]) + (50 if it.get("ok") else 0)
        if key not in best or score > best[key]["score"]:
            best[key] = {**it, "score": score}
    return best


def sort_groups(groups):
    def order(g):
        if g in GROUP_ORDER:
            return GROUP_ORDER.index(g)
        return 999
    return sorted(set(groups), key=order)


def write_m3u(channels, logo_base, epg, out_path):
    lines = ['#EXTM3U url-tvg="%s" x-tvg-url="%s"' % (epg, epg)]
    groups = sort_groups([c["group"] for c in channels])
    for g in groups:
        grp_chans = [c for c in channels if c["group"] == g]
        for c in grp_chans:
            logo = c.get("logo") or ""
            if not logo and logo_base:
                # 用频道名构造台标（兼容 fanmingming 风格）
                logo = logo_base + c["name"] + ".png"
            inf = '#EXTINF:-1 tvg-name="%s" tvg-logo="%s" group-title="%s",%s' % (
                c.get("tvg_name", c["name"]).replace('"', ""),
                logo, g, c["name"])
            lines.append(inf)
            lines.append(c["url"])
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


def write_txt(channels, out_path):
    with open(out_path, "w", encoding="utf-8") as f:
        for c in channels:
            f.write("%s,%s\n" % (c["name"], c["url"]))


def main():
    os.makedirs(OUT, exist_ok=True)
    if not os.path.exists(CONF):
        log("配置文件不存在: %s" % CONF)
        sys.exit(1)

    cfg = load_config()
    sources = cfg.get("live_sources", [])
    if not sources:
        log("未配置 live_sources")
        sys.exit(0)

    log("开始抓取 %d 个直播源..." % len(sources))
    all_items = []
    failed = []
    for s in sources:
        url = s["url"]
        try:
            text = fetch_text(url)
            kind = s.get("format", "")
            if kind == "txt" or (not kind and url.lower().endswith(".txt")):
                items = parse_txt(text)
            else:
                items = parse_m3u(text)
            log("  [OK] %s -> %d 条" % (s["name"], len(items)))
            all_items += items
        except Exception as e:
            log("  [FAIL] %s: %s" % (s["name"], e))
            failed.append({"name": s["name"], "error": str(e)})

    log("共抓取 %d 条原始频道，开始探测测速..." % len(all_items))

    results = []
    if HAVE_REQUESTS:
        with ThreadPoolExecutor(max_workers=40) as ex:
            futs = [ex.submit(probe, it) for it in all_items]
            for i, fut in enumerate(as_completed(futs), 1):
                results.append(fut.result())
                if i % 100 == 0:
                    log("  探测进度 %d/%d" % (i, len(all_items)))
    else:
        for i, it in enumerate(all_items, 1):
            results.append(probe(it))
            if i % 50 == 0:
                log("  探测进度 %d/%d" % (i, len(all_items)))

    ok_items = [r[0] for r in results if r[1]]
    log("有效频道 %d 条，无效 %d 条" % (len(ok_items), len(results) - len(ok_items)))

    # 归类 + 去重
    for it in ok_items:
        it["group"] = classify_group(it["name"])

    best = build_group(ok_items)
    channels = sorted(best.values(), key=lambda c: -c["score"])
    log("去重后 %d 个频道" % len(channels))

    base_url = cfg.get("base_url", "").rstrip("/")
    logo_base = cfg.get("logo_base", "")
    epg = cfg.get("epg", "")
    epg = epg.replace("${BASE}", base_url) if epg else ""

    write_m3u(channels, logo_base, epg, os.path.join(OUT, "live.m3u"))
    write_txt(channels, os.path.join(OUT, "live.txt"))
    log("已生成 output/live.m3u, output/live.txt")

    # 统计
    by_group = {}
    for c in channels:
        by_group[c["group"]] = by_group.get(c["group"], 0) + 1
    log("分组统计: " + ", ".join("%s %d" % kv for kv in
                                sorted(by_group.items(), key=lambda kv: -kv[1])))

    if failed:
        log("失败源: " + ", ".join(f["name"] for f in failed))


if __name__ == "__main__":
    main()
