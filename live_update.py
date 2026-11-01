#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直播源自动更新脚本
1) 抓取多个上游 M3U 源
2) 测速/探测，只保留可用的流
3) 去重 + 按分辨率排序（优先 4K）
4) 生成 m3u (带分组、台标) + 同步写入 config.json 的 lives 字段
"""
import json, os, re, sys, concurrent.futures, urllib.request
from datetime import datetime

WORK_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR    = os.path.join(WORK_DIR, "output")
os.makedirs(OUT_DIR, exist_ok=True)

CONFIG_FILE = os.path.join(WORK_DIR, "config", "sources.json")

# ---------------- 配置 ----------------
PROBE_TIMEOUT = 6      # 探测超时(秒)
WORKERS       = 40
HEADERS       = {"User-Agent": "Mozilla/5.0 (TVBox)"}

# 台标 CDN（按频道名匹配）
LOGO_BASE = "https://cdn.jsdelivr.net/gh/fanmingming/live@main/tv/"

# 频道名中的分辨率关键词权重（用于 4K 优先排序）
RES_WEIGHT = [
    (re.compile(r"4[kK]"),                      100),
    (re.compile(r"2160"),                        90),
    (re.compile(r"超清|原画|蓝光"),               70),
    (re.compile(r"1080[pP]|高清"),               50),
    (re.compile(r"720[pP]"),                     30),
    (re.compile(r"标清|流畅"),                   10),
]


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_sources():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, encoding="utf-8") as f:
            cfg = json.load(f)
        return cfg.get("live_sources", []), cfg.get("base_url", "")
    return [], ""


def fetch_text(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read()
    for enc in ("utf-8", "gbk", "gb2312", "big5"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", errors="ignore")


def parse_m3u(text):
    """解析 M3U，返回 [(name, url, group, logo)]"""
    items, buf = [], {"name": "未知", "group": "", "logo": "", "url": ""}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#EXTM3U"):
            continue
        if line.startswith("#EXTINF"):
            grp = re.search(r'group-title="([^"]*)"', line)
            logo = re.search(r'tvg-logo="([^"]*)"', line)
            name = re.split(r",", line)[-1].strip()
            buf = {"name": name,
                   "group": grp.group(1) if grp else "",
                   "logo": logo.group(1) if logo else ""}
        elif not line.startswith("#"):
            buf["url"] = line
            items.append(dict(buf))
            buf = {"name": "未知", "group": "", "logo": "", "url": ""}
    return items


def probe(item):
    """探测流可用性：HEAD 请求看状态码 + Content-Length"""
    url = item["url"]
    try:
        req = urllib.request.Request(url, headers=HEADERS, method="HEAD")
        with urllib.request.urlopen(req, timeout=PROBE_TIMEOUT) as r:
            ok = r.status == 200
            length = int(r.headers.get("Content-Length", 0))
        if ok and length > 1000:
            return item
    except Exception:
        pass
    # HEAD 被拒就降级 GET 探测
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=PROBE_TIMEOUT) as r:
            if r.status == 200 and int(r.headers.get("Content-Length", 0)) > 1000:
                return item
    except Exception:
        pass
    return None


def res_score(name):
    n = name + " "
    return max((w for pat, w in RES_WEIGHT if pat.search(n)), default=20)


def build_m3u(records):
    groups = {}
    for r in records:
        g = r.get("group") or r["name"].split("-")[0] or "其他"
        groups.setdefault(g, []).append(r)

    lines = ["#EXTM3U"]
    total = 0
    for g, chans in sorted(groups.items(), key=lambda x: -len(x[1])):
        for c in sorted(chans, key=lambda x: -res_score(x["name"])):
            logo = c.get("logo") or LOGO_BASE + urllib.request.quote(c["name"])
            lines.append(f'#EXTINF:-1 group-title="{g}" tvg-logo="{logo}",{c["name"]}')
            lines.append(c["url"])
            total += 1
    return "\n".join(lines), total


def main():
    live_srcs, base_url = load_sources()
    if not live_srcs:
        log("没有配置直播源，检查 config/sources.json 的 live_sources")
        sys.exit(1)

    all_items = []
    failed = []
    for s in live_srcs:
        try:
            log(f"抓取上游: {s}")
            text = fetch_text(s)
            items = parse_m3u(text)
            log(f"  解析到 {len(items)} 条频道")
            all_items += items
        except Exception as e:
            log(f"  失败: {e}")
            failed.append(s)

    log(f"上游合计 {len(all_items)} 条, 去重+探测中...")

    # 去重(按 url), 保序
    seen, uniq = set(), []
    for it in all_items:
        if it["url"] not in seen:
            seen.add(it["url"])
            uniq.append(it)

    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        results = list(ex.map(probe, uniq))

    valid = [r for r in results if r]
    log(f"可用 {len(valid)} 条, 失效 {len(uniq)-len(valid)} 条, 失败上游 {len(failed)}")

    m3u_text, total = build_m3u(valid)
    m3u_path = os.path.join(OUT_DIR, "live.m3u")
    with open(m3u_path, "w", encoding="utf-8") as f:
        f.write(m3u_text)
    log(f"已写入 {m3u_path} ({total} 频道)")

    # 同步主配置
    main_cfg = os.path.join(WORK_DIR, "output", "tvbox.json")
    if os.path.exists(main_cfg):
        with open(main_cfg, encoding="utf-8") as f:
            cfg = json.load(f)
    else:
        cfg = {"spider": "", "sites": [], "lives": []}

    live_url = (base_url.rstrip("/") + "/live.m3u") if base_url else ""
    cfg["lives"] = [{
        "name": "自动更新直播源",
        "type": 0,
        "url": live_url or "https://你的加速域名/live.m3u",
        "playerType": 1,
        "epg": "",
        "logo": LOGO_BASE,
    }]
    with open(main_cfg, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    log(f"已同步 {main_cfg}")
    log("完成")


if __name__ == "__main__":
    main()
