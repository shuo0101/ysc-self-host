#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
点播源聚合：拉取多个上游 tvbox.json，合并去重，输出单一配置
支持单仓 (sites) 和多仓 (urls) 两种格式
"""
import json
import os
import re
import sys
import time

try:
    import requests
    HAVE_REQUESTS = True
except ImportError:
    HAVE_REQUESTS = False
    import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONF = os.path.join(ROOT, "config", "sources.json")
OUT = os.path.join(ROOT, "output")


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def fetch_text(url, timeout=20):
    if HAVE_REQUESTS:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        return r.text
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def safe_loads(text, url):
    """容错解析：去掉 JS 注释、多余逗号等"""
    t = text.strip()
    # 去掉 // 单行注释（简单处理：仅处理行首或空白后的 //）
    lines = []
    for line in t.split("\n"):
        ls = line.strip()
        if ls.startswith("//"):
            continue
        lines.append(line)
    t = "\n".join(lines)
    try:
        return json.loads(t)
    except Exception as e:
        # 去尾部逗号
        t2 = re.sub(r",(\s*[}\]])", r"\1", t)
        try:
            return json.loads(t2)
        except Exception:
            log("  [PARSE FAIL] %s: %s" % (url, e))
            return None


def main():
    os.makedirs(OUT, exist_ok=True)
    with open(CONF, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    vod_sources = cfg.get("vod_sources", [])
    if not vod_sources:
        log("未配置 vod_sources")
        return

    log("开始聚合 %d 个点播源..." % len(vod_sources))
    sites = []
    seen_keys = set()
    seen_apis = set()
    spiders = []
    lives = []
    multi_urls = []
    failed = []

    for s in vod_sources:
        try:
            text = fetch_text(s["url"])
            data = safe_loads(text, s["url"])
            if not data or not isinstance(data, dict):
                log("  [EMPTY] %s" % s["name"])
                continue
        except Exception as e:
            log("  [FAIL] %s: %s" % (s["name"], e))
            failed.append({"name": s["name"], "error": str(e)})
            continue

        # 单仓格式
        for st in data.get("sites", []) or []:
            key = st.get("key", "").strip()
            api = (st.get("api", "") or "").strip().rstrip("/")
            if not key or not api:
                continue
            if api in seen_apis:
                continue
            seen_apis.add(api)
            sites.append({**st, "name": st.get("name") or key})
            seen_keys.add(key)

        # spider
        sp = data.get("spider", "")
        if sp and sp not in spiders:
            spiders.append(sp)

        # 内嵌多仓
        for u in data.get("urls", []) or []:
            uu = u.get("url", "") if isinstance(u, dict) else u
            if uu and uu not in [x.get("url") for x in multi_urls]:
                multi_urls.append(u if isinstance(u, dict) else {"url": u, "name": ""})

        # 内嵌直播
        for lv in data.get("lives", []) or []:
            if lv.get("url") and lv not in lives:
                lives.append(lv)

    log("合并后点播站点 %d 个" % len(sites))

    # 若配置里没有 spider，使用首个源的 spider
    if not spiders and sites:
        spiders.append("")

    out = {
        "spider": spiders[0] if spiders else "",
        "sites": sites,
        "lives": lives,
        "urls": multi_urls,
    }
    # 清理空字段
    out = {k: v for k, v in out.items() if v}

    out_path = os.path.join(OUT, "tvbox.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    log("已生成 output/tvbox.json")

    # 同时生成影视仓可直接用的单仓精简版
    single = {
        "spider": out.get("spider", ""),
        "sites": sites[:60],  # 限制数量，避免配置过大加载慢
    }
    if lives:
        single["lives"] = lives
    single_path = os.path.join(OUT, "tvbox_single.json")
    with open(single_path, "w", encoding="utf-8") as f:
        json.dump(single, f, ensure_ascii=False, indent=2)
    log("已生成 output/tvbox_single.json（精简单仓，站点上限60）")

    if failed:
        log("失败源: " + ", ".join(f["name"] for f in failed))


if __name__ == "__main__":
    main()
