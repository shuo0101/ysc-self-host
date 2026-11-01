#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
点播源聚合脚本
- 抓取多个上游 config.json
- 合并去重 sites
- 输出多仓订阅 (urls.json) + 单仓聚合 (tvbox.json)
"""
import json, os, sys, urllib.request
from datetime import datetime

WORK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR  = os.path.join(WORK_DIR, "output")
os.makedirs(OUT_DIR, exist_ok=True)
CONFIG_FILE = os.path.join(WORK_DIR, "config", "sources.json")
HEADERS = {"User-Agent": "Mozilla/5.0 (TVBox)"}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def load_sources():
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


def fetch_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def collect(name, url):
    """收集单个上游的 sites"""
    try:
        log(f"抓取 [{name}] {url}")
        data = fetch_json(url)
        sites = data.get("sites", [])
        spider = data.get("spider", "")
        log(f"  获得 {len(sites)} 个站点")
        return sites, spider
    except Exception as e:
        log(f"  失败: {e}")
        return [], ""


def main():
    cfg = load_sources()
    vod = cfg.get("vod_sources", [])
    base_url = cfg.get("base_url", "").rstrip("/")

    all_sites, spiders = [], []
    for item in vod:
        sites, sp = collect(item["name"], item["url"])
        all_sites += sites
        if sp:
            spiders.append(sp)

    # 去重(按 api 地址)
    seen, uniq = set(), []
    for s in all_sites:
        key = s.get("api", "") or s.get("key", "")
        if key not in seen:
            seen.add(key)
            uniq.append(s)

    spider = spiders[0] if spiders else cfg.get("spider", "")

    # 单仓聚合
    tvbox = {
        "spider": spider,
        "sites": uniq,
        "lives": cfg.get("lives_default", []),
    }
    tvbox_path = os.path.join(OUT_DIR, "tvbox.json")
    with open(tvbox_path, "w", encoding="utf-8") as f:
        json.dump(tvbox, f, ensure_ascii=False, indent=2)
    log(f"单仓: {tvbox_path} ({len(uniq)} 站点)")

    # 多仓订阅
    urls = [{"name": v["name"], "url": v["url"]} for v in vod]
    urls_path = os.path.join(OUT_DIR, "urls.json")
    with open(urls_path, "w", encoding="utf-8") as f:
        json.dump({"urls": urls}, f, ensure_ascii=False, indent=2)
    log(f"多仓: {urls_path}")
    log("完成")


if __name__ == "__main__":
    main()
