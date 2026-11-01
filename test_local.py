#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""本地测试: 用内置测试源跑一遍, 不依赖真实网络的上游地址"""
import json, os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from live_update import parse_m3u, build_m3u

WORK_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(WORK_DIR, "output")

# 模拟一个 M3U 文本
sample = """#EXTM3U
#EXTINF:-1 group-title="央视" tvg-logo="cctv1.png",CCTV-1 综合 4K
http://example.com/cctv1.m3u8
#EXTINF:-1 group-title="央视",CCTV-2 财经
http://example.com/cctv2.m3u8
#EXTINF:-1 group-title="卫视" tvg-logo="hunan.png",湖南卫视 高清
http://example.com/hunan.m3u8
"""

items = parse_m3u(sample)
print(f"解析到 {len(items)} 条频道:")
for it in items:
    print(f"  [{it['group']}] {it['name']} -> {it['url']}")

recs = [{"name": i["name"], "url": i["url"], "group": i["group"], "logo": i["logo"]} for i in items]
m3u, total = build_m3u(recs)
print(f"\n生成 M3U ({total} 频道):")
print(m3u[:400], "...")

os.makedirs(OUT, exist_ok=True)
with open(os.path.join(OUT, "live.m3u"), "w", encoding="utf-8") as f:
    f.write(m3u)
print(f"\n已写入 {OUT}/live.m3u")

# 生成测试用 config
cfg = {
    "spider": "",
    "sites": [{"key": "demo", "name": "测试站", "type": 3, "api": "https://api.example.com/api", "playUrl": "", "ext": ""}],
    "lives": [{"name": "测试直播", "type": 0, "url": "https://gh-proxy.com/.../live.m3u", "playerType": 1, "epg": "", "logo": ""}]
}
with open(os.path.join(OUT, "tvbox.json"), "w", encoding="utf-8") as f:
    json.dump(cfg, f, ensure_ascii=False, indent=2)
print(f"已写入 {OUT}/tvbox.json")
print("本地测试通过!")
