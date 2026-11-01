#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地测试：不联网验证分组、台标、清晰度权重、EPG 等核心逻辑
用法: python scripts/test_local.py
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from live_update import (classify_group, quality_score, sort_groups,
                        GROUP_ORDER, parse_m3u, parse_txt)


def log(msg):
    print(msg, flush=True)


def assert_eq(name, got, expect):
    ok = got == expect
    log(("[OK]   " if ok else "[FAIL] ") + name)
    if not ok:
        log("       期望: %s" % expect)
        log("       实际: %s" % got)
    return ok


def assert_true(name, cond):
    log(("[OK]   " if cond else "[FAIL] ") + name)
    return cond


def test_quality():
    log("\n== 清晰度权重 ==")
    ok = True
    ok &= assert_eq("4K 权重最高", quality_score("CCTV4K频道"), 100)
    ok &= assert_eq("2160 识别", quality_score("测试2160P"), 95)
    ok &= assert_eq("1080 识别", quality_score("高清1080P源"), 70)
    ok &= assert_eq("720 识别", quality_score("标清720P"), 40)
    ok &= assert_eq("普通源无加成", quality_score("普通频道"), 0)
    return ok


def test_group():
    log("\n== 分组归类 ==")
    ok = True
    ok &= assert_eq("CCTV 归央视频道", classify_group("CCTV1综合"), "央视频道")
    ok &= assert_eq("卫视含省份归地方", classify_group("湖南卫视高清"), "地方频道")
    ok &= assert_eq("纯4K频道归4K专区", classify_group("4K超清频道"), "4K专区")
    ok &= assert_eq("CCTV4K归央视频道", classify_group("CCTV4K超清"), "央视频道")
    ok &= assert_eq("体育归体育赛事", classify_group("CCTV5体育"), "体育赛事")
    ok &= assert_eq("少儿归少儿动画", classify_group("CCTV14少儿"), "少儿动画")
    ok &= assert_eq("纪录片归类", classify_group("CCTV9纪录"), "纪录片")
    ok &= assert_eq("影视轮播归类", classify_group("成龙影视"), "影视轮播")
    ok &= assert_eq("北京归地方", classify_group("北京卫视"), "地方频道")
    ok &= assert_eq("未知归网络频道", classify_group("某神秘台"), "网络频道")
    return ok


def test_parse_m3u():
    log("\n== M3U 解析 ==")
    sample = """#EXTM3U
#EXTINF:-1 tvg-logo="http://x/y.png" group-title="央视频道",CCTV1综合
http://example.com/cctv1.m3u8
#EXTINF:-1 group-title="4K专区",CCTV4K频道
http://example.com/4k.m3u8
"""
    items = parse_m3u(sample)
    ok = True
    ok &= assert_eq("解析出2条", len(items), 2)
    ok &= assert_eq("台标提取", items[0]["logo"], "http://x/y.png")
    ok &= assert_eq("分组提取", items[0]["group"], "央视频道")
    ok &= assert_eq("频道名提取", items[1]["name"], "CCTV4K频道")
    return ok


def test_parse_txt():
    log("\n== TXT 解析 ==")
    sample = """CCTV1,http://example.com/1.m3u8
湖南卫视$http://example.com/hn.m3u8
# 这是注释
,http://invalid
"""
    items = parse_txt(sample)
    ok = True
    ok &= assert_eq("有效2条", len(items), 2)
    ok &= assert_eq("逗号分隔", items[0]["url"], "http://example.com/1.m3u8")
    ok &= assert_eq("美元符分隔", items[1]["name"], "湖南卫视")
    return ok


def test_group_sort():
    log("\n== 分组排序 ==")
    gs = ["其他频道", "央视频道", "4K专区", "地方频道"]
    sorted_g = sort_groups(gs)
    ok = True
    ok &= assert_eq("央视频道排最前", sorted_g[0], "央视频道")
    ok &= assert_true("央视频道与地方频道在前", sorted_g.index("央视频道") < sorted_g.index("4K专区"))
    return ok


def main():
    log("=== 影视仓自建源 本地测试 ===")
    results = []
    for f in [test_quality, test_group, test_parse_m3u,
              test_parse_txt, test_group_sort]:
        try:
            results.append(bool(f()))
        except Exception as e:
            log("[FAIL] %s 抛异常: %s" % (f.__name__, e))
            results.append(False)
    ok = all(results)
    log("\n" + ("全部通过 ✅" if ok else "存在失败用例 ❌"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
