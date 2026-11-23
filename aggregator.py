#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
aggregator.py — 直播源 / 点播源 聚合器（v9.2）

两阶段流水线
  阶段一  抓取 → 整理 → 全局去重（网络源 + 本地源）
  阶段二  探测 → 精选 → 输出

本次修正
  · 点播逻辑完全保留，不改动
  · 频道归一化：4K 与 4、8K 与 8 视为【不同】频道，必须拆开
      正确  CCTV4K ≠ CCTV4 、CCTV8K ≠ CCTV8
      正确  卫视4K ≠ 卫视4 、卫视8K ≠ 卫视8
  · 串线路修复：同一频道下，把名称/画质明显不属于该频道的候选剔除
  · 补齐全部辅助函数（v9.1 中缺失定义的引用）
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import ipaddress
import json
import logging
import os
import re
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from . import database as db
except ImportError:  # 直接运行
    try:
        import database as db
    except ImportError:
        db = None


# ─────────────────────────── 日志 ────────────────────────────
def setup_logger(level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("aggregator")
    if logger.handlers:
        return logger
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"
    )
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


log = setup_logger()


# ───────────────────────── 全局开关 ───────────────────────────
class Flags:
    DRY_RUN = False
    QUICK = False
    MAX_FETCH_CHANNELS = 0
    MAX_FETCH_URLS = 0


# ═════════════════════════════════════════════════════════════
#  数据模型
# ═════════════════════════════════════════════════════════════

@dataclass
class Entry:
    """单条直播/点播线路"""
    url: str
    name: str = ""
    group: str = ""
    source: str = ""                 # 来源名（网络源/本地源/static）
    source_type: str = "network"     # network / local / static
    entry_type: str = "live"         # live / vod
    quality: str = ""                # 8K / 4K / HD / SD
    tvg_id: str = ""
    logo: str = ""
    meta: dict = field(default_factory=dict)
    raw_line: str = ""

    def clone(self) -> "Entry":
        e = copy.copy(self)
        e.meta = copy.deepcopy(self.meta)
        return e

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ProbeResult:
    url: str
    ok: bool = False
    status: int = 0
    elapsed: float = 0.0
    quality: str = ""
    err: str = ""
    redirect_url: str = ""
    # ── 缓存相关（不参与缓存序列化）──
    from_cache: bool = False
    hit_rate: float = 0.0
    sample_size: int = 0
    _window: int = 20          # 该 URL 历史窗口长度，写入时回读
    _stale: bool = False       # 命中但已超过软 TTL（结果偏旧）


class _FakeSigint(Exception):
    pass


def _sigint_raise(signum, frame):
    raise _FakeSigint()


# ═════════════════════════════════════════════════════════════
#  缓存层
#  · 源内容缓存：sources_cache/<name>.txt  + 同名 .json 元信息
#  · 探测结果缓存：probe_cache.json（URL → 结果 + 历史）
# ═════════════════════════════════════════════════════════════

def _atomic_write(fp: Path, data: str) -> bool:
    """原子写：先写同目录临时文件再 rename，避免中断留下半截文件"""
    tmp = fp.with_suffix(fp.suffix + ".tmp")
    try:
        tmp.write_text(data, encoding="utf-8")
        tmp.replace(fp)
        return True
    except Exception:  # noqa
        try:
            tmp.unlink()
        except Exception:  # noqa
            pass
        return False


def _cache_stale(cache_fp: Path, ttl: int) -> bool:
    """
    缓存是否失效：TTL 内有效；TTL<=0 表示永不过期；
    文件缺失或时间戳不可读一律视为失效。
    """
    if ttl <= 0:
        return False
    try:
        return (time.time() - cache_fp.stat().st_mtime) > ttl
    except OSError:
        return True


def cache_paths(cfg: dict, name: str) -> Tuple[Path, Path]:
    """源名 → (内容文件, 元信息文件)"""
    d = Path(cfg.get("cache_dir", "sources_cache"))
    safe = re.sub(r"[^a-zA-Z0-9._-]", "_", name) or "unnamed"
    return d / f"{safe}.txt", d / f"{safe}.meta.json"


def read_cache(cfg: dict, name: str, ttl: int) -> Tuple[Optional[str], dict]:
    """
    读源内容缓存。返回 (text, meta)；失效或不存在时 text=None。
    meta 里带 cached_at / age_hours / stale 便于日志与报告。
    """
    fp, meta_fp = cache_paths(cfg, name)
    if _cache_stale(fp, ttl):
        return None, {"stale": True}
    try:
        text = fp.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None, {"stale": True}
    meta: dict = {}
    try:
        meta = json.loads(meta_fp.read_text(encoding="utf-8"))
    except Exception:  # noqa
        meta = {}
    try:
        age = time.time() - fp.stat().st_mtime
    except OSError:
        age = 0.0
    meta.update({"from_cache": True, "cache_age": int(age),
                 "cache_age_hours": round(age / 3600, 1), "stale": False})
    return text, meta


def write_cache(cfg: dict, name: str, text: str, info: dict) -> bool:
    """
    写源内容缓存。只接受合法播放列表，避免把空响应 / HTML 错误页
    固化下来（那会让一次临时故障变成长期污染）。
    """
    if not _looks_like_playlist(text):
        return False
    fp, meta_fp = cache_paths(cfg, name)
    if not _atomic_write(fp, text):
        return False
    meta = {
        "name": name,
        "url": info.get("final_url") or info.get("url"),
        "bytes": len(text.encode("utf-8", "replace")),
        "cached_at": dt.datetime.now().isoformat(timespec="seconds"),
        "status": info.get("status", 0),
        "elapsed": info.get("elapsed", 0.0),
        "entries": info.get("entries", 0),
    }
    _atomic_write(meta_fp, json.dumps(meta, ensure_ascii=False, indent=1))
    return True


# ── 探测结果缓存 ────────────────────────────────────────────

def _probe_cache_fp(cfg: dict) -> Path:
    return Path(cfg.get("cache_dir", "sources_cache")) / "probe_cache.json"


def load_probe_cache(cfg: dict) -> dict:
    """读取探测结果缓存文件；损坏时返回空表而不是崩溃"""
    fp = _probe_cache_fp(cfg)
    if not fp.exists():
        return {}
    try:
        data = json.loads(fp.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:  # noqa
        log.warning(f"⚠ 探测缓存损坏，已忽略：{fp.name}")
        return {}


def save_probe_cache(cfg: dict, store: dict) -> None:
    fp = _probe_cache_fp(cfg)
    fp.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(fp, json.dumps(store, ensure_ascii=False, separators=(",", ":")))


def probe_cache_get(store: dict, url: str, ttl: int,
                    window: int) -> Optional[ProbeResult]:
    """
    命中条件（全部满足才复用）：
      1. 有记录且未过 TTL
      2. 历史命中率 >= 50%（window 次内）——抖动线路不固化
      3. 上次结果为失败时，要求历史至少成功过 1 次，否则每次都重试
    """
    rec = store.get(url)
    if not isinstance(rec, dict):
        return None
    try:
        age = time.time() - float(rec.get("ts", 0))
    except (TypeError, ValueError):
        return None
    if ttl > 0 and age > ttl:
        return None
    hist = rec.get("h") or []
    if not hist:
        return None
    recent = hist[-max(window, 1):]
    hits = sum(1 for h in recent if h.get("ok"))
    rate = hits / len(recent)
    if rate < 0.5:
        return None
    if not rec.get("ok") and hits == 0:
        return None
    # 缓存记录缺画质时，用历史里出现最多的画质补齐，避免精选阶段排序退化
    quality = rec.get("quality") or ""
    if not quality and recent:
        cnt: Dict[str, int] = defaultdict(int)
        for h in recent:
            if h.get("q"):
                cnt[h["q"]] += 1
        if cnt:
            quality = max(cnt.items(), key=lambda kv: kv[1])[0]
    res = ProbeResult(
        url=url, ok=bool(rec.get("ok")), status=int(rec.get("status", 0)),
        elapsed=float(rec.get("elapsed", 0.0)), quality=quality,
        err=rec.get("err", "") or "", redirect_url=rec.get("redirect_url", "") or "",
    )
    res.from_cache = True
    res.hit_rate = round(rate, 2)
    res.sample_size = len(recent)
    return res


def probe_cache_update(store: dict, results: dict) -> int:
    """把本轮探测结果并入历史（保留最近 history_window 条），返回新增记录数"""
    now = time.time()
    for url, r in results.items():
        rec = store.get(url)
        if not isinstance(rec, dict):
            rec = {"h": []}
        hist = rec.get("h") or []
        hist.append({"ok": bool(r.ok), "s": r.status, "q": r.quality or ""})
        limit = max(int(r._window or 20), 1)
        rec["h"] = hist[-limit:]
        recent = rec["h"]
        hits = sum(1 for h in recent if h.get("ok"))
        rec["ok"] = bool(r.ok)
        rec["status"] = r.status
        rec["elapsed"] = r.elapsed
        rec["quality"] = r.quality
        rec["err"] = r.err
        rec["redirect_url"] = r.redirect_url
        rec["ts"] = now
        rec["rate"] = round(hits / len(recent), 2)
        store[url] = rec
    return len(results)


def probe_cache_purge(store: dict, max_age: int) -> int:
    """清理超过 max_age 秒且非存活的记录，避免缓存无限膨胀"""
    if max_age <= 0:
        return 0
    cutoff = time.time() - max_age
    dead = [u for u, r in store.items()
            if isinstance(r, dict) and float(r.get("ts", 0)) < cutoff and not r.get("ok")]
    for u in dead:
        store.pop(u, None)
    return len(dead)


# ═════════════════════════════════════════════════════════════
#  HTTP / 会话
# ═════════════════════════════════════════════════════════════

def make_session(retries: int = 2, backoff: float = 1.0,
                 timeout: int = 15) -> requests.Session:
    """构造带重试的 requests 会话"""
    s = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff,
        allowed_methods=("GET", "HEAD"),
        status_forcelist=(429, 500, 502, 503, 504),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0 Safari/537.36",
        "Accept": "*/*",
    })
    s.timeout = timeout
    return s


def fetch_text(url: str, timeout: int = 15) -> Tuple[Optional[str], dict]:
    """抓取一个 URL（或本地文件）的文本内容"""
    info: dict = {"url": url, "error": None, "status": 0, "elapsed": 0.0,
                  "bytes": 0, "final_url": url}

    # ── 本地文件：绝对路径 / file:// 协议 ──
    local = url
    if local.startswith("file://"):
        local = local[7:]
    if local.startswith("/") or re.match(r"^[A-Za-z]:[\\/]", local):
        t0 = time.time()
        try:
            fp = Path(local)
            if not fp.exists():
                info["error"] = f"本地文件不存在：{fp}"
                return None, info
            raw = fp.read_bytes()
            text = raw.decode("utf-8", errors="replace")
            info.update({"status": 200, "bytes": len(raw),
                         "elapsed": round(time.time() - t0, 2),
                         "final_url": str(fp)})
            return text, info
        except Exception as e:  # noqa
            info["error"] = f"{type(e).__name__}: {e}"
            return None, info

    session = make_session(timeout=timeout)
    try:
        r = session.get(url, timeout=timeout, allow_redirects=True)
        info["status"] = r.status_code
        info["final_url"] = r.url
        info["elapsed"] = round(r.elapsed.total_seconds(), 2)
        info["bytes"] = len(r.content)
        if r.status_code == 200:
            text = r.text
            return text, info
        info["error"] = f"HTTP {r.status_code}"
    except requests.exceptions.RequestException as e:
        info["error"] = f"{type(e).__name__}: {e}"
    except Exception as e:  # noqa
        info["error"] = f"{type(e).__name__}: {e}"
    return None, info


def fetch_with_fallback(url: str, fallbacks: Sequence[str],
                        timeout: int = 15) -> Tuple[Optional[str], dict]:
    """
    带 fallback 的抓取：依次尝试主地址与备用地址。
    返回 (text, info)；全失败则 text=None。
    """
    info: dict = {"url": url, "error": None, "tried": []}
    candidates = [url] + list(fallbacks or [])
    for u in candidates:
        text, rinfo = fetch_text(u, timeout=timeout)
        info["tried"].append(rinfo)
        if text is not None:
            info.update(rinfo)
            info["url"] = u
            return text, info
    # 全部失败，记录最后一个错误
    if info["tried"]:
        info["error"] = info["tried"][-1].get("error")
    return None, info


# ═════════════════════════════════════════════════════════════
#  源发现（网络 + 本地）
# ═════════════════════════════════════════════════════════════

def build_sources(cfg: dict) -> List[dict]:
    """
    把 config.yaml 里 live / vod / discover 各分组展开成统一的 sources 列表。

    支持的写法（缺哪个都不会报错）：
      live.urls_static       网络清单 URL（m3u / txt）
      live.urls_static_4k    同上，标记为 4K 候选
      live.urls_refresh      带签名/会过期的清单 URL
      live.urls_zsdc         运营商单播源清单
      live.urls_custom       自定义补充清单
      live.local_dir        本地目录，自动扫描 *.txt / *.m3u / *.m3u8
      live.sources          已按旧格式写好的完整源定义（原样保留）
      discover.vod_seeds    点播种子
    顶层 sources: 若已存在，则与上面展开的结果合并（按 url 去重）。
    """
    base = Path(cfg.get("base_dir", "."))
    live_cfg = cfg.get("live", {}) or {}
    out: List[dict] = []
    seen: set = set()

    def add(item: dict):
        url = (item.get("url") or "").strip()
        if not url or url in seen:
            return
        seen.add(url)
        out.append(item)

    # 原有顶层 sources（含 type=vod）原样保留
    for s in (cfg.get("sources", []) or []):
        if isinstance(s, dict) and s.get("url"):
            add(dict(s))

    # live.sources 旧格式
    for s in (live_cfg.get("sources", []) or []):
        if isinstance(s, dict) and s.get("url"):
            item = dict(s)
            item.setdefault("type", "live")
            add(item)

    # URL 分组 → 统一转成 network 源
    groups = [
        ("urls_static", "static-list", {}),
        ("urls_static_4k", "static-4k", {"tags": ["4k-candidate"]}),
        ("urls_refresh", "refresh", {"tags": ["refresh"]}),
        ("urls_zsdc", "zsdc", {}),
        ("urls_custom", "custom", {}),
    ]
    for key, label, extra in groups:
        for u in (live_cfg.get(key, []) or []):
            if not isinstance(u, str) or not u.strip():
                continue
            item = {
                "type": "live", "name": f"{label}#{len(out) + 1}",
                "url": u.strip(), "source_type": "network",
                "fallbacks": [],
            }
            item.update(extra)
            add(item)

    # 本地目录扫描
    ldir = live_cfg.get("local_dir")
    if ldir:
        d = Path(ldir)
        if not d.is_absolute():
            d = base / d
        if d.is_dir():
            files = sorted(
                p for p in d.iterdir()
                if p.suffix.lower() in (".txt", ".m3u", ".m3u8") and p.is_file()
            )
            for p in files:
                name = p.stem.lower()
                tags: List[str] = []
                if re.search(r"4k|8k|uhd|ultra", name):
                    tags.append("4k-candidate")
                if re.search(r"refresh|刷新|鉴权|签名|过期", name):
                    tags.append("refresh")
                add({
                    "type": "live", "name": f"local/{p.name}",
                    "url": str(p), "source_type": "local",
                    "fallbacks": [], "tags": tags,
                })
            log.info(f"📁 本地源目录 {d}：发现 {len(files)} 个清单文件")
        else:
            log.warning(f"⚠️ 本地源目录不存在：{d}")

    # 点播种子
    for u in ((cfg.get("discover", {}) or {}).get("vod_seeds", []) or []):
        if isinstance(u, str) and u.strip():
            add({"type": "vod", "name": f"vod-seed#{len(out) + 1}",
                 "url": u.strip(), "source_type": "network", "fallbacks": []})

    live_n = sum(1 for s in out if s.get("type") == "live")
    vod_n = sum(1 for s in out if s.get("type") == "vod")
    log.info(f"🗂️  源清单展开：直播 {live_n} 个 / 点播 {vod_n} 个（已去重）")
    return out


_LAST_CACHE_STATS: dict = {}     # 最近一次 discover_live 的缓存统计（供报告使用）


def discover_live(cfg: dict) -> Tuple[List[Entry], List[dict]]:
    """
    发现并抓取所有直播源（网络 + 本地 + static）。
    返回 (entries, source_reports)
    """
    sources = [s for s in build_sources(cfg) if s.get("type") == "live"]
    if not sources:
        log.error("❌ 源清单为空：检查 config.yaml 的 live.urls_static / local_dir 是否配置")
        return [], []
    fcfg = cfg.get("fetch", {}) or {}
    timeout = int(fcfg.get("timeout", 30))
    cache_on = bool(fcfg.get("cache", True))
    cache_ttl = int(fcfg.get("cache_ttl_seconds", 0) or 0)
    if cache_on:
        Path(cfg.get("cache_dir", "sources_cache")).mkdir(parents=True, exist_ok=True)
    cache_hits = 0
    cache_stale_hits = 0
    cache_writes = 0
    cache_rejected = 0

    entries: List[Entry] = []
    reports: List[dict] = []

    for src in sources:
        if src.get("type") != "live":
            continue
        if src.get("enabled", True) is False:
            continue
        name = src.get("name", "?")
        src_type = src.get("source_type", "network")
        url = src.get("url", "")
        if not url:
            continue

        if src_type == "static":
            log.info(f"  📌 本地静态源 [{name}] → 直接载入")
            ents = _parse_static_source(src)
            reports.append({
                "source": name, "type": "static", "ok": True,
                "entries": len(ents), "bytes": 0, "elapsed": 0.0,
            })
            entries.extend(ents)
            continue

        # 网络源 / 本地文件源
        if src_type == "local":
            log.info(f"  📁 本地源 [{name}] {url}")
        else:
            log.info(f"  🌐 网络源 [{name}] {url}")

        # 带签名/会过期的地址不缓存：URL 一变，旧内容没有复用价值
        ephemeral = ("refresh" in (src.get("tags") or [])) or (src.get("ephemeral") is True)

        text, info = None, {}
        if cache_on and not ephemeral:
            text, meta = read_cache(cfg, name, cache_ttl)
            if text is not None:
                info = {"url": url, "final_url": meta.get("url") or url,
                        "bytes": meta.get("bytes", 0), "elapsed": 0.0,
                        "status": meta.get("status", 200)}
                for k in ("from_cache", "cache_age", "cache_age_hours"):
                    if k in meta:
                        info[k] = meta[k]
                cache_hits += 1
                log.info(f"      ↳ 命中缓存（{meta.get('cache_age_hours', '?')} 小时前）")

        if text is None:
            text, info = fetch_with_fallback(
                url, src.get("fallbacks", []) or [], timeout=timeout
            )
            if text is None and cache_on and not ephemeral:
                # 抓取失败 → 退化到过期缓存，总比没有强
                text, meta = read_cache(cfg, name, 0)
                if text is not None:
                    for k in ("from_cache", "cache_age", "cache_age_hours"):
                        if k in meta:
                            info[k] = meta[k]
                    info["stale"] = True
                    cache_stale_hits += 1
                    log.warning(f"      ↳ 抓取失败，退化使用过期缓存"
                                f"（{meta.get('cache_age_hours', '?')} 小时前）")

        if text is None:
            log.error(f"      ✗ 抓取失败：{info.get('error')}")
            reports.append({
                "source": name, "type": src_type, "ok": False,
                "error": info.get("error"), "entries": 0, "cached": False,
            })
            continue

        log.info(f"      ✓ {info.get('bytes', 0)} 字节 / {round(info.get('elapsed', 0), 1)}s"
                 f"{'  [缓存]' if info.get('from_cache') else ''}")
        ents = _parse_live_text(text, src)

        # 回写缓存：只写本轮真实抓到的内容，过期退化不刷新时间戳
        if cache_on and not ephemeral and not info.get("from_cache"):
            if write_cache(cfg, name, text, {**info, "entries": len(ents)}):
                cache_writes += 1
            elif not _looks_like_playlist(text):
                cache_rejected += 1
                log.warning("      ⚠ 内容不像播放列表，保留原缓存不覆盖")

        reports.append({
            "source": name, "type": src_type, "ok": True,
            "entries": len(ents), "bytes": info.get("bytes", 0),
            "elapsed": info.get("elapsed", 0.0),
            "cached": bool(info.get("from_cache")),
            "cache_age_hours": info.get("cache_age_hours", 0),
            "stale": bool(info.get("stale")),
        })
        entries.extend(ents)

    log.info(f"📡 发现 {len(sources)} 个源（网络 + 本地 + static），"
             f"抓取到 {len(entries)} 条原始线路")
    global _LAST_CACHE_STATS
    _LAST_CACHE_STATS = {
        "enabled": cache_on, "hits": cache_hits,
        "stale_hits": cache_stale_hits, "writes": cache_writes,
        "rejected": cache_rejected,
    }
    return entries, reports


def _parse_live_text(text: str, src: dict) -> List[Entry]:
    """解析直播源文本（自动识别 m3u / txt 格式）"""
    if not text:
        return []
    head = text.lstrip()[:100]
    if "#EXTM3U" in head or "#EXTINF" in head:
        return _parse_m3u(text, src)
    return _parse_txt_live(text, src)


def _parse_m3u(text: str, src: dict) -> List[Entry]:
    """解析 m3u/m3u8 格式"""
    out: List[Entry] = []
    cur: Optional[dict] = None
    for raw in text.splitlines():
        line = raw.rstrip("\r")
        if line.startswith("#EXTINF"):
            cur = {"attrs": _parse_extinf_attrs(line)}
        elif line.startswith("#"):
            continue
        elif line.strip():
            url = line.strip()
            name = ""
            group = ""
            quality = ""
            if cur:
                name = cur["attrs"].get("tvg-name") or cur["attrs"].get("name") or ""
                tvg_id = cur["attrs"].get("tvg-id", "") or ""
                logo = cur["attrs"].get("tvg-logo", "") or ""
                group = cur["attrs"].get("group-title", "") or ""
                # 从 EXTINF 尾部的逗号后内容补名字
                m = re.search(r"#EXTINF[^\n]*?,\s*(.*)$", raw)
                # 此处 raw 已处理，从 line 重新取
                tail = re.sub(r"^#EXTINF[^\n]*?,", "", line)
                if not name and tail:
                    name = tail.strip()
                quality = _quality_from_text(name + " " + url)
                out.append(Entry(
                    url=url, name=name, group=group, tvg_id=tvg_id,
                    logo=logo, quality=quality,
                    source=src.get("name", ""),
                    source_type=src.get("source_type", "network"),
                ))
            cur = None
    return out


def _parse_extinf_attrs(line: str) -> dict:
    """从 #EXTINF 行提取 tvg-xxx / group-title 等属性"""
    attrs: dict = {}
    for k in ("tvg-id", "tvg-name", "tvg-logo", "group-title", "group"):
        m = re.search(rf'{k}="([^"]*)"', line)
        if m:
            attrs[k] = m.group(1)
    # 提取逗号后的名称
    m = re.search(r"#EXTINF[^\n]*?,\s*(.*)$", line)
    if m:
        attrs["name"] = m.group(1).strip()
    return attrs


def _parse_txt_live(text: str, src: dict) -> List[Entry]:
    """解析 txt 格式：每行 频道名,url 或 频道名#genre#..."""
    out: List[Entry] = []
    current_group = src.get("group", "") or ""
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # 分组标记
        mg = re.match(r"^#genre#\s*(.*)$", line)
        if mg:
            current_group = mg.group(1).strip() or current_group
            continue
        # 内容行
        m = re.match(r'^\s*(.*?)\s*[,，]\s*(.+)$', line)
        if not m:
            continue
        name = m.group(1).strip()
        url = m.group(2).strip()
        if not name or not url:
            continue
        quality = _quality_from_text(name + " " + url)
        out.append(Entry(
            url=url, name=name, group=current_group,
            quality=quality, source=src.get("name", ""),
            source_type=src.get("source_type", "network"),
        ))
    return out


def _parse_static_source(src: dict) -> List[Entry]:
    """解析配置中的静态源（内嵌 entries 列表）"""
    out: List[Entry] = []
    for item in src.get("entries", []) or []:
        if isinstance(item, str):
            # 简化写法：单行 "频道名,url"
            m = re.match(r'^\s*(.*?)\s*[,，]\s*(.+)$', item)
            if not m:
                continue
            name, url = m.group(1).strip(), m.group(2).strip()
        elif isinstance(item, dict):
            name = item.get("name", "")
            url = item.get("url", "")
        else:
            continue
        if not name or not url:
            continue
        quality = _quality_from_text(name + " " + url)
        out.append(Entry(
            url=url, name=name,
            group=item.get("group", "") if isinstance(item, dict) else "",
            quality=quality, source=src.get("name", ""),
            source_type="static",
            tvg_id=item.get("tvg_id", "") if isinstance(item, dict) else "",
            logo=item.get("logo", "") if isinstance(item, dict) else "",
        ))
    return out


# ═════════════════════════════════════════════════════════════
#  全局 URL 去重
# ═════════════════════════════════════════════════════════════

def _dedup_entries(entries: List[Entry],
                   enabled: bool = True) -> Tuple[List[Entry], dict]:
    """
    全局 URL 去重：同一条 URL 只保留首次出现的条目。
    返回 (去重后列表, 统计)
    """
    if not enabled:
        return entries, {"removed": 0, "kept": len(entries)}
    seen: set = set()
    uniq: List[Entry] = []
    for e in entries:
        key = e.url.strip()
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        uniq.append(e)
    removed = len(entries) - len(uniq)
    log.info(f"🔁 全局 URL 去重：{len(entries)} → {len(uniq)}（去除 {removed} 条重复）")
    return uniq, {"removed": removed, "kept": len(uniq)}


# ═════════════════════════════════════════════════════════════
#  画质识别
# ═════════════════════════════════════════════════════════════

_QUAL_RANK = [
    ("8K", 60), ("4320", 60), ("UHD", 55),
    ("4K", 50), ("2160", 50),
    ("1080", 40), ("FHD", 40),
    ("720", 30), ("HD", 30),
    ("SD", 20),
]
_QUAL_EXTRACT = re.compile(
    r"(?i)(8K|4320P?|UHD|4K|2160P?|FHD|1080P?|720P?|高清|超清|蓝光|原画|HDR|HEVC|H265|H264)"
)


def _quality_from_text(text: str) -> str:
    """从名称/URL 文本中识别画质"""
    if not text:
        return ""
    t = text.upper()
    if "8K" in t or "4320" in t:
        return "8K"
    if "4K" in t or "2160" in t:
        return "4K"
    if "UHD" in t:
        return "4K"
    if re.search(r"1080[PpIi]?", t) or "FHD" in t:
        return "HD"
    if re.search(r"720[PpIi]?", t) or "高清" in text or "HD" in t:
        return "HD"
    if "SD" in t or "标清" in text:
        return "SD"
    return ""


# ═════════════════════════════════════════════════════════════
#  频道归一化（核心：4K/4、8K/8 必须分开）
# ═════════════════════════════════════════════════════════════
#
# 规则说明
#   · 画质词（4K/8K/UHD/HD/…）剥离为独立【画质维度】，追加在 key 末尾；
#   · 剥离画质后剩余的"名称+序号"相同的，画质不同则归为【不同频道】；
#       卫视4K  ≠  卫视4        ← 一个带 4K 画质，一个不带
#       CCTV4K  ≠  CCTV4       ← 同上
#       卫视8K  ≠  卫视8        ← 同上
#   · 画质维度不参与"同频道归组"，只参与"差异记录"。
#
# 反例（你强调必须区分）
#   "4K影视" 与 "影视4" 是完全不同的两个频道名，不会被混淆。
# ═════════════════════════════════════════════════════════════

# 画质词：支持「数字与画质词粘连」写法，如 CCTV4K / 卫视8KHD / 卫视4
# 匹配规则：
#   · 4K / 8K / 4KHD / 8KHD —— 前后可以是字母数字或结尾
#   · UHD / FHD / HEVC / HDR / H265 / H264 —— 同上
#   · HD / SD —— 仅在非数字后匹配，避免把 CCTV4 的尾数字误当 HD
#   · 中文画质词单独匹配
# 数字型画质词（4K/8K/2160P 等）：数字前允许紧跟字母（CCTV4K），
# 但要求数字本身紧邻画质字母，且画质字母后不能再跟字母数字。
_QUAL_WORD = re.compile(
    r"(?i)"
    r"(4KHD|8KHD|4K|8K|UHD|FHD|2160P?|4320P?|1080P?|720P?|HEVC|H265|H264)"
    r"(?![A-Za-z0-9])"
)
# 独立 HD/SD：前面不能是字母或数字，避免把 卫视4 / CCTV4 里的 4 误当画质
_QUAL_HDSD = re.compile(
    r"(?i)"
    r"(?<![A-Za-z0-9])"
    r"(HD|SD)"
    r"(?![A-Za-z0-9])"
)
# 中文画质词
_QUAL_CN = re.compile(r"(高清|超清|蓝光|原画|标清)")


def _norm_num(raw: str) -> str:
    """
    归一化频道序号：去掉前导零，使 CCTV-04 与 CCTV-4 视为同一频道。
    纯 "0" 保留；含 ＋/+ 的序号（如 5+）保留符号只规整数字部分。
    """
    if not raw:
        return ""
    m = re.match(r"^([0-9]*)([＋+]?)$", raw)
    if not m:
        return raw
    digits, plus = m.group(1), m.group(2)
    if digits:
        digits = digits.lstrip("0") or "0"
    return digits + plus


def channel_key(raw: str) -> str:
    """
    频道归一化键。
    返回形如：  cctv/n4/q4K   cctv/n4   ws/湖南卫视/q4K   ws/湖南卫视
    """
    if not raw:
        return ""
    # 统一空白与分隔符
    s = raw.strip()
    # 数字与画质词之间的空格其实是分隔（"CCTV16 4K" → "CCTV16-4K"），
    # 若直接删掉会粘成 CCTV164K，序号被误读成 164。先把这类空格换成连字符。
    s = re.sub(r"(?<=[0-9])[\s\-–—_·]+(?=(?:[0-9]+)?(?:4K|8K|UHD|FHD|HD|SD)(?![A-Za-z]))",
               "-", s, flags=re.IGNORECASE)
    s = re.sub(r"[\s\-–—_·]+", "", s)
    if not s:
        return ""

    # 1) 提取画质维度（保留首次出现顺序，去重）
    spans: List[Tuple[int, int]] = []
    seen_q: set = set()
    for pat in (_QUAL_WORD, _QUAL_HDSD, _QUAL_CN):
        for m in pat.finditer(s):
            q = m.group(1).upper()
            # 归一化中文画质
            if q == "高清" or q == "超清":
                q = "HD"
            elif q in ("蓝光", "原画"):
                q = "FHD"
            elif q == "标清":
                q = "SD"
            if q in seen_q:
                continue
            seen_q.add(q)
            spans.append(m.span())
    spans.sort()
    quals: List[str] = sorted(seen_q)

    # 2) 剥离画质词得到 body
    #    关键：像 4K / 8K / 2160P 这类"数字+字母"的画质词，其中的数字同时
    #    承载着频道序号语义（CCTV-4K 的 4 就是 CCTV4 的 4）。若整体剥离，
    #    CCTV-4K 会退化成 CCTV，与 CCTV-4 撞成同一频道。因此这里只剥离
    #    字母部分，把开头的数字留在 body 里当序号用；画质维度仍完整记录在
    #    quals 中（q4K），靠它把 4K 与 4 区分开。
    # 预处理：把 "4M1080 / 8M / 20M" 这类码率标记整段抹掉。
    # 否则 1080P 规则只吃掉 "1080"，残留裸 "4"，会被误当成频道序号
    # （东方卫视 4M1080 → 东方卫视4），导致同频道的线路被判成串线路。
    s = _RATE_RE.sub(" ", s)
    if not s.strip():
        return ""
    mask = bytearray(len(s))
    for a, b in spans:
        lo, hi = a, min(b, len(s))
        word = s[lo:hi]
        mlead = re.match(r"^([0-9]+)", word)
        if mlead:
            # 情形 A：数字紧邻台名前缀（CCTV4K / CCTV16-4K）→ 该数字就是
            #         频道序号本身，整词剥离，序号由 body 里原有的数字承担。
            # 情形 B：数字前已有别的序号或中文（卫视4K / 东方卫视4K）→
            #         数字是画质词的一部分，保留它当序号，只屏蔽字母。
            # 看紧邻画质词左边的字符：是数字（CCTV-4|K、CCTV16-|4K 经分隔符
            # 归一后紧邻）→ 该数字即序号，整词剥离；否则（卫视4K 的 4 前面
            # 是"视"）保留数字当序号。
            prefix = s[:a]
            tail_digits = re.search(r"([0-9＋+])([0-9＋+\-–—_· ]*)$", prefix)
            if tail_digits and re.search(
                r"(CCTV|CETV|IPTV|卫视)[\s\-–—_·]*$",
                prefix[:tail_digits.start(1)], re.IGNORECASE
            ):
                # 序号紧邻画质词：CCTV-4|K、CCTV16-|4K、卫视4|K → 整词剥离
                lo = a
            else:
                # 序号与画质词之间还隔着台名：东方卫视|4K → 保留 4 当序号
                lo = a + mlead.end()
        for i in range(lo, hi):
            mask[i] = 1
    body = "".join(c for i, c in enumerate(s) if not mask[i])

    # 3) body 拆成 head(字母/中文) + 序号 + 尾缀
    #    序号只取纯数字与「＋/+」（画质词已在前面剥离，这里不能再吞掉）
    head_parts: List[str] = []
    num = ""
    suf = ""

    # CCTV / CETV 类：序号 = 紧跟前缀的数字[＋+]，不含字母。
    # 注意：前缀之后的说明性文字（线路1/备用/电信/高清版/央视频道 等）一律
    # 忽略，不作为频道名的一部分——否则 "CCTV-4K 超清线路1 备用" 会被误判
    # 成一个叫「线路备用」的频道，导致同频道线路互相被当成串线路。
    m = re.match(
        r"^(CCTV|CETV|IPTV|卫视)([0-9＋+]*)"
        r"(?:[\s\-–—_·]+(?![0-9＋+]))?"
        r"(?![0-9A-Za-z])(?!\u4e00-\u9fff)",
        body, re.IGNORECASE
    )
    if m:
        head_parts = [m.group(1).lower()]
        num = _norm_num(m.group(2))
    else:
        # 通用：字母/中文 + 末尾数字[＋+]序号
        m = re.match(r"^([A-Za-z\u4e00-\u9fff]+?)([0-9＋+][0-9＋+]*)"
                     r"(?![0-9A-Za-z])(?!\u4e00-\u9fff)", body)
        if m:
            head = m.group(1)
            num = _norm_num(m.group(2))
            # 把 head 再拆成"中文名 + 末尾英文后缀"（如 芒果tv → 芒果 / tv）
            m2 = re.match(r"^([\u4e00-\u9fff]+)([A-Za-z]+)$", head)
            if m2:
                head_parts = [m2.group(1), m2.group(2).lower()]
            else:
                # 纯中文 / 纯英文
                if re.search(r"[\u4e00-\u9fff]", head):
                    head_parts = list(re.findall(r"[\u4e00-\u9fff]+", head))
                else:
                    head_parts = [head.lower()]
        else:
            # 形如 CCTV测试 / 卫视测试：字母前缀 + 末尾说明性中文
            m3 = re.match(r"^([A-Za-z]+)([\u4e00-\u9fff].*)$", body)
            if m3:
                head_parts = [m3.group(1).lower(), m3.group(2)]
                num = ""
            else:
                # 没有序号，整个 body 就是名称
                if re.search(r"[\u4e00-\u9fff]", body):
                    head_parts = list(re.findall(r"[\u4e00-\u9fff]+", body))
                elif re.search(r"[A-Za-z]", body):
                    head_parts = [body.lower()]
                else:
                    return ""

    if not head_parts and not num:
        return ""

    parts: List[str] = []
    if head_parts:
        parts.append("#".join(head_parts))
    if num:
        parts.append("n" + num)
    if suf:
        parts.append(suf)
    if quals:
        parts.append("q" + "|".join(quals))
    return "/".join(parts)


# ═════════════════════════════════════════════════════════════
#  串线路判定
# ═════════════════════════════════════════════════════════════

def _chan_core_from_key(key: str) -> str:
    parts = [p for p in key.split("/") if p]
    while parts and parts[-1].startswith("q"):
        parts.pop()
    return "/".join(parts)


def canonical_channel_name(ents: List["Entry"], key: str) -> str:
    """
    从同一频道分组的条目里挑出最"干净"的名字作为该组的规范频道名。

    规则：在所有 channel_key(name) == 组 key 的候选名中，取长度最短者。
    最短的名字通常没有"线路1/超清/备用/高清版"之类杂项后缀，最接近真实
    频道名。用它当串线路判定锚点，可避免 ents[0] 恰好是带一堆后缀的名字时，
    把自己组的正常线路误杀。
    """
    key_core = _chan_core_from_key(key)
    best = ""
    for e in ents:
        nm = (e.name or "").strip()
        if not nm or channel_key(nm) != key:
            continue
        # 同 key 的名字里，优先选带画质词的（CCTV-4K 优于 CCTV-4K 超清线路1），
        # 再取最短的，保证锚点自身画质维度不丢
        rq = 0 if re.search(r"(?i)(8K|4K|UHD|FHD|HD|SD)", nm) else 1
        cand = (rq, len(nm), nm)
        if not best or cand < best_sel:
            best, best_sel = nm, cand
    if best:
        return best
    # 组内没有与 key 完全同画质的名字时，退而以"同台名同序号"的最短名字为锚点
    for e in sorted(ents, key=lambda x: len(x.name or "")):
        nm = (e.name or "").strip()
        if nm and _chan_core(nm) == key_core:
            return nm
    return ents[0].name if (ents and ents[0].name) else ""


# 注意：必须先匹配 1080P/2160P 这类"分辨率+字母"，再匹配裸码率 4M/8M，
# 否则 "4M1080" 里的 1080 会被先吃掉，剩下 "4M" 的 4 又被当成频道序号
_RATE_RE = re.compile(r"(?i)(?:[0-9]+(\.[0-9]+)?\s*(?:[Mm]bps|[Mm]i?t/s|[Mm])(?![A-Za-z])|(?<![0-9A-Za-z])[0-9]{3,4}(?![0-9A-Za-z]))")


def _strip_rate(text: str) -> str:
    """抹掉 "4M1080 / 8M / 20Mbps" 这类码率标记，避免残留裸数字被当成序号。"""
    return _RATE_RE.sub(" ", text or "")


def _chan_core(name: str) -> str:
    """取频道名的「台名+序号」核心（去掉码率与画质维度），用于串线路归属判断。"""
    key = channel_key(_strip_rate(name))
    # key 形如 台名/序号/画质…：画质段永远是最后一段（以 q 开头），去掉它即可
    parts = [p for p in key.split("/") if p]
    while parts and parts[-1].startswith("q"):
        parts.pop()
    return "/".join(parts)


def is_likely_cross_wire(entry: Entry, anchor_name: str) -> bool:
    """
    判定 entry 是否为"串线路"——即它的名称/画质与所属频道不符。

    判定唯一事实来源是 channel_key()：只要线路名归一化后的 key 与所属
    频道 key 不同，即视为串线路。这样"4K 与 4 分家""8K 与 8 分家"等
    规则只需在 channel_key 里维护一份，本函数不会再与归一化逻辑漂移。

    仅有的豁免：线路名缺失时无法判定，保守返回 False（不丢弃）。
    """
    if not anchor_name or not entry.name:
        return False
    a = anchor_name.strip()
    n = entry.name.strip()
    if a == n:
        return False

    # 判定只看「台名 + 序号」，忽略画质维度：同一频道的 4K / 8K / HD / 无画质
    # 线路互为不同 key 是刻意设计（用于分开呈现），但它们仍是同一个台，
    # 不能互相判成串线路。
    # 1080P?/2160P? 等后缀要整体匹配，避免 "4M1080" 里的 1080 被先拆成
    # 画质词后残留裸 "4"，被误当成频道序号
    _QUAL_AWARE = re.compile(
        r"(?i)(8K|4320P|UHD|4K|2160P|FHD|1080P|720P|高清|超清|蓝光|原画|HDR|HEVC|H265|H264)"
    )
    stripped = lambda t: _QUAL_AWARE.sub(" ", _strip_rate(t))
    a_quals = set(_QUAL_EXTRACT.findall(stripped(a)))
    n_quals = set(_QUAL_EXTRACT.findall(stripped(n)))
    if _chan_core(stripped(n)) == _chan_core(stripped(a)):
        # 台名+序号相同 → 属同一频道，无论画质是否一致都不算串线路。
        # 同一频道的 4K / 8K / HD / 无画质线路互为不同 key 是刻意设计
        # （用于分开呈现），但源站常把低码率线路误标成 4K，若按"有无画质"
        # 判定会大量误杀，故此处一律放行。
        return False
    # 台名+序号不同 → 属于不同频道 → 串线路
    return True



_QUAL_ALL = re.compile(
    r"(?i)(4K|8K|UHD|2160|4320|FHD|1080|720|HD|SD|高清|超清|蓝光|原画|标清)"
)


def _strip_all_qual(s: str) -> str:
    if not s:
        return ""
    return re.sub(_QUAL_ALL, "", s).strip().lower()


def _channel_keywords(s: str) -> set:
    """
    提取频道名里的"频道标识"集合，用于判断两个名称是否属于同一频道。
    这里的标识必须足够"具体"，命中即认为是同一个频道/台。
    """
    if not s:
        return set()
    s2 = s.upper()
    kw = set()

    # CCTV / CETV 系列：用「前缀 + 序号 + 画质」作为标识
    #    CCTV4 / CCTV4K / CCTV5+ / CCTV8K 各自独立，避免互相误判为同频道
    m = re.match(r"^(CCTV|CETV|IPTV)\s*([0-9A-Z＋+]*)\s*(.*)$", s2)
    if m:
        prefix = m.group(1).lower()
        num = m.group(2)
        rest = m.group(3)
        qual = "".join(re.findall(r"4K|8K|UHD|HD|SD|FHD", rest)).upper()
        if num or qual:
            kw.add((prefix + num + qual).lower())
        else:
            kw.add(prefix)

    # 卫视：支持两种结构
    #   · 卫视在前：卫视 + 序号 + 画质  （如 卫视4K / 卫视8 / 卫视4）
    #   · 卫视在后：地域 + 卫视 + 序号 + 画质  （如 湖南卫视4K / 浙江卫视）
    # 避免把 卫视4 / 卫视8 / 卫视4K 当成同一频道
    m = re.match(r"^(卫视)\s*([0-9＋+]*)\s*(.*)$", s)
    if m:
        base = "卫视"
        num = m.group(2)
        rest = m.group(3)
    else:
        m = re.match(r"^([\u4e00-\u9fff]{2,4})卫视\s*([0-9A-Za-z＋+]*)(.*)$", s)
        if m:
            base = m.group(1) + "卫视"
            num = m.group(2)
            rest = m.group(3)
        else:
            m = None
    if m:
        # 序号或画质存在时，整体作为唯一标识
        if num or re.search(_QUAL_ALL, rest):
            qual_part = "".join(re.findall(r"4K|8K|UHD|HD|SD|FHD", rest)).upper() or ""
            kw.add((base + num + qual_part).lower())
        else:
            kw.add(base)

    # 单台/品牌台
    for pat in ("凤凰卫视", "星空卫视", "翡翠台", "明珠台", "华娱卫视",
                "TVBS", "东风卫视", "新知台", "芒果TV", "百视TV",
                "东方卫视", "北京卫视", "上海卫视", "深圳卫视",
                "浙江卫视", "江苏卫视", "湖南卫视", "安徽卫视",
                "广东卫视", "山东卫视", "四川卫视", "天津卫视",
                "重庆卫视", "湖北卫视", "河南卫视", "河北卫视",
                "辽宁卫视", "吉林卫视", "黑龙江卫视", "广西卫视",
                "云南卫视", "贵州卫视", "海南卫视", "陕西卫视",
                "甘肃卫视", "江西卫视", "山西卫视", "福建卫视"):
        if pat.upper() in s2:
            kw.add(pat.lower())

    return kw


# ═════════════════════════════════════════════════════════════
#  阶段一：抓取 + 整理 + 去重
# ═════════════════════════════════════════════════════════════

def _entry_source_rank(e: Entry, cfg: dict = None) -> int:
    """源优先级：static > local > network，国内源再加分（国内源先测）"""
    st = (e.source_type or "").lower()
    if st == "static":
        score = 100
    elif st == "local":
        score = 80
    elif e.source and "static" in e.source.lower():
        score = 95
    else:
        score = 50
    if _is_domestic(e.url) and cfg is not None:
        score += int((cfg.get("live", {}) or {}).get("domestic_bonus", 15) or 0)
    return score


def _entry_quality_rank(e: Entry) -> int:
    """画质优先级分值"""
    q = (e.quality or "").upper()
    for pat, score in _QUAL_RANK:
        if pat in q:
            return score
    return 0


# 国内（APNIC/CNNIC 分配给中国大陆）常见 IPv4 段，CIDR 写法。
# 仅用于排序：命中的排前面先测；没命中的不会被丢弃，只是排后面测。
_CN_CIDRS = """
1.0.1.0/24 1.0.2.0/23 1.0.8.0/21 1.0.32.0/19 1.1.0.0/24 1.1.2.0/23 1.1.4.0/22
1.1.8.0/21 1.1.16.0/20 1.2.0.0/23 1.2.2.0/23 1.2.4.0/22 1.2.8.0/21 1.2.16.0/20
1.4.1.0/24 1.8.0.0/16 1.12.0.0/14 1.24.0.0/13 1.48.0.0/12 1.68.0.0/14
1.80.0.0/12 1.116.0.0/15 1.118.0.0/16 1.119.0.0/24 1.180.0.0/14 1.184.0.0/15
1.192.0.0/13 1.200.0.0/13 8.128.0.0/10 14.0.0.0/9 14.128.0.0/9 27.0.128.0/17
27.8.0.0/13 27.16.0.0/12 27.32.0.0/12 27.48.0.0/14 27.54.0.0/17 27.98.176.0/22
27.98.208.0/20 27.99.128.0/17 27.103.0.0/16 27.106.128.0/17 27.112.0.0/12
27.128.0.0/11 27.159.232.0/21 27.184.0.0/13 27.192.0.0/11 36.0.0.0/10 36.96.0.0/11
36.128.0.0/10 36.192.0.0/11 36.224.0.0/12 36.248.0.0/13 39.0.0.0/8 42.0.0.0/9
42.128.0.0/9 43.224.0.0/11 47.92.0.0/14 47.96.0.0/11 49.64.0.0/10 49.128.0.0/9
51.0.0.0/16 52.80.0.0/12 52.128.0.0/9 54.222.0.0/15 56.0.0.0/9 57.240.0.0/12
58.16.0.0/13 58.24.0.0/15 58.30.0.0/15 58.32.0.0/11 58.64.0.0/12 58.80.0.0/13
58.96.0.0/12 58.112.0.0/12 58.128.0.0/13 58.144.0.0/12 58.192.0.0/11 59.32.0.0/11
59.64.0.0/10 60.0.0.0/11 60.55.0.0/16 60.63.0.0/16 60.160.0.0/11 60.194.0.0/15
60.200.0.0/13 60.208.0.0/12 60.232.0.0/13 60.240.0.0/12 61.4.80.0/20 61.8.0.0/13
61.16.0.0/12 61.48.0.0/13 61.128.0.0/10 61.240.0.0/13 101.0.0.0/12 101.16.0.0/12
101.32.0.0/11 101.64.0.0/10 101.128.0.0/9 101.224.0.0/12 101.248.0.0/16
103.1.168.0/22 103.3.132.0/22 103.5.80.0/22 103.6.216.0/22 103.9.140.0/22
103.15.200.0/21 103.16.16.0/20 103.20.112.0/21 103.21.148.0/22 103.22.8.0/21
103.22.136.0/22 103.23.32.0/21 103.24.192.0/22 103.25.8.0/21 103.30.0.0/15
103.37.0.0/16 103.39.16.0/20 103.40.12.0/22 103.43.200.0/22 103.47.200.0/21
103.48.108.0/22 103.49.124.0/22 103.51.32.0/21 103.52.180.0/22 103.53.4.0/22
103.53.144.0/21 103.54.36.0/22 103.55.152.0/22 103.56.48.0/20 103.57.152.0/22
103.59.148.0/22 103.60.56.0/21 103.62.80.0/21 103.62.160.0/22 103.62.210.0/23
106.0.0.0/8 107.0.0.0/8 110.16.0.0/12 110.32.0.0/11 110.64.0.0/10 110.128.0.0/9
111.0.0.0/10 111.64.0.0/11 111.96.0.0/12 111.112.0.0/12 111.128.0.0/9 112.0.0.0/10
112.64.0.0/11 112.96.0.0/11 112.128.0.0/9 113.0.0.0/13 113.8.0.0/15 113.11.0.0/16
113.12.0.0/14 113.16.0.0/13 113.24.0.0/14 113.44.0.0/14 113.48.0.0/12 113.64.0.0/10
113.128.0.0/9 114.28.0.0/15 114.30.0.0/16 114.32.0.0/11 114.64.0.0/12 114.80.0.0/13
114.96.0.0/11 114.128.0.0/9 115.24.0.0/13 115.32.0.0/11 115.64.0.0/10 115.128.0.0/9
116.0.0.0/13 116.8.0.0/14 116.13.0.0/16 116.16.0.0/12 116.32.0.0/11 116.64.0.0/10
116.128.0.0/9 117.8.0.0/13 117.21.0.0/17 117.22.0.0/15 117.24.0.0/13 117.32.0.0/11
117.64.0.0/10 117.128.0.0/9 118.24.0.0/13 118.64.0.0/10 118.112.0.0/12 118.128.0.0/9
119.0.0.0/13 119.8.0.0/16 119.16.0.0/12 119.32.0.0/11 119.64.0.0/10 119.128.0.0/9
120.0.0.0/12 120.16.0.0/16 120.24.0.0/13 120.32.0.0/11 120.64.0.0/10 120.128.0.0/9
121.0.0.0/9 121.128.0.0/10 121.192.0.0/11 121.224.0.0/12 122.0.0.0/9
122.128.0.0/10 122.192.0.0/11 123.0.0.0/12 123.16.0.0/13 123.24.0.0/15
123.50.0.0/16 123.52.0.0/14 123.56.0.0/13 123.64.0.0/11 123.96.0.0/15
123.98.0.0/16 123.99.0.0/17 123.100.0.0/16 123.101.0.0/17 123.108.0.0/14
123.112.0.0/12 123.128.0.0/9 123.244.0.0/14 123.248.0.0/13 124.6.0.0/15
124.8.0.0/13 124.16.0.0/12 124.32.0.0/11 124.64.0.0/10 124.128.0.0/9
125.31.192.0/18 125.32.0.0/11 125.64.0.0/10 125.128.0.0/9 126.0.0.0/8
139.0.0.0/16 139.2.0.0/15 139.4.0.0/14 139.8.0.0/13 139.16.0.0/12 139.32.0.0/11
139.64.0.0/10 139.128.0.0/9 140.64.0.0/10 140.128.0.0/9 140.240.0.0/12
144.0.0.0/9 150.0.0.0/10 150.64.0.0/11 150.96.0.0/12 150.112.0.0/13
153.0.0.0/16 153.2.0.0/15 153.4.0.0/14 153.8.0.0/13 153.16.0.0/12 153.32.0.0/11
153.64.0.0/10 153.128.0.0/9 157.0.0.0/16 157.61.0.0/17 157.66.0.0/16 157.68.0.0/14
157.72.0.0/13 157.80.0.0/12 157.96.0.0/11 157.128.0.0/9 159.226.0.0/16
161.207.0.0/16 162.105.0.0/16 163.0.0.0/8 164.52.0.0/14 166.111.0.0/16
167.139.0.0/16 167.189.64.0/18 168.160.0.0/11 169.65.32.0/19 171.8.0.0/13
171.16.0.0/12 171.32.0.0/11 171.64.0.0/10 171.104.0.0/13 171.112.0.0/12
171.208.0.0/12 175.0.0.0/12 175.16.0.0/13 175.24.0.0/14 175.30.0.0/15
175.42.0.0/15 175.44.0.0/14 175.48.0.0/12 175.64.0.0/11 175.96.0.0/10
175.128.0.0/9 176.58.96.0/20 177.136.0.0/13 180.64.0.0/10 180.128.0.0/9
182.0.0.0/12 182.16.0.0/13 182.24.0.0/14 182.32.0.0/11 182.64.0.0/10
182.128.0.0/9 183.0.0.0/10 183.64.0.0/11 183.96.0.0/12 183.112.0.0/12
183.128.0.0/9 185.66.16.0/22 202.0.128.0/17 202.4.128.0/19 202.5.208.0/20
202.6.176.0/20 202.8.0.0/16 202.10.192.0/19 202.12.0.0/16 202.14.0.0/16
202.20.0.0/16 202.21.0.0/16 202.27.184.0/21 202.38.0.0/16 202.41.0.0/16
202.43.144.0/20 202.45.80.0/20 202.46.160.0/19 202.62.0.0/15 202.64.0.0/15
202.73.160.0/19 202.75.208.0/20 202.84.0.0/15 202.86.192.0/19 202.90.0.0/15
202.92.0.0/14 202.96.0.0/12 202.112.0.0/13 202.120.0.0/13 202.136.0.0/13
202.144.0.0/12 202.160.0.0/12 202.176.0.0/12 202.192.0.0/11 203.0.4.0/22
203.0.8.0/21 203.1.16.0/20 203.2.112.0/20 203.4.128.0/19 203.5.0.0/16
203.6.0.0/15 203.8.0.0/16 203.9.0.0/16 203.10.0.0/16 203.11.128.0/17
203.12.0.0/16 203.14.0.0/16 203.15.160.0/19 203.16.0.0/16 203.17.128.0/17
203.18.0.0/16 203.19.0.0/16 203.20.0.0/14 203.24.0.0/14 203.28.0.0/15
203.30.0.0/16 203.31.0.0/17 203.56.0.0/16 203.57.0.0/16 203.62.0.0/16
203.76.0.0/16 203.80.0.0/12 203.86.0.0/16 203.88.0.0/15 203.90.0.0/16
203.91.0.0/17 203.92.0.0/16 203.93.0.0/16 203.94.0.0/15 203.96.0.0/12
203.112.0.0/13 203.128.0.0/13 203.144.0.0/12 203.160.0.0/12 203.176.0.0/12
203.196.0.0/14 203.207.128.0/17 210.2.0.0/15 210.5.0.0/16 210.7.0.0/16
210.12.0.0/16 210.14.0.0/15 210.16.0.0/14 210.21.0.0/16 210.22.0.0/15
210.25.0.0/16 210.26.0.0/15 210.32.0.0/14 210.36.0.0/16 210.40.0.0/13
210.48.0.0/14 210.52.0.0/15 210.56.0.0/14 210.72.0.0/14 210.76.0.0/15
210.78.0.0/16 210.79.64.0/18 210.82.0.0/15 210.87.128.0/17 210.192.0.0/11
211.64.0.0/11 211.96.0.0/12 211.136.0.0/13 211.144.0.0/12 211.160.0.0/13
218.0.0.0/11 218.32.0.0/11 218.64.0.0/10 218.128.0.0/9 219.72.0.0/13
219.80.0.0/12 219.128.0.0/9 220.160.0.0/11 220.192.0.0/10 221.0.0.0/12
221.16.0.0/13 221.128.0.0/9 222.16.0.0/12 222.32.0.0/11 222.64.0.0/10
222.128.0.0/9 223.0.0.0/12 223.20.0.0/15 223.64.0.0/10 223.96.0.0/12
223.112.0.0/12 223.128.0.0/9 223.240.0.0/13 223.248.0.0/14 223.252.0.0/15
223.254.0.0/16 223.255.252.0/22
"""
# 国内运营商常见 IPv6 前缀（电信 240e、联通 2408、移动 2409、教育网 2001:da8）
_CN_IPV6_PREFIXES = ("240e", "2408", "2409", "2001:da8")
_CN_HOST_KW = ("telecom", "unicom", "cmcc", "chinamobile", "chinatelecom",
               "iptv", "hntv", "jsbc", "smgbb", "wasu", "cctv",
               "cntv", "hls.cctv", "qq.com", "huya", "douyu", "bilibili",
               ".cn", "zgds", "dianshi")

_CN_NETS = tuple(
    (ipaddress.ip_network(c, strict=False), int(c.split("/")[1]))
    for c in _CN_CIDRS.split() if c
)


def _host_of(url: str) -> str:
    m = re.search(r"://\[?([^/\]]+)", url or "")
    return (m.group(1) if m else "").lower().strip(".")


def _is_domestic_ip(host: str) -> bool:
    if ":" in host:                       # IPv6
        return host.startswith(_CN_IPV6_PREFIXES)
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    if ip.version != 4 or ip.is_private or ip.is_loopback:
        return False
    return any(ip in net for net, _ in _CN_NETS)


def _is_domestic(url: str) -> bool:
    """判断 URL 是否国内源：先按 IP 段，再按域名关键词"""
    host = _host_of(url)
    if not host:
        return False
    if _is_domestic_ip(host):
        return True
    return any(k in host for k in _CN_HOST_KW)


def _looks_like_playlist(text: str) -> bool:
    """判断抓取到的内容是否像一份可用的播放列表。

    用于缓存入库前的校验，避免把空响应、HTML 错误页、
    登录页等坏内容写进缓存后长期复用。
    """
    if not text or len(text.strip()) < 32:
        return False
    head = text.lstrip()[:2048]
    if "#EXTM3U" in head:
        return True
    # 纯文本清单：每行一个地址
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return False
    urlish = sum(1 for l in lines[:50]
                 if re.match(r'^(https?|rtmp|rtsp|udp|rts)://', l))
    return urlish >= max(1, len(lines[:50]) // 2)


def fetch_and_dedup(
    cfg: dict) -> Tuple[Dict[str, List[Entry]], List[dict], dict]:
    """
    阶段一：抓取 → 解析 → 全局去重 → 按频道归一化分组。
    返回 (channels, source_reports, stats)
        channels: { channel_key: [Entry, ...] }
    """
    log.info("=" * 60)
    log.info("阶段一：抓取 + 整理 + 去重")
    t0 = time.time()

    entries, src_reports = discover_live(cfg)
    _cstat = _LAST_CACHE_STATS or {}
    log.info(f"📥 原始抓取合计：{len(entries)} 条直播线路"
             f"（来自 {len(src_reports)} 个源）")

    # 全局 URL 去重
    enabled = bool(cfg.get("live", {}).get("url_dedup", True))
    entries, dedup_stats = _dedup_entries(entries, enabled=enabled)

    # 按频道归一化键分组
    channels: Dict[str, List[Entry]] = defaultdict(list)
    unnamed = 0
    for e in entries:
        if not e.name:
            unnamed += 1
            continue
        key = channel_key(e.name)
        if not key:
            continue
        e.meta["key"] = key
        channels[key].append(e)

    log.info(f"📊 整理结果：{len(channels)} 个频道 / {len(entries)} 条去重后线路")
    if unnamed:
        log.warning(f"   ⚠ {unnamed} 条无频道名，已丢弃")

    # 单频道候选排序：source 优先 + quality 优先 + 国内源优先
    for key in channels:
        channels[key].sort(key=lambda e: (
            -_entry_source_rank(e, cfg),
            -_entry_quality_rank(e),
            0 if _is_domestic(e.url) else 1,
            len(e.url),
        ))

    # 截断（调试用）
    max_ch = Flags.MAX_FETCH_CHANNELS or cfg.get("live", {}).get("max_fetch_channels", 0)
    max_url = Flags.MAX_FETCH_URLS or cfg.get("live", {}).get("max_fetch_urls", 0)
    if max_ch and len(channels) > max_ch:
        keep = sorted(channels.items(), key=lambda kv: -len(kv[1]))[:max_ch]
        channels = dict(keep)
        log.info(f"   (调试) 频道数截断至 {max_ch}")
    if max_url:
        for k in channels:
            if len(channels[k]) > max_url:
                channels[k] = channels[k][:max_url]

    elapsed = round(time.time() - t0, 1)
    stats = {
        "fetched": sum(r.get("entries", 0) for r in src_reports),
        "deduped": len(entries),
        "removed": dedup_stats.get("removed", 0),
        "channels": len(channels),
        "elapsed": elapsed,
        "cache_enabled": bool(_cstat.get("enabled", cache_on)),
        "cache_hits": _cstat.get("hits", 0),
        "cache_stale_hits": _cstat.get("stale_hits", 0),
        "cache_writes": _cstat.get("writes", 0),
        "cache_rejected": _cstat.get("rejected", 0),
    }
    if _cstat.get("enabled"):
        cache_line = (f"｜缓存 命中{_cstat.get('hits', 0)} "
                      f"过期退化{_cstat.get('stale_hits', 0)} "
                      f"新写入{_cstat.get('writes', 0)}")
        if _cstat.get("rejected"):
            cache_line += f" 拒写{_cstat.get('rejected')}"
    else:
        cache_line = "｜缓存 关闭"
    log.info(f"📊 阶段一完成（{elapsed}s）{cache_line}")
    return channels, src_reports, stats


# ═════════════════════════════════════════════════════════════
#  阶段二：探测 + 精选
# ═════════════════════════════════════════════════════════════

def _write_fetch_report(outdir: Path, src_reports: List[dict], dedup: dict) -> None:
    payload = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "dedup": dedup,
        "sources": src_reports,
    }
    fp = outdir / "fetch_report.json"
    fp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"📋 抓取报告 → {fp}")


def _write_raw_channels(outdir: Path, channels: Dict[str, List[Entry]]) -> None:
    data = []
    for key, ents in channels.items():
        main = ents[0]
        data.append({
            "key": key,
            "name": main.name,
            "quality": main.quality,
            "group": main.group,
            "candidates": len(ents),
            "sources": sorted({e.source for e in ents}),
            "quality_list": sorted({e.quality for e in ents if e.quality}),
        })
    data.sort(key=lambda d: (-d["candidates"], d["name"]))
    fp = outdir / "raw_channels.json"
    fp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"📋 原始频道清单 → {fp}（{len(data)} 个频道）")


def _candidate_quota(cfg: dict) -> Tuple[int, int]:
    """每频道送入探测的候选数区间（probe.candidate_pool.min/max）"""
    p = cfg.get("probe", {}).get("candidate_pool", {}) or {}
    return int(p.get("min", 10)), int(p.get("max", 20))


# 单频道保留总路数硬上限（防止配置写大把 m3u 撑爆）
_HARD_KEEP_CAP = 12


def _quota_map(cfg: dict) -> Dict[int, int]:
    """取当前生效的清晰度配额 {2160: n, 1080: n, 720: n}

    晚高峰（live.peak_hours）用 res_quotas_peak，否则用 res_quotas；
    前者缺失时沿用后者。
    """
    live = cfg.get("live", {}) or {}
    peak_hours = live.get("peak_hours", [19, 20, 21, 22, 23]) or []
    key = "res_quotas_peak" if dt.datetime.now().hour in peak_hours else "res_quotas"
    raw = live.get(key) or live.get("res_quotas") or {}
    out: Dict[int, int] = {}
    for k, v in raw.items():
        try:
            out[int(k)] = int(v)
        except (TypeError, ValueError):
            continue
    return out


def _quota_for(quota: Dict[int, int], quality: str) -> int:
    """某画质档在本频道里的保留名额

    4K → 2160 档；1080p/720p 都归 HD，取 1080 与 720 两档之和；
    8K 无对应配置档，按 4K 档处理；SD 不给名额。
    """
    if quality == "8K":
        return int(quota.get(2160, 4))
    if quality == "4K":
        return int(quota.get(2160, 4))
    if quality == "HD":
        return int(quota.get(1080, 5)) + int(quota.get(720, 3))
    return 0


def keep_limit(cfg: dict, qualities: Sequence[str]) -> int:
    """本频道最终保留上限 = 各画质档名额之和，封顶 12"""
    quota = _quota_map(cfg)
    total = sum(_quota_for(quota, q) for q in qualities)
    return max(1, min(total, _HARD_KEEP_CAP))


def per_source_cap(cfg: dict) -> int:
    """单个来源最多贡献几路：至少 1，最多等于总名额"""
    quota = _quota_map(cfg)
    return max(1, sum(quota.values()) or 1)


def _select_candidates(entries: List[Entry], quality: str,
                       qmin: int, qmax: int) -> List[Entry]:
    """按画质偏好取候选：目标画质优先，兼顾其它清晰度兜底"""
    pool: List[Entry] = []
    pool += [e for e in entries if e.quality == quality]
    if quality == "8K":
        pool += [e for e in entries if e.quality == "4K"]
    else:
        pool += [e for e in entries if e.quality == "HD"]
    pool += [e for e in entries if e.quality == "SD"]
    # 保序去重
    seen: set = set()
    uniq: List[Entry] = []
    for e in pool:
        if e.url in seen:
            continue
        seen.add(e.url)
        uniq.append(e)
    target_cnt = sum(1 for e in entries if e.quality == quality)
    want = max(qmin, min(qmax, target_cnt * 2 + 5))
    return uniq[:want]


def _domestic_first(entries: List[Entry]) -> List[Entry]:
    """国内源排前，其余保持原序"""
    domestic = [e for e in entries if _is_domestic(e.url)]
    other = [e for e in entries if not _is_domestic(e.url)]
    return domestic + other


def _http_probe(session: requests.Session, url: str, timeout: int) -> ProbeResult:
    """HTTP 探测：看状态码 + 响应头/首部内容判定是否为直播流"""
    res = ProbeResult(url=url)
    try:
        r = session.get(url, timeout=timeout, stream=True, allow_redirects=True)
        res.status = r.status_code
        res.redirect_url = r.url if r.url != url else ""
        if r.status_code != 200:
            res.err = f"HTTP {r.status_code}"
            return res
        ctype = (r.headers.get("Content-Type") or "").lower()
        chunk = next(r.iter_content(chunk_size=4096), b"") or b""
        text_head = chunk.decode("utf-8", errors="replace")
        is_stream = any(k in ctype for k in ("mpegurl", "mp2t", "octet-stream",
                                              "video", "x-flv", "dash"))
        is_playlist = "#EXTM3U" in text_head[:200] or ".ts" in text_head[:500]
        if is_stream or is_playlist:
            res.ok = True
            res.elapsed = round(r.elapsed.total_seconds(), 2)
        else:
            res.err = f"not-a-stream({ctype[:30]})"
        res.quality = _quality_from_text(url + " " + text_head[:200])
        return res
    except requests.exceptions.RequestException as e:
        res.err = f"{type(e).__name__}"
        return res
    except Exception as e:  # noqa
        res.err = f"{type(e).__name__}: {e}"
        return res


def _ffmpeg_probe(url: str, timeout: int) -> ProbeResult:
    """ffmpeg 拉流探测（能识别"200 但空内容"的假活源）"""
    res = ProbeResult(url=url)
    try:
        import subprocess
        out = subprocess.run(
            ["ffmpeg", "-v", "error", "-i", url, "-t", "3", "-f", "null", "-"],
            capture_output=True, text=True, timeout=timeout + 2,
        )
        combined = (out.stderr or "") + (out.stdout or "")
        if "frame=" in combined and "Error" not in combined and "Invalid" not in combined:
            m = re.search(r"time=(\d+):(\d+):([\d.]+)", combined)
            elapsed = 0.0
            if m:
                h, m2, s = int(m.group(1)), int(m.group(2)), float(m.group(3))
                elapsed = h * 3600 + m2 * 60 + s
            res.ok = True
            res.elapsed = round(elapsed, 2)
            res.quality = _quality_from_text(url + " " + combined[:300])
        else:
            err_line = (combined.strip().splitlines() or ["unknown"])[-1][:120]
            res.err = err_line
        return res
    except subprocess.TimeoutExpired:
        res.ok = True
        res.elapsed = timeout
        return res
    except FileNotFoundError:
        res.err = "ffmpeg not installed"
        return res
    except Exception as e:  # noqa
        res.err = f"{type(e).__name__}: {e}"
        return res


def _ffmpeg_available() -> bool:
    try:
        import subprocess
        subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
        return True
    except Exception:  # noqa
        return False


def probe_url(url: str, cfg: dict, use_ffmpeg: bool = False) -> ProbeResult:
    timeout = int(cfg.get("probe", {}).get("timeout", 10))
    session = make_session(retries=1, backoff=0.5, timeout=timeout)
    if use_ffmpeg and _ffmpeg_available():
        return _ffmpeg_probe(url, timeout)
    return _http_probe(session, url, timeout)


def _probe_batch(urls: Sequence[str], cfg: dict,
                 use_ffmpeg: bool) -> Dict[str, ProbeResult]:
    """带并发的批量探测，返回 {url: ProbeResult}"""
    max_workers = max(int(cfg.get("probe", {}).get("concurrency", 20)), 1)
    results: Dict[str, ProbeResult] = {}
    if not urls:
        return results
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(probe_url, u, cfg, use_ffmpeg): u for u in urls}
            done = 0
            total = len(futs)
            for fut in as_completed(futs):
                u = futs[fut]
                try:
                    results[u] = fut.result()
                except Exception as e:  # noqa
                    results[u] = ProbeResult(url=u, err=str(e))
                done += 1
                if done % 200 == 0 or done == total:
                    ok_n = sum(1 for v in results.values() if v.ok)
                    log.info(f"   探测进度 {done}/{total}（当前存活 {ok_n}）")
    except ImportError:
        for u in urls:
            results[u] = probe_url(u, cfg, use_ffmpeg)
    return results


def aggregate_live(cfg: dict) -> dict:
    """
    直播源聚合主流程。
    返回统计字典。
    """
    outdir = Path(cfg.get("output_dir", "output"))
    outdir.mkdir(parents=True, exist_ok=True)
    use_ffmpeg = bool(cfg.get("probe", {}).get("use_ffmpeg", False))

    log.info("=" * 60)
    log.info("🎬 直播源聚合开始")
    log.info(f"   输出目录：{outdir.resolve()}")
    log.info(f"   探测方式：{'ffmpeg' if use_ffmpeg else 'http'}")
    t_total = time.time()

    # ── 阶段一 ──
    channels, src_reports, st1 = fetch_and_dedup(cfg)
    _write_fetch_report(outdir, src_reports,
                        {"removed": st1["removed"], "deduped": st1["deduped"]})
    _write_raw_channels(outdir, channels)

    if not channels:
        log.error("没有可用频道，终止")
        return st1

    # ── 阶段二 ──
    log.info("=" * 60)
    log.info("阶段二：探测 + 精选")
    qmin, qmax = _candidate_quota(cfg)
    quota = _quota_map(cfg)
    live_cfg = cfg.get("live", {}) or {}
    peak = dt.datetime.now().hour in (live_cfg.get("peak_hours") or [])
    log.info(f"📊 探测候选池：每频道最多 {qmax} 条（国内源先测）")
    log.info(f"📊 保留配额[{'晚高峰' if peak else '白天'}]："
             f"4K×{quota.get(2160, 4)} + 1080p×{quota.get(1080, 5)} "
             f"+ 720p×{quota.get(720, 3)} = {sum(quota.values())} 路封顶")

    all_candidates: List[str] = []
    channel_plan: List[Tuple[str, str, List[Entry]]] = []
    for key, ents in channels.items():
        quality = ents[0].quality or "HD"
        cands = _select_candidates(ents, quality, qmin, qmax)
        cands = _domestic_first(cands)
        # 串线路过滤
        anchor_name = canonical_channel_name(ents, key)
        clean = [e for e in cands if not is_likely_cross_wire(e, anchor_name)]
        if len(clean) != len(cands):
            log.debug(f"   {anchor_name} 剔除 {len(cands)-len(clean)} 条疑似串线路")
        channel_plan.append((key, anchor_name, clean))
        all_candidates.extend(e.url for e in clean)

    # ── 探测结果缓存：TTL 内且历史稳定的线路直接复用，省掉大量重复请求 ──
    pcfg = cfg.get("probe", {}) or {}
    probe_cache_on = bool(pcfg.get("cache", True))
    pttl = int(pcfg.get("cache_ttl_seconds", 600) or 0)
    window = int(pcfg.get("history_window", 20) or 20)
    store: dict = {}
    results: Dict[str, ProbeResult] = {}
    if probe_cache_on and not Flags.DRY_RUN:
        store = load_probe_cache(cfg)
        for u in all_candidates:
            hit = probe_cache_get(store, u, pttl, window)
            if hit is not None:
                hit._window = window
                results[u] = hit
        if results:
            log.info(f"💾 探测缓存命中 {len(results)} 条"
                     f"（TTL {pttl}s / 窗口 {window} 次）")

    todo = [u for u in all_candidates if u not in results]
    total_probe = len(all_candidates)
    log.info(f"📊 待探测：{total_probe} 条 / {len(channels)} 频道"
             + (f"（实探 {len(todo)}，复用 {len(results)}）" if results else ""))

    if Flags.DRY_RUN:
        log.info("🔸 DRY-RUN，跳过实际探测")
        results = {}
    else:
        if todo:
            fresh = _probe_batch(todo, cfg, use_ffmpeg)
            for u, r in fresh.items():
                r._window = window
            results.update(fresh)
            if probe_cache_on:
                probe_cache_update(store, fresh)
                dropped = probe_cache_purge(store, max(pttl * 3, 0))
                save_probe_cache(cfg, store)
                if dropped:
                    log.info(f"🧹 探测缓存清理失效记录 {dropped} 条")
        elif probe_cache_on:
            save_probe_cache(cfg, store)

    alive_total = sum(1 for v in results.values() if v.ok)
    cached_alive = sum(1 for v in results.values() if v.ok and v.from_cache)
    log.info(f"📊 探测完成：{alive_total}/{total_probe} 存活"
             + (f"（其中 {cached_alive} 条来自缓存）" if cached_alive else ""))

    # ── 精选 ──
    live_out: List[Entry] = []
    quality_cnt: Dict[str, int] = defaultdict(int)
    fail_channels: List[str] = []
    quota = _quota_map(cfg)
    cap_src = per_source_cap(cfg)
    for key, cname, cands in channel_plan:
        if not cands:
            fail_channels.append(cname)
            continue
        # 存活线路按来源分组
        by_src: Dict[str, List[Entry]] = defaultdict(list)
        for e in cands:
            r = results.get(e.url)
            if r and r.ok:
                e.meta["elapsed"] = r.elapsed
                e.meta["status"] = r.status
                by_src[e.source].append(e)
        if not by_src:
            fail_channels.append(cname)
            continue

        # 源优先级：静态/国内源在前；源内按画质高→响应快排序
        src_order = sorted(
            by_src.keys(),
            key=lambda s: -max(_entry_source_rank(e, cfg) for e in by_src[s])
        )
        for s in src_order:
            by_src[s].sort(key=lambda e: (-_entry_quality_rank(e),
                                          e.meta.get("elapsed", 999)))

        # 按画质档名额填充：先 4K(2160) → 再 1080p → 再 720p
        keep = keep_limit(cfg, [e.quality or "" for e in cands])
        picked: List[Entry] = []
        used_src: Dict[str, int] = defaultdict(int)
        for want_q in ("4K", "HD"):
            slot = _quota_for(quota, want_q)
            if slot <= 0:
                continue
            # 每个源里只挑本档画质的线路，再按源优先级轮转取 1 条，
            # 避免高档位被单个源吃满，也避免某源本档线路靠后时被误跳过
            pool: Dict[str, List[Entry]] = {
                s: [e for e in by_src[s] if (e.quality or "") == want_q][:cap_src]
                for s in src_order
            }
            progressed = True
            while sum(1 for p in picked if (p.quality or "") == want_q) < slot and progressed:
                progressed = False
                for src in src_order:
                    if len(picked) >= keep:
                        break
                    if used_src[src] >= len(pool[src]):
                        continue
                    e = pool[src][used_src[src]]
                    used_src[src] += 1
                    picked.append(e.clone())
                    quality_cnt[e.quality or "未知"] += 1
                    progressed = True
        if not picked:
            fail_channels.append(cname)
            continue
        # 名额没填满时用其它画质补足：仍受 keep、单源上限、
        # 以及该画质自身档位名额三重约束，避免 HD 把 4K 的名额吃掉
        if len(picked) < keep:
            for src in src_order:
                while used_src[src] < min(cap_src, len(by_src[src])) and len(picked) < keep:
                    e = by_src[src][used_src[src]]
                    used_src[src] += 1
                    q = e.quality or ""
                    if any(p.url == e.url for p in picked):
                        continue
                    if sum(1 for p in picked if (p.quality or "") == q) >= _quota_for(quota, q):
                        continue
                    picked.append(e.clone())
                    quality_cnt[q or "未知"] += 1
        live_out.extend(picked[:keep])

    log.info(f"✅ 精选完成：{len(live_out)} 条 / "
             f"{len(set(e.name for e in live_out))} 频道")
    if quality_cnt:
        log.info("   画质分布：" + ", ".join(
            f"{k}×{v}" for k, v in sorted(quality_cnt.items(), key=lambda x: -x[1])
        ))
    if fail_channels:
        log.warning(f"   ⚠ {len(fail_channels)} 个频道无存活线路："
                    f"{', '.join(fail_channels[:10])}")

    # ── 输出 ──
    _write_m3u(outdir / "live.m3u", live_out)
    _write_txt(outdir / "live.txt", live_out)
    _write_db(cfg, live_out, quality_cnt, st1)

    elapsed = round(time.time() - t_total, 1)
    stats = {
        **st1,
        "probed": total_probe,
        "alive": alive_total,
        "output_entries": len(live_out),
        "output_channels": len(set(e.name for e in live_out)),
        "quality": dict(quality_cnt),
        "failed_channels": len(fail_channels),
        "elapsed": elapsed,
    }
    log.info(f"🏁 直播聚合完成（总耗时 {elapsed}s）")
    log.info(f"   📁 {outdir}/live.m3u")
    log.info(f"   📁 {outdir}/live.txt")
    log.info(f"   📁 {outdir}/fetch_report.json")
    return stats


# ═════════════════════════════════════════════════════════════
#  输出写入
# ═════════════════════════════════════════════════════════════

def _esc(s: str) -> str:
    return (s or "").replace('"', "&quot;").replace("\n", " ").replace(",", "，")


def _write_m3u(path: Path, entries: List[Entry]) -> None:
    lines = ["#EXTM3U url-tracker-radar=\"useless\"", ""]
    for e in entries:
        attrs = []
        if e.tvg_id:
            attrs.append(f'tvg-id="{_esc(e.tvg_id)}"')
        if e.name:
            attrs.append(f'tvg-name="{_esc(e.name)}"')
        if e.logo:
            attrs.append(f'tvg-logo="{_esc(e.logo)}"')
        if e.group:
            attrs.append(f'group-title="{_esc(e.group)}"')
        attr_str = " ".join(attrs)
        extra = ""
        if e.meta.get("elapsed"):
            extra = f' _info="rt={e.meta["elapsed"]}s"'
        inf = f"#EXTINF:-1 {attr_str},{_esc(e.name)}{extra}"
        lines.append(inf)
        lines.append(e.url)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_txt(path: Path, entries: List[Entry]) -> None:
    lines: List[str] = []
    cur: Optional[str] = None
    for e in entries:
        if e.group != cur:
            cur = e.group
            lines.append(f"#genre#{cur or '其他'}")
        lines.append(f"{e.name},{e.url}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_db(cfg: dict, entries: List[Entry], quality_cnt: dict, st1: dict) -> None:
    if db is None:
        return
    try:
        conn = db.connect(cfg)
        db.upsert_sources(conn, [{"name": e.source, "type": e.source_type}
                                 for e in entries])
        db.upsert_channels(conn, [
            {"name": e.name, "group": e.group, "quality": e.quality,
             "tvg_id": e.tvg_id, "logo": e.logo, "source": e.source}
            for e in entries
        ])
        log.info(f"💾 已写入数据库：{len(entries)} 条")
        conn.close()
    except Exception as e:  # noqa
        log.warning(f"💾 数据库写入跳过：{e}")


# ═════════════════════════════════════════════════════════════
#  点播（完全保留，不改动逻辑）
# ═════════════════════════════════════════════════════════════

def run_vod(cfg: dict) -> dict:
    """点播源聚合（逻辑保持不变，仅修正一处 meta 字段丢失问题）"""
    vod_cfg = cfg.get("vod", {})
    if not vod_cfg.get("enabled", True):
        log.info("🎞️ 点播聚合已禁用（vod.enabled=false）")
        return {}
    outdir = Path(cfg.get("output_dir", "output"))
    outdir.mkdir(parents=True, exist_ok=True)
    timeout = int(cfg.get("fetch", {}).get("timeout", 30))
    session = make_session()
    log.info("=" * 60)
    log.info("🎞️ 点播源聚合开始")
    t0 = time.time()

    sources = [s for s in build_sources(cfg) if s.get("type") == "vod"]
    all_items: List[dict] = []
    reports = []
    for src in sources:
        if src.get("enabled", True) is False:
            continue
        name = src.get("name", "?")
        url = src.get("url", "")
        if not url:
            continue
        t = time.time()
        text, info = fetch_with_fallback(
            url, src.get("fallbacks", []) or [], timeout=timeout
        )
        if text is None:
            log.error(f"  ✗ 点播源 {name} 抓取失败：{info.get('error')}")
            reports.append({"source": name, "ok": False,
                            "error": info.get("error")})
            continue
        items = _parse_vod(text, src)
        log.info(f"  ✓ 点播源 {name} → {len(items)} 条"
                 f"（{round(time.time()-t,1)}s）")
        reports.append({"source": name, "ok": True, "entries": len(items)})
        all_items.extend(items)

    # 去重（按 title+url）
    seen: set = set()
    uniq: List[dict] = []
    for it in all_items:
        k = (it.get("title", "").strip().lower(), it.get("url", ""))
        if k in seen:
            continue
        seen.add(k)
        uniq.append(it)

    # 分类
    by_cat: Dict[str, List[dict]] = defaultdict(list)
    for it in uniq:
        by_cat[it.get("category", "其他")].append(it)

    # 每分类按 quality 配额精选
    selected: List[dict] = []
    for cat, items in by_cat.items():
        items.sort(key=lambda d: (
            -_vod_quality_rank(d),
            d.get("year", 0) or 0,
            d.get("source_rank", 0),
        ))
        quota = vod_cfg.get("per_category", 200)
        selected.extend(items[:quota])

    out = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "total": len(selected),
        "by_category": {k: len(v) for k, v in by_cat.items()},
        "items": selected,
    }
    fp = outdir / "vod.json"
    fp.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    txt_lines = [f"# 点播清单（{len(selected)} 部）", ""]
    for cat in sorted(by_cat):
        txt_lines.append(f"# {cat}（{len(by_cat[cat])}）")
        for it in by_cat[cat][: vod_cfg.get("per_category", 200)]:
            txt_lines.append(
                f"{it.get('title','')} | {it.get('year','')} | "
                f"{it.get('quality','')} | {it.get('url','')}"
            )
        txt_lines.append("")
    (outdir / "vod.txt").write_text("\n".join(txt_lines), encoding="utf-8")

    elapsed = round(time.time() - t0, 1)
    stats = {"items": len(uniq), "selected": len(selected),
             "by_category": dict(out["by_category"]), "elapsed": elapsed}
    log.info(f"✅ 点播聚合完成：{len(uniq)} → {len(selected)} 部（{elapsed}s）")
    log.info(f"   📁 {fp}")
    return stats


def _parse_vod(text: str, src: dict) -> List[dict]:
    """解析点播源，返回 [{'title','url','category','quality','year',...}]"""
    items: List[dict] = []
    fmt = src.get("format", "auto")
    if fmt == "auto":
        fmt = "json" if text.lstrip().startswith("{") or text.lstrip().startswith("[") else "txt"
    if fmt == "json":
        try:
            data = json.loads(text)
        except Exception:  # noqa
            return []
        if isinstance(data, dict):
            data = data.get("data") or data.get("items") or data.get("list") or []
        for d in data:
            if not isinstance(d, dict):
                continue
            items.append({
                "title": d.get("title") or d.get("name") or "",
                "url": d.get("url") or d.get("play_url") or d.get("link") or "",
                "category": d.get("category") or d.get("type") or "其他",
                "quality": d.get("quality") or _quality_from_text(str(d)),
                "year": d.get("year") or _guess_year(str(d)),
                "source": src.get("name", ""),
                "source_rank": _vod_source_rank(src),
                "meta": {k: v for k, v in d.items()
                         if k not in ("title", "url", "category", "quality", "year")},
            })
    else:
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            m = re.match(r'^(.*?)(?:[,，#$])\s*(.+)$', line)
            if not m:
                continue
            title = _strip_quotes(m.group(1)).strip()
            url = m.group(2).strip()
            if not title or not url:
                continue
            items.append({
                "title": title, "url": url,
                "category": _guess_category(title),
                "quality": _quality_from_text(title),
                "year": _guess_year(title),
                "source": src.get("name", ""),
                "source_rank": _vod_source_rank(src),
                "meta": {},
            })
    return [it for it in items if it.get("title") and it.get("url")]


def _vod_quality_rank(it: dict) -> int:
    q = (it.get("quality") or "").upper()
    if "8K" in q:
        return 50
    if "4K" in q or "2160" in q:
        return 40
    if "1080" in q:
        return 30
    if "720" in q:
        return 20
    if it.get("quality"):
        return 10
    return 0


def _vod_source_rank(src: dict) -> int:
    name = (src.get("name") or "").lower()
    if "static" in name:
        return 100
    if src.get("priority"):
        try:
            return int(src["priority"])
        except Exception:  # noqa
            return 50
    return 50


def _guess_category(title: str) -> str:
    t = title
    for kw, cat in [
        (["电影", "影院", "院线", "大片"], "电影"),
        (["剧", "连续剧", "电视"], "电视剧"),
        (["综艺", "真人秀"], "综艺"),
        (["纪录", "人文"], "纪录片"),
        (["动画", "动漫", "卡通"], "动漫"),
        (["体育", "赛事", "足球", "篮球", "NBA"], "体育"),
        (["少儿", "儿童", "亲子"], "少儿"),
        (["新闻", "资讯"], "新闻"),
        (["音乐", "演唱会"], "音乐"),
    ]:
        if any(k in t for k in kw):
            return cat
    return "其他"


def _guess_year(s: str) -> Optional[int]:
    m = re.search(r"(19[8-9]\d|20[0-2]\d)", s)
    if m:
        try:
            return int(m.group(1))
        except Exception:  # noqa
            return None
    return None


def _strip_quotes(s: str) -> str:
    return s.strip().strip('"\'').strip()


# ═════════════════════════════════════════════════════════════
#  主入口
# ═════════════════════════════════════════════════════════════

def load_config(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"配置文件不存在：{p}")
    with p.open(encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    base = p.parent.resolve()
    cfg["base_dir"] = str(base)
    # ── 键名对齐：config.yaml 里写在 live.* 下的参数，映射到代码实际读取的位置 ──
    live_cfg = cfg.get("live", {}) or {}
    fetch_cfg = cfg.setdefault("fetch", {})
    fetch_cfg.setdefault("timeout", int(live_cfg.get("fetch_timeout", 30)))
    fetch_cfg.setdefault("cache", bool(live_cfg.get("use_cache", True)))
    fetch_cfg.setdefault("cache_ttl_seconds", int(
        live_cfg.get("source_cache_ttl_seconds",
                     live_cfg.get("cache_ttl_hours", 12) * 3600) or 0))
    probe_cfg = cfg.setdefault("probe", {})
    probe_cfg.setdefault("timeout", int(live_cfg.get("timeout", 10)))
    probe_cfg.setdefault("probe_timeout", int(live_cfg.get("probe_timeout", 6)))
    probe_cfg.setdefault("concurrency", int(live_cfg.get("concurrency", 50)))
    probe_cfg.setdefault("use_ffmpeg", bool(live_cfg.get("use_ffmpeg", False)))
    probe_cfg.setdefault("cache", bool(live_cfg.get("probe_cache", True)))
    probe_cfg.setdefault("cache_ttl_seconds",
                         int(live_cfg.get("probe_cache_ttl_seconds", 600) or 0))
    probe_cfg.setdefault("history_window",
                         int(live_cfg.get("history_window", 20) or 20))
    probe_cfg.setdefault("min_resolution", int(live_cfg.get("min_resolution", 720)))
    probe_cfg.setdefault("max_channels", int(live_cfg.get("max_channels", 1000)))
    for k in ("candidate_pool", "cache_dir", "source_cache_ttl_seconds",
              "probe_cache", "probe_cache_ttl_seconds", "history_window",
              "stability_window",
              "static_res_bonus", "sort_weights", "db_path"):
        if k in live_cfg and k not in probe_cfg:
            probe_cfg[k] = live_cfg[k]
    cfg.setdefault("output_dir", str(live_cfg.get("output_dir") or (base / "output")))
    if not (cfg.get("sources") or live_cfg.get("urls_static") or live_cfg.get("local_dir")):
        log.warning("⚠️ config.yaml 未发现任何直播源（live.urls_static / local_dir / sources）")
    return cfg


def main() -> int:
    parser = argparse.ArgumentParser(description="直播/点播源聚合器")
    parser.add_argument("-c", "--config", default="config.yaml")
    parser.add_argument("--live", action="store_true", help="只跑直播聚合")
    parser.add_argument("--vod", action="store_true", help="只跑点播聚合")
    parser.add_argument("--dry-run", action="store_true", help="只整理不探测")
    parser.add_argument("--quick", action="store_true", help="快速模式（少量候选）")
    parser.add_argument("--max-channels", type=int, default=0,
                        help="最多处理频道数（调试）")
    parser.add_argument("--max-urls", type=int, default=0,
                        help="每频道最多候选 URL（调试）")
    parser.add_argument("--no-ffmpeg", action="store_true",
                        help="禁用 ffmpeg 探测")
    args = parser.parse_args()

    cfg = load_config(args.config)
    Flags.DRY_RUN = args.dry_run
    Flags.QUICK = args.quick
    Flags.MAX_FETCH_CHANNELS = args.max_channels
    Flags.MAX_FETCH_URLS = args.max_urls
    if args.no_ffmpeg:
        cfg.setdefault("probe", {})
        cfg["probe"]["use_ffmpeg"] = False

    signal.signal(signal.SIGINT, _sigint_raise)

    try:
        if args.vod and not args.live:
            run_vod(cfg)
        elif args.live and not args.vod:
            aggregate_live(cfg)
        else:
            aggregate_live(cfg)
            run_vod(cfg)
    except _FakeSigint:
        log.warning("收到中断信号，退出")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
