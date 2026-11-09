#!/usr/bin/env python3
"""TVBox 影视仓 全自动聚合（含 EPG + 台标）
v7 特性：
  1. 多代理轮询：每个源自动尝试【直连 → jsDelivr镜像 / GitHub代理】，失败自动换下一个
  2. 本地缓存兜底：网络全失败时回退过期缓存，绝不返回空
  3. tvg-id 归一化：CCTV-1 / CCTV14.cn@SD / CCTV-4 统一成 CCTV14
  4. 脏数据过滤：拆多流行、剔 migutoken 签名 / .php 中转（IPv6 不过滤，单栈环境仍可访问）
  5. 分频运行：--live 仅直播，--vod 仅点播，默认全跑
  6. 直播源三分：
       urls_static       —— 无需鉴权的固定直链，每次都抓取（官方聚合 / 官方 HLS / iptv-org 目录）
       urls_static_4k    —— 静态源里「可能出 4K」的（河南系 / 湖南系），不享受静态源加分，
                            推迟到第 3 轮与刷新源同场竞争
       urls_refresh      —— 带签名会过期的源，只在缓存缺失或 FORCE_REFRESH=1 时抓取
  7. 三轮探测 + 频道级跳过：
       第 1 轮：只探静态源中的 4K 线路，达标的频道整体跳过
       第 2 轮：只探静态源中的 1080p 线路，达标的频道整体跳过
       第 3 轮：静态源剩余 + urls_static_4k + 刷新源，全量探测、按分数补全到 N 路
  8. 双权重排序：清晰度(45%) + 稳定性(25%) 主导，静态源享有官方源加分
  9. live_history 采用「追加写入」结构（见 database.py），稳定性统计基于真实历史
"""
import argparse
import asyncio
import aiohttp
import aiofiles
import json
import re
import subprocess
import time
import os
import hashlib
import yaml
from datetime import datetime
from pathlib import Path
from database import init_db, record_vod_probe, record_live_probe, get_blacklist, log_run, DB as _DB_PATH

CONFIG_FILE = Path(__file__).parent / 'config.yaml'


def load_config():
    if not CONFIG_FILE.exists():
        return {}
    try:
        with CONFIG_FILE.open(encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"⚠️ 读取 {CONFIG_FILE} 失败: {e}")
        return {}


CONFIG = load_config()

# ── 从 config.yaml 读取参数（带默认值兜底）──
VOD_CFG = CONFIG.get('vod', {}) or {}
LIVE_CFG = CONFIG.get('live', {}) or {}
LOGO_BASE = (CONFIG.get('logo', {}) or {}).get('base_url', "https://live.fanmingming.com/tv")
CACHE_DIR = Path(LIVE_CFG.get('cache_dir', 'sources_cache'))
CACHE_TTL_HOURS = float(LIVE_CFG.get('cache_ttl_hours', 12))
# 数据库路径统一引用 database.py 的 DB，避免两边路径不一致导致面板读不到数据
DB_PATH = _DB_PATH
FETCH_TIMEOUT = int(LIVE_CFG.get('fetch_timeout', 30))

# 直播探测参数
PROBE_TIMEOUT = int(LIVE_CFG.get('probe_timeout', 6))          # ffprobe 探测超时
PROBE_CONCURRENCY = int(LIVE_CFG.get('concurrency', 50))       # 直播探测并发
HEAD_TIMEOUT = int(LIVE_CFG.get('timeout', 8))                 # HEAD 探测超时
URLS_PER_CHANNEL = int(LIVE_CFG.get('urls_per_channel', 4))    # 每频道保留线路数
MIN_RESOLUTION = int(LIVE_CFG.get('min_resolution', 720))      # 最低分辨率
MAX_LIVE_CH = int(LIVE_CFG.get('max_channels', 1000))          # 最多频道数
SORT_WEIGHTS = LIVE_CFG.get('sort_weights') or {               # 评分权重（自动归一化）
    'resolution': 0.45, 'speed': 0.15, 'delay': 0.15, 'stability': 0.25,
}
# 稳定性统计窗口：取最近 N 次历史探测记录计算该 URL 的成功率
STABILITY_WINDOW = int(LIVE_CFG.get('stability_window', 10))

# 三轮探测：跳过阈值
STATIC_4K_SKIP = bool(LIVE_CFG.get('static_4k_skip', True))
STATIC_HD_SKIP = bool(LIVE_CFG.get('static_hd_skip', True))
MIN_STATIC_4K = int(LIVE_CFG.get('min_static_4k', 2160))       # 第 1 轮达标线（4K）
MIN_STATIC_HD = int(LIVE_CFG.get('min_static_hd', 1080))       # 第 2 轮达标线（1080p）
MIN_STABILITY_EFFECTIVE = float(LIVE_CFG.get('min_stability_effective', 0.80))  # 稳定性有效阈值
STATIC_RES_BONUS = int(LIVE_CFG.get('static_res_bonus', 5))    # 静态源清晰度加分（上限 100）

# 每频道最多送入探测的候选线路数（避免个别源解析出几十条把时间烧完）
CANDIDATES_PER_CHANNEL = int(LIVE_CFG.get('candidates_per_channel', 15))

# 点播参数
VOD_TIMEOUT = int(VOD_CFG.get('timeout', 12))
VOD_CONCURRENCY = int(VOD_CFG.get('concurrency', 100))
MIN_JSON_SIZE = int(VOD_CFG.get('min_json_size', 200))
MAX_VOD = int(VOD_CFG.get('max_sources', 1500))
DISCOVER_PER_PAGE = int(VOD_CFG.get('discover_per_page', 20))

# 多代理参数
GH_PROXIES = list(LIVE_CFG.get('proxies') or [])
JSD_HOSTS = list(LIVE_CFG.get('jsdelivr_hosts') or [])
PER_CANDIDATE_TO = int(LIVE_CFG.get('per_candidate_timeout', 5))   # 单代理/候选超时（秒）
CONNECT_TIMEOUT = int(LIVE_CFG.get('connect_timeout', 3))          # 连接阶段超时
MAX_CANDIDATES = int(LIVE_CFG.get('max_candidates', 4))            # 单源最多试几种方式

# 台标映射：fanmingming 命名是 CCTV1.png 这种纯编号
LOGO_MAP = {
    'CCTV1综合': 'CCTV1', 'CCTV1': 'CCTV1',
    'CCTV2财经': 'CCTV2', 'CCTV2': 'CCTV2',
    'CCTV3综艺': 'CCTV3', 'CCTV3': 'CCTV3',
    'CCTV4中文国际': 'CCTV4', 'CCTV4国际': 'CCTV4', 'CCTV4': 'CCTV4',
    'CCTV5体育': 'CCTV5', 'CCTV5': 'CCTV5',
    'CCTV5+体育赛事': 'CCTV5PLUS', 'CCTV5+': 'CCTV5PLUS',
    'CCTV6电影': 'CCTV6', 'CCTV6': 'CCTV6',
    'CCTV7军事农业': 'CCTV7', 'CCTV7': 'CCTV7',
    'CCTV8电视剧': 'CCTV8', 'CCTV8': 'CCTV8',
    'CCTV9记录': 'CCTV9', 'CCTV9': 'CCTV9',
    'CCTV10科教': 'CCTV10', 'CCTV10': 'CCTV10',
    'CCTV11戏曲': 'CCTV11', 'CCTV11': 'CCTV11',
    'CCTV12社会与法': 'CCTV12', 'CCTV12': 'CCTV12',
    'CCTV13新闻': 'CCTV13', 'CCTV13': 'CCTV13',
    'CCTV14少儿': 'CCTV14', 'CCTV14': 'CCTV14',
    'CCTV15音乐': 'CCTV15', 'CCTV15': 'CCTV15',
    'CCTV16奥林匹克': 'CCTV16', 'CCTV16': 'CCTV16',
    'CCTV17农业农村': 'CCTV17', 'CCTV17': 'CCTV17',
}


# ── m3u 解析正则 ──
_EXTINF = re.compile(
    r'^#EXTINF:-?\d+'
    r'(?:\s+[^,]*)?'
    r'(?:,)?'
    r'\s*(.*)$'
)
_TV = re.compile(r'(?:^|[\s,])tvg-([a-zA-Z0-9_-]+)="([^"]*)"')
_NAME = re.compile(r'tvg-name="([^"]*)"')
_EMOJI = re.compile(r'[\U00010000-\U0010ffff\U00002600-\U000027BF\U0001F300-\U0001FAFF]')
# 脏数据过滤：tvg-id / 频道名 里出现「看起来像日期或乱码」的，直接丢弃
_JUNK_NAME = re.compile(r'\d{4}-\d{2}-\d{2}|\d{2}:\d{2}:\d{2}')


# ── 工具 ──
def clean_name(n):
    """清洗用于拼 logo / 匹配的键，保留空格，不删卫视"""
    if not n:
        return ''
    n = (n.replace('4K', '').replace('高清', '').replace('标清', '')
            .replace('HD', '').replace('SD', ''))
    n = re.sub(r'\s*频道\s*$', '', n)
    m = re.match(r'(CCTV\d+)', n)
    if m:
        return m.group(1)
    return n.strip()


def normalize_tvg_id(raw):
    """把各种 CCTV 写法归一为 CCTV<数字>，卫视归为「XX卫视」
    例：CCTV-1 / CCTV14.cn@SD / CCTV-4  ->  CCTV1 / CCTV14 / CCTV4
    """
    if not raw:
        return ''
    s = raw.strip()
    m = re.match(r'CCTV-?(\d{1,2})', s, re.I)
    if m:
        return f'CCTV{m.group(1)}'
    m = re.match(r'CCTV(\d{1,2})\.cn@SD', s, re.I)
    if m:
        return f'CCTV{m.group(1)}'
    m = re.match(r'([\u4e00-\u9fa5]{2,4})卫视', s)
    if m:
        return m.group(1) + '卫视'
    s = re.sub(r'\s*[\(（].*?[\)）]\s*$', '', s)
    s = re.sub(r'\.(cn@SD|cn@HD|cn@4K)', '', s)
    return _EMOJI.sub('', s).strip()


def logo_url(n):
    key = clean_name(n)
    return f"{LOGO_BASE}/{LOGO_MAP.get(key, key)}.png"


def tvg_id(n):
    """用于匹配 EPG 的规范 ID"""
    t = clean_name(n)
    return t.replace('卫视', '')


def classify(name):
    """按名称归类频道"""
    if not name:
        return '其他'
    t = clean_name(name)
    if t.startswith('CCTV'):
        return '央视频道'
    if '卫视' in name or name.endswith('卫视'):
        return '卫视频道'
    return '地方频道'


# ── 评分权重归一化 ──
def _norm_weights(w):
    s = sum(w.values()) or 1
    return {k: v / s for k, v in w.items()}


NW = _norm_weights(SORT_WEIGHTS)


# ── 多代理轮询 ──
def build_candidates(url):
    """为单个源 URL 生成多种访问方式：直连优先，失败才降级"""
    cands = [url]
    if 'jsdelivr.net' in url:
        for h in JSD_HOSTS:
            if h not in url:
                cands.append(url.replace('https://cdn.jsdelivr.net', h))
    elif ('github.com' in url or 'githubusercontent.com' in url):
        for p in GH_PROXIES:
            cands.append(p + url)
    return cands


def _cache_path(url, cache_dir):
    """缓存文件名：原文件名 + URL哈希前12位"""
    name = url.rsplit('/', 1)[-1].split('?')[0] or 'source'
    name = re.sub(r'[^\w.\-]', '_', name)[:40]
    h = hashlib.md5(url.encode()).hexdigest()[:12]
    return cache_dir / f"{name}_{h}.m3u"


def _cache_age_hours(cp):
    if not cp.exists():
        return None
    return (time.time() - cp.stat().st_mtime) / 3600.0


def _save_cache(cp, text):
    try:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cp.write_text(text, encoding='utf-8')
    except Exception:
        pass


async def fetch_text_any(sess, url, label='', timeout=None):
    """
    多方式尝试 + 缓存兜底，四级降级：
      ① 新鲜缓存（< TTL）     → 直接用
      ② 网络拉取成功          → 写缓存 + 返回
      ③ 网络失败 + 有过期缓存 → 回退旧缓存
      ④ 网络失败 + 无缓存     → 返回空

    每个候选/代理使用独立短超时，到点即停、继续试下一个，不阻塞整个源。
    """
    timeout = timeout or FETCH_TIMEOUT
    cp = _cache_path(url, CACHE_DIR)
    age = _cache_age_hours(cp)

    # ① 新鲜缓存
    if age is not None and age < CACHE_TTL_HOURS:
        print(f"  📦 {label} → 缓存 ({age:.1f}h 前)")
        return cp.read_text(encoding='utf-8', errors='ignore')

    # ② 网络拉取（多方式轮询，每个候选独立超时）
    candidates = build_candidates(url)[:MAX_CANDIDATES]
    for i, cand in enumerate(candidates):
        t0 = time.monotonic()
        try:
            async with sess.get(
                cand,
                timeout=aiohttp.ClientTimeout(total=PER_CANDIDATE_TO, connect=CONNECT_TIMEOUT),
            ) as r:
                if r.status == 200:
                    t = await r.text()
                    if t.strip():
                        tag = '直连' if i == 0 else f'备用{i}'
                        print(f"  ✓ {label} → {tag} 成功 ({len(t)} 字节, {time.monotonic()-t0:.1f}s)")
                        _save_cache(cp, t)
                        return t
                    else:
                        print(f"  ✗ {label} → {tag} 空响应 ({time.monotonic()-t0:.1f}s)")
                else:
                    print(f"  ✗ {label} → 方式{i} HTTP {r.status} ({time.monotonic()-t0:.1f}s)")
        except asyncio.TimeoutError:
            print(f"  ✗ {label} → 方式{i} 超时({PER_CANDIDATE_TO}s) ({time.monotonic()-t0:.1f}s)，跳过")
        except Exception as e:
            print(f"  ✗ {label} → 方式{i} {type(e).__name__} ({time.monotonic()-t0:.1f}s)")
        # 不 return，继续试下一个候选

    # ③ 网络全失败 → 回退过期缓存
    if age is not None:
        print(f"  ⚠️ {label} 网络失败，回退过期缓存 ({age:.1f}h 前)")
        return cp.read_text(encoding='utf-8', errors='ignore')

    # ④ 彻底失败
    print(f"  ✗ {label} 所有方式均失败且无缓存")
    return ''


# ── 点播 ──
async def discover_vod(session):
    urls = set()
    # 统一从 discover.vod_seeds 读取种子源；旧 key 兼容层已移除
    for u in (CONFIG.get('discover', {}) or {}).get('vod_seeds', []) or []:
        urls.add(u)
    headers = {"Accept": "application/vnd.github.v3+json"}
    token = os.environ.get('GH_TOKEN')
    if token:
        headers['Authorization'] = f'Bearer {token}'
    try:
        async with session.get(
            f"https://api.github.com/search/code?q=tvbox+extension:json&per_page={DISCOVER_PER_PAGE}",
            headers=headers,
            timeout=15,
        ) as r:
            if r.status == 200:
                data = await r.json()
                for it in data.get('items', []):
                    raw = it.get('raw_url') or it.get('html_url', '')
                    if raw and raw.endswith('.json'):
                        raw = raw.replace('github.com', 'raw.githubusercontent.com').replace('/blob/', '/')
                        urls.add(raw)
            elif r.status in (403, 429):
                print(f"⚠️ GitHub Search 限流 ({r.status})，使用种子源")
    except Exception as e:
        print(f"⚠️ discover_vod 失败: {e}")
    return list(urls)[:MAX_VOD * 3]


async def probe_vod(session, url, bl):
    if url in bl:
        return None
    try:
        st = time.time()
        async with session.get(url, timeout=VOD_TIMEOUT) as r:
            d = time.time() - st
            if r.status != 200:
                record_vod_probe(url, 'fail', d)
                return None
            t = await r.text()
            if len(t) < MIN_JSON_SIZE:
                record_vod_probe(url, 'invalid', d)
                return None
            record_vod_probe(url, 'ok', d, len(t))
            return {'data': json.loads(t), 'size': len(t)}
    except Exception:
        record_vod_probe(url, 'fail')
        return None


async def aggregate_vod(urls, epg_url):
    bl = get_blacklist('vod_history')
    connector = aiohttp.TCPConnector(limit=VOD_CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as s:
        res = await asyncio.gather(*[probe_vod(s, u, bl) for u in urls])
    valid = sorted([x for x in res if x], key=lambda x: x['size'], reverse=True)[:MAX_VOD]
    if not valid:
        return {}
    primary = valid[0]['data']
    if not isinstance(primary, dict):
        print("⚠️ 主源不是 TVBox 配置对象(dict)")
        return {}
    primary.setdefault('_meta', {})
    primary['_meta'].update({
        'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'available': len(valid),
    })
    primary['lives'] = [{
        "name": "自动聚合直播",
        "type": 0,
        "url": "",
        "epg": epg_url,
        "logo": LOGO_BASE + "/",
    }]
    return primary


# ── 直播 ──
_JUNK_PATTERNS = ('migutoken=', 'Smart.php', 'miguvideo.com')


def parse_playlist(text):
    """按 m3u 规范解析，并过滤脏数据"""
    chs, lines = [], text.strip().split('\n')
    for i, l in enumerate(lines):
        if not l.startswith('#EXTINF:'):
            continue
        raw = lines[i + 1].strip() if i + 1 < len(lines) else ''
        # 拆多流行：只取第一条
        url = raw.split(';')[0].split(',')[0].strip()
        if not url.startswith(('http://', 'https://')):
            continue
        # 过滤不可用线路：migutoken 签名 / .php 中转
        # 注：IPv6（http://[...]）不再过滤，保证单栈 IPv6 网络也能取到可用线路
        if url.endswith('.php'):
            continue
        if any(p in url for p in _JUNK_PATTERNS):
            continue

        name = ''
        m = _NAME.search(l)
        if m:
            name = m.group(1).strip()
        else:
            m = _EXTINF.match(l)
            if m:
                name = m.group(1).strip()
        if not name:
            continue
        # 脏数据过滤：tvg-id / 频道名 长得不像频道（日期串、纯数字等）
        if _JUNK_NAME.search(name):
            continue

        attrs = {k.lower(): v.strip() for k, v in _TV.findall(l)}
        group = attrs.get('group-title', '').strip()
        if group:
            group = _EMOJI.sub('', group).strip()
        chs.append({
            'name': name,
            'url': url,
            'tvg_id': attrs.get('id', '').strip() or '',
            'logo': attrs.get('logo', '').strip() or '',
            'group': group or '',
        })
    return chs


async def discover_live(_session):
    """直播源分三组（见 config.yaml）：
      urls_static    —— 无需鉴权的固定直链，每次都抓取，享有官方源清晰度加分
      urls_static_4k —— 静态源里「可能出 4K」的（河南系 / 湖南系等），不享受加分，
                        推迟到第 3 轮与刷新源同场竞争，保留拿到 4K 的机会
      urls_refresh   —— 带签名/鉴权的源，会过期；只在缓存缺失或强制刷新时抓取，
                        命中过期后不再作为静态源写入，避免产出死链
    返回 [(url, is_static, is_4k_candidate), ...]
    """
    static = list(LIVE_CFG.get('urls_static') or [])
    static_4k = list(LIVE_CFG.get('urls_static_4k') or [])
    refresh = list(LIVE_CFG.get('urls_refresh') or [])
    force = os.environ.get('FORCE_REFRESH', '').lower() in ('1', 'true', 'yes')

    if not static and not static_4k and not refresh:
        print("⚠️ config.yaml 的 live.urls_static / urls_static_4k / urls_refresh 均未配置，直播源将为空")
        return []

    urls = [(u, True, False) for u in static]          # (url, is_static, is_4k_candidate)
    urls += [(u, False, True) for u in static_4k]      # 4K 候选组：不带 _static 标记
    if force or not refresh:
        # 强制刷新或缓存为空时，才抓取需刷新源
        urls += [(u, False, False) for u in refresh]
    else:
        print(f"    ℹ️ 跳过 {len(refresh)} 个需刷新源（缓存有效；FORCE_REFRESH=1 可强制）")
    return urls


async def probe_live_res(url):
    """ffprobe 探测分辨率，超时由外部控制"""
    try:
        r = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-print_format', 'json',
             '-show_streams', '-timeout', '5000000', url],
            capture_output=True, text=True, timeout=PROBE_TIMEOUT,
        )
        if r.returncode == 0:
            for s in json.loads(r.stdout).get('streams', []):
                if s.get('codec_type') == 'video':
                    h = int(s.get('height', 0))
                    return 2160 if h >= 2160 else 1080 if h >= 1080 else 720 if h >= 720 else 480
    except Exception:
        pass
    return None


async def score_live(ch, session, bl, stability_map=None):
    """探测单条线路并打分。
    - 黑名单直接跳过
    - HEAD 必须 200/206/302
    - ffprobe 分辨率必须 ≥ MIN_RESOLUTION
    - 清晰度分档：2160→100 / 1080→80 / 720→60 / 480→30
    - 静态源（urls_static）清晰度 +STATIC_RES_BONUS 分，上限 100
    - 稳定性取历史成功率（无历史按中性 0.5）
    """
    if ch['url'] in bl:
        return None
    d = None
    try:
        st = time.time()
        async with session.head(ch['url'], timeout=HEAD_TIMEOUT) as r:
            d = time.time() - st
            if r.status not in (200, 206, 302):
                record_live_probe(ch['url'], ch['name'], 'fail', d)
                return None
    except Exception:
        record_live_probe(ch['url'], ch['name'], 'fail', d)
        return None

    res = await probe_live_res(ch['url'])
    if not res or res < MIN_RESOLUTION:
        record_live_probe(ch['url'], ch['name'], 'fail', d)
        return None

    # 清晰度分档
    res_score = {2160: 100, 1080: 80, 720: 60}.get(res, 30)
    # 静态源（官方/长期固定直链）享有官方源清晰度加分，最高不超 100
    # 注意：urls_static_4k 组不带 _static 标记，不享受此加分
    if ch.get('_static'):
        res_score = min(100, res_score + STATIC_RES_BONUS)
    # 稳定性：从历史成功率取，无记录则为中性 0.5（新库首次跑的 URL 均无历史）
    stab = (stability_map or {}).get(ch['url'])
    if stab is None:
        stab = 0.5
    stab_score = stab * 100

    score = (res_score * NW['resolution']
             + 80 * NW['speed']
             + max(0, 100 - d * 10) * NW['delay']
             + stab_score * NW['stability'])
    record_live_probe(ch['url'], ch['name'], 'ok', d, res, round(stab, 2), round(score, 1))
    return {**ch, 'score': round(score, 1), '_res': res}


async def _fetch_source(sess, u, is_static, is_4k, idx):
    """抓取单个源并给每条线路打上来源标记，返回频道列表"""
    label = u.rsplit('/', 1)[-1].split('?')[0] or u[:40]
    tag = "[静态]" if is_static else ("[4K候选]" if is_4k else "[刷新]")
    text = await fetch_text_any(sess, u, label=label)
    if not text:
        return []
    got = parse_playlist(text)
    for ch in got:
        ch['_src_order'] = idx
        ch['_static'] = bool(is_static)     # 4K 候选组此处为 False，不享受静态源加分
        ch['_4k_candidate'] = bool(is_4k)
    print(f"    └ {tag} 解析到 {len(got)} 路 (源序 {idx})")
    return got


def _build_stability_map(chs):
    """从数据库统计每个 URL 近 STABILITY_WINDOW 次探测的成功率。

    live_history 已改为「追加写入」结构（每次探测新增一行，不再覆盖），
    因此这里取到的确实是该 URL 的最近 N 次真实实测结果，而非单点。
    返回 {url: 0.0~1.0}，无记录时返回 None（调用方按中性 0.5 处理）。
    """
    from database import get_stability
    urls = {c['url'] for c in chs}
    out = {}
    for u in urls:
        stab = get_stability(u, STABILITY_WINDOW)
        out[u] = stab  # None 表示无历史，由 score_live 统一按 0.5 处理
    return out


def _is_round_satisfied(chs, min_res, min_stab):
    """某频道是否满足「跳过」条件：至少有一路 ≥ min_res 且 稳定性 ≥ min_stab"""
    for c in chs:
        res = c.get('_res') or 0
        stab = c.get('_stab') if c.get('_stab') is not None else 0.0
        if res >= min_res and stab >= min_stab:
            return True
    return False


async def _probe_channel_group(key, chs, session, bl, stability_map, round_label):
    """对某个频道的候选线路做探测，返回 [(ch, valid_record_or_None), ...]"""
    results = []
    for c in chs[:CANDIDATES_PER_CHANNEL]:
        rec = await score_live(c, session, bl, stability_map)
        results.append(rec)
    valid = [r for r in results if r]
    print(f"    {round_label} {key}: {len(valid)}/{len(chs[:CANDIDATES_PER_CHANNEL])} 路达标")
    return valid


async def aggregate_live(url_pairs, repo_prefix):
    # ── 第 0 步：抓源 ──
    all_ch = []
    async with aiohttp.ClientSession() as s:
        for idx, item in enumerate(url_pairs):
            if isinstance(item, tuple):
                if len(item) == 3:
                    u, is_static, is_4k = item
                else:
                    u, is_static, is_4k = item[0], item[1], False
            else:
                u, is_static, is_4k = item, True, False
            got = await _fetch_source(s, u, is_static, is_4k, idx)
            all_ch.extend(got)

    if not all_ch:
        print("⚠️ 未解析到任何直播频道")
        return '', '', 0, 0

    # ── 归一化合并同名频道 ──
    groups = {}
    for ch in all_ch:
        key = normalize_tvg_id(ch.get('tvg_id') or ch['name'])
        if not key:
            continue
        groups.setdefault(key, []).append(ch)
    groups = dict(sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)[:MAX_LIVE_CH])

    stability_map = _build_stability_map(all_ch)
    bl = get_blacklist('live_history')

    skipped_4k = skipped_hd = 0
    final = {}          # key -> [线路...]

    connector = aiohttp.TCPConnector(limit=PROBE_CONCURRENCY)

    async with aiohttp.ClientSession(connector=connector) as s:
        # ── 第 1 轮：只探静态源中的 4K ──
        print(f"\n🔍 第 1 轮探测：静态源 4K（达标线 {MIN_STATIC_4K}p，稳定性≥{MIN_STABILITY_EFFECTIVE}）")
        for k, chs in groups.items():
            static_4k_chs = [c for c in chs if c.get('_static')]
            if not static_4k_chs:
                continue
            valid = await _probe_channel_group(k, static_4k_chs, s, bl, stability_map, "[R1]")
            for r in valid:
                r['_stab'] = stability_map.get(r['url']) or 0.0
            if valid and STATIC_4K_SKIP and _is_round_satisfied(valid, MIN_STATIC_4K, MIN_STABILITY_EFFECTIVE):
                final[k] = valid
                skipped_4k += 1

        # ── 第 2 轮：只探静态源中的 1080p ──
        print(f"\n🔍 第 2 轮探测：静态源 1080p（达标线 {MIN_STATIC_HD}p，稳定性≥{MIN_STABILITY_EFFECTIVE}）")
        for k, chs in groups.items():
            if k in final:
                continue
            static_chs = [c for c in chs if c.get('_static')]
            if not static_chs:
                continue
            valid = await _probe_channel_group(k, static_chs, s, bl, stability_map, "[R2]")
            for r in valid:
                r['_stab'] = stability_map.get(r['url']) or 0.0
            if valid and STATIC_HD_SKIP and _is_round_satisfied(valid, MIN_STATIC_HD, MIN_STABILITY_EFFECTIVE):
                final[k] = valid
                skipped_hd += 1

        # ── 第 3 轮：静态源剩余 + urls_static_4k + 刷新源，全量探测 ──
        print(f"\n🔍 第 3 轮探测：静态源剩余 + 4K 候选 + 刷新源（全量，按分数补全到 {URLS_PER_CHANNEL} 路）")
        for k, chs in groups.items():
            if k in final:
                continue
            # 第 3 轮不做来源限制，所有线路（静态剩余 + 4K 候选 + 刷新源）同场竞争
            valid = await _probe_channel_group(k, chs, s, bl, stability_map, "[R3]")
            for r in valid:
                r['_stab'] = stability_map.get(r['url']) or 0.0
            # 排序：清晰度分(已含静态加分) 降序 → 来源顺序优先
            valid.sort(key=lambda x: (x['score'], -x.get('_src_order', 0)), reverse=True)
            final[k] = valid[:URLS_PER_CHANNEL]

    # ── 统一输出 ──
    epg = f"{repo_prefix}/epg.xml"
    txt, m3u = [], ['#EXTM3U x-tvg-url="%s"' % epg]

    for k, chs in final.items():
        if not chs:
            continue
        n = chs[0]['name']
        tid = chs[0].get('tvg_id') or tvg_id(n)
        lg = chs[0].get('logo') or logo_url(tid or n)
        grp = next((c.get('group') for c in chs if c.get('group')), None) or classify(n)
        for c in chs:
            txt.append(f"{n},{c['url']}")
            m3u.append(
                f'#EXTINF:-1 tvg-id="{tid}" tvg-name="{n}" '
                f'tvg-logo="{lg}" group-title="{grp}",{n}'
            )
            m3u.append(c['url'])

    print(f"\n✅ 三轮探测完成：第1轮跳过 {skipped_4k} 频道，第2轮跳过 {skipped_hd} 频道，"
          f"第3轮补全 {len(groups) - skipped_4k - skipped_hd} 频道")
    return '\n'.join(txt), '\n'.join(m3u), len(final), sum(len(v) for v in final.values())


# ── 主 ──
async def run_live(repo_prefix):
    live_urls = await discover_live(None)
    print(f"📡 待抓取直播源 {len(live_urls)} 个")
    txt, m3u, lc, ll = await aggregate_live(live_urls, repo_prefix)
    if txt:
        async with aiofiles.open('output/live.txt', 'w') as f:
            await f.write(txt)
        async with aiofiles.open('output/live.m3u', 'w') as f:
            await f.write(m3u)
    return lc, ll


async def run_vod(repo_prefix):
    epg = f"{repo_prefix}/epg.xml"
    async with aiohttp.ClientSession() as s:
        vod_urls = await discover_vod(s)
    print(f"🎬 待探测点播源 {len(vod_urls)} 个")
    vod = await aggregate_vod(vod_urls, epg)
    if vod and isinstance(vod, dict):
        vod['lives'][0]['url'] = f"{repo_prefix}/live.m3u"
        async with aiofiles.open('output/tvbox.json', 'w') as f:
            await f.write(json.dumps(vod, ensure_ascii=False, indent=2))
        return 1
    return 0


async def main():
    parser = argparse.ArgumentParser(description='TVBox 自动聚合')
    parser.add_argument('--live', action='store_true', help='仅聚合直播源')
    parser.add_argument('--vod', action='store_true', help='仅聚合点播源')
    args = parser.parse_args()

    t0 = time.time()
    init_db()
    Path('output').mkdir(exist_ok=True)

    repo = os.environ.get('GITHUB_REPOSITORY', 'shuo0101/ysc-self-host')
    branch = os.environ.get('GITHUB_REF_NAME', 'main')
    prefix = f"https://cdn.jsdelivr.net/gh/{repo}@{branch}/output"

    only_live = args.live and not args.vod
    only_vod = args.vod and not args.live

    lc = ll = 0
    vo = 0
    if only_live:
        lc, ll = await run_live(prefix)
    elif only_vod:
        vo = await run_vod(prefix)
    else:
        # 默认：直播 + 点播都跑
        lc, ll = await run_live(prefix)
        vo = await run_vod(prefix)

    log_run(vd=0, vo=vo, ld=0, lc=lc, ll=ll, dur=round(time.time() - t0, 1))
    print(f"✅ 完成 {lc}频道 {ll}线路, 点播源 {vo}个")


if __name__ == '__main__':
    asyncio.run(main())
