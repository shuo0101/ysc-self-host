#!/usr/bin/env python3
"""TVBox 影视仓 全自动聚合（含 EPG + 台标）
v8.1 改动汇总：
  1. 直播输出按 央视频道/卫视频道/地方频道/数字频道/其他频道 分组
  2. clean_name / normalize_tvg_id 保留 '+' 号（CCTV5+ 不再撞 CCTV5）
  3. 新增 _TVG_ALIAS 别名表，处理命名不规范导致的 key 碰撞
  4. final 合并时按 URL 去重累加，同 key 多源线路不覆盖
  5. 新增归一化碰撞检测，启动时打印冲突项
  6. aggregate_vod 兼容主源为 list 格式（纯直播源）的情况
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
from collections import OrderedDict
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
DB_PATH = _DB_PATH
FETCH_TIMEOUT = int(LIVE_CFG.get('fetch_timeout', 30))

# 直播探测参数
PROBE_TIMEOUT = int(LIVE_CFG.get('probe_timeout', 6))
PROBE_CONCURRENCY = int(LIVE_CFG.get('concurrency', 50))
HEAD_TIMEOUT = int(LIVE_CFG.get('timeout', 8))
URLS_PER_CHANNEL = int(LIVE_CFG.get('urls_per_channel', 4))
MIN_RESOLUTION = int(LIVE_CFG.get('min_resolution', 720))
MAX_LIVE_CH = int(LIVE_CFG.get('max_channels', 1000))
SORT_WEIGHTS = LIVE_CFG.get('sort_weights') or {
    'resolution': 0.45, 'speed': 0.15, 'delay': 0.15, 'stability': 0.25,
}
STABILITY_WINDOW = int(LIVE_CFG.get('stability_window', 10))

STATIC_4K_SKIP = bool(LIVE_CFG.get('static_4k_skip', True))
STATIC_HD_SKIP = bool(LIVE_CFG.get('static_hd_skip', True))
MIN_STATIC_4K = int(LIVE_CFG.get('min_static_4k', 2160))
MIN_STATIC_HD = int(LIVE_CFG.get('min_static_hd', 1080))
MIN_STABILITY_EFFECTIVE = float(LIVE_CFG.get('min_stability_effective', 0.80))
STATIC_RES_BONUS = int(LIVE_CFG.get('static_res_bonus', 5))
CANDIDATES_PER_CHANNEL = int(LIVE_CFG.get('candidates_per_channel', 15))

# 点播参数
VOD_TIMEOUT = int(VOD_CFG.get('timeout', 12))
VOD_CONCURRENCY = int(VOD_CFG.get('concurrency', 100))
MIN_JSON_SIZE = int(VOD_CFG.get('min_json_size', 200))
MAX_VOD = int(VOD_CFG.get('max_sources', 1500))
DISCOVER_PER_PAGE = int(VOD_CFG.get('discover_per_page', 20))

# 多代理参数（v8 已废弃：纯直连模式，字段保留以兼容 config.yaml）
GH_PROXIES = []
JSD_HOSTS = []
PER_CANDIDATE_TO = int(LIVE_CFG.get('per_candidate_timeout', 8))
CONNECT_TIMEOUT = int(LIVE_CFG.get('connect_timeout', 4))
MAX_CANDIDATES = 1

# 台标映射
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

# ── 频道分组（输出顺序）──
GROUP_ORDER = ["央视频道", "卫视频道", "地方频道", "数字频道", "其他频道"]

# 省级行政区关键词（识别地方频道）
_REGION_KW = [
    "北京", "上海", "天津", "重庆", "河北", "山西", "辽宁", "吉林", "黑龙江",
    "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南",
    "广东", "广西", "海南", "四川", "贵州", "云南", "陕西", "甘肃", "青海",
    "台湾", "内蒙古", "西藏", "宁夏", "新疆", "香港", "澳门",
]

# 数字频道关键词
_DIGITAL_KW = [
    "风云", "怀旧", "经典", "戏曲", "劲爆", "魅力", "欢笑", "家庭", "靓妆",
    "高尔夫", "留学", "茶", "书画", "武术", "橄榄", "汽摩", "电子体育",
    "财富", "证券", "影像", "发现", "求索", "全纪实", "新视觉", "都市",
    "娱乐", "影视", "少儿", "纪录", "国防", "农业", "社会与法", "奥林匹克",
    "农业农村",
]

# ── tvg-id 别名表：命名不规范导致归一化碰撞时硬映射 ──
_TVG_ALIAS = {
    'CCTV-5+体育赛事': 'CCTV5+',
    'CCTV5+体育赛事': 'CCTV5+',
    'CCTV-5+': 'CCTV5+',
    'CCTV5+': 'CCTV5+',
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
_JUNK_NAME = re.compile(r'\d{4}-\d{2}-\d{2}|\d{2}:\d{2}:\d{2}')


# ── 工具 ──
def clean_name(n):
    """清洗用于拼 logo / 匹配的键。CCTV-5+ 的 '+' 号必须保留。"""
    if not n:
        return ''
    n = (n.replace('高清', '').replace('标清', '')
        .replace('HD', '').replace('SD', ''))
    n = re.sub(r'\s*频道\s*$', '', n)
    m = re.match(r'(CCTV\d+\+?)', n)
    if m:
        return m.group(1)
    return n.strip()


def normalize_tvg_id(raw):
    """归一化：CCTV-1 -> CCTV1, CCTV-5+ -> CCTV5+, CCTV14.cn@SD -> CCTV14
    先查别名表，命中即返回，避免命名不规范导致的碰撞。
    """
    if not raw:
        return ''
    s = raw.strip()
    if s in _TVG_ALIAS:
        return _TVG_ALIAS[s]
    m = re.match(r'CCTV-?(\d{1,2})\+?', s, re.I)
    if m:
        plus = '+' if '+' in s else ''
        return f'CCTV{m.group(1)}{plus}'
    m = re.match(r'CCTV(\d{1,2})\.cn@', s, re.I)
    if m:
        return f'CCTV{m.group(1)}'
    m = re.match(r'([\u4e00-\u9fa5]{2,4})卫视', s)
    if m:
        return m.group(1) + '卫视'
    s = re.sub(r'\s*[\(（].*?[\)）]\s*$', '', s)
    return _EMOJI.sub('', s).strip()


def logo_url(n):
    key = clean_name(n)
    return f"{LOGO_BASE}/{LOGO_MAP.get(key, key)}.png"


def tvg_id(n):
    t = clean_name(n)
    return t.replace('卫视', '')


def classify(name):
    """按名称归类：央视 > 卫视 > 地方(含地名) > 数字频道 > 其他（顺序敏感）"""
    if not name:
        return '其他频道'
    n = name.strip()
    if re.match(r'^CCTV', n, re.IGNORECASE) or '央视' in n or '中央' in n:
        return '央视频道'
    if '卫视' in n:
        return '卫视频道'
    for kw in _REGION_KW:
        if kw in n:
            return '地方频道'
    for kw in _DIGITAL_KW:
        if kw in n:
            return '数字频道'
    return '其他频道'


def channel_sort_key(ch):
    """组内排序权重：(分组索引, 央视数字, 名称)"""
    name = ch.get('name', '') if isinstance(ch, dict) else str(ch)
    group = classify(name)
    gidx = GROUP_ORDER.index(group)
    if group == '央视频道':
        m = re.search(r'(\d+)', name)
        num = int(m.group(1)) if m else 0
        plus = 0.5 if '+' in name else 0.0
        if re.search(r'[48]K', name, re.I):
            plus = 0.8
        return (gidx, num + plus, name)
    return (gidx, 0, name)


def group_channels(channels):
    """按分组归类并保持组内排序"""
    sorted_ch = sorted(channels, key=channel_sort_key)
    result = OrderedDict((g, []) for g in GROUP_ORDER)
    for ch in sorted_ch:
        name = ch.get('name', '')
        g = classify(name)
        result[g].append(ch)
    return result


# ── 评分权重归一化 ──
def _norm_weights(w):
    s = sum(w.values()) or 1
    return {k: v / s for k, v in w.items()}


NW = _norm_weights(SORT_WEIGHTS)


# ── 纯直连（v8：无多代理轮询）──
def build_candidates(url):
    """v8 纯直连：不再生成代理/镜像候选"""
    return [url]


def _cache_path(url, cache_dir):
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
    """纯直连：新鲜缓存 -> 单次直连+1次重试 -> 回退过期缓存 -> 返回空"""
    timeout = timeout or FETCH_TIMEOUT
    cp = _cache_path(url, CACHE_DIR)
    age = _cache_age_hours(cp)

    if age is not None and age < CACHE_TTL_HOURS:
        print(f"  📦 {label} → 缓存 ({age:.1f}h 前)")
        return cp.read_text(encoding='utf-8', errors='ignore')

    t0 = time.monotonic()
    last_err = ''
    for attempt in range(2):
        try:
            async with sess.get(
                url,
                timeout=aiohttp.ClientTimeout(total=timeout, connect=CONNECT_TIMEOUT),
            ) as r:
                if r.status == 200:
                    t = await r.text()
                    if t.strip():
                        tag = '直连' if attempt == 0 else '重试'
                        print(f"  ✓ {label} → {tag} 成功 ({len(t)} 字节, {time.monotonic()-t0:.1f}s)")
                        _save_cache(cp, t)
                        return t
                    else:
                        last_err = '空响应'
                else:
                    last_err = f'HTTP {r.status}'
        except asyncio.TimeoutError:
            last_err = f'超时({timeout}s)'
        except Exception as e:
            last_err = type(e).__name__
    print(f"  ✗ {label} 直连失败({last_err})，耗时 {time.monotonic()-t0:.1f}s")

    if age is not None:
        print(f"  ⚠️ {label} 回退过期缓存 ({age:.1f}h 前)")
        return cp.read_text(encoding='utf-8', errors='ignore')
    return ''


# ── 点播 ──
async def discover_vod(session):
    urls = set()
    for u in (CONFIG.get('discover', {}) or {}).get('vod_seeds', []) or []:
        urls.add(u)
    headers = {"Accept": "application/vnd.github.v3+json"}
    token = os.environ.get('GH_TOKEN')
    if token:
        headers['Authorization'] = f'Bearer {token}'
    try:
        async with session.get(
            f"https://api.github.com/search/code?q=tvbox+extension:json&per_page={DISCOVER_PER_PAGE}",
            headers=headers, timeout=15,
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
    # 主源可能是 list 格式（纯直播源），需从中挑出 dict 类型的 TVBox 配置对象
    if isinstance(primary, list):
        dict_candidates = [v for v in valid if isinstance(v.get('data'), dict)]
        if not dict_candidates:
            print("⚠️ 所有候选点播源都是 list 格式，无可用 TVBox 配置对象")
            return {}
        primary = dict_candidates[0]['data']
    if not isinstance(primary, dict):
        print(f"⚠️ 主源不是 TVBox 配置对象(dict)，实际类型: {type(primary).__name__}")
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
        url = raw.split(';')[0].split(',')[0].strip()
        if not url.startswith(('http://', 'https://')):
            continue
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
    """直播源分三组：
      urls_static    —— 无需鉴权固定直链，每次抓取，享官方源清晰度加分
      urls_static_4k —— 静态源里可能出 4K 的，不带 _static 标记，不享受加分，
                        推迟到第 3 轮与刷新源同场竞争
      urls_refresh   —— 带签名/鉴权会过期，仅缓存缺失或 FORCE_REFRESH=1 时抓取
    返回 [(url, is_static, is_4k_candidate), ...]
    """
    static = list(LIVE_CFG.get('urls_static') or [])
    static_4k = list(LIVE_CFG.get('urls_static_4k') or [])
    refresh = list(LIVE_CFG.get('urls_refresh') or [])
    force = os.environ.get('FORCE_REFRESH', '').lower() in ('1', 'true', 'yes')

    if not static and not static_4k and not refresh:
        print("⚠️ config.yaml 的 live.urls_static / urls_static_4k / urls_refresh 均未配置，直播源将为空")
        return []

    urls = [(u, True, False) for u in static]
    urls += [(u, False, True) for u in static_4k]
    if force or not refresh:
        urls += [(u, False, False) for u in refresh]
    else:
        print(f"    ℹ️ 跳过 {len(refresh)} 个需刷新源（缓存有效；FORCE_REFRESH=1 可强制）")
    return urls


async def probe_live_res(url):
    """ffprobe 探测分辨率"""
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
    """探测单条线路并打分"""
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

    res_score = {2160: 100, 1080: 80, 720: 60}.get(res, 30)
    if ch.get('_static'):
        res_score = min(100, res_score + STATIC_RES_BONUS)
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
    label = u.rsplit('/', 1)[-1].split('?')[0] or u[:40]
    tag = "[静态]" if is_static else ("[4K候选]" if is_4k else "[刷新]")
    text = await fetch_text_any(sess, u, label=label)
    if not text:
        return []
    got = parse_playlist(text)
    for ch in got:
        ch['_src_order'] = idx
        ch['_static'] = bool(is_static)
        ch['_4k_candidate'] = bool(is_4k)
    print(f"    └ {tag} 解析到 {len(got)} 路 (源序 {idx})")
    return got


def _build_stability_map(chs):
    """从数据库统计每个 URL 近 STABILITY_WINDOW 次探测的成功率"""
    from database import get_stability
    urls = {c['url'] for c in chs}
    out = {}
    for u in urls:
        out[u] = get_stability(u, STABILITY_WINDOW)
    return out


def _is_round_satisfied(chs, min_res, min_stab):
    for c in chs:
        res = c.get('_res') or 0
        stab = c.get('_stab') if c.get('_stab') is not None else 0.0
        if res >= min_res and stab >= min_stab:
            return True
    return False


async def _probe_channel_group(key, chs, session, bl, stability_map, round_label):
    results = []
    for c in chs[:CANDIDATES_PER_CHANNEL]:
        rec = await score_live(c, session, bl, stability_map)
        results.append(rec)
    valid = [r for r in results if r]
    print(f"    {round_label} {key}: {len(valid)}/{len(chs[:CANDIDATES_PER_CHANNEL])} 路达标")
    return valid


async def aggregate_live(url_pairs, repo_prefix):
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

    # ── 碰撞检测：同一 key 对应多个不同频道名 ──
    print("\n🔎 归一化碰撞检测：")
    collisions = 0
    for key, chs in groups.items():
        names = {c.get('name', '') for c in chs if c.get('name')}
        names.discard('')
        if len(names) > 1:
            collisions += 1
            print(f"   ⚠️ key={key} 碰撞 {len(names)} 个频道: {', '.join(sorted(names))}")
    if collisions == 0:
        print("   ✓ 无碰撞，频道不会因归一化丢失")
    else:
        print(f"   ✗ 发现 {collisions} 处碰撞，请补充 _TVG_ALIAS 别名映射")

    stability_map = _build_stability_map(all_ch)
    bl = get_blacklist('live_history')

    skipped_4k = skipped_hd = 0
    final = {}

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

        # ── 第 3 轮：静态源剩余 + 4K 候选 + 刷新源，全量探测 ──
        print(f"\n🔍 第 3 轮探测：静态源剩余 + 4K 候选 + 刷新源（全量，按分数补全到 {URLS_PER_CHANNEL} 路）")
        for k, chs in groups.items():
            if k in final:
                continue
            valid = await _probe_channel_group(k, chs, s, bl, stability_map, "[R3]")
            for r in valid:
                r['_stab'] = stability_map.get(r['url']) or 0.0
            valid.sort(key=lambda x: (x['score'], -x.get('_src_order', 0)), reverse=True)
            # ── 合并而非覆盖：同 key 多源线路按 URL 去重累加 ──
            if k in final:
                existing = final[k]
                seen_urls = {c['url'] for c in existing}
                for c in valid:
                    if c['url'] not in seen_urls:
                        existing.append(c)
                        seen_urls.add(c['url'])
                existing.sort(key=lambda x: x.get('score', 0), reverse=True)
                final[k] = existing[:URLS_PER_CHANNEL]
            else:
                final[k] = valid[:URLS_PER_CHANNEL]

    # ── 统一输出（按分组排序，不丢频道）──
    epg = f"{repo_prefix}/epg.xml"

    channel_map = OrderedDict()
    for key, urls in final.items():
        if not urls:
            continue
        primary = max(urls, key=lambda x: x.get('score', 0))
        n = primary.get('name', '')
        tid = primary.get('tvg_id') or tvg_id(n)
        channel_map[key] = {
            'name': n,
            'tvg_id': tid,
            'logo': primary.get('logo') or logo_url(tid or n),
            'group': classify(n),
            'urls': urls,
        }

    def _out_sort_key(item):
        key, ch = item
        return channel_sort_key(ch)

    sorted_items = sorted(channel_map.items(), key=_out_sort_key)

    grouped_output = OrderedDict((g, []) for g in GROUP_ORDER)
    for key, ch in sorted_items:
        grouped_output[ch['group']].append(ch)

    txt_lines, m3u_lines = [], ['#EXTM3U x-tvg-url="%s"' % epg]
    total_urls = 0
    for g in GROUP_ORDER:
        chs_in_group = grouped_output.get(g, [])
        if not chs_in_group:
            continue
        m3u_lines.append(f'\n# === {g}（{len(chs_in_group)}）===')
        for ch in chs_in_group:
            n = ch['name']
            for c in ch['urls']:
                txt_lines.append(f"{n},{c['url']}")
                m3u_lines.append(
                    f'#EXTINF:-1 tvg-id="{ch["tvg_id"]}" tvg-name="{n}" '
                    f'tvg-logo="{ch["logo"]}" group-title="{g}",{n}'
                )
                m3u_lines.append(c['url'])
                total_urls += 1

    total_ch = len(channel_map)
    total_lines = total_urls

    print(f"\n✅ 三轮探测完成：第1轮跳过 {skipped_4k} 频道，第2轮跳过 {skipped_hd} 频道，"
          f"第3轮补全 {len(groups) - skipped_4k - skipped_hd} 频道")
    print(f"📊 final 频道数: {len(final)}，分组后: {total_ch}，线路总数: {total_lines}")
    print(f"📂 分组统计：")
    for g in GROUP_ORDER:
        cnt = len(grouped_output.get(g, []))
        if cnt:
            print(f"   {g}: {cnt} 个频道")
    if collisions == 0:
        print("🔒 结论：无 key 碰撞，频道与线路均不会丢失")
    else:
        print(f"⚠️ 结论：存在 {collisions} 处 key 碰撞，请补充 _TVG_ALIAS 后重跑")

    return '\n'.join(txt_lines), '\n'.join(m3u_lines), total_ch, total_lines


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
        lc, ll = await run_live(prefix)
        vo = await run_vod(prefix)

    log_run(vd=0, vo=vo, ld=0, lc=lc, ll=ll, dur=round(time.time() - t0, 1))
    print(f"✅ 完成 {lc}频道 {ll}线路, 点播源 {vo}个")


if __name__ == '__main__':
    asyncio.run(main())
