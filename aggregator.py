#!/usr/bin/env python3
"""TVBox 影视仓 全自动聚合（含 EPG + 台标）
v8.15：一轮探测 · 国内源优先 · 分清晰度达标即停 · 缓存复用 · 稳定性统计 · 4:5:3 配额 · 12 线路
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
from database import (
    init_db, record_vod_probe, record_live_probe,
    get_blacklist, get_stability, get_avg_score, is_first_run, log_run, DB as _DB_PATH,
)

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

VOD_CFG = CONFIG.get('vod', {}) or {}
LIVE_CFG = CONFIG.get('live', {}) or {}
LOGO_BASE = (CONFIG.get('logo', {}) or {}).get('base_url', "https://live.fanmingming.com/tv")
CACHE_DIR = Path(LIVE_CFG.get('cache_dir', 'sources_cache'))
CACHE_TTL_HOURS = float(LIVE_CFG.get('cache_ttl_hours', 12))

FETCH_TIMEOUT = int(LIVE_CFG.get('fetch_timeout', 30))
PROBE_TIMEOUT = int(LIVE_CFG.get('probe_timeout', 6))
PROBE_CONCURRENCY = int(LIVE_CFG.get('concurrency', 50))
HEAD_TIMEOUT = int(LIVE_CFG.get('timeout', 8))
MAX_LIVE_CH = int(LIVE_CFG.get('max_channels', 1000))
SORT_WEIGHTS = LIVE_CFG.get('sort_weights') or {
    'resolution': 0.45, 'speed': 0.15, 'delay': 0.15, 'stability': 0.25,
}
STABILITY_WINDOW = int(LIVE_CFG.get('stability_window', 10))
MIN_RESOLUTION = int(LIVE_CFG.get('min_resolution', 720))

CANDIDATES_PER_CHANNEL = int(LIVE_CFG.get('candidates_per_channel', 30))
STATIC_RES_BONUS = int(LIVE_CFG.get('static_res_bonus', 5))

# ── 每频道线路数：白天 9 / 晚高峰 12 ──
URLS_PER_CHANNEL_DAY = int(LIVE_CFG.get('urls_per_channel', 9))
URLS_PER_CHANNEL_PEAK = int(LIVE_CFG.get('urls_per_channel_peak', 12))
PEAK_HOURS = set(LIVE_CFG.get('peak_hours') or [19, 20, 21, 22, 23])

# ── 清晰度配额：4K×4 + 1080p×5 + 720p×3 = 12 ──
RES_QUOTAS = LIVE_CFG.get('res_quotas') or {2160: 4, 1080: 5, 720: 3}
QUOTA_4K_LIMIT = int(LIVE_CFG.get('quota_4k_limit', 4))
QUOTA_1080_LIMIT = int(LIVE_CFG.get('quota_1080_limit', 5))
QUOTA_720_LIMIT = int(LIVE_CFG.get('quota_720_limit', 3))

# ── 国内源加分 / 海外源降权 ──
DOMESTIC_BONUS = float(LIVE_CFG.get('domestic_bonus', 15))
OVERSEAS_PENALTY = float(LIVE_CFG.get('overseas_penalty', -15))

# ── 缓存 ──
USE_CACHE = bool(LIVE_CFG.get('use_cache', True))
CACHE_TTL_SECONDS = int(LIVE_CFG.get('cache_ttl_seconds', 600))
HISTORY_WINDOW = int(LIVE_CFG.get('history_window', 20))
MAX_CACHE_SIZE = 5000

VOD_TIMEOUT = int(VOD_CFG.get('timeout', 12))
VOD_CONCURRENCY = int(VOD_CFG.get('concurrency', 100))
MIN_JSON_SIZE = int(VOD_CFG.get('min_json_size', 200))
MAX_VOD = int(VOD_CFG.get('max_sources', 1500))
DISCOVER_PER_PAGE = int(VOD_CFG.get('discover_per_page', 20))

PER_CANDIDATE_TO = int(LIVE_CFG.get('per_candidate_timeout', 8))
CONNECT_TIMEOUT = int(LIVE_CFG.get('connect_timeout', 4))

NW = SORT_WEIGHTS


def is_peak_hour():
    return datetime.now().hour in PEAK_HOURS


def effective_urls_limit():
    """晚高峰 12 条，其余 9 条"""
    return URLS_PER_CHANNEL_PEAK if is_peak_hour() else URLS_PER_CHANNEL_DAY


def logo_url(n):
    key = clean_name(n)
    if key in ('CCTV4K', 'CCTV8K', 'CCTV5+'):
        return f"{LOGO_BASE}/{key}.png"
    return f"{LOGO_BASE}/{LOGO_MAP.get(key, key)}.png"


LOGO_MAP = {
    'CCTV1综合': 'CCTV1', 'CCTV1': 'CCTV1',
    'CCTV2财经': 'CCTV2', 'CCTV2': 'CCTV2',
    'CCTV3综艺': 'CCTV3', 'CCTV3': 'CCTV3',
    'CCTV4中文国际': 'CCTV4', 'CCTV4国际': 'CCTV4', 'CCTV4': 'CCTV4',
    'CCTV5体育': 'CCTV5', 'CCTV5': 'CCTV5',
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

GROUP_ORDER = ["央视频道", "卫视频道", "地方频道", "数字频道", "其他频道"]

_REGION_KW = [
    "北京", "上海", "天津", "重庆", "河北", "山西", "辽宁", "吉林", "黑龙江",
    "江苏", "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南",
    "广东", "广西", "海南", "四川", "贵州", "云南", "陕西", "甘肃", "青海",
    "台湾", "内蒙古", "西藏", "宁夏", "新疆", "香港", "澳门",
]

_DIGITAL_KW = [
    "风云", "怀旧", "经典", "戏曲", "劲爆", "魅力", "欢笑", "家庭", "靓妆",
    "高尔夫", "留学", "茶", "书画", "武术", "橄榄", "汽摩", "电子体育",
    "财富", "证券", "影像", "发现", "求索", "全纪实", "新视觉", "都市",
    "娱乐", "影视", "少儿", "纪录", "国防", "农业", "社会与法", "奥林匹克",
    "农业农村",
]

_TVG_ALIAS = {
    # ── CCTV K 频道 ──
    'CCTV-4K HD (1080p)': 'CCTV4K', 'CCTV-8K HD (1080p)': 'CCTV8K',
    'CCTV-4K': 'CCTV4K', 'CCTV-8K': 'CCTV8K',
    'CCTV4K HD (1080p)': 'CCTV4K', 'CCTV8K HD (1080p)': 'CCTV8K',
    'CCTV4K': 'CCTV4K', 'CCTV8K': 'CCTV8K',
    # ── CCTV5+ 系列 ──
    'CCTV-5+体育赛事': 'CCTV5+', 'CCTV5+体育赛事': 'CCTV5+',
    'CCTV-5+': 'CCTV5+', 'CCTV5+': 'CCTV5+',
    'CCTV-5+ (1080p)': 'CCTV5+', 'CCTV5+ (1080p)': 'CCTV5+',
    'CCTV-5+体育赛事 (1080p)': 'CCTV5+',
    # ── 地方台别名 ──
    '上虞文化影院': '上虞文化影视', '绍兴文化影院': '绍兴文化影视',
    '哈哈炫动': '炫动卡通',
    '凤凰中文': '凤凰卫视中文台', '凤凰卫视': '凤凰卫视中文台',
    '数码时代': '浙江数码时代',
    '象山综合': '象山新闻综合', '缙云综合': '缙云新闻综合',
    '文成综合': '文成新闻综合', '庆元综合': '庆元新闻综合',
    '浙江钱江': '浙江钱江都市', '湖州公共': '湖州公共民生',
    '绍兴公共': '绍兴电视台公共', '涟水电视台综合': '涟水综合',
    '普陀电视台': '普陀新闻综合', '苏州4k': '苏州新闻综合', '苏州4K': '苏州新闻综合',
    '陕西农林': '陕西农林卫视',
    # ── Beijing / CCTV 后缀冲突 ──
    'Beijing Satellite TV HD (1080p)': '北京卫视',
    'Beijing Satellite TV [Not 24/7]': '北京卫视',
    'BeijingSatelliteTV.cn@': '北京卫视',
    'CCTV-4K HD (1080p)': 'CCTV4K', 'CCTV-8K HD (1080p)': 'CCTV8K',
    'CCTV.cn@': 'CCTV综合',
    'CCTV-4 中文国际': 'CCTV4', 'CCTV4中文国际': 'CCTV4',
}


_EXTINF = re.compile(r'^#EXTINF:-?\d+(?:\s+[^,]*)?(?:,)?\s*(.*)$')
_TV = re.compile(r'(?:^|[\s,])tvg-([a-zA-Z0-9_-]+)="([^"]*)"')
_NAME = re.compile(r'tvg-name="([^"]*)"')
_EMOJI = re.compile(r'[\U00010000-\U0010ffff\U00002600-\U000027BF\U0001F300-\U0001FAFF]')
_JUNK_NAME = re.compile(r'\d{4}-\d{2}-\d{2}|\d{2}:\d{2}:\d{2}')
_JUNK_PATTERNS = ('migutoken=', 'Smart.php', 'miguvideo.com')


def clean_name(n):
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
    """归一化频道名，作为频道唯一 key。
    核心规则：
    - CCTV-8K / CCTV-4K 是独立频道，绝不能与 CCTV8 / CCTV4 合并
    - CCTV-5+ 的 '+' 必须保留
    匹配顺序：先 K 频道，再 + 频道，最后普通频道。
    """
    if not raw:
        return ''
    s = raw.strip()
    if s in _TVG_ALIAS:
        return _TVG_ALIAS[s]

    # ① K 频道前置识别 —— 必须在分辨率清洗之前完成
    s = re.sub(r'CCTV\s*-?\s*4\s*[Kk]\b', 'CCTV4K', s, flags=re.I)
    s = re.sub(r'CCTV\s*-?\s*8\s*[Kk]\b', 'CCTV8K', s, flags=re.I)
    # ② 加号保护
    s = re.sub(r'(CCTV\s*-?\s*\d+)\s*\+', r'\1‡PLUS‡', s, flags=re.I)

    # ③ 清洗后缀
    cleaned = re.sub(r'\s*[\(（]\s*[0-9]+[pi]\s*[\)）]', '', s)
    cleaned = re.sub(r'\s*[\(（]\s*[0-9]+[Kk]\s*[\)）]', '', s)
    cleaned = re.sub(r'\s*[\[【].*?[\]】]', '', cleaned)
    cleaned = re.sub(r'\s*(HD|SD|FHD)\b', '', cleaned, flags=re.I)  # 注意：此处不含 4K/8K
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    cleaned = cleaned.replace('‡PLUS‡', '+')

    if cleaned in _TVG_ALIAS:
        return _TVG_ALIAS[cleaned]

    # ⑥ CCTV 系列：按序匹配
    m = re.match(r'CCTV\s*-?\s*(\d{1,2})[Kk]\b', cleaned)
    if m:
        return f'CCTV{m.group(1)}K'
    m = re.match(r'CCTV\s*(\d{1,2})\+', cleaned)
    if m:
        return f'CCTV{m.group(1)}+'
    m = re.match(r'CCTV\s*-?\s*(\d{1,2})\b', cleaned)
    if m:
        return f'CCTV{m.group(1)}'
    m = re.match(r'CCTV\s*(\d{1,2})\.cn@', cleaned)
    if m:
        return f'CCTV{m.group(1)}'

    m = re.match(r'([\u4e00-\u9fa5]{2,4})卫视', cleaned)
    if m:
        return m.group(1) + '卫视'

    cleaned2 = re.sub(r'\s*[\(（].*?[\)）]', '', cleaned).strip()
    if cleaned2 in _TVG_ALIAS:
        return _TVG_ALIAS[cleaned2]

    return _EMOJI.sub('', cleaned).strip()


def tvg_id(n):
    t = clean_name(n)
    return t.replace('卫视', '')


def _resolve_4k8k_key(base_key, name_key, raw_name):
    """解决 K/8K/+ 频道与同名普通频道的 key 冲突。

    规则：
    - 若名字明确是 K/8K/+ 频道（name_key 含 4K/8K/+），则以名字为准，
      即使源的 tvg-id 漏写了 K，也不并入普通频道。
    - 若名字是普通频道（如 CCTV4），但 tvg-id 却带 K，同样以名字为准，
      避免误拆。
    一句话：以"名字"里是否带 K/8K/+' 作为频道身份的最终裁定。
    """
    if not base_key:
        return name_key
    if not name_key:
        return base_key

    base_has_k = bool(re.search(r'[48]K', base_key)) or ('+' in base_key)
    name_has_k = bool(re.search(r'[48]K', name_key)) or ('+' in name_key)

    # 双方一致：直接用归一化结果
    if base_has_k == name_has_k:
        return base_key

    # 不一致：以名字为准
    return name_key


def classify(name):
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
    name = ch.get('name', '') if isinstance(ch, dict) else str(ch)
    group = classify(name)
    gidx = GROUP_ORDER.index(group)
    if group == '央视频道':
        m = re.search(r'(\d+)', name)
        num = int(m.group(1)) if m else 0
        if re.search(r'8K', name, re.I):
            plus = 0.9
        elif re.search(r'4K', name, re.I):
            plus = 0.8
        elif '+' in name:
            plus = 0.5
        else:
            plus = 0.0
        return (gidx, num + plus, name)
    return (gidx, 0, name)


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


# ============================================================
#  点播（未改动）
# ============================================================
async def discover_vod(session):
    urls = set()
    seeds = (CONFIG.get('discover', {}) or {}).get('vod_seeds', []) or []
    for u in seeds:
        urls.add(u)
    print(f"   🔍 配置的种子源 {len(seeds)} 个")

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
                print(f"   🔍 GitHub Search 补充 {len(data.get('items', []))} 个候选")
            elif r.status in (403, 429):
                print(f"⚠️ GitHub Search 限流 ({r.status})，使用种子源")
    except Exception as e:
        print(f"⚠️ discover_vod 失败: {e}")

    result = list(urls)[:MAX_VOD * 3]
    print(f"   🔍 最终待探测点播源 {len(result)} 个")
    return result


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
    print(f"   🔍 aggregate_vod 收到 {len(urls)} 个候选源")
    if not urls:
        print("⚠️ 点播种子源为空：config.yaml 的 discover.vod_seeds 未配置，且 GitHub Search 未返回结果")
        return {}

    bl = get_blacklist('vod_history')
    connector = aiohttp.TCPConnector(limit=VOD_CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as s:
        res = await asyncio.gather(*[probe_vod(s, u, bl) for u in urls])

    valid = [x for x in res if x]
    if not valid:
        print("⚠️ 所有点播候选源都探测失败（可能源失效或被墙）")
        return {}

    valid.sort(key=lambda x: x['size'], reverse=True)
    valid = valid[:MAX_VOD]
    print(f"   ✓ 点播可用 {len(valid)}/{len(urls)} 个，最大 {valid[0]['size']} 字节")

    primary = valid[0]['data']
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


# ============================================================
#  直播
# ============================================================
def parse_playlist(text, default_group=''):
    """解析直播清单，自动识别两种格式：
       1) M3U：#EXTINF 行 + 下一行 URL
       2) TXT：纯文本，每行 "频道名,URL" 且用 ",#genre#" 行分隔分组
    """
    if not text:
        return []
    text = text.lstrip('\ufeff').strip()
    lines = text.split('\n')

    # ── 格式判定：是否含 #EXTINF ──
    has_extinf = any(l.startswith('#EXTINF:') for l in lines[:200])

    if has_extinf:
        return _parse_m3u(lines, default_group)

    return _parse_txt(lines, default_group)


def _parse_m3u(lines, default_group=''):
    chs = []
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
        group = attrs.get('group-title', '').strip() or default_group
        if group:
            group = _EMOJI.sub('', group).strip()
        chs.append({
            'name': name, 'url': url,
            'tvg_id': attrs.get('id', '').strip() or '',
            'logo': attrs.get('logo', '').strip() or '',
            'group': group or '',
        })
    return chs


def _parse_txt(lines, default_group=''):
    """解析 TXT 格式：
       分组行：    "...内容...,#genre#"（逗号在 genre 前）
       频道行：    "频道名,http://..."
       无效行（说明/注释）跳过
    """
    chs = []
    group = default_group
    for l in lines:
        l = l.strip()
        if not l:
            continue
        # 分组行
        if l.endswith('#genre#'):
            g = l[:-len('#genre#')].strip()
            # 去掉行首遗留的说明片段（如 "央视官源,"）
            if g.endswith(','):
                g = g[:-1].strip()
            if g:
                group = g
            continue
        # 只处理含单个逗号分隔的 名称,URL 行
        if ',' not in l:
            continue
        name, url = l.split(',', 1)
        name, url = name.strip(), url.strip()
        if not name or not url:
            continue
        # URL 只取第一个备选（分号/逗号分隔的多个地址取首个）
        url = url.split(';')[0].split(',')[0].strip()
        if not url.startswith(('http://', 'https://')):
            continue
        if url.endswith('.php'):
            continue
        if any(p in url for p in _JUNK_PATTERNS):
            continue
        if _JUNK_NAME.search(name):
            continue
        chs.append({
            'name': name, 'url': url,
            'tvg_id': '', 'logo': '',
            'group': group or '',
        })
    return chs


def is_domestic_url(url):
    try:
        host = re.sub(r'^https?://', '', url).split('/')[0].split(':')[0]
    except Exception:
        return False
    if not host:
        return False
    if host.endswith('.cn') or host == 'cn':
        return True
    domestic_keywords = (
        'cztv.com', 'bupt.edu.cn', 'hebtv.com', 'sdetv.com.cn',
        'bestv.cn', 'sztv.com.cn', 'tdm.com.mo', 'hkstv.tv',
        'fanmingming.com',
        # 成都/四川电信 IPTV 项目（zsdc.eu.org 根域），含 iptv./epg. 等所有子域名
        # 虽为 .eu.org 但属国内电信内网源，应加分
        'zsdc.eu.org',
    )
    for kw in domestic_keywords:
        if kw in host:
            return True
    return False


async def _fetch_source(session, url, is_static, is_4k, idx):
    """抓取单个源并解析成频道列表。
    url 支持两种形式：
      - http(s):// 远程地址，走 fetch_text_any（带缓存）
      - 本地文件路径（含 inputs/ 本地源），直接读文件
    """
    if is_static:
        label = f"静态源#{idx}"
    elif is_4k:
        label = f"4K源#{idx}"
    else:
        label = f"刷新源#{idx}"

    # ── 本地文件（本地源/inputs/）──
    if not url.startswith(('http://', 'https://')):
        chs = _read_local_source(url, label)
        for c in chs:
            c['_static'] = is_static
            c['_4k'] = is_4k
        return chs

    text = await fetch_text_any(session, url, label)
    if not text:
        return []
    chs = parse_playlist(text)
    for c in chs:
        c['_static'] = is_static
        c['_4k'] = is_4k
    print(f"    📺 {label} 解析到 {len(chs)} 个频道")
    return chs


def _read_local_source(path, label):
    """读取本地直播源文件（TXT / M3U 自动识别），返回频道列表"""
    p = Path(path)
    if not p.is_absolute():
        p = Path(__file__).parent / p
    if not p.exists():
        print(f"    ✗ {label} 本地文件不存在: {p}")
        return []
    try:
        text = p.read_text(encoding='utf-8', errors='ignore')
    except Exception as e:
        print(f"    ✗ {label} 读取失败: {e}")
        return []
    chs = parse_playlist(text)
    for c in chs:
        c['_local'] = True
    print(f"    📺 {label} 本地源 {p.name} 解析到 {len(chs)} 个频道")
    return chs


async def discover_live(_session):
    static = list(LIVE_CFG.get('urls_static') or [])
    static_4k = list(LIVE_CFG.get('urls_static_4k') or [])
    refresh = list(LIVE_CFG.get('urls_refresh') or [])
    zsdc = list(LIVE_CFG.get('urls_zsdc') or [])   # 成都/四川电信 IPTV（zsdc 单播源，配套回看+时移）
    force = os.environ.get('FORCE_REFRESH', '').lower() in ('1', 'true', 'yes')

    # ── 本地源目录：自动扫描（默认 inputs/）──
    # 支持的文件：.txt / .m3u / .m3u8
    # 自动按文件名识别分组与类型：含 "4K" 或 "4k" → 走 4K 候选轮次；
    # 含 "refresh" / "鉴权" / "签名" → 走需刷新轮次；其余按静态源处理
    local_dir = LIVE_CFG.get('local_dir', 'inputs')
    local_sources = _discover_local_sources(local_dir)

    all_entries = static + static_4k + refresh + zsdc + local_sources
    if not all_entries:
        print("⚠️ config.yaml 的 live.urls_static / urls_static_4k / urls_refresh / urls_zsdc 均未配置，且本地源目录也为空，直播源将为空")
        return []

    urls = [(u, True, False) for u in static]
    urls += [(u, False, True) for u in static_4k]
    if force or not refresh:
        urls += [(u, False, False) for u in refresh]
    else:
        print(f"    ℹ️ 跳过 {len(refresh)} 个需刷新源（缓存有效；FORCE_REFRESH=1 可强制）")
    # zsdc 单播源作为静态源处理（固定直链），走国内源加分
    urls += [(u, True, False) for u in zsdc]
    # ── 本地源 ──
    for tag, path in local_sources:
        is_4k = tag == '4k'
        urls.append((path, True, is_4k))
    return urls


def _discover_local_sources(local_dir):
    """扫描本地源目录，返回 [(tag, path), ...]。
    tag: 'static' | '4k' | 'refresh'
    识别规则（按文件名，不区分大小写）：
      - 含 4k/4K/8k/8K/ultra        → 4k
      - 含 refresh/刷新/鉴权/签名/过期 → refresh
      - 其余                              → static
    忽略 hidden 文件与 README。
    """
    base = Path(local_dir)
    if not base.is_absolute():
        base = Path(__file__).parent / base
    if not base.exists() or not base.is_dir():
        return []

    result = []
    files = sorted(base.iterdir())
    for f in files:
        if not f.is_file():
            continue
        if f.name.startswith(('.', '_')) or f.name.upper().startswith('README'):
            continue
        if f.suffix.lower() not in ('.txt', '.m3u', '.m3u8'):
            continue
        name = f.stem.lower()
        if any(k in name for k in ('4k', '8k', 'ultra', 'uhd')):
            tag = '4k'
        elif any(k in name for k in ('refresh', '刷新', '鉴权', '签名', '过期', 'token')):
            tag = 'refresh'
        else:
            tag = 'static'
        result.append((tag, str(f)))
    print(f"    📂 本地源目录 {base}: 发现 {len(result)} 个源文件")
    for tag, p in result:
        print(f"       [{tag:7s}] {Path(p).name}")
    return result


async def probe_live_res(url):
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


# ── 缓存 ──
_probe_cache = {}
_cache_hits = 0
_cache_misses = 0


def _evict_cache():
    global _probe_cache
    if len(_probe_cache) > MAX_CACHE_SIZE:
        now = time.time()
        # 清理过期项
        expired = [k for k, (_, ts) in _probe_cache.items() if now - ts > CACHE_TTL_SECONDS]
        for k in expired:
            del _probe_cache[k]
        # 若仍超限，删最老的一半
        if len(_probe_cache) > MAX_CACHE_SIZE:
            items = sorted(_probe_cache.items(), key=lambda x: x[1][1])
            for k, _ in items[:len(items) // 2]:
                del _probe_cache[k]


async def _cached_probe(ch, session, bl, stability_map):
    """带缓存的探测：TTL 内复用 ffprobe 结果，但 score 按最新稳定性重算。"""
    global _cache_hits, _cache_misses
    url = ch['url']
    now = time.time()

    if USE_CACHE and url in _probe_cache:
        rec, ts = _probe_cache[url]
        if now - ts < CACHE_TTL_SECONDS and rec is not None:
            _cache_hits += 1
            return _rescore(rec, ch, stability_map)

    _cache_misses += 1
    rec = await score_live(ch, session, bl, stability_map)
    _probe_cache[url] = (rec, now)
    _evict_cache()
    return rec


def _rescore(rec, ch, stability_map):
    """复用已有探测结果，按最新稳定性/国内属性重算评分。"""
    if not rec:
        return None
    stab = (stability_map or {}).get(rec['url'])
    # get_stability 无记录返回 None；此处无历史视为中性 0.5
    if stab is None:
        stab = 0.5
    domestic = rec.get('_domestic', is_domestic_url(rec['url']))

    res = rec.get('_res') or 0
    res_score = {2160: 100, 1080: 80, 720: 60}.get(res, 30)
    domestic_score = DOMESTIC_BONUS if domestic else OVERSEAS_PENALTY
    stab_score = stab * 100
    latency = rec.get('_latency') or 3

    score = (res_score * NW.get('resolution', 0)
             + 80 * NW.get('speed', 0)
             + max(0, 100 - latency * 10) * NW.get('delay', 0)
             + stab_score * NW.get('stability', 0)
             + domestic_score)
    return {**rec, 'score': round(max(0.0, min(100.0, score)), 1),
            '_domestic': domestic}


async def score_live(ch, session, bl, stability_map=None):
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

    domestic = is_domestic_url(ch['url'])
    if domestic:
        domestic_score = DOMESTIC_BONUS
    else:
        domestic_score = OVERSEAS_PENALTY

    stab = (stability_map or {}).get(ch['url'])
    if stab is None:
        stab = 0.5
    stab_score = stab * 100

    score = (res_score * NW.get('resolution', 0)
             + 80 * NW.get('speed', 0)
             + max(0, 100 - d * 10) * NW.get('delay', 0)
             + stab_score * NW.get('stability', 0)
             + domestic_score)
    score = max(0.0, min(100.0, score))

    # 历史平均分加权（最多 ±5 分）
    avg_score = get_avg_score(ch['url'], HISTORY_WINDOW)
    if avg_score is not None:
        score = min(100.0, score + (avg_score - 50) * 0.1)

    record_live_probe(ch['url'], ch['name'], 'ok', d, res, 0, round(score, 1))
    return {**ch, 'score': round(score, 1), '_res': res,
            '_domestic': domestic, '_latency': d}


def _build_stability_map(chs):
    urls = {c['url'] for c in chs}
    out = {}
    for u in urls:
        out[u] = get_stability(u, STABILITY_WINDOW)
    return out


def _bucket(res):
    if res >= 2160:
        return 2160
    elif res >= 1080:
        return 1080
    elif res >= 720:
        return 720
    else:
        return 480


def _is_dup(rec, picked):
    for p in picked:
        if p['url'] == rec['url']:
            return True
    return False


def _sort_key(x):
    """国内源优先，评分高的优先"""
    return (0 if x.get('_domestic') else 1, -(x.get('score') or 0))


async def probe_channel_once(key, chs, session, bl, stability_map):
    """一轮探测：国内源优先，分清晰度达标即停。
    每频道探测到配额数即停该档位；最终保留 urls_per_channel 条。
    """
    candidates = chs[:CANDIDATES_PER_CHANNEL]
    domestic = [c for c in candidates if is_domestic_url(c['url'])]
    overseas = [c for c in candidates if not is_domestic_url(c['url'])]
    ordered = domestic + overseas

    buckets = {2160: [], 1080: [], 720: [], 480: []}
    counts = {2160: 0, 1080: 0, 720: 0}
    limits = {2160: QUOTA_4K_LIMIT, 1080: QUOTA_1080_LIMIT, 720: QUOTA_720_LIMIT}
    picked = []
    limit = effective_urls_limit()

    for c in ordered:
        if len(picked) >= limit:
            break
        rec = await _cached_probe(c, session, bl, stability_map)
        if not rec:
            continue
        res = rec.get('_res') or 0
        bucket = _bucket(res)
        if bucket not in buckets:
            continue
        if _is_dup(rec, picked):
            continue

        if counts[bucket] < limits[bucket]:
            buckets[bucket].append(rec)
            picked.append(rec)
            counts[bucket] += 1
        elif len(picked) < limit:
            # 该档位已达标，但总条数还不足，可收作"补全"
            picked.append(rec)

    return picked, counts


def pick_final(picked, limit):
    """按 4:5:3 配额精选，不足 limit 条时用国内源优先补满。"""
    by_res = {2160: [], 1080: [], 720: [], 480: []}
    for c in picked:
        by_res[_bucket(c.get('_res') or 0)].append(c)

    final_list = []
    seen_urls = set()
    for res in (2160, 1080, 720):
        quota = RES_QUOTAS.get(res, 0)
        candidates = [c for c in by_res[res] if c['url'] not in seen_urls]
        candidates.sort(key=_sort_key)
        for c in candidates[:quota]:
            final_list.append(c)
            seen_urls.add(c['url'])

    if len(final_list) < limit:
        rest = [c for c in picked if c['url'] not in seen_urls]
        rest.sort(key=_sort_key)
        for c in rest[:limit - len(final_list)]:
            final_list.append(c)
            seen_urls.add(c['url'])

    return final_list


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

    groups = {}
    for ch in all_ch:
        # ── key 生成：优先 tvg-id，但名字里含 K/8K/+ 时必须保留，
        #    避免源的 tvg-id 漏写 K 导致 CCTV4K 被并入 CCTV4 ──
        # 例：tvg-id="CCTV4" tvg-name="CCTV-4K" → 归为 CCTV4K，而非并入 CCTV4
        base_key = normalize_tvg_id(ch.get('tvg_id') or ch['name'])
        name_key = normalize_tvg_id(ch.get('name') or '')
        key = _resolve_4k8k_key(base_key, name_key, ch.get('name', ''))
        if not key:
            continue
        groups.setdefault(key, []).append(ch)
    groups = dict(sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)[:MAX_LIVE_CH])

    # ── 归一化碰撞检测 ──
    print("\n🔎 归一化碰撞检测：")
    collisions = 0
    for key, chs in groups.items():
        names = {c.get('name', '') for c in chs if c.get('name')}
        names.discard('')
        if len(names) > 1:
            distinct = set()
            for n in names:
                norm = re.sub(r'\s*[\(（\[【].*?[\)）\]】]', '', n).strip()
                norm = re.sub(r'\s*(HD|SD|4K|8K)\b', '', norm, flags=re.I).strip()
                distinct.add(norm)
            if len(distinct) > 1:
                collisions += 1
                print(f"   ⚠️ key={key} 真碰撞 {len(distinct)} 个频道: {', '.join(sorted(names))}")
            else:
                print(f"   ✓ key={key} 已合并 {len(names)} 个写法: {sorted(names)[0]}")
    if collisions == 0:
        print("   ✓ 无碰撞，频道不会因归一化丢失")

    stability_map = _build_stability_map(all_ch)
    bl = get_blacklist('live_history')

    mode = "晚高峰" if is_peak_hour() else "白天"
    limit = effective_urls_limit()
    first = is_first_run()
    if first:
        print("   ℹ️ 首次运行，无历史稳定性数据")

    print(f"\n⏰ 当前模式：{mode}，每频道保留 {limit} 路")
    print(f"📋 探测策略：一轮探测 · 缓存复用 · 国内源优先 · "
          f"分清晰度达标即停 (4K≤{QUOTA_4K_LIMIT}, 1080p≤{QUOTA_1080_LIMIT}, 720p≤{QUOTA_720_LIMIT})")

    final = {}
    connector = aiohttp.TCPConnector(limit=PROBE_CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as s:
        for k, chs in groups.items():
            picked, counts = await probe_channel_once(k, chs, s, bl, stability_map)
            if picked:
                final[k] = picked
                detail = f"4K={counts[2160]} 1080p={counts[1080]} 720p={counts[720]}"
                print(f"    [探测] {k}: {len(picked)}/{len(chs[:CANDIDATES_PER_CHANNEL])} 路达标 ({detail})")

    # ── 按配额精选 ──
    selected = OrderedDict()
    for k, urls in final.items():
        picked = pick_final(urls, limit)
        if picked:
            selected[k] = picked

    # ── EPG：读 config.yaml 的 epg.primary，缺省回退仓库内 epg.xml ──
    _epg_cfg = (CONFIG.get('epg', {}) or {})
    epg = _epg_cfg.get('primary') or f"{repo_prefix}/epg.xml"
    # ── zsdc 配套 EPG：zsdc 频道单独打标签，走 epg.zsdc ──
    zsdc_epg = _epg_cfg.get('zsdc') or epg
    _is_zsdc_url = lambda u: 'iptv.zsdc.eu.org' in (u or '')
    _epg_for = lambda u: zsdc_epg if _is_zsdc_url(u) else epg

    channel_map = OrderedDict()
    for key, urls in selected.items():
        if not urls:
            continue
        primary = max(urls, key=lambda x: x.get('score', 0))
        n = primary.get('name', '')
        tid = primary.get('tvg_id') or tvg_id(n)
        max_res = max((c.get('_res') or 0) for c in urls)
        domestic_cnt = sum(1 for c in urls if c.get('_domestic'))
        channel_map[key] = {
            'name': n, 'tvg_id': tid,
            'logo': primary.get('logo') or logo_url(tid or n),
            'group': classify(n), 'urls': urls,
            'resolution': max_res, 'domestic': domestic_cnt,
            'is_zsdc': _is_zsdc_url(primary.get('url', '')),   # zsdc 频道标记
        }

    sorted_items = sorted(channel_map.items(), key=lambda item: channel_sort_key(item[1]))

    grouped_output = OrderedDict((g, []) for g in GROUP_ORDER)
    for key, ch in sorted_items:
        grouped_output[ch['group']].append(ch)

    txt_lines, m3u_lines = [], ['#EXTM3U x-tvg-url="%s"' % epg]
    total_urls = 0
    total_domestic = 0
    zsdc_lines = 0
    for g in GROUP_ORDER:
        chs_in_group = grouped_output.get(g, [])
        if not chs_in_group:
            continue
        m3u_lines.append(f'\n# === {g}（{len(chs_in_group)}）===')
        for ch in chs_in_group:
            n = ch['name']
            res = ch.get('resolution') or 0
            if res >= 2160:
                res_tag = "3840"
            elif res >= 1080:
                res_tag = "1920"
            elif res >= 720:
                res_tag = "1280"
            else:
                res_tag = "720"
            # 该频道走哪套 EPG
            ch_epg = zsdc_epg if ch.get('is_zsdc') else epg
            for c in ch['urls']:
                txt_lines.append(f"{n},{c['url']}")
                line = (
                    f'#EXTINF:-1 tvg-id="{ch["tvg_id"]}" tvg-name="{n}" '
                    f'tvg-logo="{ch["logo"]}" tvg-resolution="{res_tag}" '
                    f'group-title="{g}"'
                )
                # zsdc 频道的每条线路都单独指向 zsdc 配套 EPG
                if ch.get('is_zsdc'):
                    line += f' tvg-url="{zsdc_epg}"'
                    zsdc_lines += 1
                line += f',{n}'
                m3u_lines.append(line)
                m3u_lines.append(c['url'])
                total_urls += 1
                if c.get('_domestic'):
                    total_domestic += 1

    total_ch = len(channel_map)

    # ── 统计稳定性分布 ──
    stable_count = 0
    for ch in channel_map.values():
        for c in ch['urls']:
            stab = stability_map.get(c['url'])
            if stab is not None and stab >= 0.8:
                stable_count += 1

    print(f"\n✅ 一轮探测完成：{len(groups)} 频道探测，{len(selected)} 频道有线路")
    print(f"📊 最终频道数: {total_ch}，分组后: {total_ch}，线路总数: {total_urls}")
    if zsdc_lines:
        print(f"📺 zsdc 成都/四川电信源：{zsdc_lines} 条线路（走配套 EPG {zsdc_epg}）")
    if total_urls > 0:
        print(f"📊 线路来源：国内源 {total_domestic} 条 ({total_domestic/total_urls*100:.0f}%)，"
              f"海外源 {total_urls-total_domestic} 条 ({(total_urls-total_domestic)/total_urls*100:.0f}%)")
        if stable_count > 0:
            print(f"📊 高稳定性线路（≥80%）：{stable_count} 条 ({stable_count/total_urls*100:.0f}%)")
    print(f"📊 缓存命中：{_cache_hits} 次命中 / {_cache_misses} 次未命中")
    print(f"📂 分组统计：")
    for g in GROUP_ORDER:
        cnt = len(grouped_output.get(g, []))
        if cnt:
            print(f"   {g}: {cnt} 个频道")
    if collisions == 0:
        print("🔒 结论：无 key 碰撞，频道与线路均不会丢失")
    else:
        print(f"⚠️ 结论：存在 {collisions} 处真碰撞，请补充 _TVG_ALIAS 后重跑")

    return '\n'.join(txt_lines), '\n'.join(m3u_lines), total_ch, total_urls


# ============================================================
#  主流程
# ============================================================
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
        print(f"   ✓ 点播源已写入 output/tvbox.json（可用源 {vod['_meta']['available']} 个）")
        return 1
    print("   ✗ 点播源为空，未写入 tvbox.json")
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
