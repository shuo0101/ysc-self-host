#!/usr/bin/env python3
"""TVBox 影视仓 全自动聚合（含 EPG + 台标）
v4 特性：
  1. 多代理轮询：每个源自动尝试【直连 → jsDelivr镜像 / GitHub代理】，失败自动换下一个
  2. 本地缓存兜底：网络全失败时回退过期缓存，绝不返回空
  3. tvg-id 归一化：CCTV-1 / CCTV14.cn@SD / CCTV-4 统一成 CCTV14
  4. 脏数据过滤：拆多流行、剔 IPv6 / migutoken 签名 / .php 中转
  5. 分频运行：--live 仅直播，--vod 仅点播，默认全跑
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
URLS_PER_CHANNEL = int(LIVE_CFG.get('urls_per_channel', 6))    # 每频道保留线路数
MIN_RESOLUTION = int(LIVE_CFG.get('min_resolution', 720))      # 最低分辨率
MAX_LIVE_CH = int(LIVE_CFG.get('max_channels', 1500))         # 最多频道数
SORT_WEIGHTS = LIVE_CFG.get('sort_weights') or {              # 评分权重（自动归一化）
    'resolution': 0.40, 'speed': 0.30, 'delay': 0.20, 'stability': 0.10,
}

# 点播参数
VOD_TIMEOUT = int(VOD_CFG.get('timeout', 12))
VOD_CONCURRENCY = int(VOD_CFG.get('concurrency', 100))
MIN_JSON_SIZE = int(VOD_CFG.get('min_json_size', 200))
MAX_VOD = int(VOD_CFG.get('max_sources', 1500))
DISCOVER_PER_PAGE = int(VOD_CFG.get('discover_per_page', 20))

# 多代理参数
GH_PROXIES = list(LIVE_CFG.get('proxies') or [])
JSD_HOSTS = list(LIVE_CFG.get('jsdelivr_hosts') or [])
PER_CANDIDATE_TO = int(LIVE_CFG.get('per_candidate_timeout', 8))   # 单代理/候选超时
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
    # 兼容两种配置位置：discover.vod_seeds（新） / live.vod_seed（旧）
    for key in ('vod_seeds', 'live_seeds'):
        for u in (CONFIG.get('discover', {}) or {}).get(key, []) or []:
            urls.add(u)
    for u in LIVE_CFG.get('vod_seed', []) or []:
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


async def discover_live(_session):
    """兼容两种配置 key：live.urls 优先，回退 discover.live_seeds"""
    return list(
        LIVE_CFG.get('urls')
        or (CONFIG.get('discover', {}) or {}).get('live_seeds')
        or []
    )


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
        # 过滤不可用线路：IPv6 / 带签名 / php 中转
        if url.startswith('http://[') or url.endswith('.php'):
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


async def score_live(ch, session, bl):
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

    sc = {2160: 100, 1080: 80, 720: 60}.get(res, 30)
    # 权重已归一化，保证总和为 1
    score = (sc * NW['resolution']
             + 80 * NW['speed']
             + max(0, 100 - d * 10) * NW['delay']
             + 100 * NW['stability'])
    record_live_probe(ch['url'], ch['name'], 'ok', d, res, 0.5, score)
    return {**ch, 'score': round(score, 1)}


async def aggregate_live(urls, repo_prefix):
    all_ch = []
    async with aiohttp.ClientSession() as s:
        for u in urls:
            label = u.rsplit('/', 1)[-1].split('?')[0] or u[:40]
            text = await fetch_text_any(s, u, label=label)
            if text:
                got = parse_playlist(text)
                all_ch.extend(got)
                print(f"    └ 解析到 {len(got)} 路")

    if not all_ch:
        print("⚠️ 未解析到任何直播频道")
        return '', '', 0, 0

    groups = {}
    for ch in all_ch:
        # 归一化 tvg-id，保证跨源同名频道能合并
        key = normalize_tvg_id(ch.get('tvg_id') or ch['name'])
        if not key:
            continue
        groups.setdefault(key, []).append(ch)

    groups = dict(sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)[:MAX_LIVE_CH])

    bl = get_blacklist('live_history')
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=PROBE_CONCURRENCY)
    ) as s:
        for k, chs in groups.items():
            results = await asyncio.gather(*[score_live(c, s, bl) for c in chs[:15]])
            groups[k] = sorted(
                [r for r in results if r],
                key=lambda x: x['score'], reverse=True,
            )[:URLS_PER_CHANNEL]

    epg = f"{repo_prefix}/epg.xml"
    txt, m3u = [], [f'#EXTM3U x-tvg-url="{epg}"']

    for chs in groups.values():
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

    return '\n'.join(txt), '\n'.join(m3u), len(groups), sum(len(v) for v in groups.values())


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

    repo = os.environ.get('GITHUB_REPOSITORY', '你的用户名/ysc-self-host')
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
