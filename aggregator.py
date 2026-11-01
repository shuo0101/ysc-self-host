#!/usr/bin/env python3
"""TVBox 影视仓 全自动聚合（含 EPG + 台标）"""
import asyncio
import aiohttp
import aiofiles
import json
import re
import subprocess
import time
import os
import yaml
from datetime import datetime
from pathlib import Path
from database import init_db, record_vod_probe, record_live_probe, get_blacklist, log_run

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

MAX_VOD = int(CONFIG.get('vod', {}).get('max_sources', 1500))
MAX_LIVE_CH = int(CONFIG.get('live', {}).get('max_channels', 1500))
LOGO_BASE = CONFIG.get('logo_base', "https://live.fanmingming.com/tv")

# 台标映射：fanmingming 命名是 CCTV1.png 这种纯编号，不带"综合/国际"后缀
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

DEFAULT_CONFIG = {
    'vod': {
        'timeout': 12, 'concurrency': 15, 'min_json_size': 200,
        'discover_per_page': 20,
    },
    'live': {
        'timeout': 8, 'probe_timeout': 6, 'concurrency': 10,
        'min_resolution': 720, 'urls_per_channel': 6,
        'sort_weights': {
            'resolution': 0.40, 'speed': 0.30, 'delay': 0.20, 'stability': 0.10,
        },
        'urls': [
            "https://cdn.jsdelivr.net/gh/Lightconer/TVBox-Sources@main/output/live.txt",
            "https://cdn.jsdelivr.net/gh/Lightconer/TVBox-Sources@main/output/live.m3u",
            "https://iptv-org.github.io/iptv/categories/china.m3u",
        ],
        'vod_seed': [
            "https://gh-proxy.com/https://raw.githubusercontent.com/jifeng250/tvbox-sources/main/urls.json",
            "https://gh-proxy.com/https://raw.githubusercontent.com/wzh15802/tvbox/main/tv.json",
        ],
    },
}

# 合并配置：DEFAULT_CONFIG 为底，CONFIG 覆盖
def _deep_merge(base, override):
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base

CFG = _deep_merge(dict(DEFAULT_CONFIG), CONFIG)
VOD_CFG = CFG['vod']
LIVE_CFG = CFG['live']

# ── m3u 解析正则 ──
_EXTINF = re.compile(
    r'^#EXTINF:-?\d+'
    r'(?:\s+[^,]*)?'   # 空格分隔的属性段（若有）
    r'(?:,)?'
    r'\s*(.*)$'        # 逗号后真正的显示名
)
_TV = re.compile(r'(?:^|[\s,])tvg-([a-zA-Z0-9_-]+)="([^"]*)"')
_NAME = re.compile(r'tvg-name="([^"]*)"')

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

def logo_url(n):
    key = clean_name(n)
    return f"{LOGO_BASE}/{LOGO_MAP.get(key, key)}.png"

def tvg_id(n):
    """用于匹配 EPG 的规范 ID，卫视在这里去掉"""
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

# ── 发现 ──
async def discover_vod(session):
    urls = set(LIVE_CFG.get('vod_seed', []))
    per_page = int(VOD_CFG.get('discover_per_page', 20))
    try:
        async with session.get(
            f"https://api.github.com/search/code?q=tvbox+extension:json&per_page={per_page}",
            headers={"Accept": "application/vnd.github.v3+json"},
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
    return list(LIVE_CFG.get('urls', []))

# ── 点播 ──
async def probe_vod(session, url, bl):
    if url in bl:
        return None
    try:
        st = time.time()
        async with session.get(url, timeout=VOD_CFG['timeout']) as r:
            d = time.time() - st
            if r.status != 200:
                record_vod_probe(url, 'fail', d)
                return None
            t = await r.text()
            if len(t) < VOD_CFG['min_json_size']:
                record_vod_probe(url, 'invalid', d)
                return None
            record_vod_probe(url, 'ok', d, len(t))
            return {'data': json.loads(t), 'size': len(t)}
    except Exception:
        record_vod_probe(url, 'fail')
        return None

async def aggregate_vod(urls, epg_url):
    bl = get_blacklist('vod_history')
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=VOD_CFG['concurrency'])
    ) as s:
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
def parse_playlist(text):
    """按 m3u 规范解析：属性段 + 逗号后的显示名分开取"""
    chs, lines = [], text.strip().split('\n')
    for i, l in enumerate(lines):
        if not l.startswith('#EXTINF:'):
            continue
        url = lines[i + 1].strip() if i + 1 < len(lines) else ''
        if not url.startswith(('http://', 'https://')):
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
        chs.append({
            'name': name,
            'url': url,
            'tvg_id': attrs.get('id', '').strip() or '',
            'logo': attrs.get('logo', '').strip() or '',
            'group': attrs.get('group-title', '').strip() or '',
        })
    return chs

async def probe_live_res(url):
    try:
        r = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-print_format', 'json',
             '-show_streams', '-timeout', '5000000', url],
            capture_output=True, text=True, timeout=6,
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
        async with session.head(ch['url'], timeout=LIVE_CFG['timeout']) as r:
            d = time.time() - st
            if r.status not in (200, 206, 302):
                record_live_probe(ch['url'], ch['name'], 'fail', d)
                return None
    except Exception:
        record_live_probe(ch['url'], ch['name'], 'fail', d)
        return None

    res = await probe_live_res(ch['url'])
    if not res or res < LIVE_CFG['min_resolution']:
        record_live_probe(ch['url'], ch['name'], 'fail', d)
        return None

    w = LIVE_CFG['sort_weights']
    sc = {2160: 100, 1080: 80, 720: 60}.get(res, 30)
    score = sc * w['resolution'] + 80 * w['speed'] + max(0, 100 - d * 10) * w['delay'] + 100 * w['stability']
    record_live_probe(ch['url'], ch['name'], 'ok', d, res, 0.5, score)
    return {**ch, 'score': round(score, 1)}

async def aggregate_live(urls, repo_prefix):
    all_ch = []
    async with aiohttp.ClientSession() as s:
        for u in urls:
            try:
                async with s.get(u, timeout=15) as r:
                    if r.status == 200:
                        all_ch.extend(parse_playlist(await r.text()))
            except Exception as e:
                print(f"⚠️ 拉取直播源失败 {u}: {e}")

    if not all_ch:
        print("⚠️ 未解析到任何直播频道")
        return '', '', 0, 0

    groups = {}
    for ch in all_ch:
        # 用 tvg-id 优先、否则显示名分组
        key = ch['tvg_id'] or ch['name']
        groups.setdefault(key, []).append(ch)

    # 按源数量排序取前 N
    groups = dict(sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)[:MAX_LIVE_CH])

    bl = get_blacklist('live_history')
    urls_per_ch = int(LIVE_CFG.get('urls_per_channel', 6))
    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(limit=LIVE_CFG['concurrency'])
    ) as s:
        for k, chs in groups.items():
            results = await asyncio.gather(*[score_live(c, s, bl) for c in chs[:15]])
            groups[k] = sorted(
                [r for r in results if r],
                key=lambda x: x['score'], reverse=True,
            )[:urls_per_ch]

    epg = f"{repo_prefix}/epg.xml"
    txt, m3u = [], [f'#EXTM3U x-tvg-url="{epg}"']

    for chs in groups.values():
        if not chs:
            continue
        n = chs[0]['name']                      # 显示名，保持原样
        tid = chs[0].get('tvg_id') or tvg_id(n) # 匹配 EPG 用
        lg = chs[0].get('logo') or logo_url(tid or n)
        # 分组：源自带 group 优先，否则按名称归类
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
async def main():
    t0 = time.time()
    init_db()
    Path('output').mkdir(exist_ok=True)

    repo = os.environ.get('GITHUB_REPOSITORY', '你的用户名/ysc-self-host')
    branch = os.environ.get('GITHUB_REF_NAME', 'main')
    prefix = f"https://cdn.jsdelivr.net/gh/{repo}@{branch}/output"
    epg = f"{prefix}/epg.xml"

    async with aiohttp.ClientSession() as s:
        vod_urls = await discover_vod(s)
        live_urls = await discover_live(s)

    vod = await aggregate_vod(vod_urls, epg)
    if vod and isinstance(vod, dict):
        vod['lives'][0]['url'] = f"{prefix}/live.m3u"

    txt, m3u, lc, ll = await aggregate_live(live_urls, prefix)

    if vod:
        async with aiofiles.open('output/tvbox.json', 'w') as f:
            await f.write(json.dumps(vod, ensure_ascii=False, indent=2))
    async with aiofiles.open('output/live.txt', 'w') as f:
        await f.write(txt)
    async with aiofiles.open('output/live.m3u', 'w') as f:
        await f.write(m3u)
    async with aiofiles.open('output/live_config.json', 'w') as f:
        await f.write(json.dumps({
            'lives': [{'name': '直播', 'url': f'{prefix}/live.m3u', 'epg': epg}],
            'updated': datetime.now().isoformat(),
        }, ensure_ascii=False, indent=2))

    log_run(vd=len(vod_urls), vo=1 if vod else 0,
            ld=len(live_urls), lc=lc, ll=ll, dur=round(time.time() - t0, 1))
    print(f"✅ 完成 {lc}频道 {ll}线路")

if __name__ == '__main__':
    asyncio.run(main())
