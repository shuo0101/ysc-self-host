#!/usr/bin/env python3
"""TVBox 影视仓 全自动聚合（含 EPG + 台标）"""
import asyncio, aiohttp, aiofiles, json, re, subprocess, time, os
from datetime import datetime
from pathlib import Path
from database import init_db, record_vod_probe, record_live_probe, get_blacklist, log_run
try:
    import yaml
    _CFG = yaml.safe_load(open('config.yaml', encoding='utf-8')) or {}
except Exception:
    _CFG = {}

_vod_cfg = _CFG.get('vod') or {}
_live_cfg = _CFG.get('live') or {}

MAX_VOD = int(_vod_cfg.get('max_sources', 1500))
MAX_LIVE_CH = int(_live_cfg.get('max_channels', 1500))
LOGO_BASE = (_CFG.get('logo') or {}).get('base_url', "https://live.fanmingming.com/tv")

CONFIG = {
    'vod': {'timeout': _vod_cfg.get('timeout', 12), 'concurrency': _vod_cfg.get('concurrency', 15),
            'min_json_size': _vod_cfg.get('min_json_size', 200)},
    'live': {'timeout': _live_cfg.get('timeout', 8), 'probe_timeout': _live_cfg.get('probe_timeout', 6),
             'concurrency': _live_cfg.get('concurrency', 10),
             'min_resolution': _live_cfg.get('min_resolution', 720),
             'urls_per_ch': _live_cfg.get('urls_per_channel', 6),
             'sort_w': _live_cfg.get('sort_weights') or
                       {'resolution': 0.40, 'speed': 0.30, 'delay': 0.20, 'stability': 0.10}},
}

# ── 工具 ──
def clean_name(n):
    for s in ['4K', '高清', '标清', 'HD', 'SD', '频道', ' ']:
        n = n.replace(s, '')
    if re.match(r'CCTV\d+', n):
        return re.match(r'(CCTV\d+)', n).group(1)
    return n.replace('卫视', '').strip()

def logo_url(n):
    return f"{LOGO_BASE}/{clean_name(n)}.png"

def tvg_id(n):
    return clean_name(n)

# ── 发现 ──
async def discover_vod(session):
    urls = set((_CFG.get('discover') or {}).get('vod_seeds') or [
        "https://gh-proxy.com/https://raw.githubusercontent.com/jifeng250/tvbox-sources/main/urls.json",
        "https://gh-proxy.com/https://raw.githubusercontent.com/wzh15802/tvbox/main/tv.json",
    ])
    try:
        async with session.get("https://api.github.com/search/code?q=tvbox+extension:json&per_page=20",
                               headers={"Accept": "application/vnd.github.v3+json"}, timeout=15) as r:
            # 403/429 等一律不致命，退化为仅使用内置种子
            if r.status == 200:
                data = await r.json()
                for it in data.get('items', []):
                    raw = it.get('raw_url') or it.get('html_url', '').replace(
                        'github.com', 'raw.githubusercontent.com').replace('/blob/', '/')
                    if it.get('html_url') and it.get('html_url').endswith('.json'):
                        urls.add(raw)
    except Exception:
        pass
    return list(urls)[:MAX_VOD * 3]

async def discover_live(session):
    urls = set((_CFG.get('discover') or {}).get('live_seeds') or [
        "https://cdn.jsdelivr.net/gh/Lightconer/TVBox-Sources@main/output/live.txt",
        "https://cdn.jsdelivr.net/gh/Lightconer/TVBox-Sources@main/output/live.m3u",
    ])
    return list(urls)

# ── 点播 ──
async def probe_vod(session, url, bl):
    if url in bl:
        return None
    try:
        st = time.time()
        async with session.get(url, timeout=CONFIG['vod']['timeout']) as r:
            d = time.time() - st
            if r.status != 200:
                record_vod_probe(url, 'fail', d)
                return None
            t = await r.text()
            if len(t) < CONFIG['vod']['min_json_size']:
                record_vod_probe(url, 'invalid', d)
                return None
            record_vod_probe(url, 'ok', d, len(t))
            return {'data': json.loads(t), 'size': len(t)}
    except Exception:
        record_vod_probe(url, 'fail')
        return None

async def aggregate_vod(urls, epg_url):
    bl = get_blacklist('vod_history')
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=CONFIG['vod']['concurrency'])) as s:
        res = await asyncio.gather(*[probe_vod(s, u, bl) for u in urls])
    valid = sorted([x for x in res if x], key=lambda x: x['size'], reverse=True)[:MAX_VOD]
    if not valid:
        return {}
    primary = valid[0]['data']
    # 必须校验结构：解析成功不代表它是 TVBox 配置，避免 TypeError
    if not isinstance(primary, dict):
        log_run(note='primary config is not a dict')
        return {}
    primary['_meta'] = {'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'available': len(valid)}
    primary['lives'] = [{"name": "自动聚合直播", "type": 0, "url": "", "epg": epg_url, "logo": LOGO_BASE + "/"}]
    return primary

# ── 直播 ──
def parse_playlist(text):
    chs = []
    if '#EXTINF' in text:
        lines = text.strip().split('\n')
        for i, l in enumerate(lines):
            if l.startswith('#EXTINF:'):
                name = re.sub(r'^#EXTINF:-?\d+\s*', '', l).strip()
                if i + 1 < len(lines) and lines[i + 1].strip().startswith(('http', 'https')):
                    chs.append({'name': name, 'url': lines[i + 1].strip()})
    return chs

async def probe_live_res(url):
    try:
        r = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_streams',
             '-timeout', '5000000', url],
            capture_output=True, text=True, timeout=6)
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
    # d 必须在 try 外初始化，防止 HEAD 请求异常时引用未定义变量
    d = None
    try:
        st = time.time()
        async with session.head(ch['url'], timeout=CONFIG['live']['timeout']) as r:
            d = time.time() - st
            if r.status not in (200, 206, 302):
                record_live_probe(ch['url'], ch['name'], 'fail', d)
                return None
    except Exception:
        record_live_probe(ch['url'], ch['name'], 'fail', d)
        return None
    res = await probe_live_res(ch['url'])
    if not res or res < CONFIG['live']['min_resolution']:
        record_live_probe(ch['url'], ch['name'], 'fail', d)
        return None
    w = CONFIG['live']['sort_w']
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
            except Exception:
                pass
    groups = {}
    for ch in all_ch:
        groups.setdefault(clean_name(ch['name']), []).append(ch)
    groups = dict(sorted(groups.items(), key=lambda x: len(x[1]), reverse=True)[:MAX_LIVE_CH])
    bl = get_blacklist('live_history')
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=CONFIG['live']['concurrency'])) as s:
        for k, chs in groups.items():
            results = await asyncio.gather(*[score_live(c, s, bl) for c in chs[:15]])
            groups[k] = sorted([r for r in results if r], key=lambda x: x['score'], reverse=True)[:CONFIG['live']['urls_per_ch']]
    txt, m3u = [], [f'#EXTM3U x-tvg-url="{repo_prefix}/epg.xml"']
    for chs in groups.values():
        if not chs:
            continue
        n = chs[0]['name']
        lg = logo_url(n)
        tid = tvg_id(n)
        grp = '央视' if tid.startswith('CCTV') else '卫视'
        for c in chs:
            txt.append(f"{n},{c['url']}")
            m3u += [f'#EXTINF:-1 tvg-id="{tid}" tvg-name="{n}" tvg-logo="{lg}" group-title="{grp}",{n}', c['url']]
    return '\n'.join(txt), '\n'.join(m3u), len(groups), sum(len(v) for v in groups.values())

# ── 主 ──
async def main():
    t0 = time.time()
    init_db()
    Path('output').mkdir(exist_ok=True)
    repo = os.environ.get('GITHUB_REPOSITORY', '你的用户名/tvbox-auto-sources')
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
            'updated': datetime.now().isoformat()
        }, ensure_ascii=False, indent=2))
    log_run(vd=len(vod_urls), vo=1 if vod else 0, ld=len(live_urls), lc=lc, ll=ll, dur=round(time.time() - t0, 1))
    print(f"✅ 完成 {lc}频道 {ll}线路")

if __name__ == '__main__':
    asyncio.run(main())
