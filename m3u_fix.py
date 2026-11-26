# -*- coding: utf-8 -*-
"""M3U 输出净化：合并同名频道线路 + 清洗显示名/分组名"""
import re

def clean_name(t):
    if not t: return ''
    t = re.sub(r'\s+_info\s*=\s*"[^"]*"', '', t)
    t = re.sub(r'\s+_?info\s*=\s*\S+', '', t)
    t = re.sub(r'\s*[（(]\s*(更新)?时间?\s*[:：][^）)]*[）)]', '', t)
    t = re.sub(r'\s*\|?\s*(更新)?时间?\s*[:：]\s*\S+\s*$', '', t)
    return re.sub(r'\s+', ' ', t).strip()

def clean_group(g, name=''):
    g = re.sub(r'[\u3000\s]+', ' ', (g or '').strip())
    if re.search(r'更新|时间|节目单|直播源|最新|Undefined|undefined|无分组', g):
        g = ''
    if not g:
        if re.search(r'CCTV|CGTN|央视|中央', name):
            g = '📺央视频道'
        elif re.search(r'卫视|BRTV|凤凰', name):
            g = '📡卫视频道'
        else:
            g = '📺其他频道'
    return g

def _key(t):
    return re.sub(r'[\s\-_（）()【】\[\]]+', '', t).lower()

def merge(entries):
    order, bk = [], {}
    for e in entries:
        nm = clean_name(e.name)
        if not nm: continue
        k = _key(nm)
        if k not in bk:
            bk[k] = {'name': nm, 'group': clean_group(e.group, nm), 'urls': [], 'tpl': e}
            order.append(k)
        b = bk[k]
        if e.url not in b['urls']:
            b['urls'].append(e.url)
        if not b['tpl'].logo and e.logo:
            b['tpl'] = e
    return [(bk[k]['name'], bk[k]['group'], bk[k]['urls'], bk[k]['tpl']) for k in order]

def render(entries):
    def esc(s):
        return (s or '').replace('&', '&amp;').replace('"', '&quot;')
    lines = ['#EXTM3U url-tracker-radar="useless"', '']
    for name, group, urls, tpl in merge(entries):
        attrs = [f'tvg-name="{esc(name)}"', f'group-title="{esc(group)}"']
        if tpl.tvg_id: attrs.append(f'tvg-id="{esc(tpl.tvg_id)}"')
        if tpl.logo: attrs.append(f'tvg-logo="{esc(tpl.logo)}"')
        el = (tpl.meta or {}).get('elapsed')
        extra = f' _info="rt={el}s"' if el else ''
        inf = f"#EXTINF:-1 {' '.join(attrs)},{esc(name)}{extra}"
        for u in urls:
            lines.append(inf)
            lines.append(u)
    return '\n'.join(lines) + '\n'
