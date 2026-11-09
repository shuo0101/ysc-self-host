#!/usr/bin/env python3
"""TVBox 影视仓 · 本地 Web 监控面板

功能：
  - 直播源健康度、可用数量统计
  - 点播源可用数量统计
  - 运行历史（最近 N 次聚合结果）
  - 缓存目录状态（文件数、总大小、最后更新时间）

统计口径：可用数量 = last_status='ok' 的数量。

live_history 已改为「追加写入」结构（每次探测新增一行，自增 id 为主键），
旧的 enabled / success_count / consecutive_fails 等覆盖式字段已废弃，
连续失败黑名单改由 get_blacklist() 实时统计该 URL 最近 5 条记录判定。

数据库说明：
  - 数据库路径统一引用 config.yaml 的 live.db_path（默认 data/sources.db），
    与 database.py 中的 DB 保持一致，避免面板与聚合器各读各的库。
  - run_log 表列名以 database.py 的建表语句为准：run_time / vod_discovered /
    vod_output / live_discovered / live_channels / live_lines / duration。
"""
import os
import json
import time
import yaml
from pathlib import Path
from datetime import datetime

try:
    from flask import Flask, jsonify, render_template_string, request
except ImportError:
    raise SystemExit("缺少 flask：pip install flask")

# ── 配置读取（与 aggregator.py 保持一致）──────────────────────────────
CONFIG_FILE = Path(__file__).parent / 'config.yaml'


def _load_config():
    if not CONFIG_FILE.exists():
        return {}
    try:
        with CONFIG_FILE.open(encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


CONFIG = _load_config()
LIVE_CFG = CONFIG.get('live', {})

# 与 database.py 的 DB 保持一致；面板与聚合器必须读同一个库
DB_PATH = Path(__file__).parent / (LIVE_CFG.get('db_path') or 'data/sources.db')
CACHE_DIR = Path(__file__).parent / LIVE_CFG.get('cache_dir', 'sources_cache')

app = Flask(__name__)


# ── 数据库读取 ────────────────────────────────────────────────────────
def _query(sql, params=()):
    import sqlite3
    if not DB_PATH.exists():
        return []
    try:
        con = sqlite3.connect(str(DB_PATH))
        con.row_factory = sqlite3.Row
        rows = con.execute(sql, params).fetchall()
        con.close()
        return rows
    except Exception as e:
        print(f"⚠️ 查询失败: {e}")
        return []


def _fmt_size(n):
    n = float(n)
    if n >= 1024 * 1024:
        return f"{n / 1024 / 1024:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n:.0f} B"


def _fmt_age(ts):
    if not ts:
        return '未知'
    try:
        age = time.time() - float(ts)
    except Exception:
        return '未知'
    if age < 3600:
        return f"{age / 60:.0f} 分钟前"
    if age < 86400:
        return f"{age / 3600:.1f} 小时前"
    return f"{age / 86400:.1f} 天前"


# ── 统计接口 ──────────────────────────────────────────────────────────
@app.route('/api/stats')
def stats_api():
    """总览统计：直播可用数、点播可用数、运行次数"""
    live = _query("""
        SELECT
            COUNT(*)                          AS total,
            SUM(CASE WHEN last_status='ok' THEN 1 ELSE 0 END) AS ok,
            SUM(CASE WHEN last_status='fail' THEN 1 ELSE 0 END) AS fail
        FROM live_history
    """)
    vod = _query("""
        SELECT
            COUNT(*)                          AS total,
            SUM(CASE WHEN last_status='ok' THEN 1 ELSE 0 END) AS ok
        FROM vod_history
    """)
    runs = _query("SELECT COUNT(*) AS n FROM run_log")
    live_r, vod_r, runs_n = live[0], vod[0], runs[0]

    return jsonify({
        'live':  {'total': live_r['total'], 'ok': live_r['ok'], 'fail': live_r['fail']},
        'vod':   {'total': vod_r['total'], 'ok': vod_r['ok']},
        'runs':  runs_n['n'],
        'cache': _cache_info(),
        'time':  datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    })


@app.route('/api/history')
def history_api():
    """最近 N 次运行记录

    列名必须与 database.py 的 run_log 建表语句一致：
    run_time / vod_discovered / vod_output / live_discovered /
    live_channels / live_lines / duration
    """
    limit = int(request.args.get('limit', 20))
    rows = _query(
        "SELECT * FROM run_log ORDER BY id DESC LIMIT ?", (limit,))
    return jsonify([
        {
            'id': r['id'], 'time': r['run_time'],
            'vod_discovered': r['vod_discovered'], 'vod_output': r['vod_output'],
            'live_discovered': r['live_discovered'], 'live_channels': r['live_channels'],
            'live_lines': r['live_lines'], 'duration': r['duration'],
        }
        for r in rows
    ])


@app.route('/api/cache')
def cache_api():
    """缓存目录状态：文件数、总大小、最后更新时间"""
    return jsonify(_cache_info())


def _cache_info():
    info = {'dir': str(CACHE_DIR), 'exists': CACHE_DIR.exists(),
            'files': [], 'count': 0, 'total_size': 0, 'last_update': None}
    if not CACHE_DIR.exists():
        return info
    entries = []
    for p in sorted(CACHE_DIR.iterdir()):
        if not p.is_file():
            continue
        st = p.stat()
        entries.append({
            'name': p.name,
            'size': st.st_size, 'size_fmt': _fmt_size(st.st_size),
            'mtime': st.st_mtime, 'age': _fmt_age(st.mtime),
        })
        info['total_size'] += st.st_size
        if info['last_update'] is None or st.st_mtime > info['last_update']:
            info['last_update'] = st.mtime
    info['files'] = entries
    info['count'] = len(entries)
    info['total_size_fmt'] = _fmt_size(info['total_size'])
    info['last_update_fmt'] = _fmt_age(info['last_update'])
    return info


# ── 页面 ──────────────────────────────────────────────────────────────
INDEX_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TVBox 影视仓 · 监控面板</title>
<style>
  * { box-sizing: border-box; }
  body { font-family: -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif;
         margin: 0; background: #0f1419; color: #e6edf3; }
  header { background: #161b22; padding: 18px 24px; border-bottom: 1px solid #30363d; }
  header h1 { margin: 0; font-size: 20px; }
  header .time { color: #8b949e; font-size: 13px; margin-top: 4px; }
  .wrap { padding: 24px; max-width: 1200px; margin: 0 auto; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
          gap: 16px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
          padding: 18px; }
  .card h3 { margin: 0 0 12px; font-size: 13px; color: #8b949e; font-weight: 500; }
  .card .num { font-size: 32px; font-weight: 700; }
  .card .num.ok { color: #3fb950; }
  .card .num.fail { color: #f85149; }
  .card .num.warn { color: #d29922; }
  .card .sub { font-size: 12px; color: #8b949e; margin-top: 6px; }
  .section { margin-top: 28px; }
  .section h2 { font-size: 16px; border-bottom: 1px solid #30363d; padding-bottom: 8px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #21262d; }
  th { color: #8b949e; font-weight: 500; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 12px;
           font-size: 12px; }
  .badge.ok { background: rgba(63,185,80,.15); color: #3fb950; }
  .badge.fail { background: rgba(248,81,73,.15); color: #f85149; }
  .badge.warn { background: rgba(210,153,34,.15); color: #d29922; }
  .cache-list { font-size: 13px; }
  .cache-list li { padding: 6px 0; border-bottom: 1px solid #21262d;
                   display: flex; justify-content: space-between; }
  .cache-list .name { font-family: monospace; }
  .refresh { background: #238636; color: #fff; border: 0; padding: 8px 16px;
             border-radius: 6px; cursor: pointer; font-size: 13px; }
</style>
</head>
<body>
<header>
  <h1>📺 TVBox 影视仓 · 聚合监控面板</h1>
  <div class="time" id="time"></div>
</header>
<div class="wrap">
  <div style="margin-bottom:16px;">
    <button class="refresh" onclick="loadAll()">🔄 刷新数据</button>
  </div>

  <div class="grid">
    <div class="card">
      <h3>直播源 · 可用</h3>
      <div class="num ok" id="live-ok">–</div>
      <div class="sub" id="live-sub">共 0 条已探测</div>
    </div>
    <div class="card">
      <h3>直播源 · 失败</h3>
      <div class="num fail" id="live-fail">–</div>
      <div class="sub">可加入黑名单后下次跳过</div>
    </div>
    <div class="card">
      <h3>点播源 · 可用</h3>
      <div class="num ok" id="vod-ok">–</div>
      <div class="sub" id="vod-sub">共 0 条已探测</div>
    </div>
    <div class="card">
      <h3>聚合运行次数</h3>
      <div class="num" id="runs">–</div>
      <div class="sub">累计 workflow 成功次数</div>
    </div>
  </div>

  <div class="section">
    <h2>🗄️ 缓存目录状态</h2>
    <div class="card">
      <div id="cache-body">加载中…</div>
    </div>
  </div>

  <div class="section">
    <h2>📜 最近运行记录</h2>
    <table>
      <thead>
        <tr><th>ID</th><th>时间</th><th>点播发现/产出</th><th>直播发现</th>
            <th>频道</th><th>线路</th><th>耗时</th></tr>
      </thead>
      <tbody id="history-body"></tbody>
    </table>
  </div>
</div>

<script>
function loadStats() {
  fetch('/api/stats').then(r => r.json()).then(d => {
    document.getElementById('live-ok').textContent = d.live.ok;
    document.getElementById('live-fail').textContent = d.live.fail;
    document.getElementById('live-sub').textContent = '共 ' + d.live.total + ' 条已探测';
    document.getElementById('vod-ok').textContent = d.vod.ok;
    document.getElementById('vod-sub').textContent = '共 ' + d.vod.total + ' 条已探测';
    document.getElementById('runs').textContent = d.runs;
    document.getElementById('time').textContent = '最后刷新：' + d.time;
    renderCache(d.cache);
  }).catch(e => console.error(e));
}

function renderCache(c) {
  const el = document.getElementById('cache-body');
  if (!c.exists) {
    el.innerHTML = '<span class="badge fail">缓存目录不存在</span>'
      + '<p class="sub">目录：' + c.dir + '</p>'
      + '<p class="sub">首次运行后会自动生成，记得提交进仓库让 CI 也能用。</p>';
    return;
  }
  if (c.count === 0) {
    el.innerHTML = '<span class="badge warn">缓存目录为空</span>'
      + '<p class="sub">尚未抓取过源，或缓存已过期被清理。</p>';
    return;
  }
  let html = '<p><span class="badge ok">' + c.count + ' 个文件</span>'
    + ' <span class="sub">总大小 ' + c.total_size_fmt
    + ' · 最后更新 ' + c.last_update_fmt + '</span></p>'
    + '<ul class="cache-list">';
  c.files.forEach(f => {
    html += '<li><span class="name">' + f.name + '</span>'
      + '<span class="sub">' + f.size_fmt + ' · ' + f.age + '</span></li>';
  });
  html += '</ul>';
  el.innerHTML = html;
}

function loadHistory() {
  fetch('/api/history?limit=20').then(r => r.json()).then(rows => {
    const el = document.getElementById('history-body');
    if (!rows.length) { el.innerHTML = '<tr><td colspan="7" class="sub">暂无记录</td></tr>'; return; }
    el.innerHTML = rows.map(r => {
      // vod_output：本次成功写入 tvbox.json 的点播源数（0 表示无可用主源）
      // live_lines：本次产出的可用线路总数，比 live_channels 更能反映可用量
      const ok = r.vod_output ? '<span class="badge ok">' + r.vod_output + '</span>'
                              : '<span class="badge fail">0</span>';
      return '<tr><td>' + r.id + '</td><td>' + r.time + '</td>'
        + '<td>' + ok + ' / ' + r.vod_discovered + '</td>'
        + '<td>' + r.live_discovered + '</td>'
        + '<td>' + r.live_channels + '</td><td>' + r.live_lines + '</td>'
        + '<td>' + r.duration + 's</td></tr>';
    }).join('');
  }).catch(e => console.error(e));
}

function loadAll() { loadStats(); loadHistory(); }
loadAll();
setInterval(loadAll, 60000);
</script>
</body>
</html>
"""


@app.route('/')
def index():
    return render_template_string(INDEX_HTML)


# ── 入口 ──────────────────────────────────────────────────────────────
if __name__ == '__main__':
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', 8080))
    print(f"🚀 监控面板启动: http://{host}:{port}")
    print(f"   数据库: {DB_PATH}  {'✅' if DB_PATH.exists() else '⚠️ 不存在'}")
    print(f"   缓存目录: {CACHE_DIR}  {'✅' if CACHE_DIR.exists() else '⚠️ 不存在'}")
    app.run(host=host, port=port, debug=False)
