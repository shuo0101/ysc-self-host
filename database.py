import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DB = Path("data/sources.db")
# 每次连接最多等 30s 拿锁，CI 并发时避免偶发 "database is locked"
CONNECT_TIMEOUT = 30.0
# 进程级连接单例
_conn = None


def _connect():
    global _conn
    if _conn is not None:
        try:
            _conn.execute("SELECT 1")
            return _conn
        except Exception:
            _conn = None
    _conn = sqlite3.connect(DB, timeout=CONNECT_TIMEOUT)
    return _conn


def init_db():
    """初始化全部表。live_history 采用「追加写入」结构。"""
    DB.parent.mkdir(exist_ok=True)
    c = _connect()
    c.execute('''CREATE TABLE IF NOT EXISTS vod_history (
        url TEXT PRIMARY KEY, last_status TEXT, last_delay REAL, last_size INTEGER,
        success_count INTEGER DEFAULT 0, fail_count INTEGER DEFAULT 0, consecutive_fails INTEGER DEFAULT 0,
        last_checked TEXT, score REAL DEFAULT 50, enabled INTEGER DEFAULT 1)''')
    c.execute('''CREATE TABLE IF NOT EXISTS live_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT, channel_name TEXT,
        last_status TEXT, last_delay REAL, last_resolution INTEGER, last_speed REAL,
        last_score REAL, last_checked TEXT)''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_live_history_url_time
        ON live_history(url, last_checked DESC)''')
    c.execute('''CREATE TABLE IF NOT EXISTS run_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, run_time TEXT, vod_discovered INTEGER,
        vod_output INTEGER, live_discovered INTEGER, live_channels INTEGER, live_lines INTEGER, duration REAL)''')
    c.commit()
    c.close()
    _migrate_live_history()


def _migrate_live_history():
    """一次性迁移旧表到追加写入结构。"""
    c = _connect()
    cols = c.execute("PRAGMA table_info(live_history)").fetchall()
    col_names = {r[1] for r in cols}
    c.close()
    if 'id' in col_names:
        return
    c = _connect()
    c.execute("ALTER TABLE live_history RENAME TO live_history_old")
    c.execute('''CREATE TABLE live_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT, channel_name TEXT,
        last_status TEXT, last_delay REAL, last_resolution INTEGER, last_speed REAL,
        last_score REAL, last_checked TEXT)''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_live_history_url_time
        ON live_history(url, last_checked DESC)''')
    rows = c.execute('''SELECT url, channel_name, last_status, last_delay,
        last_resolution, last_speed, last_score, last_checked
        FROM live_history_old
        GROUP BY url
        HAVING last_checked = MAX(last_checked)''').fetchall()
    for r in rows:
        c.execute('''INSERT INTO live_history
            (url, channel_name, last_status, last_delay, last_resolution,
            last_speed, last_score, last_checked) VALUES (?,?,?,?,?,?,?,?)''', r)
    c.execute("DROP TABLE live_history_old")
    c.commit()
    c.close()
    print(f"    └ live_history 已迁移到追加写入结构，迁入 {len(rows)} 条历史记录")


def record_live_probe(url, channel, status, delay=0, resolution=0, speed=0, score=0):
    """追加写入一条探测记录（不覆盖历史）。"""
    c = _connect()
    c.execute('''INSERT INTO live_history
        (url, channel_name, last_status, last_delay, last_resolution,
        last_speed, last_score, last_checked) VALUES (?,?,?,?,?,?,?,?)''',
        (url, channel, status, delay, resolution, speed, score, datetime.now().isoformat()))
    c.commit()
    c.close()


def record_vod_probe(url, status, delay=0, size=0):
    """vod_history 保持覆盖更新语义。"""
    c = _connect()
    row = c.execute("SELECT success_count,fail_count,consecutive_fails FROM vod_history WHERE url=?", (url,)).fetchone()
    if row is None:
        sc, fc, cf = (1 if status == 'ok' else 0, 0 if status == 'ok' else 1, 0 if status == 'ok' else 1)
    else:
        sc = row[0] + (1 if status == 'ok' else 0)
        fc = row[1] + (0 if status == 'ok' else 1)
        cf = 0 if status == 'ok' else (row[2] + 1)
    en = 0 if cf >= 5 else 1
    c.execute('''INSERT OR REPLACE INTO vod_history
        (url, last_status, last_delay, last_size, success_count, fail_count,
        consecutive_fails, last_checked, enabled) VALUES (?,?,?,?,?,?,?,?,?)''',
        (url, status, delay, size, sc, fc, cf, datetime.now().isoformat(), en))
    c.commit()
    c.close()


def get_blacklist(table):
    """黑名单：基于最近 5 次连续失败判定，兼容新旧结构。"""
    c = _connect()
    bl = set()
    if table == 'live_history':
        urls = c.execute("SELECT DISTINCT url FROM live_history").fetchall()
        for (url,) in urls:
            rows = c.execute(
                "SELECT last_status FROM live_history WHERE url=? ORDER BY last_checked DESC LIMIT 5",
                (url,),
            ).fetchall()
            if rows and all(r[0] != 'ok' for r in rows):
                bl.add(url)
    else:
        cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
        c.execute(f"UPDATE {table} SET enabled=1 WHERE enabled=0 AND last_checked<?", (cutoff,))
        c.commit()
        bl = {r[0] for r in c.execute(f"SELECT url FROM {table} WHERE enabled=0").fetchall()}
    c.close()
    return bl


def query_live_history(url, limit=10):
    """按 last_checked 倒序取该 URL 最近 limit 条真实探测记录。"""
    c = _connect()
    rows = c.execute(
        "SELECT last_status, last_score FROM live_history "
        "WHERE url=? ORDER BY last_checked DESC LIMIT ?",
        (url, limit),
    ).fetchall()
    c.close()
    return [{'status': r[0], 'score': r[1]} for r in rows]


def get_stability(url, window=10):
    """实时聚合该 URL 最近 window 次探测的成功率。无记录返回 None。"""
    history = query_live_history(url, limit=window)
    if not history:
        return None
    ok = sum(1 for h in history if h['status'] == 'ok')
    return ok / len(history)


def get_avg_score(url, window=20):
    """该 URL 最近 window 次探测的平均评分。无记录返回 None。"""
    history = query_live_history(url, limit=window)
    scores = [h['score'] for h in history if h.get('score') is not None]
    if not scores:
        return None
    return sum(scores) / len(scores)


def is_first_run():
    """判断是否为首次运行（无历史探测记录）。"""
    c = _connect()
    r = c.execute("SELECT COUNT(*) as c FROM live_history").fetchone()
    c.close()
    return (r[0] if r else 0) == 0


def log_run(**kw):
    c = _connect()
    c.execute('''INSERT INTO run_log VALUES (NULL,?,?,?,?,?,?,?)''',
        (datetime.now().isoformat(), kw.get('vd', 0), kw.get('vo', 0), kw.get('ld', 0),
        kw.get('lc', 0), kw.get('ll', 0), kw.get('dur', 0)))
    c.commit()
    c.close()
