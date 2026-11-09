import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DB = Path("data/sources.db")
# 每次连接最多等 30s 拿锁，CI 并发时避免偶发 "database is locked"
CONNECT_TIMEOUT = 30.0
# 进程级连接单例：sqlite 默认串行化写锁，同一进程内多连接并发写会互相等锁、
# 甚至因某连接未关闭而让后续连接全部报 I/O error。改为复用同一连接。
_conn = None


def _connect():
    global _conn
    if _conn is not None:
        try:
            # 连接仍可用则复用
            _conn.execute("SELECT 1")
            return _conn
        except Exception:
            _conn = None
    _conn = sqlite3.connect(DB, timeout=CONNECT_TIMEOUT)
    return _conn


def init_db():
    """初始化全部表。

    live_history 采用「追加写入」新结构：
      - id INTEGER PRIMARY KEY AUTOINCREMENT  自增主键，每条探测都新增一行
      - 去掉原表的 success_count / fail_count / consecutive_fails / enabled 列
        （这些统计量现在由 query_live_history() 实时聚合，而非冗余存储）
    这样 query_live_history(url) 才能返回该 URL 的最近 N 条真实历史记录，
    供 stability 评分使用。
    """
    DB.parent.mkdir(exist_ok=True)
    c = _connect()
    c.execute('''CREATE TABLE IF NOT EXISTS vod_history (
        url TEXT PRIMARY KEY, last_status TEXT, last_delay REAL, last_size INTEGER,
        success_count INTEGER DEFAULT 0, fail_count INTEGER DEFAULT 0, consecutive_fails INTEGER DEFAULT 0,
        last_checked TEXT, score REAL DEFAULT 50, enabled INTEGER DEFAULT 1)''')
    # 新结构：追加写入，自增 id 为主键
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
    # 把老库一次性迁移到新结构
    _migrate_live_history()


def _migrate_live_history():
    """一次性迁移：将旧的「覆盖更新」live_history 表迁移为新的「追加写入」结构。

    迁移策略：把原表每条记录的 url 只保留一条（取 last_checked 最新的），
    作为该 URL 的一条历史样本写入新表，其余字段原样保留。
    这会让 stability 统计从「单点」变为「每个 URL 至少 1 条历史」，
    跑满 stability_window 次 run 后即获得完整 N 条历史。
    """
    c = _connect()
    cols = c.execute("PRAGMA table_info(live_history)").fetchall()
    col_names = {r[1] for r in cols}
    c.close()
    # 判断是否需要迁移：新结构必有 'id' 列，老结构主键是 url 没有 id
    if 'id' in col_names:
        return
    # 老表，执行迁移
    c = _connect()
    c.execute("ALTER TABLE live_history RENAME TO live_history_old")
    c.execute('''CREATE TABLE live_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, url TEXT, channel_name TEXT,
        last_status TEXT, last_delay REAL, last_resolution INTEGER, last_speed REAL,
        last_score REAL, last_checked TEXT)''')
    c.execute('''CREATE INDEX IF NOT EXISTS idx_live_history_url_time
        ON live_history(url, last_checked DESC)''')
    # 每条 url 取最近一条记录迁入新表
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
    """追加写入一条探测记录（不复盖历史）。"""
    c = _connect()
    c.execute('''INSERT INTO live_history
        (url, channel_name, last_status, last_delay, last_resolution,
         last_speed, last_score, last_checked) VALUES (?,?,?,?,?,?,?,?)''',
        (url, channel, status, delay, resolution, speed, score, datetime.now().isoformat()))
    c.commit()
    c.close()


def record_vod_probe(url, status, delay=0, size=0):
    """vod_history 保持原覆盖更新语义（单 URL 单记录够用，无需追加）。"""
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
    """黑名单：基于最近 5 次连续失败判定，兼容新旧结构。

    新结构（追加写入）：统计该 URL 最近 5 条记录的连续失败次数。
    """
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
        # vod_history 仍用 enabled 列
        cutoff = (datetime.now() - timedelta(hours=24)).isoformat()
        c.execute(f"UPDATE {table} SET enabled=1 WHERE enabled=0 AND last_checked<?", (cutoff,))
        c.commit()
        bl = {r[0] for r in c.execute(f"SELECT url FROM {table} WHERE enabled=0").fetchall()}
    c.close()
    return bl


def query_live_history(url, limit=10):
    """按 last_checked 倒序取该 URL 最近 limit 条真实探测记录，用于稳定性统计。

    新结构下每次探测都是新增一行，因此这里返回的是该 URL 的最近 N 次实测结果。
    返回 [{'status': 'ok'|'fail'}, ...]，无记录返回 []。
    """
    c = _connect()
    rows = c.execute(
        "SELECT last_status FROM live_history "
        "WHERE url=? ORDER BY last_checked DESC LIMIT ?",
        (url, limit),
    ).fetchall()
    c.close()
    return [{'status': r[0]} for r in rows]


def get_stability(url, window=10):
    """实时聚合该 URL 最近 window 次探测的成功率，供稳定性评分使用。

    返回 float 0.0~1.0；无记录返回 None（调用方按"无历史"处理）。
    """
    history = query_live_history(url, limit=window)
    if not history:
        return None
    ok = sum(1 for h in history if h['status'] == 'ok')
    return ok / len(history)


def log_run(**kw):
    c = _connect()
    c.execute('''INSERT INTO run_log VALUES (NULL,?,?,?,?,?,?,?)''',
              (datetime.now().isoformat(), kw.get('vd', 0), kw.get('vo', 0), kw.get('ld', 0),
               kw.get('lc', 0), kw.get('ll', 0), kw.get('dur', 0)))
    c.commit()
    c.close()
