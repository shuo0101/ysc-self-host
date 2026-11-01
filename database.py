import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

DB = Path("data/sources.db")

def init_db():
    DB.parent.mkdir(exist_ok=True)
    c = sqlite3.connect(DB)
    c.execute('''CREATE TABLE IF NOT EXISTS vod_history (
        url TEXT PRIMARY KEY, last_status TEXT, last_delay REAL, last_size INTEGER,
        success_count INTEGER DEFAULT 0, fail_count INTEGER DEFAULT 0, consecutive_fails INTEGER DEFAULT 0,
        last_checked TEXT, score REAL DEFAULT 50, enabled INTEGER DEFAULT 1)''')
    c.execute('''CREATE TABLE IF NOT EXISTS live_history (
        url TEXT PRIMARY KEY, channel_name TEXT, last_status TEXT, last_delay REAL,
        last_resolution INTEGER, last_score REAL, success_count INTEGER DEFAULT 0,
        fail_count INTEGER DEFAULT 0, consecutive_fails INTEGER DEFAULT 0, last_checked TEXT, enabled INTEGER DEFAULT 1)''')
    c.execute('''CREATE TABLE IF NOT EXISTS run_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, run_time TEXT, vod_discovered INTEGER,
        vod_output INTEGER, live_discovered INTEGER, live_channels INTEGER, live_lines INTEGER, duration REAL)''')
    c.commit(); c.close()

def _upd(t, url, st, **kw):
    c = sqlite3.connect(DB)
    row = c.execute(f"SELECT success_count,fail_count,consecutive_fails FROM {t} WHERE url=?",(url,)).fetchone()
    # 修复：row 为 None 时（新记录）给默认值
    if row is None:
        sc, fc, cf = (1 if st=='ok' else 0, 0 if st=='ok' else 1, 0 if st=='ok' else 1)
    else:
        sc = row[0] + (1 if st=='ok' else 0)
        fc = row[1] + (0 if st=='ok' else 1)
        cf = 0 if st=='ok' else (row[2] + 1)
    en = 0 if cf >= 5 else 1
    cols = ['url','last_status','success_count','fail_count','consecutive_fails','last_checked','enabled'] + list(kw.keys())
    vals = [url,st,sc,fc,cf,datetime.now().isoformat(),en] + list(kw.values())
    c.execute(f"INSERT OR REPLACE INTO {t} ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})", vals)
    c.commit(); c.close()

def record_vod_probe(url, status, delay=0, size=0):
    _upd('vod_history', url, status, last_delay=delay, last_size=size)

def record_live_probe(url, channel, status, delay=0, resolution=0, speed=0, score=0):
    _upd('live_history', url, status, channel_name=channel, last_delay=delay, last_resolution=resolution, last_speed=speed, last_score=score)

def get_blacklist(table):
    c = sqlite3.connect(DB)
    cutoff = (datetime.now()-timedelta(hours=24)).isoformat()
    c.execute(f"UPDATE {table} SET enabled=1 WHERE enabled=0 AND last_checked<?",(cutoff,))
    c.commit()
    bl = {r[0] for r in c.execute(f"SELECT url FROM {table} WHERE enabled=0").fetchall()}
    c.close(); return bl

def log_run(**kw):
    c = sqlite3.connect(DB)
    c.execute('''INSERT INTO run_log VALUES (NULL,?,?,?,?,?,?,?)''',
              (datetime.now().isoformat(), kw.get('vd',0), kw.get('vo',0), kw.get('ld',0), kw.get('lc',0), kw.get('ll',0), kw.get('dur',0)))
    c.commit(); c.close()