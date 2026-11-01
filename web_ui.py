from flask import Flask, jsonify
import sqlite3
from pathlib import Path
from database import init_db

app = Flask(__name__)
DB = Path("data/sources.db")

@app.route('/')
def index():
    return '<h1 style="color:white;background:#0f172a;padding:20px">📺 TVBox 源状态面板</h1><p>访问 /api/status</p>'

@app.route('/api/status')
def status():
    c = sqlite3.connect(DB)
    v = c.execute("SELECT COUNT(*),COUNT(CASE WHEN enabled=1 THEN 1 END) FROM vod_history").fetchone()
    l = c.execute("SELECT COUNT(DISTINCT channel_name),COUNT(CASE WHEN enabled=1 THEN 1 END) FROM live_history").fetchone()
    c.close()
    return jsonify({'vod_total':v[0],'vod_ok':v[1],'live_ch':l[0],'live_ok':l[1]})

if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=8080)