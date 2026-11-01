# 📺 TVBox 影视仓全自动聚合源

> 点播 + 直播全自动发现 · 4K 优先 · 清晰度/速度/稳定度智能排序 · SQLite 历史降权 · EPG 节目单 · 台标自动匹配 · GitHub Actions 每日自动更新

[![Update](https://github.com/你的用户名/tvbox-auto-sources/actions/workflows/daily-update.yml/badge.svg)](https://github.com/你的用户名/tvbox-auto-sources/actions)
[![EPG](https://github.com/你的用户名/tvbox-auto-sources/actions/workflows/sync-epg.yml/badge.svg)](https://github.com/你的用户名/tvbox-auto-sources/actions)

---

## ✨ 功能一览

| 模块 | 说明 |
|------|------|
| 🔍 自动发现 | GitHub API + 种子索引自动发现候选源，无需手动维护 |
| 📊 智能排序 | 点播按内容丰富度+响应速度；直播 ffprobe 真探流，清晰度40%+速度30%+延迟20%+稳定10% |
| 🛡️ 稳定性追踪 | SQLite 记录历史，连续失败3次降权、5次禁用24h后自动重试 |
| 📺 4K 优先 | 2160 > 1080 > 720，4K 源自动排最前 |
| 🔢 数量控制 | 点播 ≤1500 条，直播频道 ≤1500 个，每频道保留 Top 6 线路 |
| 📅 EPG 节目单 | 每天自动同步 3 个 EPG 源到仓库，影视仓直接显示当前/下一档节目 |
| 🖼️ 台标匹配 | 自动清洗频道名，匹配 fanmingming 开源台标库，M3U 带 `tvg-logo` |
| ⏰ 自动更新 | 源聚合每天北京 8:00 + 20:00；EPG 每天北京 10:30 |
| 🌐 CDN 分发 | jsDelivr 加速，国内访问流畅 |

---

## 🚀 快速使用（30 秒）

### 影视仓 / TVBox / OK影视 / TiviMate

**只需填一个地址：**
https://cdn.jsdelivr.net/gh/你的用户名/tvbox-auto-sources@main/output/tvbox.json
这一个地址包含：点播 + 直播 + EPG + 台标，全齐。

> ⚠️ 把 `你的用户名` 替换成你的 GitHub 用户名

### 配置路径

| 播放器 | 填写位置 |
|--------|---------|
| 影视仓 | 设置 → 配置/仓库管理 → 填入上面的地址 |
| TVBox | 设置 → 配置地址 → 填入 |
| OK影视 | 设置 → 订阅管理 → 添加 |
| DIYP/TiviMate | 直播源填 `live.m3u` 地址（见下方） |

### 分开填（可选）

如果你只想用直播或想单独指定 EPG：
点播：https://cdn.jsdelivr.net/gh/你的用户名/tvbox-auto-sources@main/output/tvbox.json
直播：https://cdn.jsdelivr.net/gh/你的用户名/tvbox-auto-sources@main/output/live.m3u
EPG ：https://cdn.jsdelivr.net/gh/你的用户名/tvbox-auto-sources@main/output/epg.xml
纯文本
---

## 📦 自行部署

### 1. Fork 仓库

点击右上角 **Fork**，fork 到你自己的 GitHub 账号。

### 2. 开启 Actions

- **Settings** → **Actions** → **General** → **Workflow permissions** → 选 **Read and write permissions**
- 进入 **Actions** 标签页 → 点击 **I understand my workflows, go ahead and enable them**

### 3. 手动触发测试

- **Actions** → **Daily TVBox Sources Update** → **Run workflow**
- **Actions** → **Sync EPG** → **Run workflow**
- 等待绿色 ✓，`output/` 下自动生成所有文件

### 4. 填地址

把 `你的用户名` 替换后填入影视仓即可。

---

## 📁 仓库结构
├── .github/workflows/
│ ├── daily-update.yml # 源聚合（每天2次）
│ └── sync-epg.yml # EPG同步（每天1次）
├── aggregator.py # 主聚合脚本（含EPG+台标）
├── database.py # SQLite历史追踪
├── web_ui.py # Web状态面板
├── config.yaml # 排序权重/阈值配置
├── requirements.txt # Python依赖
├── README.md
├── .gitignore
├── output/ # 聚合输出（自动生成）
│ ├── tvbox.json # 点播+直播+EPG（主入口）
│ ├── live.txt # 纯直播 TXT
│ ├── live.m3u # 带台标+EPG头的 M3U
│ ├── live_config.json # 直播仓库配置
│ ├── epg.xml # 主 EPG（自缓存）
│ ├── epg_fanmingming.xml # 备用 EPG
│ ├── epg_112114.xml # 备用 EPG 2
│ └── epg_index.json # EPG 索引
└── data/
└── sources.db # 历史探测数据库
纯文本
---

## 🖥️ 本地运行
bash
安装依赖
pip install -r requirements.txt
sudo apt-get install ffmpeg # 直播探流需要
初始化数据库
python -c 'from database import init_db; init_db()'
跑一次聚合
python aggregator.py
启动 Web 状态面板
python web_ui.py
浏览器打开 http://localhost:8080
纯文本
Web UI 可查看：
- 点播/直播可用数量、均分
- 每个源的分数、延迟、成功/失败次数
- 运行历史

---

## ⚙️ 配置说明

编辑 `config.yaml`：
yaml
vod:
timeout: 12 # 点播探测超时(秒)
concurrency: 15 # 并发数
max_sources: 1500 # 点播源上限
min_json_size: 200 # 最小 JSON 字节数
live:
timeout: 8 # 直播 HTTP 超时
probe_timeout: 6 # ffprobe 超时
concurrency: 10 # 并发数
min_resolution: 720 # 最低分辨率
max_resolution: 2160 # 最高(4K)
urls_per_channel: 6 # 每频道保留线路数
sort_weights: # 排序权重
resolution: 0.40 # 清晰度 40%
speed: 0.30 # 速度 30%
delay: 0.20 # 延迟 20%
stability: 0.10 # 稳定性 10%
纯文本
---

## 📅 EPG 节目单

### 数据源

| 优先级 | 地址 | 说明 |
|--------|------|------|
| 主 | 自己仓库缓存 `output/epg.xml` | 每天自动同步，最稳定 |
| 备1 | `https://live.fanmingming.com/e.xml` | fanmingming |
| 备2 | `https://epg.112114.xyz/pp.xml` | 112114 |

### 查看 EPG 索引
https://cdn.jsdelivr.net/gh/你的用户名/tvbox-auto-sources@main/output/epg_index.json
纯文本
### 手动更新 EPG

Actions → **Sync EPG** → **Run workflow**

---

## 🖼️ 台标

- 台标库：`https://live.fanmingming.com/tv/{频道名}.png`
- 自动匹配规则：
  - `CCTV1综合` → `CCTV1.png`
  - `湖南卫视高清` → `湖南.png`
  - `北京卫视` → `北京.png`
- 台标不显示不影响播放，自动回退空白

---

## 🛡️ 稳定性机制
连续失败 1~2 次 → 降权（分数降低）
连续失败 3 次 → 大幅降权
连续失败 5 次 → 禁用 24 小时
24 小时后 → 自动重新探测
纯文本
历史数据持久化在 `data/sources.db`。

---

## 🔄 更新频率

| 工作流 | 频率 | 北京时间 |
|--------|------|---------|
| daily-update | 每天2次 | 8:00 + 20:00 |
| sync-epg | 每天1次 | 10:30 |

修改 `.github/workflows/daily-update.yml` 的 `cron:` 调整频率（UTC 时间 = 北京 - 8）。

---

## 🔗 容灾链路
主 EPG（自己仓库缓存）→ 备用 fanmingming → 备用 112114
台标挂了 → 空白图标，不影响播放
直播源挂了 → 同频道下一条线路自动顶上
点播源挂了 → 历史降权，下次排后面
纯文本
---

## ❓ 常见问题

**Q: 影视仓加载不出来？**
- 确认 `你的用户名` 已替换
- jsDelivr CDN 有 2~5 分钟缓存，刚更新完稍等
- 加 `?v=1` 强制刷新：`...tvbox.json?v=1`

**Q: 没有 4K 源？**
- 4K 需要源站本身有 4K 资源，脚本只负责排序
- 确保网络支持 IPv6（很多 4K 直播走 v6）
- 播放器设置里选 EXO 或 IJK

**Q: 台标不显示？**
- M3U 里 `tvg-logo` 必须是完整 http(s) 图片地址
- 频道名和台标文件名要对得上
- 影视仓：设置 → 直播 → 开启「显示台标」

**Q: EPG 不显示节目单？**
- 确认 `tvg-id` 和 EPG XML 里的频道 ID 匹配
- 部分小频道没有 EPG 数据，属正常
- 手动触发 Sync EPG 再试

**Q: Actions 跑失败了？**
- 进 Actions → 点失败的 run → 查看日志
- GitHub API 限流时有兜底，下次自动恢复
- 手动再 Run 一次即可

**Q: 能商用吗？**
- 本工具仅用于学习交流，请勿用于商业用途
- 源版权归各自所有者，请遵守相关法律法规

---

## 📜 开源协议

MIT License © 2024

---

> 🔗 **原理**：脚本每天自动从全网发现源 → 探测可用性 → ffprobe 检测直播流清晰度 → 综合打分排序 → 匹配台标 → 输出订阅文件 → jsDelivr CDN 分发 → 影视仓填一个固定地址，永久自动更新，带节目单和台标