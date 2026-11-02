# 📺 TVBox 影视仓 · 全自动聚合项目

一个自动化聚合直播源、点播源、EPG 和台标的 TVBox / 影视仓接口生成项目。每日定时运行 GitHub Actions，无需自建服务器。

## ✨ 功能特性

| 特性 | 说明 |
|---|---|
| 🔄 多代理轮询 | 每个源自动尝试【直连 → jsDelivr 镜像 / GitHub 代理】，失败自动换下一个 |
| 🗄️ 本地缓存兜底 | 网络全失败时回退过期缓存，绝不返回空列表 |
| 🧹 脏数据过滤 | 自动剔除 IPv6 地址、`migutoken=` 签名线路、`.php` 中转页、多流行拼接 |
| 🏷️ tvg-id 归一化 | `CCTV-1` / `CCTV14.cn@SD` / `CCTV-4` 统一为 `CCTV14`，跨源去重 |
| 📡 直播源聚合 | 合并 Guovin、Lightconer、iptv-org 等多个源的可用线路 |
| 🎬 点播源聚合 | 自动发现 GitHub 上的 TVBox 接口，按大小排序择优 |
| 📅 EPG 自动同步 | 每日 02:30（UTC+8）从 Fanmingming 拉取节目单 |
| 📊 监控面板 | 内置 Flask Web UI，查看可用数量、缓存状态、运行历史 |

## 🗂️ 项目结构

```
.
├── aggregator.py        # 主聚合脚本（核心）
├── database.py          # SQLite 数据库：探测记录、黑名单、运行日志
├── web_ui.py            # 本地监控面板（Flask）
├── config.yaml          # 配置文件（源地址、代理池、缓存参数）
├── requirements.txt     # Python 依赖
├── .github/
│   └── workflows/
│       ├── daily-update.yml   # 每日聚合 workflow（02:30 UTC+8）
│       └── sync-epg.yml       # 每日 EPG 同步 workflow
├── output/              # 产物目录（提交到仓库，作为 jsDelivr 源）
│   ├── tvbox.json       # 影视仓接口文件
│   ├── live.m3u         # 直播源列表
│   ├── live.txt         # 直播源文本版
│   └── epg.xml         # 节目单
└── sources_cache/       # ⚠️ 直播源缓存（必须提交进仓库）
    └── *.m3u
```

## 🚀 快速开始

### 1. Fork & Clone

```bash
git clone https://github.com/你的用户名/ysc-self-host.git
cd ysc-self-host
```

### 2. 本地运行

```bash
pip install -r requirements.txt
python aggregator.py
```

首次运行会联网抓取源并写入 `sources_cache/`，之后命中缓存时零网络秒出。

### 3. 启动监控面板

```bash
python web_ui.py
# 浏览器打开 http://localhost:8080
```

面板展示：直播/点播可用数量、缓存文件状态、Guovin 源解析情况、最近运行记录。

## ⚙️ 配置说明（`config.yaml`）

```yaml
live:
  # 直播源地址（支持多个）
  urls:
    - "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.m3u"
    - "https://cdn.jsdelivr.net/gh/Lightconer/TVBox-Sources@main/output/live.txt"
    - "https://cdn.jsdelivr.net/gh/Lightconer/TVBox-Sources@main/output/live.m3u"
    - "https://iptv-org.github.io/iptv/categories/china.m3u"

  # GitHub 代理前缀池（依次尝试，挂了就换）
  proxies:
    - "https://gh-proxy.com/"
    - "https://ghproxy.cc/"
    - "https://gh.llkk.cc/"
    - "https://hub.fastgit.xyz/"
    - "https://gh.api.99988866.xyz/"

  # jsDelivr 镜像域名池
  jsdelivr_hosts:
    - "https://cdn.jsdelivr.net"
    - "https://fastly.jsdelivr.net"
    - "https://gcore.jsdelivr.net"
    - "https://testingcf.jsdelivr.net"

  # 缓存
  cache_dir: "sources_cache"     # 缓存目录
  cache_ttl_hours: 12            # 缓存有效期（小时）
  fetch_timeout: 30              # 单次拉取超时（秒）

  # 评分权重
  sort_weights:
    resolution: 0.40
    speed: 0.30
    delay: 0.20
    stability: 0.10
  min_resolution: 720            # 最低分辨率
  urls_per_channel: 6            # 每个频道保留的线路数

vod:
  max_sources: 1500
  timeout: 12
  concurrency: 15
  min_json_size: 200
  discover_per_page: 20

logo:
  base_url: "https://live.fanmingming.com/tv"
```

### 换代理

某个代理挂了，直接在 `proxies` 列表里删掉或替换，**不用改代码**。脚本会自动跳过失效代理尝试下一个。

## 🌐 多代理轮询原理

每个源 URL 按域名自动生成多种访问方式，直连优先，失败逐级降级：

| 源域名 | 生成方式数 |
|---|---|
| `raw.githubusercontent.com` | **6 种**（直连 + 5 个 GitHub 代理） |
| `cdn.jsdelivr.net/gh/...` | **4 种**（直连 + 3 个 jsDelivr 镜像） |
| 其他（如 iptv-org） | 1 种（不套代理） |

日志示例：

```
📡 待抓取直播源 4 个
  ✓ result.m3u → 直连 成功 (323437 字节)
  ✗ live.m3u 直连失败，尝试备用方式...
  ✓ live.m3u → 备用1 成功 (51203 字节)
```

看到 `备用N` 说明直连不通走了代理，此时应考虑换源或让缓存兜底。

## 📦 Guovin 源

Guovin（`Guovin/iptv-api`）是本项目最主要的频道来源，特点是：

- **自动测速排序**：工作流会把延迟低的线路排前面，所以每个频道只取前 3~6 路即可
- **规范 ID**：`tvg-id` 为 `CCTV-1` 格式，`group-title` 带 emoji（如 `📺央视频道`）
- **唯一可用地址**：实测 jsDelivr 和多个 GitHub 代理对该源返回 403，只有 `raw.githubusercontent.com` 直连可用

脚本已自动做归一化：`CCTV-1` → `CCTV1`，`📺央视频道` → `央视频道`，与自有源的频道能正确合并去重。

## ⚠️ 缓存目录必须提交进仓库

`aggregator.py` 的缓存机制依赖 `sources_cache/` 目录。**这个目录必须提交进仓库**，否则 GitHub Actions 每次全新环境都读不到缓存，缓存兜底形同虚设。

```bash
# 首次运行生成缓存后提交
git add output/ sources_cache/
git commit -m "chore: add sources cache"
git push
```

> **为什么目录名叫 `sources_cache` 而不是 `cache`？**
> 仓库的 `.gitignore` 里忽略了 `cache/`。如果缓存目录叫 `cache`，它会被 git 忽略，CI 里永远读不到——所以特意取名 `sources_cache`。

提交后，即使某次 Actions 拉不到任何源，也能从仓库里的过期缓存产出结果，彻底摆脱外网依赖。

## 🔧 GitHub Actions

### 每日聚合（02:30 UTC+8）

`.github/workflows/daily-update.yml` 每日自动运行：

- 抓取所有直播/点播源（多代理轮询）
- 测速、评分、去重
- 生成 `output/tvbox.json`、`output/live.m3u` 等
- 更新 `sources_cache/` 并自动提交回仓库
- 支持 `workflow_dispatch` 手动触发

### EPG 同步

`.github/workflows/sync-epg.yml` 每日拉取节目单，同样支持手动触发。

### 手动触发

在 GitHub 仓库页面 **Actions → 选择 workflow → Run workflow** 即可立即执行，无需等待定时。

## 📱 影视仓配置

在影视仓 App 中添加接口：

```
https://cdn.jsdelivr.net/gh/你的用户名/ysc-self-host@main/output/tvbox.json
```

直播源地址：

```
https://cdn.jsdelivr.net/gh/你的用户名/ysc-self-host@main/output/live.m3u
```

EPG 地址：

```
https://cdn.jsdelivr.net/gh/你的用户名/ysc-self-host@main/output/epg.xml
```

> 影视仓设置里的"接口超时"建议设为 15 秒以上，避免大文件加载被中断。

## 📊 数据库与监控

`database.py` 维护三张表：

| 表名 | 用途 |
|---|---|
| `live_history` | 直播源探测记录（状态、延迟、分辨率、评分） |
| `vod_history` | 点播源探测记录 |
| `run_log` | 每次聚合运行的统计日志 |

监控面板的"可用数量"按 `last_status='ok'` 统计，反映真实可用情况，而非按"是否被禁用"统计。

## ❓ 常见问题

**Q：接口添加失败，提示"接口无效"？**
A：先确认网络能访问 jsDelivr；再检查 `tvbox.json` 根节点必须是 JSON 对象 `{}`；影视仓接口超时调大到 15 秒以上。

**Q：直播源很少 / 为空？**
A：检查 `sources_cache/` 是否提交；查看 Actions 日志里哪些源走了 `备用N` 方式，可能需要换代理或换源。

**Q：Guovin 源拉不到？**
A：该源只有 raw 直连可用。如果 raw 也超时，说明当前网络访问 GitHub 受限，此时会回退到过期缓存（前提是已提交过）。

## 📄 License

MIT
