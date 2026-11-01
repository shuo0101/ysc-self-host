# 影视仓自建源 (TVBox Self-Host)

让影视仓/影视仓系 App 的**点播源 + 直播源自动更新**的零成本方案。

## 原理

```
影视仓 App ──自动拉取──▶ GitHub 仓库 (本仓库)
                           │
                           ├── output/tvbox.json  (单仓聚合, 点播)
                           ├── output/urls.json    (多仓订阅, 点播)
                           └── output/live.m3u     (直播源, 每日刷新)
```

GitHub Actions 每天北京时间 **06:00 / 18:00** 自动抓取多个上游源, 合并去重、测速筛选, 只保留可用的。

## 快速开始

### 1. Fork 本仓库

点右上角 Fork, 然后 **Settings → Actions → General → Workflow permissions → 选 `Read and write permissions` → Save**。
否则工作流没权限把结果写回仓库。

### 2. 编辑 `config/sources.json`

只需要改这个文件:

| 字段 | 说明 |
|---|---|
| `base_url` | 你仓库 raw 地址(套上国内加速前缀), 用于回写进 lives |
| `vod_sources` | 点播源列表, 每个是 `{name, url}` |
| `live_sources` | 直播源列表, 直接填 m3u/txt 地址 |
| `spider` | 可选, 自定义 jar 包地址 |

### 3. 开启 Actions

进入仓库 **Actions** 标签 → 开启工作流 → **Run workflow** 手动跑一次。
跑完后 `output/` 目录会生成 3 个文件。

### 4. 套国内加速前缀

把 raw 地址拼在加速站后面:

```
https://gh-proxy.com/https://raw.githubusercontent.com/你的用户名/tvbox-self-host/main/output/tvbox.json
```

> 仓库必须 **Public**, 否则影视仓无法免鉴权拉取。不要放任何私人信息。

## 在影视仓里填地址

| 入口 | 地址 |
|---|---|
| 配置地址 (点播) | `.../output/tvbox.json` |
| 直播订阅 (直播) | `.../output/live.m3u` |

**注意**: 直播 m3u 不要填进"配置地址", 否则会提示解析失败。

## 目录结构

```
config/sources.json          # 你只需要改这个
scripts/vod_update.py        # 点播源聚合
scripts/live_update.py       # 直播源抓取+测速+去重, 4K优先排序
.github/workflows/update.yml # 每日自动更新
output/                      # 产物(自动生成, 不要手改)
```

## 关于 4K

- 直播源按 **分辨率关键词** 排序, 4K/2160 频道排在最前
- 真 4K 直播源稀少, 标着 4K 的很多实际是 1080P, 以实际播放分辨率为准
- 点播 4K 取决于 spider 里有没有高码率线路

## 免责声明

本仓库仅提供**技术框架**, 不存储、不提供任何影视内容。源地址由用户自行配置与维护, 请确保使用合法授权的资源。
