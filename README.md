# 影视仓自建源

TVBox / 影视仓的**源自动更新仓库**：点播源聚合去重 + 直播源每日探测刷新，输出带台标、EPG、分组的 m3u。

## 目录结构

```
config/sources.json            ← 你唯一需要改的文件
scripts/
  ├── live_update.py           直播源抓取+探测测速+4K权重排序+分组+台标
  ├── vod_update.py            点播源聚合去重
  └── test_local.py            本地测试（不联网）
.github/workflows/update.yml   每天北京时间 06:00/18:00 自动跑
output/                        自动生成，别手改
  ├── tvbox.json               多仓聚合配置
  ├── tvbox_single.json        精简单仓（站点上限60，加载快）
  ├── live.m3u                 直播源（带台标+EPG+分组）
  └── live.txt                 直播源（简洁格式）
```

## 7 步搭建

### 1. 新建仓库
GitHub → New repository → 起名（如 `ysc-self-host`）→ **Public** → 不勾选任何初始化选项。

### 2. 上传文件
把本仓库内容（不含外层文件夹）拖到仓库上传页面 → Commit message 写 `init` → Commit。

### 3. 开写入权限（必做）
Settings → Actions → General → Workflow permissions → **Read and write permissions** → Save。

### 4. 改 `config/sources.json`
只改 `base_url` 这一行，换成你的实际地址：

```json
"base_url": "https://mirror.ghproxy.com/https://raw.githubusercontent.com/你的用户名/你的仓库名/main"
```

源地址已经预填了 20 个点播源 + 12 个直播源，全部套了加速前缀，国内可直接访问。

### 5. 跑工作流
Actions → 开启工作流 → 左侧「每日更新源」→ Run workflow → 等 1~2 分钟变绿。
仓库根目录会出现 `output/` 文件夹。

### 6. 拿地址
点开 `output/tvbox.json` → Raw → 复制链接 → 前面加加速前缀：

```
https://mirror.ghproxy.com/https://raw.githubusercontent.com/你的用户名/你的仓库名/main/output/tvbox.json
https://mirror.ghproxy.com/https://raw.githubusercontent.com/你的用户名/你的仓库名/main/output/live.m3u
```

### 7. 填进影视仓
- **配置地址**（点播）：填 `tvbox.json` 地址
- **直播订阅**（直播）：填 `live.m3u` 地址

> 直播 m3u **别填进配置地址**，会报解析失败。

## 以后改源

只改 `config/sources.json`：
- **加源**：在 `vod_sources` / `live_sources` 数组里加一个对象
- **删失效源**：删掉那一行
- **改地址**：只改 `url` 的值

改完手动 Run workflow 一次，几十秒后影视仓下次打开自动拿到新的。

## 本地测试

```bash
python scripts/test_local.py
```

验证分组、台标、清晰度权重、EPG 等逻辑，不联网。

## 注意事项

- 仓库必须 **Public**
- 别在仓库里放任何私人信息
- 本仓库只提供技术框架，源由你自行配置维护，注意合规
- 源地址会过期，失效了在 `sources.json` 里删掉那一行即可
