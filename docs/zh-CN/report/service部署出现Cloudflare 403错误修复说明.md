# service部署出现Cloudflare 403错误修复说明

这个方案用于下面这种场景：

- CodexManager 的 service 部署在服务器上运行
- service 访问 `https://chatgpt.com` 时出现 Cloudflare `403`、`cf-mitigated: challenge`，或者云服务器出口 IP 被风控 / 类似“被 ban”的现象
- `curl_cffi` 直连或走 WARP 代理访问 `https://chatgpt.com` 可以通过，但 CodexManager 自身通过 `reqwest/rustls` 访问上游时仍然失败

核心思路：

1. 在本机启动一个轻量 HTTP 反向代理
2. 这个代理内部使用 `curl_cffi` + 浏览器指纹 `impersonate`
3. CodexManager service 不再直接访问 `chatgpt.com`，而是把上游地址改为本机代理

---

## 1. 一键准备 Cloudflare / WARP 本地代理

如果你就是遇到 Oracle / 其他云服务器部署 service 后，直连 `chatgpt.com` 被 Cloudflare `403` 或 `challenge` 拦截的那类场景，仓库现在提供了一个一键准备脚本：

```bash
scripts/setup-cloudflare-warp-proxy.sh --port 40000
```

这个脚本会：

- 安装 `cloudflare-warp`
- 自动尝试注册 WARP 设备
- 切到本地代理模式，而不是全局接管模式
- 把监听端口设置为 `127.0.0.1:40000`
- 连接 WARP，并打印下一步启动 `curl_cffi` 代理和 `codexmanager.env` 的建议配置

说明：

- 它面向 Ubuntu / Debian 系机器
- 默认不会改成全局隧道模式，因此不会像全局 WARP 那样更容易把 SSH 链路搞断
- 脚本同时兼容较新的 `warp-cli mode proxy / proxy port` 和较老的 `set-mode / set-proxy-port` 命令风格

如果你已经自己装好了 WARP，也可以跳过安装：

```bash
scripts/setup-cloudflare-warp-proxy.sh --skip-install --port 40000
```

---

## 2. 安装依赖

先安装 Python 依赖：

```bash
python3 -m pip install --upgrade curl_cffi
```

如果你的系统没有 `pip`：

```bash
sudo apt-get update
sudo apt-get install -y python3-pip
python3 -m pip install --upgrade curl_cffi
```

---

## 3. 启动本地代理

仓库也提供了一个更短的启动脚本：

```bash
scripts/run-curl-cffi-chatgpt-proxy.sh \
  --proxy socks5h://127.0.0.1:40000 \
  --install-deps \
  --verbose
```

它会：

- 检查并按需安装 `curl_cffi`
- 用仓库内的 `curl_cffi_chatgpt_proxy.py` 启动本地代理
- 打印建议写入 `codexmanager.env` 的配置片段

如果你已经装好了依赖，也可以继续直接运行 Python 脚本：

仓库内已提供脚本：

```bash
python3 scripts/curl_cffi_chatgpt_proxy.py \
  --listen 127.0.0.1:8787 \
  --proxy socks5h://127.0.0.1:40000
```

常见参数：

- `--listen 127.0.0.1:8787`
  本地监听地址
- `--proxy socks5h://127.0.0.1:40000`
  让 `curl_cffi` 再通过 WARP / Clash / sing-box 出口访问上游
- `--upstream-origin https://chatgpt.com`
  默认就是这个，一般不用改
- `--impersonate chrome124`
  浏览器指纹，默认已经内置

启动成功后会输出类似：

```text
curl_cffi proxy listening on http://127.0.0.1:8787 (upstream_origin=https://chatgpt.com, proxy=socks5h://127.0.0.1:40000, impersonate=chrome124)
point CodexManager at: CODEXMANAGER_UPSTREAM_BASE_URL=http://127.0.0.1:8787/backend-api/codex
```

---

## 4. 先单独验证代理

先不要改 CodexManager，直接验证这个代理本身是否能通：

```bash
curl -i http://127.0.0.1:8787/__proxy_health
```

再验证它是否真的能走到 `chatgpt.com`：

```bash
curl -i http://127.0.0.1:8787/
```

如果你的出口环境适合，返回不应再是 `cf-mitigated: challenge` 或 Cloudflare `403` 页面。

---

## 5. 修改 CodexManager 配置

把 `codexmanager.env` 改成这样：

```dotenv
CODEXMANAGER_SERVICE_ADDR=0.0.0.0:5010
CODEXMANAGER_WEB_ADDR=0.0.0.0:5011
CODEXMANAGER_DB_PATH=./data/codexmanager.db
CODEXMANAGER_RPC_TOKEN_FILE=./data/codexmanager.rpc-token
CODEXMANAGER_WEB_NO_OPEN=1

# 关键：让 CodexManager 把上游改到本地 curl_cffi 代理
CODEXMANAGER_UPSTREAM_BASE_URL=http://127.0.0.1:8787/backend-api/codex

```

建议把这项清掉，避免双重代理：

```dotenv
# 不要再保留旧的 reqwest 出口代理
# CODEXMANAGER_UPSTREAM_PROXY_URL=socks5h://127.0.0.1:40000
```

原因：

- 现在真正访问上游的是 `curl_cffi_chatgpt_proxy.py`
- CodexManager 只需要访问本机 `127.0.0.1:8787`
- 如果 CodexManager 自己也再走一层 SOCKS 代理，容易把“本地代理请求”又绕回去，增加排障难度

改完后重启：

```bash
./codexmanager-start
```

---

## 6. 验证 CodexManager 是否接到了本地代理

重启后先请求模型列表：

```bash
curl http://127.0.0.1:5010/v1/models \
  -H 'Authorization: Bearer <你的平台APIKey>'
```

如果成功，再测具体模型：

```bash
curl http://127.0.0.1:5010/v1/responses \
  -H 'Authorization: Bearer <你的平台APIKey>' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "gpt-5.4",
    "input": "你好"
  }'
```

同时观察 `curl_cffi_chatgpt_proxy.py` 的终端输出。

如果代理脚本有请求日志，而 CodexManager 不再报 `Cloudflare 安全验证页`，说明链路已经切换成功。

---

## 7. 常见问题

### 7.1 `curl_cffi` 能访问 `chatgpt.com`，但 CodexManager 还是 challenge

优先确认两件事：

1. `CODEXMANAGER_UPSTREAM_BASE_URL` 是否真的改成了 `http://127.0.0.1:8787/backend-api/codex`
2. `CODEXMANAGER_UPSTREAM_PROXY_URL` 是否已经清掉，避免 CodexManager 仍然走旧链路

### 7.2 代理脚本启动了，但请求没有打过来

说明 CodexManager 还没切到本地代理。重点检查：

- `codexmanager.env` 是否被当前进程读取
- 是否完整重启了 `codexmanager-start`
- 平台 Key 是否单独配置了自己的 `upstreamBaseUrl`

### 7.3 想让脚本自己走 WARP / Clash / sing-box

直接使用：

```bash
python3 scripts/curl_cffi_chatgpt_proxy.py \
  --listen 127.0.0.1:8787 \
  --proxy socks5h://127.0.0.1:40000
```

如果你使用的是 HTTP 代理，也可以写：

```bash
python3 scripts/curl_cffi_chatgpt_proxy.py \
  --listen 127.0.0.1:8787 \
  --proxy http://127.0.0.1:7890
```

---

### 7.4 service 进程里总是又带回旧的 `CODEXMANAGER_UPSTREAM_PROXY_URL`

如果你明明已经从 `codexmanager.env` 里删掉了 `CODEXMANAGER_UPSTREAM_PROXY_URL`，但进程环境里仍然出现它，通常是数据库里的持久化设置又把它同步回来了。

可以先检查：

```bash
sqlite3 ./data/codexmanager.db "select key, value from app_settings where key='gateway.upstream_proxy_url';"
```

如果查到了旧值，就删掉：

```bash
sqlite3 ./data/codexmanager.db "delete from app_settings where key='gateway.upstream_proxy_url';"
```

然后完整重启 `codexmanager-start`。

---

## 8. 适用边界

这个脚本当前主要面向：

- `/backend-api/codex/models`
- `/backend-api/codex/responses`
- `/backend-api/codex/chat/completions`

它按原始路径做通用转发，因此通常不需要针对单个接口再单独适配。

如果后续你需要更强的能力，例如：

- 更细的请求头改写
- WebSocket 特殊处理
- 单独的重试/日志脱敏

可以继续在 `scripts/curl_cffi_chatgpt_proxy.py` 基础上扩展。
