<p align="center">
  <img src="assets/logo/logo.png" alt="CodexManager Logo" width="220" />
</p>

<h1 align="center">CodexManager</h1>

<p align="center">本地桌面端 + 服务进程的 Codex 账号管理器+网关转发</p>

<p align="center">
  <a href="README.en.md">English</a>|
  <a href="#赞助支持">赞助支持</a>
</p>



本地桌面端 + 服务进程的 Codex 账号池管理器，用于统一管理账号、用量与平台 Key，并提供本地网关能力。

## 源码说明：
> 本产品完全由本人指挥+AI打造 Codex（98%） Gemini (2%) 如果在使用过程中产生问题请友好交流，因为开源只是觉得有人能用的上，基本功能也没什么问题，不喜勿喷。
> 其次是本人没有足够的环境来验证每个包都有没有问题，本人也要上班(我只是个穷逼买不起mac之类的)，本人只保证win的桌面端的可用性，如果其他端有问题，请在交流群反馈或者在充分测试后提交Issues，有时间我自会处理
> 最后感谢各位使用者在交流群反馈的各个平台的问题和参与的部分测试。


## 免责声明

- 本项目仅用于学习与开发目的。

- 使用者必须遵守相关平台的服务条款（例如 OpenAI、Anthropic）。

- 作者不提供或分发任何账号、API Key 或代理服务，也不对本软件的具体使用方式负责。

- 请勿使用本项目绕过速率限制或服务限制。

## 首页导览
| 你要做什么 | 直接进入 |
| --- | --- |
| 首次启动、部署、Docker、macOS 放行 | [运行与部署指南](docs/report/20260310122606850_运行与部署指南.md) |
| 配置端口、代理、数据库、Web 密码、环境变量 | [环境变量与运行配置](docs/report/20260309195355187_环境变量与运行配置说明.md) |
| 排查账号不命中、导入失败、挑战拦截、请求异常 | [FAQ 与账号命中规则](docs/report/20260310122606852_FAQ与账号命中规则.md) |
| 本地构建、打包、发版、脚本调用 | [构建发布与脚本说明](docs/release/20260310122606851_构建发布与脚本说明.md) |

## 最近变更
- 当前最新版本：`v0.1.12`（2026-03-20）
- `v0.1.12` 继续补齐这一轮桌面与文档收口：平台密钥名称更新链路现已在 Web / 桌面端统一打通，密钥 ID 默认完整显示，README 也补上了赞助支持入口与跳转。
- 账号管理补齐了这一轮最常用的治理能力：`account_deactivated` 与 `workspace_deactivated` 会被自动识别为不可用信号，页面支持直接筛选“封禁”，并提供“一键清理封禁账号”入口。
- 账号页的 5 小时 / 7 天额度现在都会在进度条下方显示重置时间；仅提供 7 天窗口的 free 账号也会把重置时间正确显示到 7 天列，避免看错窗口。
- 平台密钥新增服务等级配置：`跟随请求`、`Fast`、`Flex`。其中 `Fast` 会映射为上游 `priority`，`Flex` 会直传为 `flex`；桌面端创建 / 编辑链路也已修正，现在能正常保存与回显。
- 设置页补回了服务监听切换，支持在 `localhost` 与 `0.0.0.0` 之间切换；“检查更新”按钮现在只会在手动点击时显示加载状态，不会再被静默自动检查误触发。
- Web / 桌面交互层也做了补丁修复：Web 非首页刷新不再误下载文件，复制 API Key / 登录链接在缺少 `navigator.clipboard.writeText` 的环境下也会自动降级复制。
- 发布链路继续统一收口：版本已提升到 `0.1.12`，workspace、前端包、Tauri 桌面端、版本一致性校验脚本和 README 版本说明已同步对齐。完整历史请看 [CHANGELOG.md](CHANGELOG.md)。

### 近期提交摘要
- `cb990a1`：完善账号清理入口与文档收口。账号操作菜单补齐封禁清理入口与数量展示，README / 文档入口同步收紧到当前主线路径。
- `42219c7`：增加封禁筛选并修复平台密钥配置展示。账号页支持封禁筛选与状态原因透传，平台密钥桌面端保存 / 回显链路同步修正。
- `07dffc0`：增加平台密钥服务等级配置。平台密钥支持 `跟随请求 / Fast / Flex` 三种服务等级，并接入实际请求改写。
- `feb759b`：修复设置页监听地址与更新按钮状态。补回 `localhost / 0.0.0.0` 切换入口，并修正自动检查更新误触发按钮 loading 的问题。
- `50d6a03`：修复 Web 刷新下载与剪贴板复制失败。静态路由尾斜杠行为已收口，剪贴板在不支持原生 API 时也会自动降级。
- `e3a7557`：移除 upstream cookie 链路。当前主路径不再依赖全局上游 Cookie，出站请求行为继续向官方 Codex 收口。



## 功能概览
- 账号池管理：分组、标签、排序、备注、封禁识别与封禁筛选
- 批量导入 / 导出：支持多文件导入、桌面端文件夹递归导入 JSON、按账号导出单文件
- 用量展示：兼容 5 小时 + 7 日双窗口，以及仅返回 7 日单窗口的账号，并展示对应窗口的重置时间
- 授权登录：浏览器授权 + 手动回调解析
- 平台 Key：生成、禁用、删除、模型绑定、推理等级、服务等级（跟随请求 / Fast / Flex）
- 本地服务：自动拉起、可自定义端口与监听地址
- 本地网关：为 CLI 和第三方工具提供统一 OpenAI 兼容入口

## 截图
![仪表盘](assets/images/dashboard.png)
![账号管理](assets/images/accounts.png)
![平台 Key](assets/images/platform-key.png)
![日志视图](assets/images/log.png)
![设置页](assets/images/themes.png)

## 快速开始
1. 启动桌面端，点击“启动服务”。
2. 进入“账号管理”，添加账号并完成授权。
3. 如回调失败，粘贴回调链接手动完成解析。
4. 刷新用量并确认账号状态。

## 默认数据目录
- 桌面端默认会把 SQLite 数据库写到应用数据目录下，文件名固定为 `codexmanager.db`。
- Windows：`%APPDATA%\\com.codexmanager.desktop\\codexmanager.db`
- macOS：`~/Library/Application Support/com.codexmanager.desktop/codexmanager.db`
- Linux：`~/.local/share/com.codexmanager.desktop/codexmanager.db`
- 如需调整数据库、代理、监听地址等运行配置，可继续查看 [环境变量与运行配置](docs/report/20260309195355187_环境变量与运行配置说明.md)。

## 页面展示
### 桌面端
- 账号管理：集中导入、导出、刷新账号与用量，支持低配额 / 封禁筛选与重置时间展示
- 平台 Key：按模型、推理等级、服务等级绑定平台 Key，并查看调用日志
- 设置页：统一管理端口、监听地址、代理、主题、自动更新、后台行为

### Service 版
- `codexmanager-service`：提供本地 OpenAI 兼容网关
- `codexmanager-web`：提供浏览器管理页面
- `codexmanager-start`：一键拉起 service + web

## 常用文档
- 版本历史：[CHANGELOG.md](CHANGELOG.md)
- 协作约定：[CONTRIBUTING.md](CONTRIBUTING.md)
- 架构说明：[ARCHITECTURE.md](ARCHITECTURE.md)
- 测试基线：[TESTING.md](TESTING.md)
- 安全说明：[SECURITY.md](SECURITY.md)
- 文档索引：[docs/README.md](docs/README.md)

## 专题页面
| 页面 | 内容 |
| --- | --- |
| [运行与部署指南](docs/report/20260310122606850_运行与部署指南.md) | 首次启动、Docker、Service 版、macOS 放行 |
| [环境变量与运行配置](docs/report/20260309195355187_环境变量与运行配置说明.md) | 应用配置、代理、监听地址、数据库、Web 安全 |
| [FAQ 与账号命中规则](docs/report/20260310122606852_FAQ与账号命中规则.md) | 账号命中、挑战拦截、导入导出、常见异常 |
| [最小排障手册](docs/report/20260307234235414_最小排障手册.md) | 快速定位服务启动、请求转发、模型刷新异常 |
| [构建发布与脚本说明](docs/release/20260310122606851_构建发布与脚本说明.md) | 本地构建、Tauri 打包、Release workflow、脚本参数 |
| [发布与产物说明](docs/release/20260309195355216_发布与产物说明.md) | 各平台发版产物、命名、是否 pre-release |
| [脚本与发布职责对照](docs/report/20260309195735631_脚本与发布职责对照.md) | 各脚本负责什么、什么场景该用哪个 |
| [协议兼容回归清单](docs/report/20260309195735632_协议兼容回归清单.md) | `/v1/chat/completions`、`/v1/responses`、tools 回归项 |
| [CHANGELOG.md](CHANGELOG.md) | 最新发版内容、未发版更新与完整版本历史 |

## 目录结构
```text
.
├─ apps/                # 前端与 Tauri 桌面端
│  ├─ src/
│  ├─ src-tauri/
│  └─ dist/
├─ crates/              # Rust core/service
│  ├─ core
│  ├─ service
│  ├─ start              # Service 版本一键启动器（拉起 service + web）
│  └─ web                # Service 版本 Web UI（可内嵌静态资源 + /api/rpc 代理）
├─ docs/                # 正式文档目录
├─ scripts/             # 构建与发布脚本
└─ README.md
```

## 鸣谢与参考项目

- Codex（OpenAI）：本项目在请求链路、登录语义与上游兼容行为上参考了该项目的实现与源码结构 <https://github.com/openai/codex>

## 认可社区
<p align="left">
  <a href="https://linux.do/t/topic/1688401" title="LINUX DO">
    <img
      src="https://cdn3.linux.do/original/4X/d/1/4/d146c68151340881c884d95e0da4acdf369258c6.png"
      alt="LINUX DO"
      width="100"
      hight="100"
    />
  </a>
</p>

## 赞助支持

感谢每一位支持 CodexManager 的朋友。因为有你们的赞助与捐赠，项目才能持续迭代、稳定维护。

特别感谢方木木、[Wonderdch](https://github.com/Wonderdch)、Catch_Bat 对项目的支持。

- 方木木：感谢提供 token 支持。他的 GPT 卡网支持自助购买、自助兑换激活，稳定不到车，质保 30 天，支持 Codex 5.4。官网：[https://www.aixiamo.com/](https://www.aixiamo.com/)
- 捐赠鸣谢：[Wonderdch](https://github.com/Wonderdch)、Catch_Bat

如果这个项目对你有帮助，欢迎请作者喝杯咖啡，支持后续维护与更新。

<p align="left">
  <img src="assets/images/wechatPay.jpg" alt="微信赞助码" width="180" />
  <img src="assets/images/AliPay.jpg" alt="支付宝赞助码" width="180" />
</p>

## 联系方式
- 公众号：七线牛马
- 微信： ProsperGao

- 交流群：答案是项目名：CodexManager

  <img src="assets/images/qq_group.jpg" alt="交流群二维码" width="280" />
