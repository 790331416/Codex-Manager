# Codex 链路识别与删除范围

日期：2026-04-24

## 结论

仓库里的“Codex 链路”不是单个模块，而是一条贯穿前端、Tauri 壳、service 网关、协议适配、请求改写、会话锚点、上游头构造、模型缓存同步的完整实现链。

如果目标是“全部删掉重做”，真正需要动的核心范围主要在：

1. `crates/service/src/gateway/**`
2. `crates/service/src/http/gateway_endpoint.rs`
3. `crates/service/src/http/backend_router.rs`
4. `crates/service/src/account/account_warmup.rs`
5. `crates/core/src/storage/conversation_bindings.rs`
6. `crates/core/src/storage/mod.rs`
7. `crates/core/migrations/034_conversation_bindings.sql`
8. `apps/src/lib/constants/codex.ts`
9. `apps/src/components/layout/codex-cli-onboarding-dialog.tsx`
10. `apps/src/components/layout/app-bootstrap.tsx`
11. `apps/src/types/runtime.ts`
12. `apps/src-tauri/src/commands/service.rs`
13. `apps/src-tauri/src/service_runtime.rs`

## 核心后端链路

这些文件是“真正处理 Codex 请求”的主干，不删就不算重做：

- `crates/service/src/gateway/mod.rs`
  - 网关总入口。
  - 明确识别 `x-codex-*`、`session_id`、`conversation_id`、`codex_cli_rs`。
  - 组装并导出几乎所有 Codex 相关子模块。
- `crates/service/src/http/gateway_endpoint.rs`
  - HTTP 网关入口，实际把请求送进 `gateway::handle_gateway_request`。
- `crates/service/src/gateway/request/request_entry.rs`
  - 网关请求主流程，含本地校验、本地 models/count_tokens 特判、最终代理转发。
- `crates/service/src/gateway/protocol_adapter/*`
  - 协议适配层。
  - 包含 `codex_adapter.rs`、`request_router.rs`、`response_conversion/*`。
- `crates/service/src/gateway/request/*`
  - 请求重写、头解析、会话亲和、thread anchor、local models、local count tokens。
- `crates/service/src/gateway/upstream/*`
  - 上游 URL、头、候选路由、重试、failover、实际传输。
- `crates/service/src/gateway/routing/*`
  - 会话绑定、候选选择、冷却、路由策略。
- `crates/service/src/gateway/observability/*`
  - 请求日志、SSE 聚合、错误日志、指标。
- `crates/service/src/gateway/local_validation/*`
  - 本地请求合法性校验，已经深度依赖 Codex 语义。

## Codex 特有耦合点

这些不是网关入口，但明显属于 Codex 实现的一部分：

- `crates/service/src/account/account_warmup.rs`
  - 预热 URL 固定指向 `https://chatgpt.com/backend-api/codex/responses`。
- `crates/service/src/gateway/upstream/config.rs`
  - 默认上游基址是 `https://chatgpt.com/backend-api/codex`。
- `crates/service/src/gateway/upstream/headers/codex_headers.rs`
  - 明确构造 `x-codex-window-id`、`x-codex-parent-thread-id`、`x-codex-turn-state`、`x-codex-turn-metadata`。
- `crates/service/src/gateway/request/incoming_headers.rs`
  - 明确解析所有关键 `x-codex-*` 头。
- `crates/service/src/gateway/request/thread_anchor.rs`
  - 原生线程锚点逻辑。
- `crates/service/src/gateway/request/session_affinity.rs`
  - 基于 thread anchor 的会话亲和与冲突处理。
- `crates/service/src/gateway/request/request_rewrite.rs`
  - 针对 `/backend-api/codex` 的路径兼容重写。
- `crates/service/src/gateway/request/request_rewrite_responses.rs`
  - responses 形态重写与 Codex header 兼容。

## 存储层耦合

如果要“彻底重做”，下面这些也要一起清：

- `crates/core/src/storage/conversation_bindings.rs`
- `crates/core/src/storage/mod.rs`
  - `thread_anchor` 字段定义在这里。
- `crates/core/migrations/034_conversation_bindings.sql`
  - conversation binding 表迁移。

说明：

- 这部分不删，新的链路仍会被旧的 conversation/thread 绑定模型约束。
- 真要重做，通常还要同步清理依赖它的调用点与测试。

## 前端与桌面壳耦合

这些文件不是请求代理核心，但承担了 Codex CLI 的产品入口与本地联动：

- `apps/src/lib/constants/codex.ts`
  - 默认 `originator` 与 `user-agent version` 常量。
- `apps/src/components/layout/codex-cli-onboarding-dialog.tsx`
  - 完整的 Codex CLI 接入引导 UI。
- `apps/src/components/layout/app-bootstrap.tsx`
  - 引导弹窗接入点。
- `apps/src/types/runtime.ts`
  - `codexHome` 出现在初始化结果类型里。
- `apps/src-tauri/src/commands/service.rs`
  - 启停 service。
  - 解析 `codex_cli_rs/...`。
  - 负责同步 `~/.codex/models_cache.json`。
- `apps/src-tauri/src/service_runtime.rs`
  - 用 `initialize` 返回的 `userAgent` 与 `codexHome` 识别服务是不是“正确的 CodexManager 服务”。

## 大量测试会被连带删除或失效

下面这些测试不是核心实现，但强依赖当前 Codex 链路语义：

- `crates/service/tests/gateway_logs/openai.rs`
- `crates/service/tests/gateway_logs/anthropic.rs`
- `crates/service/tests/gateway_logs/retry_logging.rs`
- `crates/service/tests/gateway/availability/upstream_headers.rs`
- `crates/service/src/http/tests/proxy_runtime_tests.rs`
- `crates/service/src/gateway/protocol_adapter/tests/**`
- `apps/tests/*` 中所有依赖 `gatewayOriginator = "codex-cli"`、`codexHome`、`/api/runtime`、`/api/rpc` 或模型缓存行为的测试

## 不建议跟着一起删的范围

以下内容包含 `codex` 字样，但不应直接视为“Codex 链路代码”：

- 项目名、crate 名、二进制名、Docker 镜像名中的 `codexmanager`
- README、文档、发布脚本、CI 中的品牌名和产物名
- 非链路性的普通 RPC、UI 框架、账户管理、插件中心、用量统计页面壳

## 建议的实际删除顺序

如果你确认“全部删掉重做”，建议按下面顺序做，风险最低：

1. 先删前端接入与 Tauri 同步层
2. 再删 `crates/service/src/gateway/**` 与 `http/gateway_endpoint.rs`
3. 再删 conversation binding 存储与 migration
4. 最后清理相关测试、文档和常量

## 当前判断

就当前仓库而言，“Codex 链路代码”的最小可闭环删除范围，不是简单搜 `codex` 字符串，而是至少要覆盖：

- `crates/service/src/gateway/**`
- `crates/service/src/http/gateway_endpoint.rs`
- `crates/service/src/account/account_warmup.rs`
- `crates/core/src/storage/conversation_bindings.rs`
- `crates/core/migrations/034_conversation_bindings.sql`
- `apps/src/lib/constants/codex.ts`
- `apps/src/components/layout/codex-cli-onboarding-dialog.tsx`
- `apps/src-tauri/src/commands/service.rs`
- `apps/src-tauri/src/service_runtime.rs`

如果只删其中一部分，仓库会留下大量半失效依赖，后续重做成本反而更高。
