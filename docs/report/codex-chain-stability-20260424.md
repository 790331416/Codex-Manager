# Codex 链路稳定性对比（2026-04-24）

## 结论

我们当前主链路整体已经是：入站 `tiny_http`/HTTP → 本地鉴权与请求解析 → 协议适配与请求体重写 → 候选账号路由 → ChatGPT backend Codex 出站 header/body 组装 → `reqwest`/WebSocket 出站发送。

按官方 Codex 源码对齐时，出口以官方 `ResponsesApiRequest`、`ResponsesOptions`、`build_responses_headers`、`BearerAuthProvider` 和 `default_headers` 为准；不按经验猜字段。

## 官方 Codex HTTP `/responses` 出口

- Body 字段来自 `codex-rs/codex-api/src/common.rs` 的 `ResponsesApiRequest`：`model`、`instructions`、`input`、`tools`、`tool_choice`、`parallel_tool_calls`、`reasoning`、`store`、`stream`、`include`、`service_tier`、`prompt_cache_key`、`text`、`client_metadata`。
- `instructions` 使用 `skip_serializing_if = "String::is_empty"`，所以空字符串不会出现在官方出口 body。
- 官方 HTTP 请求由 `codex-rs/core/src/client.rs` 构造：`tool_choice = "auto"`、`stream = true`、`include` 仅在有 `reasoning` 时包含 `reasoning.encrypted_content`。
- 官方 ChatGPT/Codex backend 鉴权 provider 会添加 `Authorization`，有账号 ID 时会添加 `ChatGPT-Account-ID`。
- 官方默认 client 会添加 `originator`，并通过 reqwest user-agent 设置 `User-Agent`；有 residency requirement 时添加 `x-openai-internal-codex-residency`。
- 官方 `client_metadata` 会包含 `x-codex-installation-id`，来源是 `codex_home/installation_id` 文件中的持久化 UUID。

## 当前已落地修复

- WebSocket：将上游 `websocket_connection_limit_reached` 映射为 `429`，复用候选账号轮换逻辑。
- HTTP body：按官方 `skip_serializing_if = "String::is_empty"` 语义，不再给缺失的 `instructions` 补空字符串，并移除空字符串 `instructions`。
- HTTP body：按官方 `codex_home/installation_id` 语义，在 `process_env::db_dir()/installation_id` 持久化 UUID，并合并到 `client_metadata.x-codex-installation-id`。
- 测试覆盖：新增/更新对应单测，锁住官方行为。

## 后续只按官方证据继续对齐

1. WebSocket 出站对齐官方 `permessage_deflate` 与 custom CA connector。
2. HTTP/WS 统一捕获官方响应元数据：`OpenAI-Model`、`X-Models-Etag`、`X-Reasoning-Included`、`x-codex-turn-state`、rate limit headers。
3. 请求压缩严格对齐官方：已有 `Content-Encoding` 时压缩应失败，不发送 header/body 不一致的请求。
