# Gateway 优化 TODO Round 11

更新时间：2026-04-13

本轮目标：抽取前端 HTTP JSON-RPC POST 底座，统一 `postWebRpc` 与 `requestlogListViaHttpRpc` 的请求模板。

- [x] 新建 HTTP JSON-RPC helper 模块
- [x] 让 `transport.ts` 复用共享 POST helper
- [x] 为新模块补最小 Node 单测
- [x] 运行关键前端验证并记录结果

本轮验证：

- `pnpm test:runtime`
- `pnpm build:desktop`
