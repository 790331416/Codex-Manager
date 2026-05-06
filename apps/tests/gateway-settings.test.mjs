import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import ts from "../node_modules/typescript/lib/typescript.js";

const appsRoot = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(
  appsRoot,
  "src",
  "lib",
  "api",
  "gateway-settings.ts"
);

async function loadGatewaySettingsModule() {
  const source = await fs.readFile(sourcePath, "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ES2022,
      target: ts.ScriptTarget.ES2022,
    },
    fileName: sourcePath,
  });

  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "codexmanager-gateway-settings-")
  );
  const tempFile = path.join(tempDir, "gateway-settings.mjs");
  await fs.writeFile(tempFile, compiled.outputText, "utf8");
  return import(pathToFileURL(tempFile).href);
}

const gatewaySettings = await loadGatewaySettingsModule();

test("readGatewayTransportSettings 读取真实传输配置并补齐默认值", () => {
  const settings = gatewaySettings.readGatewayTransportSettings({
    sseKeepaliveIntervalMs: 5000,
    upstreamStreamTimeoutMs: "120000",
  });

  assert.equal(settings.sseKeepaliveIntervalMs, 5000);
  assert.equal(settings.upstreamStreamTimeoutMs, 120000);
  assert.deepEqual(settings.envKeys, [
    "CODEXMANAGER_SSE_KEEPALIVE_INTERVAL_MS",
    "CODEXMANAGER_UPSTREAM_STREAM_TIMEOUT_MS",
    "CODEXMANAGER_UPSTREAM_TOTAL_TIMEOUT_MS",
  ]);
  assert.equal(settings.requiresRestart, false);
});

test("readGatewayUpstreamProxySettings 与 readGatewayRouteStrategySettings 对齐对象返回", () => {
  const proxy = gatewaySettings.readGatewayUpstreamProxySettings({
    proxyUrl: "http://127.0.0.1:7890",
  });
  assert.equal(proxy.proxyUrl, "http://127.0.0.1:7890");
  assert.equal(proxy.envKey, "CODEXMANAGER_UPSTREAM_PROXY_URL");

  const route = gatewaySettings.readGatewayRouteStrategySettings({
    strategy: "balanced",
    manualPreferredAccountId: "acc-1",
  });
  assert.equal(route.strategy, "balanced");
  assert.deepEqual(route.options, ["ordered", "balanced"]);
  assert.equal(route.manualPreferredAccountId, "acc-1");
});

test("readGatewayConcurrencyRecommendation 解析推荐并补齐保底值", () => {
  const recommendation = gatewaySettings.readGatewayConcurrencyRecommendation({
    cpuCores: "12",
    memoryMib: 32768,
    usageRefreshWorkers: 6,
    httpWorkerFactor: "5",
    httpWorkerMin: 12,
    httpStreamWorkerFactor: 2,
    httpStreamWorkerMin: 4,
    accountMaxInflight: 2,
  });

  assert.equal(recommendation.cpuCores, 12);
  assert.equal(recommendation.memoryMib, 32768);
  assert.equal(recommendation.usageRefreshWorkers, 6);
  assert.equal(recommendation.httpWorkerFactor, 5);
  assert.equal(recommendation.httpWorkerMin, 12);
  assert.equal(recommendation.httpStreamWorkerFactor, 2);
  assert.equal(recommendation.httpStreamWorkerMin, 4);
  assert.equal(recommendation.accountMaxInflight, 2);
  assert.equal(recommendation.queueWaitTimeoutMs, 100);
});

test("readServiceListenConfig 对齐监听模式配置返回", () => {
  const listenConfig = gatewaySettings.readServiceListenConfig({
    mode: "all_interfaces",
    requiresRestart: true,
  });
  assert.equal(listenConfig.mode, "all_interfaces");
  assert.deepEqual(listenConfig.options, ["loopback", "all_interfaces"]);
  assert.equal(listenConfig.requiresRestart, true);
});

test("readGatewayManualAccountId 统一读取 manual account id", () => {
  assert.equal(
    gatewaySettings.readGatewayManualAccountId({ accountId: "acc-9" }),
    "acc-9"
  );
  assert.equal(gatewaySettings.readGatewayManualAccountId(null), "");
});
