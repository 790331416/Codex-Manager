import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import ts from "../node_modules/typescript/lib/typescript.js";

const appsRoot = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(appsRoot, "src", "lib", "gateway", "endpoints.ts");

async function loadEndpointModule() {
  const source = await fs.readFile(sourcePath, "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ES2022,
      target: ts.ScriptTarget.ES2022,
    },
    fileName: sourcePath,
  });

  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "codexmanager-gateway-endpoints-"),
  );
  const tempFile = path.join(tempDir, "gateway-endpoints.mjs");
  await fs.writeFile(tempFile, compiled.outputText, "utf8");
  return import(pathToFileURL(tempFile).href);
}

const endpoints = await loadEndpointModule();

test("resolveGatewayOrigin 在 Web 网关模式下优先使用当前浏览器 origin", () => {
  assert.equal(
    endpoints.resolveGatewayOrigin({
      browserOrigin: "https://manager.example.com/",
      runtimeMode: "web-gateway",
      serviceAddr: "localhost:48760",
    }),
    "https://manager.example.com",
  );
});

test("resolveGatewayOrigin 在桌面模式下使用服务地址并补齐协议", () => {
  assert.equal(
    endpoints.resolveGatewayOrigin({
      browserOrigin: "https://manager.example.com",
      runtimeMode: "desktop-tauri",
      serviceAddr: "127.0.0.1:49999",
    }),
    "http://127.0.0.1:49999",
  );
});

test("buildOpenAiGatewayEndpoint 只追加一次 /v1", () => {
  assert.equal(
    endpoints.buildOpenAiGatewayEndpoint("http://localhost:48760"),
    "http://localhost:48760/v1",
  );
  assert.equal(
    endpoints.buildOpenAiGatewayEndpoint("http://localhost:48760/v1"),
    "http://localhost:48760/v1",
  );
});

test("buildClaudeCodeGatewayEndpoint 返回 Claude Code 使用的根地址", () => {
  assert.equal(
    endpoints.buildClaudeCodeGatewayEndpoint("http://localhost:48760/v1"),
    "http://localhost:48760",
  );
});

test("buildGeminiGatewayEndpoint 返回 Gemini CLI 使用的根地址", () => {
  assert.equal(
    endpoints.buildGeminiGatewayEndpoint("http://localhost:48760/v1beta"),
    "http://localhost:48760",
  );
  assert.equal(
    endpoints.buildGeminiGatewayEndpoint("http://localhost:48760/v1alpha"),
    "http://localhost:48760",
  );
});
