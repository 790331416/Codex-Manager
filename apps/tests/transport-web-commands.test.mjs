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
  "transport-web-commands.ts"
);

async function loadTransportWebCommandsModule() {
  const source = await fs.readFile(sourcePath, "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ES2022,
      target: ts.ScriptTarget.ES2022,
    },
    fileName: sourcePath,
  });

  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "codexmanager-transport-web-commands-")
  );
  const tempFile = path.join(tempDir, "transport-web-commands.mjs");
  await fs.writeFile(tempFile, compiled.outputText, "utf8");
  return import(pathToFileURL(tempFile).href);
}

const transportWebCommands = await loadTransportWebCommandsModule();
const commandMap = transportWebCommands.createWebCommandMap(async () => ({}));

test("createWebCommandMap 复用 keyId 到 id 的参数映射", () => {
  const descriptor = commandMap.service_apikey_delete;
  assert.ok(descriptor.mapParams);
  assert.deepEqual(descriptor.mapParams({ keyId: "key-1", extra: 1 }), {
    keyId: "key-1",
    extra: 1,
    id: "key-1",
  });
});

test("createWebCommandMap 为登录命令补齐 Web 运行壳参数", () => {
  const startLogin = commandMap.service_login_start;
  assert.ok(startLogin.mapParams);
  assert.deepEqual(startLogin.mapParams({ loginType: "chatgpt" }), {
    loginType: "chatgpt",
    type: "chatgpt",
    openBrowser: false,
  });

  const authTokens = commandMap.service_login_chatgpt_auth_tokens;
  assert.ok(authTokens.mapParams);
  assert.deepEqual(authTokens.mapParams({ foo: "bar" }), {
    foo: "bar",
    type: "chatgptAuthTokens",
  });
});

test("createWebCommandMap 为账号预热命令提供 Web RPC 映射", () => {
  const warmup = commandMap.service_account_warmup;
  assert.deepEqual(warmup, {
    rpcMethod: "account/warmup",
  });
});

test("createWebCommandMap 为按状态清理账号提供 Web RPC 映射", () => {
  const cleanup = commandMap.service_account_delete_by_statuses;
  assert.deepEqual(cleanup, {
    rpcMethod: "account/deleteByStatuses",
  });
});
