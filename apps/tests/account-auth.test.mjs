import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import ts from "../node_modules/typescript/lib/typescript.js";

const appsRoot = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(appsRoot, "src", "lib", "api", "account-auth.ts");

async function loadAccountAuthModule() {
  const source = await fs.readFile(sourcePath, "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ES2022,
      target: ts.ScriptTarget.ES2022,
    },
    fileName: sourcePath,
  });

  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "codexmanager-account-auth-")
  );
  const tempFile = path.join(tempDir, "account-auth.mjs");
  await fs.writeFile(tempFile, compiled.outputText, "utf8");
  return import(pathToFileURL(tempFile).href);
}

const accountAuth = await loadAccountAuthModule();

test("readLoginStatusResult 统一读取登录状态对象", () => {
  const result = accountAuth.readLoginStatusResult({
    status: " success ",
    error: " temporary ",
  });
  assert.equal(result.status, "success");
  assert.equal(result.error, "temporary");
});

test("readCurrentAccessTokenAccountReadResult 解析当前账号与认证要求", () => {
  const result = accountAuth.readCurrentAccessTokenAccountReadResult({
    account: {
      type: "chatgpt",
      accountId: "acc-1",
      email: "demo@example.com",
      planType: "pro",
      planTypeRaw: "pro",
      hasSubscription: true,
      subscriptionPlan: "pro",
      subscriptionExpiresAt: 1746502289,
      subscriptionRenewsAt: 1746502289,
      chatgptAccountId: "org-1",
      workspaceId: "ws-1",
      status: "active",
    },
    requiresOpenaiAuth: true,
  });

  assert.equal(result.account?.accountId, "acc-1");
  assert.equal(result.account?.email, "demo@example.com");
  assert.equal(result.account?.chatgptAccountId, "org-1");
  assert.equal(result.account?.hasSubscription, true);
  assert.equal(result.account?.subscriptionPlan, "pro");
  assert.equal(result.requiresOpenaiAuth, true);
});

test("readChatgptAuthTokensRefreshResult 对齐刷新返回字段", () => {
  const result = accountAuth.readChatgptAuthTokensRefreshResult({
    accessToken: " token ",
    chatgptAccountId: " org-2 ",
    chatgptPlanType: " team ",
    hasSubscription: true,
    subscriptionPlan: "team",
    subscriptionExpiresAt: 1746502289,
    subscriptionRenewsAt: 1746502289,
  });

  assert.equal(result.accessToken, "token");
  assert.equal(result.chatgptAccountId, "org-2");
  assert.equal(result.chatgptPlanType, "team");
  assert.equal(result.hasSubscription, true);
  assert.equal(result.subscriptionPlan, "team");
});

test("readChatgptAuthTokensRefreshAllResult 对齐批量刷新返回字段", () => {
  const result = accountAuth.readChatgptAuthTokensRefreshAllResult({
    requested: "2",
    succeeded: 1,
    failed: "1",
    skipped: 0,
    results: [
      {
        accountId: " acc-1 ",
        accountName: " demo@example.com ",
        ok: true,
        message: null,
      },
      {
        accountId: "acc-2",
        accountName: "failed@example.com",
        ok: false,
        message: " reused ",
      },
    ],
  });

  assert.equal(result.requested, 2);
  assert.equal(result.succeeded, 1);
  assert.equal(result.failed, 1);
  assert.equal(result.results[0].accountId, "acc-1");
  assert.equal(result.results[1].message, "reused");
});
