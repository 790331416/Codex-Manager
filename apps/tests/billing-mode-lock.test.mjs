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
  "billing-mode-lock.ts"
);

async function loadBillingModeLockModule() {
  const source = await fs.readFile(sourcePath, "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ES2022,
      target: ts.ScriptTarget.ES2022,
    },
    fileName: sourcePath,
  });

  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "codexmanager-billing-mode-lock-")
  );
  const tempFile = path.join(tempDir, "billing-mode-lock.mjs");
  await fs.writeFile(tempFile, compiled.outputText, "utf8");
  return import(pathToFileURL(tempFile).href);
}

const billingModeLock = await loadBillingModeLockModule();

test("readBillingModeLock 补齐未锁定默认值", () => {
  assert.deepEqual(billingModeLock.readBillingModeLock(null), {
    accountModeLocked: false,
    distributionLocked: false,
    reasons: [],
  });
  assert.deepEqual(billingModeLock.readBillingModeLock({}), {
    accountModeLocked: false,
    distributionLocked: false,
    reasons: [],
  });
});

test("readBillingModeLock 解析锁定状态并过滤空原因", () => {
  assert.deepEqual(
    billingModeLock.readBillingModeLock({
      accountModeLocked: true,
      distributionLocked: true,
      reasons: [" member_users ", "", 12, "wallet_ledger"],
    }),
    {
      accountModeLocked: true,
      distributionLocked: true,
      reasons: ["member_users", "wallet_ledger"],
    }
  );
});
