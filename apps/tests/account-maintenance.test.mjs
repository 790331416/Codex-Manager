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
  "account-maintenance.ts"
);

async function loadAccountMaintenanceModule() {
  const source = await fs.readFile(sourcePath, "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ES2022,
      target: ts.ScriptTarget.ES2022,
    },
    fileName: sourcePath,
  });

  const tempDir = await fs.mkdtemp(
    path.join(os.tmpdir(), "codexmanager-account-maintenance-")
  );
  const tempFile = path.join(tempDir, "account-maintenance.mjs");
  await fs.writeFile(tempFile, compiled.outputText, "utf8");
  return import(pathToFileURL(tempFile).href);
}

const accountMaintenance = await loadAccountMaintenanceModule();

test("readAccountImportResult 统一清洗导入结果与错误列表", () => {
  const result = accountMaintenance.readAccountImportResult({
    canceled: false,
    total: "3",
    created: 2,
    updated: 1,
    failed: 0,
    fileCount: "3",
    directoryPath: " C:/imports ",
    contents: [" a ", "b", 1],
    errors: [{ index: "2", message: " invalid " }, null],
  });

  assert.equal(result.total, 3);
  assert.equal(result.created, 2);
  assert.equal(result.updated, 1);
  assert.equal(result.fileCount, 3);
  assert.equal(result.directoryPath, "C:/imports");
  assert.deepEqual(result.contents, ["a", "b"]);
  assert.deepEqual(result.errors, [{ index: 2, message: "invalid" }]);
});

test("readAccountExportResult 与 readDeleteUnavailableFreeResult 对齐数字字段", () => {
  const exportResult = accountMaintenance.readAccountExportResult({
    canceled: true,
    exported: "4",
    outputDir: " C:/exports ",
  });
  assert.equal(exportResult.canceled, true);
  assert.equal(exportResult.exported, 4);
  assert.equal(exportResult.outputDir, "C:/exports");

  const deleteResult = accountMaintenance.readDeleteUnavailableFreeResult({
    deleted: "6",
  });
  assert.equal(deleteResult.deleted, 6);

  const cleanupResult = accountMaintenance.readDeleteAccountsByStatusesResult({
    scanned: "9",
    deleted: 4,
    skippedStatus: "5",
    targetStatuses: [" banned ", "limited", 1],
    deletedAccountIds: [" acc-1 ", "acc-2"],
  });
  assert.equal(cleanupResult.scanned, 9);
  assert.equal(cleanupResult.deleted, 4);
  assert.equal(cleanupResult.skippedStatus, 5);
  assert.deepEqual(cleanupResult.targetStatuses, ["banned", "limited"]);
  assert.deepEqual(cleanupResult.deletedAccountIds, ["acc-1", "acc-2"]);
});

test("readApiKeySecret 统一读取 secret 字段", () => {
  const secret = accountMaintenance.readApiKeySecret({
    key: " secret-value ",
  });
  assert.equal(secret, "secret-value");
});
