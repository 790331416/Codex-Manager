import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import ts from "../node_modules/typescript/lib/typescript.js";

const appsRoot = path.resolve(import.meta.dirname, "..");
const sourcePath = path.join(appsRoot, "src", "lib", "utils", "ccswitch.ts");

async function loadCcSwitchModule() {
  const source = await fs.readFile(sourcePath, "utf8");
  const compiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.ES2022,
      target: ts.ScriptTarget.ES2022,
    },
    fileName: sourcePath,
  });

  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "codexmanager-ccswitch-"));
  const tempFile = path.join(tempDir, "ccswitch.mjs");
  await fs.writeFile(tempFile, compiled.outputText, "utf8");
  return import(pathToFileURL(tempFile).href);
}

const ccswitch = await loadCcSwitchModule();

test("normalizeCodexManagerGatewayEndpoint maps service address to local /v1 endpoint", () => {
  assert.equal(
    ccswitch.normalizeCodexManagerGatewayEndpoint("localhost:49760"),
    "http://localhost:49760/v1",
  );
  assert.equal(
    ccswitch.normalizeCodexManagerGatewayEndpoint("http://127.0.0.1:48760"),
    "http://127.0.0.1:48760/v1",
  );
  assert.equal(
    ccswitch.normalizeCodexManagerGatewayEndpoint("0.0.0.0:48760"),
    "http://localhost:48760/v1",
  );
});

test("buildCcSwitchProviderImportUrl encodes provider import parameters", () => {
  const url = ccswitch.buildCcSwitchProviderImportUrl({
    app: "codex",
    name: "CodexManager - Team Key",
    endpoint: "http://localhost:48760/v1",
    apiKey: "cm-test-key",
    model: "gpt-5.4",
    notes: "Imported from CodexManager",
    enabled: true,
  });

  const parsed = new URL(url);
  assert.equal(parsed.protocol, "ccswitch:");
  assert.equal(parsed.host, "v1");
  assert.equal(parsed.pathname, "/import");
  assert.equal(parsed.searchParams.get("resource"), "provider");
  assert.equal(parsed.searchParams.get("app"), "codex");
  assert.equal(parsed.searchParams.get("name"), "CodexManager - Team Key");
  assert.equal(parsed.searchParams.get("endpoint"), "http://localhost:48760/v1");
  assert.equal(parsed.searchParams.get("apiKey"), "cm-test-key");
  assert.equal(parsed.searchParams.get("model"), "gpt-5.4");
  assert.equal(parsed.searchParams.get("enabled"), "true");
});

test("buildCcSwitchProviderName keeps existing CodexManager prefix", () => {
  assert.equal(
    ccswitch.buildCcSwitchProviderName("Team Key", "key-1"),
    "CodexManager - Team Key",
  );
  assert.equal(
    ccswitch.buildCcSwitchProviderName("CodexManager Shared", "key-1"),
    "CodexManager Shared",
  );
});
