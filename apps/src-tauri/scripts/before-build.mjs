import { existsSync } from "node:fs";
import { resolve } from "node:path";
import { spawnSync } from "node:child_process";

const cwd = process.cwd();
const task = process.argv[2] || "build:desktop";
const candidates = [
  cwd,
  resolve(cwd, "apps"),
  resolve(cwd, "..", "apps"),
  resolve(cwd, "..", "..", "apps"),
  resolve(cwd, ".."),
  resolve(cwd, "..", ".."),
];

function hasFrontendPackage(dir) {
  return existsSync(resolve(dir, "package.json"));
}

function hasBuiltFrontendDist(dir) {
  return existsSync(resolve(dir, "out", "index.html"));
}

const frontendDir = candidates.find(hasFrontendPackage);
if (!frontendDir) {
  console.error(`前端项目目录不存在，当前工作目录: ${cwd}`);
  process.exit(1);
}

if (task === "build:desktop" && hasBuiltFrontendDist(frontendDir)) {
  console.log(`前端产物已存在，跳过重复构建: ${resolve(frontendDir, "out", "index.html")}`);
  process.exit(0);
}

console.log(`执行前端任务: pnpm --dir ${frontendDir} run ${task}`);
const result = spawnSync("pnpm", ["--dir", frontendDir, "run", task], {
  stdio: "inherit",
  shell: false,
});

if (result.error) {
  console.error(`前端构建启动失败: ${result.error.message}`);
  process.exit(1);
}

if (typeof result.status === "number" && result.status !== 0) {
  process.exit(result.status);
}

process.exit(0);
