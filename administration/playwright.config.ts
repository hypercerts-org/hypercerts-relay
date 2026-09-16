import { defineConfig } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { isAbsolute, join } from "node:path";

const controlGoBinary =
  process.env.CONTROL_GO_BINARY ??
  join(
    execFileSync("go", ["env", "GOROOT"], { encoding: "utf8" }).trim(),
    "bin",
    "go",
  );
if (!isAbsolute(controlGoBinary) || !existsSync(controlGoBinary))
  throw new Error(
    "CONTROL_GO_BINARY must identify an absolute Go executable path",
  );

export default defineConfig({
  testDir: "tests/browser",
  workers: 1,
  timeout: 90000,
  use: { baseURL: "http://127.0.0.1:3188", headless: true },
  webServer: {
    command: "npx tsx tests/browser-server.ts",
    env: { ...process.env, CONTROL_GO_BINARY: controlGoBinary },
    url: "http://127.0.0.1:3188/health",
    reuseExistingServer: false,
  },
  reporter: "list",
});
