import { defineConfig } from "@playwright/test";
import { existsSync } from "node:fs";
import { isAbsolute } from "node:path";

const controlGoBinary = process.env.CONTROL_GO_BINARY;
if (
  !controlGoBinary ||
  !isAbsolute(controlGoBinary) ||
  !existsSync(controlGoBinary)
)
  throw new Error(
    "CONTROL_GO_BINARY must identify an absolute Go executable path",
  );

export default defineConfig({
  testDir: "tests/browser",
  workers: 1,
  timeout: 90000,
  use: { baseURL: "http://127.0.0.1:3188", headless: true },
  webServer: {
    // Run node directly: the npx and tsx wrapper processes relay and escalate
    // signals, which kills the server partway through its cleanup.
    command: "node --import tsx tests/browser-server.ts",
    env: { ...process.env, CONTROL_GO_BINARY: controlGoBinary },
    timeout: 300_000,
    url: "http://127.0.0.1:3188/health",
    reuseExistingServer: false,
    // Without this Playwright SIGKILLs the server, which skips its shutdown
    // and leaves the control fixtures' temporary directory behind.
    gracefulShutdown: { signal: "SIGTERM", timeout: 15_000 },
  },
  reporter: "list",
});
