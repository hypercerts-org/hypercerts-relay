import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "tests/browser",
  workers: 1,
  timeout: 90000,
  use: { baseURL: "http://127.0.0.1:3188", headless: true },
  webServer: {
    command: "npx tsx tests/browser-server.ts",
    url: "http://127.0.0.1:3188/health",
    reuseExistingServer: false,
  },
  reporter: "list",
});
