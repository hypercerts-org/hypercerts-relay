import { test, expect } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import type { ArchiveKey } from "../../src/api";

test("consumer key screen supports creation, one-time reveal, and revocation", async ({ page }) => {
  let keys: ArchiveKey[] = [{
    id: "AbCdEfGhIjKl",
    name: "indexer",
    owner: "Data team",
    requestsPerMinute: 60,
    archiveMegabytesPerMinute: 100,
    createdAt: "2026-09-28T12:00:00Z",
  }];
  await page.route("**/api/v1/archive-keys**", async (route) => {
    const request = route.request();
    if (request.method() === "GET") {
      await route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ keys }) });
    } else if (request.method() === "POST") {
      const input = request.postDataJSON();
      const key = { ...input, id: "MnOpQrStUvWx", createdAt: "2026-09-28T12:05:00Z" };
      keys = [key, ...keys];
      await route.fulfill({ status: 201, contentType: "application/json", body: JSON.stringify({ key, token: "hck_one_time_test_token" }) });
    } else {
      const id = request.url().split("/").at(-1);
      keys = keys.map((key) => key.id === id ? { ...key, revokedAt: "2026-09-28T12:10:00Z" } : key);
      await route.fulfill({ status: 204 });
    }
  });
  await page.goto("/");
  await page.getByLabel("Handle or DID").fill("operator.example");
  await page.getByRole("button", { name: "Sign in with AT Protocol" }).click();
  await page.getByRole("link", { name: "Consumer API keys" }).click();
  await expect(page.getByRole("heading", { name: "Consumer API keys" })).toBeVisible();
  await expect(page.locator('#archive-key-owners option[value="Data team"]')).toHaveCount(1);

  for (const width of [1505, 390]) {
    await page.setViewportSize({ width, height: 1045 });
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
    const results = await new AxeBuilder({ page }).analyze();
    expect(results.violations.filter((finding) => ["serious", "critical"].includes(finding.impact ?? ""))).toEqual([]);
    await page.screenshot({ path: test.info().outputPath(`archive-keys-${width}.png`), fullPage: true });
  }

  await page.getByLabel("Key name").fill("mirror");
  await page.getByLabel("Owner").fill("Data team");
  await page.getByLabel("Requests/minute").fill("25");
  await page.getByLabel("Archive MB/minute").fill("5");
  await page.getByRole("button", { name: "Create key" }).click();
  await expect(page.getByLabel("New consumer API key")).toHaveValue("hck_one_time_test_token");
  await page.getByRole("button", { name: "Dismiss" }).click();
  await expect(page.getByLabel("New consumer API key")).toHaveCount(0);
  await page.getByRole("button", { name: "Revoke mirror" }).click();
  await page.getByRole("button", { name: "Confirm revoke" }).click();
  await expect(page.getByRole("row", { name: /mirror/ })).toContainText("Revoked");
});
