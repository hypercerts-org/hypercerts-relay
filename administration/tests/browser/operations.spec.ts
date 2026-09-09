import { test, expect } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
async function login(page: import("@playwright/test").Page) {
  await page.goto("/");
  await page.getByLabel("Handle or DID").fill("operator.example");
  await page.getByRole("button", { name: "Sign in with AT Protocol" }).click();
  await expect(
    page.getByRole("heading", { name: "Relay operations" }),
  ).toBeVisible();
}
test("administrator operates sources, collections, jobs, rates and revokes the session", async ({
  page,
}) => {
  await login(page);
  await page.getByRole("link", { name: "PDS sources", exact: true }).click();
  await page
    .getByLabel("PDS origin", { exact: true })
    .fill("https://quiet.example");
  await page.getByRole("button", { name: "Add source", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "quiet.example", exact: true }),
  ).toBeVisible({ timeout: 12000 });
  for (const name of ["Disable", "Re-enable"]) {
    await page.getByRole("button", { name, exact: true }).click();
    await expect(
      page.getByRole("button", {
        name: name === "Disable" ? "Re-enable" : "Disable",
        exact: true,
      }),
    ).toBeVisible({ timeout: 12000 });
  }
  await page
    .getByRole("button", { name: "quiet.example", exact: true })
    .click();
  const quota = page.getByLabel("Account quota", { exact: true });
  await quota.fill("-1");
  await page
    .getByRole("button", { name: "Request account quota change" })
    .click();
  await expect(quota).toBeFocused();
  await expect(page.getByRole("alert")).toContainText("Enter a whole number");
  await quota.fill("250");
  for (const width of [1365, 390]) {
    await page.setViewportSize({ width, height: 900 });
    await quota.focus();
    const results = await new AxeBuilder({ page }).analyze();
    expect(
      results.violations.filter((v) =>
        ["serious", "critical"].includes(v.impact ?? ""),
      ),
    ).toEqual([]);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: test.info().outputPath(`quota-${width}.png`),
      fullPage: true,
    });
  }
  await page
    .getByRole("button", { name: "Request account quota change" })
    .click();
  await expect(page.getByText("0 / 250 accounts")).toBeVisible({
    timeout: 12000,
  });
  await page.reload();
  await page
    .getByRole("button", { name: "quiet.example", exact: true })
    .click();
  await expect(page.getByLabel("Account quota", { exact: true })).toHaveValue(
    "250",
  );
  await page.getByRole("link", { name: "Collections", exact: true }).click();
  await page
    .getByLabel("Enabled collection NSIDs")
    .fill("app.bsky.feed.post\napp.bsky.feed.like");
  await page.getByRole("button", { name: "Request policy change" }).click();
  await expect(page.getByText("2 enabled collections")).toBeVisible({
    timeout: 12000,
  });
  await page.getByRole("link", { name: "Backfill jobs", exact: true }).click();
  await page.getByLabel("Enabled PDS origin").fill("https://quiet.example");
  await page.getByRole("button", { name: "Submit backfill" }).click();
  await expect(page.getByText("source unavailable")).toBeVisible({
    timeout: 12000,
  });
  await page.getByRole("link", { name: "Rate limits", exact: true }).click();
  await page.getByLabel("Events per second").fill("12");
  await page.getByRole("button", { name: "Request limit change" }).click();
  await expect(
    page.getByRole("cell", { name: "12 events/second", exact: true }),
  ).toBeVisible({ timeout: 12000 });
  await page.getByRole("link", { name: "Audit history", exact: true }).click();
  await expect(page.getByRole("table")).toContainText(
    "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
  );
  await page.getByRole("button", { name: "Revoke my sessions" }).click();
  await expect(
    page.getByRole("button", { name: "Sign in with AT Protocol" }),
  ).toBeVisible();
  expect((await page.request.get("/api/v1/session")).status()).toBe(401);
});
test("desktop and mobile screens have no serious accessibility violations or page overflow", async ({
  page,
}) => {
  await login(page);
  for (const width of [1365, 390]) {
    await page.setViewportSize({ width, height: 900 });
    for (const route of [
      "/",
      "/sources",
      "/collections",
      "/jobs",
      "/coverage",
      "/limits",
      "/changes",
      "/audit",
      "/administrators",
    ]) {
      await page.goto(route);
      await expect(page.locator("h1")).toBeVisible();
      const results = await new AxeBuilder({ page }).analyze();
      expect(
        results.violations.filter((v) =>
          ["serious", "critical"].includes(v.impact ?? ""),
        ),
        JSON.stringify(results.violations),
      ).toEqual([]);
      expect(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= window.innerWidth,
        ),
      ).toBe(true);
    }
  }
  await page.goto("/sources");
  await page.screenshot({
    path: test.info().outputPath("mobile.png"),
    fullPage: true,
  });
  await page.setViewportSize({ width: 1365, height: 900 });
  await page.screenshot({
    path: test.info().outputPath("desktop.png"),
    fullPage: true,
  });
});

test("administrator grants and removes access through the UI", async ({
  page,
}) => {
  await login(page);
  await page.getByRole("link", { name: "Administrators", exact: true }).click();
  await expect(page.getByText("Test Operator", { exact: true })).toBeVisible();
  await expect(
    page.getByText("@operator.example", { exact: true }),
  ).toBeVisible();
  await page.reload();
  await expect(page.getByText("Test Operator", { exact: true })).toBeVisible();
  const did = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
  await page
    .getByLabel("Administrator DID", { exact: true })
    .fill("invalid.example");
  await page
    .getByRole("button", { name: "Grant administrator access" })
    .click();
  await expect(page.getByRole("alert")).toContainText("Enter a valid DID");
  await expect(
    page.getByLabel("Administrator DID", { exact: true }),
  ).toBeFocused();
  await page.getByLabel("Administrator DID", { exact: true }).fill(did);
  await page
    .getByRole("button", { name: "Grant administrator access" })
    .click();
  await expect(
    page
      .getByRole("status")
      .filter({ hasText: "Administrator access granted" }),
  ).toBeVisible();
  await page.reload();
  const remove = page.getByRole("button", {
    name: `Remove administrator ${did}`,
    exact: true,
  });
  await expect(remove).toBeVisible();
  for (const width of [1365, 390]) {
    await page.setViewportSize({ width, height: 900 });
    await remove.click();
    await expect(
      page.getByRole("heading", { name: "Remove administrator access?" }),
    ).toBeVisible();
    const results = await new AxeBuilder({ page }).analyze();
    expect(
      results.violations.filter((v) =>
        ["serious", "critical"].includes(v.impact ?? ""),
      ),
    ).toEqual([]);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    await page.screenshot({
      path: test.info().outputPath(`administrators-${width}.png`),
      fullPage: true,
    });
    await page.getByRole("button", { name: "Cancel", exact: true }).click();
  }
  await remove.click();
  await page
    .getByRole("button", { name: "Confirm removal", exact: true })
    .click();
  await expect(remove).toHaveCount(0);
  await page.reload();
  await expect(remove).toHaveCount(0);
});
