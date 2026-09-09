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
    await page.evaluate(() => window.scrollTo(0, 0));
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
  for (const width of [1365, 768, 390]) {
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
  await expect(
    page.locator("main").getByText("Test Operator", { exact: true }),
  ).toBeVisible();
  await expect(
    page.locator("main").getByText("@operator.example", { exact: true }),
  ).toBeVisible();
  await page.reload();
  await expect(
    page.locator("main").getByText("Test Operator", { exact: true }),
  ).toBeVisible();
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

test("branded sign-in and sidebar identity work with and without profile metadata", async ({
  page,
}) => {
  await page.goto("/");
  for (const width of [1365, 390]) {
    await page.setViewportSize({ width, height: 900 });
    await expect(
      page.getByRole("heading", { name: "Relay administration" }),
    ).toBeVisible();
    const fonts = await page.evaluate(async () => {
      await Promise.all([
        document.fonts.load('400 48px "Instrument Serif"'),
        document.fonts.load('italic 400 48px "Instrument Serif"'),
        document.fonts.load('700 16px "Switzer Variable"'),
      ]);
      return [...document.fonts].map(({ family, status }) => ({
        family,
        status,
      }));
    });
    expect(fonts.filter((font) => font.status === "loaded")).toHaveLength(3);
    const results = await new AxeBuilder({ page }).analyze();
    expect(
      results.violations.filter((v) =>
        ["serious", "critical"].includes(v.impact ?? ""),
      ),
    ).toEqual([]);
    await page.screenshot({
      path: test.info().outputPath(`login-${width}.png`),
      fullPage: true,
    });
  }
  await login(page);
  for (const width of [1365, 390]) {
    await page.setViewportSize({ width, height: 900 });
    const account = page.getByLabel("Signed-in account");
    await expect(
      account.getByText("Test Operator", { exact: true }),
    ).toBeVisible();
    await expect(
      account.getByText("@operator.example", { exact: true }),
    ).toBeVisible();
    await expect(account.locator("strong")).toHaveCSS("font-weight", "700");
    await expect(account.locator(".account-identity > span")).toHaveCSS(
      "color",
      "rgb(83, 83, 83)",
    );
    if (width === 1365) {
      const box = await account.boundingBox();
      expect(box!.x).toBeLessThan(264);
      expect(box!.y).toBeGreaterThan(600);
      expect(box!.y + box!.height).toBeLessThanOrEqual(900);
    }
    await page.screenshot({
      path: test.info().outputPath(`overview-${width}.png`),
      fullPage: true,
    });
  }
  await page.setViewportSize({ width: 1365, height: 768 });
  const shortAccount = page.getByLabel("Signed-in account");
  await page.screenshot({
    path: test.info().outputPath("overview-short-desktop.png"),
    fullPage: true,
  });
  const shortBox = await shortAccount.boundingBox();
  expect(shortBox!.y + shortBox!.height).toBeLessThanOrEqual(768);
  await page.getByRole("link", { name: "Administrators", exact: true }).focus();
  await expect(
    page.getByRole("link", { name: "Administrators", exact: true }),
  ).toBeInViewport();
  await expect(
    shortAccount.getByText("Test Operator", { exact: true }),
  ).toBeInViewport();
  await expect(
    shortAccount.getByRole("button", { name: "Revoke my sessions" }),
  ).toBeInViewport();
  await page.setViewportSize({ width: 390, height: 900 });
  for (const profile of [
    {
      displayName: null,
      handle: "operator.example",
      label: "@operator.example",
    },
    {
      displayName: null,
      handle: null,
      label: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
    },
    {
      displayName:
        "Operator with a very long display name that must wrap without overflowing the sidebar",
      handle: "very.long.operator.handle.example",
      label:
        "Operator with a very long display name that must wrap without overflowing the sidebar",
    },
  ]) {
    await page.route("**/api/v1/session", async (route) => {
      const response = await route.fetch();
      const session = await response.json();
      await route.fulfill({
        response,
        json: {
          ...session,
          displayName: profile.displayName,
          handle: profile.handle,
        },
      });
    });
    await page.reload();
    const account = page.getByLabel("Signed-in account");
    await expect(account.locator("strong")).toHaveText(profile.label);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    await page.unroute("**/api/v1/session");
  }
});

test("successful polling clears observation errors without hiding action failures", async ({
  page,
}) => {
  await login(page);
  await page.route("**/api/v1/limits?*", (route) =>
    route.fulfill({
      status: 502,
      contentType: "text/html",
      body: "Bad gateway",
    }),
  );
  await page.getByRole("link", { name: "Rate limits", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("HTTP 502");
  await page.unroute("**/api/v1/limits?*");
  await expect(page.getByRole("alert")).toHaveCount(0, { timeout: 12_000 });
  await page.route("**/api/v1/operations", (route) =>
    route.fulfill({
      status: 409,
      contentType: "application/json",
      body: JSON.stringify({ error: "revision_conflict" }),
    }),
  );
  await page.getByLabel("Events per second").fill("150");
  await page.getByRole("button", { name: "Request limit change" }).click();
  await expect(page.getByRole("alert")).toContainText("revision conflict");
  await page.waitForResponse((response) =>
    response.url().includes("/api/v1/limits?"),
  );
  await expect(page.getByRole("alert")).toContainText("revision conflict");
});
