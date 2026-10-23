import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import { chromium, type Page } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

const scenarios = ['sparse', 'failed', 'waiting', 'throttled', 'unknown', 'empty-unknown', 'zero', 'stale', 'future', 'unavailable'];

function fixtureBase(): URL {
  const raw = process.env.PUBLIC_STATUS_FIXTURE_URL;
  assert.ok(raw, 'PUBLIC_STATUS_FIXTURE_URL is required');
  const base = new URL(raw);
  assert.ok(!base.username && !base.password, 'fixture URL must not include credentials');
  assert.ok(base.protocol === 'http:' && ['127.0.0.1', 'localhost', '::1'].includes(base.hostname), 'fixture must use loopback HTTP');
  return base;
}

async function assertScenario(page: Page, base: URL, scenario: string): Promise<void> {
  const response = await page.goto(new URL(`${scenario}/status`, base).href, { waitUntil: 'domcontentloaded' });
  assert.equal(response?.status(), 200, `${scenario} HTTP status`);
  await page.getByRole('heading', { name: 'Current-policy acquisition' }).waitFor();
  const text = await page.locator('main').innerText();
  assert.ok(!text.includes('Backfill has not started'));
  assert.ok(!text.includes('healthy'));
  const expected: Record<string, RegExp> = {
    failed: /14541[\s\S]*5980/,
    sparse: /No match[\s\S]*14539/,
    waiting: /retry_waiting/,
    throttled: /stopped_incomplete/,
    unknown: /unknown/,
    'empty-unknown': /no enabled sources/,
    unavailable: /evidence unavailable/,
    zero: /0/,
    stale: /86400 seconds ago/,
    future: /clock skew possible/,
  };
  if (expected[scenario]) assert.match(text, expected[scenario]);
  assert.ok(!(await page.locator('body').evaluate((el) => el.scrollWidth > window.innerWidth)), `${scenario} horizontal overflow`);
  const axe = await new AxeBuilder({ page }).analyze();
  assert.deepEqual(axe.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical'), [], `${scenario} accessibility`);
  await assertKeyboardLink(page, scenario);
}

async function assertKeyboardLink(page: Page, scenario: string): Promise<void> {
  await page.keyboard.press('Tab');
  const focused = await page.evaluate(() => {
    const active = document.activeElement;
    return active instanceof HTMLAnchorElement && active.matches(':focus-visible')
      ? { href: active.getAttribute('href'), text: active.innerText.trim() }
      : null;
  });
  assert.ok(focused, `${scenario} Tab reaches a keyboard-visible link`);
  assert.ok(focused.href, `${scenario} focused link has a destination`);
}

async function runBrowserFixture(): Promise<void> {
  const base = fixtureBase();
  const browser = await chromium.launch({ headless: true });
  try {
    for (const width of [1365, 390]) {
      const context = await browser.newContext({ viewport: { width, height: 900 } });
      try {
        const page = await context.newPage();
        for (const scenario of scenarios) await assertScenario(page, base, scenario);
        console.log(`public status ${width}px: ${scenarios.length} scenarios passed`);
      } finally {
        await context.close();
      }
    }
  } finally {
    await browser.close();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await runBrowserFixture();
}
