import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';
import { chromium } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

async function main(): Promise<void> {
  const raw = process.env.PUBLIC_STATUS_FIXTURE_URL;
  assert.ok(raw, 'PUBLIC_STATUS_FIXTURE_URL is required');
  const base = new URL(raw);
  assert.ok(!base.username && !base.password, 'fixture URL must not include credentials');
  assert.ok(base.protocol === 'http:' && ['127.0.0.1', 'localhost', '::1'].includes(base.hostname), 'fixture must use loopback HTTP');
  const scenarios = ['sparse', 'failed', 'waiting', 'throttled', 'unknown', 'empty-unknown', 'zero', 'stale', 'future', 'unavailable'];
  const browser = await chromium.launch({ headless: true });
  try {
    for (const width of [1365, 390]) {
      const context = await browser.newContext({ viewport: { width, height: 900 } });
      const page = await context.newPage();
      for (const scenario of scenarios) {
        const response = await page.goto(new URL(`${scenario}/status`, base).href, { waitUntil: 'domcontentloaded' });
        assert.equal(response?.status(), 200, `${scenario} HTTP status`);
        await page.getByRole('heading', { name: 'Current-policy acquisition' }).waitFor();
        const text = await page.locator('main').innerText();
        assert.ok(!text.includes('Backfill has not started'));
        assert.ok(!text.includes('healthy'));
        if (scenario === 'failed') assert.match(text, /14541[\s\S]*5980/);
        if (scenario === 'sparse') assert.match(text, /No match[\s\S]*14539/);
        if (scenario === 'waiting') assert.match(text, /retry_waiting/);
        if (scenario === 'throttled') assert.match(text, /stopped_incomplete/);
        if (scenario === 'unknown') assert.match(text, /unknown/);
        if (scenario === 'empty-unknown') assert.match(text, /no enabled sources/);
        if (scenario === 'unavailable') assert.match(text, /evidence unavailable/);
        if (scenario === 'zero') assert.match(text, /0/);
        if (scenario === 'stale') assert.match(text, /86400 seconds ago/);
        if (scenario === 'future') assert.match(text, /clock skew possible/);
        assert.ok(!(await page.locator('body').evaluate((el) => el.scrollWidth > window.innerWidth)), `${scenario} horizontal overflow`);
        const axe = await new AxeBuilder({ page }).analyze();
        assert.deepEqual(axe.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical'), [], `${scenario} accessibility`);
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
      console.log(`public status ${width}px: ${scenarios.length} scenarios passed`);
      await page.close();
    }
  } finally {
    await browser.close();
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err: unknown) => { console.error(err); process.exitCode = 1; });
}
