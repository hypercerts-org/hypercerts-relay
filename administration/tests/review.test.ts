import { test } from "node:test";
import assert from "node:assert/strict";
import { chmodSync, mkdtempSync, statSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import express, { type ErrorRequestHandler } from "express";
import { Store } from "../server/store.ts";
import { Worker } from "../server/worker.ts";
import { Services } from "../server/services.ts";
import { loginLimit, railwayClientIP } from "../server/login-limit.ts";
import { api } from "../src/api.ts";
const actor = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const command = {
  kind: "limit",
  scope: "global",
  eventsPerSecond: 10,
} as const;

test("journal pages follow time with UUID tie-breaking and reject invalid cursors", (t) => {
  const store = new Store(":memory:");
  t.after(() => store.close());
  const ids = [
    "FFFFFFFF-FFFF-4FFF-8FFF-FFFFFFFFFFFF",
    "00000000-0000-4000-8000-000000000000",
    "11111111-1111-4111-8111-111111111111",
  ];
  for (const [i, id] of ids.entries()) {
    store.request(actor, command, id);
    store.db
      .prepare("UPDATE operations SET createdAt=? WHERE id=?")
      .run(`2026-09-09T00:00:0${i === 0 ? 0 : 1}.000Z`, id);
  }
  let cursor = "";
  const seen: string[] = [];
  do {
    const page = store.page("operations", cursor, 1);
    seen.push(
      ...page.items.map((row) => ("id" in row ? row.id : "") as string),
    );
    cursor = page.next ?? "";
  } while (cursor);
  assert.deepEqual(seen, ids);
  assert.throws(() => store.page("operations", "bad", 50), /invalid_cursor/);
});
test("stale transitions leave state and audit unchanged; canceled work is not claimed", async (t) => {
  const store = new Store(":memory:");
  t.after(() => store.close());
  const op = store.request(actor, command);
  assert.equal(
    store.transition(op.id, "canceled", null, null, actor, ["requested"]),
    true,
  );
  const count = store.page("audit", "", 100).items.length;
  assert.equal(
    store.transition(op.id, "applying", null, null, undefined, ["requested"]),
    false,
  );
  assert.equal(store.page("audit", "", 100).items.length, count);
  assert.equal(store.operation(op.id)?.state, "canceled");
  const next = store.request(actor, command);
  const original = store.transition.bind(store);
  store.transition = (...args) => {
    if (args[1] === "applying")
      original(next.id, "canceled", null, null, actor, ["requested"]);
    return original(...args);
  };
  const worker = new Worker(store, {
    apply: async () => assert.fail("canceled work applied"),
  } as unknown as Services);
  await worker.tick();
  assert.equal(store.operation(next.id)?.state, "canceled");
});
test("existing database and WAL files become private without chmod of a shared parent", (t) => {
  const dir = mkdtempSync(join(tmpdir(), "control-permissions-"));
  chmodSync(dir, 0o755);
  const path = join(dir, "control.db");
  const first = new Store(path);
  first.set("fixture", "key", { private: true });
  for (const suffix of ["", "-wal", "-shm"]) {
    assert.equal(statSync(path + suffix).mode & 0o777, 0o600);
    chmodSync(path + suffix, 0o644);
  }
  const second = new Store(path);
  t.after(() => {
    second.close();
    first.close();
    rmSync(dir, { recursive: true, force: true });
  });
  assert.equal(statSync(dir).mode & 0o777, 0o755);
  for (const suffix of ["", "-wal", "-shm"])
    assert.equal(statSync(path + suffix).mode & 0o777, 0o600);
});
test("proxy login limiting isolates clients only when the immediate proxy is trusted", async (t) => {
  for (const trusted of [false, true]) {
    const app = express();
    app.set("trust proxy", trusted ? ["loopback"] : false);
    app.use(railwayClientIP({ railway: true }));
    app.use(loginLimit(2));
    app.use((_req, res) => res.sendStatus(200));
    const errors: ErrorRequestHandler = (error, _req, res, _next) => {
      res.status(error.status ?? 500).end();
    };
    app.use(errors);
    const server = app.listen(0, "127.0.0.1");
    await new Promise<void>((resolve) => server.once("listening", resolve));
    t.after(() => server.close());
    const { port } = server.address() as { port: number };
    const request = (ip: string) =>
      fetch(`http://127.0.0.1:${port}`, {
        headers: { "X-Real-IP": ip, "X-Forwarded-For": "203.0.113.99" },
      });
    for (let i = 0; i < 10; i++)
      assert.equal((await request("203.0.113.1")).status, 200);
    assert.equal((await request("203.0.113.1")).status, 429);
    assert.equal((await request("203.0.113.2")).status, trusted ? 200 : 429);
    if (trusted) {
      // At capacity an existing client's remaining allowance is preserved.
      assert.equal((await request("203.0.113.2")).status, 200);
      assert.equal((await request("203.0.113.3")).status, 200);
      assert.equal((await request("203.0.113.2")).status, 200);
    }
  }
});
test("control URLs reject malformed origins and retain encrypted private-network HTTP support", () => {
  const config = {
    url: "http://relay.railway.internal:2472",
    token: "fixture",
  };
  assert.doesNotThrow(() => new Services({ ...config }, { ...config }));
  for (const url of [
    "ftp://example.com",
    "https://user:pass@example.com",
    "https://example.com/path",
    "https://example.com?secret=1",
    "https://example.com/#fragment",
  ])
    assert.throws(() => new Services({ ...config, url }, { ...config }));
});
test("API reports proxy HTML failures and invalid success bodies without JSON parser errors", async (t) => {
  const fetcher = t.mock.method(globalThis, "fetch");
  fetcher.mock.mockImplementation(
    async () => new Response("<h1>Bad gateway</h1>", { status: 502 }),
  );
  await assert.rejects(api("/status"), /HTTP 502/);
  fetcher.mock.mockImplementation(
    async () => new Response("invalid", { status: 200 }),
  );
  await assert.rejects(api("/status"), /invalid response/);
  fetcher.mock.mockImplementation(async () =>
    Response.json({ error: "revision_conflict" }, { status: 409 }),
  );
  await assert.rejects(api("/status"), /revision conflict/);
});
