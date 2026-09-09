import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Store } from "../server/store.ts";
import { Worker } from "../server/worker.ts";
import { Services } from "../server/services.ts";
import { Auth, type OAuthProvider } from "../server/auth.ts";
import { createApp } from "../server/app.ts";
import { ApiError, command } from "../server/contracts.ts";

const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const oauth: OAuthProvider = {
  async authorize() {
    return new URL("https://pds.example/authorize");
  },
  async callback() {
    return { session: { did }, state: null };
  },
  async revoke() {},
};
async function fixture(t: TestContext) {
  const store = new Store(":memory:");
  store.db.prepare("INSERT INTO administrators VALUES(?)").run(did);
  const token = "browser-session",
    csrf = "x".repeat(43),
    hash = createHash("sha256").update(token).digest("hex");
  store.db
    .prepare("INSERT INTO sessions VALUES(?,?,?,?)")
    .run(hash, did, csrf, Date.now() + 60000);
  const services = new Services(
    { url: "http://127.0.0.1:1", token: "unused-test-service-token" },
    { url: "http://127.0.0.1:1", token: "unused-test-service-token" },
  );
  const base = "http://127.0.0.1:3000";
  const auth = new Auth(store, base, oauth);
  const server = createApp(store, services, auth, {}).listen(0, "127.0.0.1");
  await new Promise<void>((resolve) => server.once("listening", resolve));
  const address = server.address() as { port: number };
  t.after(() => {
    server.close();
    store.close();
  });
  const request = (
    path: string,
    body?: unknown,
    headers: Record<string, string> = {},
  ) =>
    fetch(`http://127.0.0.1:${address.port}${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: `relay_session=${token}`,
        Origin: base,
        "X-CSRF-Token": csrf,
        "Idempotency-Key": randomUUID(),
        ...headers,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  return { store, services, auth, request };
}
test("authentication, CSRF and immediate administrator removal guard durable mutations", async (t) => {
  const { store, request } = await fixture(t);
  const body = { kind: "source", pds: "https://pds.example", state: "enabled" };
  for (const headers of [
    { Cookie: "" },
    { "X-CSRF-Token": "" },
    { Origin: "https://evil.example" },
  ] as Record<string, string>[])
    assert.ok(
      (await request("/api/v1/operations", body, headers)).status >= 400,
    );
  assert.equal(store.page("operations", "", 50).items.length, 0);
  assert.equal((await request("/api/v1/operations", body)).status, 202);
  store.db.prepare("DELETE FROM administrators WHERE did=?").run(did);
  assert.equal((await request("/api/v1/operations", body)).status, 401);
  assert.equal(store.page("operations", "", 50).items.length, 1);
});
test("signout and revocation invalidate server-side sessions", async (t) => {
  const { request } = await fixture(t);
  assert.equal((await request("/api/v1/signout", {})).status, 204);
  assert.equal((await request("/api/v1/session")).status, 401);
});
test("quota requests require administrator CSRF and validated integer values", async (t) => {
  const { request, store } = await fixture(t);
  const body = {
    kind: "account_quota",
    pds: "https://pds.example",
    expectedLimit: 100,
    accountLimit: 250,
  };
  assert.equal(
    (await request("/api/v1/operations", body, { Cookie: "" })).status,
    401,
  );
  assert.equal(
    (await request("/api/v1/operations", body, { "X-CSRF-Token": "" })).status,
    403,
  );
  for (const invalid of [-1, 1.5, Number.MAX_SAFE_INTEGER + 1, "250", null]) {
    assert.equal(
      (await request("/api/v1/operations", { ...body, accountLimit: invalid }))
        .status,
      400,
    );
  }
  assert.equal(store.page("operations", "", 50).items.length, 0);
  assert.equal((await request("/api/v1/operations", body)).status, 202);
  assert.equal(store.page("operations", "", 50).items.length, 1);
});
test("durable operations recover after restart and retain actor and failure states", async () => {
  const path = join(mkdtempSync(join(tmpdir(), "control-test-")), "state.db");
  let store = new Store(path);
  const cmd = command.parse({
    kind: "collections",
    expectedRevision: 1,
    collections: ["app.bsky.feed.post"],
  });
  const op = store.request(did, cmd);
  store.transition(op.id, "applying");
  store.close();
  store = new Store(path);
  let fail = true;
  const services = {
    async apply() {
      if (fail) throw new ApiError(503, "jetstream_unavailable");
      return { revision: 2, collections: ["app.bsky.feed.post"] };
    },
  } as unknown as Services;
  const worker = new Worker(store, services);
  worker.recover();
  assert.equal(store.operation(op.id)?.state, "requested");
  await worker.tick();
  assert.equal(store.operation(op.id)?.state, "incomplete");
  fail = false;
  store.transition(op.id, "requested");
  await worker.tick();
  assert.equal(store.operation(op.id)?.state, "applied");
  assert.equal(store.operation(op.id)?.actor, did);
  assert.ok(store.page("audit", "", 100).items.length >= 6);
  store.close();
});
test("idempotency and invalid rate policies cannot create extra work", async (t) => {
  const { request, store } = await fixture(t);
  const id = randomUUID();
  const body = { kind: "limit", scope: "global", eventsPerSecond: 10 };
  for (let i = 0; i < 2; i++)
    assert.equal(
      (await request("/api/v1/operations", body, { "Idempotency-Key": id }))
        .status,
      202,
    );
  assert.equal(store.page("operations", "", 50).items.length, 1);
  assert.equal(
    (await request("/api/v1/operations", { ...body, eventsPerSecond: -1 }))
      .status,
    400,
  );
  assert.equal(
    (
      await request(
        "/api/v1/operations",
        { ...body, eventsPerSecond: 11 },
        { "Idempotency-Key": id },
      )
    ).status,
    409,
  );
});

test("a source absent from Jetstream can still be disabled in Relay", async () => {
  const services = new Services(
    { url: "https://unused.example", token: "unused" },
    { url: "https://unused.example", token: "unused" },
  );
  const calls: string[] = [];
  services.call = async <T>(service: "relay" | "jetstream") => {
    calls.push(service);
    if (service === "jetstream")
      throw new ApiError(404, "jetstream_rejected_404");
    return { DesiredState: "disabled" } as T;
  };
  const result = await services.apply(
    { kind: "source", pds: "https://relay-only.example", state: "disabled" },
    randomUUID(),
  );
  assert.deepEqual(calls, ["jetstream", "relay"]);
  assert.deepEqual(result, {
    relay: { DesiredState: "disabled" },
    jetstream: { enabled: false },
  });
});

test("job actions always pass a durable receipt key even when the target already matches", async () => {
  const services = new Services(
    { url: "https://unused.example", token: "unused" },
    { url: "https://unused.example", token: "unused" },
  );
  let seen: unknown[] = [];
  services.call = async <T>(...args: Parameters<Services["call"]>) => {
    seen = args;
    return { state: "canceled" } as T;
  };
  const receipt = randomUUID();
  await services.apply(
    { kind: "job_action", id: "a".repeat(32), action: "cancel" },
    receipt,
  );
  assert.equal(seen[2], "POST");
  assert.equal(seen[4], receipt);
});

test("OAuth callback binding rejects a different browser and non-admin identity", async (t) => {
  const { auth, request, store } = await fixture(t);
  const binding = "opaque-browser-binding";
  auth.provider.callback = async () => ({
    session: { did },
    state: createHash("sha256").update(binding).digest("hex"),
  });
  let response = await request("/auth/callback?code=fixture", undefined, {
    Cookie: "relay_oauth=different",
  });
  assert.equal(response.status, 403);
  auth.provider.callback = async () => ({
    session: { did: "did:plc:nonadmin" },
    state: createHash("sha256").update(binding).digest("hex"),
  });
  response = await request("/auth/callback?code=fixture", undefined, {
    Cookie: `relay_oauth=${binding}`,
  });
  assert.equal(response.status, 403);
  assert.equal(
    store.db.prepare("SELECT count(*) AS n FROM sessions").get()!.n,
    1,
  );
});

test("admins manage other administrators with CSRF, immediate revocation and actor audit", async (t) => {
  const { request, store } = await fixture(t);
  const other = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
  const grant = { did: other, action: "grant" };
  for (const headers of [
    { Cookie: "" },
    { "X-CSRF-Token": "" },
    { Origin: "https://evil.example" },
  ] as Record<string, string>[])
    assert.ok(
      (await request("/api/v1/administrators", grant, headers)).status >= 400,
    );
  assert.equal(
    (
      await request("/api/v1/administrators", {
        ...grant,
        did: "a.handle.example",
      })
    ).status,
    400,
  );
  assert.equal((await request("/api/v1/administrators", grant)).status, 204);
  assert.equal((await request("/api/v1/administrators", grant)).status, 204);
  assert.equal(
    (await request("/api/v1/administrators", { did, action: "remove" })).status,
    400,
  );
  const first = await (await request("/api/v1/administrators?limit=1")).json();
  assert.deepEqual(first.items, [{ did }]);
  assert.equal(first.next, did);
  const second = await (
    await request(
      `/api/v1/administrators?after=${encodeURIComponent(first.next)}&limit=1`,
    )
  ).json();
  assert.deepEqual(second.items, [{ did: other }]);
  store.db
    .prepare("INSERT INTO sessions VALUES(?,?,?,?)")
    .run(
      createHash("sha256").update("other-session").digest("hex"),
      other,
      "x".repeat(43),
      Date.now() + 60000,
    );
  store.set("oauth-session", did, { encrypted: "test-only-value" });
  assert.equal(
    (
      await request(
        "/api/v1/administrators",
        { did, action: "remove" },
        { Cookie: "relay_session=other-session" },
      )
    ).status,
    204,
  );
  assert.equal((await request("/api/v1/session")).status, 401);
  assert.equal((await request("/api/v1/administrators", grant)).status, 401);
  assert.equal(store.get("oauth-session", did), undefined);
  const audit = store.page("audit", "", 50).items;
  assert.deepEqual(
    audit.map((row: any) => [row.actor, row.action]),
    [
      [did, "administrator_grant"],
      [other, "administrator_remove"],
    ],
  );
});
