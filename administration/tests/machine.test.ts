import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { createServer } from "node:http";
import { Store } from "../server/store.ts";
import { Services } from "../server/services.ts";
import { Auth, type OAuthProvider } from "../server/auth.ts";
import { createApp } from "../server/app.ts";
import { discoveryActor } from "../server/machine.ts";
import { Worker } from "../server/worker.ts";

const machineToken = "m".repeat(40);
const oauth: OAuthProvider = {
  async authorize() {
    return new URL("https://pds.example/authorize");
  },
  async callback() {
    return { session: { did: "did:plc:unused" }, state: null };
  },
  async revoke() {},
};

// One fake control server answers Relay source pages and lookups, and the
// Jetstream policy. `states` holds the Relay desired state of known sources.
async function controlServer(t: TestContext) {
  const calls: string[] = [];
  const states = new Map<string, string>();
  const server = createServer((req, res) => {
    calls.push(`${req.method} ${req.url} ${req.headers.authorization}`);
    res.setHeader("Content-Type", "application/json");
    if (req.method === "GET" && req.url?.startsWith("/hypercerts/v1/source?")) {
      const pds = new URL(req.url, "http://x").searchParams.get("pds") ?? "";
      const state = states.get(pds);
      if (!state) {
        res.statusCode = 404;
        res.end(JSON.stringify({ error: "source_not_found" }));
        return;
      }
      res.end(JSON.stringify({ Hostname: new URL(pds).host, DesiredState: state }));
      return;
    }
    if (req.method === "PUT" && req.url === "/hypercerts/v1/source") {
      res.end(JSON.stringify({ Revision: 1, Validation: { Status: "passed" }, Handoff: null }));
      return;
    }
    if (req.url === "/hypercerts/v1/policy") {
      res.end(JSON.stringify({ revision: 7, collections: ["org.hypercerts.claim.activity"] }));
      return;
    }
    if (req.url?.startsWith("/hypercerts/v1/sources")) {
      const after = new URL(req.url, "http://x").searchParams.get("after");
      res.end(
        JSON.stringify(
          after === ""
            ? {
                Sources: [
                  { Hostname: "a.example", NoSSL: false, DesiredState: "enabled", AccountQuota: { Limit: 100, Count: 3 } },
                  { Hostname: "b.example", NoSSL: false, DesiredState: "removed", AccountQuota: { Limit: 100, Count: 0 } },
                ],
                NextAfterHostID: 2,
              }
            : {
                Sources: [{ Hostname: "localhost:2583", NoSSL: true, DesiredState: "disabled" }],
                NextAfterHostID: 0,
              },
        ),
      );
      return;
    }
    res.statusCode = 404;
    res.end("{}");
  }).listen(0, "127.0.0.1");
  await new Promise<void>((resolve) => server.once("listening", resolve));
  t.after(() => server.close());
  const { port } = server.address() as { port: number };
  return { url: `http://127.0.0.1:${port}`, calls, states };
}

const adminDID = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const browserCookie = "browser-session";
const browserCSRF = "x".repeat(43);

async function fixture(t: TestContext, token: string | undefined, controlURL = "http://127.0.0.1:1") {
  const store = new Store(":memory:");
  // A signed-in administrator, to prove a browser session cannot use the
  // discovery routes and the discovery token cannot use browser routes.
  store.db.prepare("INSERT INTO administrators VALUES(?)").run(adminDID);
  store.db
    .prepare("INSERT INTO sessions VALUES(?,?,?,?)")
    .run(createHash("sha256").update(browserCookie).digest("hex"), adminDID, browserCSRF, Date.now() + 60000);
  const services = new Services(
    { url: controlURL, token: "relay-control-token" },
    { url: controlURL, token: "jetstream-control-token" },
  );
  const base = "http://127.0.0.1:3000";
  const server = createApp(store, services, new Auth(store, base, oauth), {}, undefined, {
    machineToken: token,
  }).listen(0, "127.0.0.1");
  await new Promise<void>((resolve) => server.once("listening", resolve));
  t.after(() => {
    server.close();
    store.close();
  });
  const { port } = server.address() as { port: number };
  const request = (
    path: string,
    body?: unknown,
    headers: Record<string, string> = {},
  ) =>
    fetch(`http://127.0.0.1:${port}${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${machineToken}`,
        "Idempotency-Key": randomUUID(),
        ...headers,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  const browser = (path: string, body?: unknown) =>
    request(path, body, {
      Authorization: "",
      Cookie: `relay_session=${browserCookie}`,
      Origin: base,
      "X-CSRF-Token": browserCSRF,
    });
  return { store, services, request, browser };
}

const enable = { kind: "source", pds: "https://new.example", state: "enabled" };

test("discovery routes are absent without a configured token", async (t) => {
  const { request, browser, store } = await fixture(t, undefined);
  // Unmounted, the paths fall through to the browser API: the token alone is
  // not a session, and a signed-in administrator finds no such route.
  assert.equal((await request("/api/v1/sources/discovery/state")).status, 401);
  assert.equal((await request("/api/v1/sources/discovery/operations", enable)).status, 401);
  assert.equal((await browser("/api/v1/sources/discovery/state")).status, 404);
  assert.equal((await browser("/api/v1/sources/discovery/operations", enable)).status, 404);
  assert.equal(store.page("operations", "", 50).items.length, 0);
});

test("a browser session cannot use the discovery routes", async (t) => {
  const { browser, store } = await fixture(t, machineToken);
  const state = await browser("/api/v1/sources/discovery/state");
  assert.equal(state.status, 401);
  assert.deepEqual(await state.json(), { error: "machine_token_required" });
  assert.equal((await browser("/api/v1/sources/discovery/operations", enable)).status, 401);
  assert.equal(store.page("operations", "", 50).items.length, 0);
  // The browser API itself is unaffected by mounting the discovery routes.
  assert.equal((await browser("/api/v1/session")).status, 200);
});

test("machine routes reject a short configured token", () => {
  const store = new Store(":memory:");
  const services = new Services(
    { url: "http://127.0.0.1:1", token: "unused" },
    { url: "http://127.0.0.1:1", token: "unused" },
  );
  assert.throws(() =>
    createApp(store, services, new Auth(store, "http://127.0.0.1:3000", oauth), {}, undefined, {
      machineToken: "short",
    }),
  );
  store.close();
});

test("machine routes require the bearer token", async (t) => {
  const { request, store } = await fixture(t, machineToken);
  for (const authorization of ["", "Bearer wrong", machineToken, `Basic ${machineToken}`]) {
    const state = await request("/api/v1/sources/discovery/state", undefined, { Authorization: authorization });
    assert.equal(state.status, 401);
    const op = await request("/api/v1/sources/discovery/operations", enable, { Authorization: authorization });
    assert.equal(op.status, 401);
  }
  assert.equal(store.page("operations", "", 50).items.length, 0);
});

test("the machine token does not grant browser API access", async (t) => {
  const { request } = await fixture(t, machineToken);
  assert.equal((await request("/api/v1/operations")).status, 401);
  assert.equal((await request("/api/v1/operations", enable)).status, 401);
});

test("discovery state lists every source state, account limits and the policy", async (t) => {
  const control = await controlServer(t);
  const { request } = await fixture(t, machineToken, control.url);
  const response = await request("/api/v1/sources/discovery/state");
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    policyRevision: 7,
    collections: ["org.hypercerts.claim.activity"],
    sources: [
      { pds: "https://a.example", state: "enabled", accountLimit: 100 },
      { pds: "https://b.example", state: "removed", accountLimit: 100 },
      { pds: "http://localhost:2583", state: "disabled", accountLimit: null },
    ],
    maxAccountLimit: 100000,
  });
  assert.deepEqual(control.calls, [
    "GET /hypercerts/v1/policy Bearer jetstream-control-token",
    "GET /hypercerts/v1/sources?after= Bearer relay-control-token",
    "GET /hypercerts/v1/sources?after=2 Bearer relay-control-token",
  ]);
});

test("machine operations are limited to enabling sources and raising account limits", async (t) => {
  const { request, store } = await fixture(t, machineToken);
  for (const forbidden of [
    { kind: "source", pds: "https://new.example", state: "disabled" },
    { kind: "source", pds: "https://new.example", state: "removed" },
    { kind: "account_quota", pds: "https://new.example", expectedLimit: 2000, accountLimit: 1000 },
    { kind: "account_quota", pds: "https://new.example", expectedLimit: 1000, accountLimit: 1000 },
    { kind: "collections", expectedRevision: 1, collections: [] },
    { kind: "limit", scope: "global", eventsPerSecond: 10 },
    { kind: "job", pds: "https://new.example", reason: "backfill" },
  ]) {
    const response = await request("/api/v1/sources/discovery/operations", forbidden);
    assert.equal(response.status, 403, JSON.stringify(forbidden));
  }
  assert.equal((await request("/api/v1/sources/discovery/operations", { kind: "source", pds: "ftp://x", state: "enabled" })).status, 400);
  assert.equal((await request("/api/v1/sources/discovery/operations", enable, { "Idempotency-Key": "not-a-uuid" })).status, 400);
  assert.equal(store.page("operations", "", 50).items.length, 0);
});

test("machine operations are journaled and audited under the discovery actor", async (t) => {
  const control = await controlServer(t);
  const { request, store } = await fixture(t, machineToken, control.url);
  const key = randomUUID();
  const first = await request("/api/v1/sources/discovery/operations", enable, { "Idempotency-Key": key });
  assert.equal(first.status, 202);
  const op = (await first.json()) as { id: string; actor: string; state: string };
  assert.equal(op.id, key);
  assert.equal(op.actor, discoveryActor);
  assert.equal(op.state, "requested");
  const repeat = await request("/api/v1/sources/discovery/operations", enable, { "Idempotency-Key": key });
  assert.equal(repeat.status, 202);
  assert.equal(((await repeat.json()) as { id: string }).id, key);
  const quota = { kind: "account_quota", pds: "https://new.example", expectedLimit: 100, accountLimit: 2000 };
  assert.equal((await request("/api/v1/sources/discovery/operations", quota)).status, 202);
  assert.equal(
    (await request("/api/v1/sources/discovery/operations", quota, { "Idempotency-Key": key })).status,
    409,
  );
  assert.equal(store.page("operations", "", 50).items.length, 2);
  const audit = store.page("audit", "", 50).items as unknown as { actor: string; action: string }[];
  assert.deepEqual(
    audit.map((row) => [row.actor, row.action]),
    [
      [discoveryActor, "requested"],
      [discoveryActor, "requested"],
    ],
  );
});

test("machine enables never override an operator's disable or remove", async (t) => {
  const control = await controlServer(t);
  const { request, store } = await fixture(t, machineToken, control.url);
  control.states.set("https://disabled.example", "disabled");
  control.states.set("https://removed.example", "removed");
  control.states.set("https://enabled.example", "enabled");
  for (const pds of ["https://disabled.example", "https://removed.example"]) {
    const response = await request("/api/v1/sources/discovery/operations", { ...enable, pds });
    assert.equal(response.status, 409, pds);
    assert.deepEqual(await response.json(), { error: "source_disabled_by_operator" });
  }
  // An unknown or already enabled source may be (re-)requested.
  for (const pds of ["https://unknown.example", "https://enabled.example"])
    assert.equal((await request("/api/v1/sources/discovery/operations", { ...enable, pds })).status, 202, pds);
  assert.equal(store.page("operations", "", 50).items.length, 2);
});

test("a machine enable recorded before an operator disable is not applied", async (t) => {
  const control = await controlServer(t);
  const { request, store, services } = await fixture(t, machineToken, control.url);
  const key = randomUUID();
  assert.equal((await request("/api/v1/sources/discovery/operations", enable, { "Idempotency-Key": key })).status, 202);
  // The operator disables the PDS before the worker reaches the request.
  control.states.set("https://new.example", "disabled");
  await new Worker(store, services).tick();
  const op = store.operation(key)!;
  assert.equal(op.state, "failed");
  assert.equal(op.error, "source_disabled_by_operator");
  assert.ok(!control.calls.some((call) => call.startsWith("PUT ")), "Relay source was changed");
  // Replaying the same key returns the recorded operation without a new check.
  const replay = await request("/api/v1/sources/discovery/operations", enable, { "Idempotency-Key": key });
  assert.equal(replay.status, 202);
  assert.equal(((await replay.json()) as { state: string }).state, "failed");
});

test("the worker applies a machine enable for an unknown source", async (t) => {
  const control = await controlServer(t);
  const { request, store, services } = await fixture(t, machineToken, control.url);
  const key = randomUUID();
  assert.equal((await request("/api/v1/sources/discovery/operations", enable, { "Idempotency-Key": key })).status, 202);
  await new Worker(store, services).tick();
  assert.equal(store.operation(key)!.state, "applied");
  assert.ok(control.calls.some((call) => call.startsWith("PUT /hypercerts/v1/source ")));
});

test("administrators manage the discovery maximum account limit with CSRF and audit", async (t) => {
  const control = await controlServer(t);
  const { browser, request, store } = await fixture(t, machineToken, control.url);
  const settings = await browser("/api/v1/sources/discovery/settings");
  assert.equal(settings.status, 200);
  assert.deepEqual(await settings.json(), { maxAccountLimit: 100000 });
  // The machine token cannot read or change settings.
  assert.equal((await request("/api/v1/sources/discovery/settings")).status, 401);
  assert.equal((await request("/api/v1/sources/discovery/settings", { maxAccountLimit: 5000 })).status, 401);
  for (const invalid of [999, 1.5, "5000", null, Number.MAX_SAFE_INTEGER + 1])
    assert.equal(
      (await browser("/api/v1/sources/discovery/settings", { maxAccountLimit: invalid })).status,
      400,
      String(invalid),
    );
  assert.equal(
    (await browser("/api/v1/sources/discovery/settings", { maxAccountLimit: 5000, extra: 1 })).status,
    400,
  );
  const missingCSRF = await request("/api/v1/sources/discovery/settings", { maxAccountLimit: 5000 }, {
    Authorization: "",
    Cookie: "relay_session=browser-session",
    Origin: "http://127.0.0.1:3000",
  });
  assert.equal(missingCSRF.status, 403);
  const saved = await browser("/api/v1/sources/discovery/settings", { maxAccountLimit: 5000 });
  assert.equal(saved.status, 200);
  assert.deepEqual(await saved.json(), { maxAccountLimit: 5000 });
  const audit = store.page("audit", "", 50).items as unknown as { actor: string; action: string; detail: unknown }[];
  assert.deepEqual(
    audit.map((row) => [row.actor, row.action, row.detail]),
    [[adminDID, "discovery_settings_changed", { previous: { maxAccountLimit: 100000 }, current: { maxAccountLimit: 5000 } }]],
  );
  // Discovery receives the new maximum and cannot request more than it.
  const state = await request("/api/v1/sources/discovery/state");
  assert.equal(((await state.json()) as { maxAccountLimit: number }).maxAccountLimit, 5000);
  const above = await request("/api/v1/sources/discovery/operations", {
    kind: "account_quota",
    pds: "https://new.example",
    expectedLimit: 100,
    accountLimit: 6000,
  });
  assert.equal(above.status, 403);
  assert.deepEqual(await above.json(), { error: "account_limit_above_discovery_maximum" });
  const within = await request("/api/v1/sources/discovery/operations", {
    kind: "account_quota",
    pds: "https://new.example",
    expectedLimit: 100,
    accountLimit: 5000,
  });
  assert.equal(within.status, 202);
});

test("discovery settings stay available to administrators without a discovery token", async (t) => {
  const { browser } = await fixture(t, undefined);
  assert.equal((await browser("/api/v1/sources/discovery/settings")).status, 200);
  assert.equal((await browser("/api/v1/sources/discovery/settings", { maxAccountLimit: 2000 })).status, 200);
});

test("a discovery account limit above a maximum lowered after the request is not applied", async (t) => {
  const control = await controlServer(t);
  const { request, browser, store, services } = await fixture(t, machineToken, control.url);
  const key = randomUUID();
  const quota = { kind: "account_quota", pds: "https://new.example", expectedLimit: 100, accountLimit: 50000 };
  assert.equal((await request("/api/v1/sources/discovery/operations", quota, { "Idempotency-Key": key })).status, 202);
  assert.equal((await browser("/api/v1/sources/discovery/settings", { maxAccountLimit: 10000 })).status, 200);
  await new Worker(store, services).tick();
  const op = store.operation(key)!;
  assert.equal(op.state, "failed");
  assert.equal(op.error, "account_limit_above_discovery_maximum");
  assert.ok(!control.calls.some((call) => call.startsWith("PUT ")), "Relay quota was changed");
});
