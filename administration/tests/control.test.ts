import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createServer } from "node:http";
import { Store } from "../server/store.ts";
import { Worker } from "../server/worker.ts";
import { Services } from "../server/services.ts";
import { Auth, type OAuthProvider } from "../server/auth.ts";
import { createApp, type AppOptions } from "../server/app.ts";
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
async function fixture(t: TestContext, options: AppOptions = {}) {
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
  const server = createApp(
    store,
    services,
    auth,
    {},
    undefined,
    options,
  ).listen(0, "127.0.0.1");
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
    method?: string,
  ) =>
    fetch(`http://127.0.0.1:${address.port}${path}`, {
      method: method ?? (body === undefined ? "GET" : "POST"),
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
test("status displays only configured public service origins", async (t) => {
  const { request } = await fixture(t, {
    publicServiceOrigins: {
      relay: "https://relay.example",
      rainbow: "https://rainbow.example",
      jetstream: "https://jetstream.example",
    },
  });
  const response = await request("/api/v1/status");
  assert.equal(response.status, 200);
  const status = await response.json();
  assert.deepEqual(status.publicServiceOrigins, {
    relay: "https://relay.example",
    rainbow: "https://rainbow.example",
    jetstream: "https://jetstream.example",
  });
});
test("archive keys require an administrator, CSRF and bounded metadata", async (t) => {
  const { request, services, store } = await fixture(t);
  const seen: unknown[] = [];
  services.archiveKeys = async () => ({ keys: [] });
  services.createArchiveKey = async (input) => {
    seen.push(input);
    return {
      key: {
        id: "AbCdEfGhIjKl",
        ...input,
        createdAt: "2026-09-28T00:00:00Z",
      },
      token: "hck_secret_must_not_be_stored",
    };
  };
  services.revokeArchiveKey = async (id) => {
    seen.push(id);
    return null;
  };
  const input = {
    name: "  Staging consumer  ",
    owner: "  Reporting team  ",
    requestsPerMinute: 30,
    archiveMegabytesPerMinute: 12,
  };
  assert.equal(
    (await request("/api/v1/archive-keys", input, { Cookie: "" })).status,
    401,
  );
  assert.equal(
    (await request("/api/v1/archive-keys", input, { "X-CSRF-Token": "" }))
      .status,
    403,
  );
  assert.equal(
    (await request("/api/v1/archive-keys", undefined, { Cookie: "" })).status,
    401,
  );
  for (const bad of [
    { ...input, owner: " " },
    { ...input, requestsPerMinute: 0 },
    { ...input, archiveMegabytesPerMinute: 1.5 },
    { ...input, archiveMegabytesPerMinute: 100001 },
    { ...input, token: "caller-chosen-secret" },
  ])
    assert.equal((await request("/api/v1/archive-keys", bad)).status, 400);
  assert.deepEqual(seen, []);
  const response = await request("/api/v1/archive-keys", input);
  assert.equal(response.status, 201);
  const created = await response.json();
  assert.equal(created.token, "hck_secret_must_not_be_stored");
  assert.deepEqual(seen, [
    {
      name: "Staging consumer",
      owner: "Reporting team",
      requestsPerMinute: 30,
      archiveMegabytesPerMinute: 12,
    },
  ]);
  assert.match(response.headers.get("Cache-Control")!, /no-store/);
  assert.equal(store.page("operations", "", 50).items.length, 0);
  assert.equal(
    (
      await request(
        "/api/v1/archive-keys/AbCdEfGhIjKl",
        undefined,
        { "X-CSRF-Token": "" },
        "DELETE",
      )
    ).status,
    403,
  );
  assert.equal(
    (await request("/api/v1/archive-keys/invalid!", undefined, {}, "DELETE"))
      .status,
    400,
  );
  assert.equal(
    (
      await request(
        "/api/v1/archive-keys/AbCdEfGhIjKl",
        undefined,
        {},
        "DELETE",
      )
    ).status,
    204,
  );
  assert.deepEqual(seen[1], "AbCdEfGhIjKl");
  const audit = store.page("audit", "", 50).items;
  assert.deepEqual(
    audit.map((row: any) => row.action),
    ["archive_key_created", "archive_key_revoked"],
  );
  assert.doesNotMatch(JSON.stringify(audit), /hck_secret_must_not_be_stored/);
});
test("archive key service calls keep Jetstream paths and error codes", async (t) => {
  const calls: {
    method: string;
    path: string;
    authorization: string;
    body: string;
  }[] = [];
  const server = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    calls.push({
      method: req.method ?? "",
      path: req.url ?? "",
      authorization: req.headers.authorization ?? "",
      body: Buffer.concat(chunks).toString(),
    });
    res.setHeader("Content-Type", "application/json");
    if (req.method === "GET") {
      res.end(JSON.stringify({ keys: [] }));
    } else if (req.method === "POST") {
      res.statusCode = 400;
      res.end(JSON.stringify({ error: "invalid_input" }));
    } else {
      res.statusCode = 404;
      res.end(JSON.stringify({ error: "not_found" }));
    }
  }).listen(0, "127.0.0.1");
  await new Promise<void>((resolve) => server.once("listening", resolve));
  t.after(() => server.close());
  const address = server.address() as { port: number };
  const services = new Services(
    { url: "http://127.0.0.1:1", token: "relay-control-token" },
    {
      url: `http://127.0.0.1:${address.port}`,
      token: "jetstream-control-token",
    },
  );
  assert.deepEqual(await services.archiveKeys(), { keys: [] });
  const input = {
    name: "Consumer",
    owner: "Team",
    requestsPerMinute: 5,
    archiveMegabytesPerMinute: 10,
  };
  await assert.rejects(
    services.createArchiveKey(input),
    (error: unknown) =>
      error instanceof ApiError &&
      error.status === 400 &&
      error.code === "invalid_input",
  );
  await assert.rejects(
    services.revokeArchiveKey("AbCdEfGhIjKl"),
    (error: unknown) =>
      error instanceof ApiError &&
      error.status === 404 &&
      error.code === "not_found",
  );
  assert.deepEqual(
    calls.map(({ method, path, authorization }) => ({
      method,
      path,
      authorization,
    })),
    [
      {
        method: "GET",
        path: "/hypercerts/v1/archive-keys",
        authorization: "Bearer jetstream-control-token",
      },
      {
        method: "POST",
        path: "/hypercerts/v1/archive-keys",
        authorization: "Bearer jetstream-control-token",
      },
      {
        method: "DELETE",
        path: "/hypercerts/v1/archive-keys/AbCdEfGhIjKl",
        authorization: "Bearer jetstream-control-token",
      },
    ],
  );
  assert.deepEqual(JSON.parse(calls[1].body), input);
});
test("repository detail proxy keeps the private path, cursor and bearer boundary", async (t) => {
  const calls: { path: string; authorization: string }[] = [];
  const server = createServer((req, res) => {
    calls.push({
      path: req.url ?? "",
      authorization: req.headers.authorization ?? "",
    });
    res.setHeader("Content-Type", "application/json");
    res.end(
      JSON.stringify({
        job: {
          id: "0123456789abcdef0123456789abcdef",
          pds: "https://pds.example",
          policy: { revision: 1, collections: [] },
          reason: "backfill",
          state: "incomplete",
          completedRepos: 2,
          totalRepos: 3,
          totalReposKnown: true,
          attempts: 1,
          createdAt: "2026-09-15T00:00:00.000Z",
          coverage: "current_state",
          diagnostics: {
            execution: "stopped",
            unresolvedRepos: 1,
            retryingRepos: 0,
            maxRepositoryAttempts: 3,
          },
        },
        repositories: [],
        nextCursor: "",
      }),
    );
  }).listen(0, "127.0.0.1");
  await new Promise<void>((resolve) => server.once("listening", resolve));
  t.after(() => server.close());
  const address = server.address() as { port: number };
  const services = new Services(
    { url: "http://127.0.0.1:1", token: "relay-control-token" },
    {
      url: `http://127.0.0.1:${address.port}`,
      token: "jetstream-control-token",
    },
  );
  const page = await services.repositoryDetails(
    "0123456789abcdef0123456789abcdef",
    "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
    25,
  );
  assert.equal(page.job.state, "incomplete");
  assert.equal(page.job.diagnostics.execution, "stopped");
  assert.deepEqual(page.repositories, []);
  assert.equal(page.nextCursor, "");
  const query = new URLSearchParams({
    limit: "25",
    after: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
  });
  assert.deepEqual(calls, [
    {
      path: `/hypercerts/v1/jobs/0123456789abcdef0123456789abcdef/repositories?${query}`,
      authorization: "Bearer jetstream-control-token",
    },
  ]);
});
test("repository detail proxy preserves snapshot expiry and size error codes", async (t) => {
  const server = createServer((req, res) => {
    const expired = req.url?.includes("limit=25");
    res.statusCode = expired ? 410 : 413;
    res.setHeader("Content-Type", "application/json");
    res.end(
      JSON.stringify({
        error: expired
          ? "repository_snapshot_expired"
          : "repository_snapshot_too_large",
      }),
    );
  }).listen(0, "127.0.0.1");
  await new Promise<void>((resolve) => server.once("listening", resolve));
  t.after(() => server.close());
  const address = server.address() as { port: number };
  const services = new Services(
    { url: "http://127.0.0.1:1", token: "relay-control-token" },
    {
      url: `http://127.0.0.1:${address.port}`,
      token: "jetstream-control-token",
    },
  );
  await assert.rejects(
    services.repositoryDetails("0123456789abcdef0123456789abcdef", "opaque", 25),
    (error: unknown) => {
      assert.ok(error instanceof ApiError);
      assert.equal(error.status, 410);
      assert.equal(error.code, "repository_snapshot_expired");
      return true;
    },
  );
  await assert.rejects(
    services.repositoryDetails("0123456789abcdef0123456789abcdef", "", 26),
    (error: unknown) => {
      assert.ok(error instanceof ApiError);
      assert.equal(error.status, 413);
      assert.equal(error.code, "repository_snapshot_too_large");
      return true;
    },
  );
});
test("administration repository detail route validates and proxies a read-only page", async (t) => {
  const { request, services, store } = await fixture(t);
  const seen: unknown[] = [];
  services.repositoryDetails = async (id, after = "", limit = 100) => {
    seen.push([id, after, limit]);
    return {
      job: {
        id,
        pds: "https://pds.example",
        policy: { revision: 1, collections: [] },
        reason: "backfill",
        state: "incomplete",
        completedRepos: 2,
        totalRepos: 3,
        totalReposKnown: true,
        attempts: 1,
        createdAt: "2026-09-15T00:00:00.000Z",
        coverage: "current_state",
        diagnostics: {
          execution: "stopped",
          unresolvedRepos: 1,
          retryingRepos: 0,
          maxRepositoryAttempts: 3,
        },
      },
      repositories: [
        {
          did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
          listedRevision: "3l3qo2vutsw2b",
          state: "unresolved",
          attempts: 3,
          failure: {
            category: "http",
            httpStatus: 503,
            stage: "getRepo/request",
            code: "source_unavailable",
          },
        },
      ],
      nextCursor: "",
    };
  };
  const id = "0123456789abcdef0123456789abcdef";
  const after = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
  const response = await request(
    `/api/v1/jobs/${id}/repositories?limit=25&after=${encodeURIComponent(after)}`,
  );
  assert.equal(response.status, 200);
  const page = await response.json();
  assert.equal(page.repositories[0].failure.category, "http");
  assert.equal(page.job.state, "incomplete");
  assert.equal(page.job.diagnostics.execution, "stopped");
  assert.equal(page.repositories[0].failure.httpStatus, 503);
  assert.deepEqual(seen, [[id, after, 25]]);
  assert.equal(store.page("operations", "", 50).items.length, 0);
  assert.equal(
    (await request("/api/v1/jobs/not-a-job/repositories")).status,
    400,
  );
  assert.equal(
    (await request(`/api/v1/jobs/${id}/repositories?limit=101`)).status,
    400,
  );
});
test("T10 coverage preserves current-state limits, unknown historical provenance and Jetstream reason", async (t) => {
  const { request, services } = await fixture(t);
  services.coverage = async () => ({
    items: [
      {
        pds: "https://pds.example",
        policy: { revision: 2, collections: ["app.bsky.feed.post"] },
        jobId: "coverage-job",
        reason: "quota_recovery",
        state: "incomplete",
        completedRepos: 3,
        totalRepos: 10,
        totalReposKnown: true,
        errorCode: "source_unavailable",
        createdAt: "2026-09-15T00:00:00.000Z",
        coverage: "current_state",
        diagnostics: {
          execution: "stopped",
          unresolvedRepos: 7,
          retryingRepos: 0,
          maxRepositoryAttempts: 3,
        },
      },
    ],
    nextCursor: "next-page",
  });

  const response = await request("/api/v1/coverage");
  assert.equal(response.status, 200);
  const page = await response.json();
  assert.equal(page.items[0].reason, "quota_recovery");
  assert.equal(page.items[0].errorCode, "source_unavailable");
  assert.equal(page.items[0].coverage, "current_state");
  assert.equal(page.items[0].historicalPDSAttribution, "unknown");
  assert.equal(page.items[0].diagnostics.unresolvedRepos, 7);
  assert.equal(page.items[0].diagnostics.maxRepositoryAttempts, 3);
  assert.equal(page.next, "next-page");
});
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
test("session exposes only the signed-in profile and keeps authorization bound to the DID", async (t) => {
  const { store, request } = await fixture(t);
  let response = await request("/api/v1/session");
  let session = await response.json();
  assert.equal(session.did, did);
  assert.equal(session.displayName, null);
  assert.equal(session.handle, null);
  store.set("administrator-profile", did, {
    displayName: "Test Operator",
    handle: "operator.example",
    did: "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
    csrf: "profile-must-not-override-session",
    unexpected: "must-not-be-exposed",
  });
  response = await request("/api/v1/session");
  session = await response.json();
  assert.deepEqual(
    Object.keys(session).sort((a, b) => a.localeCompare(b)),
    ["csrf", "did", "displayName", "expires", "handle"],
  );
  assert.equal(session.did, did);
  assert.equal(session.csrf, "x".repeat(43));
  assert.equal(session.displayName, "Test Operator");
  assert.equal(session.handle, "operator.example");
  assert.match(response.headers.get("Cache-Control")!, /no-store/);
  assert.equal(
    (await request("/api/v1/session", undefined, { Cookie: "" })).status,
    401,
  );
  store.removeAdministrator(did);
  assert.equal((await request("/api/v1/session")).status, 401);
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
