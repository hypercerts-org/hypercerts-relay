import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { createServer, type IncomingMessage } from "node:http";
import { Store } from "../server/store.ts";
import { Worker } from "../server/worker.ts";
import { Services } from "../server/services.ts";
import { Auth, type OAuthProvider } from "../server/auth.ts";
import { createApp } from "../server/app.ts";
import { ApiError } from "../server/contracts.ts";
import {
  Collectiondir,
  CollectiondirError,
  collectiondirConfig,
} from "../server/collectiondir.ts";
import { CollectionCrawls } from "../server/collection-crawls.ts";

const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const token = "collectiondir-test-token-must-stay-private-0123456789";
const machineToken = "discovery-machine-token-for-crawl-tests-0123";
const oauth: OAuthProvider = {
  async authorize() {
    return new URL("https://pds.example/authorize");
  },
  async callback() {
    return { session: { did }, state: null };
  },
  async revoke() {},
};

interface DirectoryCall {
  method: string;
  path: string;
  authorization: string;
  body: string;
}
interface DirectoryReply {
  status?: number;
  body?: unknown;
  raw?: string;
  delayMs?: number;
}
type DirectoryHandler = (call: DirectoryCall) => DirectoryReply;

// A stand-in collection directory admin API on loopback.
async function stubDirectory(t: TestContext, handler: DirectoryHandler) {
  const calls: DirectoryCall[] = [];
  const state = { handler };
  const server = createServer(async (req: IncomingMessage, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    const call = {
      method: req.method ?? "",
      path: req.url ?? "",
      authorization: req.headers.authorization ?? "",
      body: Buffer.concat(chunks).toString(),
    };
    calls.push(call);
    const reply = state.handler(call);
    if (reply.delayMs)
      await new Promise((resolve) => setTimeout(resolve, reply.delayMs));
    res.statusCode = reply.status ?? 200;
    res.setHeader("Content-Type", "application/json");
    res.end(reply.raw ?? JSON.stringify(reply.body ?? {}));
  }).listen(0, "127.0.0.1");
  await new Promise<void>((resolve) => server.once("listening", resolve));
  t.after(() => {
    server.closeAllConnections();
    server.close();
  });
  const address = server.address() as { port: number };
  return {
    url: `http://127.0.0.1:${address.port}`,
    calls,
    setHandler(next: DirectoryHandler) {
      state.handler = next;
    },
  };
}

const crawlStart = "2026-11-17T10:00:00.123456789Z";
function emptyStatus(extra: Record<string, unknown> = {}) {
  return {
    host_starts: {},
    server_time: new Date().toISOString(),
    process_start: "2026-11-17T09:00:00Z",
    host_results: {},
    ...extra,
  };
}
// Answers status with no crawls and starts every submitted host.
const startingDirectory: DirectoryHandler = (call) => {
  if (call.path === "/admin/crawlStatus") return { body: emptyStatus() };
  const hosts = (JSON.parse(call.body) as { hosts: string[] }).hosts;
  return {
    body: {
      message: "ok",
      hosts: Object.fromEntries(
        hosts.map((host) => [host, { state: "started", start: crawlStart }]),
      ),
    },
  };
};
function postedHosts(calls: DirectoryCall[]) {
  return calls
    .filter((call) => call.path === "/admin/pds/requestCrawl")
    .flatMap((call) => (JSON.parse(call.body) as { hosts: string[] }).hosts);
}

function sourceView(hostname: string, overrides: Record<string, unknown> = {}) {
  return {
    HostID: 1,
    Hostname: hostname,
    NoSSL: false,
    DesiredState: "enabled",
    Revision: 1,
    Validation: { Status: "passed", Reason: "" },
    ...overrides,
  };
}

async function fixture(
  t: TestContext,
  directoryURL: string | null,
  relaySources: Record<string, ReturnType<typeof sourceView>> = {},
) {
  const store = new Store(":memory:");
  store.db.prepare("INSERT INTO administrators VALUES(?)").run(did);
  const session = "browser-session",
    csrf = "x".repeat(43);
  store.db
    .prepare("INSERT INTO sessions VALUES(?,?,?,?)")
    .run(
      createHash("sha256").update(session).digest("hex"),
      did,
      csrf,
      Date.now() + 60000,
    );
  const services = new Services(
    { url: "http://127.0.0.1:1", token: "unused-test-service-token" },
    { url: "http://127.0.0.1:1", token: "unused-test-service-token" },
  );
  const relayLookups: string[] = [];
  services.call = async <T>(...args: Parameters<Services["call"]>) => {
    const [service, path] = args;
    assert.equal(service, "relay");
    const pds = new URLSearchParams(path.split("?")[1] ?? "").get("pds")!;
    relayLookups.push(pds);
    const view = relaySources[pds];
    if (!view) throw new ApiError(404, "relay_rejected_404");
    return view as T;
  };
  services.policy = async () => ({ revision: 1, collections: [] });
  const collectiondir = new Collectiondir(
    directoryURL ? { url: directoryURL, token } : null,
  );
  const crawls = new CollectionCrawls(store, services, collectiondir);
  const worker = new Worker(store, services, crawls);
  const base = "http://127.0.0.1:3000";
  const server = createApp(
    store,
    services,
    new Auth(store, base, oauth),
    {},
    undefined,
    { collectionCrawls: crawls, machineToken },
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
  ) =>
    fetch(`http://127.0.0.1:${address.port}${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers: {
        "Content-Type": "application/json",
        Cookie: `relay_session=${session}`,
        Origin: base,
        "X-CSRF-Token": csrf,
        "Idempotency-Key": randomUUID(),
        ...headers,
      },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  const crawl = async (scope: string) => {
    const response = await request("/api/v1/operations", {
      kind: "collection_crawl",
      scope,
    });
    assert.equal(response.status, 202);
    const op = (await response.json()) as { id: string };
    await worker.tick();
    return store.operation(op.id)!;
  };
  const rows = () =>
    store.db
      .prepare("SELECT * FROM collection_crawls ORDER BY pds")
      .all() as Record<string, unknown>[];
  return {
    store,
    services,
    crawls,
    worker,
    request,
    crawl,
    rows,
    relayLookups,
  };
}

function manySources(count: number) {
  return Array.from({ length: count }, (_, i) =>
    sourceView(`pds${String(i).padStart(3, "0")}.example`),
  );
}

interface AuditEntry {
  action: string;
  actor: string;
  operation: string | null;
  detail: Record<string, unknown>;
}
function auditEntries(store: Store) {
  return store.page("audit", "", 100).items as unknown as AuditEntry[];
}

function rejectsWith(failure: string, status: number) {
  return (error: unknown) =>
    error instanceof CollectiondirError &&
    error.failure === failure &&
    error.status === status &&
    error.code === `collectiondir_${failure}` &&
    !error.message.includes(token);
}

// --- Collection directory client ---

test("collectiondir client authenticates fixed admin paths and maps per-host replies", async (t) => {
  const directory = await stubDirectory(t, (call) =>
    call.path === "/admin/crawlStatus"
      ? { body: emptyStatus() }
      : {
          body: {
            message: "ok",
            hosts: {
              "https://a.example": { state: "started", start: crawlStart },
              "https://b.example": {
                state: "already_active",
                start: "2026-11-17T09:58:00Z",
              },
            },
          },
        },
  );
  const client = new Collectiondir({ url: directory.url, token });
  assert.equal(client.configured, true);
  const status = await client.status();
  assert.equal(status.historySupported, true);
  assert.equal(status.processStartedAt, "2026-11-17T09:00:00Z");
  const submitted = await client.requestCrawl([
    "https://a.example",
    "https://b.example",
  ]);
  assert.deepEqual(submitted, [
    { pds: "https://a.example", outcome: "started", startedAt: crawlStart },
    {
      pds: "https://b.example",
      outcome: "already_active",
      startedAt: "2026-11-17T09:58:00Z",
    },
  ]);
  assert.deepEqual(
    directory.calls.map(({ method, path, authorization }) => ({
      method,
      path,
      authorization,
    })),
    [
      {
        method: "GET",
        path: "/admin/crawlStatus",
        authorization: `Bearer ${token}`,
      },
      {
        method: "POST",
        path: "/admin/pds/requestCrawl",
        authorization: `Bearer ${token}`,
      },
    ],
  );
  assert.deepEqual(JSON.parse(directory.calls[1].body), {
    hosts: ["https://a.example", "https://b.example"],
  });
});

test("collectiondir client submits in chunks of 100 hosts", async (t) => {
  const directory = await stubDirectory(t, startingDirectory);
  const client = new Collectiondir({ url: directory.url, token });
  const origins = Array.from(
    { length: 150 },
    (_, i) => `https://pds${i}.example`,
  );
  const submitted = await client.requestCrawl(origins);
  assert.equal(submitted.length, 150);
  assert.deepEqual(
    directory.calls.map(
      (call) => (JSON.parse(call.body) as { hosts: string[] }).hosts.length,
    ),
    [100, 50],
  );
});

test("collectiondir client classifies every failure without exposing the token", async (t) => {
  await assert.rejects(
    new Collectiondir(null).status(),
    rejectsWith("not_configured", 409),
  );
  await assert.rejects(
    new Collectiondir(null).requestCrawl(["https://pds.example"]),
    rejectsWith("not_configured", 409),
  );
  await assert.rejects(
    new Collectiondir({ url: "http://127.0.0.1:1", token }).status(),
    rejectsWith("unavailable", 503),
  );
  const directory = await stubDirectory(t, () => ({}));
  const client = new Collectiondir({ url: directory.url, token });
  const cases: [DirectoryReply, string, number][] = [
    [{ status: 500, body: { error: "InternalError" } }, "unavailable", 503],
    [{ status: 503 }, "unavailable", 503],
    [{ status: 401 }, "unauthorized", 502],
    [{ status: 403, body: { error: "AdminRequired" } }, "unauthorized", 502],
    [
      { status: 400, body: { error: "AdminAuthRequired" } },
      "unauthorized",
      502,
    ],
    [{ status: 400, body: { error: "BadRequest" } }, "rejected", 502],
    [{ status: 404, body: { message: "Not Found" } }, "rejected", 502],
    [{ body: { host_starts: [] } }, "invalid_response", 502],
    [
      { body: { server_time: "2026-11-17T10:00:00Z" } },
      "invalid_response",
      502,
    ],
    [{ raw: "<html>proxy error</html>" }, "invalid_response", 502],
    [{ raw: `"${"x".repeat(2_100_000)}"` }, "invalid_response", 502],
    [{ body: emptyStatus(), delayMs: 300 }, "unavailable", 503],
  ];
  for (const [reply, failure, status] of cases) {
    directory.setHandler(() => reply);
    await assert.rejects(client.status(100), rejectsWith(failure, status)); // NOSONAR: each case replaces the shared stub reply
  }
  directory.setHandler(() => ({
    body: { message: "ok", hosts: { "https://pds.example": { state: "odd" } } },
  }));
  await assert.rejects(
    client.requestCrawl(["https://pds.example"]),
    rejectsWith("invalid_response", 502),
  );
});

test("collectiondir client rejects non-HTTPS origins before any request", async (t) => {
  const directory = await stubDirectory(t, startingDirectory);
  const client = new Collectiondir({ url: directory.url, token });
  await Promise.all(
    [
      "http://pds.example",
      "https://pds.example/path",
      "pds.example",
      "https://user@pds.example",
    ].map((origin) =>
      assert.rejects(
        client.requestCrawl([origin]),
        (error: unknown) =>
          error instanceof ApiError && error.code === "invalid_crawl_origin",
      ),
    ),
  );
  assert.equal(directory.calls.length, 0);
});

test("an older collectiondir reply without hosts is reported as accepted", async (t) => {
  const directory = await stubDirectory(t, () => ({ body: { message: "ok" } }));
  const client = new Collectiondir({ url: directory.url, token });
  assert.deepEqual(await client.requestCrawl(["https://pds.example"]), [
    { pds: "https://pds.example", outcome: "accepted", startedAt: null },
  ]);
});

test("a reply with neither hosts nor the legacy acknowledgement is invalid", async (t) => {
  // Each reply gets its own stub directory, so the cases are independent.
  await Promise.all(
    [{}, { message: "queued" }, { error: "busy" }].map(async (body) => {
      const directory = await stubDirectory(t, () => ({ body }));
      const client = new Collectiondir({ url: directory.url, token });
      await assert.rejects(client.requestCrawl(["https://pds.example"]), {
        code: "collectiondir_invalid_response",
      });
    }),
  );
});

test("collectiondir status maps active and ended crawls and drops non-HTTPS hosts", async (t) => {
  const directory = await stubDirectory(t, () => ({
    body: {
      host_starts: {
        "https://a.example": {
          start: crawlStart,
          seen: 12,
          describe_errors: 1,
        },
        "http://127.0.0.1:8080": { start: crawlStart, seen: 1 },
      },
      server_time: "2026-11-17T10:01:00Z",
      process_start: "2026-11-17T09:00:00Z",
      host_results: {
        "https://b.example": {
          start: "2026-11-17T09:30:00Z",
          end: "2026-11-17T09:40:00Z",
          seen: 0,
          describe_errors: 0,
          state: "failed",
          error_category: "http",
          http_status: 502,
        },
        "https://c.example": {
          start: "2026-11-17T09:31:00Z",
          end: "2026-11-17T09:45:00Z",
          seen: 3,
          describe_errors: 0,
          state: "finished",
        },
      },
    },
  }));
  const status = await new Collectiondir({
    url: directory.url,
    token,
  }).status();
  assert.equal(status.serverTime, "2026-11-17T10:01:00Z");
  assert.deepEqual(status.active, [
    {
      pds: "https://a.example",
      startedAt: crawlStart,
      reposDescribed: 12,
      describeErrors: 1,
    },
  ]);
  assert.deepEqual(status.recent, [
    {
      pds: "https://c.example",
      startedAt: "2026-11-17T09:31:00Z",
      finishedAt: "2026-11-17T09:45:00Z",
      reposDescribed: 3,
      describeErrors: 0,
      state: "finished",
      errorCategory: null,
      httpStatus: null,
    },
    {
      pds: "https://b.example",
      startedAt: "2026-11-17T09:30:00Z",
      finishedAt: "2026-11-17T09:40:00Z",
      reposDescribed: 0,
      describeErrors: 0,
      state: "failed",
      errorCategory: "http",
      httpStatus: 502,
    },
  ]);
  directory.setHandler(() => ({
    body: {
      host_starts: { "https://a.example": { start: crawlStart, seen: 2 } },
      server_time: "2026-11-17T10:01:00Z",
    },
  }));
  const legacy = await new Collectiondir({
    url: directory.url,
    token,
  }).status();
  assert.equal(legacy.historySupported, false);
  assert.equal(legacy.processStartedAt, null);
  assert.deepEqual(legacy.recent, []);
  assert.equal(legacy.active[0].describeErrors, null);
});

test("collectiondir configuration requires both settings and a distinct private origin", () => {
  assert.equal(
    collectiondirConfig({}, () => "unused"),
    null,
  );
  for (const env of [
    { COLLECTIONDIR_CONTROL_URL: "https://collectiondir.example" },
    { COLLECTIONDIR_CONTROL_TOKEN_FILE: "/run/secrets/collectiondir" },
  ])
    assert.throws(
      () => collectiondirConfig(env, () => token),
      /must be set together/,
    );
  const read: string[] = [];
  assert.deepEqual(
    collectiondirConfig(
      {
        COLLECTIONDIR_CONTROL_URL: "https://collectiondir.example",
        COLLECTIONDIR_CONTROL_TOKEN_FILE: "/run/secrets/collectiondir",
      },
      (name) => {
        read.push(name);
        return token;
      },
    ),
    { url: "https://collectiondir.example", token },
  );
  assert.deepEqual(read, ["COLLECTIONDIR_CONTROL_TOKEN_FILE"]);
  for (const url of [
    "http://collectiondir.example",
    "https://collectiondir.example/admin",
    "https://user:pass@collectiondir.example",
    // Plain HTTP is only allowed on Railway's private network; this case
    // checks it is refused without that opt-in.
    "http://collectiondir.railway.internal:2510", // NOSONAR
  ])
    assert.throws(() => new Collectiondir({ url, token }), /requires HTTPS/);
  assert.equal(
    new Collectiondir(
      // The private-network opt-in is what permits plain HTTP here.
      { url: "http://collectiondir.railway.internal:2510", token }, // NOSONAR
      { railwayPrivateNetwork: true },
    ).configured,
    true,
  );
  for (const forbidden of [
    // A forbidden Relay control origin is plain HTTP on the private network.
    "http://relay.railway.internal:2471", // NOSONAR
    "https://rainbow.example",
  ])
    assert.throws(
      () =>
        new Collectiondir(
          { url: forbidden, token },
          { railwayPrivateNetwork: true, forbiddenOrigins: [forbidden] },
        ),
      /not a Relay, Rainbow or Jetstream origin/,
    );
});

// --- Journal command, records, routes and reconciliation ---

test("crawl requests require an administrator session, CSRF and Origin", async (t) => {
  const directory = await stubDirectory(t, startingDirectory);
  const { request, store } = await fixture(t, directory.url, {
    "https://pds.example": sourceView("pds.example"),
  });
  const body = { kind: "collection_crawl", scope: "https://pds.example" };
  await Promise.all(
    (
      [
        [{ Cookie: "" }, 401],
        [{ "X-CSRF-Token": "" }, 403],
        [{ Origin: "https://evil.example" }, 403],
      ] as [Record<string, string>, number][]
    ).map(async ([headers, status]) =>
      assert.equal(
        (await request("/api/v1/operations", body, headers)).status,
        status,
        JSON.stringify(headers),
      ),
    ),
  );
  await Promise.all(
    [
      "/api/v1/collection-directory/status",
      "/api/v1/collection-directory/crawls",
    ].map(async (path) =>
      assert.equal(
        (await request(path, undefined, { Cookie: "" })).status,
        401,
      ),
    ),
  );
  store.db.prepare("DELETE FROM administrators WHERE did=?").run(did);
  assert.equal((await request("/api/v1/operations", body)).status, 401);
  assert.equal(store.page("operations", "", 50).items.length, 0);
  assert.equal(directory.calls.length, 0);
});

test("the PDS discovery machine API cannot request a crawl", async (t) => {
  const directory = await stubDirectory(t, startingDirectory);
  const { request, store, relayLookups } = await fixture(t, directory.url, {
    "https://pds.example": sourceView("pds.example"),
  });
  await Promise.all(
    ["https://pds.example", "all"].map(async (scope) => {
      const response = await request(
        "/api/v1/sources/discovery/operations",
        { kind: "collection_crawl", scope },
        { Cookie: "", Authorization: `Bearer ${machineToken}` },
      );
      assert.equal(response.status, 403);
      assert.deepEqual(await response.json(), {
        error: "machine_command_not_permitted",
      });
    }),
  );
  assert.equal(store.page("operations", "", 50).items.length, 0);
  assert.deepEqual(relayLookups, []);
  assert.equal(directory.calls.length, 0);
});

test("a single enabled source is crawled at Relay's stored HTTPS origin and recorded", async (t) => {
  const directory = await stubDirectory(t, startingDirectory);
  const { crawl, rows, store, relayLookups } = await fixture(t, directory.url, {
    "https://pds.example": sourceView("pds.example"),
  });
  const op = await crawl("https://PDS.Example");
  assert.equal(op.state, "applied", op.error ?? "");
  assert.deepEqual(relayLookups, ["https://pds.example"]);
  assert.deepEqual(postedHosts(directory.calls), ["https://pds.example"]);
  assert.deepEqual(op.result, {
    collectiondir: {
      submitted: 1,
      started: 1,
      alreadyActive: 0,
      alreadySubmitted: 0,
      skipped: { not_enabled: 0, not_https: 0 },
    },
  });
  const [row] = rows();
  assert.equal(row.pds, "https://pds.example");
  assert.equal(row.operation, op.id);
  assert.equal(row.actor, did);
  assert.equal(row.cause, "manual");
  assert.equal(row.submission, "started");
  assert.equal(row.state, "accepted");
  assert.equal(row.crawlStartedAt, crawlStart);
  assert.equal(row.requestedAt, op.createdAt);
  const accepted = auditEntries(store).filter(
    (entry) => entry.action === "collection_crawl_accepted",
  );
  assert.equal(accepted.length, 1);
  assert.equal(accepted[0].actor, did);
  assert.equal(accepted[0].operation, op.id);
  assert.deepEqual(accepted[0].detail, {
    pds: "https://pds.example",
    submission: "started",
  });
});

test("unknown, disabled, unvalidated and plaintext sources fail without contacting the directory", async (t) => {
  const directory = await stubDirectory(t, startingDirectory);
  const { crawl, rows } = await fixture(t, directory.url, {
    "https://disabled.example": sourceView("disabled.example", {
      DesiredState: "disabled",
    }),
    "https://unvalidated.example": sourceView("unvalidated.example", {
      Validation: { Status: "failed", Reason: "host_check_failed" },
    }),
    "https://plaintext.example": sourceView("plaintext.example", {
      NoSSL: true,
    }),
  });
  for (const [scope, code] of [
    ["https://unknown.example", "source_not_approved"],
    ["https://disabled.example", "source_not_enabled"],
    ["https://unvalidated.example", "source_not_enabled"],
    ["https://plaintext.example", "source_not_https"],
  ]) {
    const op = await crawl(scope); // NOSONAR: the worker applies one operation per tick
    assert.equal(op.state, "failed", scope);
    assert.equal(op.error, code, scope);
  }
  assert.equal(directory.calls.length, 0);
  assert.equal(rows().length, 0);
});

test("crawl all submits every eligible source across Relay pages", async (t) => {
  const directory = await stubDirectory(t, startingDirectory);
  const { crawl, rows, services } = await fixture(t, directory.url);
  const pages: Record<string, unknown> = {
    "": {
      Sources: [
        sourceView("a.example"),
        sourceView("off.example", { DesiredState: "disabled" }),
        sourceView("plain.example", { NoSSL: true }),
      ],
      NextAfterHostID: 3,
    },
    "3": {
      Sources: [
        sourceView("b.example"),
        sourceView("pending.example", {
          Validation: { Status: "pending", Reason: "" },
        }),
      ],
      NextAfterHostID: 0,
    },
  };
  const requested: string[] = [];
  services.sources = async (after = "") => {
    requested.push(after);
    return pages[after] as Record<string, unknown>;
  };
  const op = await crawl("all");
  assert.equal(op.state, "applied", op.error ?? "");
  assert.deepEqual(requested, ["", "3"]);
  assert.deepEqual(postedHosts(directory.calls), [
    "https://a.example",
    "https://b.example",
  ]);
  assert.deepEqual(op.result, {
    collectiondir: {
      submitted: 2,
      started: 2,
      alreadyActive: 0,
      alreadySubmitted: 0,
      skipped: { not_enabled: 2, not_https: 1 },
    },
  });
  assert.deepEqual(
    rows().map((row) => row.pds),
    ["https://a.example", "https://b.example"],
  );
  services.sources = async () => ({
    Sources: [sourceView("off.example", { DesiredState: "disabled" })],
    NextAfterHostID: 0,
  });
  const none = await crawl("all");
  assert.equal(none.state, "failed");
  assert.equal(none.error, "no_enabled_sources");
});

test("an already running crawl is applied and recorded with the existing start", async (t) => {
  const existing = "2026-11-17T09:58:00Z";
  const directory = await stubDirectory(t, (call) =>
    call.path === "/admin/crawlStatus"
      ? { body: emptyStatus() }
      : {
          body: {
            message: "ok",
            hosts: {
              "https://pds.example": {
                state: "already_active",
                start: existing,
              },
            },
          },
        },
  );
  const { crawl, rows } = await fixture(t, directory.url, {
    "https://pds.example": sourceView("pds.example"),
  });
  const op = await crawl("https://pds.example");
  assert.equal(op.state, "applied");
  assert.equal(op.error, null);
  assert.equal(
    (op.result as { collectiondir: { alreadyActive: number } }).collectiondir
      .alreadyActive,
    1,
  );
  const [row] = rows();
  assert.equal(row.submission, "already_active");
  assert.equal(row.crawlStartedAt, existing);
});

test("an unavailable directory leaves the request incomplete and retryable", async (t) => {
  const directory = await stubDirectory(t, () => ({ status: 503 }));
  const { crawl, crawls, rows, request, worker, store } = await fixture(
    t,
    directory.url,
    { "https://pds.example": sourceView("pds.example") },
  );
  const op = await crawl("https://pds.example");
  assert.equal(op.state, "incomplete");
  assert.equal(op.error, "collectiondir_unavailable");
  assert.equal(rows().length, 0);
  assert.equal(postedHosts(directory.calls).length, 0);
  const statusResponse = await request("/api/v1/collection-directory/status");
  assert.equal(statusResponse.status, 200);
  const status = await statusResponse.json();
  assert.equal(status.configured, true);
  assert.equal(status.available, false);
  assert.equal(status.error, "collectiondir_unavailable");
  assert.deepEqual(status.active, []);
  const overall = await (await request("/api/v1/status")).json();
  assert.equal(overall.collectiondir, "unavailable");

  directory.setHandler(startingDirectory);
  assert.equal(
    (await request(`/api/v1/operations/${op.id}/retry`, {})).status,
    200,
  );
  await worker.tick();
  assert.equal(store.operation(op.id)!.state, "applied");
  assert.equal(rows().length, 1);
  // Re-applying the same operation (for example after a restart between
  // submission and recording) finds the crawl running and adds no rows.
  directory.setHandler((call) =>
    call.path === "/admin/crawlStatus"
      ? { body: emptyStatus() }
      : {
          body: {
            message: "ok",
            hosts: {
              "https://pds.example": {
                state: "already_active",
                start: crawlStart,
              },
            },
          },
        },
  );
  await crawls.apply(store.operation(op.id)!);
  assert.equal(rows().length, 1);
  assert.equal(rows()[0].submission, "started");
  assert.equal(
    auditEntries(store).filter(
      (entry) => entry.action === "collection_crawl_accepted",
    ).length,
    1,
  );
});

test("without configuration crawl requests are refused and other routes keep working", async (t) => {
  const { request, store, services } = await fixture(t, null);
  services.sources = async () => ({
    Sources: [sourceView("pds.example")],
    NextAfterHostID: 0,
  });
  services.coverageFor = async () => ({ items: [] });
  const response = await request("/api/v1/operations", {
    kind: "collection_crawl",
    scope: "all",
  });
  assert.equal(response.status, 409);
  assert.deepEqual(await response.json(), {
    error: "collectiondir_not_configured",
  });
  assert.equal(store.page("operations", "", 50).items.length, 0);
  const status = await (
    await request("/api/v1/collection-directory/status")
  ).json();
  assert.equal(status.configured, false);
  assert.equal(status.available, false);
  assert.equal(status.error, "collectiondir_not_configured");
  const overall = await (await request("/api/v1/status")).json();
  assert.equal(overall.collectiondir, "not_configured");
  const sources = await request("/api/v1/sources");
  assert.equal(sources.status, 200);
  assert.equal((await sources.json()).Sources[0].Hostname, "pds.example");
  assert.equal(
    (
      await request("/api/v1/operations", {
        kind: "source",
        pds: "https://pds.example",
        state: "disabled",
      })
    ).status,
    202,
  );
});

test("reconciliation records progress, completion, failure and unknown outcomes", async (t) => {
  let statusBody: Record<string, unknown> = emptyStatus();
  const directory = await stubDirectory(t, (call) =>
    call.path === "/admin/crawlStatus"
      ? { body: statusBody }
      : startingDirectory(call),
  );
  const sources = Object.fromEntries(
    ["running", "done", "broken", "lost", "evicted"].map((name) => [
      `https://${name}.example`,
      sourceView(`${name}.example`),
    ]),
  );
  const { crawl, crawls, rows, request, store } = await fixture(
    t,
    directory.url,
    sources,
  );
  for (const origin of Object.keys(sources)) await crawl(origin); // NOSONAR: the worker applies one operation per tick
  const byPDS = () =>
    Object.fromEntries(rows().map((row) => [row.pds as string, row]));
  let now = Date.now();

  // An unreachable directory leaves every row accepted.
  directory.setHandler(() => ({ status: 503 }));
  await crawls.reconcile(now);
  assert.ok(rows().every((row) => row.state === "accepted"));

  directory.setHandler((call) =>
    call.path === "/admin/crawlStatus"
      ? { body: statusBody }
      : startingDirectory(call),
  );
  statusBody = emptyStatus({
    host_starts: {
      "https://running.example": {
        start: crawlStart,
        seen: 12,
        describe_errors: 1,
      },
    },
    host_results: {
      "https://done.example": {
        start: crawlStart,
        end: "2026-11-17T10:05:00Z",
        seen: 40,
        describe_errors: 2,
        state: "finished",
      },
      "https://broken.example": {
        start: crawlStart,
        end: "2026-11-17T10:00:01Z",
        seen: 0,
        describe_errors: 0,
        state: "failed",
        error_category: "http",
        http_status: 502,
      },
    },
  });
  // Throttled: a second call within five seconds does not query again.
  const before = directory.calls.length;
  await crawls.reconcile(now + 1000);
  assert.equal(directory.calls.length, before);
  now += 5000;
  await crawls.reconcile(now);
  let current = byPDS();
  assert.equal(current["https://running.example"].state, "accepted");
  assert.equal(current["https://running.example"].reposDescribed, 12);
  assert.equal(current["https://running.example"].describeErrors, 1);
  assert.equal(current["https://done.example"].state, "finished");
  assert.equal(current["https://done.example"].reposDescribed, 40);
  assert.equal(current["https://done.example"].describeErrors, 2);
  assert.equal(
    current["https://done.example"].finishedAt,
    "2026-11-17T10:05:00Z",
  );
  assert.equal(current["https://broken.example"].state, "failed");
  assert.equal(current["https://broken.example"].errorCategory, "http");
  assert.equal(current["https://broken.example"].httpStatus, 502);
  // Not yet old enough to call missing.
  assert.equal(current["https://lost.example"].state, "accepted");

  // The directory restarted after the crawl began.
  statusBody = emptyStatus({
    process_start: "2026-11-17T11:00:00Z",
    host_starts: {
      "https://running.example": { start: crawlStart, seen: 13 },
    },
  });
  now += 5000;
  await crawls.reconcile(now);
  current = byPDS();
  assert.equal(current["https://lost.example"].state, "unknown");
  assert.equal(
    current["https://lost.example"].errorCategory,
    "collectiondir_restarted",
  );
  assert.equal(current["https://running.example"].state, "accepted");

  // Past the grace period, a result the directory no longer reports.
  statusBody = emptyStatus();
  now += 31000;
  await crawls.reconcile(now);
  current = byPDS();
  assert.equal(current["https://running.example"].state, "unknown");
  assert.equal(
    current["https://running.example"].errorCategory,
    "result_not_retained",
  );

  const actions = new Set(auditEntries(store).map((entry) => entry.action));
  for (const action of [
    "collection_crawl_finished",
    "collection_crawl_failed",
    "collection_crawl_unknown",
  ])
    assert.ok(actions.has(action), action);
  const failedAudit = auditEntries(store).find(
    (entry) => entry.action === "collection_crawl_failed",
  );
  assert.ok(failedAudit);
  assert.equal(failedAudit.actor, did);
  assert.equal(failedAudit.detail.pds, "https://broken.example");
  assert.equal(failedAudit.detail.errorCategory, "http");

  const page = await (
    await request(
      `/api/v1/collection-directory/crawls?pds=${encodeURIComponent("https://broken.example")}`,
    )
  ).json();
  assert.equal(page.items.length, 1);
  assert.equal(page.items[0].state, "failed");
  assert.equal(page.items[0].httpStatus, 502);
  assert.equal(page.next, null);
  const firstPage = await (
    await request("/api/v1/collection-directory/crawls?limit=2")
  ).json();
  assert.equal(firstPage.items.length, 2);
  const secondPage = await (
    await request(
      `/api/v1/collection-directory/crawls?limit=10&after=${encodeURIComponent(firstPage.next)}`,
    )
  ).json();
  assert.equal(secondPage.items.length, 3);
  assert.equal(
    new Set([...firstPage.items, ...secondPage.items].map((row) => row.id))
      .size,
    5,
  );
  assert.equal(
    (await request("/api/v1/collection-directory/crawls?after=bogus")).status,
    400,
  );
});

test("crawl history can refresh selected IDs with pagination and validates the filter", async (t) => {
  const directory = await stubDirectory(t, startingDirectory);
  const sources = {
    "https://a.example": sourceView("a.example"),
    "https://b.example": sourceView("b.example"),
    "https://c.example": sourceView("c.example"),
  };
  const { crawl, rows, request, store } = await fixture(
    t,
    directory.url,
    sources,
  );
  for (const origin of Object.keys(sources)) await crawl(origin); // NOSONAR: the worker applies one operation per tick
  const [first, , last] = rows();
  store.db
    .prepare("UPDATE collection_crawls SET state='finished' WHERE id=?")
    .run(String(first.id));
  const ids = `${first.id},${last.id}`;
  const firstPage = await (
    await request(
      `/api/v1/collection-directory/crawls?ids=${encodeURIComponent(ids)}&limit=1`,
    )
  ).json();
  assert.equal(firstPage.items.length, 1);
  assert.ok(firstPage.next);
  const secondPage = await (
    await request(
      `/api/v1/collection-directory/crawls?ids=${encodeURIComponent(ids)}&limit=1&after=${encodeURIComponent(firstPage.next)}`,
    )
  ).json();
  assert.equal(secondPage.items.length, 1);
  assert.equal(secondPage.next, null);
  const selected = [...firstPage.items, ...secondPage.items];
  assert.deepEqual(
    new Set(selected.map((row) => row.id)),
    new Set([first.id, last.id]),
  );
  assert.equal(selected.find((row) => row.id === first.id).state, "finished");
  const byPDS = await (
    await request(
      `/api/v1/collection-directory/crawls?ids=${encodeURIComponent(ids)}&pds=${encodeURIComponent(String(first.pds))}`,
    )
  ).json();
  assert.deepEqual(
    byPDS.items.map((row: { id: string }) => row.id),
    [first.id],
  );
  assert.deepEqual(
    await (
      await request(`/api/v1/collection-directory/crawls?ids=${randomUUID()}`)
    ).json(),
    { items: [], next: null },
  );
  for (const invalid of [
    "",
    "not-a-uuid",
    `${first.id},`,
    Array(101).fill(first.id).join(","),
  ])
    assert.equal(
      (
        await request(
          `/api/v1/collection-directory/crawls?ids=${encodeURIComponent(invalid)}`,
        )
      ).status,
      400,
    );
  assert.equal(
    (await (await request("/api/v1/collection-directory/crawls")).json()).items
      .length,
    3,
  );
});

test("a directory without status history marks unseen crawls as unsupported", async (t) => {
  const directory = await stubDirectory(t, (call) =>
    call.path === "/admin/crawlStatus"
      ? {
          body: {
            host_starts: {},
            server_time: new Date().toISOString(),
          },
        }
      : { body: { message: "ok" } },
  );
  const { crawl, crawls, rows } = await fixture(t, directory.url, {
    "https://pds.example": sourceView("pds.example"),
  });
  await crawl("https://pds.example");
  assert.equal(rows()[0].submission, "accepted");
  await crawls.reconcile(Date.now() + 31000);
  assert.equal(rows()[0].state, "unknown");
  assert.equal(rows()[0].errorCategory, "status_history_unsupported");
});

test("the collection directory token never appears in responses or audit history", async (t) => {
  const directory = await stubDirectory(t, (call) =>
    call.path === "/admin/crawlStatus"
      ? {
          body: emptyStatus({
            host_starts: {
              "https://pds.example": { start: crawlStart, seen: 1 },
            },
          }),
        }
      : startingDirectory(call),
  );
  const { crawl, crawls, request } = await fixture(t, directory.url, {
    "https://pds.example": sourceView("pds.example"),
  });
  await crawl("https://pds.example");
  await crawls.reconcile();
  const bodies = await Promise.all(
    [
      "/api/v1/collection-directory/status",
      "/api/v1/collection-directory/crawls",
      "/api/v1/status",
      "/api/v1/operations",
      "/api/v1/audit",
    ].map(async (path) => (await request(path)).text()),
  );
  directory.setHandler(() => ({ status: 401 }));
  // The status reads above filled the short status cache; a reconcile past
  // its throttle reads the directory again and caches the 401 outcome.
  await crawls.reconcile(Date.now() + 60_000);
  const rejected = await (
    await request("/api/v1/collection-directory/status")
  ).text();
  assert.equal(JSON.parse(rejected).error, "collectiondir_unauthorized");
  bodies.push(rejected);
  for (const body of bodies) assert.doesNotMatch(body, new RegExp(token));
  assert.ok(
    directory.calls.every((call) => call.authorization === `Bearer ${token}`),
  );
});

test("crawl all records accepted chunks before a later chunk fails", async (t) => {
  let posts = 0;
  const seen = new Set<string>();
  // The first chunk is accepted; the second fails until the directory
  // recovers. The first chunk finishes before the operation is retried.
  let failSecond = true;
  const directory = await stubDirectory(t, (call) => {
    if (call.path === "/admin/crawlStatus")
      return {
        body: emptyStatus({
          host_results: Object.fromEntries(
            [...seen].map((host) => [
              host,
              {
                start: crawlStart,
                end: "2026-11-17T10:01:00Z",
                seen: 1,
                state: "finished",
              },
            ]),
          ),
        }),
      };
    posts++;
    if (failSecond && posts === 2) return { status: 503 };
    const hosts = (JSON.parse(call.body) as { hosts: string[] }).hosts;
    const reply = Object.fromEntries(
      hosts.map((host) => [
        host,
        {
          state: "started",
          start: crawlStart,
        },
      ]),
    );
    for (const host of hosts) seen.add(host);
    return { body: { message: "ok", hosts: reply } };
  });
  const { crawl, crawls, rows, services, store, request, worker } =
    await fixture(t, directory.url);
  services.sources = async () => ({
    Sources: manySources(150),
    NextAfterHostID: 0,
  });
  // Counted in SQL: the audit page holds at most 100 entries.
  const accepted = () =>
    (
      store.db
        .prepare(
          "SELECT COUNT(*) AS n FROM audit WHERE action='collection_crawl_accepted' AND operation=?",
        )
        .get(op.id) as { n: number }
    ).n;
  const op = await crawl("all");
  assert.equal(op.state, "incomplete");
  assert.equal(op.error, "collectiondir_unavailable");
  assert.equal(rows().length, 100);
  assert.equal(accepted(), 100);
  assert.ok(rows().every((row) => row.operation === op.id));

  await crawls.reconcile();
  assert.ok(rows().every((row) => row.state === "finished"));
  failSecond = false;
  const callsBeforeRetry = directory.calls.length;
  assert.equal(
    (await request(`/api/v1/operations/${op.id}/retry`, {})).status,
    200,
  );
  await worker.tick();
  const retried = store.operation(op.id)!;
  assert.equal(retried.state, "applied", retried.error ?? "");
  assert.deepEqual(
    postedHosts(directory.calls.slice(callsBeforeRetry)),
    manySources(150)
      .slice(100)
      .map((source) => `https://${source.Hostname}`),
  );
  assert.deepEqual(retried.result, {
    collectiondir: {
      submitted: 50,
      started: 50,
      alreadyActive: 0,
      alreadySubmitted: 100,
      skipped: { not_enabled: 0, not_https: 0 },
    },
  });
  assert.equal(rows().length, 150);
  assert.equal(accepted(), 150);
  assert.equal(
    new Set(rows().map((row) => row.pds)).size,
    150,
    "no source recorded twice",
  );
  assert.ok(
    rows()
      .slice(0, 100)
      .every((row) => row.submission === "started"),
  );
});

test("recovered applying crawls reuse recorded sources and skip directory calls when complete", async (t) => {
  let posts = 0;
  const directory = await stubDirectory(t, (call) => {
    if (call.path === "/admin/pds/requestCrawl" && ++posts === 2)
      return { status: 503 };
    return startingDirectory(call);
  });
  const { crawls, rows, services, store, worker } = await fixture(
    t,
    directory.url,
  );
  const origins = manySources(150).map(
    (source) => `https://${source.Hostname}`,
  );
  services.sources = async () => ({
    Sources: manySources(150),
    NextAfterHostID: 0,
  });
  const op = store.request(did, { kind: "collection_crawl", scope: "all" });
  store.transition(op.id, "applying");
  // Stop before the Worker can finalize the interrupted operation.
  await assert.rejects(crawls.apply(store.operation(op.id)!), {
    code: "collectiondir_unavailable",
  });
  assert.equal(store.operation(op.id)?.state, "applying");
  assert.equal(rows().length, 100);

  const callsBeforeRecovery = directory.calls.length;
  const recovered = new Worker(store, services, crawls);
  recovered.recover();
  assert.equal(store.operation(op.id)?.state, "requested");
  await recovered.tick();
  assert.equal(store.operation(op.id)?.state, "applied");
  assert.equal(rows().length, 150);
  assert.deepEqual(
    postedHosts(directory.calls.slice(callsBeforeRecovery)),
    origins.slice(100),
  );
  assert.deepEqual(store.operation(op.id)?.result, {
    collectiondir: {
      submitted: 50,
      started: 50,
      alreadyActive: 0,
      alreadySubmitted: 100,
      skipped: { not_enabled: 0, not_https: 0 },
    },
  });

  // Recovery after every chunk was recorded needs no status probe, even
  // when the directory is unavailable.
  store.transition(op.id, "applying");
  directory.setHandler(() => ({ status: 503 }));
  const callsBeforeCompleteRecovery = directory.calls.length;
  worker.recover();
  await worker.tick();
  assert.equal(store.operation(op.id)?.state, "applied");
  assert.equal(directory.calls.length, callsBeforeCompleteRecovery);
  assert.equal(rows().length, 150);
  assert.deepEqual(store.operation(op.id)?.result, {
    collectiondir: {
      submitted: 0,
      started: 0,
      alreadyActive: 0,
      alreadySubmitted: 150,
      skipped: { not_enabled: 0, not_https: 0 },
    },
  });
});

test("accepted crawls close as unknown once the directory is no longer configured", async (t) => {
  const directory = await stubDirectory(t, startingDirectory);
  const { crawl, rows, store, services } = await fixture(t, directory.url, {
    "https://pds.example": sourceView("pds.example"),
  });
  await crawl("https://pds.example");
  assert.equal(rows()[0].state, "accepted");
  const unconfigured = new CollectionCrawls(
    store,
    services,
    new Collectiondir(null),
  );
  await unconfigured.reconcile();
  assert.equal(rows()[0].state, "unknown");
  assert.equal(rows()[0].errorCategory, "collectiondir_not_configured");
  const audit = auditEntries(store).filter(
    (entry) => entry.action === "collection_crawl_unknown",
  );
  assert.equal(audit.length, 1);
  assert.equal(audit[0].actor, did);
  assert.equal(audit[0].detail.errorCategory, "collectiondir_not_configured");
  const calls = directory.calls.length;
  const auditCount = auditEntries(store).length;
  await unconfigured.reconcile(Date.now() + 10_000);
  assert.equal(auditEntries(store).length, auditCount);
  assert.equal(directory.calls.length, calls);
});

test("reconciliation sees every active crawl while the browser list is capped", async (t) => {
  const origins = manySources(1001).map(
    (source) => `https://${source.Hostname}`,
  );
  // The oldest crawl sorts last, beyond the browser's 1,000 entries.
  const starts = origins.map((_, i) =>
    new Date(Date.UTC(2026, 10, 17, 10, 0, 1001 - i)).toISOString(),
  );
  const target = origins[1000];
  const directory = await stubDirectory(t, (call) =>
    call.path === "/admin/crawlStatus"
      ? {
          body: emptyStatus({
            host_starts: Object.fromEntries(
              origins.map((pds, i) => [pds, { start: starts[i], seen: i }]),
            ),
          }),
        }
      : {
          body: {
            message: "ok",
            hosts: {
              [target]: { state: "already_active", start: starts[1000] },
            },
          },
        },
  );
  const { crawl, crawls, rows, request } = await fixture(t, directory.url, {
    [target]: sourceView(new URL(target).hostname),
  });
  await crawl(target);
  await crawls.reconcile(Date.now() + 60_000);
  assert.equal(rows()[0].state, "accepted");
  assert.equal(rows()[0].reposDescribed, 1000);
  const status = await (
    await request("/api/v1/collection-directory/status")
  ).json();
  assert.equal(status.active.length, 1000);
  assert.ok(
    !status.active.some((entry: { pds: string }) => entry.pds === target),
  );
});

test("status reads share a short cache and the overview probe is bounded", async (t) => {
  const directory = await stubDirectory(t, () => ({ body: emptyStatus() }));
  const { request } = await fixture(t, directory.url);
  for (let i = 0; i < 3; i++) {
    const response = await request("/api/v1/collection-directory/status"); // NOSONAR: sequential reads exercise the status cache
    assert.equal((await response.json()).available, true); // NOSONAR: completes this read before the next
  }
  assert.equal(
    directory.calls.filter((call) => call.path === "/admin/crawlStatus").length,
    1,
  );

  const slow = await stubDirectory(t, () => ({
    body: emptyStatus(),
    delayMs: 2500,
  }));
  const { request: slowRequest } = await fixture(t, slow.url);
  const started = Date.now();
  const overall = await (await slowRequest("/api/v1/status")).json();
  assert.ok(Date.now() - started < 2000, "overview waited for the directory");
  assert.equal(overall.collectiondir, "unavailable");
});

test("reconciliation surfaces database failures instead of swallowing them", async (t) => {
  let statusBody: Record<string, unknown> = emptyStatus();
  const directory = await stubDirectory(t, (call) =>
    call.path === "/admin/crawlStatus"
      ? { body: statusBody }
      : startingDirectory(call),
  );
  const { crawl, crawls, store } = await fixture(t, directory.url, {
    "https://pds.example": sourceView("pds.example"),
  });
  await crawl("https://pds.example");
  statusBody = emptyStatus({
    host_results: {
      "https://pds.example": {
        start: crawlStart,
        end: "2026-11-17T10:05:00Z",
        seen: 1,
        describe_errors: 0,
        state: "finished",
      },
    },
  });
  store.transaction = () => {
    throw new Error("disk I/O error");
  };
  await assert.rejects(crawls.reconcile(Date.now()), /disk I\/O error/);
});
