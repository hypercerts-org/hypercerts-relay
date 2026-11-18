import { test, type TestContext } from "node:test";
import assert from "node:assert/strict";
import { randomUUID, createHash } from "node:crypto";
import { createServer, type IncomingMessage } from "node:http";
import { z } from "zod";
import { Store } from "../server/store.ts";
import { Worker } from "../server/worker.ts";
import { Services } from "../server/services.ts";
import { Auth, type OAuthProvider } from "../server/auth.ts";
import { createApp } from "../server/app.ts";
import {
  ApiError,
  type CollectionBootstrapView,
  type CollectionCrawl,
  type Operation,
} from "../server/contracts.ts";
import { Collectiondir } from "../server/collectiondir.ts";
import { CollectionCrawls } from "../server/collection-crawls.ts";
import {
  CollectionBootstrap,
  bootstrapActor,
  bootstrapOperationId,
} from "../server/collection-bootstrap.ts";
import { discoveryActor } from "../server/machine.ts";

const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const token = "collectiondir-bootstrap-token-must-stay-private-01";
const machineToken = "discovery-machine-token-for-bootstrap-tests";
const oauth: OAuthProvider = {
  async authorize() {
    return new URL("https://pds.example/authorize");
  },
  async callback() {
    return { session: { did }, state: null };
  },
  async revoke() {},
};
const crawlStart = "2026-11-17T10:00:00.123456789Z";
const minute = 60_000;

interface DirectoryCall {
  path: string;
  authorization: string;
  body: string;
}
type DirectoryHandler = (call: DirectoryCall) => {
  status?: number;
  body?: unknown;
};

function emptyStatus(extra: Record<string, unknown> = {}) {
  return {
    host_starts: {},
    server_time: new Date().toISOString(),
    process_start: "2026-11-17T09:00:00Z",
    host_results: {},
    ...extra,
  };
}
function startingDirectory(state = "started"): DirectoryHandler {
  return (call) => {
    if (call.path === "/admin/crawlStatus") return { body: emptyStatus() };
    const hosts = (JSON.parse(call.body) as { hosts: string[] }).hosts;
    return {
      body: {
        message: "ok",
        hosts: Object.fromEntries(
          hosts.map((host) => [host, { state, start: crawlStart }]),
        ),
      },
    };
  };
}
const unavailableDirectory: DirectoryHandler = () => ({ status: 503 });

// A stand-in collection directory admin API on loopback.
async function stubDirectory(t: TestContext, handler: DirectoryHandler) {
  const calls: DirectoryCall[] = [];
  const state = { handler };
  const server = createServer(async (req: IncomingMessage, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(Buffer.from(chunk));
    const call = {
      path: req.url ?? "",
      authorization: req.headers.authorization ?? "",
      body: Buffer.concat(chunks).toString(),
    };
    calls.push(call);
    const reply = state.handler(call);
    res.statusCode = reply.status ?? 200;
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify(reply.body ?? {}));
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
    posts: () =>
      calls.filter((call) => call.path === "/admin/pds/requestCrawl"),
    setHandler(next: DirectoryHandler) {
      state.handler = next;
    },
  };
}

interface RelaySource {
  HostID: number;
  Hostname: string;
  NoSSL: boolean;
  DesiredState: string;
  Revision: number;
  Validation: { Status: string; Reason: string };
}

// A Relay source table with the revision rules the bootstrap relies on: an
// enable of an already enabled, validated source keeps its revision; every
// state change advances it. Validation is synchronous and can be rejected.
function stubRelay() {
  const sources = new Map<string, RelaySource>();
  const rejected = new Set<string>();
  let nextHostID = 1;
  return {
    sources,
    rejected,
    put(pds: string, state: string): RelaySource {
      if (state === "enabled" && rejected.has(pds))
        throw new ApiError(422, "relay_rejected_422");
      const url = new URL(pds);
      let source = sources.get(pds);
      if (!source) {
        source = {
          HostID: nextHostID++,
          Hostname: url.host,
          NoSSL: url.protocol === "http:",
          DesiredState: state,
          Revision: 1,
          Validation: { Status: "passed", Reason: "" },
        };
        sources.set(pds, source);
      } else if (source.DesiredState !== state) {
        source.DesiredState = state;
        source.Revision++;
      }
      return structuredClone(source);
    },
  };
}

async function fixture(t: TestContext, directoryURL: string | null) {
  const store = new Store(":memory:");
  store.db.prepare("INSERT INTO administrators VALUES(?)").run(did);
  store.set("administrator-profile", did, { handle: "operator.example" });
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
  const relay = stubRelay();
  services.call = async <T>(...args: Parameters<Services["call"]>) => {
    const [service, path, method = "GET", body] = args;
    assert.equal(service, "relay");
    if (method === "PUT" && path === "/source") {
      const input = body as { pds: string; state: string };
      return relay.put(input.pds, input.state) as T;
    }
    const pds = new URLSearchParams(path.split("?")[1] ?? "").get("pds")!;
    const view = relay.sources.get(pds);
    if (!view) throw new ApiError(404, "relay_rejected_404");
    return structuredClone(view) as T;
  };
  services.sources = async () => ({
    Sources: [...relay.sources.values()],
    NextAfterHostID: 0,
  });
  services.coverageFor = async () => ({ items: [] });
  const collectiondir = new Collectiondir(
    directoryURL ? { url: directoryURL, token } : null,
  );
  const crawls = new CollectionCrawls(store, services, collectiondir);
  const bootstrap = new CollectionBootstrap(store, collectiondir.configured);
  const worker = new Worker(store, services, crawls, bootstrap);
  const base = "http://127.0.0.1:3000";
  const server = createApp(
    store,
    services,
    new Auth(store, base, oauth),
    {},
    undefined,
    {
      collectionCrawls: crawls,
      collectionBootstrap: bootstrap,
      machineToken,
    },
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
  // Records a source operation without applying it.
  const requestSource = async (pds: string, state = "enabled") => {
    const response = await request("/api/v1/operations", {
      kind: "source",
      pds,
      state,
    });
    assert.equal(response.status, 202);
    return ((await response.json()) as Operation).id;
  };
  // Records and applies a source operation; follow-ups stay requested.
  const source = async (pds: string, state = "enabled") => {
    const id = await requestSource(pds, state);
    await worker.tick();
    return store.operation(id)!;
  };
  const operations = (kind?: string) =>
    (
      store.db
        .prepare("SELECT id FROM operations ORDER BY createdAt,id")
        .all() as { id: string }[]
    )
      .map((row) => store.operation(row.id)!)
      .filter((op) => !kind || op.command.kind === kind);
  const bootstrapRows = () =>
    store.db
      .prepare("SELECT * FROM collection_bootstraps ORDER BY createdAt")
      .all() as Record<string, unknown>[];
  const bootstraps = async (pds?: string) => {
    const response = await request(
      `/api/v1/collection-directory/bootstraps${
        pds ? `?pds=${encodeURIComponent(pds)}` : ""
      }`,
    );
    assert.equal(response.status, 200);
    return ((await response.json()) as { items: CollectionBootstrapView[] })
      .items;
  };
  const crawlRows = () =>
    store.db.prepare("SELECT * FROM collection_crawls").all() as Record<
      string,
      unknown
    >[];
  const audit = (action?: string) =>
    (
      store.db.prepare("SELECT * FROM audit ORDER BY seq").all() as {
        action: string;
        actor: string;
        operation: string | null;
        detail: string;
      }[]
    ).filter((row) => !action || row.action === action);
  return {
    store,
    services,
    relay,
    crawls,
    bootstrap,
    worker,
    request,
    requestSource,
    source,
    operations,
    bootstrapRows,
    bootstraps,
    crawlRows,
    audit,
  };
}

type CollectionDirectoryResult = {
  collectionDirectory?: { bootstrap: string; pds: string; operation?: string };
};
function bootstrapResult(op: Operation) {
  return (op.result as CollectionDirectoryResult).collectionDirectory;
}

test("bootstrap operation IDs are deterministic version 8 UUIDs", () => {
  const source = randomUUID();
  const id = bootstrapOperationId(source);
  assert.equal(id, bootstrapOperationId(source));
  assert.notEqual(id, bootstrapOperationId(randomUUID()));
  assert.ok(z.uuid().safeParse(id).success, id);
  assert.equal(id[14], "8");
  assert.ok("89ab".includes(id[19]));
});

test("an applied source enable queues one crawl of Relay's HTTPS origin", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { store, source, operations, bootstraps, crawlRows, audit, worker } =
    await fixture(t, directory.url);
  const op = await source("PDS.Example");
  assert.equal(op.state, "applied", op.error ?? "");
  const crawlId = bootstrapOperationId(op.id);
  assert.deepEqual(bootstrapResult(op), {
    bootstrap: "queued",
    pds: "https://pds.example",
    operation: crawlId,
  });
  const queued = operations("collection_crawl");
  assert.equal(queued.length, 1);
  assert.equal(queued[0].id, crawlId);
  assert.equal(queued[0].actor, did);
  assert.equal(queued[0].state, "requested");
  assert.deepEqual(queued[0].command, {
    kind: "collection_crawl",
    scope: "https://pds.example",
  });
  // The applied transition, the bootstrap row, the follow-up operation and
  // both audit entries are written together.
  const sourceAudit = audit().filter((row) => row.operation === op.id);
  assert.deepEqual(
    sourceAudit.map((row) => row.action),
    [
      "requested",
      "applying",
      "applying",
      "applied",
      "collection_bootstrap_queued",
    ],
  );
  assert.deepEqual(JSON.parse(sourceAudit[4].detail), {
    pds: "https://pds.example",
    crawlOperation: crawlId,
  });
  assert.deepEqual(
    audit()
      .filter((row) => row.operation === crawlId)
      .map((row) => [row.action, row.actor]),
    [["requested", did]],
  );
  const [waiting] = await bootstraps();
  assert.equal(waiting.phase, "waiting");

  await worker.tick();
  assert.equal(store.operation(crawlId)!.state, "applied");
  const posts = directory.posts();
  assert.equal(posts.length, 1);
  assert.deepEqual(JSON.parse(posts[0].body), {
    hosts: ["https://pds.example"],
  });
  assert.equal(posts[0].authorization, `Bearer ${token}`);
  const [row] = crawlRows();
  assert.equal(row.cause, "source_added");
  assert.equal(row.operation, crawlId);
  assert.equal(row.actor, did);
  // The source operation stays applied whatever happens to the crawl.
  assert.equal(store.operation(op.id)!.state, "applied");

  const [accepted] = await bootstraps();
  assert.equal(accepted.sourceOperation, op.id);
  assert.equal(accepted.actor, did);
  assert.equal(accepted.actorHandle, "operator.example");
  assert.equal(accepted.outcome, "queued");
  assert.equal(accepted.crawlOperation, crawlId);
  assert.equal(accepted.phase, "accepted");
  assert.equal(accepted.request?.state, "applied");
  assert.equal(accepted.crawl?.state, "accepted");
  assert.equal(accepted.crawl?.cause, "source_added");
  assert.equal(accepted.crawl?.sourceOperation, op.id);
});

test("the crawl list reports the source operation behind an initial crawl", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { source, worker, request } = await fixture(t, directory.url);
  const op = await source("https://pds.example");
  await worker.tick();
  const page = (await (
    await request("/api/v1/collection-directory/crawls")
  ).json()) as { items: CollectionCrawl[] };
  assert.equal(page.items.length, 1);
  assert.equal(page.items[0].cause, "source_added");
  assert.equal(page.items[0].sourceOperation, op.id);
});

test("failed admission and disable or remove queue nothing", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { relay, source, request, worker, store, operations, bootstrapRows } =
    await fixture(t, directory.url);
  relay.rejected.add("https://pds.example");
  const failed = await source("https://pds.example");
  assert.equal(failed.state, "incomplete");
  assert.equal(bootstrapRows().length, 0);
  assert.equal(operations("collection_crawl").length, 0);
  assert.equal(directory.calls.length, 0);

  relay.rejected.clear();
  assert.equal(
    (await request(`/api/v1/operations/${failed.id}/retry`, {})).status,
    200,
  );
  await worker.tick();
  assert.equal(store.operation(failed.id)!.state, "applied");
  assert.equal(bootstrapRows().length, 1);
  assert.equal(operations("collection_crawl").length, 1);
  // Apply the queued crawl so the next source operations run in turn.
  await worker.tick();

  for (const state of ["disabled", "removed"]) {
    const op = await source("https://pds.example", state); // NOSONAR: disable, then remove, one worker tick each
    assert.equal(op.state, "applied");
    assert.equal(bootstrapResult(op), undefined);
  }
  assert.equal(bootstrapRows().length, 1);
  assert.equal(operations("collection_crawl").length, 1);
});

test("a no-op enable does not re-crawl; disable then enable does", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { source, worker, operations, bootstrapRows, audit } = await fixture(
    t,
    directory.url,
  );
  const first = await source("https://pds.example");
  await worker.tick();
  const again = await source("https://pds.example");
  assert.equal(again.state, "applied");
  assert.deepEqual(bootstrapResult(again), {
    bootstrap: "unchanged",
    pds: "https://pds.example",
  });
  assert.equal(bootstrapRows().length, 1);
  assert.equal(operations("collection_crawl").length, 1);
  assert.equal(audit("collection_bootstrap_unchanged").length, 1);

  await source("https://pds.example", "disabled");
  const reenabled = await source("https://pds.example");
  assert.equal(bootstrapResult(reenabled)?.bootstrap, "queued");
  assert.equal(bootstrapRows().length, 2);
  assert.deepEqual(
    operations("collection_crawl").map((op) => op.id),
    [bootstrapOperationId(first.id), bootstrapOperationId(reenabled.id)],
  );
  await worker.tick();
  assert.equal(directory.posts().length, 2);
});

test("planning and committing the same operation twice records one bootstrap", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { store, bootstrap, source, operations, bootstrapRows, audit } =
    await fixture(t, directory.url);
  const op = await source("https://pds.example");
  const follow = bootstrap.plan(op, op.result)!;
  assert.equal(
    (follow.result as CollectionDirectoryResult).collectionDirectory?.bootstrap,
    "queued",
  );
  store.transaction(follow.commit);
  assert.equal(bootstrapRows().length, 1);
  assert.equal(operations("collection_crawl").length, 1);
  assert.equal(audit("collection_bootstrap_queued").length, 1);
});

test("an already running crawl is applied without a second submission", async (t) => {
  const directory = await stubDirectory(t, startingDirectory("already_active"));
  const { store, source, worker, crawlRows, bootstraps } = await fixture(
    t,
    directory.url,
  );
  const op = await source("https://pds.example");
  await worker.tick();
  assert.equal(store.operation(bootstrapOperationId(op.id))!.state, "applied");
  assert.equal(directory.posts().length, 1);
  assert.equal(crawlRows().length, 1);
  assert.equal(crawlRows()[0].submission, "already_active");
  assert.equal((await bootstraps())[0].phase, "accepted");
});

test("the source stays unapplied when the bootstrap cannot be recorded", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { store, requestSource, worker, operations, bootstrapRows, audit } =
    await fixture(t, directory.url);
  const id = await requestSource("https://pds.example");
  store.enqueue = () => {
    throw new Error("disk I/O error");
  };
  await worker.tick();
  // The rolled-back applied transition leaves the worker's failure path,
  // which records the unexpected error without a follow-up.
  const op = store.operation(id)!;
  assert.equal(op.state, "failed");
  assert.equal(op.error, "application_failed");
  assert.equal(operations("collection_crawl").length, 0);
  assert.equal(bootstrapRows().length, 0);
  assert.equal(
    audit().filter((row) => row.operation === id && row.action === "applied")
      .length,
    0,
  );
  assert.equal(audit("collection_bootstrap_queued").length, 0);
});

test("an unavailable directory is retried with backoff for 24 hours", async (t) => {
  const directory = await stubDirectory(t, unavailableDirectory);
  const { store, source, worker, bootstrap, bootstraps, crawlRows, audit } =
    await fixture(t, directory.url);
  const op = await source("https://pds.example");
  assert.equal(op.state, "applied");
  const crawlId = bootstrapOperationId(op.id);
  await worker.tick();
  let crawl = store.operation(crawlId)!;
  assert.equal(crawl.state, "incomplete");
  assert.equal(crawl.error, "collectiondir_unavailable");
  assert.equal(store.operation(op.id)!.state, "applied");

  // Not yet due: the first retry waits 30 seconds after the failure.
  const failedAt = Date.parse(crawl.updatedAt);
  bootstrap.retryDue(failedAt + 29_000);
  assert.equal(store.operation(crawlId)!.state, "incomplete");
  let [view] = await bootstraps();
  assert.equal(view.phase, "retrying");
  assert.equal(view.request?.error, "collectiondir_unavailable");
  assert.equal(view.request?.attempts, 0);
  assert.equal(
    view.request?.nextAttemptAt,
    new Date(failedAt + 30_000).toISOString(),
  );

  bootstrap.retryDue(failedAt + 30_000);
  assert.equal(store.operation(crawlId)!.state, "requested");
  const retried = audit().filter(
    (row) => row.operation === crawlId && row.action === "requested",
  );
  assert.deepEqual(
    retried.map((row) => row.actor),
    [did, bootstrapActor],
  );
  [view] = await bootstraps();
  assert.equal(view.phase, "waiting");
  assert.equal(view.request?.attempts, 1);

  // A second failure doubles the wait.
  await worker.tick();
  crawl = store.operation(crawlId)!;
  assert.equal(crawl.state, "incomplete");
  bootstrap.retryDue(Date.parse(crawl.updatedAt) + 59_000);
  assert.equal(store.operation(crawlId)!.state, "incomplete");
  [view] = await bootstraps();
  assert.equal(
    view.request?.nextAttemptAt,
    new Date(Date.parse(crawl.updatedAt) + 60_000).toISOString(),
  );

  directory.setHandler(startingDirectory());
  bootstrap.retryDue(Date.parse(crawl.updatedAt) + 60_000);
  await worker.tick();
  assert.equal(store.operation(crawlId)!.state, "applied");
  assert.equal(crawlRows().length, 1);
  assert.equal(crawlRows()[0].cause, "source_added");
  [view] = await bootstraps();
  assert.equal(view.phase, "accepted");
});

test("after 24 hours automatic retries stop and a manual retry still works", async (t) => {
  const directory = await stubDirectory(t, unavailableDirectory);
  const { store, source, worker, bootstrap, bootstraps, request, audit } =
    await fixture(t, directory.url);
  const op = await source("https://pds.example");
  const crawlId = bootstrapOperationId(op.id);
  await worker.tick();
  const createdAt = Date.parse((await bootstraps())[0].createdAt);
  bootstrap.retryDue(createdAt + 24 * 60 * minute);
  bootstrap.retryDue(createdAt + 25 * 60 * minute);
  assert.equal(store.operation(crawlId)!.state, "incomplete");
  const [view] = await bootstraps();
  assert.equal(view.phase, "needs_attention");
  assert.equal(view.request?.nextAttemptAt, null);
  const exhausted = audit("collection_bootstrap_retry_exhausted");
  assert.equal(exhausted.length, 1);
  assert.equal(exhausted[0].actor, bootstrapActor);
  assert.equal(exhausted[0].operation, crawlId);

  directory.setHandler(startingDirectory());
  assert.equal(
    (await request(`/api/v1/operations/${crawlId}/retry`, {})).status,
    200,
  );
  await worker.tick();
  assert.equal(store.operation(crawlId)!.state, "applied");
  assert.equal((await bootstraps())[0].phase, "accepted");
});

test("a source disabled before its crawl runs fails the crawl, which is never retried", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { store, requestSource, worker, bootstrap, bootstraps } = await fixture(
    t,
    directory.url,
  );
  const enable = await requestSource("https://pds.example");
  // The disable is queued before the enable is applied, so it runs before
  // the follow-up crawl.
  await requestSource("https://pds.example", "disabled");
  await worker.tick();
  await worker.tick();
  await worker.tick();
  const crawlId = bootstrapOperationId(enable);
  const crawl = store.operation(crawlId)!;
  assert.equal(crawl.state, "failed");
  assert.equal(crawl.error, "source_not_enabled");
  assert.equal(directory.calls.length, 0);
  assert.equal(store.operation(enable)!.state, "applied");
  bootstrap.retryDue(Date.now() + 60 * minute);
  assert.equal(store.operation(crawlId)!.state, "failed");
  const [view] = await bootstraps();
  assert.equal(view.phase, "not_requested");
  assert.equal(view.request?.error, "source_not_enabled");
});

test("plaintext sources are skipped and recorded without contacting the directory", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { source, operations, bootstraps } = await fixture(t, directory.url);
  const op = await source("http://localhost:2583");
  assert.equal(op.state, "applied");
  assert.deepEqual(bootstrapResult(op), {
    bootstrap: "not_https",
    pds: "http://localhost:2583",
  });
  assert.equal(operations("collection_crawl").length, 0);
  assert.equal(directory.calls.length, 0);
  const [view] = await bootstraps("http://localhost:2583");
  assert.equal(view.phase, "skipped_not_https");
  assert.equal(view.crawlOperation, null);
  assert.equal(view.request, null);
  assert.deepEqual(await bootstraps("https://other.example"), []);
});

test("without a collection directory the source is added and the skip is recorded", async (t) => {
  const { source, operations, bootstraps, request, audit } = await fixture(
    t,
    null,
  );
  const op = await source("https://pds.example");
  assert.equal(op.state, "applied");
  assert.deepEqual(bootstrapResult(op), {
    bootstrap: "not_configured",
    pds: "https://pds.example",
  });
  assert.equal(operations("collection_crawl").length, 0);
  assert.equal(audit("collection_bootstrap_not_configured").length, 1);
  const [view] = await bootstraps();
  assert.equal(view.phase, "skipped_not_configured");
  const sources = await request("/api/v1/sources");
  assert.equal(sources.status, 200);
  assert.equal((await sources.json()).Sources[0].Hostname, "pds.example");
});

test("a failed crawl is reported on the bootstrap while the source stays applied", async (t) => {
  let statusBody: Record<string, unknown> = emptyStatus();
  const starting = startingDirectory();
  const directory = await stubDirectory(t, (call) =>
    call.path === "/admin/crawlStatus" ? { body: statusBody } : starting(call),
  );
  const { store, source, worker, crawls, bootstraps } = await fixture(
    t,
    directory.url,
  );
  const op = await source("https://pds.example");
  await worker.tick();
  statusBody = emptyStatus({
    host_results: {
      "https://pds.example": {
        start: crawlStart,
        end: "2026-11-17T10:05:00Z",
        seen: 3,
        describe_errors: 0,
        state: "failed",
        error_category: "http",
        http_status: 502,
      },
    },
  });
  await crawls.reconcile(Date.now());
  const [view] = await bootstraps();
  assert.equal(view.phase, "failed");
  assert.equal(view.crawl?.errorCategory, "http");
  assert.equal(view.crawl?.httpStatus, 502);
  assert.equal(store.operation(op.id)!.state, "applied");
});

test("the bootstraps route requires a session and the command schema is unchanged", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { request, source, worker, store } = await fixture(t, directory.url);
  assert.equal(
    (
      await request("/api/v1/collection-directory/bootstraps", undefined, {
        Cookie: "",
      })
    ).status,
    401,
  );
  const withCause = await request("/api/v1/operations", {
    kind: "collection_crawl",
    scope: "https://pds.example",
    cause: "source_added",
  });
  assert.equal(withCause.status, 400);
  assert.equal(
    (await request("/api/v1/collection-directory/bootstraps?after=bad")).status,
    400,
  );
  await source("https://pds.example");
  await worker.tick();
  // The directory token never reaches responses or audit history.
  await Promise.all(
    [
      "/api/v1/collection-directory/bootstraps",
      "/api/v1/collection-directory/crawls",
      "/api/v1/operations",
      "/api/v1/audit",
    ].map(async (path) => {
      const text = await (await request(path)).text();
      assert.ok(!text.includes(token), path);
    }),
  );
  assert.ok(
    !JSON.stringify(store.db.prepare("SELECT * FROM audit").all()).includes(
      token,
    ),
  );
});

test("the bootstraps route pages newest first", async (t) => {
  const { source, request } = await fixture(t, null);
  const origins = [
    "https://a.example",
    "https://b.example",
    "https://c.example",
  ];
  for (const origin of origins) await source(origin); // NOSONAR: request order defines newest first
  type Page = { items: CollectionBootstrapView[]; next: string | null };
  const first = (await (
    await request("/api/v1/collection-directory/bootstraps?limit=2")
  ).json()) as Page;
  assert.equal(first.items.length, 2);
  assert.ok(first.next);
  const second = (await (
    await request(
      `/api/v1/collection-directory/bootstraps?limit=2&after=${encodeURIComponent(first.next!)}`,
    )
  ).json()) as Page;
  assert.equal(second.items.length, 1);
  assert.equal(second.next, null);
  // Rows created in the same millisecond tie-break on operation ID, so only
  // the order of the keys and the complete, disjoint pages are asserted.
  const items = [...first.items, ...second.items];
  assert.deepEqual(
    items.map((item) => item.pds).sort((a, b) => a.localeCompare(b)),
    origins,
  );
  const key = (item: CollectionBootstrapView) =>
    `${item.createdAt}|${item.sourceOperation}`;
  for (let i = 1; i < items.length; i++)
    assert.ok(key(items[i - 1]) > key(items[i]), "newest first");
});

test("open bootstraps remain pageable behind more than 100 terminal rows", async (t) => {
  const { store, request } = await fixture(t, null);
  const bootstrap = new CollectionBootstrap(store, true);
  const record = (
    index: number,
    phase: "needs_attention" | "retrying" | "waiting" | "accepted" | "finished",
  ) => {
    const pds = `https://pds-${index}.example`;
    const source = store.request(did, {
      kind: "source",
      pds,
      state: "enabled",
    });
    const follow = bootstrap.plan(source, {
      relay: {
        HostID: index + 1,
        Hostname: new URL(pds).host,
        NoSSL: false,
        DesiredState: "enabled",
        Revision: 1,
        Validation: { Status: "passed" },
      },
    })!;
    store.transition(
      source.id,
      "applied",
      follow.result,
      null,
      undefined,
      ["requested"],
      follow.commit,
    );
    const crawlOperation = bootstrapOperationId(source.id);
    const createdAt = new Date(
      Date.parse(crawlStart) + index * 1000,
    ).toISOString();
    store.db
      .prepare(
        "UPDATE collection_bootstraps SET createdAt=?,gaveUpAt=? WHERE sourceOperation=?",
      )
      .run(
        createdAt,
        phase === "needs_attention" ? createdAt : null,
        source.id,
      );
    if (phase === "needs_attention" || phase === "retrying")
      store.transition(
        crawlOperation,
        "incomplete",
        null,
        "collectiondir_unavailable",
      );
    else if (phase === "accepted" || phase === "finished") {
      store.transition(crawlOperation, "applied", null, null);
      store.db
        .prepare(
          "INSERT INTO collection_crawls(id,operation,pds,actor,cause,submission,state,requestedAt,acceptedAt) VALUES(?,?,?,?,?,?,?,?,?)",
        )
        .run(
          randomUUID(),
          crawlOperation,
          pds,
          did,
          "source_added",
          "started",
          phase,
          createdAt,
          createdAt,
        );
    }
    return source.id;
  };
  const needsAttention = record(0, "needs_attention");
  const retrying = record(1, "retrying");
  const waiting = record(2, "waiting");
  const accepted = record(3, "accepted");
  for (let i = 4; i < 105; i++) record(i, "finished");

  type Page = { items: CollectionBootstrapView[]; next: string | null };
  const page = async (query: string) => {
    const response = await request(
      `/api/v1/collection-directory/bootstraps?${query}`,
    );
    assert.equal(response.status, 200);
    return (await response.json()) as Page;
  };
  const unfiltered = await page("limit=100");
  assert.equal(unfiltered.items.length, 100);
  assert.ok(unfiltered.next);
  assert.ok(unfiltered.items.every((item) => item.phase === "finished"));

  const first = await page("phase=open&limit=2");
  assert.deepEqual(
    first.items.map((item) => item.sourceOperation),
    [accepted, waiting],
  );
  assert.ok(first.next);
  const second = await page(
    `phase=open&limit=2&after=${encodeURIComponent(first.next!)}`,
  );
  assert.deepEqual(
    second.items.map((item) => item.sourceOperation),
    [retrying, needsAttention],
  );
  assert.deepEqual(
    second.items.map((item) => item.phase),
    ["retrying", "needs_attention"],
  );
  assert.equal(second.next, null);
  const filtered = await page(
    "phase=open&limit=100&pds=https%3A%2F%2Fpds-0.example",
  );
  assert.deepEqual(
    filtered.items.map((item) => item.sourceOperation),
    [needsAttention],
  );
  assert.equal(filtered.next, null);
  assert.equal(
    (await request("/api/v1/collection-directory/bootstraps?phase=finished"))
      .status,
    400,
  );
});

test("a source enabled by PDS discovery queues a crawl owned by the discovery actor", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { store, request, worker, bootstraps, crawlRows, audit } =
    await fixture(t, directory.url);
  const response = await request(
    "/api/v1/sources/discovery/operations",
    { kind: "source", pds: "https://pds.example", state: "enabled" },
    { Cookie: "", Authorization: `Bearer ${machineToken}` },
  );
  assert.equal(response.status, 202);
  const op = (await response.json()) as Operation;
  assert.equal(op.actor, discoveryActor);
  await worker.tick();
  assert.equal(store.operation(op.id)!.state, "applied");
  const crawlId = bootstrapOperationId(op.id);
  const crawl = store.operation(crawlId)!;
  assert.equal(crawl.actor, discoveryActor);
  assert.equal(audit("collection_bootstrap_queued")[0].actor, discoveryActor);
  let [view] = await bootstraps();
  assert.equal(view.actor, discoveryActor);
  assert.equal(view.actorHandle, null);
  assert.equal(view.phase, "waiting");

  await worker.tick();
  assert.equal(store.operation(crawlId)!.state, "applied");
  assert.equal(crawlRows()[0].actor, discoveryActor);
  assert.equal(crawlRows()[0].cause, "source_added");
  [view] = await bootstraps();
  assert.equal(view.phase, "accepted");
  assert.equal(view.crawl?.actor, discoveryActor);
});

test("a transition that does not happen does not run its follow-up", () => {
  const store = new Store(":memory:");
  const op = store.request(did, {
    kind: "source",
    pds: "https://pds.example",
    state: "enabled",
  });
  let ran = 0;
  assert.equal(
    store.transition(
      op.id,
      "applied",
      null,
      null,
      undefined,
      ["applying"],
      () => {
        ran++;
      },
    ),
    false,
  );
  assert.equal(ran, 0);
  assert.equal(
    store.transition(
      op.id,
      "applying",
      null,
      null,
      undefined,
      ["requested"],
      () => {
        ran++;
      },
    ),
    true,
  );
  assert.equal(ran, 1);
  store.close();
});

test("automatic retries surface database failures instead of swallowing them", async (t) => {
  const directory = await stubDirectory(t, unavailableDirectory);
  const { store, source, worker, bootstrap } = await fixture(t, directory.url);
  const op = await source("https://pds.example");
  await worker.tick();
  const failedAt = Date.parse(
    store.operation(bootstrapOperationId(op.id))!.updatedAt,
  );
  store.transition = () => {
    throw new Error("disk I/O error");
  };
  assert.throws(() => bootstrap.retryDue(failedAt + 30_000), /disk I\/O error/);
});

test("server-derived version 8 keys are refused on both operation routes", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { request, operations } = await fixture(t, directory.url);
  const reserved = bootstrapOperationId(randomUUID());
  const enable = {
    kind: "source",
    pds: "https://pds.example",
    state: "enabled",
  };
  const browser = await request("/api/v1/operations", enable, {
    "Idempotency-Key": reserved,
  });
  assert.equal(browser.status, 400);
  assert.deepEqual(await browser.json(), {
    error: "reserved_idempotency_key",
  });
  const machine = await request(
    "/api/v1/sources/discovery/operations",
    enable,
    {
      Cookie: "",
      Authorization: `Bearer ${machineToken}`,
      "Idempotency-Key": reserved,
    },
  );
  assert.equal(machine.status, 400);
  assert.deepEqual(await machine.json(), {
    error: "reserved_idempotency_key",
  });
  assert.equal(operations().length, 0);
  // Ordinary random and name-based keys are still accepted.
  assert.equal(
    (
      await request("/api/v1/operations", enable, {
        "Idempotency-Key": randomUUID(),
      })
    ).status,
    202,
  );
});

test("a follow-up ID already in use skips the crawl and the source still applies", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { store, requestSource, worker, operations, bootstraps, audit } =
    await fixture(t, directory.url);
  const id = await requestSource("https://pds.example");
  // Pre-create an unrelated operation at the derived ID, as a client key
  // could have before version 8 keys were reserved.
  const squatter = store.request(
    discoveryActor,
    { kind: "source", pds: "https://pds.example", state: "enabled" },
    bootstrapOperationId(id),
  );
  await worker.tick();
  const op = store.operation(id)!;
  assert.equal(op.state, "applied", op.error ?? "");
  assert.deepEqual(bootstrapResult(op), {
    bootstrap: "conflict",
    pds: "https://pds.example",
  });
  assert.equal(operations("collection_crawl").length, 0);
  assert.deepEqual(store.operation(squatter.id)!.command, squatter.command);
  const conflicts = audit("collection_bootstrap_conflict");
  assert.equal(conflicts.length, 1);
  assert.equal(conflicts[0].operation, id);
  const [view] = await bootstraps();
  assert.equal(view.outcome, "conflict");
  assert.equal(view.phase, "skipped_conflict");
  assert.equal(view.crawlOperation, null);
  assert.equal(view.request, null);
  assert.equal(directory.posts().length, 0);
});

test("a conflict that appears at commit is recorded instead of rolling back", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { store, relay, bootstrap, requestSource, bootstrapRows, audit } =
    await fixture(t, directory.url);
  const id = await requestSource("https://pds.example");
  const view = relay.put("https://pds.example", "enabled");
  const follow = bootstrap.plan(store.operation(id)!, {
    relay: view,
    jetstream: { pending: false, enabled: true },
  })!;
  assert.equal(
    (follow.result as CollectionDirectoryResult).collectionDirectory?.bootstrap,
    "queued",
  );
  store.request(
    discoveryActor,
    { kind: "source", pds: "https://pds.example", state: "enabled" },
    bootstrapOperationId(id),
  );
  assert.doesNotThrow(() => store.transaction(follow.commit));
  const [row] = bootstrapRows();
  assert.equal(row.outcome, "conflict");
  assert.equal(row.crawlOperation, null);
  assert.equal(audit("collection_bootstrap_conflict").length, 1);
  assert.equal(audit("collection_bootstrap_queued").length, 0);
});

test("an enable interrupted while applying is re-applied with exactly one bootstrap", async (t) => {
  const directory = await stubDirectory(t, startingDirectory());
  const { store, relay, requestSource, worker, bootstrapRows, audit } =
    await fixture(t, directory.url);
  const id = await requestSource("https://pds.example");
  // Relay applied the enable, then the process stopped before the applied
  // transaction committed.
  assert.ok(
    store.transition(id, "applying", null, null, undefined, ["requested"]),
  );
  relay.put("https://pds.example", "enabled");
  worker.recover();
  assert.equal(store.operation(id)!.state, "requested");
  await worker.tick();
  const op = store.operation(id)!;
  assert.equal(op.state, "applied", op.error ?? "");
  assert.equal(bootstrapResult(op)?.bootstrap, "queued");
  const rows = bootstrapRows();
  assert.equal(rows.length, 1);
  assert.equal(rows[0].sourceOperation, id);
  assert.equal(rows[0].outcome, "queued");
  assert.equal(audit("collection_bootstrap_queued").length, 1);
  assert.equal(audit("collection_bootstrap_unchanged").length, 0);
});
