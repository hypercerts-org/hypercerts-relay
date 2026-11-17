import { test, expect, vi, afterEach } from "vitest";
import {
  render,
  screen,
  fireEvent,
  cleanup,
  within,
} from "@testing-library/svelte";
import Sources from "../src/Sources.svelte";
import CollectionDirectory from "../src/CollectionDirectory.svelte";
import {
  bootstrapNotice,
  bootstrapSummary,
  crawlEligible,
  crawlNotice,
  crawlSummary,
  crawlsInProgress,
  createDirectoryPoller,
  directoryMessage,
  directoryPollMs,
  directoryRetryMs,
  refreshCrawls,
} from "../src/collectionDirectory";
import type {
  CollectionBootstrapView,
  CollectionCrawl,
  CollectionDirectoryStatus,
  Operation,
  Source,
} from "../src/api";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

// The URL a stubbed fetch was called with.
function requestUrl(input: RequestInfo | URL): string {
  if (typeof input === "string") return input;
  return input instanceof URL ? input.href : input.url;
}

let nextHostID = 1;
function source(hostname: string, overrides: Partial<Source> = {}): Source {
  return {
    HostID: nextHostID++,
    Hostname: hostname,
    NoSSL: false,
    DesiredState: "enabled",
    RuntimeState: "connected",
    Revision: 1,
    RecoveryRequired: false,
    LastDurableCursor: 1,
    Validation: { Status: "passed", Reason: "" },
    AccountQuota: { Count: 1, Limit: 100 },
    ...overrides,
  };
}

function directory(
  overrides: Partial<CollectionDirectoryStatus> = {},
): CollectionDirectoryStatus {
  return {
    configured: true,
    available: true,
    error: null,
    observedAt: "2026-11-17T10:00:00.000Z",
    processStartedAt: "2026-11-17T09:00:00Z",
    historySupported: true,
    active: [],
    recent: [],
    ...overrides,
  };
}

function crawl(
  pds: string,
  overrides: Partial<CollectionCrawl> = {},
): CollectionCrawl {
  return {
    id: `${pds}-${overrides.state ?? "accepted"}`,
    operation: "00000000-0000-4000-8000-000000000001",
    pds,
    actor: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
    actorHandle: "operator.example",
    cause: "manual",
    sourceOperation: null,
    submission: "started",
    state: "accepted",
    requestedAt: "2026-11-17T10:00:01.000Z",
    acceptedAt: "2026-11-17T10:00:01.100Z",
    crawlStartedAt: "2026-11-17T10:00:00.05Z",
    finishedAt: null,
    reposDescribed: null,
    describeErrors: null,
    errorCategory: null,
    httpStatus: null,
    observedAt: null,
    ...overrides,
  };
}

const crawlButton = "Crawl collection directory";

test("only enabled, validated HTTPS sources offer a crawl", () => {
  render(Sources, {
    rows: [
      source("eligible.example"),
      source("disabled.example", { DesiredState: "disabled" }),
      source("pending.example", {
        Validation: { Status: "pending", Reason: "" },
      }),
      source("plain.example", { NoSSL: true }),
    ],
    submit: vi.fn(),
    directory: directory(),
  });
  const buttons = screen.getAllByRole("button", { name: crawlButton });
  expect(buttons).toHaveLength(1);
  const row = buttons[0].closest("tr")!;
  expect(within(row).getByText("eligible.example")).toBeTruthy();
  expect(crawlEligible(source("x.example"))).toBe(true);
  expect(crawlEligible(source("x.example", { NoSSL: true }))).toBe(false);
});

test("the crawl action submits exactly the source's HTTPS origin", async () => {
  const submit = vi.fn();
  render(Sources, {
    rows: [source("pds.example")],
    submit,
    directory: directory(),
  });
  await fireEvent.click(screen.getByRole("button", { name: crawlButton }));
  expect(submit).toHaveBeenCalledWith({
    kind: "collection_crawl",
    scope: "https://pds.example",
  });
});

test.each([
  [
    directory({
      configured: false,
      available: false,
      error: "collectiondir_not_configured",
    }),
    /COLLECTIONDIR_CONTROL_URL and COLLECTIONDIR_CONTROL_TOKEN_FILE/,
  ],
  [
    directory({ available: false, error: "collectiondir_unavailable" }),
    /collection directory is unavailable/,
  ],
  [
    directory({ available: false, error: "collectiondir_unauthorized" }),
    /rejected the Administration credential/,
  ],
])(
  "an unusable directory disables only the crawl action",
  async (status, message) => {
    const submit = vi.fn();
    render(Sources, {
      rows: [source("pds.example")],
      submit,
      directory: status,
    });
    const crawlAction = screen.getByRole("button", {
      name: crawlButton,
    }) as HTMLButtonElement;
    expect(crawlAction.disabled).toBe(true);
    const explanation = screen.getByRole("status");
    expect(explanation.textContent).toMatch(message);
    expect(crawlAction.getAttribute("aria-describedby")).toBe(explanation.id);
    expect(screen.queryByRole("alert")).toBeNull();
    await fireEvent.click(screen.getByRole("button", { name: "Disable" }));
    await fireEvent.click(screen.getByRole("button", { name: "Remove" }));
    expect(submit).toHaveBeenCalledWith({
      kind: "source",
      pds: "https://pds.example",
      state: "disabled",
    });
    expect(submit).toHaveBeenCalledWith({
      kind: "source",
      pds: "https://pds.example",
      state: "removed",
    });
    expect(submit).toHaveBeenCalledTimes(2);
  },
);

test("an active crawl replaces the action with its progress and start", async () => {
  const fetcher = vi.fn(() =>
    Promise.resolve(Response.json({ error: "unavailable" }, { status: 503 })),
  );
  vi.stubGlobal("fetch", fetcher);
  const active = {
    pds: "https://pds.example",
    startedAt: "2026-11-17T10:00:00.05Z",
    reposDescribed: 12,
    describeErrors: 1,
  };
  render(Sources, {
    rows: [source("pds.example")],
    submit: vi.fn(),
    directory: directory({ active: [active] }),
    crawls: [crawl("https://pds.example")],
  });
  const running = screen.getByRole("button", {
    name: "Crawling… 12 repositories described",
  }) as HTMLButtonElement;
  expect(running.disabled).toBe(true);
  expect(screen.queryByRole("button", { name: crawlButton })).toBeNull();
  await fireEvent.click(screen.getByRole("button", { name: "pds.example" }));
  const detail = screen.getByRole("region", {
    name: "Collection directory crawl",
  });
  expect(
    within(detail).getByText(
      "Accepted, crawl running · 12 repositories described",
    ),
  ).toBeTruthy();
  expect(
    within(detail).getByText(
      new Date("2026-11-17T10:00:00.05Z").toLocaleString(),
    ),
  ).toBeTruthy();
  expect(
    within(detail).getByText(
      "Results are candidate DID/collection pairs, not an authoritative inventory.",
    ),
  ).toBeTruthy();
});

test("accepted, finished, failed and unknown crawls read differently", () => {
  const pds = "https://pds.example";
  const summaries = [
    crawlSummary(crawl(pds)),
    crawlSummary(crawl(pds, { state: "finished", reposDescribed: 40 })),
    crawlSummary(
      crawl(pds, { state: "failed", errorCategory: "http", httpStatus: 502 }),
    ),
    crawlSummary(
      crawl(pds, {
        state: "unknown",
        errorCategory: "collectiondir_restarted",
      }),
    ),
  ];
  expect(summaries).toEqual([
    "Accepted, crawl running",
    "Finished · 40 repositories described",
    "Failed: http 502",
    "Outcome unknown: collection directory restarted",
  ]);
  render(CollectionDirectory, {
    directory: directory(),
    crawls: [
      crawl(pds),
      crawl("https://b.example", { state: "finished", reposDescribed: 40 }),
      crawl("https://c.example", {
        state: "failed",
        errorCategory: "http",
        httpStatus: 502,
      }),
      crawl("https://d.example", {
        state: "unknown",
        errorCategory: "collectiondir_restarted",
      }),
    ],
    submit: vi.fn(),
  });
  const table = screen.getByRole("table", { name: "Crawl requests" });
  for (const summary of summaries)
    expect(within(table).getByText(summary)).toBeTruthy();
  expect(within(table).getAllByText("@operator.example")).toHaveLength(4);
});

test("an already running crawl is explained, not reported as a failure", () => {
  const op = {
    id: "00000000-0000-4000-8000-000000000001",
    actor: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
    command: { kind: "collection_crawl", scope: "https://pds.example" },
    state: "applied",
    createdAt: "2026-11-17T10:00:00.000Z",
    updatedAt: "2026-11-17T10:00:00.000Z",
    result: {
      collectiondir: {
        submitted: 1,
        started: 0,
        alreadyActive: 1,
        skipped: { not_enabled: 0, not_https: 0 },
      },
    },
    error: null,
  } as Operation;
  const row = crawl("https://pds.example", {
    submission: "already_active",
    crawlStartedAt: "2026-11-17T09:58:00Z",
  });
  expect(crawlNotice(op, [row])).toBe(
    `A crawl of https://pds.example is already running (started ${new Date(
      "2026-11-17T09:58:00Z",
    ).toLocaleString()}). No second crawl was started.`,
  );
  expect(crawlNotice({ ...op, state: "applying" }, [row])).toBeNull();
  expect(
    crawlNotice(
      {
        ...op,
        command: { kind: "collection_crawl", scope: "all" },
        result: {
          collectiondir: {
            submitted: 3,
            started: 2,
            alreadyActive: 1,
            skipped: { not_enabled: 2, not_https: 1 },
          },
        },
      },
      [],
    ),
  ).toBe(
    "Crawl requested for 3 sources: 2 started, 1 already running. Skipped 2 not enabled and 1 without HTTPS.",
  );
});

test("crawl notices explain sources already submitted by an earlier attempt", () => {
  const op = {
    id: "00000000-0000-4000-8000-000000000001",
    command: { kind: "collection_crawl", scope: "https://pds.example" },
    state: "applied",
    result: {
      collectiondir: {
        submitted: 0,
        started: 0,
        alreadyActive: 0,
        alreadySubmitted: 1,
        skipped: { not_enabled: 0, not_https: 0 },
      },
    },
  } as Operation;
  expect(crawlNotice(op, [])).toBe(
    "The crawl of https://pds.example was already submitted by an earlier attempt of this request. No second crawl was started.",
  );
  expect(
    crawlNotice(
      {
        ...op,
        command: { kind: "collection_crawl", scope: "all" },
        result: {
          collectiondir: {
            submitted: 50,
            started: 50,
            alreadyActive: 0,
            alreadySubmitted: 100,
            skipped: { not_enabled: 0, not_https: 0 },
          },
        },
      },
      [],
    ),
  ).toBe(
    "Crawl requested for 50 sources: 50 started, 0 already running. 100 already submitted by an earlier attempt were not sent again.",
  );
});

test("crawl all requires confirmation before submitting every source", async () => {
  const submit = vi.fn(async () => {});
  render(CollectionDirectory, { directory: directory(), crawls: [], submit });
  await fireEvent.click(
    screen.getByRole("button", { name: "Crawl all enabled sources" }),
  );
  expect(submit).not.toHaveBeenCalled();
  await fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(screen.queryByRole("button", { name: "Confirm crawl" })).toBeNull();
  await fireEvent.click(
    screen.getByRole("button", { name: "Crawl all enabled sources" }),
  );
  await fireEvent.click(screen.getByRole("button", { name: "Confirm crawl" }));
  expect(submit).toHaveBeenCalledExactlyOnceWith({
    kind: "collection_crawl",
    scope: "all",
  });
});

test("crawl all is disabled with an explanation when the directory is not configured", () => {
  render(CollectionDirectory, {
    directory: directory({
      configured: false,
      available: false,
      error: "collectiondir_not_configured",
    }),
    crawls: [],
    submit: vi.fn(),
  });
  const button = screen.getByRole("button", {
    name: "Crawl all enabled sources",
  }) as HTMLButtonElement;
  expect(button.disabled).toBe(true);
  expect(screen.getByRole("status").textContent).toMatch(
    /COLLECTIONDIR_CONTROL_URL/,
  );
  expect(directoryMessage(directory())).toBeNull();
});

test("the active crawls table shows start time and repositories described", () => {
  render(CollectionDirectory, {
    directory: directory({
      active: [
        {
          pds: "https://pds.example",
          startedAt: "2026-11-17T10:00:00.05Z",
          reposDescribed: 12,
          describeErrors: 0,
        },
      ],
    }),
    crawls: [],
    submit: vi.fn(),
  });
  const table = screen.getByRole("table", { name: "Active crawls" });
  expect(within(table).getByText("https://pds.example")).toBeTruthy();
  expect(
    within(table).getByText(
      new Date("2026-11-17T10:00:00.05Z").toLocaleString(),
    ),
  ).toBeTruthy();
  expect(within(table).getByText("12")).toBeTruthy();
});

test("the poller runs only while a crawl is active, pauses when hidden, and stops", async () => {
  vi.useFakeTimers();
  let active = true;
  let hidden = false;
  const page = Object.defineProperty(new EventTarget(), "hidden", {
    get: () => hidden,
  }) as unknown as Document;
  const refresh = vi.fn(async () => {});
  const poller = createDirectoryPoller(refresh, () => active, page);
  poller.update();
  await vi.advanceTimersByTimeAsync(directoryPollMs - 1);
  expect(refresh).not.toHaveBeenCalled();
  await vi.advanceTimersByTimeAsync(1);
  expect(refresh).toHaveBeenCalledTimes(1);
  await vi.advanceTimersByTimeAsync(directoryPollMs);
  expect(refresh).toHaveBeenCalledTimes(2);

  // Hidden pages do not refresh; becoming visible resumes immediately.
  hidden = true;
  await vi.advanceTimersByTimeAsync(directoryPollMs * 3);
  expect(refresh).toHaveBeenCalledTimes(2);
  hidden = false;
  page.dispatchEvent(new Event("visibilitychange"));
  await vi.advanceTimersByTimeAsync(0);
  expect(refresh).toHaveBeenCalledTimes(3);

  // A failed refresh waits longer before retrying.
  refresh.mockRejectedValueOnce(new Error("offline"));
  await vi.advanceTimersByTimeAsync(directoryPollMs);
  expect(refresh).toHaveBeenCalledTimes(4);
  await vi.advanceTimersByTimeAsync(directoryRetryMs - 1);
  expect(refresh).toHaveBeenCalledTimes(4);
  await vi.advanceTimersByTimeAsync(1);
  expect(refresh).toHaveBeenCalledTimes(5);

  // Nothing active: polling goes idle.
  active = false;
  await vi.advanceTimersByTimeAsync(directoryPollMs * 4);
  expect(refresh).toHaveBeenCalledTimes(6);
  await vi.advanceTimersByTimeAsync(directoryPollMs * 4);
  expect(refresh).toHaveBeenCalledTimes(6);

  // Stopped on unmount: no further refreshes even if work appears.
  active = true;
  poller.update();
  poller.stop();
  await vi.advanceTimersByTimeAsync(directoryPollMs * 4);
  expect(refresh).toHaveBeenCalledTimes(6);
});

test("the source detail loads its newest crawl even when it is not in the shared list", async () => {
  const older = crawl("https://pds.example", {
    id: "older-row",
    state: "failed",
    errorCategory: "http",
    httpStatus: 502,
    requestedAt: "2026-11-01T10:00:00.000Z",
  });
  const fetcher = vi.fn((input: RequestInfo | URL) =>
    Promise.resolve(
      requestUrl(input).startsWith("/api/v1/collection-directory/crawls")
        ? Response.json({ items: [older], next: null })
        : Response.json({ error: "unavailable" }, { status: 503 }),
    ),
  );
  vi.stubGlobal("fetch", fetcher);
  render(Sources, {
    rows: [source("pds.example")],
    submit: vi.fn(),
    directory: directory(),
    // The shared list holds only other sources' newer requests.
    crawls: [crawl("https://other.example")],
  });
  await fireEvent.click(screen.getByRole("button", { name: "pds.example" }));
  const detail = screen.getByRole("region", {
    name: "Collection directory crawl",
  });
  expect(await within(detail).findByText("Failed: http 502")).toBeTruthy();
  expect(
    fetcher.mock.calls.some(
      ([input]) =>
        requestUrl(input) ===
        "/api/v1/collection-directory/crawls?pds=https%3A%2F%2Fpds.example&limit=1",
    ),
  ).toBe(true);
});

test("accepted rows do not keep polling once the directory is unconfigured", () => {
  const accepted = [crawl("https://pds.example")];
  expect(crawlsInProgress(directory(), accepted, null)).toBe(true);
  expect(
    crawlsInProgress(
      directory({
        configured: false,
        available: false,
        error: "collectiondir_not_configured",
      }),
      accepted,
      null,
    ),
  ).toBe(false);
  expect(
    crawlsInProgress(
      directory({ available: false, error: "collectiondir_unavailable" }),
      accepted,
      null,
    ),
  ).toBe(true);
  expect(crawlsInProgress(directory(), [], null)).toBe(false);
});

test("the initial directory check is a status note, not an error", () => {
  render(CollectionDirectory, { directory: null, crawls: [], submit: vi.fn() });
  const message = screen.getByRole("status");
  expect(message.textContent).toMatch(/Checking the collection directory/);
  expect(message.classList.contains("error")).toBe(false);
});

test("adding a source works while the collection directory is unconfigured", async () => {
  const submit = vi.fn();
  render(Sources, {
    rows: [],
    submit,
    directory: directory({
      configured: false,
      available: false,
      error: "collectiondir_not_configured",
    }),
  });
  await fireEvent.input(screen.getByLabelText("PDS origin"), {
    target: { value: "new.example" },
  });
  const add = screen.getByRole("button", {
    name: "Add source",
  }) as HTMLButtonElement;
  expect(add.disabled).toBe(false);
  await fireEvent.click(add);
  expect(submit).toHaveBeenCalledExactlyOnceWith({
    kind: "source",
    pds: "new.example",
    state: "enabled",
  });
});

test("refreshing a finished crawl on page two lets polling go idle", async () => {
  const newest = Array.from({ length: 100 }, (_, i) =>
    crawl(`https://new${i}.example`, { state: "finished" }),
  );
  const older = crawl("https://older.example", {
    id: "00000000-0000-4000-8000-000000000001",
    requestedAt: "2026-11-01T10:00:00.000Z",
  });
  const finished = { ...older, state: "finished" as const, reposDescribed: 40 };
  const read = vi.fn(async (path: string) =>
    path.includes("&ids=")
      ? { items: [finished], next: null }
      : { items: newest, next: "newest-page-cursor" },
  );
  expect(crawlsInProgress(directory(), [...newest, older], null)).toBe(true);
  const refreshed = await refreshCrawls([...newest, older], true, read);
  expect(read).toHaveBeenLastCalledWith(
    `/collection-directory/crawls?limit=100&ids=${older.id}`,
  );
  expect(refreshed.items).toHaveLength(101);
  expect(refreshed.items[100]).toEqual(finished);
  expect(crawlSummary(refreshed.items[100])).toBe(
    "Finished · 40 repositories described",
  );
  expect(crawlsInProgress(directory(), refreshed.items, null)).toBe(false);
});

test("refreshing loaded history checks every older open row in bounded chunks", async () => {
  const older = Array.from({ length: 101 }, (_, i) =>
    crawl(`https://older${i}.example`, {
      id: `00000000-0000-4000-8000-${String(i).padStart(12, "0")}`,
    }),
  );
  const terminal = older.map((row) => ({ ...row, state: "finished" as const }));
  const checked: string[][] = [];
  const read = vi.fn(async (path: string) => {
    const ids = new URL(path, "https://admin.example").searchParams.get("ids");
    if (!ids) return { items: [], next: "newest-page-cursor" };
    const chunk = ids.split(",");
    checked.push(chunk);
    return {
      items: terminal.filter((row) => chunk.includes(row.id)),
      next: null,
    };
  });
  const refreshed = await refreshCrawls(older, true, read);
  expect(checked.map((chunk) => chunk.length)).toEqual([100, 1]);
  expect(checked.flat()).toEqual(older.map((row) => row.id));
  expect(refreshed.items).toEqual(terminal);
  expect(crawlsInProgress(directory(), refreshed.items, null)).toBe(false);
  // Refreshing without loaded history keeps the original newest-page result.
  expect(await refreshCrawls(older, false, read)).toEqual({
    items: [],
    next: "newest-page-cursor",
  });
});

const sourceOperation = "00000000-0000-4000-8000-0000000000a1";
const bootstrapOperation = "00000000-0000-8000-8000-0000000000b2";
function bootstrap(
  phase: CollectionBootstrapView["phase"],
  overrides: Partial<CollectionBootstrapView> = {},
): CollectionBootstrapView {
  const skipped = phase.startsWith("skipped_");
  const outcomes: Partial<
    Record<CollectionBootstrapView["phase"], CollectionBootstrapView["outcome"]>
  > = {
    skipped_not_https: "not_https",
    skipped_not_configured: "not_configured",
    skipped_conflict: "conflict",
  };
  return {
    sourceOperation,
    pds: "https://pds.example",
    actor: "service:pds-discovery",
    actorHandle: null,
    createdAt: "2026-11-17T10:00:00.000Z",
    outcome: outcomes[phase] ?? "queued",
    crawlOperation: skipped ? null : bootstrapOperation,
    phase,
    request: skipped
      ? null
      : {
          state: "requested",
          error: null,
          attempts: 0,
          nextAttemptAt: null,
          updatedAt: "2026-11-17T10:00:01.000Z",
        },
    crawl: null,
    ...overrides,
  };
}
function enableOperation(
  state: Operation["state"],
  result: unknown,
): Operation {
  return {
    id: sourceOperation,
    actor: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
    command: { kind: "source", pds: "https://pds.example", state: "enabled" },
    state,
    createdAt: "2026-11-17T10:00:00.000Z",
    updatedAt: "2026-11-17T10:00:00.000Z",
    result,
    error: null,
  };
}

test("the source notice explains each initial crawl outcome", () => {
  const notice = (bootstrap: string) =>
    bootstrapNotice(
      enableOperation("applied", {
        collectionDirectory: { bootstrap, pds: "https://pds.example" },
      }),
    );
  expect(notice("queued")).toEqual({
    text: "Initial collection directory crawl queued.",
    queued: true,
  });
  expect(notice("not_configured")?.text).toBe(
    "Collection directory is not configured, so no initial crawl was requested. The source was added.",
  );
  expect(notice("not_https")?.text).toMatch(
    /^No initial crawl: the collection directory only crawls HTTPS sources\./,
  );
  expect(notice("conflict")?.text).toBe(
    "No initial crawl was requested because its request ID was already in use. The source was added.",
  );
  expect(notice("unchanged")).toBeNull();
  expect(
    bootstrapNotice(
      enableOperation("requested", {
        collectionDirectory: { bootstrap: "queued" },
      }),
    ),
  ).toBeNull();
  expect(bootstrapNotice(enableOperation("applied", { relay: {} }))).toBeNull();
});

test("initial crawl phases read differently; accepted is not finished", () => {
  expect(bootstrapSummary(bootstrap("waiting"))).toBe(
    "Waiting to be submitted",
  );
  expect(
    bootstrapSummary(
      bootstrap("retrying", {
        request: {
          state: "incomplete",
          error: "collectiondir_unavailable",
          attempts: 1,
          nextAttemptAt: "2026-11-17T10:01:00.000Z",
          updatedAt: "2026-11-17T10:00:30.000Z",
        },
      }),
    ),
  ).toMatch(
    /^Collection directory unavailable; retrying automatically \(attempt 2, next at .+\)$/,
  );
  expect(
    bootstrapSummary(
      bootstrap("needs_attention", {
        request: {
          state: "incomplete",
          error: "collectiondir_unauthorized",
          attempts: 9,
          nextAttemptAt: null,
          updatedAt: "2026-11-18T10:00:00.000Z",
        },
      }),
    ),
  ).toBe(
    "Could not be submitted: collectiondir unauthorized. Automatic retries have stopped.",
  );
  expect(
    bootstrapSummary(
      bootstrap("not_requested", {
        request: {
          state: "failed",
          error: "source_not_enabled",
          attempts: 0,
          nextAttemptAt: null,
          updatedAt: "2026-11-17T10:00:01.000Z",
        },
      }),
    ),
  ).toBe("Not requested: source was no longer enabled");
  expect(bootstrapSummary(bootstrap("skipped_not_configured"))).toMatch(
    /^Skipped: collection directory not configured/,
  );
  expect(bootstrapSummary(bootstrap("skipped_not_https"))).toMatch(
    /^Skipped: the collection directory only crawls HTTPS sources/,
  );
  expect(bootstrapSummary(bootstrap("skipped_conflict"))).toMatch(
    /^Skipped: another request already used this initial crawl's ID/,
  );
  const accepted = bootstrapSummary(
    bootstrap("accepted", {
      crawl: crawl("https://pds.example", { cause: "source_added" }),
    }),
    {
      pds: "https://pds.example",
      startedAt: "2026-11-17T10:00:00.05Z",
      reposDescribed: 4,
      describeErrors: 0,
    },
  );
  const finished = bootstrapSummary(
    bootstrap("finished", {
      crawl: crawl("https://pds.example", {
        state: "finished",
        reposDescribed: 4,
      }),
    }),
  );
  expect(accepted).toBe("Accepted, crawl running · 4 repositories described");
  expect(finished).toBe("Finished · 4 repositories described");
  expect(
    bootstrapSummary(
      bootstrap("failed", {
        crawl: crawl("https://pds.example", {
          state: "failed",
          errorCategory: "http",
          httpStatus: 502,
        }),
      }),
    ),
  ).toBe("Failed: http 502");
});

test("the source detail offers Retry only once automatic retries have stopped", async () => {
  const action = vi.fn(async () => {});
  let current = bootstrap("retrying", {
    request: {
      state: "incomplete",
      error: "collectiondir_unavailable",
      attempts: 0,
      nextAttemptAt: "2026-11-17T10:01:00.000Z",
      updatedAt: "2026-11-17T10:00:30.000Z",
    },
  });
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.startsWith("/api/v1/collection-directory/bootstraps"))
        return Response.json({ items: [current], next: null });
      if (url.startsWith("/api/v1/collection-directory/crawls"))
        return Response.json({ items: [], next: null });
      return Response.json({ error: "unavailable" }, { status: 503 });
    }),
  );
  render(Sources, {
    rows: [source("pds.example")],
    submit: vi.fn(),
    action,
    directory: directory(),
  });
  await fireEvent.click(screen.getByRole("button", { name: "pds.example" }));
  const detail = screen.getByRole("region", {
    name: "Collection directory crawl",
  });
  expect(
    await within(detail).findByText(/retrying automatically/),
  ).toBeTruthy();
  expect(within(detail).getByText(/by service:pds-discovery/)).toBeTruthy();
  expect(within(detail).getByText(/The source was added/)).toBeTruthy();
  expect(
    within(detail).queryByRole("button", { name: "Retry initial crawl" }),
  ).toBeNull();

  current = bootstrap("needs_attention", {
    request: {
      state: "incomplete",
      error: "collectiondir_unavailable",
      attempts: 9,
      nextAttemptAt: null,
      updatedAt: "2026-11-18T10:00:00.000Z",
    },
  });
  await fireEvent.click(screen.getByRole("button", { name: "Close" }));
  await fireEvent.click(screen.getByRole("button", { name: "pds.example" }));
  const retry = await within(
    screen.getByRole("region", { name: "Collection directory crawl" }),
  ).findByRole("button", { name: "Retry initial crawl" });
  await fireEvent.click(retry);
  expect(action).toHaveBeenCalledExactlyOnceWith(bootstrapOperation, "retry");
});

test("crawl requests show their trigger and initial crawls awaiting submission are listed", () => {
  render(CollectionDirectory, {
    directory: directory(),
    crawls: [
      crawl("https://manual.example"),
      crawl("https://added.example", {
        cause: "source_added",
        actor: "service:pds-discovery",
        actorHandle: null,
        sourceOperation,
      }),
    ],
    bootstraps: [
      bootstrap("retrying", { pds: "https://waiting.example" }),
      bootstrap("accepted", {
        pds: "https://accepted.example",
        sourceOperation: "00000000-0000-4000-8000-0000000000c3",
      }),
    ],
    submit: vi.fn(),
  });
  const requests = screen.getByRole("table", { name: "Crawl requests" });
  const added = within(requests)
    .getByText("https://added.example")
    .closest("tr")!;
  expect(within(added).getByText("Source added")).toBeTruthy();
  expect(within(added).getByText("service:pds-discovery")).toBeTruthy();
  const manual = within(requests)
    .getByText("https://manual.example")
    .closest("tr")!;
  expect(within(manual).getByText("Manual")).toBeTruthy();
  expect(within(manual).getByText("@operator.example")).toBeTruthy();
  const awaiting = screen.getByRole("table", {
    name: "Initial crawls awaiting submission",
  });
  expect(within(awaiting).getByText("https://waiting.example")).toBeTruthy();
  expect(within(awaiting).queryByText("https://accepted.example")).toBeNull();
});

test("polling continues for open initial crawls and pending source enables only", () => {
  const idle = directory();
  for (const phase of ["waiting", "retrying", "accepted"] as const)
    expect(crawlsInProgress(idle, [], null, [bootstrap(phase)])).toBe(true);
  for (const phase of [
    "needs_attention",
    "not_requested",
    "finished",
    "failed",
    "unknown",
    "skipped_not_configured",
    "skipped_not_https",
    "skipped_conflict",
  ] as const)
    expect(crawlsInProgress(idle, [], null, [bootstrap(phase)])).toBe(false);
  expect(crawlsInProgress(idle, [], enableOperation("requested", null))).toBe(
    true,
  );
  expect(crawlsInProgress(idle, [], enableOperation("applied", null))).toBe(
    false,
  );
});
