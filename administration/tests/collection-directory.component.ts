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
  crawlEligible,
  crawlNotice,
  crawlSummary,
  crawlsInProgress,
  createDirectoryPoller,
  directoryMessage,
  directoryPollMs,
  directoryRetryMs,
} from "../src/collectionDirectory";
import type {
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
  const fetcher = vi.fn(async () =>
    Response.json({ error: "unavailable" }, { status: 503 }),
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
  const fetcher = vi.fn(async (input: RequestInfo | URL) => {
    const url = String(input);
    if (url.startsWith("/api/v1/collection-directory/crawls"))
      return Response.json({ items: [older], next: null });
    return Response.json({ error: "unavailable" }, { status: 503 });
  });
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
        String(input) ===
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
