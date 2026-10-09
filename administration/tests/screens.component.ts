import { test, expect, vi, afterEach } from "vitest";
import { render, screen, fireEvent, cleanup, within } from "@testing-library/svelte";
import Sources from "../src/Sources.svelte";
import Collections from "../src/Collections.svelte";
import Operations from "../src/Operations.svelte";
import Limits from "../src/Limits.svelte";
import AccountQuota from "../src/AccountQuota.svelte";
import type { Job, JobDiagnostics } from "../server/contracts.ts";

type RepositoryJobOverrides = Partial<
  Omit<Job, "id" | "pds" | "policy" | "diagnostics">
> & { diagnostics?: Partial<JobDiagnostics> };

function repositoryJob(
  id: string,
  pds: string,
  overrides: RepositoryJobOverrides = {},
): Job {
  const { diagnostics, ...jobOverrides } = overrides;
  return {
    id,
    pds,
    policy: { revision: 2, collections: ["app.bsky.feed.post"] },
    reason: "backfill",
    state: "incomplete",
    completedRepos: 0,
    totalRepos: 2,
    totalReposKnown: true,
    attempts: 1,
    createdAt: "2026-09-09T00:00:00.000Z",
    coverage: "current_state",
    ...jobOverrides,
    diagnostics: {
      execution: "stopped",
      unresolvedRepos: 2,
      retryingRepos: 0,
      maxRepositoryAttempts: 3,
      ...diagnostics,
    },
  };
}

function renderJobs(jobs: Job[]) {
  return render(Operations, {
    screen: "jobs",
    submit: vi.fn(),
    action: vi.fn(),
    jobs,
  });
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
afterEach(cleanup);
test("quota drafts survive observations and reject stale or invalid changes", async () => {
  const source = {
    HostID: 1,
    Hostname: "quota.example",
    NoSSL: false,
    DesiredState: "enabled",
    RuntimeState: "connected",
    Revision: 1,
    RecoveryRequired: true,
    LastDurableCursor: 12,
    Validation: { Status: "passed", Reason: "" },
    AccountQuota: { Count: 10, Limit: 100 },
  };
  const submit = vi.fn().mockResolvedValue(undefined);
  const view = render(AccountQuota, { source, submit });
  const input = screen.getByLabelText("Admission quota") as HTMLInputElement;
  await fireEvent.input(input, { target: { value: "-1" } });
  await fireEvent.submit(input.form!);
  expect(submit).not.toHaveBeenCalled();
  expect(document.activeElement).toBe(input);
  await fireEvent.input(input, { target: { value: "250" } });
  await view.rerender({
    source: { ...source, AccountQuota: { Count: 11, Limit: 100 } },
  });
  expect(input.value).toBe("250");
  await fireEvent.submit(input.form!);
  expect(submit).toHaveBeenCalledWith({
    kind: "account_quota",
    pds: "https://quota.example",
    expectedLimit: 100,
    accountLimit: 250,
  });
  await view.rerender({
    source: { ...source, AccountQuota: { Count: 11, Limit: 250 } },
  });
  expect(
    screen.queryByRole("button", { name: "Use current quota" }),
  ).toBeNull();
  await fireEvent.input(input, { target: { value: "300" } });
  await fireEvent.submit(input.form!);
  expect(submit).toHaveBeenLastCalledWith({
    kind: "account_quota",
    pds: "https://quota.example",
    expectedLimit: 250,
    accountLimit: 300,
  });
  submit.mockClear();
  await view.rerender({
    source: { ...source, AccountQuota: { Count: 11, Limit: 150 } },
  });
  expect(input.value).toBe("300");
  await fireEvent.submit(input.form!);
  expect(submit).not.toHaveBeenCalled();
  await fireEvent.click(
    screen.getByRole("button", { name: "Use current quota" }),
  );
  expect(input.value).toBe("150");
});
test("source form submits a backend command without inventing an applied state", async () => {
  const submit = vi.fn().mockResolvedValue(undefined);
  render(Sources, { rows: [], submit });
  await fireEvent.input(screen.getByLabelText("PDS origin"), {
    target: { value: "pds.example" },
  });
  await fireEvent.click(screen.getByRole("button", { name: "Add source" }));
  expect(submit).toHaveBeenCalledWith({
    kind: "source",
    pds: "pds.example",
    state: "enabled",
  });
  expect(screen.queryByText("applied")).toBeNull();
});
test("collection drafts support removal and an explicit empty policy", async () => {
  const submit = vi.fn().mockResolvedValue(undefined);
  render(Collections, {
    policy: { revision: 4, collections: ["app.bsky.feed.post"] },
    submit,
  });
  await fireEvent.click(
    screen.getByRole("button", { name: "Remove app.bsky.feed.post" }),
  );
  await fireEvent.click(
    screen.getByRole("button", { name: "Request policy change" }),
  );
  expect(submit).toHaveBeenCalledWith({
    kind: "collections",
    expectedRevision: 4,
    collections: [],
  });
});
test("collection drafts reject invalid and duplicate NSIDs", async () => {
  render(Collections, {
    policy: { revision: 4, collections: ["app.bsky.feed.post"] },
    submit: vi.fn(),
  });
  const input = screen.getByLabelText("Collection NSID");
  await fireEvent.input(input, { target: { value: "not an nsid" } });
  await fireEvent.click(screen.getByRole("button", { name: "Add collection" }));
  expect(screen.getByRole("alert").textContent).toContain(
    "Enter a valid exact collection NSID.",
  );
  await fireEvent.input(input, { target: { value: "app.bsky.feed.like" } });
  await fireEvent.click(screen.getByRole("button", { name: "Add collection" }));
  expect(screen.getByText("app.bsky.feed.like")).toBeTruthy();
  await fireEvent.input(input, { target: { value: "app.bsky.feed.like" } });
  await fireEvent.click(screen.getByRole("button", { name: "Add collection" }));
  expect(screen.getByRole("alert").textContent).toContain(
    "already in this policy",
  );
});
test("collection suggestions use the pinned seed without restricting exact NSIDs", async () => {
  render(Collections, {
    policy: { revision: 4, collections: [] },
    submit: vi.fn(),
  });
  const input = screen.getByLabelText("Collection NSID") as HTMLInputElement;
  expect(input.getAttribute("list")).toBe("collection-suggestions");
  expect(
    document.querySelector('option[value="org.hypercerts.claim.activity"]'),
  ).toBeTruthy();
  await fireEvent.input(input, { target: { value: "app.bsky.feed.post" } });
  await fireEvent.click(screen.getByRole("button", { name: "Add collection" }));
  expect(screen.getByText("app.bsky.feed.post")).toBeTruthy();
});
test("collection acknowledgement adopts canonical collections at the new revision", async () => {
  const submit = vi.fn().mockResolvedValue(undefined);
  const view = render(Collections, {
    policy: { revision: 4, collections: ["app.bsky.feed.post"] },
    submit,
  });
  await fireEvent.input(screen.getByLabelText("Collection NSID"), {
    target: { value: "app.bsky.feed.like" },
  });
  await fireEvent.click(screen.getByRole("button", { name: "Add collection" }));
  await fireEvent.click(
    screen.getByRole("button", { name: "Request policy change" }),
  );
  expect(submit).toHaveBeenCalledWith({
    kind: "collections",
    expectedRevision: 4,
    collections: ["app.bsky.feed.post", "app.bsky.feed.like"],
  });
  await view.rerender({
    policy: {
      revision: 5,
      collections: ["app.bsky.feed.like", "app.bsky.feed.post"],
    },
  });
  expect(screen.queryByRole("status")).toBeNull();
  await fireEvent.click(
    screen.getByRole("button", { name: "Request policy change" }),
  );
  expect(submit).toHaveBeenLastCalledWith({
    kind: "collections",
    expectedRevision: 5,
    collections: ["app.bsky.feed.like", "app.bsky.feed.post"],
  });
});
test("collection drafts survive polling and require reset after a stale revision", async () => {
  const submit = vi.fn().mockResolvedValue(undefined);
  const policy = { revision: 4, collections: ["app.bsky.feed.post"] };
  const view = render(Collections, { policy, submit });
  await fireEvent.input(screen.getByLabelText("Collection NSID"), {
    target: { value: "app.bsky.feed.like" },
  });
  await fireEvent.click(screen.getByRole("button", { name: "Add collection" }));
  await view.rerender({
    policy: { revision: 4, collections: ["app.bsky.feed.repost"] },
  });
  expect(screen.getByText("app.bsky.feed.like")).toBeTruthy();
  await view.rerender({
    policy: { revision: 5, collections: ["app.bsky.feed.repost"] },
  });
  expect(screen.getByRole("status").textContent).toContain(
    "draft is preserved",
  );
  await fireEvent.click(
    screen.getByRole("button", { name: "Request policy change" }),
  );
  expect(submit).not.toHaveBeenCalled();
  await fireEvent.click(
    screen.getByRole("button", { name: "Use current policy" }),
  );
  expect(screen.getByText("app.bsky.feed.repost")).toBeTruthy();
  await fireEvent.click(
    screen.getByRole("button", { name: "Request policy change" }),
  );
  expect(submit).toHaveBeenCalledWith({
    kind: "collections",
    expectedRevision: 5,
    collections: ["app.bsky.feed.repost"],
  });
});
test("audit log combines requested changes and handle-first actor display", () => {
  render(Operations, {
    screen: "audit",
    submit: vi.fn(),
    action: vi.fn(),
    changes: [
      {
        id: "00000000-0000-4000-8000-000000000000",
        actor: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
        actorHandle: "operator.example",
        command: { kind: "limit", scope: "global", eventsPerSecond: 10 },
        state: "requested",
        createdAt: "2026-09-09T00:00:00.000Z",
        updatedAt: "2026-09-09T00:00:00.000Z",
        result: null,
        error: null,
      },
    ],
    hasMoreChanges: true,
    hasMoreAudit: true,
    audit: [
      {
        seq: 1,
        actor: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
        actorHandle: "operator.example",
        action: "requested",
        time: "2026-09-09T00:00:00.000Z",
        operation: "00000000-0000-4000-8000-000000000000",
        detail: {},
      },
    ],
  });
  expect(screen.getByText("Audit Log")).toBeTruthy();
  expect(screen.getByText("Requested changes")).toBeTruthy();
  expect(screen.getByText("Audit history")).toBeTruthy();
  const handles = screen.getAllByText("@operator.example");
  expect(handles[0].getAttribute("title")).toBe(
    "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
  );
  expect(
    screen.getAllByRole("button", { name: "Copy DID for @operator.example" })
      .length,
  ).toBe(2);
  expect(
    screen.getByRole("button", { name: "Load more requested changes" }),
  ).toBeTruthy();
  expect(
    screen.getByRole("button", { name: "Load more audit history" }),
  ).toBeTruthy();
});

test("rate limits show current globals and PDS typeahead suggestions", async () => {
  render(Limits, {
    submit: vi.fn(),
    globalLimits: [
      {
        scope: "global",
        eventsPerSecond: 100,
        waitingConnections: 2,
        unit: "events_per_second",
        recovery: "none",
      },
    ],
    sources: [
      {
        HostID: 1,
        Hostname: "pds.example",
        NoSSL: false,
        DesiredState: "enabled",
        RuntimeState: "connected",
        Revision: 1,
        RecoveryRequired: false,
        LastDurableCursor: 1,
        Validation: { Status: "passed", Reason: "" },
        AccountQuota: { Count: 0, Limit: 100 },
      },
    ],
    rows: [
      {
        scope: "global",
        eventsPerSecond: 100,
        waitingConnections: 2,
        unit: "events_per_second",
        recovery: "none",
      },
      {
        scope: "https://pds.example",
        eventsPerSecond: 10,
        waitingConnections: 0,
        unit: "events_per_second",
        recovery: "quota recovery required",
      },
    ],
  });
  expect(screen.getByText("Current global rate limits")).toBeTruthy();
  expect(screen.getAllByText(/100 events\/second/).length).toBeGreaterThan(0);
  expect(screen.getByText("Rate Limit policies")).toBeTruthy();
  await fireEvent.change(screen.getByLabelText("Scope"), {
    target: { value: "pds" },
  });
  expect(
    document.querySelector('option[value="https://pds.example"]'),
  ).toBeTruthy();
});

test("jobs explain recovery types and use operator-facing progress labels", () => {
  render(Operations, {
    screen: "jobs",
    submit: vi.fn(),
    action: vi.fn(),
    jobs: [
      {
        id: "abc",
        pds: "https://pds.example",
        policy: { revision: 2, collections: ["app.bsky.feed.post"] },
        reason: "quota_recovery",
        state: "running",
        completedRepos: 3,
        totalRepos: 10,
        totalReposKnown: true,
        attempts: 1,
        createdAt: "2026-09-09T00:00:00.000Z",
        coverage: "partial",
        diagnostics: {
          execution: "running",
          unresolvedRepos: 7,
          retryingRepos: 1,
          maxRepositoryAttempts: 3,
        },
      },
    ],
  });
  expect(screen.getByText(/Selected-collection backfill fills/)).toBeTruthy();
  expect(screen.getByText("PDS / Job id")).toBeTruthy();
  expect(screen.getByText("Collections")).toBeTruthy();
  expect(screen.getAllByText("Purpose")).toHaveLength(2);
  expect(screen.getAllByText("Quota recovery")).toHaveLength(2);
  expect(screen.getByText("Status")).toBeTruthy();
  expect(
    screen.getByText(
      "3 of 10 repositories from the initial inventory processed",
    ),
  ).toBeTruthy();
  expect(screen.queryByText(/Revision/)).toBeNull();
});

test("job execution diagnostics stay distinct from durable state and lazy details page safe retry data", async () => {
  const currentJob = repositoryJob("pending-job", "https://waiting.example", {
    completedRepos: 1,
    totalRepos: 3,
    attempts: 4,
  });
  const pages = [
    Response.json({
      job: currentJob,
      repositories: [
        {
          did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
          listedRevision: "3l3qo2vutsw2b",
          state: "retry_wait",
          attempts: 2,
          failure: {
            category: "http",
            httpStatus: 429,
            stage: "getRepo/request",
            code: "rate_limited",
          },
          retryAt: "2026-09-09T00:00:01.000Z",
        },
      ],
      nextCursor: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
    }),
    Response.json({
      job: currentJob,
      repositories: [
        {
          did: "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
          listedRevision: "3l3qo2vutsw2c",
          state: "ready",
          attempts: 0,
        },
      ],
    }),
  ];
  const fetcher = vi.fn().mockImplementation(() => Promise.resolve(pages.shift()));
  vi.stubGlobal("fetch", fetcher);
  try {
    const submit = vi.fn().mockResolvedValue(undefined);
    const view = render(Operations, {
      screen: "jobs",
      submit,
      action: vi.fn(),
      jobs: [
        {
          id: "pending-job",
          pds: "https://waiting.example",
          policy: { revision: 2, collections: ["app.bsky.feed.post"] },
          reason: "backfill",
          state: "pending",
          completedRepos: 1,
          totalRepos: 3,
          totalReposKnown: true,
          attempts: 4,
          createdAt: "2026-09-09T00:00:00.000Z",
          coverage: "current_state",
          diagnostics: {
            execution: "waiting",
            reason: "repository_retry",
            retryAt: "2026-09-09T00:00:01.000Z",
            unresolvedRepos: 2,
            retryingRepos: 1,
            maxRepositoryAttempts: 3,
          },
        },
        {
          id: "running-job",
          pds: "https://running.example",
          policy: { revision: 2, collections: [] },
          reason: "backfill",
          state: "running",
          completedRepos: 0,
          totalRepos: 1,
          totalReposKnown: true,
          attempts: 1,
          createdAt: "2026-09-09T00:00:00.000Z",
          coverage: "current_state",
          diagnostics: {
            execution: "running",
            unresolvedRepos: 1,
            retryingRepos: 1,
            maxRepositoryAttempts: 3,
          },
        },
        {
          id: "stopped-job",
          pds: "https://stopped.example",
          policy: { revision: 2, collections: [] },
          reason: "backfill",
          state: "canceled",
          completedRepos: 0,
          totalRepos: 1,
          totalReposKnown: true,
          attempts: 1,
          createdAt: "2026-09-09T00:00:00.000Z",
          coverage: "current_state",
          diagnostics: {
            execution: "stopped",
            unresolvedRepos: 1,
            retryingRepos: 0,
            maxRepositoryAttempts: 3,
          },
        },
        {
          id: "unknown-job",
          pds: "https://unknown.example",
          policy: { revision: 2, collections: [] },
          reason: "backfill",
          state: "incomplete",
          completedRepos: 12,
          totalRepos: 0,
          totalReposKnown: false,
          attempts: 1,
          createdAt: "2026-09-09T00:00:00.000Z",
          coverage: "current_state",
          diagnostics: {
            execution: "stopped",
            maxRepositoryAttempts: 3,
          },
        },
      ],
    });
    const pendingRow = screen.getByText("pending-job").closest("tr");
    const runningRow = screen.getByText("running-job").closest("tr");
    const stoppedRow = screen.getByText("stopped-job").closest("tr");
    expect(pendingRow).toBeTruthy();
    expect(runningRow).toBeTruthy();
    expect(stoppedRow).toBeTruthy();
    expect(within(pendingRow!).getByText(/Execution: waiting/)).toBeTruthy();
    expect(within(runningRow!).getByText(/Execution: running/)).toBeTruthy();
    expect(within(stoppedRow!).getByText(/Execution: stopped/)).toBeTruthy();
    expect(screen.getByRole("button", { name: "Inventory not known" })).toBeTruthy();
    expect(screen.queryByText("12 repositories processed")).toBeNull();
    expect(screen.getByText("Job claim attempts: 4")).toBeTruthy();
    expect(within(pendingRow!).getByText(/up to 3 attempts per coordinate/)).toBeTruthy();
    expect(document.body.textContent).toContain(
      "Diagnostics describe current execution, not historical completeness",
    );
    await fireEvent.click(
      screen.getByRole("button", { name: "View unresolved repositories (2)" }),
    );
    expect(await screen.findByText("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")).toBeTruthy();
    expect(within(pendingRow!).getByText(/Execution: stopped/)).toBeTruthy();
    expect(screen.getByText(/Work is stopped/)).toBeTruthy();
    expect(screen.getByText(/HTTP 429.*getRepo\/request/)).toBeTruthy();
    expect(screen.getByText(/rate limited/)).toBeTruthy();
    await fireEvent.click(
      screen.getByRole("button", { name: "Load more repositories" }),
    );
    expect(await screen.findByText("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")).toBeTruthy();
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(fetcher.mock.calls[0][0]).toBe(
      "/api/v1/jobs/pending-job/repositories?limit=50",
    );
    expect(fetcher.mock.calls[1][0]).toContain("after=did%3Aplc%3Aaaaaaaaa");

    await fireEvent.click(
      within(pendingRow!).getByRole("button", { name: "Retry job" }),
    );
    expect(submit).toHaveBeenCalledWith({
      kind: "job_action",
      id: "pending-job",
      action: "retry",
    });
    await view.rerender({
      jobs: [
        {
          ...currentJob,
          state: "running",
          attempts: 5,
          diagnostics: {
            execution: "running",
            unresolvedRepos: 2,
            retryingRepos: 1,
            maxRepositoryAttempts: 3,
          },
        },
      ],
    });
    const refreshedRow = screen.getByText("pending-job").closest("tr");
    expect(refreshedRow).toBeTruthy();
    expect(within(refreshedRow!).getByText("Execution: running")).toBeTruthy();
    expect(within(refreshedRow!).queryByText("Execution: stopped")).toBeNull();
    expect(screen.queryByText("Work is stopped")).toBeNull();
  } finally {
    cleanup();
    vi.unstubAllGlobals();
  }
});

test("expired repository snapshot resets accumulated rows and offers a first-page restart", async () => {
  const job = repositoryJob("expired-job", "https://expired.example");
  const pages = [
    Response.json({
      job,
      repositories: [
        {
          did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
          listedRevision: "3l3qo2vutsw2b",
          state: "ready",
          attempts: 0,
        },
      ],
      nextCursor: "opaque-first-cursor",
    }),
    Response.json({ error: "repository_snapshot_expired" }, { status: 410 }),
    Response.json({
      job: {
        ...job,
        state: "running",
        diagnostics: {
          execution: "running",
          unresolvedRepos: 2,
          retryingRepos: 0,
          maxRepositoryAttempts: 3,
        },
      },
      repositories: [
        {
          did: "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
          listedRevision: "3l3qo2vutsw2c",
          state: "ready",
          attempts: 0,
        },
      ],
    }),
  ];
  const fetcher = vi.fn().mockImplementation(() => Promise.resolve(pages.shift()));
  vi.stubGlobal("fetch", fetcher);
  try {
    renderJobs([job]);
    await fireEvent.click(
      screen.getByRole("button", { name: "View unresolved repositories (2)" }),
    );
    expect(await screen.findByText("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")).toBeTruthy();
    await fireEvent.click(
      screen.getByRole("button", { name: "Load more repositories" }),
    );
    const expiredMessage = await screen.findByRole("alert");
    expect(expiredMessage.textContent).toContain(
      "Repository snapshot expired. Restart from the first page.",
    );
    expect(screen.queryByText("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")).toBeNull();
    await fireEvent.click(
      screen.getByRole("button", { name: "Restart repository details" }),
    );
    expect(await screen.findByText("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")).toBeTruthy();
    const row = screen.getByText("expired-job").closest("tr");
    expect(row).toBeTruthy();
    expect(within(row!).getByText("Execution: running")).toBeTruthy();
    expect(fetcher).toHaveBeenCalledTimes(3);
    expect(fetcher.mock.calls[0][0]).toBe(
      "/api/v1/jobs/expired-job/repositories?limit=50",
    );
    expect(fetcher.mock.calls[1][0]).toContain("after=opaque-first-cursor");
    expect(fetcher.mock.calls[2][0]).toBe(
      "/api/v1/jobs/expired-job/repositories?limit=50",
    );
  } finally {
    cleanup();
    vi.unstubAllGlobals();
  }
});

test("reopening repository details keeps the newest first-page response and cursor", async () => {
  const job = repositoryJob("race-job", "https://race.example");
  const obsoleteJob = {
    ...job,
    diagnostics: { ...job.diagnostics, reason: "obsolete_snapshot" },
  };
  const newestJob = {
    ...job,
    diagnostics: { ...job.diagnostics, reason: "latest_snapshot" },
  };
  const obsoleteRequest = deferred<Response>();
  const reopenedRequest = deferred<Response>();
  const responses = [
    obsoleteRequest.promise,
    reopenedRequest.promise,
    Promise.resolve(
      Response.json({
        job: newestJob,
        repositories: [
          {
            did: "did:plc:cccccccccccccccccccccccc",
            listedRevision: "3l3qo2vutsw2d",
            state: "ready",
            attempts: 0,
          },
        ],
      }),
    ),
  ];
  const fetcher = vi.fn().mockImplementation(() => responses.shift());
  vi.stubGlobal("fetch", fetcher);
  try {
    render(Operations, {
      screen: "jobs",
      submit: vi.fn(),
      action: vi.fn(),
      jobs: [job],
    });
    await fireEvent.click(
      screen.getByRole("button", { name: "View unresolved repositories (2)" }),
    );
    await fireEvent.click(
      screen.getByRole("button", { name: "Hide repository details" }),
    );
    await fireEvent.click(
      screen.getByRole("button", { name: "View unresolved repositories (2)" }),
    );
    expect(fetcher).toHaveBeenCalledTimes(2);

    reopenedRequest.resolve(
      Response.json({
        job: newestJob,
        repositories: [
          {
            did: "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
            listedRevision: "3l3qo2vutsw2c",
            state: "retry_wait",
            attempts: 1,
          },
        ],
        nextCursor: "latest-cursor",
      }),
    );
    expect(
      await screen.findByText("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb"),
    ).toBeTruthy();
    obsoleteRequest.resolve(
      Response.json({
        job: obsoleteJob,
        repositories: [
          {
            did: "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
            listedRevision: "3l3qo2vutsw2b",
            state: "ready",
            attempts: 0,
          },
        ],
        nextCursor: "obsolete-cursor",
      }),
    );
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    expect(screen.getByText("did:plc:bbbbbbbbbbbbbbbbbbbbbbbb")).toBeTruthy();
    expect(screen.queryByText("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa")).toBeNull();
    expect(screen.getByText("Reason: latest snapshot")).toBeTruthy();
    await fireEvent.click(
      screen.getByRole("button", { name: "Load more repositories" }),
    );
    expect(await screen.findByText("did:plc:cccccccccccccccccccccccc")).toBeTruthy();
    expect(fetcher.mock.calls[2][0]).toBe(
      "/api/v1/jobs/race-job/repositories?limit=50&after=latest-cursor",
    );
  } finally {
    cleanup();
    vi.unstubAllGlobals();
  }
});

test("obsolete repository failure cannot replace the reopened request loading state", async () => {
  const job = repositoryJob("error-race-job", "https://error-race.example");
  const obsoleteRequest = deferred<Response>();
  const reopenedRequest = deferred<Response>();
  const responses = [obsoleteRequest.promise, reopenedRequest.promise];
  const fetcher = vi.fn().mockImplementation(() => responses.shift());
  vi.stubGlobal("fetch", fetcher);
  try {
    render(Operations, {
      screen: "jobs",
      submit: vi.fn(),
      action: vi.fn(),
      jobs: [job],
    });
    await fireEvent.click(
      screen.getByRole("button", { name: "View unresolved repositories (2)" }),
    );
    await fireEvent.click(
      screen.getByRole("button", { name: "Hide repository details" }),
    );
    await fireEvent.click(
      screen.getByRole("button", { name: "View unresolved repositories (2)" }),
    );

    obsoleteRequest.reject(new Error("obsolete fetch failed"));
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(screen.getByRole("status").textContent).toContain(
      "Loading repository details",
    );
    expect(screen.queryByRole("alert")).toBeNull();

    reopenedRequest.resolve(
      Response.json({
        job,
        repositories: [
          {
            did: "did:plc:dddddddddddddddddddddddd",
            listedRevision: "3l3qo2vutsw2e",
            state: "ready",
            attempts: 0,
          },
        ],
      }),
    );
    expect(
      await screen.findByText("did:plc:dddddddddddddddddddddddd"),
    ).toBeTruthy();
    expect(fetcher).toHaveBeenCalledTimes(2);
  } finally {
    cleanup();
    vi.unstubAllGlobals();
  }
});

test("hiding repository details ignores a late job snapshot", async () => {
  const job = repositoryJob("hidden-race-job", "https://hidden-race.example", {
    diagnostics: { reason: "initial_diagnostics" },
  });
  const request = deferred<Response>();
  const fetcher = vi.fn().mockImplementation(() => request.promise);
  vi.stubGlobal("fetch", fetcher);
  try {
    renderJobs([job]);
    await fireEvent.click(
      screen.getByRole("button", { name: "View unresolved repositories (2)" }),
    );
    await fireEvent.click(
      screen.getByRole("button", { name: "Hide repository details" }),
    );

    request.resolve(
      Response.json({
        job: {
          ...job,
          diagnostics: {
            ...job.diagnostics,
            reason: "late_diagnostics",
            unresolvedRepos: 1,
          },
        },
        repositories: [],
      }),
    );
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    const row = screen.getByText("hidden-race-job").closest("tr");
    expect(row).toBeTruthy();
    expect(within(row!).getByText("Reason: initial diagnostics")).toBeTruthy();
    expect(
      within(row!).getByRole("button", { name: "View unresolved repositories (2)" }),
    ).toBeTruthy();
    expect(within(row!).queryByText("Reason: late diagnostics")).toBeNull();
    expect(screen.queryByRole("heading", { name: "Unresolved repositories" })).toBeNull();
    expect(fetcher).toHaveBeenCalledTimes(1);
  } finally {
    cleanup();
    vi.unstubAllGlobals();
  }
});

test("switching jobs revokes the previous repository request", async () => {
  const firstJob = repositoryJob("first-switch-job", "https://first-switch.example", {
    diagnostics: { reason: "initial_diagnostics" },
  });
  const secondJob = {
    ...firstJob,
    id: "second-switch-job",
    pds: "https://second-switch.example",
  };
  const firstRequest = deferred<Response>();
  const secondRequest = deferred<Response>();
  const responses = [firstRequest.promise, secondRequest.promise];
  const fetcher = vi.fn().mockImplementation(() => responses.shift());
  vi.stubGlobal("fetch", fetcher);
  try {
    renderJobs([firstJob, secondJob]);
    const firstRow = screen.getByText(firstJob.id).closest("tr");
    const secondRow = screen.getByText(secondJob.id).closest("tr");
    expect(firstRow).toBeTruthy();
    expect(secondRow).toBeTruthy();
    await fireEvent.click(
      within(firstRow!).getByRole("button", { name: "View unresolved repositories (2)" }),
    );
    await fireEvent.click(
      within(secondRow!).getByRole("button", { name: "View unresolved repositories (2)" }),
    );

    firstRequest.resolve(
      Response.json({
        job: {
          ...firstJob,
          diagnostics: {
            ...firstJob.diagnostics,
            reason: "late_diagnostics",
            unresolvedRepos: 1,
          },
        },
        repositories: [],
      }),
    );
    await new Promise<void>((resolve) => setTimeout(resolve, 0));

    expect(within(firstRow!).getByText("Reason: initial diagnostics")).toBeTruthy();
    expect(screen.getByRole("status").textContent).toContain(
      "Loading repository details",
    );
    secondRequest.resolve(
      Response.json({
        job: secondJob,
        repositories: [
          {
            did: "did:plc:eeeeeeeeeeeeeeeeeeeeeeee",
            listedRevision: "3l3qo2vutsw2f",
            state: "ready",
            attempts: 0,
          },
        ],
      }),
    );
    expect(
      await screen.findByText("did:plc:eeeeeeeeeeeeeeeeeeeeeeee"),
    ).toBeTruthy();
    expect(fetcher).toHaveBeenCalledTimes(2);
  } finally {
    cleanup();
    vi.unstubAllGlobals();
  }
});

test("coverage groups collections by collapsed PDS and explains unknown history", () => {
  render(Operations, {
    screen: "coverage",
    submit: vi.fn(),
    action: vi.fn(),
    coverage: [
      {
        pds: "https://pds.example",
        policy: { revision: 1, collections: ["app.bsky.feed.post"] },
        jobId: "older",
        state: "incomplete",
        completedRepos: 1,
        totalRepos: 3,
        totalReposKnown: true,
        createdAt: "2026-09-08T00:00:00.000Z",
        reason: "source_unavailable",
        historicalPDSAttribution: "unknown",
        coverage: "current_state",
        diagnostics: {
          execution: "stopped",
          unresolvedRepos: 2,
          retryingRepos: 0,
          maxRepositoryAttempts: 3,
        },
      },
      {
        pds: "https://pds.example",
        policy: { revision: 2, collections: ["app.bsky.feed.post"] },
        jobId: "abc",
        state: "complete",
        completedRepos: 3,
        totalRepos: 3,
        totalReposKnown: true,
        createdAt: "2026-09-09T00:00:00.000Z",
        reason: null,
        historicalPDSAttribution: "unknown",
        coverage: "current_state",
        diagnostics: {
          execution: "complete",
          unresolvedRepos: 0,
          retryingRepos: 0,
          maxRepositoryAttempts: 3,
        },
      },
    ],
  });
  const group = screen.getByText("https://pds.example").closest("details");
  expect(group?.hasAttribute("open")).toBe(false);
  expect(document.body.textContent).toContain(
    "do not preserve which PDS supplied older records",
  );
  expect(screen.getAllByText("app.bsky.feed.post")).toHaveLength(1);
  expect(screen.getByText(/Policy revision 2; current state/)).toBeTruthy();
  expect(screen.getByText(/Execution: complete/)).toBeTruthy();
  expect(
    screen.getByRole("link", { name: "Manage PDS instances" }),
  ).toBeTruthy();
});

test("selected source detail is loaded on demand without background polling", async () => {
  const source = {
    HostID: 1,
    Hostname: "outside.example",
    NoSSL: false,
    DesiredState: "enabled",
    RuntimeState: "connected",
    Revision: 1,
    RecoveryRequired: true,
    LastDurableCursor: 12,
    Validation: { Status: "passed", Reason: "" },
    AccountQuota: { Count: 0, Limit: 100 },
  };
  const fetcher = vi.fn().mockImplementation(async (url: string) =>
    url.includes("/coverage")
      ? {
          ok: true,
          status: 200,
          json: async () => ({
            items: [
              {
                pds: "https://outside.example",
                policy: { revision: 2, collections: ["app.bsky.feed.post"] },
                jobId: "job-1",
                state: "complete",
                completedRepos: 43,
                totalRepos: 13895,
                totalReposKnown: true,
                createdAt: "2026-09-09T00:00:00.000Z",
                reason: null,
                historicalPDSAttribution: "unknown",
                coverage: "current_state",
                diagnostics: {
                  execution: "complete",
                  unresolvedRepos: 0,
                  retryingRepos: 0,
                  maxRepositoryAttempts: 3,
                },
              },
            ],
          }),
        }
      : { ok: true, status: 200, json: async () => source },
  );
  vi.stubGlobal("fetch", fetcher);
  try {
    render(Sources, { rows: [], submit: vi.fn() });
    await fireEvent.input(screen.getByLabelText("Filter to PDS"), {
      target: { value: "https://outside.example" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Filter" }));
    expect(await screen.findByText("connected")).toBeTruthy();
    expect(
      await screen.findByText(
        "43 of 13895 repositories from the initial inventory scanned",
      ),
    ).toBeTruthy();
    expect(screen.getByText("13895")).toBeTruthy();
    expect(screen.getByText("43")).toBeTruthy();
    expect(screen.getByText("Execution")).toBeTruthy();
    expect(document.body.textContent).toContain(
      "Up to 3 attempts per repository and job retry cycle",
    );
    expect(document.body.textContent).toContain(
      "Current snapshot only; historical coverage is unknown.",
    );
    const collections = screen
      .getByText("1 selected collections")
      .closest("details");
    expect(collections?.hasAttribute("open")).toBe(false);
    expect(
      screen.getByRole("button", { name: "Backfill collections" }),
    ).toBeTruthy();
    expect(fetcher).toHaveBeenCalledTimes(2);
  } finally {
    cleanup();
    vi.unstubAllGlobals();
  }
});
test("latest same-source detail refresh wins over an older response", async () => {
  const source = {
    HostID: 1,
    Hostname: "race.example",
    NoSSL: false,
    DesiredState: "enabled",
    RuntimeState: "connected",
    Revision: 1,
    RecoveryRequired: false,
    LastDurableCursor: 1,
    Validation: { Status: "passed", Reason: "" },
    AccountQuota: { Count: 4, Limit: 25 },
  };
  const oldCoverage = deferred<Response>();
  const newCoverage = deferred<Response>();
  const coverageResponses = [oldCoverage, newCoverage];
  const fetcher = vi
    .fn()
    .mockImplementation(() => coverageResponses.shift()!.promise);
  vi.stubGlobal("fetch", fetcher);
  try {
    render(Sources, { rows: [source], submit: vi.fn() });
    const select = screen.getByRole("button", { name: "race.example" });
    await fireEvent.click(select);
    await fireEvent.click(select);
    expect(fetcher).toHaveBeenCalledTimes(2);
    newCoverage.resolve(
      Response.json({
        items: [
          {
            pds: "https://race.example",
            policy: { revision: 1, collections: ["app.bsky.feed.post"] },
            jobId: "new",
            state: "complete",
            completedRepos: 20,
            totalRepos: 20,
            totalReposKnown: true,
            createdAt: "2026-09-09T00:00:00.000Z",
            reason: null,
            historicalPDSAttribution: "unknown",
            coverage: "current_state",
            diagnostics: {
              execution: "complete",
              unresolvedRepos: 0,
              retryingRepos: 0,
              maxRepositoryAttempts: 3,
            },
          },
        ],
      }),
    );
    expect(
      await screen.findByText(
        "20 of 20 repositories from the initial inventory scanned",
      ),
    ).toBeTruthy();
    oldCoverage.reject(new Error("stale coverage failure"));
    await Promise.resolve();
    expect(
      screen.getByText(
        "20 of 20 repositories from the initial inventory scanned",
      ),
    ).toBeTruthy();
    expect(
      screen.queryByText("Jetstream did not return coverage for this source."),
    ).toBeNull();
  } finally {
    cleanup();
    vi.unstubAllGlobals();
  }
});
test("source overview separates admission quota from Relay-observed accounts", () => {
  render(Sources, {
    rows: [
      {
        HostID: 1,
        Hostname: "overview.example",
        NoSSL: false,
        DesiredState: "enabled",
        RuntimeState: "connected",
        Revision: 1,
        RecoveryRequired: false,
        LastDurableCursor: 12,
        Validation: { Status: "passed", Reason: "reachable" },
        AccountQuota: { Count: 4, Limit: 25 },
      },
    ],
    submit: vi.fn(),
  });
  expect(screen.getByText("passed reachable")).toBeTruthy();
  expect(screen.getByText("25 accounts")).toBeTruthy();
  expect(screen.getByText("4")).toBeTruthy();
  expect(screen.getByText("Admission quota")).toBeTruthy();
  expect(screen.getByText("Total accounts")).toBeTruthy();
  expect(screen.getByText("Relay accounts")).toBeTruthy();
  expect(screen.getByText("Jetstream accounts")).toBeTruthy();
  expect(screen.getByText("State / runtime connection")).toBeTruthy();
  expect(
    screen.getByText(/historical collection counts are unavailable/),
  ).toBeTruthy();
});
