import { test, expect, vi, afterEach } from "vitest";
import { render, screen, fireEvent, cleanup } from "@testing-library/svelte";
import Sources from "../src/Sources.svelte";
import Collections from "../src/Collections.svelte";
import Operations from "../src/Operations.svelte";
import Limits from "../src/Limits.svelte";
import AccountQuota from "../src/AccountQuota.svelte";
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
  const input = screen.getByLabelText("Quota") as HTMLInputElement;
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
  await fireEvent.click(screen.getByRole("button", { name: "Remove app.bsky.feed.post" }));
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
  expect(screen.getByRole("alert").textContent).toContain("Enter a valid exact collection NSID.");
  await fireEvent.input(input, { target: { value: "app.bsky.feed.like" } });
  await fireEvent.click(screen.getByRole("button", { name: "Add collection" }));
  expect(screen.getByText("app.bsky.feed.like")).toBeTruthy();
  await fireEvent.input(input, { target: { value: "app.bsky.feed.like" } });
  await fireEvent.click(screen.getByRole("button", { name: "Add collection" }));
  expect(screen.getByRole("alert").textContent).toContain("already in this policy");
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
  await fireEvent.click(screen.getByRole("button", { name: "Request policy change" }));
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
  await fireEvent.click(screen.getByRole("button", { name: "Request policy change" }));
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
  await view.rerender({ policy: { revision: 4, collections: ["app.bsky.feed.repost"] } });
  expect(screen.getByText("app.bsky.feed.like")).toBeTruthy();
  await view.rerender({ policy: { revision: 5, collections: ["app.bsky.feed.repost"] } });
  expect(screen.getByRole("status").textContent).toContain("draft is preserved");
  await fireEvent.click(screen.getByRole("button", { name: "Request policy change" }));
  expect(submit).not.toHaveBeenCalled();
  await fireEvent.click(screen.getByRole("button", { name: "Use current policy" }));
  expect(screen.getByText("app.bsky.feed.repost")).toBeTruthy();
  await fireEvent.click(screen.getByRole("button", { name: "Request policy change" }));
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
  expect(handles[0].getAttribute("title")).toBe("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa");
  expect(screen.getAllByRole("button", { name: "Copy DID" }).length).toBe(2);
});

test("rate limits show current globals and PDS typeahead suggestions", async () => {
  render(Limits, {
    submit: vi.fn(),
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
  await fireEvent.change(screen.getByLabelText("Scope"), { target: { value: "pds" } });
  expect(document.querySelector('option[value="https://pds.example"]')).toBeTruthy();
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
        state: "running",
        completedRepos: 3,
        totalRepos: 10,
        attempts: 1,
        historyComplete: false,
        coverage: "partial",
      },
    ],
  });
  expect(screen.getByText(/Selected-collection backfill fills/)).toBeTruthy();
  expect(screen.getByText("PDS / Job id")).toBeTruthy();
  expect(screen.getByText("Collections")).toBeTruthy();
  expect(screen.getByText("Status")).toBeTruthy();
  expect(screen.getByText("3 of 10 repositories processed")).toBeTruthy();
  expect(screen.queryByText(/Revision/)).toBeNull();
});

test("coverage groups collections by collapsed PDS and explains unknown history", () => {
  render(Operations, {
    screen: "coverage",
    submit: vi.fn(),
    action: vi.fn(),
    coverage: [
      {
        pds: "https://pds.example",
        policy: { revision: 2, collections: ["app.bsky.feed.post"] },
        jobId: "abc",
        state: "complete",
        completedRepos: 3,
        reason: null,
        historyComplete: false,
        historicalPDSAttribution: "unknown",
      },
    ],
  });
  const group = screen.getByText("https://pds.example").closest("details");
  expect(group?.hasAttribute("open")).toBe(false);
  expect(document.body.textContent).toContain("do not preserve which PDS supplied older records");
  expect(screen.getByText("app.bsky.feed.post")).toBeTruthy();
  expect(screen.queryByText("Policy revision")).toBeNull();
  expect(screen.getByRole("link", { name: "Manage PDS instances" })).toBeTruthy();
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
  const fetcher = vi
    .fn()
    .mockResolvedValue({ ok: true, status: 200, json: async () => source });
  vi.stubGlobal("fetch", fetcher);
  try {
    render(Sources, { rows: [], submit: vi.fn() });
    await fireEvent.input(screen.getByLabelText("Filter to PDS"), {
      target: { value: "https://outside.example" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Filter" }));
    expect(screen.getByText("connected")).toBeTruthy();
    expect(fetcher).toHaveBeenCalledTimes(1);
  } finally {
    cleanup();
    vi.unstubAllGlobals();
  }
});
test("source overview shows admission and account quota metadata", () => {
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
  expect(screen.getByText("4 / 25 accounts")).toBeTruthy();
  expect(screen.getByText("State / runtime connection")).toBeTruthy();
  expect(screen.getByText(/historical collection counts are unavailable/)).toBeTruthy();
});
