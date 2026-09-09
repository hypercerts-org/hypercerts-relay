import { test, expect, vi, afterEach } from "vitest";
import { render, screen, fireEvent, cleanup } from "@testing-library/svelte";
import Sources from "../src/Sources.svelte";
import Collections from "../src/Collections.svelte";
import Operations from "../src/Operations.svelte";
afterEach(cleanup);
test("source form submits a backend command without inventing an applied state", async () => {
  const submit = vi.fn().mockResolvedValue(undefined);
  render(Sources, { rows: [], submit });
  await fireEvent.input(screen.getByLabelText("PDS origin"), {
    target: { value: "https://pds.example" },
  });
  await fireEvent.click(screen.getByRole("button", { name: "Add source" }));
  expect(submit).toHaveBeenCalledWith({
    kind: "source",
    pds: "https://pds.example",
    state: "enabled",
  });
  expect(screen.queryByText("applied")).toBeNull();
});
test("empty collection selection remains an explicit editable policy", async () => {
  const submit = vi.fn().mockResolvedValue(undefined);
  render(Collections, {
    policy: { revision: 4, collections: ["app.bsky.feed.post"] },
    submit,
  });
  await fireEvent.input(screen.getByLabelText("Enabled collection NSIDs"), {
    target: { value: "" },
  });
  await fireEvent.click(
    screen.getByRole("button", { name: "Request policy change" }),
  );
  expect(submit).toHaveBeenCalledWith({
    kind: "collections",
    expectedRevision: 4,
    collections: [],
  });
});
test("complete current-state coverage still discloses unknown history", () => {
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
  expect(screen.getByText("Historical coverage: incomplete")).toBeTruthy();
  expect(screen.getByText("unknown")).toBeTruthy();
});

test("selected source detail refreshes even when found outside the current page", async () => {
  vi.useFakeTimers();
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
    await fireEvent.input(screen.getByLabelText("Find a source by origin"), {
      target: { value: "https://outside.example" },
    });
    await fireEvent.click(screen.getByRole("button", { name: "Find source" }));
    expect(screen.getByText("connected")).toBeTruthy();
    fetcher.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        ...source,
        RuntimeState: "disabled",
        LastDurableCursor: 50,
      }),
    });
    await vi.advanceTimersByTimeAsync(5000);
    expect(screen.getByText("disabled")).toBeTruthy();
    expect(screen.getByText("50")).toBeTruthy();
    fetcher.mockRejectedValue(new Error("offline"));
    await vi.advanceTimersByTimeAsync(5000);
    expect(screen.getByText(/Latest observation unavailable/)).toBeTruthy();
  } finally {
    cleanup();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  }
});
