import { afterEach, expect, test, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/svelte";
import AcquisitionEvidence from "../src/AcquisitionEvidence.svelte";
import Operations from "../src/Operations.svelte";
afterEach(cleanup);
const complete = {
  state: "complete" as const,
  progressScope: "job_inventory" as const,
  counts: {
    initialInventory: 14541,
    scanned: 14541,
    matching: 2,
    noMatch: 14539,
    unresolved: 0,
    attributableRecords: 3,
  },
  diagnostics: { lastProgressAt: "2026-10-22T12:00:00Z", attempts: 0 },
};
test("sparse complete evidence distinguishes successful no-match work and zero", () => {
  render(AcquisitionEvidence, { acquisition: complete });
  expect(screen.getByText("complete")).toBeTruthy();
  expect(screen.getByText("14539")).toBeTruthy();
  expect(screen.getByText("0", { selector: "dd" })).toBeTruthy();
  expect(
    screen.getByText(/No-match repositories are successfully scanned/),
  ).toBeTruthy();
  expect(screen.queryByText(/2 of 14541 complete/)).toBeNull();
});
test("unknown acquisition and null values never become zero", () => {
  const unknown = {
    state: "unknown" as const,
    progressScope: "job_inventory" as const,
    counts: {
      initialInventory: null,
      scanned: null,
      matching: null,
      noMatch: null,
      unresolved: null,
      attributableRecords: null,
    },
    diagnostics: { lastProgressAt: "invalid", attempts: 0 },
  };
  render(AcquisitionEvidence, { acquisition: unknown });
  expect(screen.getAllByText("Unknown").length).toBeGreaterThanOrEqual(6);
  expect(screen.getByText("Attempts: 0")).toBeTruthy();
  expect(screen.getByText(/Last progress: Unknown/)).toBeTruthy();
});
test("all-enabled aggregate is authoritative across empty item pages", () => {
  const view = render(Operations, {
    screen: "coverage",
    coverage: {
      schemaVersion: 1,
      scope: "enabled_sources_current_policy",
      policy: { revision: 9, collections: [] },
      items: [
        {
          pds: "https://one.example",
          sourceRevision: 1,
          policy: { revision: 9, collections: [] },
          matchingJob: false,
          acquisition: {
            state: "unknown",
            progressScope: "job_inventory",
            counts: {
              initialInventory: null,
              scanned: null,
              matching: null,
              noMatch: null,
              unresolved: null,
              attributableRecords: null,
            },
            diagnostics: { lastProgressAt: null, attempts: 0 },
          },
          history: { state: "unknown" },
          live: { state: "unknown" },
        },
      ],
      aggregate: {
        scope: "enabled_sources_current_policy",
        policy: { revision: 9, collections: [] },
        enabledSources: 3,
        state: "incomplete",
      },
      next: null,
    },
    submit: async () => {},
    action: async () => {},
  });
  expect(screen.getByText(/3 enabled sources/)).toBeTruthy();
  expect(screen.getByText(/incomplete/)).toBeTruthy();
  view.rerender({
    screen: "coverage",
    coverage: {
      schemaVersion: 1,
      scope: "enabled_sources_current_policy",
      policy: { revision: 9, collections: [] },
      items: [],
      aggregate: {
        scope: "enabled_sources_current_policy",
        policy: { revision: 9, collections: [] },
        enabledSources: 3,
        state: "incomplete",
      },
      next: null,
    },
    submit: async () => {},
    action: async () => {},
  });
  expect(screen.getByText(/3 enabled sources/)).toBeTruthy();
  expect(screen.getByText(/No coverage results on this page/)).toBeTruthy();
});
test("failed partial shows explicit retry and bounded diagnostics without healthy tint", () => {
  const acquisition = {
    state: "failed" as const,
    progressScope: "job_inventory" as const,
    counts: {
      initialInventory: 14541,
      scanned: 5980,
      matching: 2,
      noMatch: 5978,
      unresolved: 8561,
      attributableRecords: 3,
    },
    diagnostics: {
      failureCategory: "source_unavailable",
      lastProgressAt: "2026-10-22T12:00:00Z",
      attempts: 3,
      affectedRepository: "did:plc:example",
      retryAction: "explicit_retry",
    },
  };
  const { container } = render(AcquisitionEvidence, { acquisition });
  expect(screen.getByText("5980")).toBeTruthy();
  expect(screen.getByText(/Explicit retry required/)).toBeTruthy();
  expect(screen.getByText(/did:plc:example/)).toBeTruthy();
  expect(container.querySelector(".good")).toBeNull();
});
test("jobs show authoritative acquisition independently from execution controls", () => {
  const job = {
    diagnostics: {
      execution: "stopped" as const,
      unresolvedRepos: 8561,
      retryingRepos: 1,
      maxRepositoryAttempts: 3,
    },
    id: "job-evidence",
    pds: "https://failed.example",
    policy: { revision: 9, collections: ["app.bsky.feed.post"] },
    reason: "backfill",
    state: "failed",
    completedRepos: 0,
    totalRepos: 14541,
    totalReposKnown: true,
    attempts: 3,
    createdAt: "2026-10-22T12:00:00Z",
    coverage: "current_state",
    acquisition: {
      state: "failed" as const,
      progressScope: "job_inventory" as const,
      counts: {
        initialInventory: 14541,
        scanned: 5980,
        matching: 2,
        noMatch: 5978,
        unresolved: 8561,
        attributableRecords: 3,
      },
      diagnostics: {
        failureCategory: "source_unavailable",
        lastProgressAt: "2026-10-22T12:00:00Z",
        attempts: 3,
        affectedRepository: "did:plc:example",
        retryAction: "explicit_retry",
      },
    },
  };
  const submit = vi.fn(async () => {});
  render(Operations, {
    screen: "jobs",
    jobs: [job],
    submit,
    action: async () => {},
  });
  expect(screen.getByText("5980")).toBeTruthy();
  expect(screen.getByText(/Failure category: source unavailable/)).toBeTruthy();
  expect(screen.getAllByText(/Last progress:/)[0]).toBeTruthy();
  expect(screen.getByText("Attempts: 3")).toBeTruthy();
  expect(screen.getByText(/did:plc:example/)).toBeTruthy();
  expect(screen.getByText("Explicit retry required")).toBeTruthy();
  expect(screen.getByText(/Execution: stopped/)).toBeTruthy();
  expect(
    screen.getByText(/History: Unknown; live freshness: Unknown/),
  ).toBeTruthy();
  expect(screen.getByRole("button", { name: "Retry job" })).toBeTruthy();
});
test("retry waiting conveys no timing", () => {
  render(AcquisitionEvidence, {
    acquisition: {
      state: "retry_waiting",
      progressScope: "job_inventory",
      counts: {
        initialInventory: null,
        scanned: null,
        matching: null,
        noMatch: null,
        unresolved: null,
        attributableRecords: null,
      },
      diagnostics: { lastProgressAt: null, attempts: 1 },
    },
  });
  expect(screen.getByText("Waiting; timing unknown.")).toBeTruthy();
  expect(document.body.textContent).not.toMatch(/ETA|next retry|countdown/i);
});

test("retry waiting renders recorded deadlines and classified failure without inventing timing", () => {
  render(AcquisitionEvidence, {
    acquisition: {
      ...complete,
      state: "retry_waiting",
      diagnostics: {
        ...complete.diagnostics,
        failureCategory: "http",
        retryAt: "2026-10-22T12:01:00Z",
        pdsCooldownUntil: "2026-10-22T12:02:00Z",
        waitReason: "pds_cooldown",
      },
    },
  });
  expect(
    screen.getByText(
      `Next repository retry: ${new Date("2026-10-22T12:01:00Z").toLocaleString()}`,
    ),
  ).toBeTruthy();
  expect(
    screen.getByText(
      `PDS cooldown until: ${new Date("2026-10-22T12:02:00Z").toLocaleString()}`,
    ),
  ).toBeTruthy();
  expect(screen.getByText("Wait reason: pds cooldown")).toBeTruthy();
  expect(screen.getByText("Failure category: http")).toBeTruthy();
  expect(screen.queryByText("Waiting; timing unknown.")).toBeNull();
});
