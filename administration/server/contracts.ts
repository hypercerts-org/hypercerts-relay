import { z } from "zod";
import { isValidNsid } from "@atproto/syntax";

export const origin = z
  .string()
  .max(300)
  .transform((raw, ctx) => {
    try {
      const trimmed = raw.trim();
      const u = new URL(
        trimmed.includes("://") ? trimmed : `https://${trimmed}`,
      );
      const local = ["localhost", "127.0.0.1", "[::1]"].includes(u.hostname);
      if (
        (u.protocol !== "https:" && !(local && u.protocol === "http:")) ||
        u.username ||
        u.password ||
        u.search ||
        u.hash ||
        u.pathname !== "/"
      )
        throw new Error("Invalid PDS origin");
      return u.origin;
    } catch {
      ctx.addIssue({
        code: "custom",
        message:
          "Use a PDS hostname or HTTPS origin without a path or credentials.",
      });
      return z.NEVER;
    }
  });
// Repository recovery scope bounds, matching Jetstream's control API.
export const recoveryScopeLimit = 4096;
export const recoveryDIDMaxLength = 512;
// A full scope of maximum-length DIDs: each DID adds its quotes and a
// separator, and the remaining fields fit in the default 64 KiB allowance.
export const operationBodyLimit =
  recoveryScopeLimit * (recoveryDIDMaxLength + 3) + 64 * 1024;
export const rate = z.number().int().min(1).max(1_000_000);
export const archiveKeyInput = z
  .object({
    name: z.string().trim().min(1).max(120),
    owner: z.string().trim().min(1).max(120),
    requestsPerMinute: z.number().int().min(1).max(100_000),
    archiveMegabytesPerMinute: z.number().int().min(1).max(100_000),
  })
  .strict();
export type ArchiveKeyInput = z.infer<typeof archiveKeyInput>;
export interface ArchiveKey extends ArchiveKeyInput {
  id: string;
  createdAt: string;
  revokedAt?: string;
}
export interface CreatedArchiveKey {
  key: ArchiveKey;
  token: string;
}
export const command = z.discriminatedUnion("kind", [
  z
    .object({
      kind: z.literal("account_quota"),
      pds: origin,
      expectedLimit: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
      accountLimit: z.number().int().min(0).max(Number.MAX_SAFE_INTEGER),
    })
    .strict(),
  z
    .object({
      kind: z.literal("source"),
      pds: origin,
      state: z.enum(["enabled", "disabled", "removed"]),
    })
    .strict(),
  z
    .object({
      kind: z.literal("collections"),
      expectedRevision: z.number().int().positive(),
      collections: z
        .array(
          z
            .string()
            .refine(
              (value): boolean => isValidNsid(value),
              "Use an exact collection NSID.",
            ),
        )
        .max(1000)
        .transform((v) => [...new Set(v)].sort(compareStrings)),
    })
    .strict(),
  z
    .object({
      kind: z.literal("job"),
      pds: origin,
      reason: z.enum(["backfill", "quota_recovery", "repository_recovery"]),
      // A repository-recovery job is scoped to exact DIDs that lost events to a
      // verify-queue overflow, rather than to the whole PDS.
      dids: z
        .array(
          z
            .string()
            .startsWith("did:", "Use an exact DID.")
            .max(recoveryDIDMaxLength),
        )
        .max(recoveryScopeLimit)
        .transform((v) => [...new Set(v)].sort(compareStrings))
        .optional(),
    })
    .strict(),
  z
    .object({
      kind: z.literal("job_action"),
      id: z.string().regex(/^[a-f0-9]{32}$/),
      action: z.enum(["retry", "cancel"]),
    })
    .strict(),
  z
    .object({
      kind: z.literal("limit"),
      scope: z.union([z.literal("global"), origin]),
      eventsPerSecond: rate,
    })
    .strict(),
  // Asks the collection directory to crawl one enabled source, or every
  // enabled source. Applied means the directory accepted the request.
  z
    .object({
      kind: z.literal("collection_crawl"),
      scope: z.union([z.literal("all"), origin]),
    })
    .strict(),
]);
export type Command = z.infer<typeof command>;
export type OperationState =
  "requested" | "applying" | "applied" | "failed" | "canceled" | "incomplete";
export interface Operation {
  id: string;
  actor: string;
  actorHandle?: string | null;
  command: Command;
  state: OperationState;
  createdAt: string;
  updatedAt: string;
  result: unknown;
  error: string | null;
}
// A crawl the collection directory reports as running. describeErrors is null
// when the directory does not report it.
export interface ActiveCrawl {
  pds: string;
  startedAt: string;
  reposDescribed: number;
  describeErrors: number | null;
}
// The latest ended crawl the collection directory retains for a source.
export interface EndedCrawl extends ActiveCrawl {
  finishedAt: string;
  state: "finished" | "failed";
  errorCategory: string | null;
  httpStatus: number | null;
}
export interface CollectionDirectoryStatus {
  configured: boolean;
  available: boolean;
  error: string | null;
  observedAt: string;
  processStartedAt: string | null;
  historySupported: boolean;
  active: ActiveCrawl[];
  recent: EndedCrawl[];
}
// One source accepted by the collection directory for one crawl request.
// Acceptance fields are fixed at submission; completion fields are observed.
export interface CollectionCrawl {
  id: string;
  operation: string;
  pds: string;
  actor: string;
  actorHandle: string | null;
  // "source_added" when the request was an automatic initial crawl.
  cause: string;
  // The source operation that queued an automatic initial crawl, else null.
  sourceOperation: string | null;
  submission: "started" | "already_active" | "accepted";
  state: "accepted" | "finished" | "failed" | "unknown";
  requestedAt: string;
  acceptedAt: string;
  crawlStartedAt: string | null;
  finishedAt: string | null;
  reposDescribed: number | null;
  describeErrors: number | null;
  errorCategory: string | null;
  httpStatus: number | null;
  observedAt: string | null;
}
// How an automatic initial crawl of a newly enabled source is progressing.
export type CollectionBootstrapPhase =
  | "skipped_not_configured"
  | "skipped_not_https"
  | "skipped_conflict"
  | "waiting"
  | "retrying"
  | "needs_attention"
  | "not_requested"
  | "accepted"
  | "finished"
  | "failed"
  | "unknown";
// One automatic initial crawl, recorded when a source enable was applied.
export interface CollectionBootstrapView {
  sourceOperation: string;
  pds: string;
  actor: string;
  actorHandle: string | null;
  createdAt: string;
  outcome: "queued" | "not_configured" | "not_https" | "conflict";
  crawlOperation: string | null;
  phase: CollectionBootstrapPhase;
  // The follow-up crawl operation; null when no crawl was queued.
  request: {
    state: OperationState;
    error: string | null;
    attempts: number;
    nextAttemptAt: string | null;
    updatedAt: string;
  } | null;
  crawl: CollectionCrawl | null;
}
export interface Policy {
  revision: number;
  collections: string[];
}
export interface JobDiagnostics {
  execution: "queued" | "running" | "waiting" | "stopped" | "complete";
  reason?: string;
  retryAt?: string;
  pdsCooldownUntil?: string;
  unresolvedRepos?: number;
  retryingRepos?: number;
  maxRepositoryAttempts: number;
}
export type RepositoryRetryState =
  "ready" | "in_flight" | "retry_wait" | "unresolved";
export interface RepositoryRetryFailure {
  category:
    | "timeout"
    | "transport"
    | "body_read"
    | "http"
    | "unknown"
    | "interrupted"
    | "rejected";
  httpStatus?: number;
  stage: "getRepo/request" | "getRepo/body";
  code?: string;
}
export interface JobLastFailure {
  category:
    "timeout" | "cancel" | "body_read" | "http" | "transport" | "unknown";
  httpStatus?: number;
  stage: "listRepos";
  failedAt: string;
}
export interface RepositoryDetail {
  did: string;
  listedRevision: string;
  state: RepositoryRetryState;
  attempts: number;
  failure?: RepositoryRetryFailure;
  failedAt?: string;
  retryAt?: string;
}
export interface RepositoryDetailsPage {
  job: Job;
  repositories: RepositoryDetail[];
  nextCursor?: string;
}
// Job-list status filters accepted by Jetstream: one durable job state, or
// "active" for work that is pending or running.
export const jobStateFilters = [
  "active",
  "pending",
  "running",
  "incomplete",
  "failed",
  "canceled",
  "complete",
] as const;
export type JobStateFilter = (typeof jobStateFilters)[number];
export interface Job {
  id: string;
  pds: string;
  policy: Policy;
  reason: string;
  state: string;
  completedRepos: number;
  totalRepos: number;
  totalReposKnown: boolean;
  attempts: number;
  errorCode?: string;
  // Omitted when Jetstream has no recorded creation time.
  createdAt?: string;
  lastProgressAt?: string;
  lastFailure?: JobLastFailure;
  coverage: string;
  acquisition?: Acquisition;
  diagnostics: JobDiagnostics;
}
export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
  ) {
    super(code);
  }
}

function compareStrings(a: string, b: string) {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

// Version 1 Go-owned snapshot; null means no durable evidence, never zero.
export type AcquisitionState =
  | "unknown"
  | "running"
  | "retry_waiting"
  | "stopped_incomplete"
  | "failed"
  | "canceled"
  | "complete";
export interface Acquisition {
  state: AcquisitionState;
  progressScope: "job_inventory";
  counts: {
    initialInventory: number | null;
    scanned: number | null;
    matching: number | null;
    noMatch: number | null;
    unresolved: number | null;
    attributableRecords: number | null;
  };
  diagnostics: {
    failureCategory?: string;
    lastProgressAt: string | null;
    attempts: number;
    affectedRepository?: string;
    retryAction?: string;
    retryAt?: string;
    pdsCooldownUntil?: string;
    waitReason?: "repository_retry" | "pds_cooldown";
  };
}
export interface CoverageSource {
  pds: string;
  sourceRevision: number;
  policy: Policy;
  matchingJob: boolean;
  jobId?: string;
  acquisition: Acquisition;
  history: { state: "unknown" };
  live: { state: "unknown" };
  // Legacy execution fields exist only with matchingJob=true.
  diagnostics?: JobDiagnostics;
  lastProgressAt?: string;
  lastFailure?: JobLastFailure;
  reason?: string;
  state?: string;
  completedRepos?: number | null;
  totalRepos?: number | null;
  totalReposKnown?: boolean;
  errorCode?: string;
  createdAt?: string;
  coverage?: string;
}
export interface CoveragePage {
  schemaVersion: 1;
  scope: "enabled_sources_current_policy";
  policy: Policy;
  items: CoverageSource[];
  aggregate: {
    scope: "enabled_sources_current_policy";
    policy: Policy;
    enabledSources: number;
    state: "unknown" | "incomplete" | "complete";
  };
  nextCursor?: string;
}
