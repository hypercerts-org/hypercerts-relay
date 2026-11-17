import { createHash } from "node:crypto";
import { z } from "zod";
import {
  ApiError,
  type CollectionBootstrapPhase,
  type CollectionBootstrapView,
  type CollectionCrawl,
  type Command,
  type Operation,
  type OperationState,
} from "./contracts.ts";
import { Store } from "./store.ts";
import { crawlCursor } from "./collection-crawls.ts";

// Automatic retries of an initial crawl are journaled under this actor.
export const bootstrapActor = "system:collection-bootstrap";
// Backoff between automatic retries: 30 s, doubling, at most 10 min, for 24 h
// after the source was enabled. After that an operator retries manually.
const retryBaseMs = 30_000;
const retryMaxMs = 10 * 60_000;
const retryWindowMs = 24 * 60 * 60_000;

export type BootstrapOutcome =
  "queued" | "not_configured" | "not_https" | "conflict" | "unchanged";

interface RelaySourceView {
  HostID: number;
  Hostname: string;
  NoSSL: boolean;
  DesiredState: string;
  Revision: number;
  Validation?: { Status: string };
}

interface BootstrapRow {
  sourceOperation: string;
  pds: string;
  hostId: number;
  sourceRevision: number;
  actor: string;
  outcome: CollectionBootstrapView["outcome"];
  crawlOperation: string | null;
  attempts: number;
  nextAttemptAt: string | null;
  gaveUpAt: string | null;
  createdAt: string;
}

// The follow-up crawl operation ID for a source operation: a UUID-shaped
// (version 8, RFC 4122 variant) SHA-256 digest, so a replay finds the same
// operation instead of queuing another.
export function bootstrapOperationId(sourceOperationId: string): string {
  const hex = createHash("sha256")
    .update(`collection-bootstrap:${sourceOperationId}`)
    .digest("hex");
  const variant = ((Number.parseInt(hex[16], 16) & 0x3) | 0x8).toString(16);
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-8${hex.slice(13, 16)}-${variant}${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
}

// Parses a client Idempotency-Key. Version 8 UUIDs are reserved for IDs the
// server derives, such as bootstrapOperationId, so a client cannot claim a
// follow-up operation's ID before the server queues it.
export function clientIdempotencyKey(value: unknown): string {
  const key = z.uuid().parse(value);
  if (key[14] === "8") throw new ApiError(400, "reserved_idempotency_key");
  return key;
}

function retryDelayMs(attempts: number) {
  return Math.min(retryBaseMs * 2 ** attempts, retryMaxMs);
}

// Requests an initial collection directory crawl when a source enable is
// applied, and retries it while the directory is unavailable.
export class CollectionBootstrap {
  constructor(
    private readonly store: Store,
    private readonly configured: boolean,
  ) {}

  // Decides the follow-up for an applied operation. Returns null unless the
  // operation enabled a source that Relay reports enabled and validated. The
  // caller runs commit() in the transaction that marks the operation applied.
  plan(
    op: Operation,
    result: unknown,
  ): { result: unknown; commit: () => void } | null {
    const command = op.command;
    if (command.kind !== "source" || command.state !== "enabled") return null;
    const relay = (result as { relay?: RelaySourceView } | null)?.relay;
    if (
      relay?.DesiredState !== "enabled" ||
      relay.Validation?.Status !== "passed" ||
      typeof relay.Hostname !== "string" ||
      !relay.Hostname
    )
      return null;
    const pds = `${relay.NoSSL ? "http" : "https"}://${relay.Hostname}`;
    const crawlOperation = bootstrapOperationId(op.id);
    const followUp: Command = { kind: "collection_crawl", scope: pds };
    const existing = this.store.operation(crawlOperation);
    let outcome: BootstrapOutcome;
    if (relay.NoSSL) outcome = "not_https";
    else if (!this.configured) outcome = "not_configured";
    // An enable that did not change the source keeps its revision; a crawl
    // already queued for that revision is not repeated.
    else if (
      this.store.db
        .prepare(
          "SELECT 1 FROM collection_bootstraps WHERE pds=? AND hostId=? AND sourceRevision=? AND outcome='queued' AND sourceOperation<>?",
        )
        .get(pds, relay.HostID, relay.Revision, op.id)
    )
      outcome = "unchanged";
    // Another request already holds the follow-up ID. The source is still
    // applied; the crawl is skipped and recorded instead.
    else if (
      existing &&
      (existing.actor !== op.actor ||
        JSON.stringify(existing.command) !== JSON.stringify(followUp))
    )
      outcome = "conflict";
    else outcome = "queued";
    const queued = outcome === "queued";
    return {
      result: {
        ...(result as object),
        collectionDirectory: {
          bootstrap: outcome,
          pds,
          ...(queued ? { operation: crawlOperation } : {}),
        },
      },
      commit: () => {
        let recorded = outcome;
        if (recorded === "queued") {
          // plan() already detects a conflicting ID. This guards the
          // transaction: a conflict must never roll back the source change.
          try {
            this.store.enqueue(op.actor, followUp, crawlOperation);
          } catch (e) {
            if (!(e instanceof ApiError && e.code === "idempotency_conflict"))
              throw e;
            recorded = "conflict";
          }
        }
        const recordedQueued = recorded === "queued";
        if (recorded !== "unchanged") {
          const inserted = this.store.db
            .prepare(
              "INSERT OR IGNORE INTO collection_bootstraps(sourceOperation,pds,hostId,sourceRevision,actor,outcome,crawlOperation,createdAt) VALUES(?,?,?,?,?,?,?,?)",
            )
            .run(
              op.id,
              pds,
              relay.HostID,
              relay.Revision,
              op.actor,
              recorded,
              recordedQueued ? crawlOperation : null,
              new Date().toISOString(),
            );
          // Already committed for this operation.
          if (!inserted.changes) return;
        }
        this.store.audit(
          op.actor,
          `collection_bootstrap_${recorded}`,
          { pds, crawlOperation: recordedQueued ? crawlOperation : null },
          op.id,
        );
      },
    };
  }

  // Moves initial crawls left incomplete (the directory was unavailable or
  // refused) back to requested after a backoff. Failed crawls are never
  // retried. Database errors propagate to the caller.
  retryDue(now = Date.now()): void {
    const rows = this.store.db
      .prepare(
        `SELECT b.sourceOperation, b.pds, b.crawlOperation, b.attempts, b.nextAttemptAt, b.createdAt, o.updatedAt
         FROM collection_bootstraps b JOIN operations o ON o.id=b.crawlOperation
         WHERE b.outcome='queued' AND b.gaveUpAt IS NULL AND o.state='incomplete'`,
      )
      .all() as unknown as (Pick<
      BootstrapRow,
      "sourceOperation" | "pds" | "attempts" | "nextAttemptAt" | "createdAt"
    > & { crawlOperation: string; updatedAt: string })[];
    for (const row of rows) {
      if (now - Date.parse(row.createdAt) >= retryWindowMs) {
        this.store.transaction(() => {
          const stopped = this.store.db
            .prepare(
              "UPDATE collection_bootstraps SET gaveUpAt=?,nextAttemptAt=NULL WHERE sourceOperation=? AND gaveUpAt IS NULL",
            )
            .run(new Date(now).toISOString(), row.sourceOperation);
          if (stopped.changes)
            this.store.audit(
              bootstrapActor,
              "collection_bootstrap_retry_exhausted",
              { pds: row.pds, attempts: row.attempts },
              row.crawlOperation,
            );
        });
        continue;
      }
      // Measured from the latest failure, so a failed manual retry also
      // waits a full interval.
      const next = new Date(
        Date.parse(row.updatedAt) + retryDelayMs(row.attempts),
      ).toISOString();
      if (row.nextAttemptAt !== next)
        this.store.db
          .prepare(
            "UPDATE collection_bootstraps SET nextAttemptAt=? WHERE sourceOperation=?",
          )
          .run(next, row.sourceOperation);
      if (Date.parse(next) > now) continue;
      const op = this.store.operation(row.crawlOperation)!;
      this.store.transition(
        op.id,
        "requested",
        op.result,
        null,
        bootstrapActor,
        ["incomplete"],
        () =>
          this.store.db
            .prepare(
              "UPDATE collection_bootstraps SET attempts=attempts+1,nextAttemptAt=NULL WHERE sourceOperation=?",
            )
            .run(row.sourceOperation),
      );
    }
  }

  // Newest first. The cursor is the last row's createdAt|sourceOperation.
  page(
    after: string,
    limit: number,
    pds?: string,
  ): { items: CollectionBootstrapView[]; next: string | null } {
    const [createdAt, id] = crawlCursor(after);
    const rows = this.store.db
      .prepare(
        `SELECT * FROM collection_bootstraps
         WHERE (?='' OR (createdAt,sourceOperation)<(?,?)) AND (?='' OR pds=?)
         ORDER BY createdAt DESC,sourceOperation DESC LIMIT ?`,
      )
      .all(
        createdAt,
        createdAt,
        id,
        pds ?? "",
        pds ?? "",
        limit + 1,
      ) as unknown as BootstrapRow[];
    return {
      items: rows.slice(0, limit).map((row) => this.view(row)),
      next:
        rows.length > limit
          ? `${rows[limit - 1].createdAt}|${rows[limit - 1].sourceOperation}`
          : null,
    };
  }

  private view(row: BootstrapRow): CollectionBootstrapView {
    const op = row.crawlOperation
      ? this.store.operation(row.crawlOperation)
      : undefined;
    const crawlRow = op
      ? (this.store.db
          .prepare(
            "SELECT * FROM collection_crawls WHERE operation=? AND pds=?",
          )
          .get(op.id, row.pds) as
          Omit<CollectionCrawl, "actorHandle" | "sourceOperation"> | undefined)
      : undefined;
    const crawl: CollectionCrawl | null = crawlRow
      ? {
          ...crawlRow,
          actorHandle: op!.actorHandle ?? null,
          sourceOperation: row.sourceOperation,
        }
      : null;
    return {
      sourceOperation: row.sourceOperation,
      pds: row.pds,
      actor: row.actor,
      actorHandle:
        this.store.operation(row.sourceOperation)?.actorHandle ?? null,
      createdAt: row.createdAt,
      outcome: row.outcome,
      crawlOperation: row.crawlOperation,
      phase: phase(row, op?.state, crawl),
      request: op
        ? {
            state: op.state,
            error: op.error,
            attempts: row.attempts,
            nextAttemptAt: row.nextAttemptAt,
            updatedAt: op.updatedAt,
          }
        : null,
      crawl,
    };
  }
}

function phase(
  row: BootstrapRow,
  state: OperationState | undefined,
  crawl: CollectionCrawl | null,
): CollectionBootstrapPhase {
  if (row.outcome === "not_configured") return "skipped_not_configured";
  if (row.outcome === "not_https") return "skipped_not_https";
  if (row.outcome === "conflict") return "skipped_conflict";
  switch (state) {
    case "requested":
    case "applying":
      return "waiting";
    case "incomplete":
      return row.gaveUpAt ? "needs_attention" : "retrying";
    case "failed":
    case "canceled":
      return "not_requested";
    case "applied":
      return crawl?.state ?? "unknown";
    default:
      return "unknown";
  }
}
