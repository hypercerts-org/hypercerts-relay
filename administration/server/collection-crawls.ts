import { randomUUID } from "node:crypto";
import {
  ApiError,
  type CollectionCrawl,
  type CollectionDirectoryStatus,
  type EndedCrawl,
  type Operation,
} from "./contracts.ts";
import { Store } from "./store.ts";
import { Services } from "./services.ts";
import {
  Collectiondir,
  CollectiondirError,
  crawlSubmissionChunk,
  type CollectiondirStatus,
  type CrawlSubmission,
} from "./collectiondir.ts";

// How often accepted crawls are compared with the collection directory.
const reconcileIntervalMs = 5000;
// An accepted crawl absent from both active and retained results after this
// long has an outcome the directory no longer reports.
const resultGraceMs = 30000;
// How long one directory observation answers browser status requests.
const statusCacheMs = 2000;
// The most active and recent crawls returned to the browser. Reconciliation
// always uses the directory's complete lists.
const browserListLimit = 1000;

interface RelaySourceView {
  Hostname: string;
  NoSSL: boolean;
  DesiredState: string;
  Validation?: { Status: string };
}
type Ineligibility = "not_enabled" | "not_https";

function ineligibility(source: RelaySourceView): Ineligibility | null {
  if (
    source.DesiredState !== "enabled" ||
    source.Validation?.Status !== "passed"
  )
    return "not_enabled";
  if (source.NoSSL) return "not_https";
  return null;
}

type CrawlRow = Omit<CollectionCrawl, "actorHandle">;

// Records manual collection directory crawl requests and follows each accepted
// source until the directory reports how its crawl ended.
export class CollectionCrawls {
  private lastReconcile = Number.NEGATIVE_INFINITY;
  private reconciling = false;
  // The latest directory read, successful or not.
  private observation: {
    at: number;
    status?: CollectiondirStatus;
    error?: string;
  } | null = null;
  private observing: Promise<CollectiondirStatus> | null = null;
  constructor(
    private readonly store: Store,
    private readonly services: Services,
    private readonly collectiondir: Collectiondir,
  ) {}

  get configured() {
    return this.collectiondir.configured;
  }

  // Called by the Worker. Eligibility is checked against Relay at apply time,
  // and the crawl target is always rebuilt from Relay's stored hostname.
  async apply(op: Operation): Promise<unknown> {
    const command = op.command;
    if (command.kind !== "collection_crawl")
      throw new ApiError(400, "invalid_command");
    if (!this.collectiondir.configured)
      throw new CollectiondirError("not_configured");
    const { targets, skipped } =
      command.scope === "all"
        ? await this.enabledTargets()
        : {
            targets: [await this.sourceTarget(command.scope)],
            skipped: { not_enabled: 0, not_https: 0 },
          };
    const submissions = await this.submit(op, targets);
    return {
      collectiondir: {
        submitted: submissions.length,
        started: submissions.filter((s) => s.outcome === "started").length,
        alreadyActive: submissions.filter((s) => s.outcome === "already_active")
          .length,
        skipped,
      },
    };
  }

  // Every eligible source across Relay's pages, counting the ineligible ones.
  private async enabledTargets(): Promise<{
    targets: string[];
    skipped: Record<Ineligibility, number>;
  }> {
    const skipped: Record<Ineligibility, number> = {
      not_enabled: 0,
      not_https: 0,
    };
    const targets: string[] = [];
    let after = "";
    for (;;) {
      const page = (await this.services.sources(after)) as unknown as {
        Sources: RelaySourceView[];
        NextAfterHostID: number;
      };
      for (const source of page.Sources) {
        const reason = ineligibility(source);
        if (reason) skipped[reason]++;
        else targets.push(`https://${source.Hostname}`);
      }
      if (!page.NextAfterHostID) break;
      after = String(page.NextAfterHostID);
    }
    if (!targets.length) throw new ApiError(409, "no_enabled_sources");
    return { targets, skipped };
  }

  // The one requested source, which must be approved, enabled and HTTPS.
  private async sourceTarget(scope: string): Promise<string> {
    let view: RelaySourceView;
    try {
      view = await this.services.call<RelaySourceView>(
        "relay",
        `/source?pds=${encodeURIComponent(scope)}`,
      );
    } catch (e) {
      if (e instanceof ApiError && e.status === 404)
        throw new ApiError(409, "source_not_approved");
      throw e;
    }
    const reason = ineligibility(view);
    if (reason === "not_enabled") throw new ApiError(409, "source_not_enabled");
    if (reason === "not_https") throw new ApiError(409, "source_not_https");
    return `https://${view.Hostname}`;
  }

  private async submit(
    op: Operation,
    targets: string[],
  ): Promise<CrawlSubmission[]> {
    // Fail before submitting when the directory is unreachable or rejects
    // the token.
    await this.collectiondir.status();
    // Each accepted chunk is recorded before the next is sent, so a later
    // failure leaves the operation incomplete without losing accepted crawls.
    // A retry finds those crawls running and records nothing twice.
    const submissions: CrawlSubmission[] = [];
    for (let i = 0; i < targets.length; i += crawlSubmissionChunk) {
      const chunk = targets.slice(i, i + crawlSubmissionChunk);
      const accepted = await this.collectiondir.requestCrawl(chunk); // NOSONAR: sequential by design, see above
      this.record(op, accepted);
      submissions.push(...accepted);
    }
    return submissions;
  }

  private record(op: Operation, accepted: CrawlSubmission[]) {
    const acceptedAt = new Date().toISOString();
    const insert = this.store.db.prepare(
      `INSERT OR IGNORE INTO collection_crawls(id,operation,pds,actor,cause,submission,state,requestedAt,acceptedAt,crawlStartedAt)
       VALUES(?,?,?,?,'manual',?,'accepted',?,?,?)`,
    );
    this.store.transaction(() => {
      for (const submission of accepted) {
        const inserted = insert.run(
          randomUUID(),
          op.id,
          submission.pds,
          op.actor,
          submission.outcome,
          op.createdAt,
          acceptedAt,
          submission.startedAt,
        );
        if (inserted.changes)
          this.store.audit(
            op.actor,
            "collection_crawl_accepted",
            { pds: submission.pds, submission: submission.outcome },
            op.id,
          );
      }
    });
  }

  // Compares accepted crawls with the directory. Self-throttled. A failed
  // status read leaves every row accepted for the next attempt; database
  // errors propagate to the caller.
  async reconcile(now = Date.now()): Promise<void> {
    if (this.reconciling || now - this.lastReconcile < reconcileIntervalMs)
      return;
    const pending = this.store.db
      .prepare(
        "SELECT * FROM collection_crawls WHERE state='accepted' ORDER BY requestedAt,id",
      )
      .all() as unknown as CrawlRow[];
    if (!pending.length) return;
    this.lastReconcile = now;
    // Without a directory nothing can report these crawls again.
    if (!this.configured) {
      const observedAt = new Date(now).toISOString();
      for (const row of pending)
        this.finish(
          row,
          "unknown",
          observedAt,
          null,
          "collectiondir_not_configured",
        );
      return;
    }
    this.reconciling = true;
    try {
      let status: CollectiondirStatus;
      try {
        status = await this.observe();
      } catch {
        return;
      }
      this.applyObservation(pending, status, now);
    } finally {
      this.reconciling = false;
    }
  }

  // One directory status read at a time; its outcome also answers browser
  // status requests for statusCacheMs.
  private observe(timeoutMs?: number): Promise<CollectiondirStatus> {
    this.observing ??= this.collectiondir
      .status(timeoutMs)
      .then(
        (status) => {
          this.observation = { at: Date.now(), status };
          return status;
        },
        (e: unknown) => {
          this.observation = {
            at: Date.now(),
            error: e instanceof ApiError ? e.code : "collectiondir_unavailable",
          };
          throw e;
        },
      )
      .finally(() => {
        this.observing = null;
      });
    return this.observing;
  }

  // A caller with a shorter deadline than a read already in flight stops
  // waiting at its own deadline.
  private async observeWithin(timeoutMs?: number) {
    if (timeoutMs === undefined) return this.observe();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      return await Promise.race([
        this.observe(timeoutMs),
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new CollectiondirError("unavailable")),
            timeoutMs,
          );
        }),
      ]);
    } finally {
      clearTimeout(timer);
    }
  }

  private applyObservation(
    pending: CrawlRow[],
    status: CollectiondirStatus,
    now: number,
  ) {
    const active = new Map(status.active.map((crawl) => [crawl.pds, crawl]));
    const recent = new Map(status.recent.map((crawl) => [crawl.pds, crawl]));
    const processStarted = status.processStartedAt
      ? Date.parse(status.processStartedAt)
      : Number.NaN;
    const progress = this.store.db.prepare(
      "UPDATE collection_crawls SET crawlStartedAt=?,reposDescribed=?,describeErrors=?,observedAt=? WHERE id=? AND state='accepted'",
    );
    for (const row of pending) {
      const running = active.get(row.pds);
      if (
        running &&
        (row.crawlStartedAt === null ||
          row.crawlStartedAt === running.startedAt)
      ) {
        progress.run(
          running.startedAt,
          running.reposDescribed,
          running.describeErrors,
          status.observedAt,
          row.id,
        );
        continue;
      }
      const ended = recent.get(row.pds);
      if (row.crawlStartedAt === ended?.startedAt) {
        this.finish(row, ended.state, status.observedAt, ended);
        continue;
      }
      const reference = Date.parse(row.crawlStartedAt ?? row.acceptedAt);
      if (processStarted > reference) {
        this.finish(
          row,
          "unknown",
          status.observedAt,
          null,
          "collectiondir_restarted",
        );
        continue;
      }
      if (now - Date.parse(row.acceptedAt) > resultGraceMs)
        this.finish(
          row,
          "unknown",
          status.observedAt,
          null,
          status.historySupported
            ? "result_not_retained"
            : "status_history_unsupported",
        );
    }
  }

  private finish(
    row: CrawlRow,
    state: "finished" | "failed" | "unknown",
    observedAt: string,
    ended: EndedCrawl | null,
    reason: string | null = null,
  ) {
    const outcome = {
      finishedAt: ended?.finishedAt ?? null,
      reposDescribed: ended?.reposDescribed ?? row.reposDescribed,
      describeErrors: ended?.describeErrors ?? row.describeErrors,
      errorCategory: ended ? ended.errorCategory : reason,
      httpStatus: ended?.httpStatus ?? null,
    };
    this.store.transaction(() => {
      const updated = this.store.db
        .prepare(
          "UPDATE collection_crawls SET state=?,finishedAt=?,reposDescribed=?,describeErrors=?,errorCategory=?,httpStatus=?,observedAt=? WHERE id=? AND state='accepted'",
        )
        .run(
          state,
          outcome.finishedAt,
          outcome.reposDescribed,
          outcome.describeErrors,
          outcome.errorCategory,
          outcome.httpStatus,
          observedAt,
          row.id,
        );
      if (updated.changes)
        this.store.audit(
          row.actor,
          `collection_crawl_${state}`,
          { pds: row.pds, crawlStartedAt: row.crawlStartedAt, ...outcome },
          row.operation,
        );
    });
  }

  // Never throws: an unconfigured or unreachable directory is a status. A
  // successful observation younger than statusCacheMs is reused.
  async status(timeoutMs?: number): Promise<CollectionDirectoryStatus> {
    const empty = {
      observedAt: new Date().toISOString(),
      processStartedAt: null,
      historySupported: false,
      active: [],
      recent: [],
    };
    if (!this.configured)
      return {
        configured: false,
        available: false,
        error: "collectiondir_not_configured",
        ...empty,
      };
    try {
      const cached =
        this.observation && Date.now() - this.observation.at < statusCacheMs
          ? this.observation
          : null;
      if (cached?.error) throw new ApiError(503, cached.error);
      const status = cached?.status ?? (await this.observeWithin(timeoutMs));
      return {
        configured: true,
        available: true,
        error: null,
        observedAt: status.observedAt,
        processStartedAt: status.processStartedAt,
        historySupported: status.historySupported,
        active: status.active.slice(0, browserListLimit),
        recent: status.recent.slice(0, browserListLimit),
      };
    } catch (e) {
      return {
        configured: true,
        available: false,
        error: e instanceof ApiError ? e.code : "collectiondir_unavailable",
        ...empty,
      };
    }
  }

  // Newest requests first. The cursor is the last row's requestedAt|id.
  page(
    after: string,
    limit: number,
    pds?: string,
  ): { items: CollectionCrawl[]; next: string | null } {
    const [requestedAt, id] = crawlCursor(after);
    const rows = this.store.db
      .prepare(
        `SELECT * FROM collection_crawls
         WHERE (?='' OR (requestedAt,id)<(?,?)) AND (?='' OR pds=?)
         ORDER BY requestedAt DESC,id DESC LIMIT ?`,
      )
      .all(
        requestedAt,
        requestedAt,
        id,
        pds ?? "",
        pds ?? "",
        limit + 1,
      ) as unknown as CrawlRow[];
    const items = rows.slice(0, limit).map((row) => ({
      ...row,
      actorHandle: this.actorHandle(row),
    }));
    return {
      items,
      next:
        rows.length > limit
          ? `${rows[limit - 1].requestedAt}|${rows[limit - 1].id}`
          : null,
    };
  }

  private actorHandle(row: CrawlRow): string | null {
    return (
      this.store.get<{ handle?: string | null }>(
        "administrator-profile",
        row.actor,
      )?.handle ??
      (
        this.store.db
          .prepare(
            "SELECT actorHandle FROM audit WHERE operation=? AND actor=? AND actorHandle IS NOT NULL ORDER BY seq LIMIT 1",
          )
          .get(row.operation, row.actor) as
          { actorHandle?: string | null } | undefined
      )?.actorHandle ??
      null
    );
  }
}

function crawlCursor(after: string): [string, string] {
  if (!after) return ["", ""];
  const parts = after.split("|");
  if (
    parts.length !== 2 ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(parts[0]) ||
    !Number.isFinite(Date.parse(parts[0])) ||
    !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(
      parts[1],
    )
  )
    throw new ApiError(400, "invalid_cursor");
  return [parts[0], parts[1]];
}
