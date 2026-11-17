import type {
  ActiveCrawl,
  CollectionBootstrapView,
  CollectionCrawl,
  CollectionDirectoryStatus,
  Operation,
  Source,
} from "./api";

// Polling cadence while a crawl is running, and after a failed refresh.
export const directoryPollMs = 5000;
export const directoryRetryMs = 15000;

// Only enabled, validated HTTPS sources can be crawled; the server enforces
// the same rule against Relay when it applies the request.
export function crawlEligible(source: Source) {
  return (
    source.DesiredState === "enabled" &&
    source.Validation.Status === "passed" &&
    !source.NoSSL
  );
}

// Why a crawl cannot be requested now, or null when it can.
export function directoryMessage(
  status: CollectionDirectoryStatus | null,
): string | null {
  if (!status) return "Checking the collection directory…";
  if (!status.configured)
    return "Collection directory crawls are not configured. Set COLLECTIONDIR_CONTROL_URL and COLLECTIONDIR_CONTROL_TOKEN_FILE on the Administration service to enable them.";
  if (status.available) return null;
  if (status.error === "collectiondir_unauthorized")
    return "The collection directory rejected the Administration credential. Check that COLLECTIONDIR_CONTROL_TOKEN_FILE holds its COLLECTIONS_ADMIN_TOKEN.";
  if (status.error === "status_request_failed")
    return "Collection directory status could not be loaded. Refresh to try again.";
  return "The collection directory is unavailable. Crawls can be requested again when it responds.";
}

function time(value: string | null) {
  return value ? new Date(value).toLocaleString() : "not recorded";
}

function described(count: number | null) {
  return `${count ?? 0} ${count === 1 ? "repository" : "repositories"} described`;
}

const unknownReasons: Record<string, string> = {
  collectiondir_restarted: "collection directory restarted",
  result_not_retained: "result no longer reported by the collection directory",
  status_history_unsupported:
    "this collection directory version does not report results",
};

// One line describing a crawl request; accepted is never shown as finished.
export function crawlSummary(
  row: CollectionCrawl | null,
  active?: ActiveCrawl,
): string {
  if (!row)
    return active
      ? `Crawl running since ${time(active.startedAt)} · ${described(active.reposDescribed)}`
      : "No recent crawl request recorded.";
  switch (row.state) {
    case "accepted": {
      const running =
        active &&
        (row.crawlStartedAt === null || active.startedAt === row.crawlStartedAt)
          ? active
          : null;
      return running
        ? `Accepted, crawl running · ${described(running.reposDescribed)}`
        : "Accepted, crawl running";
    }
    case "finished":
      return `Finished · ${described(row.reposDescribed)}${
        row.describeErrors
          ? ` · ${row.describeErrors} could not be described`
          : ""
      }`;
    case "failed":
      return `Failed: ${(row.errorCategory ?? "unknown").replaceAll("_", " ")}${
        row.httpStatus ? ` ${row.httpStatus}` : ""
      }`;
    case "unknown":
      return `Outcome unknown: ${
        unknownReasons[row.errorCategory ?? ""] ?? "not reported"
      }`;
  }
}

// Explains an applied crawl request; an already running crawl is not an error.
export function crawlNotice(
  operation: Operation | null,
  rows: CollectionCrawl[],
): string | null {
  if (
    operation?.command.kind !== "collection_crawl" ||
    operation.state !== "applied"
  )
    return null;
  const result = (
    operation.result as {
      collectiondir?: {
        submitted: number;
        started: number;
        alreadyActive: number;
        // Absent from results recorded before retries skipped them.
        alreadySubmitted?: number;
        skipped: { not_enabled: number; not_https: number };
      };
    } | null
  )?.collectiondir;
  if (!result) return null;
  const scope = operation.command.scope;
  const earlier = result.alreadySubmitted ?? 0;
  if (scope !== "all") {
    const row = rows.find(
      (candidate) =>
        candidate.operation === operation.id && candidate.pds === scope,
    );
    if (earlier && !result.submitted)
      return `The crawl of ${scope} was already submitted by an earlier attempt of this request. No second crawl was started.`;
    if (result.alreadyActive)
      return `A crawl of ${scope} is already running${
        row?.crawlStartedAt ? ` (started ${time(row.crawlStartedAt)})` : ""
      }. No second crawl was started.`;
    if (result.started) return `Crawl of ${scope} started.`;
    return `The collection directory accepted the crawl of ${scope}.`;
  }
  const skipped = result.skipped.not_enabled + result.skipped.not_https;
  return `Crawl requested for ${result.submitted} sources: ${result.started} started, ${result.alreadyActive} already running.${
    earlier
      ? ` ${earlier} already submitted by an earlier attempt were not sent again.`
      : ""
  }${
    skipped
      ? ` Skipped ${result.skipped.not_enabled} not enabled and ${result.skipped.not_https} without HTTPS.`
      : ""
  }`;
}

// Initial crawl phases that the server can still move forward on its own.
const openBootstrapPhases = new Set(["waiting", "retrying", "accepted"]);

// Whether crawl status can still change and is worth polling. Accepted rows
// are ignored without a configured directory: only the server can close them.
// A source enable is followed because applying it may queue an initial crawl.
export function crawlsInProgress(
  directory: CollectionDirectoryStatus | null,
  rows: CollectionCrawl[],
  operation: Operation | null,
  bootstraps: CollectionBootstrapView[] = [],
): boolean {
  return (
    (directory?.active.length ?? 0) > 0 ||
    (directory?.configured !== false &&
      rows.some((row) => row.state === "accepted")) ||
    (operation !== null &&
      (operation.command.kind === "collection_crawl" ||
        (operation.command.kind === "source" &&
          operation.command.state === "enabled")) &&
      ["requested", "applying"].includes(operation.state)) ||
    bootstraps.some((bootstrap) => openBootstrapPhases.has(bootstrap.phase))
  );
}

// What happened to the initial crawl when a source enable was applied. The
// source itself was added in every case.
export function bootstrapNotice(operation: Operation | null): {
  text: string;
  queued: boolean;
} | null {
  if (operation?.command.kind !== "source" || operation.state !== "applied")
    return null;
  const bootstrap = (
    operation.result as {
      collectionDirectory?: { bootstrap: string };
    } | null
  )?.collectionDirectory?.bootstrap;
  switch (bootstrap) {
    case "queued":
      return {
        text: "Initial collection directory crawl queued.",
        queued: true,
      };
    case "not_configured":
      return {
        text: "Collection directory is not configured, so no initial crawl was requested. The source was added.",
        queued: false,
      };
    case "not_https":
      return {
        text: "No initial crawl: the collection directory only crawls HTTPS sources. The source was added.",
        queued: false,
      };
    case "conflict":
      return {
        text: "No initial crawl was requested because its request ID was already in use. The source was added.",
        queued: false,
      };
    default:
      return null;
  }
}

function errorText(code: string | null) {
  return (code ?? "unknown error").replaceAll("_", " ");
}

// One line describing an initial crawl. Once the directory accepts it, the
// wording matches crawlSummary.
export function bootstrapSummary(
  bootstrap: CollectionBootstrapView,
  active?: ActiveCrawl,
): string {
  const request = bootstrap.request;
  switch (bootstrap.phase) {
    case "skipped_not_configured":
      return "Skipped: collection directory not configured. Once it is configured, use Crawl collection directory.";
    case "skipped_not_https":
      return "Skipped: the collection directory only crawls HTTPS sources.";
    case "skipped_conflict":
      return "Skipped: another request already used this initial crawl's ID. Use Crawl collection directory.";
    case "waiting":
      return "Waiting to be submitted";
    case "retrying": {
      const reason =
        request?.error === "collectiondir_unavailable"
          ? "Collection directory unavailable"
          : `Not submitted: ${errorText(request?.error ?? null)}`;
      return `${reason}; retrying automatically (attempt ${
        (request?.attempts ?? 0) + 1
      }${
        request?.nextAttemptAt ? `, next at ${time(request.nextAttemptAt)}` : ""
      })`;
    }
    case "needs_attention":
      return `Could not be submitted: ${errorText(request?.error ?? null)}. Automatic retries have stopped.`;
    case "not_requested":
      if (request?.state === "canceled") return "Not requested: canceled";
      if (request?.error === "source_not_enabled")
        return "Not requested: source was no longer enabled";
      return `Not requested: ${errorText(request?.error ?? null)}`;
    case "accepted":
    case "finished":
    case "failed":
    case "unknown":
      return bootstrap.crawl
        ? crawlSummary(bootstrap.crawl, active)
        : "Outcome unknown: not reported";
  }
}

// Initial crawls the collection directory has not yet accepted.
export function awaitingBootstraps(bootstraps: CollectionBootstrapView[]) {
  return bootstraps.filter(
    (bootstrap) =>
      !["accepted", "finished", "failed", "unknown"].includes(bootstrap.phase),
  );
}

export interface CrawlPage {
  items: CollectionCrawl[];
  next: string | null;
}

// Keep loaded history current after it moves beyond the newest page. Looking
// up accepted row IDs also returns their final state once they stop running.
export async function refreshCrawls(
  loaded: CollectionCrawl[],
  keepOlder: boolean,
  read: (path: string) => Promise<CrawlPage>,
): Promise<CrawlPage> {
  const latest = await read("/collection-directory/crawls?limit=100");
  if (!keepOlder) return latest;
  const newest = new Set(latest.items.map((row) => row.id));
  const open = loaded.filter(
    (row) => row.state === "accepted" && !newest.has(row.id),
  );
  const refreshed = new Map<string, CollectionCrawl>();
  for (let i = 0; i < open.length; i += 100) {
    const ids = open.slice(i, i + 100).map((row) => row.id);
    const page = await read(
      `/collection-directory/crawls?limit=100&ids=${encodeURIComponent(ids.join(","))}`,
    );
    for (const row of page.items) refreshed.set(row.id, row);
  }
  return {
    items: [
      ...latest.items,
      ...loaded
        .filter((row) => !newest.has(row.id))
        .map((row) => refreshed.get(row.id) ?? row),
    ],
    next: latest.next,
  };
}

export interface DirectoryPoller {
  // Re-evaluates whether polling is needed, for example after a load.
  update(): void;
  stop(): void;
}

// Refreshes crawl status every five seconds only while isActive() holds and
// the page is visible; waits fifteen seconds after a failed refresh.
export function createDirectoryPoller(
  refresh: () => Promise<void>,
  isActive: () => boolean,
  page: Document = document,
): DirectoryPoller {
  let timer: ReturnType<typeof setTimeout> | undefined,
    stopped = false,
    paused = false;
  const clear = () => {
    if (timer !== undefined) clearTimeout(timer);
    timer = undefined;
  };
  const schedule = (delay: number) => {
    clear();
    if (!stopped) timer = setTimeout(() => void poll(), delay);
  };
  async function poll() {
    timer = undefined;
    if (stopped) return;
    if (page.hidden) {
      paused = true;
      return;
    }
    try {
      await refresh();
    } catch {
      schedule(directoryRetryMs);
      return;
    }
    update();
  }
  function update() {
    if (stopped) return;
    if (!isActive()) clear();
    else if (timer === undefined) schedule(directoryPollMs);
  }
  const visibility = () => {
    if (!page.hidden && paused) {
      paused = false;
      if (isActive()) schedule(0);
    }
  };
  page.addEventListener("visibilitychange", visibility);
  return {
    update,
    stop() {
      stopped = true;
      clear();
      page.removeEventListener("visibilitychange", visibility);
    },
  };
}
