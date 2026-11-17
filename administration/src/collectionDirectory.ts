import type {
  ActiveCrawl,
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
    !operation ||
    operation.command.kind !== "collection_crawl" ||
    operation.state !== "applied"
  )
    return null;
  const result = (
    operation.result as {
      collectiondir?: {
        submitted: number;
        started: number;
        alreadyActive: number;
        skipped: { not_enabled: number; not_https: number };
      };
    } | null
  )?.collectiondir;
  if (!result) return null;
  const scope = operation.command.scope;
  if (scope !== "all") {
    const row = rows.find(
      (candidate) =>
        candidate.operation === operation.id && candidate.pds === scope,
    );
    if (result.alreadyActive)
      return `A crawl of ${scope} is already running${
        row?.crawlStartedAt ? ` (started ${time(row.crawlStartedAt)})` : ""
      }. No second crawl was started.`;
    if (result.started) return `Crawl of ${scope} started.`;
    return `The collection directory accepted the crawl of ${scope}.`;
  }
  const skipped = result.skipped.not_enabled + result.skipped.not_https;
  return `Crawl requested for ${result.submitted} sources: ${result.started} started, ${result.alreadyActive} already running.${
    skipped
      ? ` Skipped ${result.skipped.not_enabled} not enabled and ${result.skipped.not_https} without HTTPS.`
      : ""
  }`;
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
