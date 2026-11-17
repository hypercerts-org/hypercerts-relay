import { ApiError, type ActiveCrawl, type EndedCrawl } from "./contracts.ts";
import {
  allowedControlTransport,
  readControlBody,
  type ServiceConfig,
} from "./services.ts";

export type { ActiveCrawl, EndedCrawl } from "./contracts.ts";

// Client for the optional collection directory's private admin API. It knows
// only the two fixed admin paths and never returns the bearer token or a raw
// upstream body in an error.
export type CollectiondirFailure =
  | "not_configured"
  | "unavailable"
  | "unauthorized"
  | "rejected"
  | "invalid_response";

const failureStatus: Record<CollectiondirFailure, number> = {
  not_configured: 409,
  unavailable: 503,
  unauthorized: 502,
  rejected: 502,
  invalid_response: 502,
};

export class CollectiondirError extends ApiError {
  constructor(readonly failure: CollectiondirFailure) {
    super(failureStatus[failure], `collectiondir_${failure}`);
  }
}

export interface CrawlSubmission {
  pds: string;
  // "accepted" is reported by an older collection directory that does not say
  // whether a crawl started.
  outcome: "started" | "already_active" | "accepted";
  startedAt: string | null;
}

export interface CollectiondirStatus {
  observedAt: string;
  serverTime: string;
  processStartedAt: string | null;
  historySupported: boolean;
  active: ActiveCrawl[];
  recent: EndedCrawl[];
}

// Relay normalizes source hostnames to lower case; only localhost carries a port.
const crawlOrigin = /^https:\/\/[a-z0-9.-]+(?::\d{1,5})?$/;
// The most hosts sent in one requestCrawl call.
export const crawlSubmissionChunk = 100;
const crawlPath = "/admin/pds/requestCrawl";

export function isCrawlOrigin(value: string) {
  return crawlOrigin.test(value);
}

// Reads the optional pair of settings. Both absent disables the feature;
// exactly one set is a configuration error.
export function collectiondirConfig(
  env: Record<string, string | undefined>,
  readSecret: (name: string) => string,
): ServiceConfig | null {
  const url = env.COLLECTIONDIR_CONTROL_URL,
    tokenFile = env.COLLECTIONDIR_CONTROL_TOKEN_FILE;
  if (!url && !tokenFile) return null;
  if (!url || !tokenFile)
    throw new Error(
      "COLLECTIONDIR_CONTROL_URL and COLLECTIONDIR_CONTROL_TOKEN_FILE must be set together",
    );
  return { url, token: readSecret("COLLECTIONDIR_CONTROL_TOKEN_FILE") };
}

export class Collectiondir {
  readonly configured: boolean;
  private readonly origin: string;
  private readonly token: string;
  constructor(
    config: ServiceConfig | null,
    options: {
      railwayPrivateNetwork?: boolean;
      forbiddenOrigins?: string[];
    } = {},
  ) {
    this.configured = config !== null;
    this.origin = "";
    this.token = "";
    if (!config) return;
    const url = new URL(config.url);
    if (
      !allowedControlTransport(url, options.railwayPrivateNetwork ?? false) ||
      url.username ||
      url.password ||
      url.pathname !== "/" ||
      url.search ||
      url.hash
    )
      throw new Error(
        "COLLECTIONDIR_CONTROL_URL requires HTTPS (or loopback / Railway private HTTP) without credentials, path or query",
      );
    for (const forbidden of options.forbiddenOrigins ?? []) {
      if (forbidden && new URL(forbidden).origin === url.origin)
        throw new Error(
          "COLLECTIONDIR_CONTROL_URL must be the collection directory, not a Relay, Rainbow or Jetstream origin",
        );
    }
    this.origin = url.origin;
    this.token = config.token;
  }

  async status(timeoutMs = 3000): Promise<CollectiondirStatus> {
    const body = await this.request(
      "/admin/crawlStatus",
      "GET",
      undefined,
      timeoutMs,
    );
    if (
      !isRecord(body) ||
      !isRecord(body.host_starts) ||
      typeof body.server_time !== "string"
    )
      throw new CollectiondirError("invalid_response");
    const historySupported = body.host_results !== undefined;
    if (historySupported && !isRecord(body.host_results))
      throw new CollectiondirError("invalid_response");
    const active: ActiveCrawl[] = [];
    for (const [pds, value] of Object.entries(body.host_starts)) {
      if (!isCrawlOrigin(pds)) continue;
      active.push(activeCrawl(pds, value));
    }
    const recent: EndedCrawl[] = [];
    for (const [pds, value] of Object.entries(
      (body.host_results ?? {}) as Record<string, unknown>,
    )) {
      if (!isCrawlOrigin(pds)) continue;
      recent.push(endedCrawl(pds, value));
    }
    active.sort((a, b) => compareDesc(a.startedAt, b.startedAt));
    recent.sort((a, b) => compareDesc(a.finishedAt, b.finishedAt));
    return {
      observedAt: new Date().toISOString(),
      serverTime: body.server_time,
      processStartedAt: timestamp(body.process_start) ?? null,
      historySupported,
      active,
      recent,
    };
  }

  async requestCrawl(
    origins: string[],
    timeoutMs = 10000,
  ): Promise<CrawlSubmission[]> {
    if (!this.configured) throw new CollectiondirError("not_configured");
    const unique = [...new Set(origins)];
    if (unique.some((origin) => !isCrawlOrigin(origin)))
      throw new ApiError(400, "invalid_crawl_origin");
    const submissions: CrawlSubmission[] = [];
    // One chunk at a time: a failed chunk stops the rest from being sent.
    for (let i = 0; i < unique.length; i += crawlSubmissionChunk) {
      const hosts = unique.slice(i, i + crawlSubmissionChunk);
      const body = await this.request(crawlPath, "POST", { hosts }, timeoutMs); // NOSONAR: sequential by design, see above
      submissions.push(...chunkSubmissions(hosts, body));
    }
    return submissions;
  }

  private async request(
    path: "/admin/crawlStatus" | "/admin/pds/requestCrawl",
    method: "GET" | "POST",
    body: unknown,
    timeoutMs: number,
  ): Promise<unknown> {
    if (!this.configured) throw new CollectiondirError("not_configured");
    let response: Response;
    try {
      response = await fetch(`${this.origin}${path}`, {
        method,
        headers: {
          Authorization: `Bearer ${this.token}`,
          Accept: "application/json",
          ...(body === undefined ? {} : { "Content-Type": "application/json" }),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(timeoutMs),
        redirect: "error",
      });
    } catch {
      throw new CollectiondirError("unavailable");
    }
    if (!response.ok) throw await failure(response);
    if (!response.body) throw new CollectiondirError("invalid_response");
    try {
      return await readControlBody<unknown>(response);
    } catch (e) {
      throw new CollectiondirError(
        e instanceof ApiError || e instanceof SyntaxError
          ? "invalid_response"
          : "unavailable",
      );
    }
  }
}

async function failure(response: Response): Promise<CollectiondirError> {
  if (response.status >= 500) {
    await discard(response);
    return new CollectiondirError("unavailable");
  }
  if (response.status === 401 || response.status === 403) {
    await discard(response);
    return new CollectiondirError("unauthorized");
  }
  if (response.status === 400) {
    // collectiondir answers a missing status token with 400 AdminAuthRequired.
    let code: unknown;
    try {
      const body = response.body
        ? await readControlBody<unknown>(response)
        : null;
      code = isRecord(body) ? body.error : undefined;
    } catch {
      code = undefined;
    }
    if (code === "AdminAuthRequired" || code === "AdminRequired")
      return new CollectiondirError("unauthorized");
    return new CollectiondirError("rejected");
  }
  await discard(response);
  return new CollectiondirError("rejected");
}

async function discard(response: Response) {
  try {
    await response.body?.cancel();
  } catch {
    // The status code alone determines the failure.
  }
}

function activeCrawl(pds: string, value: unknown): ActiveCrawl {
  const startedAt = isRecord(value) ? timestamp(value.start) : undefined;
  if (!isRecord(value) || !startedAt || !isCount(value.seen))
    throw new CollectiondirError("invalid_response");
  return {
    pds,
    startedAt,
    reposDescribed: value.seen,
    describeErrors: optionalCount(value.describe_errors),
  };
}

function endedCrawl(pds: string, value: unknown): EndedCrawl {
  const crawl = activeCrawl(pds, value);
  const record = value as Record<string, unknown>;
  const finishedAt = timestamp(record.end);
  if (!finishedAt || (record.state !== "finished" && record.state !== "failed"))
    throw new CollectiondirError("invalid_response");
  const category = record.error_category;
  const httpStatus = record.http_status;
  return {
    ...crawl,
    finishedAt,
    state: record.state,
    errorCategory: errorCategory(category, record.state),
    httpStatus:
      typeof httpStatus === "number" &&
      Number.isInteger(httpStatus) &&
      httpStatus >= 100 &&
      httpStatus <= 599
        ? httpStatus
        : null,
  };
}

// A failed crawl without a well-formed category is reported as "unknown".
function errorCategory(
  category: unknown,
  state: "finished" | "failed",
): string | null {
  if (typeof category === "string" && /^[a-z_]{1,40}$/.test(category))
    return category;
  return state === "failed" ? "unknown" : null;
}

// One requestCrawl response. An older directory reports no per-host outcome.
function chunkSubmissions(hosts: string[], body: unknown): CrawlSubmission[] {
  if (!isRecord(body)) throw new CollectiondirError("invalid_response");
  if (body.hosts === undefined)
    return hosts.map((pds) => ({ pds, outcome: "accepted", startedAt: null }));
  const outcomes = body.hosts;
  if (!isRecord(outcomes)) throw new CollectiondirError("invalid_response");
  return hosts.map((pds) => {
    const entry = outcomes[pds];
    const startedAt = isRecord(entry) ? timestamp(entry.start) : undefined;
    if (
      !isRecord(entry) ||
      (entry.state !== "started" && entry.state !== "already_active") ||
      !startedAt
    )
      throw new CollectiondirError("invalid_response");
    return { pds, outcome: entry.state, startedAt };
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isCount(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function optionalCount(value: unknown) {
  return isCount(value) ? value : null;
}

// Collection directory timestamps are RFC 3339 strings compared verbatim to
// identify a crawl, so they are validated but never reformatted.
function timestamp(value: unknown): string | undefined {
  return typeof value === "string" &&
    value.length <= 64 &&
    Number.isFinite(Date.parse(value))
    ? value
    : undefined;
}

function compareDesc(a: string, b: string) {
  return Date.parse(b) - Date.parse(a);
}
