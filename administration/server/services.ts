import { ApiError, type Command, type Job, type Policy } from "./contracts.ts";

export interface ServiceConfig {
  url: string;
  token: string;
}
export class Services {
  constructor(
    private relay: ServiceConfig,
    private jetstream: ServiceConfig,
  ) {}
  async call<T>(
    service: "relay" | "jetstream",
    path: string,
    method = "GET",
    body?: unknown,
    id?: string,
  ): Promise<T> {
    const config = service === "relay" ? this.relay : this.jetstream;
    let response: Response;
    try {
      response = await fetch(`${config.url}/hypercerts/v1${path}`, {
        method,
        headers: {
          Authorization: `Bearer ${config.token}`,
          "Content-Type": "application/json",
          ...(id ? { "Idempotency-Key": id } : {}),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(25000),
        redirect: "error",
      });
    } catch {
      throw new ApiError(503, `${service}_unavailable`);
    }
    if (!response.ok)
      throw new ApiError(
        response.status >= 500 ? 503 : response.status,
        `${service}_rejected_${response.status}`,
      );
    if (response.status === 204) return null as T;
    const reader = response.body!.getReader();
    let size = 0;
    const chunks: Uint8Array[] = [];
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        size += value.length;
        if (size > 2_000_000)
          throw new ApiError(502, "service_response_too_large");
        chunks.push(value);
      }
    } finally {
      await reader.cancel();
    }
    return JSON.parse(Buffer.concat(chunks).toString()) as T;
  }
  policy() {
    return this.call<Policy>("jetstream", "/policy");
  }
  jobs(after = "", pds = "") {
    return this.call<{ jobs: Job[]; nextCursor?: string }>(
      "jetstream",
      `/jobs?limit=50&after=${encodeURIComponent(after)}&pds=${encodeURIComponent(pds)}`,
    );
  }
  sources(after = "") {
    return this.call<Record<string, unknown>>(
      "relay",
      `/sources?after=${encodeURIComponent(after)}`,
    );
  }
  async apply(
    command: Command,
    id: string,
    progress: (value: unknown) => void = () => {},
  ): Promise<unknown> {
    switch (command.kind) {
      case "account_quota":
        return this.call("relay", "/source/quota", "PUT", {
          pds: command.pds,
          expectedLimit: command.expectedLimit,
          accountLimit: command.accountLimit,
        });
      case "source": {
        // Stop acquisition at Jetstream before disabling the raw source. Retry is safe.
        if (command.state !== "enabled") {
          try {
            await this.call("jetstream", "/sources", "DELETE", {
              pds: command.pds,
            });
          } catch (error) {
            // A Relay-only source is already absent from Jetstream.
            if (!(error instanceof ApiError && error.status === 404))
              throw error;
          }
          progress({ jetstream: { enabled: false } });
        }
        const relay = await this.call("relay", "/source", "PUT", {
          pds: command.pds,
          state: command.state,
        });
        progress({
          relay,
          ...(command.state !== "enabled"
            ? { jetstream: { enabled: false } }
            : {}),
        });
        const jetstream =
          command.state === "enabled"
            ? await this.call("jetstream", "/sources", "POST", {
                pds: command.pds,
              })
            : { enabled: false };
        return { relay, jetstream };
      }
      case "collections": {
        const policy = await this.policy();
        if (
          JSON.stringify(policy.collections) ===
          JSON.stringify(command.collections)
        )
          return policy;
        return this.call("jetstream", "/policy", "PUT", {
          expectedRevision: command.expectedRevision,
          collections: command.collections,
        });
      }
      case "job":
        return this.call("jetstream", "/jobs", "POST", {
          pds: command.pds,
          reason: command.reason,
          requestId: id,
        });
      case "job_action": {
        return this.call(
          "jetstream",
          `/jobs/${command.id}/${command.action}`,
          "POST",
          {},
          id,
        );
      }
      case "limit":
        return this.call("relay", "/limits", "PUT", {
          scope: command.scope,
          eventsPerSecond: command.eventsPerSecond,
        });
    }
  }
}
