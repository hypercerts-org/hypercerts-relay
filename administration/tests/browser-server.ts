// Test-only OAuth provider and service double; production main never imports this.
import { createApp } from "../server/app.ts";
import { Auth, type OAuthProvider } from "../server/auth.ts";
import { Store } from "../server/store.ts";
import { Services } from "../server/services.ts";
import { Worker } from "../server/worker.ts";
import { ApiError, type Command } from "../server/contracts.ts";
import { resolve } from "node:path";
const base = "http://127.0.0.1:3188",
  did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const store = new Store(":memory:");
store.seedAdministrator(did);
const oauth: OAuthProvider = {
  async authorize(input, options) {
    return new URL(
      `${base}/auth/callback?state=${options.state}&admin=${input === "operator.example"}`,
    );
  },
  async callback(params) {
    return {
      session: {
        did: params.get("admin") === "true" ? did : "did:plc:nonadministrator",
      },
      state: params.get("state"),
    };
  },
  async revoke() {},
};
let policy = { revision: 1, collections: ["app.bsky.feed.post"] };
let sources: any[] = [];
let jobs: any[] = [];
let limits: any[] = [];
const services = {
  async sources() {
    return { Sources: sources, NextAfterHostID: 0 };
  },
  async policy() {
    return policy;
  },
  async jobs() {
    return { jobs };
  },
  async call(_service: string, path: string) {
    if (path.startsWith("/limits")) return { items: limits, next: null };
    if (path.startsWith("/source?")) {
      const pds = new URL(path, base).searchParams.get("pds");
      const source = sources.find((s) => `https://${s.Hostname}` === pds);
      if (!source) throw new ApiError(404, "source_not_found");
      return source;
    }
    throw new Error("Unexpected fixture route");
  },
  async apply(command: Command, id: string) {
    switch (command.kind) {
      case "account_quota": {
        const source = sources.find(
          (s) => s.Hostname === new URL(command.pds).host,
        );
        if (!source) throw new ApiError(404, "source_not_found");
        if (
          source.AccountQuota.Limit !== command.expectedLimit &&
          source.AccountQuota.Limit !== command.accountLimit
        )
          throw new ApiError(409, "revision_conflict");
        source.AccountQuota.Limit = command.accountLimit;
        return source;
      }
      case "source": {
        let source = sources.find(
          (s) => s.Hostname === new URL(command.pds).host,
        );
        if (!source) {
          source = {
            HostID: sources.length + 1,
            Hostname: new URL(command.pds).host,
            NoSSL: false,
            DesiredState: "enabled",
            RuntimeState: "configured",
            Revision: 1,
            RecoveryRequired: true,
            LastDurableCursor: -1,
            Validation: { Status: "passed", Reason: "" },
            AccountQuota: { Count: 0, Limit: 100 },
          };
          sources.push(source);
        }
        source.DesiredState = command.state;
        return { relay: source };
      }
      case "collections":
        policy = {
          revision: policy.revision + 1,
          collections: command.collections,
        };
        return policy;
      case "limit":
        limits = [
          ...limits.filter((l) => l.scope !== command.scope),
          {
            ...command,
            waitingConnections: 0,
            unit: "events/second",
            recovery:
              "Backpressure pauses socket reads; replay gaps require recovery.",
          },
        ];
        return command;
      case "job": {
        const job = {
          id: id.replaceAll("-", "").slice(0, 32),
          pds: command.pds,
          policy,
          state: "incomplete",
          completedRepos: 0,
          attempts: 1,
          errorCode: "source_unavailable",
          historyComplete: false,
          coverage: "current_state",
        };
        jobs.push(job);
        return job;
      }
      case "job_action": {
        const job = jobs.find((j) => j.id === command.id);
        job.state = command.action === "cancel" ? "canceled" : "pending";
        return job;
      }
    }
  },
} as unknown as Services;
const worker = new Worker(store, services);
setInterval(() => void worker.tick(), 50);
createApp(
  store,
  services,
  new Auth(store, base, oauth),
  {},
  resolve("dist"),
).listen(3188, "127.0.0.1");
