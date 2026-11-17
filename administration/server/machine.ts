import { Router, type NextFunction, type Request, type Response } from "express";
import { createHash, timingSafeEqual } from "node:crypto";
import { z } from "zod";
import { ApiError, command, type Command } from "./contracts.ts";
import type { Services } from "./services.ts";
import type { Store } from "./store.ts";

// The automatic PDS discovery service is the only machine caller. Its
// operations are journaled and audited under this fixed, non-DID actor.
export const discoveryActor = "service:pds-discovery";
export const discoveryPrefix = "/api/v1/sources/discovery";

// Relay returns at most 100 sources per page; this bounds a full listing.
const maxSourcePages = 1000;

// Administrator-managed settings for automatic PDS discovery.
export interface DiscoverySettings {
  // The largest account limit discovery may request for a PDS it added.
  maxAccountLimit: number;
}
export const defaultDiscoverySettings: DiscoverySettings = {
  maxAccountLimit: 100_000,
};
export const discoverySettingsInput = z
  .object({
    maxAccountLimit: z.number().int().min(1000).max(Number.MAX_SAFE_INTEGER),
  })
  .strict();
const settingsNamespace = "discovery-settings";

export function discoverySettings(store: Store): DiscoverySettings {
  return {
    ...defaultDiscoverySettings,
    ...store.get<DiscoverySettings>(settingsNamespace, "current"),
  };
}

// Records a settings change and its audit row in one transaction.
export function changeDiscoverySettings(
  store: Store,
  actor: string,
  input: DiscoverySettings,
): DiscoverySettings {
  return store.transaction(() => {
    const previous = discoverySettings(store);
    store.set(settingsNamespace, "current", input);
    store.audit(actor, "discovery_settings_changed", { previous, current: input });
    return discoverySettings(store);
  });
}

interface RelaySourcePage {
  Sources: {
    Hostname: string;
    NoSSL: boolean;
    DesiredState: string;
    AccountQuota?: { Limit?: number };
  }[];
  NextAfterHostID: number;
}

export interface DiscoverySource {
  pds: string;
  state: string;
  accountLimit: number | null;
}
export interface DiscoveryState {
  policyRevision: number;
  collections: string[];
  sources: DiscoverySource[];
  maxAccountLimit: number;
}

// An operator's disable or remove always wins: a machine caller may enable a
// source only when Relay has no source for it or it is already enabled. Both the
// request route and the worker check this, so a source disabled after the
// request is still not re-enabled.
export async function machineEnableBlocked(
  services: Services,
  input: Command,
): Promise<boolean> {
  if (input.kind !== "source" || input.state !== "enabled") return false;
  try {
    const view = (await services.call(
      "relay",
      `/source?pds=${encodeURIComponent(input.pds)}`,
    )) as { DesiredState?: string };
    return view.DesiredState !== "enabled";
  } catch (e) {
    if (e instanceof ApiError && e.status === 404) return false;
    throw e;
  }
}

// A machine caller may only enable a source or raise an account limit. It
// cannot disable or remove a source, change collections, limits or jobs.
function permitted(input: Command): boolean {
  if (input.kind === "source") return input.state === "enabled";
  if (input.kind === "account_quota")
    return input.accountLimit > input.expectedLimit;
  return false;
}

// machineRouter serves the discovery service's two routes under
// discoveryPrefix. The bearer check applies only to those routes, so other
// paths under the prefix fall through to the browser API and its session check.
export function machineRouter(
  store: Store,
  services: Services,
  token: string,
): Router {
  if (token.length < 32)
    throw new Error("Machine token must contain at least 32 bytes");
  const expected = createHash("sha256").update(token).digest();
  const machineAuth = (req: Request, _res: Response, next: NextFunction) => {
    const header = req.get("Authorization") ?? "";
    const supplied = createHash("sha256")
      .update(header.startsWith("Bearer ") ? header.slice(7) : "")
      .digest();
    if (!header.startsWith("Bearer ") || !timingSafeEqual(expected, supplied))
      return next(new ApiError(401, "machine_token_required"));
    next();
  };
  const router = Router();
  router.get("/state", machineAuth, async (_req, res) => {
    const policy = await services.policy();
    const sources: DiscoverySource[] = [];
    let after = "";
    for (let page = 0; ; page++) {
      if (page >= maxSourcePages)
        throw new ApiError(503, "relay_source_listing_too_large");
      const result = (await services.sources(after)) as unknown as RelaySourcePage;
      for (const source of result.Sources)
        sources.push({
          pds: `${source.NoSSL ? "http" : "https"}://${source.Hostname}`,
          state: source.DesiredState,
          accountLimit:
            typeof source.AccountQuota?.Limit === "number"
              ? source.AccountQuota.Limit
              : null,
        });
      if (!result.NextAfterHostID) break;
      after = String(result.NextAfterHostID);
    }
    const state: DiscoveryState = {
      policyRevision: policy.revision,
      collections: policy.collections,
      sources,
      maxAccountLimit: discoverySettings(store).maxAccountLimit,
    };
    res.json(state);
  });
  router.post("/operations", machineAuth, async (req, res) => {
    const input = command.parse(req.body),
      key = z.uuid().parse(req.get("Idempotency-Key"));
    if (!permitted(input))
      throw new ApiError(403, "machine_command_not_permitted");
    // A replayed key returns its recorded operation without new checks.
    if (!store.operation(key)) {
      if (
        input.kind === "account_quota" &&
        input.accountLimit > discoverySettings(store).maxAccountLimit
      )
        throw new ApiError(403, "account_limit_above_discovery_maximum");
      if (await machineEnableBlocked(services, input))
        throw new ApiError(409, "source_disabled_by_operator");
    }
    res.status(202).json(store.request(discoveryActor, input, key));
  });
  return router;
}
