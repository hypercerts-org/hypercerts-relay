import type { Command, Operation, Policy, Job } from "../server/contracts";
export type { Command, Operation, Policy, Job };
export interface Source {
  HostID: number;
  Hostname: string;
  NoSSL: boolean;
  DesiredState: string;
  RuntimeState: string;
  Revision: number;
  RecoveryRequired: boolean;
  LastDurableCursor: number;
  Validation: { Status: string; Reason: string };
  AccountQuota: { Count: number; Limit: number };
}
export interface Limit {
  scope: string;
  eventsPerSecond: number;
  waitingConnections: number;
  unit: string;
  recovery: string;
}
export interface Coverage {
  pds: string;
  policy: Policy;
  jobId: string;
  state: string;
  completedRepos: number;
  reason: string | null;
  historyComplete: boolean;
  historicalPDSAttribution: string;
}
export interface Audit {
  seq: number;
  actor: string;
  action: string;
  time: string;
  operation: string | null;
  detail: Record<string, unknown>;
}
export async function api<T>(
  path: string,
  body?: unknown,
  csrf?: string,
  id?: string,
): Promise<T> {
  const response = await fetch(`/api/v1${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: {
      "Content-Type": "application/json",
      ...(csrf ? { "X-CSRF-Token": csrf } : {}),
      ...(id ? { "Idempotency-Key": id } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (response.status === 204) return undefined as T;
  const data = await response.json();
  if (!response.ok)
    throw new Error(
      data.details?.join(" ") ??
        data.error?.replaceAll("_", " ") ??
        "The request failed. Try again.",
    );
  return data as T;
}
export function target(command: Command) {
  switch (command.kind) {
    case "source":
      return `${command.state} · ${command.pds}`;
    case "collections":
      return `${command.collections.length} selected collections`;
    case "job":
      return `${command.reason.replace("_", " ")} · ${command.pds}`;
    case "job_action":
      return `${command.action} job ${command.id.slice(0, 8)}`;
    case "limit":
      return `${command.scope} · ${command.eventsPerSecond} events/second`;
  }
}
export function sourceOrigin(source: Source) {
  return `${source.NoSSL ? "http" : "https"}://${source.Hostname}`;
}
