import { z } from "zod";
import { isValidNsid } from "@atproto/syntax";

export const origin = z
  .string()
  .max(300)
  .transform((raw, ctx) => {
    try {
      const u = new URL(raw);
      const local = ["localhost", "127.0.0.1", "[::1]"].includes(u.hostname);
      if (
        (u.protocol !== "https:" && !(local && u.protocol === "http:")) ||
        u.username ||
        u.password ||
        u.search ||
        u.hash ||
        u.pathname !== "/"
      )
        throw new Error();
      return u.origin;
    } catch {
      ctx.addIssue({
        code: "custom",
        message: "Use an HTTPS PDS origin without a path or credentials.",
      });
      return z.NEVER;
    }
  });
export const rate = z.number().int().min(1).max(1_000_000);
export const command = z.discriminatedUnion("kind", [
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
        .transform((v) =>
          [...new Set(v)].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)),
        ),
    })
    .strict(),
  z
    .object({
      kind: z.literal("job"),
      pds: origin,
      reason: z.enum(["backfill", "quota_recovery"]),
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
]);
export type Command = z.infer<typeof command>;
export type OperationState =
  "requested" | "applying" | "applied" | "failed" | "canceled" | "incomplete";
export interface Operation {
  id: string;
  actor: string;
  command: Command;
  state: OperationState;
  createdAt: string;
  updatedAt: string;
  result: unknown;
  error: string | null;
}
export interface Policy {
  revision: number;
  collections: string[];
}
export interface Job {
  id: string;
  pds: string;
  policy: Policy;
  state: string;
  completedRepos: number;
  attempts: number;
  errorCode?: string;
  historyComplete: boolean;
  coverage: string;
}
export class ApiError extends Error {
  constructor(
    public status: number,
    public code: string,
  ) {
    super(code);
  }
}
