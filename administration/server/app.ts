import {
  loginLimit,
  railwayClientIP,
  type ProxyOptions,
} from "./login-limit.ts";
import express, { type ErrorRequestHandler } from "express";
import { z, ZodError } from "zod";
import { join } from "node:path";
import { Auth, type Session } from "./auth.ts";
import { ApiError, command, origin } from "./contracts.ts";
import { Store } from "./store.ts";
import { Services } from "./services.ts";
import type { AdministratorProfile } from "./profile.ts";

const page = z.object({
  after: z.string().max(200).default(""),
  limit: z.coerce.number().int().min(1).max(100).default(50),
});
export function createApp(
  store: Store,
  services: Services,
  auth: Auth,
  metadata: unknown,
  staticDir?: string,
  proxy: ProxyOptions = {},
) {
  const app = express();
  app.disable("x-powered-by");
  app.set("trust proxy", proxy.trustProxy ?? false);
  app.use(railwayClientIP(proxy));
  app.use((_req, res, next) => {
    res.set({
      "Cache-Control": "no-store",
      "X-Content-Type-Options": "nosniff",
      "Referrer-Policy": "no-referrer",
      "Content-Security-Policy":
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'",
    });
    next();
  });
  app.use(express.json({ limit: "64kb", type: "application/json" }));
  app.use(express.urlencoded({ extended: false, limit: "4kb" }));
  app.get("/health", (_req, res) => res.json({ status: "ok" }));
  app.get("/oauth-client-metadata.json", (_req, res) => res.json(metadata));
  app.post("/auth/login", loginLimit(), (req, res) => auth.login(req, res));
  app.get("/auth/callback", (req, res) => auth.callback(req, res));
  app.use("/api/v1", auth.require);
  app.get("/api/v1/session", (_req, res) => {
    const { did, csrf, expires } = res.locals.session as Session;
    const profile = store.get<AdministratorProfile>(
      "administrator-profile",
      did,
    );
    res.json({
      did,
      csrf,
      expires,
      displayName: profile?.displayName ?? null,
      handle: profile?.handle ?? null,
    });
  });
  app.get("/api/v1/administrators", (req, res) => {
    const p = page
      .extend({ after: z.string().max(2048).default("") })
      .parse(req.query);
    res.json(store.administrators(p.after, p.limit));
  });
  app.post("/api/v1/administrators", (req, res) => {
    const input = z
      .object({
        did: z.string().max(2048),
        action: z.enum(["grant", "remove"]),
      })
      .strict()
      .parse(req.body);
    store.changeAdministrator(
      (res.locals.session as Session).did,
      input.did,
      input.action,
    );
    res.status(204).end();
  });
  app.post("/api/v1/signout", (req, res) => auth.logout(req, res));
  app.post("/api/v1/revoke-sessions", (_req, res) => {
    const session = res.locals.session as Session;
    store.transaction(() => {
      store.revokeSessions(session.did);
      store.audit(session.did, "sessions_revoked", {});
    });
    res.clearCookie("relay_session", auth.options(0));
    res.status(204).end();
  });
  app.post("/api/v1/operations", (req, res) => {
    const input = command.parse(req.body),
      key = z.uuid().parse(req.get("Idempotency-Key"));
    res
      .status(202)
      .json(store.request((res.locals.session as Session).did, input, key));
  });
  app.get("/api/v1/operations", (req, res) => {
    const p = page.parse(req.query);
    res.json(store.page("operations", p.after, p.limit));
  });
  app.get("/api/v1/operations/:id", (req, res) => {
    const op = store.operation(z.uuid().parse(req.params.id));
    if (!op) throw new ApiError(404, "operation_not_found");
    res.json(op);
  });
  app.post("/api/v1/operations/:id/:action", (req, res) => {
    const id = z.uuid().parse(req.params.id),
      action = z.enum(["retry", "cancel"]).parse(req.params.action),
      op = store.operation(id);
    if (!op) throw new ApiError(404, "operation_not_found");
    if (action === "cancel" && op.state !== "requested")
      throw new ApiError(409, "only_requested_operations_can_be_canceled");
    if (action === "retry" && !["failed", "incomplete"].includes(op.state))
      throw new ApiError(409, "operation_not_retryable");
    const transitioned = store.transition(
      id,
      action === "cancel" ? "canceled" : "requested",
      op.result,
      null,
      (res.locals.session as Session).did,
      action === "cancel" ? ["requested"] : ["failed", "incomplete"],
    );
    if (!transitioned) throw new ApiError(409, "operation_state_changed");
    res.json(store.operation(id));
  });
  app.get("/api/v1/audit", (req, res) => {
    const p = page.parse(req.query);
    if (p.after && !/^\d+$/.test(p.after))
      throw new ApiError(400, "invalid_cursor");
    res.json(store.page("audit", p.after, p.limit));
  });
  app.get("/api/v1/sources", (req, res) => {
    const p = page.parse(req.query);
    return services.sources(p.after).then((v) => res.json(v));
  });
  app.get("/api/v1/source", (req, res) =>
    services
      .call(
        "relay",
        `/source?pds=${encodeURIComponent(origin.parse(req.query.pds))}`,
      )
      .then((v) => res.json(v)),
  );
  app.get("/api/v1/policy", (_req, res) =>
    services.policy().then((v) => res.json(v)),
  );
  app.get("/api/v1/jobs", (req, res) => {
    const p = page.parse(req.query);
    return services
      .jobs(p.after, req.query.pds ? origin.parse(req.query.pds) : "")
      .then((v) => res.json(v));
  });
  app.get("/api/v1/coverage", async (req, res) => {
    const p = page.parse(req.query),
      jobs = await services.jobs(
        p.after,
        req.query.pds ? origin.parse(req.query.pds) : "",
      );
    res.json({
      items: jobs.jobs.map((j) => ({
        pds: j.pds,
        policy: j.policy,
        jobId: j.id,
        state: j.state,
        completedRepos: j.completedRepos,
        reason: j.errorCode ?? null,
        coverage: j.coverage,
        historyComplete: j.historyComplete,
        historicalPDSAttribution: "unknown",
      })),
      next: jobs.nextCursor ?? null,
    });
  });
  app.get("/api/v1/limits", (req, res) =>
    services
      .call(
        "relay",
        `/limits?after=${encodeURIComponent(page.parse(req.query).after)}`,
      )
      .then((v) => res.json(v)),
  );
  app.get("/api/v1/status", async (_req, res) => {
    const checks = await Promise.allSettled([
      services.sources(),
      services.policy(),
    ]);
    res.json({
      relay: checks[0].status === "fulfilled" ? "available" : "unavailable",
      jetstream: checks[1].status === "fulfilled" ? "available" : "unavailable",
      observedAt: new Date().toISOString(),
    });
  });
  app.use("/api", (_req, res) => res.status(404).json({ error: "not_found" }));
  if (staticDir) {
    app.use(express.static(staticDir));
    app.get("/{*path}", (_req, res) =>
      res.sendFile(join(staticDir, "index.html")),
    );
  }
  const errors: ErrorRequestHandler = (error, _req, res, _next) => {
    if (error instanceof ZodError) {
      res.status(400).json({
        error: "invalid_input",
        details: error.issues.map((i) => `${i.path.join(".")}: ${i.message}`),
      });
      return;
    }
    if (error instanceof ApiError) {
      res.status(error.status).json({ error: error.code });
      return;
    }
    if (error?.type === "entity.too.large") {
      res.status(413).json({ error: "request_too_large" });
      return;
    }
    if (error instanceof SyntaxError) {
      res.status(400).json({ error: "invalid_json" });
      return;
    }
    res.status(500).json({ error: "request_failed" });
  };
  app.use(errors);
  return app;
}
