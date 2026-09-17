// T13 deliberately uses only a local OAuth double. It proves the application
// boundary and real loopback service owners, not external OAuth qualification.
import { test } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { once } from "node:events";
import { createServer } from "node:net";
import { createApp } from "../server/app.ts";
import { Auth, type OAuthProvider } from "../server/auth.ts";
import { Store } from "../server/store.ts";
import { Services } from "../server/services.ts";
import { Worker } from "../server/worker.ts";
import { startControlFixtures } from "./control-fixtures.ts";

const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";

function cookies(response: Response) {
  return response.headers
    .getSetCookie()
    .map((value) => value.split(";", 1)[0])
    .join("; ");
}

test(
  "T13 local OAuth double authorizes CSRF-protected durable actions through real owners",
  { timeout: 360_000 },
  async (t) => {
    const fixtures = await startControlFixtures();
    t.after(() => fixtures.close());
    const dir = mkdtempSync(join(tmpdir(), "t13-administration-"));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const store = new Store(join(dir, "control.db"));
    t.after(() => {
      try {
        store.close();
      } catch {
        // The test closes the store before reopening it for its durability seal.
      }
    });
    store.seedAdministrator(did);
    const reservation = createServer();
    reservation.listen(0, "127.0.0.1");
    await once(reservation, "listening");
    const { port } = reservation.address() as { port: number };
    await new Promise<void>((resolve) => reservation.close(() => resolve()));
    const base = `http://127.0.0.1:${port}`;
    const provider: OAuthProvider = {
      async authorize(input, options) {
        return new URL(
          `${base}/auth/callback?state=${options.state}&admin=${input === "operator.example"}`,
        );
      },
      async callback(params) {
        return {
          session: {
            did:
              params.get("admin") === "true"
                ? did
                : "did:plc:not-an-administrator",
          },
          state: params.get("state"),
        };
      },
      async revoke() {},
    };
    const services = new Services(
      { url: fixtures.relayURL, token: fixtures.token },
      { url: fixtures.jetstreamURL, token: fixtures.token },
    );
    const worker = new Worker(store, services);
    const auth = new Auth(store, base, provider);
    const app = createApp(store, services, auth, {});
    const appListener = app.listen(port, "127.0.0.1");
    await once(appListener, "listening");
    t.after(() => appListener.close());

    const login = await fetch(`${base}/auth/login`, {
      method: "POST",
      headers: { Origin: base, "Content-Type": "application/json" },
      body: JSON.stringify({ handle: "operator.example" }),
    });
    assert.equal(login.status, 200);
    const oauthCookie = cookies(login);
    const { redirectUrl } = (await login.json()) as { redirectUrl: string };
    const callback = await fetch(redirectUrl, {
      headers: { Cookie: oauthCookie },
      redirect: "manual",
    });
    assert.equal(callback.status, 303);
    const sessionCookie = cookies(callback);
    const sessionResponse = await fetch(`${base}/api/v1/session`, {
      headers: { Cookie: sessionCookie },
    });
    assert.equal(sessionResponse.status, 200);
    const session = (await sessionResponse.json()) as { csrf: string };

    const operation = { kind: "limit", scope: "global", eventsPerSecond: 17 };
    const rejectedHeaders: Record<string, string>[] = [
      { Cookie: sessionCookie },
      {
        Cookie: sessionCookie,
        Origin: "https://evil.example",
        "X-CSRF-Token": session.csrf,
      },
    ];
    for (const headers of rejectedHeaders) {
      const rejected = await fetch(`${base}/api/v1/operations`, {
        method: "POST",
        headers: {
          ...headers,
          "Content-Type": "application/json",
          "Idempotency-Key": randomUUID(),
        },
        body: JSON.stringify(operation),
      });
      assert.equal(rejected.status, 403);
    }
    const accepted = await fetch(`${base}/api/v1/operations`, {
      method: "POST",
      headers: {
        Cookie: sessionCookie,
        Origin: base,
        "X-CSRF-Token": session.csrf,
        "Content-Type": "application/json",
        "Idempotency-Key": randomUUID(),
      },
      body: JSON.stringify(operation),
    });
    assert.equal(accepted.status, 202);
    const created = (await accepted.json()) as { id: string };
    await worker.tick();
    assert.equal(store.operation(created.id)?.state, "applied");
    assert.equal(
      (
        await services.call<{
          items: { scope: string; eventsPerSecond: number }[];
        }>("relay", "/limits")
      ).items.find((item) => item.scope === "global")?.eventsPerSecond,
      17,
    );
    store.set("oauth-session", did, { fixture: true });
    const revoked = await fetch(`${base}/api/v1/revoke-sessions`, {
      method: "POST",
      headers: {
        Cookie: sessionCookie,
        Origin: base,
        "X-CSRF-Token": session.csrf,
      },
    });
    assert.equal(revoked.status, 204);
    assert.equal(store.get("oauth-session", did), undefined);
    assert.equal(
      (
        await fetch(`${base}/api/v1/session`, {
          headers: { Cookie: sessionCookie },
        })
      ).status,
      401,
    );
    store.close();
    const reopened = new Store(join(dir, "control.db"));
    assert.equal(reopened.operation(created.id)?.state, "applied");
    reopened.close();
  },
);
