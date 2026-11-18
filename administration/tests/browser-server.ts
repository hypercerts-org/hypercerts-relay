// Test-only OAuth provider; production main never imports it. The service owners
// below are real loopback Relay and Jetstream private-control fixtures.
import { createApp } from "../server/app.ts";
import { Auth, type OAuthProvider } from "../server/auth.ts";
import { Store } from "../server/store.ts";
import { Services } from "../server/services.ts";
import { Worker } from "../server/worker.ts";
import { resolve } from "node:path";
import type { Server } from "node:http";
import {
  startControlFixtures,
  type ControlFixtures,
} from "./control-fixtures.ts";
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
  async profile() {
    return { handle: "operator.example", displayName: "Test Operator" };
  },
};
const startup = new AbortController();
let fixtures: ControlFixtures | undefined;
let timer: ReturnType<typeof setInterval> | undefined;
let server: Server | undefined;
const ticks = new Set<Promise<void>>();
async function start() {
  fixtures = await startControlFixtures(startup.signal);
  startup.signal.throwIfAborted();
  const services = new Services(
    { url: fixtures.relayURL, token: fixtures.token },
    { url: fixtures.jetstreamURL, token: fixtures.token },
  );
  const worker = new Worker(store, services);
  function tick() {
    const pending = worker.tick();
    ticks.add(pending);
    void pending.finally(() => ticks.delete(pending)).catch(console.error);
  }
  timer = setInterval(tick, 50);
  await new Promise<void>((resolveListening, reject) => {
    server = createApp(
      store,
      services,
      new Auth(store, base, oauth),
      {},
      resolve("dist"),
    ).listen(3188, "127.0.0.1");
    server.once("error", reject);
    server.once("listening", () => {
      server!.off("error", reject);
      resolveListening();
    });
  });
}
let stopping: Promise<void> | undefined;
function closeServer() {
  return new Promise<void>((resolve, reject) => {
    if (!server?.listening) return resolve();
    server.close((error) => (error ? reject(error) : resolve()));
  });
}
// Startup that this shutdown cancelled is expected. Any other startup error,
// including a cleanup failure while cancelling, fails the exit status.
function cancelled(error: unknown) {
  return (error as { name?: unknown } | undefined)?.name === "AbortError";
}
// A worker tick can wait up to the service timeout for an owner call. Give
// in-flight ticks a short grace period, stop the owners so any remaining call
// fails, then wait again, so the fixtures close within Playwright's window.
const tickGrace = 2000;
function settled(pending: Promise<unknown>, milliseconds: number) {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([
    pending.then(() => true),
    new Promise<boolean>((resolve) => {
      timeout = setTimeout(() => resolve(false), milliseconds);
    }),
  ]).finally(() => clearTimeout(timeout));
}
// Every caller waits for the same shutdown, so a repeated signal neither
// starts a second one nor exits before the first has finished.
function shutdown() {
  stopping ??= (async () => {
    startup.abort();
    // Startup owns its catch-path cleanup until it settles, even when the
    // signal arrives during compilation or before a fixture is ready.
    const failure = await starting.then(
      () => undefined,
      (error: unknown) => error,
    );
    clearInterval(timer);
    const drained = await settled(Promise.allSettled(ticks), tickGrace);
    await closeServer();
    await fixtures?.close();
    if (!drained) await Promise.allSettled(ticks);
    store.close();
    if (failure !== undefined && !cancelled(failure)) throw failure;
  })();
  return stopping;
}
// Playwright stops this server with SIGTERM to its whole process group
// (gracefulShutdown in playwright.config.ts). Handle every signal, since an
// unhandled repeat would kill the process while it removes the fixtures'
// temporary directory, and exit once that is done. Shutdown rejects for a
// startup failure, so a signal joining that cleanup keeps the failure status.
function stop() {
  shutdown().then(
    () => process.exit(0),
    (error: unknown) => {
      console.error(error);
      process.exit(1);
    },
  );
}
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
const starting = start();
// A startup failure shuts down the same way; shutdown reports the failure.
starting.catch(stop);
