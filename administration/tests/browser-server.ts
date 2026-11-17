// Test-only OAuth provider; production main never imports it. The service owners
// below are real loopback Relay and Jetstream private-control fixtures.
import { createApp } from "../server/app.ts";
import { Auth, type OAuthProvider } from "../server/auth.ts";
import { Store } from "../server/store.ts";
import { Services } from "../server/services.ts";
import { Worker } from "../server/worker.ts";
import { resolve } from "node:path";
import { startControlFixtures } from "./control-fixtures.ts";
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
const fixtures = await startControlFixtures();
const services = new Services(
  { url: fixtures.relayURL, token: fixtures.token },
  { url: fixtures.jetstreamURL, token: fixtures.token },
);
const worker = new Worker(store, services);
const ticks = new Set<Promise<void>>();
function tick() {
  const pending = worker.tick();
  ticks.add(pending);
  void pending.finally(() => ticks.delete(pending)).catch(console.error);
}
const timer = setInterval(tick, 50);
const server = createApp(
  store,
  services,
  new Auth(store, base, oauth),
  {},
  resolve("dist"),
).listen(3188, "127.0.0.1");
let stopping: Promise<void> | undefined;
function closeServer() {
  return new Promise<void>((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });
}
// Every caller waits for the same shutdown, so a repeated signal neither
// starts a second one nor exits before the first has finished.
function shutdown() {
  stopping ??= (async () => {
    clearInterval(timer);
    await Promise.allSettled(ticks);
    await closeServer();
    store.close();
    await fixtures.close();
  })();
  return stopping;
}
// Playwright stops this server with SIGTERM to its whole process group
// (gracefulShutdown in playwright.config.ts). Handle every signal, since an
// unhandled repeat would kill the process while it removes the fixtures'
// temporary directory, and exit once that is done.
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
