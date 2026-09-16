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
const timer = setInterval(() => void worker.tick(), 50);
const server = createApp(
  store,
  services,
  new Auth(store, base, oauth),
  {},
  resolve("dist"),
).listen(3188, "127.0.0.1");
let stopping = false;
async function shutdown() {
  if (stopping) return;
  stopping = true;
  clearInterval(timer);
  server.close();
  store.close();
  await fixtures.close();
}
process.once("SIGINT", () => void shutdown());
process.once("SIGTERM", () => void shutdown());
