import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { createHash } from "node:crypto";
import { Store } from "./store.ts";
import { Auth, oauthProvider } from "./auth.ts";
import { createApp } from "./app.ts";
import { Services } from "./services.ts";
import { Worker } from "./worker.ts";

function required(name: string) {
  const value = process.env[name];
  if (!value) throw Error(`${name} is required`);
  return value;
}
function secret(name: string) {
  const value = readFileSync(required(name), "utf8").trim();
  if (value.length < 32) throw Error(`${name} must contain at least 32 bytes`);
  return value;
}
const base = new URL(required("ADMIN_PUBLIC_ORIGIN")).origin;
if (
  !base.startsWith("https:") &&
  !["127.0.0.1", "[::1]"].includes(new URL(base).hostname)
)
  throw Error("Use HTTPS or a loopback development origin");
const store = new Store(required("ADMIN_DATABASE"));
const key = createHash("sha256")
  .update(secret("ADMIN_ENCRYPTION_KEY_FILE"))
  .digest();
const { provider, metadata } = oauthProvider(store, base, key);
const services = new Services(
  {
    url: required("RELAY_CONTROL_URL"),
    token: secret("RELAY_CONTROL_TOKEN_FILE"),
  },
  {
    url: required("JETSTREAM_CONTROL_URL"),
    token: secret("JETSTREAM_CONTROL_TOKEN_FILE"),
  },
);
const worker = new Worker(store, services);
worker.recover();
const server = createApp(
  store,
  services,
  new Auth(store, base, provider),
  metadata,
  resolve("dist"),
).listen(
  Number(process.env.PORT ?? 3000),
  process.env.ADMIN_BIND ?? "127.0.0.1",
);
let stopping = false;
async function run() {
  while (!stopping) {
    await worker.tick();
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
}
const running = run().catch(() => {
  console.error("Control-plane persistence failed; stopping.");
  stopping = true;
  server.close();
  process.exitCode = 1;
});
async function shutdown() {
  if (stopping) return;
  stopping = true;
  server.close();
  await running;
  store.close();
}
process.once("SIGINT", shutdown);
process.once("SIGTERM", shutdown);
