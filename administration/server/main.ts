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
  if (!value) throw new Error(`${name} is required`);
  return value;
}
function secret(name: string) {
  const value = readFileSync(required(name), "utf8").trim();
  if (value.length < 32)
    throw new Error(`${name} must contain at least 32 bytes`);
  return value;
}
function publicOrigin(name: string) {
  const value = process.env[name];
  if (!value) return undefined;
  const url = new URL(value);
  const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (
    (url.protocol !== "https:" && !(url.protocol === "http:" && loopback)) ||
    url.username ||
    url.password ||
    url.pathname !== "/" ||
    url.search ||
    url.hash
  )
    throw new Error(
      `${name} must be a public HTTPS origin without credentials, path, query, or fragment`,
    );
  return url.origin;
}
const base = new URL(required("ADMIN_PUBLIC_ORIGIN")).origin;
if (
  !base.startsWith("https:") &&
  !["127.0.0.1", "[::1]"].includes(new URL(base).hostname)
)
  throw new Error("Use HTTPS or a loopback development origin");
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
  { railwayPrivateNetwork: process.env.HC_RAILWAY_STARTUP === "1" },
);
store.seedAdministrator(process.env.ADMIN_SEED_DID);
const worker = new Worker(store, services);
worker.recover();
const server = createApp(
  store,
  services,
  new Auth(store, base, provider),
  metadata,
  resolve("dist"),
  {
    railway: process.env.HC_RAILWAY_STARTUP === "1",
    trustProxy:
      process.env.ADMIN_TRUST_PROXY?.split(",").map((value) => value.trim()) ??
      (process.env.HC_RAILWAY_STARTUP === "1"
        ? ["loopback", "linklocal", "uniquelocal"]
        : undefined),
    publicServiceOrigins: {
      relay: publicOrigin("RELAY_PUBLIC_ORIGIN"),
      rainbow: publicOrigin("RAINBOW_PUBLIC_ORIGIN"),
      jetstream: publicOrigin("JETSTREAM_PUBLIC_ORIGIN"),
    },
  },
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
