import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { Store } from "../server/store.ts";

const seed = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const other = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";

test("seed is a one-time ordinary administrator and stays revoked after reopen or configuration changes", (t) => {
  const dir = mkdtempSync(join(tmpdir(), "admin-seed-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, "control.db");
  let store = new Store(path);
  store.seedAdministrator();
  assert.deepEqual(store.administrators("", 50).items, []);
  assert.throws(() => store.seedAdministrator("operator.example"), /valid DID/);
  store.seedAdministrator(seed);
  store.seedAdministrator(seed);
  store.changeAdministrator(seed, other, "grant");
  store.changeAdministrator(other, seed, "remove");
  store.close();
  store = new Store(path);
  store.seedAdministrator(seed);
  store.seedAdministrator("did:plc:cccccccccccccccccccccccc");
  assert.deepEqual(
    store.administrators("", 50).items.map((row) => row.did),
    [other],
  );
  assert.equal(
    store.db
      .prepare(
        "SELECT COUNT(*) AS n FROM audit WHERE action='administrator_seed'",
      )
      .get()!.n,
    1,
  );
  // Even removal of every admin through local recovery must not reseed.
  store.db.prepare("DELETE FROM administrators").run();
  store.close();
  store = new Store(path);
  store.seedAdministrator(seed);
  assert.deepEqual(store.administrators("", 50).items, []);
  store.close();
});

test("existing access history blocks seeding and seed membership, audit and marker commit atomically", () => {
  const store = new Store(":memory:");
  store.db.exec(
    "CREATE TRIGGER fail_seed BEFORE INSERT ON audit BEGIN SELECT RAISE(ABORT, 'audit unavailable'); END;",
  );
  assert.throws(() => store.seedAdministrator(seed), /audit unavailable/);
  assert.deepEqual(store.administrators("", 50).items, []);
  assert.equal(store.get("bootstrap", "administrator"), undefined);
  store.db.exec("DROP TRIGGER fail_seed");
  store.audit("local:operator", "administrator_remove", { did: seed });
  store.seedAdministrator(seed);
  assert.deepEqual(store.administrators("", 50).items, []);
  store.close();
  const existing = new Store(":memory:");
  existing.db.prepare("INSERT INTO administrators VALUES(?)").run(other);
  existing.seedAdministrator(seed);
  assert.deepEqual(
    existing.administrators("", 50).items.map((row) => row.did),
    [other],
  );
  existing.close();
});

test("production startup consumes ADMIN_SEED_DID without requiring the recovery CLI", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "admin-startup-"));
  const path = join(dir, "control.db");
  const secret = join(dir, "secret");
  writeFileSync(secret, "test-only-local-secret-".repeat(3), { mode: 0o600 });
  const child = spawn(process.execPath, ["--import", "tsx", "server/main.ts"], {
    env: {
      ...process.env,
      ADMIN_DATABASE: path,
      ADMIN_SEED_DID: seed,
      ADMIN_PUBLIC_ORIGIN: "http://127.0.0.1:3000",
      ADMIN_BIND: "127.0.0.1",
      PORT: "0",
      ADMIN_ENCRYPTION_KEY_FILE: secret,
      RELAY_CONTROL_TOKEN_FILE: secret,
      JETSTREAM_CONTROL_TOKEN_FILE: secret,
      RELAY_CONTROL_URL: "http://127.0.0.1:1",
      JETSTREAM_CONTROL_URL: "http://127.0.0.1:1",
    },
    stdio: "ignore",
  });
  t.after(async () => {
    if (child.exitCode === null) {
      child.kill("SIGTERM");
      await once(child, "exit");
    }
    rmSync(dir, { recursive: true, force: true });
  });
  let found = false;
  for (let i = 0; i < 100; i++) {
    assert.equal(
      child.exitCode,
      null,
      "production server exited before seeding",
    );
    await new Promise((resolve) => setTimeout(resolve, 50));
    const db = new Store(path);
    found = db.administrators("", 50).items.some((row) => row.did === seed);
    db.close();
    if (found) break;
  }
  assert.ok(found, "production startup did not grant the seed DID");
});
