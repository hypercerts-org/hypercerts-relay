import { test } from "node:test";
import assert from "node:assert/strict";
import { administratorProfile } from "../server/profile.ts";
import { Store } from "../server/store.ts";
import { Auth } from "../server/auth.ts";
import type { Request, Response } from "express";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";

const did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
const resolver = {
  async resolveIdentity() {
    return { did, handle: "operator.example" };
  },
};
test("profile lookup uses the authenticated DID and prefers Certified with Bluesky fallback", async () => {
  for (const preferred of [
    "app.certified.actor.profile",
    "app.bsky.actor.profile",
  ]) {
    const requests: string[] = [];
    const profile = await administratorProfile(resolver, {
      did,
      async fetchHandler(path) {
        const query = new URL(path, "https://pds.example").searchParams;
        assert.equal(query.get("repo"), did);
        assert.equal(query.get("rkey"), "self");
        const collection = query.get("collection")!;
        requests.push(collection);
        return collection === preferred
          ? Response.json({
              uri: `at://${did}/${collection}/self`,
              value: { displayName: "  Test Operator  " },
            })
          : new Response(null, { status: 404 });
      },
    });
    assert.deepEqual(profile, {
      handle: "operator.example",
      displayName: "Test Operator",
    });
    assert.equal(requests[0], "app.certified.actor.profile");
  }
});
test("profile lookup rejects mismatched identities and oversized records and bounds stalled requests", async (t) => {
  const profile = await administratorProfile(
    {
      async resolveIdentity() {
        return { did: "did:plc:other", handle: "wrong.example" };
      },
    },
    {
      did,
      async fetchHandler() {
        return Response.json({ value: { displayName: "x".repeat(100000) } });
      },
    },
  );
  assert.deepEqual(profile, {});
  const missing = await administratorProfile(
    {
      async resolveIdentity() {
        return { did, handle: "handle.invalid" };
      },
    },
    {
      did,
      async fetchHandler() {
        return new Response(null, { status: 404 });
      },
    },
  );
  assert.deepEqual(missing, { handle: null, displayName: null });
  const notFound = await administratorProfile(resolver, {
    did,
    async fetchHandler() {
      return Response.json({ error: "RecordNotFound" }, { status: 400 });
    },
  });
  assert.equal(notFound.displayName, null);
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const stalled = administratorProfile(
    { resolveIdentity: () => new Promise(() => {}) },
    { did, fetchHandler: () => new Promise(() => {}) },
  );
  t.mock.timers.tick(4000);
  assert.deepEqual(await stalled, {});
});
test("sign-in persists and refreshes profile presentation without changing membership or requiring profile availability", async (t) => {
  const dir = mkdtempSync(join(tmpdir(), "administrator-profile-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, "state.db");
  let store = new Store(path);
  store.seedAdministrator(did);
  const binding = "test-browser-binding";
  let failed = false;
  let displayName = "First Name";
  const provider = {
    async authorize() {
      return new URL("https://pds.example/authorize");
    },
    async callback() {
      return {
        session: { did },
        state: createHash("sha256").update(binding).digest("hex"),
      };
    },
    async revoke() {},
    async profile() {
      if (failed) throw new Error("unavailable");
      return { handle: "operator.example", displayName };
    },
  };
  const request = {
    headers: { cookie: `relay_oauth=${binding}` },
    originalUrl: "/auth/callback?code=fixture",
  } as Request;
  let signedIn = false;
  const response = {
    clearCookie() {},
    cookie() {
      signedIn = true;
    },
    redirect(status: number) {
      assert.equal(status, 303);
    },
  } as unknown as Response;
  await new Auth(store, "https://admin.example", provider).callback(
    request,
    response,
  );
  assert.ok(signedIn);
  store.close();
  store = new Store(path);
  assert.equal(store.administrators("", 50).items[0].displayName, "First Name");
  displayName = "Updated Name";
  await new Auth(store, "https://admin.example", provider).callback(
    request,
    response,
  );
  failed = true;
  await new Auth(store, "https://admin.example", provider).callback(
    request,
    response,
  );
  assert.equal(
    store.administrators("", 50).items[0].displayName,
    "Updated Name",
  );
  const other = "did:plc:bbbbbbbbbbbbbbbbbbbbbbbb";
  store.changeAdministrator(did, other, "grant");
  store.changeAdministrator(other, did, "remove");
  assert.equal(store.get("administrator-profile", did), undefined);
  signedIn = false;
  await assert.rejects(
    new Auth(store, "https://admin.example", provider).callback(
      request,
      response,
    ),
    /administrator_required/,
  );
  assert.equal(signedIn, false);
  store.changeAdministrator(other, did, "grant");
  store.set("administrator-profile", did, {
    handle: "operator.example",
    displayName: "Cached Name",
  });
  store.close();
  execFileSync(
    process.execPath,
    ["--import", "tsx", "server/access.ts", "remove", did],
    { env: { ...process.env, ADMIN_DATABASE: path }, stdio: "pipe" },
  );
  store = new Store(path);
  assert.equal(store.get("administrator-profile", did), undefined);
  assert.ok(!store.administrators("", 50).items.some((row) => row.did === did));
  store.close();
});
