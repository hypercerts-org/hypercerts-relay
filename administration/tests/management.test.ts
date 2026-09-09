import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { mkdtempSync, readFileSync, existsSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve, join, isAbsolute } from "node:path";
import { Store } from "../server/store.ts";
import { Services } from "../server/services.ts";
import { Worker } from "../server/worker.ts";
import { command } from "../server/contracts.ts";

test(
  "real Relay and Jetstream APIs apply durable requests and preserve incomplete coverage",
  { timeout: 360000 },
  async (t) => {
    const goBinary = process.env.CONTROL_GO_BINARY;
    if (!goBinary || !isAbsolute(goBinary))
      throw new Error(
        "CONTROL_GO_BINARY must identify an absolute Go executable path",
      );
    const temp = mkdtempSync(join(tmpdir(), "management-acceptance-"));
    const compile = promisify(execFile);
    const fixtures = [
      { name: "relay", cwd: resolve(".."), pkg: "./cmd/relay" },
      {
        name: "jetstream",
        cwd: resolve("../jetstream"),
        pkg: "./internal/hypercerts/control",
      },
    ];
    // Finish cold dependency downloads and compilation before starting either
    // bounded fixture server. Compilation is not part of its startup deadline.
    await Promise.all(
      fixtures.map(({ name, cwd, pkg }) =>
        compile(
          goBinary,
          ["test", "-c", "-o", join(temp, name + ".test"), pkg],
          {
            cwd,
            timeout: 300000,
            signal: t.signal,
            maxBuffer: 2 * 1024 * 1024,
          },
        ),
      ),
    );
    async function fixture(name: string, cwd: string) {
      const ready = join(temp, name);
      let output = "";
      const child = spawn(
        join(temp, name + ".test"),
        ["-test.run=^TestControlPlaneAcceptanceFixture$", "-test.count=1"],
        { cwd, env: { ...process.env, CONTROL_ACCEPTANCE_READY: ready } },
      );
      child.stdout.on("data", (b) => (output += b));
      child.stderr.on("data", (b) => (output += b));
      const done = new Promise<{ code: number | null; error?: Error }>(
        (resolve) => {
          child.on("error", (error) => resolve({ code: null, error }));
          child.on("exit", (code) => resolve({ code }));
        },
      );
      t.after(async () => {
        writeFileSync(ready + ".stop", "");
        const deadline = setTimeout(() => child.kill("SIGKILL"), 5000);
        try {
          const result = await done;
          assert.ifError(result.error);
          assert.equal(result.code, 0, output);
        } finally {
          clearTimeout(deadline);
        }
      });
      const start = Date.now();
      while (!existsSync(ready)) {
        if (child.exitCode !== null || Date.now() - start > 15000)
          throw new Error(output || "Service fixture did not start");
        t.signal.throwIfAborted();
        await new Promise((r) => setTimeout(r, 50));
      }
      return readFileSync(ready, "utf8");
    }
    const [relay, jetstream] = await Promise.all(
      fixtures.map(({ name, cwd }) => fixture(name, cwd)),
    );
    const token = "fixture-service-credential-32-bytes-minimum";
    const services = new Services(
      { url: relay, token },
      { url: jetstream, token },
    );
    const store = new Store(join(temp, "control.db"));
    t.after(() => store.close());
    const worker = new Worker(store, services);
    async function apply(input: unknown) {
      const op = store.request("did:plc:operator", command.parse(input));
      await worker.tick();
      const result = store.operation(op.id)!;
      assert.equal(result.state, "applied", JSON.stringify(result));
      return result;
    }
    await apply({
      kind: "source",
      pds: "https://pds.example",
      state: "enabled",
    });
    const sources = await services.sources();
    assert.equal((sources.Sources as unknown[]).length, 1);
    await apply({
      kind: "collections",
      expectedRevision: 1,
      collections: ["app.bsky.feed.post", "app.bsky.feed.like"],
    });
    assert.equal((await services.policy()).revision, 2);
    await apply({ kind: "limit", scope: "global", eventsPerSecond: 15 });
    await apply({
      kind: "limit",
      scope: "https://pds.example",
      eventsPerSecond: 3,
    });
    const limits = await services.call<{ items: unknown[] }>(
      "relay",
      "/limits",
    );
    assert.equal(limits.items.length, 2);
    const job = await apply({
      kind: "job",
      pds: "https://pds.example",
      reason: "quota_recovery",
    });
    const jobID = (job.result as { id: string }).id;
    const again = await services.apply(job.command, job.id);
    assert.equal((again as { id: string }).id, jobID);
    await new Promise((r) => setTimeout(r, 300));
    const jobs = await services.jobs();
    assert.ok(jobs.jobs.every((j) => !j.historyComplete));
    assert.ok(jobs.jobs.some((j) => j.state === "incomplete"));
    await services.call("relay", "/source", "PUT", {
      pds: "https://relay-only.example",
      state: "enabled",
    });
    await apply({
      kind: "source",
      pds: "https://relay-only.example",
      state: "disabled",
    });
    await apply({
      kind: "source",
      pds: "https://relay-only.example",
      state: "removed",
    });
    for (const state of ["disabled", "enabled", "removed"])
      await apply({ kind: "source", pds: "https://pds.example", state });
  },
);
