import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { setTimeout as delay } from "node:timers/promises";
import { test, type TestContext } from "node:test";
import { startControlFixtures } from "./control-fixtures.ts";

async function waitFor(predicate: () => boolean) {
  const deadline = Date.now() + 3000;
  while (!predicate()) {
    assert.ok(
      Date.now() < deadline,
      "lifecycle event arrived within 3 seconds",
    );
    await delay(10);
  }
}

async function bounded<T>(
  pending: Promise<T>,
  milliseconds = 3000,
): Promise<T> {
  let timeout: NodeJS.Timeout;
  try {
    return await Promise.race([
      pending,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error("cleanup timed out")),
          milliseconds,
        );
      }),
    ]);
  } finally {
    clearTimeout(timeout!);
  }
}

function alive(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function harness(t: TestContext, mode: string) {
  const root = mkdtempSync(join(tmpdir(), "fixture-cleanup-regression-"));
  const go = join(root, "go.mjs");
  const previousGo = process.env.CONTROL_GO_BINARY;
  const children: Array<{ child: ChildProcess; done: Promise<number | null> }> =
    [];
  // These process doubles exercise only the harness lifecycle. The acceptance
  // suites continue compiling and exercising the real Go owner APIs.
  const fixture = `
const { createServer } = require("node:http");
const { existsSync, writeFileSync } = require("node:fs");
const ready = process.env.CONTROL_ACCEPTANCE_READY;
writeFileSync(${JSON.stringify(root)} + "/" + ${JSON.stringify("fixture-")} + ready.split("/").at(-1) + ".pid", String(process.pid));
if (${JSON.stringify(mode)} !== "block-startup") {
  const server = createServer((request, response) => { response.writeHead(204); response.end(); });
  server.listen(0, "127.0.0.1", () => writeFileSync(ready, "http://127.0.0.1:" + server.address().port));
  setInterval(() => {
    if (existsSync(ready + ".stop")) server.close(() => process.exit(0));
  }, 10);
} else setInterval(() => {}, 1000);
`;
  writeFileSync(
    go,
    `#!${process.execPath}
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
const root = ${JSON.stringify(root)};
const mode = ${JSON.stringify(mode)};
const output = process.argv[process.argv.indexOf("-o") + 1];
const name = basename(output, ".test");
writeFileSync(join(root, name + ".dir"), dirname(output));
writeFileSync(join(root, name + ".pid"), String(process.pid));
// Publish the marker only once this double can handle SIGTERM, so a test that
// aborts on it never signals a double that would exit without its markers.
const compiling = () => writeFileSync(join(root, name + ".compiling"), "");
if (mode === "block-compile") {
  process.on("SIGTERM", () => setTimeout(() => {
    writeFileSync(join(root, name + ".directory-present"), String(existsSync(dirname(output))));
    process.exit(0);
  }, 150));
  setInterval(() => {}, 1000);
  compiling();
} else if (mode === "stubborn-compile") {
  process.on("SIGTERM", () => writeFileSync(join(root, name + ".aborted"), ""));
  setInterval(() => {
    if (!existsSync(dirname(output))) writeFileSync(join(root, name + ".directory-missing"), "");
    mkdirSync(dirname(output), { recursive: true });
    writeFileSync(output, "compiler still writing");
  }, 10);
  compiling();
} else if (mode === "fail-compile") {
  compiling();
  if (name === "relay") {
    while (!existsSync(join(root, "jetstream.compiling"))) await delay(10);
    process.stderr.write("intentional compilation failure");
    process.exit(1);
  }
  await delay(200);
  writeFileSync(join(root, name + ".directory-present"), String(existsSync(dirname(output))));
  mkdirSync(dirname(output), { recursive: true });
  writeFileSync(output, "sibling compiler finished");
} else {
  compiling();
  writeFileSync(output, ${JSON.stringify(`#!${process.execPath}
${fixture}`)}, { mode: 0o700 });
}
`,
    { mode: 0o700 },
  );
  process.env.CONTROL_GO_BINARY = go;
  const marker = (name: string) => join(root, name);
  const dirs = () =>
    readdirSync(root)
      .filter((name) => name.endsWith(".dir"))
      .map((name) => readFileSync(marker(name), "utf8"));
  t.after(async () => {
    for (const { child } of children) child.kill("SIGKILL");
    await Promise.all(children.map(({ done }) => done));
    const pids = readdirSync(root)
      .filter((name) => name.endsWith(".pid"))
      .map((name) => Number(readFileSync(marker(name), "utf8")));
    for (const pid of pids) if (alive(pid)) process.kill(pid, "SIGKILL");
    await waitFor(() => pids.every((pid) => !alive(pid)));
    for (const dir of dirs()) rmSync(dir, { recursive: true, force: true });
    rmSync(root, { recursive: true, force: true });
    if (previousGo === undefined) delete process.env.CONTROL_GO_BINARY;
    else process.env.CONTROL_GO_BINARY = previousGo;
  });
  return {
    marker,
    async compiling() {
      await waitFor(
        () =>
          existsSync(marker("relay.compiling")) &&
          existsSync(marker("jetstream.compiling")),
      );
    },
    assertRemoved() {
      assert.ok(dirs().length > 0);
      for (const dir of dirs()) assert.equal(existsSync(dir), false, dir);
    },
    assertCompilersReaped() {
      for (const name of ["relay", "jetstream"]) {
        assert.equal(
          alive(Number(readFileSync(marker(name + ".pid"), "utf8"))),
          false,
        );
      }
    },
    spawn(file: string) {
      const child = spawn(process.execPath, ["--import", "tsx", file], {
        env: process.env,
        stdio: ["ignore", "pipe", "pipe"],
      });
      let output = "";
      child.stdout.on("data", (data) => (output += data));
      child.stderr.on("data", (data) => (output += data));
      const done = new Promise<number | null>((resolve) =>
        child.once("close", resolve),
      );
      children.push({ child, done });
      return { child, done, output: () => output };
    },
  };
}

test("control fixtures close after a fixture has already exited by signal", async (t) => {
  const h = harness(t, "success");
  const fixtures = await startControlFixtures();
  const pid = Number(readFileSync(h.marker("fixture-relay.pid"), "utf8"));
  process.kill(pid, "SIGTERM");
  await waitFor(() => !alive(pid));
  await bounded(fixtures.close());
  h.assertRemoved();
});

test("control fixture cancellation reaps compilers before removing their directory", async (t) => {
  const h = harness(t, "block-compile");
  const abort = new AbortController();
  const started = startControlFixtures(abort.signal);
  const rejected = assert.rejects(started, { name: "AbortError" });
  await h.compiling();
  abort.abort();
  await bounded(rejected);
  for (const name of ["relay", "jetstream"])
    assert.equal(
      readFileSync(h.marker(name + ".directory-present"), "utf8"),
      "true",
    );
  h.assertCompilersReaped();
  h.assertRemoved();
});

test("control fixture cancellation reaps a fixture still starting", async (t) => {
  const h = harness(t, "block-startup");
  const abort = new AbortController();
  const started = startControlFixtures(abort.signal);
  const rejected = assert.rejects(started, { name: "AbortError" });
  await waitFor(() => existsSync(h.marker("fixture-relay.pid")));
  const pid = Number(readFileSync(h.marker("fixture-relay.pid"), "utf8"));
  abort.abort();
  await bounded(rejected);
  assert.equal(alive(pid), false);
  h.assertRemoved();
});

test("control fixture compilation failure waits for its sibling writer", async (t) => {
  const h = harness(t, "fail-compile");
  await assert.rejects(
    startControlFixtures(),
    /intentional compilation failure/,
  );
  assert.equal(
    readFileSync(h.marker("jetstream.directory-present"), "utf8"),
    "true",
  );
  h.assertCompilersReaped();
  h.assertRemoved();
});

test("management compilation failure waits for its sibling writer", async (t) => {
  const h = harness(t, "fail-compile");
  const child = h.spawn("tests/management.test.ts");
  assert.equal(await bounded(child.done), 1, child.output());
  assert.match(child.output(), /intentional compilation failure/);
  assert.equal(
    readFileSync(h.marker("jetstream.directory-present"), "utf8"),
    "true",
  );
  h.assertCompilersReaped();
  h.assertRemoved();
});

test("browser server SIGTERM during compilation waits for startup cleanup", async (t) => {
  const h = harness(t, "block-compile");
  const child = h.spawn("tests/browser-server.ts");
  await h.compiling();
  child.child.kill("SIGTERM");
  assert.equal(await bounded(child.done), 0, child.output());
  h.assertCompilersReaped();
  h.assertRemoved();
});

test("management cancellation keeps its directory until stubborn compilers are reaped", async (t) => {
  const h = harness(t, "stubborn-compile");
  const runner = h.marker("cancel-management.mjs");
  writeFileSync(
    runner,
    `import { test } from "node:test";
import { existsSync } from "node:fs";
import { setTimeout as delay } from "node:timers/promises";
const abort = new AbortController();
await test("cancel management during compilation", { signal: abort.signal }, async () => {
  await import(${JSON.stringify(pathToFileURL(resolve("tests/management.test.ts")).href)});
  while (!existsSync(${JSON.stringify(h.marker("relay.compiling"))}) || !existsSync(${JSON.stringify(h.marker("jetstream.compiling"))})) await delay(10);
  abort.abort();
});
`,
  );
  const child = h.spawn(runner);
  await h.compiling();
  await waitFor(
    () =>
      existsSync(h.marker("relay.aborted")) &&
      existsSync(h.marker("jetstream.aborted")),
  );
  // Both compilers ignore SIGTERM and keep writing. Cleanup must retain their
  // directory through the five-second SIGKILL fallback, not just abort execFile.
  await delay(250);
  assert.equal(child.child.exitCode, null, child.output());
  for (const name of ["relay", "jetstream"]) {
    assert.ok(alive(Number(readFileSync(h.marker(name + ".pid"), "utf8"))));
    assert.ok(existsSync(readFileSync(h.marker(name + ".dir"), "utf8")));
  }
  assert.equal(await bounded(child.done, 12000), 1, child.output());
  assert.match(child.output(), /cancelled/);
  for (const name of ["relay", "jetstream"])
    assert.equal(existsSync(h.marker(name + ".directory-missing")), false);
  h.assertCompilersReaped();
  h.assertRemoved();
});

test("browser server listen failure reaps fixtures and removes their directory", async (t) => {
  const listener = createServer();
  t.after(
    () => new Promise<void>((resolve) => listener.close(() => resolve())),
  );
  try {
    await new Promise<void>((resolve, reject) => {
      listener.once("error", reject);
      listener.listen(3188, "127.0.0.1", resolve);
    });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EADDRINUSE") {
      t.skip(
        "127.0.0.1:3188 is already in use; cannot own the conflict listener",
      );
      return;
    }
    throw error;
  }
  const h = harness(t, "success");
  const child = h.spawn("tests/browser-server.ts");
  assert.equal(await bounded(child.done, 15000), 1, child.output());
  assert.match(child.output(), /EADDRINUSE/);
  h.assertCompilersReaped();
  for (const name of ["relay", "jetstream"])
    assert.equal(
      alive(Number(readFileSync(h.marker("fixture-" + name + ".pid"), "utf8"))),
      false,
    );
  h.assertRemoved();
});
