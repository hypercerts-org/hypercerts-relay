import assert from "node:assert/strict";
import { spawn, type ChildProcess } from "node:child_process";
import {
  chmodSync,
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

// Spawned harness children load tsx and the whole server before they compile.
// When other test files compile Go concurrently, that start alone can exceed
// the in-process lifecycle bound, so their first marker gets a startup bound.
const childStartup = 30000;

async function waitFor(predicate: () => boolean, milliseconds = 3000) {
  const deadline = Date.now() + milliseconds;
  while (!predicate()) {
    assert.ok(
      Date.now() < deadline,
      `lifecycle event arrived within ${milliseconds} ms`,
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

// Markers are published by rename, but a reader still rejects anything that
// is not a positive PID: process.kill(0) would signal this whole process group.
function parsePID(text: string) {
  const pid = Number(text);
  return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
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
const { existsSync, renameSync, writeFileSync } = require("node:fs");
const publish = (file, text) => { writeFileSync(file + ".tmp", text); renameSync(file + ".tmp", file); };
const ready = process.env.CONTROL_ACCEPTANCE_READY;
publish(${JSON.stringify(root)} + "/" + ${JSON.stringify("fixture-")} + ready.split("/").at(-1) + ".pid", String(process.pid));
if (${JSON.stringify(mode)} !== "block-startup") {
  const server = createServer((request, response) => { response.writeHead(204); response.end(); });
  server.listen(0, "127.0.0.1", () => publish(ready, "http://127.0.0.1:" + server.address().port));
  setInterval(() => {
    if (existsSync(ready + ".stop")) server.close(() => process.exit(0));
  }, 10);
} else setInterval(() => {}, 1000);
`;
  writeFileSync(
    go,
    `#!${process.execPath}
import { chmodSync, existsSync, mkdirSync, renameSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
const root = ${JSON.stringify(root)};
const mode = ${JSON.stringify(mode)};
const output = process.argv[process.argv.indexOf("-o") + 1];
const name = basename(output, ".test");
// Publish markers by rename so the harness never reads a partial one.
const publish = (file, text) => { writeFileSync(file + ".tmp", text); renameSync(file + ".tmp", file); };
publish(join(root, name + ".dir"), dirname(output));
publish(join(root, name + ".pid"), String(process.pid));
// Publish the marker only once this double can handle SIGTERM, so a test that
// aborts on it never signals a double that would exit without its markers.
const compiling = () => writeFileSync(join(root, name + ".compiling"), "");
if (mode === "locked-cleanup") {
  // On cancellation, leave an entry the fixture cleanup cannot remove.
  process.on("SIGTERM", () => {
    const locked = join(dirname(output), name + "-locked");
    mkdirSync(locked, { recursive: true });
    writeFileSync(join(locked, "entry"), "");
    chmodSync(locked, 0o500);
    publish(join(root, name + ".locked"), locked);
    process.exit(0);
  });
  setInterval(() => {}, 1000);
  compiling();
} else if (mode === "block-compile") {
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
  const markers = (suffix: string) =>
    readdirSync(root)
      .filter((name) => name.endsWith(suffix))
      .map((name) => readFileSync(marker(name), "utf8"));
  // Only a fixture or management temporary directory is ever removed.
  const owned = [
    join(tmpdir(), "admin-control-fixture-"),
    join(tmpdir(), "management-acceptance-"),
  ];
  const dirs = () =>
    markers(".dir").filter((dir) =>
      owned.some((prefix) => dir.startsWith(prefix)),
    );
  const pid = (name: string) => {
    const value = parsePID(readFileSync(marker(name + ".pid"), "utf8"));
    assert.ok(value, `${name} published a valid PID`);
    return value;
  };
  t.after(async () => {
    for (const { child } of children) child.kill("SIGKILL");
    await Promise.all(children.map(({ done }) => done));
    const pids = markers(".pid")
      .map(parsePID)
      .filter((pid) => pid !== undefined);
    for (const pid of pids) if (alive(pid)) process.kill(pid, "SIGKILL");
    await waitFor(() => pids.every((pid) => !alive(pid)));
    for (const locked of markers(".locked"))
      if (existsSync(locked)) chmodSync(locked, 0o700);
    for (const dir of dirs()) rmSync(dir, { recursive: true, force: true });
    rmSync(root, { recursive: true, force: true });
    if (previousGo === undefined) delete process.env.CONTROL_GO_BINARY;
    else process.env.CONTROL_GO_BINARY = previousGo;
  });
  return {
    marker,
    pid,
    // A spawned child gets the startup bound, and fails at once with its
    // output if it exits before both compilers have started.
    async compiling(child?: { child: ChildProcess; output(): string }) {
      await waitFor(
        () => {
          const { exitCode, signalCode } = child?.child ?? {};
          if (exitCode != null || signalCode != null)
            assert.fail(
              `child exited (${exitCode ?? signalCode}) before compiling:\n${child!.output()}`,
            );
          return (
            existsSync(marker("relay.compiling")) &&
            existsSync(marker("jetstream.compiling"))
          );
        },
        child ? childStartup : undefined,
      );
    },
    assertRemoved() {
      assert.ok(dirs().length > 0);
      for (const dir of dirs()) assert.equal(existsSync(dir), false, dir);
    },
    assertCompilersReaped() {
      for (const name of ["relay", "jetstream"]) {
        assert.equal(alive(pid(name)), false);
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
  const pid = h.pid("fixture-relay");
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
  const pid = h.pid("fixture-relay");
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
  assert.equal(await bounded(child.done, childStartup), 1, child.output());
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
  await h.compiling(child);
  child.child.kill("SIGTERM");
  assert.equal(await bounded(child.done), 0, child.output());
  h.assertCompilersReaped();
  h.assertRemoved();
});

test("browser server SIGTERM exits nonzero when cancelled cleanup fails", async (t) => {
  if (process.getuid?.() === 0) {
    t.skip("root ignores the directory permission this test relies on");
    return;
  }
  const h = harness(t, "locked-cleanup");
  const child = h.spawn("tests/browser-server.ts");
  await h.compiling(child);
  child.child.kill("SIGTERM");
  assert.equal(await bounded(child.done), 1, child.output());
  assert.match(child.output(), /EACCES/);
  h.assertCompilersReaped();
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
  await h.compiling(child);
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
    assert.ok(alive(h.pid(name)));
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
  assert.equal(await bounded(child.done, childStartup), 1, child.output());
  assert.match(child.output(), /EADDRINUSE/);
  h.assertCompilersReaped();
  for (const name of ["relay", "jetstream"])
    assert.equal(alive(h.pid("fixture-" + name)), false);
  h.assertRemoved();
});
