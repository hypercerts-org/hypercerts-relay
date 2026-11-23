// Disposable loopback owners for administration acceptance tests. They compile
// and launch the real private Relay and Jetstream control handlers; no browser
// test may replace those owner APIs with an in-process service double.
import { execFile, spawn, type ChildProcess } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { promisify } from "node:util";

const token = "fixture-service-credential-32-bytes-minimum";

interface FixtureProcess {
  child: ChildProcess;
  ready: string;
  exited: Promise<void>;
}

async function stopFixture(
  { child, ready, exited }: FixtureProcess,
  graceful: boolean,
) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  if (graceful) writeFileSync(`${ready}.stop`, "", { mode: 0o600 });
  else child.kill("SIGTERM");
  const timeout = setTimeout(() => child.kill("SIGKILL"), 5000);
  try {
    await exited;
  } finally {
    clearTimeout(timeout);
  }
}

export interface ControlFixtures {
  relayURL: string;
  jetstreamURL: string;
  token: string;
  close(): Promise<void>;
}

export async function startControlFixtures(
  signal?: AbortSignal,
): Promise<ControlFixtures> {
  signal?.throwIfAborted();
  const go = process.env.CONTROL_GO_BINARY ?? "go";
  if (!isAbsolute(go))
    throw new Error(
      "CONTROL_GO_BINARY must identify an absolute Go executable path",
    );
  const dir = mkdtempSync(join(tmpdir(), "admin-control-fixture-"));
  // Keep Go's build work and each fixture's t.TempDir inside dir, so removing
  // dir also cleans up after a fixture that was killed instead of stopped.
  mkdirSync(join(dir, "tmp"));
  const env = {
    ...process.env,
    GOCACHE: join(dir, "go-cache"),
    TMPDIR: join(dir, "tmp"),
  };
  const compile = promisify(execFile);
  const specs = [
    { name: "relay", cwd: resolve(".."), pkg: "./cmd/relay" },
    {
      name: "jetstream",
      cwd: resolve("../jetstream"),
      pkg: "./internal/hypercerts/control",
    },
  ];
  const launched: FixtureProcess[] = [];
  try {
    const compiled = await Promise.allSettled(
      specs.map(async ({ name, cwd, pkg }) => {
        const pending = compile(
          go,
          ["test", "-c", "-o", join(dir, `${name}.test`), pkg],
          {
            cwd,
            env,
            signal,
            timeout: 300_000,
            maxBuffer: 2 * 1024 * 1024,
          },
        );
        // An abort rejects execFile before its child closes. Reap it before
        // allowing cleanup to remove the directory it can still write into.
        const closed = new Promise<void>((resolve) =>
          pending.child.once("close", () => resolve()),
        );
        // execFile's timeout only sends SIGTERM and then waits for close, so a
        // compiler ignoring it would hold pending open. Escalate at the
        // deadline whether or not pending has settled.
        const deadline = setTimeout(
          () => pending.child.kill("SIGKILL"),
          305_000,
        );
        void closed.then(() => clearTimeout(deadline));
        try {
          await pending;
        } finally {
          const timeout = setTimeout(() => pending.child.kill("SIGKILL"), 5000);
          try {
            await closed;
          } finally {
            clearTimeout(timeout);
          }
        }
      }),
    );
    const failed = compiled.find((result) => result.status === "rejected");
    if (failed) throw failed.reason;
    signal?.throwIfAborted();
    async function startFixture(
      { name, cwd }: (typeof specs)[number],
      relayURL?: string,
    ) {
      signal?.throwIfAborted();
      const ready = join(dir, name);
      const child = spawn(
        join(dir, `${name}.test`),
        ["-test.run=^TestControlPlaneAcceptanceFixture$", "-test.count=1"],
        {
          cwd,
          env: {
            ...env,
            CONTROL_ACCEPTANCE_READY: ready,
            ...(relayURL === undefined
              ? {}
              : { CONTROL_ACCEPTANCE_RELAY_URL: relayURL }),
          },
        },
      );
      // Capture completion at spawn time, including a failed spawn, so cleanup
      // never waits for an exit event that has already happened.
      const exited = new Promise<void>((resolve) =>
        child.once("close", () => resolve()),
      );
      const fixture = { child, ready, exited } satisfies FixtureProcess;
      launched.push(fixture);
      let output = "";
      child.stdout?.on("data", (data) => (output += data));
      child.stderr?.on("data", (data) => (output += data));
      child.on("error", (error) => (output += error.message));
      const deadline = Date.now() + 15_000;
      while (!existsSync(ready)) {
        signal?.throwIfAborted();
        if (
          child.exitCode !== null ||
          child.signalCode !== null ||
          child.pid === undefined ||
          Date.now() >= deadline
        ) {
          child.kill("SIGKILL");
          throw new Error(output || `${name} control fixture did not start`);
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      return fixture;
    }

    const relay = await startFixture(specs[0]);
    const jetstream = await startFixture(
      specs[1],
      readFileSync(relay.ready, "utf8"),
    );
    const relayURL = readFileSync(relay.ready, "utf8");
    const jetstreamURL = readFileSync(jetstream.ready, "utf8");
    const configured = await fetch(`${relayURL}/_test/configure-handoff`, {
      signal,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ targetURL: jetstreamURL, token }),
    });
    if (!configured.ok)
      throw new Error(
        `Relay handoff fixture configuration failed: ${configured.status}`,
      );
    return {
      relayURL,
      jetstreamURL,
      token,
      async close() {
        await Promise.all(
          launched.map((fixture) => stopFixture(fixture, true)),
        );
        rmSync(dir, { recursive: true, force: true });
      },
    };
  } catch (error) {
    // All compilers have settled. Stop and reap every launched fixture before
    // removing their common ready-file directory.
    await Promise.all(launched.map((fixture) => stopFixture(fixture, false)));
    rmSync(dir, { recursive: true, force: true });
    throw error;
  }
}
