// Disposable loopback owners for administration acceptance tests. They compile
// and launch the real private Relay and Jetstream control handlers; no browser
// test may replace those owner APIs with an in-process service double.
import { execFile, spawn, type ChildProcess } from "node:child_process";
import {
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { once } from "node:events";
import { tmpdir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";
import { promisify } from "node:util";

const token = "fixture-service-credential-32-bytes-minimum";

interface FixtureProcess {
  child: ChildProcess;
  ready: string;
}

async function stopFixture(
  { child, ready }: FixtureProcess,
  graceful: boolean,
) {
  if (child.exitCode !== null) return;
  if (graceful) writeFileSync(`${ready}.stop`, "", { mode: 0o600 });
  else child.kill("SIGTERM");
  const timeout = setTimeout(() => child.kill("SIGKILL"), 5000);
  try {
    await once(child, "exit");
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

export async function startControlFixtures(): Promise<ControlFixtures> {
  const go = process.env.CONTROL_GO_BINARY ?? "go";
  if (!isAbsolute(go))
    throw new Error(
      "CONTROL_GO_BINARY must identify an absolute Go executable path",
    );
  const dir = mkdtempSync(join(tmpdir(), "admin-control-fixture-"));
  const env = { ...process.env, GOCACHE: join(dir, "go-cache") };
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
    await Promise.all(
      specs.map(({ name, cwd, pkg }) =>
        compile(go, ["test", "-c", "-o", join(dir, `${name}.test`), pkg], {
          cwd,
          env,
          timeout: 300_000,
          maxBuffer: 2 * 1024 * 1024,
        }),
      ),
    );
    const readyFixtures = await Promise.all(
      specs.map(async ({ name, cwd }) => {
        const ready = join(dir, name);
        const child = spawn(
          join(dir, `${name}.test`),
          ["-test.run=^TestControlPlaneAcceptanceFixture$", "-test.count=1"],
          { cwd, env: { ...env, CONTROL_ACCEPTANCE_READY: ready } },
        );
        const fixture = { child, ready } satisfies FixtureProcess;
        launched.push(fixture);
        let output = "";
        child.stdout?.on("data", (data) => (output += data));
        child.stderr?.on("data", (data) => (output += data));
        const deadline = Date.now() + 15_000;
        while (!existsSync(ready)) {
          if (child.exitCode !== null || Date.now() >= deadline) {
            child.kill("SIGKILL");
            throw new Error(output || `${name} control fixture did not start`);
          }
          await new Promise((resolve) => setTimeout(resolve, 50));
        }
        return fixture;
      }),
    );
    const [relay, jetstream] = readyFixtures;
    return {
      relayURL: readFileSync(relay.ready, "utf8"),
      jetstreamURL: readFileSync(jetstream.ready, "utf8"),
      token,
      async close() {
        await Promise.all(
          launched.map((fixture) => stopFixture(fixture, true)),
        );
        rmSync(dir, { recursive: true, force: true });
      },
    };
  } catch (error) {
    // Promise.all may reject while the sibling fixture is still starting. Stop
    // and reap every child before removing their common ready-file directory.
    await Promise.all(launched.map((fixture) => stopFixture(fixture, false)));
    rmSync(dir, { recursive: true, force: true });
    throw error;
  }
}
