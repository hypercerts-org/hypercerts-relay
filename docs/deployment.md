# Build and deployment source

`hypercerts-org/hypercerts-relay` contains Relay, Rainbow, and the nested Jetstream
module. Buildable components use the same reviewed repository revision.

| Component | Go build directory and command | Image build from repository root | Runtime entry point |
|---|---|---|---|
| Relay | root: `go build ./cmd/relay` | `docker build -f cmd/relay/Dockerfile .` | `relay serve` |
| Rainbow | root: `go build ./cmd/rainbow` | `docker build -f cmd/rainbow/Dockerfile .` | `rainbow` |
| Jetstream | `jetstream/`: `go build ./cmd/jetstream` | `docker build -f jetstream/Dockerfile jetstream` | `jetstream serve` |

The initial Railway launch service set is Relay, Jetstream, and Administration.
Rainbow remains buildable but is excluded from launch pending the deferred
[TECH-635](https://linear.app/hypercerts/issue/TECH-635/validate-rainbow-raw-stream-recovery-and-retention)
raw-stream recovery evidence. It must not be exposed or described as a launch
consumer path before that work is complete.

Jetstream consumes the owned Relay through `JETSTREAM_RELAY_URL` and stores its
archive and metadata under `JETSTREAM_DATA_DIR`. Each launch stateful component
needs its own persistent data directory. Consumers of retained collections connect
to Jetstream's archive/live interfaces; Rainbow is not in that launch path.

The deployment target is **Railway**. Public container and runtime
contracts are documented in [the Railway runbook](railway.md). Infrastructure as
Code and real project/environment configuration belong in a private infrastructure
repository. Railway uses the canonical
Dockerfiles and build contexts listed above. Administration uses
`docker build -f administration/Dockerfile .`; Jetstream retains its nested module
context.

Before a candidate is deployed, use the [forward upgrade and restore
runbook](upgrade-restore.md). It qualifies copied stopped state locally; it does
not provision Railway, make a production snapshot claim, or authorize binary
rollback after a state migration.

The separate `administration/` application builds with `npm ci --ignore-scripts &&
npm run build` in that directory and runs with `npm run server`. It requires Node
24.18+, durable local SQLite storage, file-based runtime secrets, private service
connectivity and an HTTPS origin. See `administration/README.md`. Adding deployment
files does not provision or qualify a running deployment.
