# Build and deployment source

`hypercerts-org/hypercerts-relay` contains Relay, Rainbow, and the nested Jetstream
module. Build all three from the same reviewed repository revision.

| Component | Go build directory and command | Image build from repository root | Runtime entry point |
|---|---|---|---|
| Relay | root: `go build ./cmd/relay` | `docker build -f cmd/relay/Dockerfile .` | `relay serve` |
| Rainbow | root: `go build ./cmd/rainbow` | `docker build -f cmd/rainbow/Dockerfile .` | `rainbow` |
| Jetstream | `jetstream/`: `go build ./cmd/jetstream` | `docker build -f jetstream/Dockerfile jetstream` | `jetstream serve` |

Jetstream consumes the owned Relay through `JETSTREAM_RELAY_URL` and stores its
archive and metadata under `JETSTREAM_DATA_DIR`. Each stateful component needs its
own persistent data directory. Rainbow is the optional raw-stream fan-out path;
consumers of retained collections connect to Jetstream's archive/live interfaces.

The deployment target is **Railway**. Public container and runtime
contracts are documented in [the Railway runbook](railway.md). Infrastructure as
Code and real project/environment configuration belong in a private infrastructure
repository. Railway uses the canonical
Dockerfiles and build contexts listed above. Administration uses
`docker build -f administration/Dockerfile .`; Jetstream retains its nested module
context.

The separate `administration/` application builds with `npm ci --ignore-scripts &&
npm run build` in that directory and runs with `npm run server`. It requires Node
24.18+, durable local SQLite storage, file-based runtime secrets, private service
connectivity and an HTTPS origin. See `administration/README.md`. Adding deployment
files does not provision or qualify a running deployment.
