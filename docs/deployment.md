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

The intended deployment target is Vercel, with this repository as the deployment
source. The entry points above are Go daemons, not Vercel request handlers. Platform
qualification must establish persistent storage, process lifetime, networking, and
restart behavior before deployment. TECH-593 documents source/build ownership;
it does not provision infrastructure or claim a qualified Vercel deployment.

The separate `administration/` application added by TECH-588 builds with
`npm ci && npm run build` in that directory and runs with `npm run server`.
It requires Node 24.18+, durable local SQLite storage, mounted secrets, private
service connectivity and an HTTPS origin. It does not extend the inherited Relay
web UI or establish a qualified Vercel deployment. See `administration/README.md`.
