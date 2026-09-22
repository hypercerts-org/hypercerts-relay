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

The separate `administration/` application builds with `npm ci --ignore-scripts &&
npm run build` in that directory and runs with `npm run server`. It requires Node
24.18+, durable local SQLite storage, file-based runtime secrets, private service
connectivity and an HTTPS origin. See `administration/README.md`. Adding deployment
files does not provision or qualify a running deployment.

## Immutable release candidates

Merging to `dev` runs the `Publish Railway release candidate` workflow. It builds
Relay, Jetstream, and Administration once, publishes a `sha-<commit>` GHCR tag for
each, and sends their immutable OCI digest references to a private infrastructure
repository. It does not contain Railway project bindings or deploy directly from
this public repository.

The staging GitHub Environment supplies `INFRA_REPOSITORY` and an
`INFRA_DISPATCH_TOKEN` that may create a repository-dispatch event only on that
private receiver. It also supplies `ADMIN_PUBLIC_ORIGIN` plus optional
`RELAY_PUBLIC_ORIGIN`, `RAINBOW_PUBLIC_ORIGIN`, and `JETSTREAM_PUBLIC_ORIGIN`.
The receiver records the three digest references, the administration public-origin
settings, and staging validation as a candidate receipt, then deploys those
references to staging. Empty optional service origins are omitted.

Merging a pull request from `dev` to `production` dispatches a promotion only when
the production merge tree equals the staged candidate tree. The private receiver
must find a successful candidate receipt and deploy its recorded digest references
to production. It must reject missing or mutable image references and must not
rebuild from Git. Production GitHub Environment protection controls the separate
production dispatch credential and supplies its own public-origin variables. The
three service origins are display-only administration settings; they must never be
private control/debug URLs, and a Rainbow URL does not authorize a Rainbow deploy.

Candidate images retain only immutable `sha-<commit>` tags. The publishing workflow
first rejects a mismatch between the root release version and the Relay/Rainbow,
Jetstream, Administration, and Docker build versions, then stamps that version,
commit, and build time into each published image's OCI metadata. A semantic
`vX.Y.Z` source tag is created separately by the Release workflow from `main` and
must match those component versions; it is not a mutable image alias and does not
authorize a rebuild or deployment.
