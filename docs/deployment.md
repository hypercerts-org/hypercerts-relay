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

## Candidate images and branch deployments

Merging to `dev` runs the `Publish Railway candidate images` workflow. It builds
Relay, Jetstream, and Administration once, publishes a `sha-<commit>` candidate
tag for each. A candidate tag that already exists stops the workflow: retries must
not rebuild or replace a candidate. Until the private infrastructure receiver is
published and reviewed, this workflow does not dispatch a staging candidate or a
production promotion and does not require `INFRA_REPOSITORY` or
`INFRA_DISPATCH_TOKEN`.

Railway remains the active deployment mechanism during this transition. Its staging
services deploy from `dev`; its production services deploy from `production`. A
merge to either branch therefore deploys that branch through the Railway service
configuration.

When the private receiver is ready, it must deploy staging using the three recorded
digest references, require a successful staging receipt, and promote only those
same digests to production. The administration public-origin settings are
display-only and must never be private control/debug URLs.

Candidate images retain only immutable `sha-<commit>` tags. The publishing workflow
first rejects a mismatch between the root release version and the Relay/Rainbow,
Jetstream, Administration, and Docker build versions, then stamps that version,
commit, and build time into each published image's OCI metadata. A semantic
`vX.Y.Z` source tag is created separately by the Release workflow from `main` and
must match those component versions; it is not a mutable image alias and does not
authorize a rebuild or deployment.
