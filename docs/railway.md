# Railway container support

This **public repository** contains container builds and runtime contracts only.
Keep Railway Infrastructure as Code, project/environment bindings, service graphs,
private domains, volume provisioning and credentials in a **private infrastructure
repository**. Do not commit `.railway/` or Railway configuration files here.

## Canonical Dockerfiles

Railway deployments should reference these existing Dockerfiles. Do not maintain
provider-specific copies.

| Component | Dockerfile | Build context / Root Directory | API port | Private management port |
| --- | --- | --- | --- | --- |
| Relay | `cmd/relay/Dockerfile` | repository root `/` | 2470 | 2472 when configured |
| Rainbow | `cmd/rainbow/Dockerfile` | repository root `/` | 2480 | metrics 2481 |
| Jetstream | `jetstream/Dockerfile` | nested module `/jetstream` | 8080 | 6060 when configured |
| Administration | `administration/Dockerfile` | repository root `/` | 3000 | authenticated `/api/v1` on the same port |

The initial launch service set is Relay, Jetstream, and Administration.
Rainbow remains buildable from its canonical Dockerfile, but is deferred from
that service set while [TECH-635](https://linear.app/hypercerts/issue/TECH-635/validate-rainbow-raw-stream-recovery-and-retention)
is backlog work. Do not provision, expose, or make a raw-consumer recovery claim
for Rainbow until its activation evidence is complete.

For Jetstream, the Dockerfile path relative to its service Root Directory is
`Dockerfile`. The other paths are relative to the repository root. Jetstream retains
its distroless runtime and module-local build context.

```sh
docker build -f cmd/relay/Dockerfile -t hypercerts-relay .
docker build -f cmd/rainbow/Dockerfile -t hypercerts-rainbow .
docker build -f jetstream/Dockerfile -t hypercerts-jetstream jetstream
docker build -f administration/Dockerfile -t hypercerts-administration .
```

The `Verify container packaging` workflow builds these images without publishing
or accessing Railway. It checks ordinary startup and the optional Railway bootstrap
using help/import commands and fixture credentials, without starting services.
It also rejects tracked Railway configuration files in this public repository.

## Runtime storage and networking contracts

Each selected running component has local state that needs its own persistent
storage. Relay stores raw replay events; Jetstream stores its archive and Pebble
metadata; administration stores its SQLite journal and OAuth state. Run one
writer per local store. Rainbow's replay buffer and cursor are not part of the
initial launch because Rainbow is not a launch service. Configure actual volume
identities, placement, capacity and backups privately.

Relay supports PostgreSQL or SQLite for metadata. Its documentation recommends
PostgreSQL for nontrivial deployments; PostgreSQL does not replace the raw-event
volume. Jetstream does not use PostgreSQL. See [Relay configuration](../cmd/relay/README.md),
[Jetstream configuration](../jetstream/README.md), and
[administration configuration](../administration/README.md).

The optional bootstrap expects a Railway volume mounted at `/data`. Railway volumes
are root-owned; configure runtime ownership privately and verify write access as
the actual container UID. Jetstream and administration default to non-root users;
Relay and Rainbow retain their existing defaults. Do not silently change ownership
or move existing volumes during an image update.

Keep service management/debug and metrics ports private. Jetstream's public
`/status` route is suitable for an availability probe on 8080; do not expose its
private 6060 listener to make a platform probe pass. Relay and Rainbow expose
`/xrpc/_health`; administration exposes `/health`. These responses establish
availability, not ingestion success or complete historical coverage.

Private networking requires compatible listener bindings; the Go services accept
`[::]:PORT`, and administration accepts `ADMIN_BIND=::`. Configure real hostnames,
ports and public domains in the private infrastructure repository. The administration
HTTPS origin must match `ADMIN_PUBLIC_ORIGIN` and serve OAuth metadata and callbacks.

## Optional runtime-secret bootstrap

All images share the static `jetstream/cmd/container-entrypoint` helper. Its source
lives in the nested module so that Jetstream's isolated Docker context can compile
it; other images compile the same file.

By default, the helper executes the ordinary service command, preserving existing
file-secret paths, PATH-based command overrides, arguments, process identity and
exit status. It does not require Railway configuration in that mode.

Setting `HC_RAILWAY_STARTUP=1` enables the following runtime contract:

- Require `RAILWAY_VOLUME_MOUNT_PATH=/data`.
- Relay consumes `HC_RELAY_CONTROL_SECRET` and sets `RELAY_CONTROL_TOKEN_FILE`.
- Jetstream consumes `HC_JETSTREAM_CONTROL_SECRET` and sets `JETSTREAM_CONTROL_TOKEN_FILE`.
- Jetstream also consumes `HC_RELAY_CONTROL_SECRET` and sets
  `JETSTREAM_RELAY_CONTROL_TOKEN_FILE`, because it must hold the Relay's
  credential to reach the Relay's private control listener. Also set
  `JETSTREAM_RELAY_CONTROL_URL` to that listener (`http://relay.railway.internal:2472`
  on Railway). Bootstrap always provides the token file, so a URL pointing at
  an unreachable or unauthenticated Relay makes collection changes fail. Such a
  change is rejected with `policy_mirror_unavailable` (HTTP 503) only when an
  enabled source with a known revision requires the mirror; with no such
  source, the change commits without consulting it.
- Administration consumes both service credentials and `HC_ADMIN_ENCRYPTION_SECRET`,
  setting the two service file paths and `ADMIN_ENCRYPTION_KEY_FILE`.
- Rainbow needs no credential conversion.

### Required variables per environment

Set these on each environment before deploying. Every URL below is a Railway
private hostname; a public domain reaches the API port instead of the private
listener and returns 404, which Jetstream reports as `policy_mirror_unavailable`.

| Service | Variable | Value |
| --- | --- | --- |
| Relay | `HC_RAILWAY_STARTUP` | `1` |
| Relay | `RAILWAY_VOLUME_MOUNT_PATH` | `/data` |
| Relay | `RELAY_CONTROL_ADDR` | `[::]:2472` |
| Relay | `HC_RELAY_CONTROL_SECRET` | At least 32 bytes |
| Jetstream | `HC_RAILWAY_STARTUP` | `1` |
| Jetstream | `RAILWAY_VOLUME_MOUNT_PATH` | `/data` |
| Jetstream | `HC_JETSTREAM_CONTROL_SECRET` | At least 32 bytes |
| Jetstream | `HC_RELAY_CONTROL_SECRET` | Same value as the Relay service |
| Jetstream | `JETSTREAM_RELAY_CONTROL_URL` | `http://relay.railway.internal:2472` |
| Administration | `HC_RAILWAY_STARTUP` | `1` |
| Administration | `RAILWAY_VOLUME_MOUNT_PATH` | `/data` |
| Administration | `HC_RELAY_CONTROL_SECRET` | Same value as the Relay service |
| Administration | `HC_JETSTREAM_CONTROL_SECRET` | Same value as the Jetstream service |
| Administration | `HC_ADMIN_ENCRYPTION_SECRET` | At least 32 bytes |
| Administration | `RELAY_CONTROL_URL` | `http://relay.railway.internal:2472` |
| Administration | `JETSTREAM_CONTROL_URL` | `http://jetstream.railway.internal:6060` |

`HC_RELAY_CONTROL_SECRET` must be identical on Relay, Jetstream and
Administration. Bootstrap writes each service its own token file, so only the
input secret is shared. `JETSTREAM_RELAY_CONTROL_URL` and the two administration
control URLs are ordinary variables, not secrets, and survive bootstrap.

A missing `JETSTREAM_RELAY_CONTROL_URL` leaves Jetstream without a policy mirror,
so any collection change that has an enabled source with a known revision is
rejected. Verify an environment by changing a collection through Administration
after deploying.

Credential values must contain at least 32 bytes after trimming whitespace. The
helper creates a mode-0700 temporary directory and mode-0600 files, removes the
three raw credential variables from the child environment, then executes the
service. Keep values in the platform's secret store, never in source, Docker build
arguments, image layers, command history or CI logs. Keep the OAuth encryption key
stable with its database; coordinate service-token rotation with client restarts.
The inherited Relay administrative password is a separate runtime setting.

For initial access, set `ADMIN_SEED_DID` on the administration service at runtime.
Startup grants that DID once on an uninitialized database. The seed is an ordinary
administrator: another admin can remove it through the Administrators screen,
and restarting or changing the seed variable cannot restore revoked access.
Keep the initialization marker with the durable administration database.

New Jetstream data directories seed the bundled Hypercerts/Certified record
collections automatically. Set `JETSTREAM_DISABLE_COLLECTION_SEED=true` on
Jetstream to opt out, or `JETSTREAM_COLLECTIONS` to supply an explicit initial
list. Saved collection policy always wins, including an empty policy. Use the
administration Collections screen to update an existing deployment; changing
startup variables cannot overwrite its policy. The seed is bundled into the
canonical image and requires no build-time credentials or external checkout.

Use the access CLI only for recovery, against the running administration
container's mounted database: `/app/node_modules/.bin/tsx server/access.ts grant DID`.
Do not bake an administrator into an image or grant access at build/pre-deploy time.
`railway run` executes locally with remote variables, not against a remote volume.
See the administration README for the full access and revocation contract.

## Repository verification

For bootstrap changes, run from `jetstream/`:

```sh
go test ./cmd/container-entrypoint
go vet ./cmd/container-entrypoint
```

Run the affected runtime verification scripts and build changed canonical images.
No Railway login, project linking, configuration planning, apply or deployment is
needed for these checks. If local Docker access is unavailable, report the limitation
and use the build-only CI evidence.

Only perform platform operations when separately requested, with an identified
project and environment and the private infrastructure repository available.
Image builds do not qualify a live deployment: runtime OAuth, private authorization,
volume persistence and cursor recovery still need deployment-specific validation.

## References

- [Railway Dockerfile builds](https://docs.railway.com/builds/dockerfiles)
- [Volume availability and ownership](https://docs.railway.com/volumes)
- [Private networking](https://docs.railway.com/networking/private-networking/how-it-works)

## Management transport

Control URLs must be HTTP(S) origins without embedded credentials, paths, queries
or fragments. Use HTTPS when traffic crosses an untrusted network. HTTP is supported
on loopback and, with `HC_RAILWAY_STARTUP=1`, single-service
`*.railway.internal` origins. Other HTTP origins fail administration startup.
Railway carries private
service traffic through WireGuard tunnels. Bind management listeners only within
that trust boundary and never attach public domains or public TCP proxies to them.
A bearer token authenticates callers but does not itself encrypt HTTP traffic.
See [Railway private networking](https://docs.railway.com/networking/private-networking).

The administration bootstrap mode also enables Railway public-edge client-IP
handling for login limits; see [proxy configuration](../administration/README.md#proxy-and-storage-boundaries).
Raw `HC_*_SECRET` inputs are removed before process replacement in both ordinary
container mode and Railway mode. Existing `*_FILE` configuration is preserved.
