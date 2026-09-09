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

Each component has local state that needs its own persistent storage. Relay stores
raw replay events; Rainbow stores its replay buffer and cursor; Jetstream stores
its archive and Pebble metadata; administration stores its SQLite journal and OAuth
state. Run one writer per local store. Configure actual volume identities, placement,
capacity and backups privately.

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
- Administration consumes both service credentials and `HC_ADMIN_ENCRYPTION_SECRET`,
  setting the two service file paths and `ADMIN_ENCRYPTION_KEY_FILE`.
- Rainbow needs no credential conversion.

Credential values must contain at least 32 bytes after trimming whitespace. The
helper creates a mode-0700 temporary directory and mode-0600 files, removes the
three raw credential variables from the child environment, then executes the
service. Keep values in the platform's secret store, never in source, Docker build
arguments, image layers, command history or CI logs. Keep the OAuth encryption key
stable with its database; coordinate service-token rotation with client restarts.
The inherited Relay administrative password is a separate runtime setting.

Administrator grants must run against the running administration container's
mounted database, using `/app/node_modules/.bin/tsx server/access.ts grant DID`.
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
- [Project-local Railway skill](../.agents/skills/hypercerts-railway/SKILL.md)
