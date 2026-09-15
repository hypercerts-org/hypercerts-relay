---
name: hypercerts-railway
description: Prepare, review, or troubleshoot the public container and runtime contracts for Hypercerts Relay, Rainbow, Jetstream and administration on Railway. Use for canonical Dockerfiles, runtime secret handling and deployment documentation; infrastructure definitions belong in a private repository.
---

# Hypercerts containers on Railway

Read [AGENTS.md](../../../AGENTS.md) and the
[public runtime runbook](../../../docs/railway.md).

## Public repository boundary

This repository is open source. Keep Railway Infrastructure as Code, project and
environment bindings, service graphs, private domains, volume provisioning and
credentials in a private infrastructure repository. Do not add `.railway/` or
`railway.json`, `railway.toml`, `railway.ts`, `railway.py` or `railway.go` here.
Do not pull live platform configuration into this checkout. Generic Dockerfiles,
public runtime-variable contracts and credential-free verification belong here.

Reuse `cmd/relay/Dockerfile`, `cmd/rainbow/Dockerfile`, `jetstream/Dockerfile` and
`administration/Dockerfile`; do not create Railway-only copies. Jetstream uses
its nested module build context; the other images use the repository root.

The shared static `jetstream/cmd/container-entrypoint` bootstrap is enabled only by
`HC_RAILWAY_STARTUP=1`. It materializes named runtime credentials into restricted
temporary files, clears those raw variables and executes the daemon. Ordinary
startup preserves file-secret paths and command overrides. Never pass credentials
as Docker build arguments or bake administrator access into an image.

Preserve separate component ownership and one writer per local store. Rainbow also
persists its replay buffer and cursor. Keep management/debug ports private; public
health probes do not prove ingestion success or historical completeness. Actual
networking, storage ownership, volume capacity and deployment choices stay private.

For bootstrap changes, run `go test ./cmd/container-entrypoint` and
`go vet ./cmd/container-entrypoint` from `jetstream/`, plus affected runtime checks.
Build canonical images when Docker is accessible; otherwise report the limitation
and use the build-only CI evidence. Check that no forbidden infrastructure files
are tracked. Repository checks need no Railway account or platform access.

## Platform operations

Only perform platform work when requested, using the private infrastructure
repository and the exact authorized project/environment/service. Repository edits
do not authorize login, linking, provisioning or deployment. Do not install global
tooling or reconfigure MCP as a side effect. Never change running admission,
policies, administrator access or persisted state outside the user's stated scope.
