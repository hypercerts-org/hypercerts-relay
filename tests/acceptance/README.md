# Plan 001 acceptance fixtures

`./tests/acceptance/run T01` owns the disposable two-PDS Relay → Jetstream
archive/live test. It rejects every other selector, including `T15-core`, until
that case is implemented by its owning plan.

## Topology

`../../docker-compose.acceptance.yml` starts only local Docker services:

```text
PDS A + PDS B → Caddy internal TLS → acceptance-tag Relay → Jetstream v2 → public client
                         ↑
                  local @did-plc/server
```

The PDS containers are official upstream images. Caddy creates an ephemeral local
CA and serves `https://plc.test`, `https://pds-a.test`, and
`https://pds-b.test` only on the Docker-internal network. The runner builds a
separate Relay image with Go build tag `acceptance` and
`RELAY_ACCEPTANCE_PRIVATE_HOSTS=true`; both are required before it allows
Docker-private PDS addresses. The normal Relay binary and `cmd/relay/Dockerfile`
retain their public-address SSRF transport.

The local PLC process uses upstream `@did-plc/server` with its mock test database.
It is intentionally limited to the disposable run: it is not a production PLC,
not durable outside the run, and not exposed outside Docker.

The runner admits two PDS origins through Relay's private source interface and,
for each PDS, creates an account plus one selected `app.bsky.feed.post` record
through create, update, and delete. Account creation supplies the `#identity` and
`#account` lifecycle observations. It checks Relay's per-source durable cursors
after controlled A-only and B-only phases, so activity from one source cannot
satisfy the other source's progress condition.

The acceptance-only Jetstream build explicitly rotates its writer until the
seed archive is observable, then the runner captures the public `planSnapshot` sealed tip and
proves every seed lifecycle/mutation event via a bounded archive-only client. A
separate public archive-to-live client must first
replay that exact seed set before live mutations are introduced. Structured JSON
assertions then require globally increasing Jetstream cursors; correct nested
identity/account payloads; create < update < delete ordering; one matching event
per DID/operation/rkey; updated record contents; delete records without a
record/CID; no duplicate expected operation; and live cursors strictly beyond
the sealed archive boundary.

## Run

Requirements: Docker/Compose, Go 1.26.6 or later, `openssl`, `curl`, `python3`, and an
internet connection for the pinned images and the PLC test dependency on first
build. The runner generates all test credentials into an ignored per-run
directory and removes containers and named volumes on exit.

```sh
./tests/acceptance/run T01
```

The command prints its results directory. It retains bounded client/Compose logs,
source revisions, sanitized driver output, archive snapshot, and consumer output
there for verification evidence. The generated credential file is removed during
cleanup; Docker containers and named volumes are also removed. A nonzero result
is an observed failure, not a skipped case.

No Cloudflare, Railway, public DNS, external PLC, or running deployment is used.
Rainbow is intentionally absent from T01; its persistence/recovery validation is
Plan 009 work.
