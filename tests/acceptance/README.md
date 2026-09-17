# Acceptance fixtures

`./tests/acceptance/run T01` owns the disposable two-PDS Relay → Jetstream
archive/live test for [TECH-594](https://linear.app/hypercerts/issue/TECH-594/connect-jetstream-v2-to-the-hypercerts-indigo-relay).
`./tests/acceptance/run T15-core` owns the direct-PDS verification/restart matrix
for [TECH-634](https://linear.app/hypercerts/issue/TECH-634/unify-jetstream-acquisition-verification-and-durability-fault-coverage).
Unknown selectors fail with exit 64.

## Topology

`../../docker-compose.acceptance.yml` starts only local Docker services:

```text
PDS A + PDS B → Caddy internal TLS → acceptance-tag Relay → Jetstream v2 → public client
                     ↓                    ↑                 ↑
            default-pass-through          │          private Docker-only control
                 fault proxy               │
                     ↓                     │
              local @did-plc/server        │
                                            └── T15 direct-PDS source/jobs
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
not durable outside the run, and not exposed outside Docker. Caddy sends the PLC
and PDS fixture origins through a Docker-internal Node proxy that passes requests
through unless T15 configures a target. The proxy can return a 5xx for one DID's
PLC resolution, return a 5xx for one PDS `listRepos`, or substitute a target DID
signing key in its PLC document. Its Docker-internal status endpoint returns
only bounded per-route hit counters and the last matched route; it does not log
or return request/response payloads.

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
./tests/acceptance/run T15-core
```

T15 creates a generated Jetstream control token only in the disposable acceptance
environment. It is mounted as a file into Jetstream and the in-network driver;
its private listener has no host port and the token is removed with the generated
environment file. It does not enable the control listener in normal builds or
runtime configuration.

Before every T15 private control request, including requests in retry phases after
a Jetstream restart, the driver authenticates and polls `GET /hypercerts/v1/policy`.
It does not use a public root-route response (which may be 404) as readiness.
T15 records sanitized phase artifacts for: a valid source/job and selected archive
coordinates/count; targeted transient identity and source faults with their
bounded proxy hit evidence and incomplete/no-rejection outcomes; the same job IDs
completing after Jetstream restart/retry; and the restart-durable invalid-signature
rejection plus archive exclusion assertion. The command prints its results
directory. It retains the base revision and `executed-tree.json`, whose dirty
manifest contains only paths/statuses and SHA-256 content digests for the tree
that ran; it contains no file contents, credentials, or record payloads. T15
archive client output, stderr, and snapshot response are temporary and deleted
immediately after the archive assertion and sanitized coordinate/count summary.
The generated credential file is removed during cleanup; Docker containers and
named volumes are also removed. A nonzero result is an observed failure, not a
skipped case.

No Cloudflare, Railway, public DNS, external PLC, or running deployment is used.
Rainbow is intentionally absent from T01; [TECH-635](https://linear.app/hypercerts/issue/TECH-635/validate-rainbow-raw-stream-recovery-and-retention)
owns its persistence/recovery validation.
