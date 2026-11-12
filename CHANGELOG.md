<!-- hypercerts: project-owned release history; review this file during every Indigo upstream sync. -->

## 0.10.1

### Patch Changes

- [#111](https://github.com/hypercerts-org/hypercerts-relay/pull/111) [`ff3e3a9`](https://github.com/hypercerts-org/hypercerts-relay/commit/ff3e3a9394dcaf825981a617ac33f90708019dbc) Thanks [@Ashex](https://github.com/Ashex)! - Jetstream live ingestion no longer stops silently when the Relay restarts.
  Previously, a single HTTP 502 or 503 from the Relay or its proxy during a
  reconnect ended the upstream stream. Jetstream then kept running without
  ingesting until its next restart. Transient rejections (408, 425, 429 and 5xx)
  are now retried with backoff from the same cursor. Any other end of the stream
  exits the process with an `upstream stream ended unexpectedly` error so it is
  restarted. Successful connections are logged as `upstream connected`.
  
  Catching up after a restart no longer overflows the per-account verify queue as
  easily. Verification now uses 128 workers by default, up from 32, so each
  account's queue holds 256 events instead of 64. Upstream reads are paced at 500
  frames per second by default. Tune these with `JETSTREAM_LIVE_VERIFY_PARALLELISM`,
  `JETSTREAM_LIVE_UPSTREAM_READ_RATE` and `JETSTREAM_LIVE_UPSTREAM_READ_BURST`.
  Pacing limits how fast Jetstream replays a large backlog.
  
  Repeated `verification failure` warnings for the same account are now logged
  once per minute, followed by a `verification failures repeated` summary with
  the count and the last error. The verifier failure metric is unchanged.

## 0.10.0

### Minor Changes

- [#105](https://github.com/hypercerts-org/hypercerts-relay/pull/105) [`4f6c0f9`](https://github.com/hypercerts-org/hypercerts-relay/commit/4f6c0f943261c8e66c0f133e9fddc02f86a897cf) Thanks [@Ashex](https://github.com/Ashex)! - Jetstream now schedules repository-scoped current-state repair after
  verify-queue loss. The live consumer caps each account's pending verification
  queue, and an account that outruns it has the oldest queued events discarded
  while the cursor advances past them. A verify-queue overflow now books a durable
  recovery job covering exactly the affected DIDs, so only those repositories are
  re-acquired rather than the whole PDS. The repair restores each repository's
  current records; it does not replay the discarded events, so a record version
  that was later updated or deleted is not recovered. Observations that have not yet been
  booked are held in memory and can still be lost on process exit or when the
  pending queue is full; check recovery warnings.
  
  Recovery jobs are visible through the control API and the administration job
  request, where `repository_recovery` accepts an explicit DID scope. They carry
  no whole-source evidence, so they never appear as a source's current-state
  coverage. Existing archive rows are not rewritten, and existing job identifiers
  are unchanged.
  
  For operators, unavailable latest revisions now leave only the affected DIDs
  undiscovered while healthy repositories reconcile. Discovery failures and partial
  progress survive restart without reporting complete recovery. Retry an incomplete
  job after restoring PDS availability; valid inventory and completed work are
  preserved, and automatic repository retries continue to respect cooldowns.

### Patch Changes

- [#25](https://github.com/hypercerts-org/hypercerts-relay/pull/25) [`333670e`](https://github.com/hypercerts-org/hypercerts-relay/commit/333670e0c2fe534bec29c143bd2b69a53d6a140c) Thanks [@Ashex](https://github.com/Ashex)! - Deliver the Hypercerts Relay with Jetstream and Admin web interface.
  
  ### Relay sources and durable events
  
  Add managed PDS sources with independent validation, enablement, disablement,
  removal, account quotas, lifecycle revisions, and status reporting. Public crawl
  requests cannot add or re-enable a source. Relay persists raw output before
  advancing source progress, so processing, cursor, disk-write, and sync failures
  remain replayable; consumers must tolerate repeated retained output. Persist
  versioned Jetstream recovery receipts before clearing recovery-required status,
  and preserve those flags across source policy changes. Recovery acknowledgements
  must match the source and collection-policy revisions, job identifier, and
  durable completion boundary. Permanent verification rejections contain no record
  payload. A completed matching backfill job retries its private Relay recovery
  receipt after restart.
  
  Add single-process rate admission with a renewable database lease, bounded
  admission measurements, and persistent global and per-PDS limits. A second Relay
  using the same database exits instead of splitting the policy. Graceful shutdown
  releases the lease and an expired lease permits recovery after a failed process.
  Private rate-policy responses expose bounded admitted-frame and waited-frame
  counters with process-local measurement scope; token balances and counters reset
  with the documented one-second burst.
  
  ### Jetstream retention and recovery
  
  Add the locally maintained Jetstream v2 archive, replay, live-subscription, and
  backfill runtime. Persist a versioned collection policy and apply it before
  record materialization on bootstrap, live, sync-replacement, restart, and retry
  paths. The default empty policy stores no record payloads; identity, account,
  and sync markers remain available and cursors continue advancing for excluded
  records. Seed new data directories with the bundled Hypercerts and Certified
  collections unless an explicit policy or opt-out is supplied. Persisted policies
  are reused and never overwritten on restart, and existing archived data is not
  purged.
  
  Add durable, idempotent jobs for configured PDS sources, collection policy
  changes, backfill, and quota recovery. Jobs freeze a validated repository
  inventory, identify their source and policy revisions, resume persisted progress,
  and report current-state coverage only. Unavailable input remains incomplete;
  historical completeness is never inferred from a snapshot. Direct-PDS snapshot
  rejection metadata retains the most recent 1,000 permanent rejections; older
  records are evicted, allowing a later retry to refetch an evicted listed
  revision. An operator must retry a failed job after correcting its input.
  
  Expose authenticated private Jetstream and Relay control APIs for policy,
  sources, jobs, progress, cancellation, retry, quotas, and recovery receipts.
  Enable the Relay control API with `RELAY_CONTROL_ADDR` and
  `RELAY_CONTROL_TOKEN_FILE`; enable the Jetstream API with
  `JETSTREAM_CONTROL_TOKEN_FILE` and `JETSTREAM_DEBUG_ADDR`. Operations use
  durable idempotency keys and separate retry namespaces. Public listeners do not
  expose control routes. Profiling is opt-in through `JETSTREAM_ENABLE_PPROF=true`
  or `--enable-pprof` and must be disabled after diagnostics.
  
  ### Administration
  
  Add the separate OAuth administration application for PDS and collection policy,
  backfill jobs, rate limits, coverage, and actor-attributed audit history.
  Requests persist before service application and can be inspected or retried.
  Seed the initial administrator once with `ADMIN_SEED_DID`; administrators can
  grant and revoke access, with immediate session revocation and durable audit
  records. Store refreshed display names and verified handles while retaining DID
  authorization and cached access when profile lookup fails. Existing
  administrators must sign in again to populate missing profile metadata, and
  revoking access also clears the cached profile. Accounts without profile
  metadata retain their DID fallback. Existing access history prevents reseeding,
  and a restart cannot restore revoked seed access.
  
  Add collection suggestions and removal controls, quota and admission metadata,
  current-state coverage, operator-facing labels, source-account inventory, and
  accessible desktop/mobile administration workflows. The local acceptance OAuth
  callback is a documented test-only double and does not qualify a production
  OAuth provider. Quota edits are validated and durably audited; stale edits are
  rejected, retries of the same value are safe, and reducing a quota does not
  remove existing accounts. Removing a source stops acquisition without purging
  retained archive rows. If an already-active source is enabled again, the
  connection is preserved; retry an incomplete enable request before retrying its
  backfill. The access CLI remains available when no administrator can sign in.
  
  The administration UI uses Hypercerts branding, displays the saved profile name
  in bold with the verified handle below it, and serves its fonts locally. Bare
  PDS hostnames are normalized to HTTPS;
  automatic polling is removed; coverage is grouped by PDS; audit history and
  requested changes are combined; and administrator rows show the last login.
  Backfill progress reports processed repositories out of the durable active
  total.
  
  The source table shows Jetstream’s latest active-repository inventory and
  processed-repository count beside Relay-observed accounts, and selected
  collections are collapsed by default. The UI distinguishes the PDS admission
  quota limit from Relay-observed account totals. Jetstream status-page host
  filters and account lookup controls have accessible labels.
  
  Claims, retries, and cancellation are atomic. Operation pages are ordered by
  creation time and clients must pass the opaque `next` cursor unchanged. Existing
  Jetstream retry receipts migrate into separate namespaces; do not downgrade
  Jetstream while journal commands remain retryable. SQLite sidecar files are
  protected, raw container secret inputs are cleared before service startup, and
  control origins are validated: use loopback or a single-service
  `*.railway.internal` host only with `HC_RAILWAY_STARTUP=1`; all other hosts must
  use HTTPS. Edge client IPs are honored for login limits, while other proxies
  must set `ADMIN_TRUST_PROXY` with trusted peer CIDRs. Event admission remains
  responsive during policy persistence, rate limits apply to every source frame,
  and management opens only after subscription initialization.
  
  ### Deployment and upgrade operations
  
  Provide canonical Railway-compatible container packaging with optional runtime
  secret bootstrap while keeping infrastructure configuration private. Set
  `HC_RAILWAY_STARTUP=1` to convert `HC_RELAY_CONTROL_SECRET`,
  `HC_JETSTREAM_CONTROL_SECRET`, and `HC_ADMIN_ENCRYPTION_SECRET` into restricted
  files; existing file-based credential interfaces remain supported. Preserve
  Relay database and event-store state together with Jetstream archive data and
  cursor positions before upgrades. Support forward upgrades and restore-only
  qualification from copied state; binary rollback after state migration is not
  supported.
  
  For new Jetstream data directories, set `JETSTREAM_COLLECTIONS` to an explicit
  comma-separated list of exact NSIDs, or use the bundled Hypercerts and Certified
  seed collections by default. `JETSTREAM_DISABLE_COLLECTION_SEED=true` (or
  `--disable-collection-seed`) disables that seed, while an explicit collections
  list takes precedence. Set `JETSTREAM_PDS_SOURCES` to initialize configured
  direct-PDS sources. The Jetstream service uses `JETSTREAM_RELAY_URL` for its
  owned Relay URL. Disposable private-host acceptance uses
  `JETSTREAM_ACCEPTANCE_PRIVATE_HOSTS` (or `--acceptance-private-hosts`) only with
  an acceptance-tag build; production images retain their private-host checks.
  Existing deployments can apply a desired collection list through the
  administration Collections screen. The administration image includes its
  production TypeScript loader.
  
  Direct-PDS bootstrap refreshes one stale DID signing-key cache entry after a
  signature mismatch. Source account pagination is optimized, commits for a
  different DID are rejected before identity refresh, and connection-failure logs
  include the failure reason. Railway Jetstream images use ordinary Docker layer
  caching rather than deployment-specific cache mounts. Profiling remains
  unauthenticated on the private listener, so restrict network access while it is
  enabled.

- [#25](https://github.com/hypercerts-org/hypercerts-relay/pull/25) [`743664b`](https://github.com/hypercerts-org/hypercerts-relay/commit/743664b33647e0f48be3ed763482279b878916da) Thanks [@Ashex](https://github.com/Ashex)! - Relay now durably mirrors Jetstream policy advances before accepting recovery work. An outdated backfill cannot acknowledge a newer source-recovery requirement; a policy update waits for Relay's private control acknowledgement. No operator action is required.

- [#38](https://github.com/hypercerts-org/hypercerts-relay/pull/38) [`e3ac806`](https://github.com/hypercerts-org/hypercerts-relay/commit/e3ac80634bcd646b480befe4667d293b6dc35087) Thanks [@Ashex](https://github.com/Ashex)! - Relay, Rainbow, Jetstream, and Administration now share the release version recorded in the source tag. Release preparation rejects mismatched component versions, and maintainers create the GitHub Release from the validated tag with generated notes.

- [#38](https://github.com/hypercerts-org/hypercerts-relay/pull/38) [`e3ac806`](https://github.com/hypercerts-org/hypercerts-relay/commit/e3ac80634bcd646b480befe4667d293b6dc35087) Thanks [@Ashex](https://github.com/Ashex)! - Release-candidate publishing now creates a container-backed Buildx builder before requesting provenance, allowing the three immutable component images to publish their attestations.

- [#44](https://github.com/hypercerts-org/hypercerts-relay/pull/44) [`a67d08d`](https://github.com/hypercerts-org/hypercerts-relay/commit/a67d08d4594315fd99ca59c57ca97a194f88f4ba) Thanks [@Ashex](https://github.com/Ashex)! - Add optional Jetstream consumer archive keys with per-key request and archive MB/minute limits. Operators can set `JETSTREAM_ARCHIVE_KEY_AUTH_ENABLED=true` alongside a private Jetstream control token and listener, issue keys through the private `/hypercerts/v1/archive-keys` API, then give each consumer its one-time token for archive replay. The flag defaults to off, so existing public archive access continues until operators enable it.

- [#46](https://github.com/hypercerts-org/hypercerts-relay/pull/46) [`9e3150c`](https://github.com/hypercerts-org/hypercerts-relay/commit/9e3150c5fd0473018b2d6d8840cb7d0ceb38c8f2) Thanks [@Ashex](https://github.com/Ashex)! - Administrators can create, inspect, and revoke optional Jetstream archive consumer keys from the Consumer API keys screen. Keys include a free-form owner, requests-per-minute budget, and archive MB-per-minute budget. Enable `JETSTREAM_ARCHIVE_KEY_AUTH_ENABLED=true` before issuing keys for enforced archive access.

- [#74](https://github.com/hypercerts-org/hypercerts-relay/pull/74) [`71efba0`](https://github.com/hypercerts-org/hypercerts-relay/commit/71efba05ceef866c814e2881f0fbe7ff3a69bc83) Thanks [@Ashex](https://github.com/Ashex)! - The administration screens now show current-policy acquisition evidence and the aggregate across all enabled sources, while keeping unknown history and live freshness distinct. No migration or configuration change is required.

- [#107](https://github.com/hypercerts-org/hypercerts-relay/pull/107) [`8742b86`](https://github.com/hypercerts-org/hypercerts-relay/commit/8742b8699c253462a8c7989d4c28c01e69dfc28b) Thanks [@holkexyz](https://github.com/holkexyz)! - The administration UI now uses Hanken Grotesk, bundled under the SIL Open Font License, as its body font in place of Switzer. Building the administration app or its image no longer downloads a font from Fontshare, so builds no longer need access to `cdn.fontshare.com`. No operator action is required.

- [#59](https://github.com/hypercerts-org/hypercerts-relay/pull/59) [`4a2d94f`](https://github.com/hypercerts-org/hypercerts-relay/commit/4a2d94f39c1bb8ad348cf8b406586b971fc0fc4a) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Use familiar repository and PDS terminology in administration job progress, failure details, and source summaries. Replace inventory and coordinate labels without changing job behavior or API fields.

- [#59](https://github.com/hypercerts-org/hypercerts-relay/pull/59) [`f20aaed`](https://github.com/hypercerts-org/hypercerts-relay/commit/f20aaedc8bafda077b240d783894e37da2c50ba3) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Jetstream now retries failed PDS downloads up to three times without losing
  progress after a restart. Rate-limit waits are capped at 30 minutes, and other
  PDS sources can continue while one waits. Jobs that still fail need a manual
  Retry. No configuration changes are needed.
  
  The cap only affects new responses. Previously saved longer waits remain
  unchanged.
  
  Older jobs without a saved inventory remain readable.

- [#59](https://github.com/hypercerts-org/hypercerts-relay/pull/59) [`f20aaed`](https://github.com/hypercerts-org/hypercerts-relay/commit/f20aaedc8bafda077b240d783894e37da2c50ba3) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Expose sanitized PDS inventory-listing failure metadata in authenticated job status. Listing failures keep the existing `source_unavailable` error code and retry behavior.

- [#59](https://github.com/hypercerts-org/hypercerts-relay/pull/59) [`4a2d94f`](https://github.com/hypercerts-org/hypercerts-relay/commit/4a2d94f39c1bb8ad348cf8b406586b971fc0fc4a) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Expose read-only backfill execution diagnostics, job progress and bounded inventory-failure details, and snapshot-consistent paginated repository details with failure times and explicit expiry and restart guidance.

- [#54](https://github.com/hypercerts-org/hypercerts-relay/pull/54) [`024d125`](https://github.com/hypercerts-org/hypercerts-relay/commit/024d12582223cbf974d4d3da595b1854a4b5e83d) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Retry transient direct-PDS `getRepo` failures up to three times with bounded backoff before recording a repository as incomplete. After a failed or incomplete backfill outcome is durably recorded, emit a structured warning without logging raw errors or response bodies.

- [#91](https://github.com/hypercerts-org/hypercerts-relay/pull/91) [`464ea50`](https://github.com/hypercerts-org/hypercerts-relay/commit/464ea50bc8feaac26d3e1096dbbe1cce1745b2ca) Thanks [@Ashex](https://github.com/Ashex)! - Add an optional collection directory for discovering candidate repositories from Relay events and PDS crawls. Operators must configure `COLLECTIONS_ADMIN_TOKEN` and restrict the API and metrics listeners; shared rate-limit bypass credentials are not forwarded. Directory results are candidates, not proof of current records or complete coverage.

- [#62](https://github.com/hypercerts-org/hypercerts-relay/pull/62) [`86512bf`](https://github.com/hypercerts-org/hypercerts-relay/commit/86512bf884de36912daef038beb2ffa36fe25d82) Thanks [@Ashex](https://github.com/Ashex)! - Expose a versioned current-policy coverage snapshot for every enabled PDS and an aggregate scoped to all enabled sources. Separate successful matching and no-match scans, unresolved inventory and attributable records, with unknown counts retained for older jobs. Historical coverage and live freshness remain independent and explicitly unknown at this private interface.
  
  Private API consumers should use schema version 1 acquisition evidence and the aggregate instead of choosing the newest job or treating job-wide progress as collection attribution. Existing archived records and job history are preserved; this change does not deploy services or recover historical data.

- [#59](https://github.com/hypercerts-org/hypercerts-relay/pull/59) [`6485a5b`](https://github.com/hypercerts-org/hypercerts-relay/commit/6485a5b7565ab08e78e50aac2753607daa542798) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Expose durable job progress timestamps in the private Jetstream job and coverage APIs. Missing checkpoint and repository failure dates remain unknown rather than being synthesized.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`f5afad3`](https://github.com/hypercerts-org/hypercerts-relay/commit/f5afad3962ddf9f30ebc1691ee039ebfc47b4e41) Thanks [@Ashex](https://github.com/Ashex)! - Relay now retries source lifecycle handoffs until Jetstream confirms the exact durable PDS revision. Jetstream treats repeated acceptance as idempotent across policy changes and restarts, while malformed or redirected acknowledgments remain retryable instead of silently dropping desired state.

- [#108](https://github.com/hypercerts-org/hypercerts-relay/pull/108) [`d6c9c22`](https://github.com/hypercerts-org/hypercerts-relay/commit/d6c9c2299b4e8ec81c14e148af45bfd2dfe8c955) Thanks [@Ashex](https://github.com/Ashex)! - Build Relay, Rainbow, the collection directory, Jetstream, and Administration with Go 1.27.3, and update the Postgres driver, GORM logging adapter, Jaeger exporter, and rate-limiting library. Existing Relay databases migrate without schema changes. No operator configuration changes are required.

- [#71](https://github.com/hypercerts-org/hypercerts-relay/pull/71) [`2f0fdf6`](https://github.com/hypercerts-org/hypercerts-relay/commit/2f0fdf64c87eeab056e422c86fe382f6c66e572d) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Simplify the administration Backfill jobs table: omit unrecorded listing-failure and last-progress lines, show run counts, unresolved counts and attempt limits only when they are notable, shorten progress to "N of M repositories processed", and stop offering Retry job on complete jobs. No operator action is required.

- [#97](https://github.com/hypercerts-org/hypercerts-relay/pull/97) [`f2cd763`](https://github.com/hypercerts-org/hypercerts-relay/commit/f2cd76326e3ac30056bc73356cdc3f7e4430c432) Thanks [@Ashex](https://github.com/Ashex)! - Jetstream now holds the Relay control credential needed to advance collection
  policy. Set `JETSTREAM_RELAY_CONTROL_URL` to the Relay's private control listener
  (`http://relay.railway.internal:2472` on Railway); the token file is provided by
  the container bootstrap as `JETSTREAM_RELAY_CONTROL_TOKEN_FILE`. When an enabled
  source requires that mirror and it is missing or unreachable, the collection
  change is rejected with `policy_mirror_unavailable` (HTTP 503) instead of failing
  as an unspecified persistence error. Existing archives are unaffected, and no
  stored data is rewritten.
  
  **Required on staging and production before deploying.** Jetstream must have
  `HC_RELAY_CONTROL_SECRET` (same value as Relay) and
  `JETSTREAM_RELAY_CONTROL_URL=http://relay.railway.internal:2472`. Relay must have
  `RELAY_CONTROL_ADDR=[::]:2472`. A public Relay domain does not serve the control
  API and returns 404. The full per-service list is in `docs/railway.md` under
  "Required variables per environment".

- [#59](https://github.com/hypercerts-org/hypercerts-relay/pull/59) [`4a2d94f`](https://github.com/hypercerts-org/hypercerts-relay/commit/4a2d94f39c1bb8ad348cf8b406586b971fc0fc4a) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Disable repository details for legacy backfill jobs without a saved repository list and show "Repository list unavailable" instead of offering a request that cannot succeed. No operator action is required.

- [#69](https://github.com/hypercerts-org/hypercerts-relay/pull/69) [`e8395aa`](https://github.com/hypercerts-org/hypercerts-relay/commit/e8395aaca02492c69bab2bb0fc24aa648deb7b50) Thanks [@Ashex](https://github.com/Ashex)! - Update Relay, Rainbow, Jetstream, and Administration container builds to patched Go and Node.js security releases. No operator configuration changes are required.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`dcf4302`](https://github.com/hypercerts-org/hypercerts-relay/commit/dcf43022e06107f9927215d5642b23da677f12f1) Thanks [@Ashex](https://github.com/Ashex)! - Control API operators: a policy change attempted before required policy adoption is initialized now returns HTTP 503 with `policy_adoption_not_ready`, rather than reporting a persistence failure. The saved policy and jobs remain unchanged; retry after initialization completes.

- [#75](https://github.com/hypercerts-org/hypercerts-relay/pull/75) [`82d766e`](https://github.com/hypercerts-org/hypercerts-relay/commit/82d766efc252993827075bb7e4a4d3a323c1adea) Thanks [@Ashex](https://github.com/Ashex)! - Correct the public status page to show current-policy PDS acquisition and keep legacy bootstrap, historical completeness, and global stream observations distinct. No configuration migration or deployment action is required.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`19903db`](https://github.com/hypercerts-org/hypercerts-relay/commit/19903db6136abd82b911fbe00d138bb04b88483c) Thanks [@Ashex](https://github.com/Ashex)! - Allow public `requestCrawl` to synchronously validate and admit a new PDS. Failed validation is retained with a one-minute retry cooldown. Concurrent or superseded admission checks return HTTP 409; clients should retry the request. The daily new-host limit applies when creating a source, returns HTTP 429 when reached, and does not apply to existing sources. Sibling relays are notified only after successful admission. Missing host checkers are reported as unavailable rather than as host-check failures.
  
  Source sockets now use a public-network-only dialer in normal builds, including for administratively added sources. HTTP and WS PDS sources are rejected even for administrators; source checks cannot downgrade HTTPS through redirects. Operators using plaintext, localhost, or private-network PDS hosts must move them to public HTTPS with valid TLS certificates and explicitly migrate any persisted `NoSSL` source state through their approved state-migration procedure. Existing insecure rows fail closed on validation and restart without rewriting their stored scheme or cursor. Remove `--allow-insecure-hosts` or `RELAY_ALLOW_INSECURE_HOSTS=true`, which now fails startup clearly. Operators already using public HTTPS hosts need no action.

- [#59](https://github.com/hypercerts-org/hypercerts-relay/pull/59) [`4a2d94f`](https://github.com/hypercerts-org/hypercerts-relay/commit/4a2d94f39c1bb8ad348cf8b406586b971fc0fc4a) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Explain repository listing failures in the administration jobs UI with concise messages that distinguish PDS HTTP errors (including HTTP 429 rate limiting), transport failures, timeouts, canceled requests, and unreadable responses. Retry behavior and error codes are unchanged. No operator action is required.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`9641b96`](https://github.com/hypercerts-org/hypercerts-relay/commit/9641b96cdf6f5104791adafd2ae0b25b3974cd07) Thanks [@Ashex](https://github.com/Ashex)! - Jetstream operators: completed recovery-receipt attempts now release their background resources promptly, including failed attempts waiting for retry. Long-running services no longer accumulate receipt watchers. No configuration change is required.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`ea24c52`](https://github.com/hypercerts-org/hypercerts-relay/commit/ea24c5257005fd12dbb50aeeb66000dcfbbc52f9) Thanks [@Ashex](https://github.com/Ashex)! - Selected-stream consumers: archive replay read failures now send a terminal `InternalError` before disconnecting instead of silently closing. Reconnect from the last successfully received cursor; failures do not skip archive rows. Cancellation remains quiet, and compressed connections receive the same error contract.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`2712a4f`](https://github.com/hypercerts-org/hypercerts-relay/commit/2712a4f2cbccbb803c7fd83b080302784c699ca1) Thanks [@Ashex](https://github.com/Ashex)! - Add an opt-in selected Jetstream v2 subscription contract that identifies stream and policy provenance, reports lifecycle reconciliation scope, and prevents excluded CAR content from being forwarded. Existing unqualified v2 subscriptions retain their upstream-compatible event payloads.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`98cc1a3`](https://github.com/hypercerts-org/hypercerts-relay/commit/98cc1a37e2f1c06c2d985955d55d209d6571e523) Thanks [@Ashex](https://github.com/Ashex)! - Selected-stream consumers: removing and re-enabling a collection at the same archive boundary no longer advertises an inverted, empty policy gap. Genuine disabled intervals remain visible. No client configuration change is required.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`eafd6cf`](https://github.com/hypercerts-org/hypercerts-relay/commit/eafd6cf467b2e1f0d5c2b2496ba69725c15ee99f) Thanks [@Ashex](https://github.com/Ashex)! - Selected Jetstream subscriptions now log and skip a known upstream continuity loss instead of stopping. An upstream `OutdatedCursor` notice or relay sequence gap is logged at error level and recorded durably, then ingestion continues; readiness, established subscriptions and restart are unaffected. The recorded loss is reported again on the next start. Treat the skipped interval as a real history gap and follow `docs/upgrade-restore.md`.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`3944288`](https://github.com/hypercerts-org/hypercerts-relay/commit/3944288b68a83ccf34d46494ff502ce28e48f481) Thanks [@Ashex](https://github.com/Ashex)! - Selected-stream consumers: `maxMessageSizeBytes` now limits the entire uncompressed event frame, including selected-stream metadata, for both replay and live delivery. Oversized events are skipped without stopping subsequent delivery, including on compressed connections. A zero limit remains unlimited.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`a2b8828`](https://github.com/hypercerts-org/hypercerts-relay/commit/a2b88289393c0852f562a30ec47bdc9983eb302c) Thanks [@Ashex](https://github.com/Ashex)! - For selected-stream consumers, Jetstream now reports stalled replay and metadata failures with a terminal InternalError when the connection is usable, without advancing past failed delivery. Equal-boundary collection gaps have a deterministic order, and JSON clients preserve both integer and object reconciliation boundaries. Operators need no configuration changes; wrapped missing segment errors remain benign during attribution recovery.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`6f6c580`](https://github.com/hypercerts-org/hypercerts-relay/commit/6f6c5802e8b8191b9df8881cdc25a09f18de8e9a) Thanks [@Ashex](https://github.com/Ashex)! - For selected-stream consumers, correct the subscription schema and generated Go types to expose existing event provenance, reconciliation scope and boundary metadata, plus capability, policy-adoption and history-gap notices. Typed JSON round trips preserve explicit null boundaries and empty collection and gap lists without adding capability fields to policy-adoption notices. The existing wire format is unchanged; ordinary subscriptions still omit this metadata. Operators do not need to change configuration.

- [#79](https://github.com/hypercerts-org/hypercerts-relay/pull/79) [`60c3fd5`](https://github.com/hypercerts-org/hypercerts-relay/commit/60c3fd5b51cfea3de18a36355baa4239827ba6a1) Thanks [@Ashex](https://github.com/Ashex)! - Correct selected-stream sync recovery during bootstrap and asynchronous ingestion, and prevent stale syncs for inactive accounts from stopping ingestion or publishing a misleading sync marker. Preserve prepared durable writes when a later event in an asynchronous batch fails, without hanging shutdown.
  
  Bootstrap sync recovery reconciles missing selected records against the destination archive during merge. Scoped sync markers survive compaction without deleting their replacements, unrelated collections, or newer records; ordinary full-sync tombstones remain unchanged.
  
  Selected subscriptions now derive their replay floor from the first durably attributed event, not initial policy history, and disclose a `legacy-scope-unknown` gap for earlier archive rows. Resumes in that region fail with `ReplayUnavailable` before streaming, regardless of filters. Until attribution activates, selected replay is unavailable and its floor is null; live-only subscriptions remain available. Empty archives do not advertise a fabricated legacy gap. Consumers upgrading an existing archive must explicitly reconcile selected state before using that floor or choosing live-only delivery. Ordinary subscriptions and raw Relay/Rainbow streams are unchanged.

- [#59](https://github.com/hypercerts-org/hypercerts-relay/pull/59) [`4a2d94f`](https://github.com/hypercerts-org/hypercerts-relay/commit/4a2d94f39c1bb8ad348cf8b406586b971fc0fc4a) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Keep administration navigation links clickable on short desktop screens by scrolling navigation within the sidebar instead of overlapping the signed-in account panel. No operator action is required.

- [#48](https://github.com/hypercerts-org/hypercerts-relay/pull/48) [`45133c6`](https://github.com/hypercerts-org/hypercerts-relay/commit/45133c6c5561e835c3d2ce9ecbcb8977a0b7b906) Thanks [@Ashex](https://github.com/Ashex)! - Give CI checks stable names and run required checks on every pull request and merge queue so branch rulesets can enforce them reliably.

- [#104](https://github.com/hypercerts-org/hypercerts-relay/pull/104) [`0ad63be`](https://github.com/hypercerts-org/hypercerts-relay/commit/0ad63be867074ff8fa334dd90c1dd717f9b7b9eb) Thanks [@Ashex](https://github.com/Ashex)! - Add an acceptance scenario for verify-queue overflow recovery. `./tests/acceptance/run T20` stops Jetstream, writes a single-account backlog to the fixture PDS, then restarts Jetstream so the Relay replays the backlog at socket speed. The replay sheds events from atmos's per-DID verify queue, and the scenario fails unless that loss is booked as a durable, repository-scoped recovery job.
  
  The acceptance PDS fixtures now run with `PDS_RATE_LIMITS_ENABLED=false`. They are disposable containers on a private network, and the write limiter otherwise paces the backlog far beyond the test budget. Existing scenarios do not depend on the limiter firing, and T01 was re-run green with it disabled.

- [#62](https://github.com/hypercerts-org/hypercerts-relay/pull/62) [`a4babaf`](https://github.com/hypercerts-org/hypercerts-relay/commit/a4babaf8f11e0cdbd7a52d24106496cecc11d555) Thanks [@Ashex](https://github.com/Ashex)! - Correct current-state coverage to distinguish automatic retry waits from running work, with recorded retry and PDS cooldown deadlines. Coverage now uses one coherent evidence capture and shows classified listing failures and the latest durable progress date without inventing legacy attribution counts. Existing job execution contracts are unchanged. No operator migration is required.

- [#54](https://github.com/hypercerts-org/hypercerts-relay/pull/54) [`854b621`](https://github.com/hypercerts-org/hypercerts-relay/commit/854b6217a5ed93d7ef98597253d2ea002a9db29d) Thanks [@Kzoeps](https://github.com/Kzoeps)! - Point the Relay and Jetstream homepage source links to their component directories in the Hypercerts Relay repository.

# Changelog

All notable Hypercerts Relay changes are documented here. [Changesets](https://github.com/changesets/changesets) maintains version sections from approved release notes. Releases use [Semantic Versioning](https://semver.org/).

Hypercerts release history starts after the Indigo baseline `41278964ec8e3253e70d4e919dfb8e34211c543d` (2026-09-03). Historical Indigo changelog entries are not Hypercerts Relay releases.
