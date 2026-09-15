# SonarCloud analysis and triage

PR #16's analysis of `0fb9d9fe` reported 207 open findings. Of these, 196
originated with the copied Jetstream baseline and 11 appeared in later Hypercerts
work. Imported origin is not grounds for ignoring a runtime defect.

## Analysis scope

`.sonarcloud.properties` configures Automatic Analysis, using the repository
root for source/test discovery and disjoint filename filters for colocated Go
and Python tests. Tests remain analyzed as tests. Only `jetstream/api/`, whose
Go files declare lexgen generation, is excluded from source analysis and
copy/paste detection. The maintained Jetstream runtime is not excluded, and
quality-gate thresholds are unchanged.

Sonar documents repository configuration on the default branch. A human merge
or authorized project-settings update may be needed before this scope affects
PR analyses. Do not claim the gate is green until a new scan confirms the applied
scope and results. No CI scanner/token migration is required by this change.

References: [Automatic analysis](https://docs.sonarsource.com/sonarqube-cloud/analyzing-source-code/automatic-analysis),
[Analysis scope](https://docs.sonarsource.com/sonarqube-cloud/managing-your-projects/project-analysis/setting-analysis-scope/setting-initial-scope).

## Dispositions to record after source fixes are scanned

These are review conclusions, not completed changes to remote issue status.

- `Web:ItemTagNotWithinContainerTagCheck` at the `backfillDuration` template is
  a false positive: its only invocation is inside the backfill `<dl>`. Do not
  insert a nested `<dl>` into that fragment to satisfy source-only parsing.
- Seven `godre:S8188` findings use deferred cancellation wrappers that also join
  workers: Hypercerts control/jobs/selection tests, live consumer, simulator
  e2e/getblock tests, and the backfill durability drainer. Preserve cancel-before-
  join ordering. The Indigo integration helper instead needed immediate,
  idempotent cleanup registration; tail/live callback tests gained fallback defers.
- `godre:S8239` and `godre:S8242` cover explicitly owned import-worker and shutdown
  lifetimes. Import work must outlive individual requests and shutdown needs a
  fresh bounded context after run cancellation. Do not substitute request contexts
  blindly or suppress these rules globally.
- The two `go:S4507` client findings write local diagnostic profiles through an
  opt-in harness. The production image builds `cmd/jetstream`, not `cmd/client`.
  Accept the diagnostic use with this evidence rather than deleting profiling.
- Server profiling is now separately opt-in and disabled by default. Enabling
  the control listener does not enable pprof. An operator who explicitly enables
  pprof must keep the listener private; bearer auth only covers the control API.

Prioritize maintained lifecycle code for further complexity work. Do not broadly
rewrite the imported runtime or generated codecs solely to lower a metric.

## PR #22 follow-up triage

The PR #22 remediation keeps three explicit transport contracts rather than
removing them solely to satisfy a scheme detector. Apply these dispositions only
after a fresh PR analysis confirms the source changes below:

- `AaCki_rGGgitzF3KtVkX` (`go:S5332`, `sourceOrigin`) is an accepted risk.
  `SourceView.NoSSL` represents an explicitly admitted non-TLS source; silently
  upgrading its origin changes the stored source contract.
- `AaCki_rGGgitzF3KtVkY` (`go:S5332`, rate-policy matching) is a false
  positive. The HTTP text is a durable policy key, not a request or transport
  operation.
- `AaCl8HaKE_EoXRMAQv7B` (`javascript:S5332`, acceptance driver) is an
  accepted test-fixture risk. The bearer-token control endpoint is reachable
  only on the disposable Compose network and has no published host port.

The remaining PR #22 findings have source fixes: fixture images are digest-only,
the PLC dependency install suppresses lifecycle scripts, Caddy and the driver
run as non-root with explicitly initialized writable volumes, the driver emits a
single JSON line rather than using a console logger, and the maintained
complexity paths are split behind characterization tests. Confirm the full
acceptance fixture before marking those findings fixed; do not broaden the
repository exclusions to hide them.
