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
