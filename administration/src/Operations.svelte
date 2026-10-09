<script lang="ts">
  import State from "./State.svelte";
  import {
    api,
    target,
    type Job,
    type Operation,
    type Coverage,
    type Audit,
    type Command,
    type RepositoryDetail,
    type RepositoryDetailsPage,
  } from "./api";
  export let screen: string;
  export let jobs: Job[] = [];
  export let changes: Operation[] = [];
  export let coverage: Coverage[] = [];
  export let audit: Audit[] = [];
  export let submit: (command: Command) => Promise<void>;
  export let action: (id: string, action: "retry" | "cancel") => Promise<void>;
  export let moreChanges: () => Promise<void> = async () => {};
  export let moreAudit: () => Promise<void> = async () => {};
  export let hasMoreChanges = false;
  export let hasMoreAudit = false;
  export let busy = false;
  let pds = "",
    reason: "backfill" | "quota_recovery" = "backfill";
  function coverageGroups(items: Coverage[]) {
    const latest = new Map<string, Coverage>();
    for (const item of items) {
      const previous = latest.get(item.pds);
      if (!previous || item.createdAt > previous.createdAt) latest.set(item.pds, item);
    }
    return [...latest.values()].map((item) => ({
      pds: item.pds,
      item,
      collections: item.policy.collections.length
        ? item.policy.collections
        : ["No collections selected"],
    }));
  }
  function jobPurpose(job: Job) {
    return job.reason === "quota_recovery"
      ? "Quota recovery"
      : "Selected-collection backfill";
  }
  function jobProgress(job: Job) {
    if (!job.totalReposKnown) {
      return job.state === "running"
        ? "Counting the initial repository inventory; total not known yet"
        : "Initial repository inventory is unknown; progress total unavailable";
    }
    return `${job.completedRepos} of ${job.totalRepos} repositories from the initial inventory processed`;
  }
  function currentStateScope(item: { coverage: string }) {
    const scope = item.coverage.replaceAll("_", " ");
    return `${scope}; historical coverage is unknown`;
  }
  function actorLabel(actor: { actor: string; actorHandle?: string | null }) {
    return actor.actorHandle ? `@${actor.actorHandle}` : actor.actor;
  }
  let copyStatus = "";
  type RepositoryDetailsAccumulator = Pick<
    RepositoryDetailsPage,
    "repositories" | "nextCursor"
  >;
  let openRepositoryJob = "";
  let repositoryPages: Record<string, RepositoryDetailsAccumulator> = {};
  let repositoryJobs: Record<string, Job> = {};
  let repositoryErrors: Record<string, string> = {};
  let repositoryLoading: Record<string, boolean> = {};
  let repositorySnapshotGeneration = 0;
  let repositoryRequestTokens = new Map<string, symbol>();
  function invalidateRepositorySnapshots() {
    repositorySnapshotGeneration++;
    repositoryRequestTokens.clear();
    repositoryPages = {};
    repositoryJobs = {};
    repositoryErrors = {};
    repositoryLoading = {};
    openRepositoryJob = "";
  }
  $: if (jobs) invalidateRepositorySnapshots();
  function jobAction(id: string, action: "retry" | "cancel") {
    invalidateRepositorySnapshots();
    void submit({ kind: "job_action", id, action });
  }
  async function copy(actor: { actor: string; actorHandle?: string | null }) {
    try {
      if (!navigator.clipboard) throw new Error("clipboard unavailable");
      await navigator.clipboard.writeText(actor.actor);
      copyStatus = `Copied DID for ${actorLabel(actor)}.`;
    } catch {
      copyStatus = `Could not copy the DID for ${actorLabel(actor)}.`;
    }
  }
  function revokeRepositoryRequest(jobId: string) {
    repositoryRequestTokens.delete(jobId);
    repositoryLoading = { ...repositoryLoading, [jobId]: false };
  }
  async function toggleRepositories(job: Job) {
    const previouslyOpenJob = openRepositoryJob;
    if (previouslyOpenJob === job.id) {
      revokeRepositoryRequest(job.id);
      openRepositoryJob = "";
      return;
    }
    if (previouslyOpenJob) revokeRepositoryRequest(previouslyOpenJob);
    openRepositoryJob = job.id;
    await loadRepositories(job.id);
  }
  async function loadRepositories(jobId: string, append = false) {
    const generation = repositorySnapshotGeneration;
    const requestToken = Symbol(jobId);
    repositoryRequestTokens.set(jobId, requestToken);
    const ownsRequest = () =>
      generation === repositorySnapshotGeneration &&
      repositoryRequestTokens.get(jobId) === requestToken;
    repositoryLoading = { ...repositoryLoading, [jobId]: true };
    repositoryErrors = { ...repositoryErrors, [jobId]: "" };
    const after = append ? repositoryPages[jobId]?.nextCursor ?? "" : "";
    try {
      const page = await api<RepositoryDetailsPage>(
        `/jobs/${encodeURIComponent(jobId)}/repositories?limit=50${after ? `&after=${encodeURIComponent(after)}` : ""}`,
      );
      if (!ownsRequest()) return;
      const previous = repositoryPages[jobId];
      repositoryJobs = { ...repositoryJobs, [jobId]: page.job };
      repositoryPages = {
        ...repositoryPages,
        [jobId]: {
          repositories: append
            ? [...(previous?.repositories ?? []), ...page.repositories]
            : page.repositories,
          nextCursor: page.nextCursor,
        },
      };
    } catch (error) {
      if (ownsRequest()) {
        const message = (error as Error).message;
        if (message === "repository snapshot expired") {
          const pages = { ...repositoryPages };
          const snapshots = { ...repositoryJobs };
          delete pages[jobId];
          delete snapshots[jobId];
          repositoryPages = pages;
          repositoryJobs = snapshots;
          repositoryErrors = {
            ...repositoryErrors,
            [jobId]: "Repository snapshot expired. Restart from the first page.",
          };
        } else if (message === "repository snapshot too large") {
          repositoryErrors = {
            ...repositoryErrors,
            [jobId]: "This inventory exceeds the 50,000-row repository detail limit; no rows were loaded.",
          };
        } else {
          repositoryErrors = { ...repositoryErrors, [jobId]: message };
        }
      }
    } finally {
      if (ownsRequest()) {
        repositoryLoading = { ...repositoryLoading, [jobId]: false };
      }
    }
  }
  function repositoryFailure(detail: RepositoryDetail) {
    const failure = detail.failure;
    if (!failure) return "No failure recorded";
    return [
      failure.category.replaceAll("_", " "),
      failure.httpStatus ? `HTTP ${failure.httpStatus}` : "",
      failure.stage,
      failure.code?.replaceAll("_", " ") ?? "",
    ]
      .filter(Boolean)
      .join(" · ");
  }
  function diagnosticReason(job: Job) {
    return job.diagnostics.reason?.replaceAll("_", " ") ?? "";
  }
  function timeLabel(value?: string) {
    return value ? new Date(value).toLocaleString() : "";
  }
</script>

{#if screen === "jobs"}
  <div class="intro">
    <p class="eyebrow">Historical recovery</p>
    <h1>Backfill jobs</h1>
    <p>
      Acquire selected current records directly from a PDS. Selected-collection
      backfill fills the enabled collections for a source; quota recovery catches
      up accounts that become admitted after a quota increase. Each PDS backfill
      inventories active repositories directly from that PDS; Relay-observed
      account counts do not limit it. Job status comes from Jetstream.
    </p>
  </div>
  <div class="notice">
    Diagnostics describe current execution, not historical completeness or the
    cause of an earlier coverage gap. Viewing never retries work. Retry is an
    explicit audited action: completed archive checkpoints are preserved;
    incomplete jobs reset eligible unresolved repository budgets, while failed
    jobs refresh their inventory.
  </div>
  <form
    class="inline-form"
    onsubmit={(e) => {
      e.preventDefault();
      void submit({ kind: "job", pds, reason });
    }}
  >
    <div class="field grow">
      <label for="job-pds">Enabled PDS origin</label><input
        id="job-pds"
        type="url"
        bind:value={pds}
        required
        placeholder="https://pds.example"
      />
    </div>
    <div class="field">
      <label for="job-reason">Purpose</label><select
        id="job-reason"
        bind:value={reason}
        ><option value="backfill">Selected-collection backfill</option><option
          value="quota_recovery">Quota recovery</option
        ></select
      >
    </div>
    <button class="primary" disabled={busy}>Submit backfill</button>
  </form>
  <!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
  <div
    class="table-wrap"
    role="region"
    aria-label="Scrollable results"
    tabindex="0"
  >
    <table>
      <caption>Jetstream jobs</caption><thead
        ><tr
          ><th>PDS / Job id</th><th>Collections</th><th>Purpose</th><th>Progress</th><th>Status</th
          ><th>Actions</th></tr
        ></thead
      ><tbody
        >{#each jobs as job}{@const currentJob = repositoryJobs[job.id] ?? job}<tr
            ><td>{currentJob.pds}<small>{currentJob.id}</small></td><td
              >{currentJob.policy.collections.join(", ") || "No collections"}</td
            ><td>{jobPurpose(currentJob)}</td><td
              >{jobProgress(currentJob)}<small>{currentStateScope(currentJob)}</small><small>Job claim attempts: {currentJob.attempts}</small><small>Repository cycle: up to {currentJob.diagnostics.maxRepositoryAttempts} attempts per coordinate</small>{#if currentJob.diagnostics.unresolvedRepos !== undefined}<small>{currentJob.diagnostics.unresolvedRepos} repositories unresolved</small>{/if}</td
            ><td
              ><State value={currentJob.state} /><small>Execution: {currentJob.diagnostics.execution.replaceAll("_", " ")}</small>{#if diagnosticReason(currentJob)}<small>Reason: {diagnosticReason(currentJob)}</small>{/if}{#if currentJob.diagnostics.retryAt}<small>Next repository retry: {timeLabel(currentJob.diagnostics.retryAt)}</small>{/if}{#if currentJob.diagnostics.pdsCooldownUntil}<small>PDS cooldown until: {timeLabel(currentJob.diagnostics.pdsCooldownUntil)}</small>{/if}{#if currentJob.diagnostics.retryingRepos}<small>Repositories with retry state: {currentJob.diagnostics.retryingRepos}</small>{/if}<small>{currentJob.errorCode?.replaceAll("_", " ") ?? ""}</small></td
            ><td
              ><div class="actions">
                <button
                  disabled={!currentJob.totalReposKnown || currentJob.diagnostics.unresolvedRepos === 0}
                  aria-expanded={openRepositoryJob === currentJob.id}
                  onclick={() => void toggleRepositories(currentJob)}
                  >{openRepositoryJob === currentJob.id
                    ? "Hide repository details"
                    : currentJob.totalReposKnown
                      ? `View unresolved repositories (${currentJob.diagnostics.unresolvedRepos ?? "unknown"})`
                      : "Inventory not known"}</button
                >
                {#if ["pending", "running"].includes(currentJob.state)}<button
                    disabled={busy}
                    onclick={() => jobAction(currentJob.id, "cancel")}>Cancel job</button
                  >{:else}<button
                    disabled={busy}
                    onclick={() => jobAction(currentJob.id, "retry")}>Retry job</button
                  >{/if}
              </div></td
            ></tr
            >{#if openRepositoryJob === currentJob.id}<tr
              ><td colspan="6">
                <section id={`repository-details-${currentJob.id}`} aria-label={`Unresolved repositories for ${currentJob.pds}`}>
                  <h3>Unresolved repositories</h3>
                  {#if repositoryLoading[currentJob.id]}<p role="status">Loading repository details…</p>
                  {:else if repositoryErrors[currentJob.id]}<p role="alert">{repositoryErrors[currentJob.id]}</p>{#if repositoryErrors[currentJob.id].startsWith("Repository snapshot expired")}<button onclick={() => void loadRepositories(currentJob.id)}>Restart repository details</button>{:else if !repositoryErrors[currentJob.id].startsWith("This inventory exceeds")}<button onclick={() => void loadRepositories(currentJob.id)}>Retry loading details</button>{/if}
                  {:else if repositoryPages[currentJob.id]?.repositories.length}
                    <div class="table-wrap" role="region" aria-label={`Repository details for ${currentJob.pds}`} tabindex="0">
                      <table>
                        <caption>Not-checkpointed repositories for {currentJob.pds}</caption>
                        <thead><tr><th>DID</th><th>Listed revision</th><th>Repository state</th><th>Retry attempts</th><th>Failure / next retry</th></tr></thead>
                        <tbody>{#each repositoryPages[currentJob.id].repositories as detail (detail.did)}<tr>
                          <td>{detail.did}</td><td>{detail.listedRevision}</td><td>{detail.state.replaceAll("_", " ")}</td>
                          <td>{detail.attempts} of {currentJob.diagnostics.maxRepositoryAttempts} in this job/repository cycle</td>
                          <td>{repositoryFailure(detail)}{#if detail.retryAt}<small>Retry deadline: {timeLabel(detail.retryAt)}</small>{/if}</td>
                        </tr>{/each}</tbody>
                      </table>
                    </div>
                    {#if repositoryPages[currentJob.id].nextCursor}<button disabled={repositoryLoading[currentJob.id]} onclick={() => void loadRepositories(currentJob.id, true)}>Load more repositories</button>{/if}
                  {:else}<p>No unresolved repositories in the frozen inventory.</p>{/if}
                  {#if ["incomplete", "failed", "canceled"].includes(currentJob.state)}<p>Work is stopped. Use the explicit Retry action only when you want Jetstream to resume eligible work.</p>{/if}
                </section>
              </td></tr>{/if}
          {:else}<tr
            ><td colspan="6" class="empty"
              >No jobs on this page. Submit a backfill for an enabled source.</td
            ></tr
          >{/each}</tbody
      >
    </table>
  </div>
{:else if screen === "coverage"}
  <div class="intro">
    <p class="eyebrow">Retention evidence</p>
    <h1>Collection coverage</h1>
    <p>
      Coverage is grouped by PDS. Expand a PDS to see each selected collection and
      the latest current-state backfill result known for it.
    </p>
  </div>
  <div class="notice">
    Historical PDS attribution is shown as <strong>unknown</strong> when archived
    events do not preserve which PDS supplied older records. That does not change
    the current PDS coverage shown below; it means the UI must not claim complete
    historical provenance.
  </div>
  <section class="coverage-groups" aria-label="Coverage by PDS">
    {#each coverageGroups(coverage) as group}
      <details class="coverage-group">
        <summary>{group.pds}</summary>
        <!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
        <div
          class="table-wrap"
          role="region"
          aria-label={`Collections for ${group.pds}`}
          tabindex="0"
        >
          <table>
            <caption>Collections retained for {group.pds}</caption><thead
              ><tr
                ><th>Collection</th><th>Current-state coverage</th><th
                  >Progress / limitations</th></tr
              ></thead
            ><tbody
              >{#each group.collections as collection}
                <tr
                  ><td>{collection}</td><td
                    ><State value={group.item.state} /><small
                      >Historical PDS attribution: {group.item.historicalPDSAttribution}</small
                    ><small>Execution: {group.item.diagnostics.execution.replaceAll("_", " ")}{#if group.item.diagnostics.reason}; {group.item.diagnostics.reason.replaceAll("_", " ")}{/if}</small
                    >{#if group.item.diagnostics.unresolvedRepos !== undefined}<small>{group.item.diagnostics.unresolvedRepos} repositories unresolved</small>{/if}{#if group.item.diagnostics.retryAt}<small>Next repository retry: {timeLabel(group.item.diagnostics.retryAt)}</small>{/if}{#if group.item.diagnostics.pdsCooldownUntil}<small>PDS cooldown until: {timeLabel(group.item.diagnostics.pdsCooldownUntil)}</small>{/if}</td
                  ><td
                    >{group.item.totalReposKnown
                      ? `${group.item.completedRepos} of ${group.item.totalRepos} repositories from the initial inventory processed`
                      : group.item.diagnostics.execution === "running"
                        ? "Counting the initial inventory; total not known yet"
                        : "Initial repository inventory is unknown; progress total unavailable"}<small
                      >Policy revision {group.item.policy.revision}; {currentStateScope(group.item)}</small
                    ><small
                      >{group.item.reason?.replaceAll("_", " ") ||
                        "No incomplete-input reason reported."}</small
                    ><small>Job {group.item.jobId}</small></td
                  ></tr
                >
              {/each}</tbody
            >
          </table>
        </div>
      </details>
    {:else}
      <p class="empty">
        No coverage results on this page. Configured sources without a backfill
        result have unknown coverage.
      </p>
    {/each}
  </section>
  <a href="/sources">Manage PDS instances</a>
{:else if screen === "audit"}
  <div class="intro">
    <p class="eyebrow">Activity record</p>
    <h1>Audit Log</h1>
    <p>
      Requested changes and actor-attributed audit events in one place. Actor
      handles are shown first; hover or focus the handle to see the DID.
    </p>
  </div>
  <p class="sr-only" role="status">{copyStatus}</p>
  <!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
  <div
    class="table-wrap"
    role="region"
    aria-label="Requested changes"
    tabindex="0"
  >
    <table>
      <caption>Requested changes</caption><thead
        ><tr
          ><th>Requested change</th><th>Actor / time</th><th>Status</th><th
            >Actions</th
          ></tr
        ></thead
      ><tbody
        >{#each changes as change}<tr
            ><td>{target(change.command)}<small>{change.id}</small></td><td
              ><span title={change.actor} tabindex="0">{actorLabel(change)}</span>
              <button class="text-button" aria-label={`Copy DID for ${actorLabel(change)}`} onclick={() => copy(change)}>Copy DID</button><small
                >{new Date(change.createdAt).toLocaleString()}</small
              ></td
            ><td
              ><State value={change.state} /><small
                >{change.error?.replaceAll("_", " ") ?? ""}</small
              ></td
            ><td
              >{#if change.state === "requested"}<button
                  disabled={busy}
                  onclick={() => action(change.id, "cancel")}
                  >Cancel change</button
                >{:else if ["failed", "incomplete"].includes(change.state)}<button
                  disabled={busy}
                  onclick={() => action(change.id, "retry")}
                  >Retry change</button
                >{:else}—{/if}</td
            ></tr
          >{:else}<tr
            ><td colspan="4" class="empty"
              >No requested changes on this page.</td
            ></tr
          >{/each}</tbody
      >
    </table>
  </div>
  {#if hasMoreChanges}<button disabled={busy} onclick={moreChanges}>Load more requested changes</button>{/if}
  <!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
  <div
    class="table-wrap"
    role="region"
    aria-label="Audit history"
    tabindex="0"
  >
    <table>
      <caption>Audit history</caption><thead
        ><tr
          ><th>Time</th><th>Actor</th><th>Event</th><th>Operation / result</th
          ></tr
        ></thead
      ><tbody
        >{#each audit as event}<tr
            ><td>{new Date(event.time).toLocaleString()}</td><td
              ><span title={event.actor} tabindex="0">{actorLabel(event)}</span>
              <button class="text-button" aria-label={`Copy DID for ${actorLabel(event)}`} onclick={() => copy(event)}>Copy DID</button></td
            ><td><State value={event.action} /></td><td
              >{event.operation ?? "Access change"}<small
                >{String(event.detail.error ?? event.detail.did ?? "")}</small
              ></td
            ></tr
          >{:else}<tr
            ><td colspan="4" class="empty">No audit events on this page.</td
            ></tr
          >{/each}</tbody
      >
    </table>
  </div>
  {#if hasMoreAudit}<button disabled={busy} onclick={moreAudit}>Load more audit history</button>{/if}
{/if}

<style>
  .sr-only {
    position: absolute;
    width: 1px;
    height: 1px;
    padding: 0;
    margin: -1px;
    overflow: hidden;
    clip: rect(0, 0, 0, 0);
    white-space: nowrap;
    border: 0;
  }
</style>
