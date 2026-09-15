<script lang="ts">
  import State from "./State.svelte";
  import {
    target,
    type Job,
    type Operation,
    type Coverage,
    type Audit,
    type Command,
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
  function jobProgress(job: Job) {
    if (["running", "in_progress"].includes(job.state)) {
      return job.totalReposKnown
        ? `${job.completedRepos} of ${job.totalRepos} repositories processed`
        : "Counting repositories before backfill starts";
    }
    return `${job.completedRepos} repositories processed`;
  }
  function actorLabel(actor: { actor: string; actorHandle?: string | null }) {
    return actor.actorHandle ? `@${actor.actorHandle}` : actor.actor;
  }
  let copyStatus = "";
  async function copy(actor: { actor: string; actorHandle?: string | null }) {
    try {
      if (!navigator.clipboard) throw new Error("clipboard unavailable");
      await navigator.clipboard.writeText(actor.actor);
      copyStatus = `Copied DID for ${actorLabel(actor)}.`;
    } catch {
      copyStatus = `Could not copy the DID for ${actorLabel(actor)}.`;
    }
  }
</script>

{#if screen === "jobs"}
  <div class="intro">
    <p class="eyebrow">Historical recovery</p>
    <h1>Backfill jobs</h1>
    <p>
      Acquire selected current records directly from a PDS. Selected-collection
      backfill fills the enabled collections for a source; quota recovery catches
      up accounts that become admitted after a quota increase. Job status comes
      from Jetstream.
    </p>
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
          ><th>PDS / Job id</th><th>Collections</th><th>Progress</th><th>Status</th
          ><th>Actions</th></tr
        ></thead
      ><tbody
        >{#each jobs as job}<tr
            ><td>{job.pds}<small>{job.id}</small></td><td
              >{job.policy.collections.join(", ") || "No collections"}</td
            ><td
              >{jobProgress(job)}<small>{job.attempts} attempts</small></td
            ><td
              ><State value={job.state} /><small
                >{job.errorCode?.replaceAll("_", " ") ?? ""}</small
              ></td
            ><td
              ><div class="actions">
                {#if ["pending", "running"].includes(job.state)}<button
                    disabled={busy}
                    onclick={() =>
                      submit({
                        kind: "job_action",
                        id: job.id,
                        action: "cancel",
                      })}>Cancel job</button
                  >{:else}<button
                    disabled={busy}
                    onclick={() =>
                      submit({
                        kind: "job_action",
                        id: job.id,
                        action: "retry",
                      })}>Retry job</button
                  >{/if}
              </div></td
            ></tr
          >{:else}<tr
            ><td colspan="5" class="empty"
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
                    ></td
                  ><td
                    >{group.item.completedRepos} repositories<small
                      >{group.item.reason?.replaceAll("_", " ") ||
                        "Current state only; historical events are not guaranteed."}</small
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
