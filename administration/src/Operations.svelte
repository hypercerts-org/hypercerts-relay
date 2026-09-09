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
  export let busy = false;
  let pds = "",
    reason: "backfill" | "quota_recovery" = "backfill";
</script>

{#if screen === "jobs"}
  <div class="intro">
    <p class="eyebrow">Historical recovery</p>
    <h1>Backfill <em>jobs</em></h1>
    <p>
      Acquire selected current records directly from a PDS. Job states come from
      Jetstream.
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
          ><th>Source / job</th><th>Policy</th><th>Progress</th><th>State</th
          ><th>Actions</th></tr
        ></thead
      ><tbody
        >{#each jobs as job}<tr
            ><td>{job.pds}<small>{job.id}</small></td><td
              >Revision {job.policy.revision}<small
                >{job.policy.collections.join(", ") || "No collections"}</small
              ></td
            ><td
              >{job.completedRepos} repositories<small
                >{job.attempts} attempts</small
              ></td
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
    <h1>Collection <em>coverage</em></h1>
    <p>
      Coverage is scoped to the named PDS and collection-policy revision. A
      complete snapshot does not prove complete history.
    </p>
  </div>
  <div class="notice">
    Historical PDS attribution is <strong>unknown</strong>. Current snapshot
    provenance is explicit below.
  </div>
  <!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
  <div
    class="table-wrap"
    role="region"
    aria-label="Scrollable results"
    tabindex="0"
  >
    <table>
      <caption>Backfill coverage by source and collection policy</caption><thead
        ><tr
          ><th>PDS / collections</th><th>Policy revision</th><th
            >Current-state coverage</th
          ><th>Progress / limitations</th></tr
        ></thead
      ><tbody
        >{#each coverage as item}<tr
            ><td
              >{item.pds}<small
                >{item.policy.collections.join(", ") ||
                  "No collections selected"}</small
              ></td
            ><td>{item.policy.revision}</td><td
              ><State value={item.state} /><small
                >Historical coverage: incomplete</small
              ></td
            ><td
              >{item.completedRepos} repositories<small
                >{item.reason?.replaceAll("_", " ") ||
                  "Current state only; historical events are not guaranteed."}</small
              ><small>Job {item.jobId}</small></td
            ></tr
          >{:else}<tr
            ><td colspan="4" class="empty"
              >No coverage results on this page. Configured sources without a
              backfill result have unknown coverage.</td
            ></tr
          >{/each}</tbody
      >
    </table>
  </div>
  <a href="/sources"
    >Inspect configured sources, connections and durable cursors</a
  >
{:else if screen === "audit"}
  <div class="intro">
    <p class="eyebrow">Activity record</p>
    <h1>Audit <em>history</em></h1>
    <p>
      Actor-attributed changes and their recorded outcomes. Entries are ordered
      by durable sequence.
    </p>
  </div>
  <!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
  <div
    class="table-wrap"
    role="region"
    aria-label="Scrollable results"
    tabindex="0"
  >
    <table>
      <caption>Management audit events</caption><thead
        ><tr
          ><th>Time</th><th>Actor</th><th>Event</th><th>Operation / result</th
          ></tr
        ></thead
      ><tbody
        >{#each audit as event}<tr
            ><td>{new Date(event.time).toLocaleString()}</td><td
              >{event.actor}</td
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
{:else}
  <div class="intro">
    <p class="eyebrow">Change tracking</p>
    <h1>Requested <em>changes</em></h1>
    <p>
      Requested configuration is recorded before application. “Applied” means
      the owning service acknowledged the change; backfill completion is
      reported separately.
    </p>
  </div>
  <!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
  <div
    class="table-wrap"
    role="region"
    aria-label="Scrollable results"
    tabindex="0"
  >
    <table>
      <caption>Durable management operations</caption><thead
        ><tr
          ><th>Requested change</th><th>Actor / time</th><th>State</th><th
            >Actions</th
          ></tr
        ></thead
      ><tbody
        >{#each changes as change}<tr
            ><td>{target(change.command)}<small>{change.id}</small></td><td
              >{change.actor}<small
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
{/if}
