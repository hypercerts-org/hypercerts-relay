<script lang="ts">
  import { onMount } from "svelte";
  import State from "./State.svelte";
  import { api, sourceOrigin, type Source, type Command } from "./api";
  export let rows: Source[] = [];
  export let submit: (command: Command) => Promise<void>;
  export let busy = false;
  let pds = "",
    search = "",
    selected: Source | null = null,
    error = "";
  let detailUnavailable = false;
  async function refreshSelected() {
    if (!selected) return;
    const origin = sourceOrigin(selected);
    try {
      const current = await api<Source>(
        `/source?pds=${encodeURIComponent(origin)}`,
      );
      if (selected && sourceOrigin(selected) === origin) {
        selected = current;
        detailUnavailable = false;
      }
    } catch {
      if (selected && sourceOrigin(selected) === origin)
        detailUnavailable = true;
    }
  }
  onMount(() => {
    const timer = setInterval(() => void refreshSelected(), 5000);
    return () => clearInterval(timer);
  });
  async function find() {
    error = "";
    try {
      selected = await api<Source>(`/source?pds=${encodeURIComponent(search)}`);
      detailUnavailable = false;
    } catch (e) {
      error = (e as Error).message;
    }
  }
</script>

<div class="intro">
  <h1>PDS sources</h1>
  <p>
    Manage approved sources and inspect their actual Relay connection state.
  </p>
</div>
<form
  class="inline-form"
  onsubmit={async (e) => {
    e.preventDefault();
    await submit({ kind: "source", pds, state: "enabled" });
  }}
>
  <div class="field grow">
    <label for="new-source">PDS origin</label><input
      id="new-source"
      type="url"
      placeholder="https://pds.example"
      bind:value={pds}
      required
    /><small
      >HTTPS origin only. Adding a source also schedules selected-collection
      acquisition.</small
    >
  </div>
  <button class="primary" disabled={busy}>Add source</button>
</form>
<form
  class="inline-form compact"
  onsubmit={(e) => {
    e.preventDefault();
    void find();
  }}
>
  <div class="field grow">
    <label for="find-source">Find a source by origin</label><input
      id="find-source"
      type="url"
      bind:value={search}
      placeholder="https://pds.example"
      required
    />
  </div>
  <button>Find source</button>
</form>
{#if error}<p role="alert" class="error">{error}</p>{/if}
{#if selected}
  <section class="detail" aria-label="Source detail">
    {#if detailUnavailable}<p class="error" role="status">
        Latest observation unavailable. The values below are from the last
        successful observation.
      </p>{/if}
    <div class="section-heading">
      <h2>{selected.Hostname}</h2>
      <button onclick={() => (selected = null)}>Close detail</button>
    </div>
    <dl>
      <div>
        <dt>Desired state</dt>
        <dd><State value={selected.DesiredState} /></dd>
      </div>
      <div>
        <dt>Connection</dt>
        <dd><State value={selected.RuntimeState} /></dd>
      </div>
      <div>
        <dt>Last durable cursor</dt>
        <dd>
          {selected.LastDurableCursor < 0
            ? "Not recorded"
            : selected.LastDurableCursor}
        </dd>
      </div>
      <div>
        <dt>Admission check</dt>
        <dd>{selected.Validation.Status} {selected.Validation.Reason}</dd>
      </div>
      <div>
        <dt>Account quota</dt>
        <dd>
          {selected.AccountQuota.Count} / {selected.AccountQuota.Limit} accounts
        </dd>
      </div>
    </dl>
    <p>
      {selected.RecoveryRequired
        ? "Recovery remains required. Inspect backfill jobs and coverage before claiming completeness."
        : "Relay reports no pending recovery requirement."}
    </p>
    <div class="actions">
      <button
        disabled={busy}
        onclick={() =>
          submit({
            kind: "job",
            pds: sourceOrigin(selected!),
            reason: "backfill",
          })}>Backfill selected collections</button
      ><a href={`/jobs?pds=${encodeURIComponent(sourceOrigin(selected))}`}
        >View jobs</a
      ><a href={`/coverage?pds=${encodeURIComponent(sourceOrigin(selected))}`}
        >Inspect coverage</a
      >
    </div>
  </section>
{/if}
<!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
<div
  class="table-wrap"
  role="region"
  aria-label="Scrollable results"
  tabindex="0"
>
  <table>
    <caption>Configured sources</caption><thead
      ><tr
        ><th>Source</th><th>Desired / connection</th><th>Last durable cursor</th
        ><th>Actions</th></tr
      ></thead
    ><tbody>
      {#each rows as source (source.HostID)}<tr
          ><td
            ><button
              class="text-button"
              onclick={() => {
                selected = source;
                detailUnavailable = false;
              }}>{source.Hostname}</button
            ><small
              >{source.RecoveryRequired
                ? "Recovery required"
                : "No recovery flagged"}</small
            ></td
          ><td
            ><State value={source.DesiredState} /><State
              value={source.RuntimeState}
            /></td
          ><td
            >{source.LastDurableCursor < 0
              ? "Not recorded"
              : source.LastDurableCursor}</td
          ><td
            ><div class="actions">
              <button
                disabled={busy}
                onclick={() =>
                  submit({
                    kind: "source",
                    pds: sourceOrigin(source),
                    state:
                      source.DesiredState === "enabled"
                        ? "disabled"
                        : "enabled",
                  })}
                >{source.DesiredState === "enabled"
                  ? "Disable"
                  : "Re-enable"}</button
              ><button
                disabled={busy || source.DesiredState === "removed"}
                onclick={() =>
                  submit({
                    kind: "source",
                    pds: sourceOrigin(source),
                    state: "removed",
                  })}>Remove</button
              >
            </div></td
          ></tr
        >
      {:else}<tr
          ><td colspan="4" class="empty"
            >No sources on this page. Add a PDS origin to begin acquisition.</td
          ></tr
        >{/each}
    </tbody>
  </table>
</div>
<p class="note">
  Disabling or removing a source stops acquisition. Archived records remain
  available.
</p>
