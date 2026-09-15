<script lang="ts">
  import { afterUpdate } from "svelte";
  import State from "./State.svelte";
  import AccountQuota from "./AccountQuota.svelte";
  import {
    api,
    sourceOrigin,
    type Source,
    type Coverage,
    type Command,
  } from "./api";
  export let rows: Source[] = [];
  export let submit: (command: Command) => Promise<void>;
  export let busy = false;
  let pds = "",
    search = "",
    selected: Source | null = null,
    error = "";
  let detailUnavailable = false,
    coverage: Coverage | null = null,
    coverageUnavailable = false,
    detailRequest = 0,
    lookupRequest = 0;
  $: filter = search.trim().toLowerCase().replace(/^https?:\/\//, "");
  $: displayedRows = filter
    ? rows.filter((source) =>
        source.Hostname.toLowerCase().includes(filter),
      )
    : rows;
  afterUpdate(() => syncSelected(rows));
  $: syncSelected(rows);
  function syncSelected(currentRows: Source[]) {
    if (!selected) return;
    const previous = selected;
    const current = currentRows.find(
      (source) => source.HostID === previous.HostID || sourceOrigin(source) === sourceOrigin(previous),
    );
    if (current && JSON.stringify(current) !== JSON.stringify(previous)) {
      detailRequest++;
      selected = { ...current };
      detailUnavailable = false;
    }
  }
  export async function refreshDetails() {
    await refreshSelected();
  }
  function resetCoverage() {
    detailRequest++;
    coverage = null;
    coverageUnavailable = false;
  }
  async function refreshCoverage(origin: string, request: number) {
    try {
      const result = await api<{ items: Coverage[] }>(
        `/coverage?pds=${encodeURIComponent(origin)}`,
      );
      if (
        request === detailRequest &&
        selected &&
        sourceOrigin(selected) === origin
      ) {
        coverage = result.items[0] ?? null;
        coverageUnavailable = false;
      }
    } catch {
      if (
        request === detailRequest &&
        selected &&
        sourceOrigin(selected) === origin
      )
        coverageUnavailable = true;
    }
  }
  async function refreshSelected() {
    if (!selected) return;
    const origin = sourceOrigin(selected),
      request = ++detailRequest;
    await Promise.all([
      (async () => {
        try {
          const current = await api<Source>(
            `/source?pds=${encodeURIComponent(origin)}`,
          );
          if (
            request === detailRequest &&
            selected &&
            sourceOrigin(selected) === origin
          ) {
            selected = current;
            detailUnavailable = false;
          }
        } catch {
          if (
            request === detailRequest &&
            selected &&
            sourceOrigin(selected) === origin
          )
            detailUnavailable = true;
        }
      })(),
      refreshCoverage(origin, request),
    ]);
  }
  function select(source: Source) {
    lookupRequest++;
    error = "";
    selected = source;
    detailUnavailable = false;
    resetCoverage();
    void refreshSelected();
  }
  async function find() {
    const query = search,
      request = ++lookupRequest;
    error = "";
    try {
      const found = await api<Source>(
        `/source?pds=${encodeURIComponent(query)}`,
      );
      if (request !== lookupRequest) return;
      selected = found;
      detailUnavailable = false;
      resetCoverage();
      const detail = ++detailRequest;
      await refreshCoverage(sourceOrigin(found), detail);
    } catch (e) {
      if (request === lookupRequest) error = (e as Error).message;
    }
  }
  function coverageProgress(item: Coverage) {
    return item.totalReposKnown
      ? `${item.completedRepos} of ${item.totalRepos} active repositories scanned`
      : `${item.completedRepos} repositories scanned; inventory is still being counted`;
  }
</script>

<div class="intro">
  <p class="eyebrow">Source admission</p>
  <h1>PDS sources</h1>
  <p>Manage PDS instances.</p>
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
      type="text"
      inputmode="url"
      placeholder="pds.example"
      bind:value={pds}
      required
    /><small
      >HTTPS is assumed when no scheme is provided. Adding a source also schedules
      selected-collection acquisition.</small
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
    <label for="find-source">Filter to PDS</label><input
      id="find-source"
      type="text"
      inputmode="url"
      bind:value={search}
      placeholder="pds.example"
      required
    /><small>Typing filters the current page. Use the button to load an exact PDS.</small>
  </div>
  <button>Filter</button>
</form>
{#if error}<p role="alert" class="error">{error}</p>{/if}
{#if selected}
  <section class="detail" aria-label="Source detail">
    {#if detailUnavailable}<p class="error" role="status">
        Latest observation unavailable. The values below are from the last
        successful observation.
      </p>{/if}
    <div class="section-heading detail-heading">
      <button
        onclick={() => {
          lookupRequest++;
          selected = null;
          resetCoverage();
        }}>Close</button
      >
      <h2>Source: <em>{selected.Hostname}</em></h2>
    </div>
    <dl>
      <div>
        <dt>State</dt>
        <dd><State value={selected.DesiredState} /></dd>
      </div>
      <div>
        <dt>Runtime connection</dt>
        <dd><State value={selected.RuntimeState} /><small>Live Relay connectivity for this PDS; administrative state above controls whether it should connect.</small></dd>
      </div>
      <div>
        <dt>Cursor</dt>
        <dd>
          {selected.LastDurableCursor < 0
            ? "Not recorded"
            : selected.LastDurableCursor}
        </dd>
      </div>
      <div>
        <dt>Admission check</dt>
        <dd>{selected.Validation.Status} {selected.Validation.Reason}<small>Validates that the configured PDS can be safely reached and admitted before Relay acquisition starts.</small></dd>
      </div>
      <div>
        <dt>Admission quota</dt>
        <dd>{selected.AccountQuota.Limit} accounts<small>Maximum accounts Relay may admit from this PDS. This is not the PDS account total.</small></dd>
      </div>
      <div>
        <dt>Relay-observed accounts</dt>
        <dd>{selected.AccountQuota.Count}<small>Accounts currently known to Relay for this source, not a census of the PDS.</small></dd>
      </div>
      <div>
        <dt>Jetstream current-state coverage</dt>
        <dd>
          {#if coverageUnavailable}
            Unavailable<small>Jetstream did not return coverage for this source.</small>
          {:else if coverage}
            <State value={coverage.state} /><small>{coverageProgress(coverage)}</small><small>Policy revision {coverage.policy.revision}: {coverage.policy.collections.join(", ") || "No collections selected"}</small><small>{coverage.historyComplete ? "Historical coverage complete" : "Current snapshot only; historical coverage is not complete."}</small>{#if coverage.reason}<small>{coverage.reason.replaceAll("_", " ")}</small>{/if}
          {:else}
            Unknown<small>No Jetstream backfill result exists for this source.</small>
          {/if}
        </dd>
      </div>
    </dl>
    {#key sourceOrigin(selected)}
      <AccountQuota
        source={selected}
        {submit}
        {busy}
        unavailable={detailUnavailable}
      />
    {/key}
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
        ><th>Source</th><th>State / runtime connection</th><th>Admission check</th
        ><th>Admission quota</th><th>Relay-observed accounts</th><th>Cursor</th><th>Actions</th></tr
      ></thead
    ><tbody>
      {#each displayedRows as source (source.HostID)}<tr
          ><td
            ><button
              class="text-button"
              onclick={() => select(source)}>{source.Hostname}</button
            ><small
              >{source.RecoveryRequired
                ? "Recovery required"
                : "No recovery flagged"}</small
            ></td
          ><td
            ><State value={source.DesiredState} /><State
              value={source.RuntimeState}
            /></td
          ><td>{source.Validation.Status} {source.Validation.Reason}</td
          ><td>{source.AccountQuota.Limit} accounts</td
          ><td>{source.AccountQuota.Count}</td
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
          ><td colspan="7" class="empty"
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
<p class="note">
  Per-PDS historical collection counts are unavailable because archived events
  do not retain historical PDS attribution.
</p>
