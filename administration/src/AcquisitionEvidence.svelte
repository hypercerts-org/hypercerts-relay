<script lang="ts">
  import State from "./State.svelte";
  import type { Acquisition } from "./api";
  export let acquisition: Acquisition | null | undefined;
  const counters = [
    ["initialInventory", "Initial inventory"],
    ["scanned", "Verified scanned"],
    ["matching", "Matching repositories"],
    ["noMatch", "Successful no-match repositories"],
    ["unresolved", "Unresolved repositories"],
    ["attributableRecords", "Attributable records"],
  ] as const;
  function value(count: number | null | undefined) {
    return typeof count === "number" && Number.isFinite(count)
      ? String(count)
      : "Unknown";
  }
  function timestamp(time: string | null | undefined) {
    if (!time) return "Unknown";
    const date = new Date(time);
    return Number.isNaN(date.getTime()) ? "Unknown" : date.toLocaleString();
  }
  function label(value: string) {
    return value.replaceAll("_", " ");
  }
</script>

<section aria-label="Current-policy acquisition evidence">
  <h2>Current-policy acquisition</h2>
  {#if acquisition}
    <State value={acquisition.state} />
    <p>Progress scope: job inventory</p>
    <dl>
      {#each counters as [key, name] (key)}
        <div>
          <dt>{name}</dt>
          <dd>{value(acquisition.counts?.[key])}</dd>
        </div>
      {/each}
    </dl>
    {#if acquisition.counts?.noMatch !== null && acquisition.counts?.noMatch !== undefined}
      <p>
        No-match repositories are successfully scanned repositories with no
        selected records.
      </p>
    {/if}
    <h3>Diagnostics</h3>
    <p>
      Failure category: {acquisition.diagnostics?.failureCategory
        ? label(acquisition.diagnostics.failureCategory)
        : "Unknown"}
    </p>
    <p>Last progress: {timestamp(acquisition.diagnostics?.lastProgressAt)}</p>
    <p>Attempts: {value(acquisition.diagnostics?.attempts)}</p>
    {#if acquisition.diagnostics?.affectedRepository}<p>
        Affected repository: {acquisition.diagnostics.affectedRepository}
      </p>{/if}
    {#if acquisition.diagnostics?.retryAction === "explicit_retry"}<p>
        Explicit retry required
      </p>
    {:else if acquisition.diagnostics?.retryAction}<p>
        Retry action: {label(acquisition.diagnostics.retryAction)}
      </p>{/if}
    {#if acquisition.diagnostics?.retryAt}<p>
        Next repository retry: {timestamp(acquisition.diagnostics.retryAt)}
      </p>{/if}
    {#if acquisition.diagnostics?.pdsCooldownUntil}<p>
        PDS cooldown until: {timestamp(
          acquisition.diagnostics.pdsCooldownUntil,
        )}
      </p>{/if}
    {#if acquisition.diagnostics?.waitReason}<p>
        Wait reason: {label(acquisition.diagnostics.waitReason)}
      </p>{/if}
    {#if acquisition.state === "retry_waiting" && !acquisition.diagnostics?.retryAt && !acquisition.diagnostics?.pdsCooldownUntil}<p
      >
        Waiting; timing unknown.
      </p>{/if}
  {:else}
    <State value="unknown" />
    <p>Progress scope: job inventory</p>
    {#each counters as [key, name] (key)}<p>{name}: Unknown</p>{/each}
    <p>Diagnostics: Unknown</p>
  {/if}
</section>
