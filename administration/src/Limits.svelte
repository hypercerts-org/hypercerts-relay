<script lang="ts">
  import type { Limit, Command } from "./api";
  export let rows: Limit[] = [];
  export let submit: (command: Command) => Promise<void>;
  export let busy = false;
  let kind = "global",
    pds = "",
    value = 100;
</script>

<div class="intro">
  <h1>Rate limits</h1>
  <p>
    Limit Relay ingestion in events per second. Global and source policies apply
    together; account admission quotas remain separate.
  </p>
</div>
<form
  class="inline-form"
  onsubmit={(e) => {
    e.preventDefault();
    void submit({
      kind: "limit",
      scope: kind === "global" ? "global" : pds,
      eventsPerSecond: value,
    });
  }}
>
  <div class="field">
    <label for="limit-scope">Scope</label><select
      id="limit-scope"
      bind:value={kind}
      ><option value="global">Global</option><option value="pds">One PDS</option
      ></select
    >
  </div>
  {#if kind === "pds"}<div class="field grow">
      <label for="limit-pds">Configured PDS origin</label><input
        id="limit-pds"
        type="url"
        bind:value={pds}
        required
      />
    </div>{/if}
  <div class="field">
    <label for="limit-value">Events per second</label><input
      id="limit-value"
      type="number"
      min="1"
      max="1000000"
      step="1"
      bind:value
      required
      aria-describedby="limit-help"
    />
  </div>
  <button class="primary" disabled={busy}>Request limit change</button>
</form>
<p id="limit-help" class="note">
  Whole numbers from 1 to 1,000,000. Burst allowance is one second of the
  configured rate. Values persist across Relay restarts.
</p>
<!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
<div
  class="table-wrap"
  role="region"
  aria-label="Scrollable results"
  tabindex="0"
>
  <table>
    <caption>Applied Relay rate policies</caption><thead
      ><tr
        ><th>Scope</th><th>Applied value</th><th>Backpressure</th><th
          >Recovery consequence</th
        ></tr
      ></thead
    ><tbody
      >{#each rows as row}<tr
          ><td>{row.scope}</td><td
            >{row.eventsPerSecond.toLocaleString()} events/second</td
          ><td>{row.waitingConnections} waiting connections</td><td
            >{row.recovery}</td
          ></tr
        >{:else}<tr
          ><td colspan="4" class="empty"
            >No explicit rate policies on this page. Existing Relay host limits
            continue to apply.</td
          ></tr
        >{/each}</tbody
    >
  </table>
</div>
<a href="/changes">Compare requested changes and service acknowledgments</a>
