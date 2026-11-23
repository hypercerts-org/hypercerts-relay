<script lang="ts">
  import { onMount } from "svelte";
  import { api } from "./api";

  export let csrf: string;

  let current: number | null = null;
  let value = 100000;
  let loading = true;
  let busy = false;
  let error = "";
  let notice = "";

  export async function refresh() {
    loading = true;
    error = "";
    try {
      const settings = await api<{ maxAccountLimit: number }>(
        "/sources/discovery/settings",
      );
      current = settings.maxAccountLimit;
      value = settings.maxAccountLimit;
    } catch (cause) {
      error = `Could not load discovery settings: ${(cause as Error).message}`;
    } finally {
      loading = false;
    }
  }

  async function save(event: SubmitEvent) {
    event.preventDefault();
    if (busy) return;
    if (
      !Number.isInteger(value) ||
      value < 1000 ||
      value > Number.MAX_SAFE_INTEGER
    ) {
      error = "Enter a whole number of at least 1,000.";
      notice = "";
      return;
    }
    busy = true;
    error = "";
    notice = "";
    try {
      const settings = await api<{ maxAccountLimit: number }>(
        "/sources/discovery/settings",
        { maxAccountLimit: value },
        csrf,
      );
      current = settings.maxAccountLimit;
      value = settings.maxAccountLimit;
      notice = "Discovery account limit maximum saved.";
    } catch (cause) {
      error = `Could not save discovery settings: ${(cause as Error).message}`;
    } finally {
      busy = false;
    }
  }

  onMount(() => {
    void refresh();
  });
</script>

<section class="overview" aria-labelledby="discovery-settings-heading">
  <h2 id="discovery-settings-heading">Automatic PDS discovery</h2>
  <p class="section-copy">
    When the discovery service adds a PDS, it raises that source's account limit
    to the Bluesky relay's account count, rounded up to the next thousand. It
    never requests more than this maximum. Operator changes to a source always
    take precedence.
  </p>
  {#if loading}
    <p role="status">Loading discovery settings…</p>
  {:else}
    <form class="inline-form" onsubmit={save}>
      <div class="field">
        <label for="discovery-max-account-limit">Maximum account limit</label>
        <input
          id="discovery-max-account-limit"
          type="number"
          min="1000"
          step="1"
          bind:value
          required
          disabled={busy}
          aria-describedby="discovery-max-account-limit-help"
        />
      </div>
      <button class="primary" disabled={busy}>
        {busy ? "Saving…" : "Save maximum"}
      </button>
    </form>
    <p id="discovery-max-account-limit-help" class="note">
      Whole number of at least 1,000.{#if current !== null}
        Currently {current.toLocaleString()} accounts.{/if} Changes are recorded
      in the audit log.
    </p>
  {/if}
  {#if error}<p class="error" role="alert">{error}</p>{/if}
  {#if notice}<p class="notice" role="status">{notice}</p>{/if}
</section>
