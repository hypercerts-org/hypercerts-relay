<script lang="ts">
  import { sourceOrigin, type Source, type Command } from "./api";
  export let source: Source;
  export let submit: (command: Command) => Promise<void>;
  export let busy = false;
  export let unavailable = false;
  let expectedLimit = source.AccountQuota.Limit;
  let draft = String(expectedLimit);
  let error = "";
  let input: HTMLInputElement;
  $: if (
    /^\d+$/.test(draft) &&
    Number.isSafeInteger(Number(draft)) &&
    Number(draft) === source.AccountQuota.Limit
  )
    expectedLimit = source.AccountQuota.Limit;
  $: changedElsewhere = source.AccountQuota.Limit !== expectedLimit;
  function useCurrent() {
    expectedLimit = source.AccountQuota.Limit;
    draft = String(expectedLimit);
    error = "";
  }
  async function save() {
    error = "";
    const accountLimit = Number(draft);
    if (!/^\d+$/.test(draft) || !Number.isSafeInteger(accountLimit)) {
      error = "Enter a whole number of accounts from 0 to 9007199254740991.";
      input.focus();
      return;
    }
    if (changedElsewhere || unavailable) return;
    await submit({
      kind: "account_quota",
      pds: sourceOrigin(source),
      expectedLimit,
      accountLimit,
    });
  }
</script>

<form
  class="editor"
  novalidate
  onsubmit={(event) => {
    event.preventDefault();
    void save();
  }}
>
  <label for="account-quota">Account quota</label>
  <input
    id="account-quota"
    bind:this={input}
    bind:value={draft}
    oninput={() => (error = "")}
    inputmode="numeric"
    aria-describedby="account-quota-help account-quota-error"
    aria-invalid={!!error}
  />
  <p id="account-quota-help" class="note">
    Maximum accounts admitted from {source.Hostname}. Zero stops new account
    admission. Lowering the quota does not remove existing accounts. Increasing
    it can reactivate accounts previously blocked by this quota; historical
    recovery is tracked separately in backfill jobs.
  </p>
  <div id="account-quota-error" role="alert">
    {#if error}<p class="error">{error}</p>{/if}
  </div>
  {#if changedElsewhere}
    <p role="status">
      The applied quota is now {source.AccountQuota.Limit} accounts. Your draft is
      preserved.
    </p>
    <button type="button" onclick={useCurrent}>Use current quota</button>
  {/if}
  <button
    class="primary"
    disabled={busy ||
      unavailable ||
      changedElsewhere ||
      draft === String(expectedLimit)}
  >
    Request account quota change
  </button>
</form>
