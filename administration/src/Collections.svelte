<script lang="ts">
  import { isValidNsid } from "@atproto/syntax";
  import type { Policy, Command } from "./api";
  import { collectionSuggestions } from "./collectionSuggestions";
  export let policy: Policy;
  export let submit: (command: Command) => Promise<void>;
  export let busy = false;
  let draft = [...policy.collections];
  let applied = [...policy.collections];
  let revision = policy.revision;
  let changedElsewhere = false;
  let collection = "";
  let error = "";
  let input: HTMLInputElement;
  $: if (policy.revision !== revision) {
    changedElsewhere =
      !sameCollections(draft, applied) && !sameCollections(draft, policy.collections);
    if (!changedElsewhere) {
      draft = [...policy.collections];
      applied = [...policy.collections];
    }
    revision = policy.revision;
  }

  function sameCollections(left: string[], right: string[]) {
    if (left.length !== right.length) return false;
    const a = [...left].sort();
    const b = [...right].sort();
    return a.every((value, index) => value === b[index]);
  }

  function addCollection() {
    const value = collection.trim();
    error = "";
    if (!isValidNsid(value)) {
      error = "Enter a valid exact collection NSID.";
      input.focus();
      return;
    }
    if (draft.includes(value)) {
      error = `${value} is already in this policy.`;
      input.focus();
      return;
    }
    draft = [...draft, value];
    collection = "";
  }

  function useCurrent() {
    draft = [...policy.collections];
    applied = [...policy.collections];
    revision = policy.revision;
    changedElsewhere = false;
    error = "";
  }

  function save() {
    if (changedElsewhere) return;
    void submit({
      kind: "collections",
      expectedRevision: revision,
      collections: draft,
    });
  }
</script>

<div class="intro">
  <p class="eyebrow">Archive policy</p>
  <h1>Record collections</h1>
  <p>
    Choose the exact collections Jetstream acquires. Each change creates a
    durable policy revision and schedules source backfills.
  </p>
</div>
<p class="policy-line">
  Applied revision <strong>{policy.revision}</strong> · {policy.collections
    .length} enabled collections
</p>
<form
  class="editor"
  novalidate
  onsubmit={(e) => {
    e.preventDefault();
    save();
  }}
>
  <label for="collection">Collection NSID</label>
  <input
    id="collection"
    bind:this={input}
    bind:value={collection}
    list="collection-suggestions"
    disabled={busy}
    spellcheck="false"
    aria-describedby="collection-help collection-error"
    aria-invalid={!!error}
  />
  <datalist id="collection-suggestions">
    {#each collectionSuggestions as nsid (nsid)}
      <option value={nsid}></option>
    {/each}
  </datalist>
  <button type="button" disabled={busy} onclick={addCollection}>Add collection</button>
  <p id="collection-help" class="note">
    Pinned Hypercerts and Certified suggestions are available as you type. Enter
    any exact NSID; wildcards are not supported. An empty list stops new record
    acquisition; previously archived rows remain.
  </p>
  <div id="collection-error" role="alert">
    {#if error}<p class="error">{error}</p>{/if}
  </div>
  <ul class="collections" aria-label="Draft collection policy">
    {#each draft as nsid (nsid)}
      <li>
        <code>{nsid}</code>
        <button
          type="button"
          disabled={busy}
          aria-label={`Remove ${nsid}`}
          onclick={() => (draft = draft.filter((value) => value !== nsid))}>Remove</button
        >
      </li>
    {/each}
  </ul>
  {#if changedElsewhere}
    <p role="status">
      The applied collection policy changed. Your draft is preserved. Use the current policy before submitting.
    </p>
    <button type="button" onclick={useCurrent}>Use current policy</button>
  {/if}
  <button class="primary" disabled={busy || changedElsewhere}>Request policy change</button>
</form>

<style>
  .collections {
    list-style: none;
    padding: 0;
  }
  li {
    display: flex;
    align-items: center;
    gap: 1rem;
    flex-wrap: wrap;
    padding: 1rem 0;
    border-bottom: 1px solid var(--color-ui-separator);
  }
  code {
    overflow-wrap: anywhere;
  }
</style>
