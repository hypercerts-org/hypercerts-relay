<script lang="ts">
  import { onMount, tick } from "svelte";
  import { api, type ArchiveKey, type ArchiveKeyInput } from "./api";

  export let csrf: string;

  let keys: ArchiveKey[] = [];
  let loading = true;
  let busy = false;
  let error = "";
  let notice = "";
  let name = "";
  let owner = "";
  let requestsPerMinute = 60;
  let archiveMegabytesPerMinute = 100;
  let token = "";
  let tokenName = "";
  let confirmingId = "";
  let confirmButton: HTMLButtonElement;
  let revokeButton: HTMLButtonElement | undefined;
  let nameInput: HTMLInputElement;

  $: owners = [...new Set(keys.map((key) => key.owner))].sort((a, b) =>
    a.localeCompare(b),
  );

  export async function refresh() {
    loading = true;
    error = "";
    try {
      const result = await api<{ keys: ArchiveKey[] }>("/archive-keys");
      keys = result.keys;
    } catch (cause) {
      error = `Could not load consumer keys: ${(cause as Error).message}`;
    } finally {
      loading = false;
    }
  }

  async function create(event: SubmitEvent) {
    event.preventDefault();
    if (token || busy) return;
    const input: ArchiveKeyInput = {
      name: name.trim(),
      owner: owner.trim(),
      requestsPerMinute,
      archiveMegabytesPerMinute,
    };
    if (
      !input.name ||
      !input.owner ||
      !Number.isInteger(input.requestsPerMinute) ||
      input.requestsPerMinute < 1 ||
      input.requestsPerMinute > 100000 ||
      !Number.isInteger(input.archiveMegabytesPerMinute) ||
      input.archiveMegabytesPerMinute < 1 ||
      input.archiveMegabytesPerMinute > 100000
    ) {
      error = "Enter a key name, owner, and whole-number limits from 1 to 100,000.";
      return;
    }
    busy = true;
    error = "";
    notice = "";
    try {
      const created = await api<{ key: ArchiveKey; token: string }>(
        "/archive-keys",
        input,
        csrf,
      );
      token = created.token;
      tokenName = created.key.name;
      keys = [created.key, ...keys];
      name = "";
      await tick();
      document.getElementById("new-archive-key")?.focus();
    } catch (cause) {
      error = `Could not create consumer key: ${(cause as Error).message}`;
    } finally {
      busy = false;
    }
  }

  async function copyToken() {
    try {
      await navigator.clipboard.writeText(token);
      notice = "Consumer key copied. Store it securely before dismissing it.";
      error = "";
    } catch {
      error = "Copy failed. Select the key text and copy it manually before dismissing it.";
    }
  }

  function dismissToken() {
    token = "";
    tokenName = "";
    notice = "Key display dismissed. It cannot be shown again.";
    void tick().then(() => nameInput.focus());
  }

  async function askToRevoke(id: string, button: HTMLButtonElement) {
    confirmingId = id;
    revokeButton = button;
    error = "";
    notice = "";
    await tick();
    confirmButton.focus();
  }

  function cancelRevoke() {
    confirmingId = "";
    void tick().then(() => revokeButton?.focus());
  }

  async function revoke(id: string) {
    if (busy) return;
    busy = true;
    error = "";
    notice = "";
    try {
      await api<void>(`/archive-keys/${encodeURIComponent(id)}`, undefined, csrf, undefined, "DELETE");
      confirmingId = "";
      notice = "Consumer key revoked. Archive requests using it will be denied.";
      await refresh();
    } catch (cause) {
      error = `Could not revoke consumer key: ${(cause as Error).message}`;
    } finally {
      busy = false;
    }
  }

  onMount(() => {
    void refresh();
  });
</script>

<div class="intro">
  <p class="eyebrow">Jetstream access</p>
  <h1>Consumer API keys</h1>
  <p>
    Issue keys for Jetstream archive replay. Each key has its own request and
    download limits. Live event subscriptions remain public.
  </p>
</div>

<section class="key-section" aria-labelledby="create-key-heading">
  <h2 id="create-key-heading">Create a consumer key</h2>
  <p class="section-copy">Set a name and owner so this key can be identified and revoked later.</p>
  <form class="key-form" onsubmit={create}>
    <div class="field">
      <label for="key-name">Key name</label>
      <input
        id="key-name"
        bind:this={nameInput}
        bind:value={name}
        placeholder="Example: indexer-prod"
        autocomplete="off"
        maxlength="120"
        required
        disabled={busy || !!token}
      />
      <small>Use a name that identifies the consumer.</small>
    </div>
    <div class="field">
      <label for="key-owner">Owner</label>
      <input
        id="key-owner"
        bind:value={owner}
        list="archive-key-owners"
        placeholder="Team or service owner"
        autocomplete="off"
        maxlength="120"
        required
        disabled={busy || !!token}
      />
      <datalist id="archive-key-owners">
        {#each owners as existingOwner}<option value={existingOwner}></option>{/each}
      </datalist>
      <small>Type a new owner or choose one used by another key.</small>
    </div>
    <div class="field">
      <label for="key-requests">Requests/minute</label>
      <input
        id="key-requests"
        type="number"
        min="1"
        max="100000"
        step="1"
        bind:value={requestsPerMinute}
        required
        disabled={busy || !!token}
      />
      <small>Maximum archive requests each minute.</small>
    </div>
    <div class="field">
      <label for="key-archive-mb">Archive MB/minute</label>
      <input
        id="key-archive-mb"
        type="number"
        min="1"
        max="100000"
        step="1"
        bind:value={archiveMegabytesPerMinute}
        required
        disabled={busy || !!token}
      />
      <small>Maximum archive download, in decimal MB.</small>
    </div>
    <div class="form-action">
      <button class="primary" disabled={busy || !!token}>
        {busy ? "Creating…" : "Create key"}
      </button>
      {#if token}<small>Dismiss the displayed key before creating another.</small>{/if}
    </div>
  </form>
</section>

{#if token}
  <section class="key-reveal" aria-labelledby="new-archive-key" aria-live="polite">
    <h2 id="new-archive-key" tabindex="-1">Copy this key now</h2>
    <p>
      This is the only time the key for <strong>{tokenName}</strong> will be shown.
      Copy it and store it securely before dismissing it.
    </p>
    <div class="key-reveal-actions">
      <input aria-label="New consumer API key" readonly value={token} onclick={(event) => event.currentTarget.select()} />
      <button class="primary" type="button" onclick={copyToken}>Copy key</button>
      <button type="button" onclick={dismissToken}>Dismiss</button>
    </div>
  </section>
{/if}

{#if error}<p class="error" role="alert">{error}</p>{/if}
{#if notice}<p class="notice" role="status">{notice}</p>{/if}

<section class="key-inventory" aria-labelledby="existing-key-heading">
  <h2 id="existing-key-heading">Existing consumer keys</h2>
  <p class="section-copy">Review limits and revoke keys that should no longer access the archive.</p>
  {#if loading}
    <p role="status">Loading consumer keys…</p>
  {:else if !keys.length && !error}
    <p class="empty">No consumer keys yet. Create the first key above.</p>
  {:else if keys.length}
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th scope="col">Name</th>
          <th scope="col">Owner</th>
          <th scope="col">Key ID</th>
          <th scope="col">Requests</th>
          <th scope="col">Archive</th>
          <th scope="col">Created</th>
          <th scope="col">Status</th>
          <th scope="col">Actions</th>
        </tr></thead>
        <tbody>
          {#each keys as key (key.id)}
            <tr>
              <td><strong>{key.name}</strong></td>
              <td>{key.owner}</td>
              <td><code>{key.id}</code></td>
              <td>{key.requestsPerMinute.toLocaleString()} / min</td>
              <td>{key.archiveMegabytesPerMinute.toLocaleString()} MB / min</td>
              <td><time datetime={key.createdAt}>{new Date(key.createdAt).toLocaleString()}</time></td>
              <td><span class:revoked={!!key.revokedAt} class="key-status">{key.revokedAt ? "Revoked" : "Active"}</span></td>
              <td>
                {#if key.revokedAt}
                  <span class="unavailable">—</span>
                {:else if confirmingId === key.id}
                  <div class="revoke-confirmation">
                    <span>Revoke {key.name}?</span>
                    <button bind:this={confirmButton} type="button" disabled={busy} onclick={() => revoke(key.id)}>Confirm revoke</button>
                    <button type="button" disabled={busy} onclick={cancelRevoke}>Cancel</button>
                  </div>
                {:else}
                  <button type="button" disabled={busy} aria-label={`Revoke ${key.name}`} onclick={(event) => askToRevoke(key.id, event.currentTarget)}>Revoke</button>
                {/if}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</section>

<style>
  .key-section { border-bottom: 1px solid var(--color-ui-separator); padding-bottom: 1.7rem; }
  .section-copy { margin: -0.3rem 0 1.3rem; }
  .key-form { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 1.25rem 1.5rem; }
  .field { min-width: 0; }
  .field small { margin-top: 0.4rem; }
  .form-action { grid-column: 1 / -1; display: flex; align-items: center; gap: 1rem; flex-wrap: wrap; }
  .key-reveal { margin: 1.5rem 0 2rem; padding: 1.25rem; border: 1px solid var(--color-brand-accent); border-radius: var(--radius-brand); background: var(--color-brand-white); }
  .key-reveal h2 { color: var(--color-brand-accent); }
  .key-reveal-actions { display: flex; gap: 0.75rem; flex-wrap: wrap; align-items: stretch; }
  .key-reveal-actions input { flex: 1 1 320px; min-width: 0; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .key-inventory { margin-top: 2rem; }
  .key-inventory table { min-width: 1040px; }
  .key-inventory td { white-space: nowrap; }
  .key-inventory td:nth-child(1), .key-inventory td:nth-child(2) { white-space: normal; }
  .key-status { display: inline-flex; align-items: center; gap: 0.45rem; white-space: nowrap; }
  .key-status::before { content: ""; width: 0.5rem; height: 0.5rem; border-radius: 50%; background: #2c8e56; }
  .key-status.revoked::before { background: var(--color-ui-grey); }
  .revoke-confirmation { display: flex; align-items: center; gap: 0.4rem; flex-wrap: wrap; max-width: 360px; white-space: normal; }
  .revoke-confirmation span { width: 100%; }
  .unavailable { color: var(--color-ui-grey-dark); }
  @media (max-width: 1150px) {
    .key-form { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  }
  @media (max-width: 700px) {
    .key-form { grid-template-columns: 1fr; }
    .form-action { grid-column: auto; }
    .key-reveal-actions { display: grid; }
    .key-reveal-actions input { width: 100%; flex: auto; }
  }
</style>
