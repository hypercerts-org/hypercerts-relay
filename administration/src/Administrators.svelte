<script lang="ts">
  import { tick } from "svelte";
  import { isValidDid } from "@atproto/syntax";
  import { api } from "./api";
  export let rows: { did: string }[] = [];
  export let currentDid: string;
  export let csrf: string;
  export let onChanged: () => Promise<void>;
  let did = "",
    removing = "",
    busy = false,
    error = "",
    notice = "";
  let input: HTMLInputElement;
  let confirmButton: HTMLButtonElement;
  let removeButton: HTMLButtonElement | undefined;
  async function confirmRemoval(target: string, button: HTMLButtonElement) {
    removing = target;
    removeButton = button;
    error = "";
    notice = "";
    await tick();
    confirmButton.focus();
  }
  async function change(target: string, action: "grant" | "remove") {
    error = "";
    notice = "";
    if (!isValidDid(target)) {
      error = "Enter a valid DID, such as did:plc:…; handles are not accepted.";
      input.focus();
      return;
    }
    busy = true;
    try {
      await api("/administrators", { did: target, action }, csrf);
      notice =
        action === "grant"
          ? `Administrator access granted to ${target}.`
          : `Administrator access removed for ${target}. Their sessions have been revoked.`;
      if (action === "grant") did = "";
      removing = "";
      await onChanged();
      await tick();
    } catch (e) {
      error = (e as Error).message;
    } finally {
      busy = false;
      await tick();
      if (!error) input.focus();
    }
  }
</script>

<div class="intro">
  <h1>Administrators</h1>
  <p>
    Administrators can manage access and change Relay and Jetstream policies.
    The initial administrator has the same access as everyone else.
  </p>
</div>
<form
  class="editor"
  onsubmit={(event) => {
    event.preventDefault();
    void change(did.trim(), "grant");
  }}
>
  <label for="administrator-did">Administrator DID</label>
  <input
    id="administrator-did"
    bind:this={input}
    bind:value={did}
    disabled={busy}
    required
    maxlength="2048"
    spellcheck="false"
    aria-describedby="administrator-help administrator-feedback"
  />
  <p id="administrator-help" class="note">
    Use the account’s full DID, not its handle. Grant access only to someone who
    should control these services.
  </p>
  <button class="primary" disabled={busy}
    >{busy ? "Saving…" : "Grant administrator access"}</button
  >
</form>
<div id="administrator-feedback">
  {#if error}<p class="error" role="alert">{error}</p>{/if}
  {#if notice}<p class="notice" role="status">{notice}</p>{/if}
</div>
{#if removing}
  <section class="editor" aria-label="Confirm administrator removal">
    <h2>Remove administrator access?</h2>
    <p>
      <code>{removing}</code> will lose access immediately, including all current
      sessions.
    </p>
    <div class="actions">
      <button
        bind:this={confirmButton}
        disabled={busy}
        onclick={() => change(removing, "remove")}>Confirm removal</button
      >
      <button
        disabled={busy}
        onclick={() => {
          removing = "";
          removeButton?.focus();
        }}>Cancel</button
      >
    </div>
  </section>
{/if}
<ul class="administrators">
  {#each rows as row (row.did)}
    <li>
      <code>{row.did}</code>
      {#if row.did === currentDid}<span>You</span>
      {:else}<button
          disabled={busy}
          aria-label={`Remove administrator ${row.did}`}
          onclick={(event) => confirmRemoval(row.did, event.currentTarget)}
          >Remove access</button
        >{/if}
    </li>
  {/each}
</ul>
<p class="note">
  Ask another administrator to remove your own access. Removing the initial
  administrator is permanent across restarts.
</p>

<style>
  .administrators {
    list-style: none;
    padding: 0;
  }
  li {
    display: flex;
    align-items: center;
    gap: 1rem;
    flex-wrap: wrap;
    padding: 1rem 0;
    border-bottom: 1px solid var(--border, #d8ddd9);
  }
  li code {
    flex: 1;
    min-width: 0;
    overflow-wrap: anywhere;
  }
  code {
    overflow-wrap: anywhere;
  }
</style>
