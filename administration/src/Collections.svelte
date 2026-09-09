<script lang="ts">
  import type { Policy, Command } from "./api";
  export let policy: Policy;
  export let submit: (command: Command) => Promise<void>;
  export let busy = false;
  let draft = policy.collections.join("\n");
  let revision = policy.revision;
  $: if (policy.revision !== revision) {
    draft = policy.collections.join("\n");
    revision = policy.revision;
  }
</script>

<div class="intro">
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
  onsubmit={(e) => {
    e.preventDefault();
    void submit({
      kind: "collections",
      expectedRevision: policy.revision,
      collections: draft.split(/\s+/).filter(Boolean),
    });
  }}
>
  <label for="collections">Enabled collection NSIDs</label><textarea
    id="collections"
    rows="10"
    bind:value={draft}
    spellcheck="false"
    aria-describedby="collection-help"></textarea>
  <p id="collection-help" class="note">
    One exact NSID per line, such as org.hypercerts.claim.activity. Wildcards
    are not supported. An empty list stops new record acquisition; previously
    archived rows remain.
  </p>
  <button class="primary" disabled={busy}>Request policy change</button>
</form>
