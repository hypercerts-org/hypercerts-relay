<script lang="ts">
  import { tick } from "svelte";
  import State from "./State.svelte";
  import { directoryMessage, crawlSummary } from "./collectionDirectory";
  import type {
    Command,
    CollectionCrawl,
    CollectionDirectoryStatus,
  } from "./api";
  export let directory: CollectionDirectoryStatus | null = null;
  export let crawls: CollectionCrawl[] = [];
  export let hasMore = false;
  export let more: () => Promise<void> = async () => {};
  export let submit: (command: Command) => Promise<void>;
  export let busy = false;
  let confirming = false;
  let confirmButton: HTMLButtonElement;
  let startButton: HTMLButtonElement;
  $: blocked = directoryMessage(directory);
  $: activeByPDS = new Map(
    (directory?.active ?? []).map((crawl) => [crawl.pds, crawl]),
  );
  function time(value: string | null) {
    return value ? new Date(value).toLocaleString() : "Not recorded";
  }
  async function askConfirmation() {
    confirming = true;
    await tick();
    confirmButton.focus();
  }
  async function crawlAll() {
    confirming = false;
    await submit({ kind: "collection_crawl", scope: "all" });
    await tick();
    startButton?.focus();
  }
</script>

<div class="intro">
  <p class="eyebrow">Repository discovery</p>
  <h1>Collection directory</h1>
  <p>
    Ask the collection directory to crawl PDS sources for repositories and
    their collections. Results are candidate DID/collection pairs, not an
    authoritative inventory, and do not replace Jetstream backfill.
  </p>
</div>
{#if blocked}<p class="error" role="status" id="directory-blocked">
    {blocked}
  </p>{/if}
<div class="actions">
  <button
    class="primary"
    bind:this={startButton}
    disabled={busy || !!blocked || confirming}
    aria-describedby={blocked ? "directory-blocked" : undefined}
    onclick={askConfirmation}>Crawl all enabled sources</button
  >
</div>
{#if confirming}
  <section class="editor crawl-confirmation" aria-label="Confirm crawl">
    <h2>Crawl all enabled sources?</h2>
    <p>
      Every enabled, approved HTTPS source is sent to the collection directory.
      Each PDS then receives repository listing and description requests at the
      directory's crawl rate. Sources already being crawled are not crawled
      twice.
    </p>
    <div class="actions">
      <button bind:this={confirmButton} disabled={busy} onclick={crawlAll}
        >Confirm crawl</button
      >
      <button
        disabled={busy}
        onclick={() => {
          confirming = false;
          startButton?.focus();
        }}>Cancel</button
      >
    </div>
  </section>
{/if}
<h2>Active crawls</h2>
<!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
<div
  class="table-wrap"
  role="region"
  aria-label="Scrollable active crawls"
  tabindex="0"
>
  <table>
    <caption>Active crawls</caption><thead
      ><tr
        ><th>Source</th><th>Started</th><th>Repositories described</th><th
          >Could not be described</th
        ></tr
      ></thead
    ><tbody>
      {#each directory?.active ?? [] as crawl (crawl.pds)}<tr
          ><td>{crawl.pds}</td><td>{time(crawl.startedAt)}</td><td
            >{crawl.reposDescribed}</td
          ><td>{crawl.describeErrors ?? "Not reported"}</td></tr
        >
      {:else}<tr
          ><td colspan="4" class="empty"
            >{directory?.available
              ? "No crawls are running."
              : "Unavailable while the collection directory cannot be reached."}</td
          ></tr
        >{/each}
    </tbody>
  </table>
</div>
<h2>Crawl requests</h2>
<!-- svelte-ignore a11y_no_noninteractive_tabindex (Scrollable tables need keyboard access; verified with axe and Chromium.) -->
<div
  class="table-wrap"
  role="region"
  aria-label="Scrollable crawl requests"
  tabindex="0"
>
  <table>
    <caption>Crawl requests</caption><thead
      ><tr
        ><th>Source</th><th>Requested by</th><th>Requested at</th><th
          >Submission</th
        ><th>State</th><th>Result</th></tr
      ></thead
    ><tbody>
      {#each crawls as row (row.id)}<tr
          ><td>{row.pds}</td><td
            >{row.actorHandle ? `@${row.actorHandle}` : row.actor}</td
          ><td>{time(row.requestedAt)}</td><td
            >{row.submission === "already_active"
              ? "Already running"
              : row.submission === "started"
                ? "Started"
                : "Accepted"}{#if row.crawlStartedAt}<small
                >Crawl started {time(row.crawlStartedAt)}</small
              >{/if}</td
          ><td><State value={row.state} /></td><td
            >{crawlSummary(row, activeByPDS.get(row.pds))}</td
          ></tr
        >
      {:else}<tr
          ><td colspan="6" class="empty">No crawl requests recorded.</td></tr
        >{/each}
    </tbody>
  </table>
</div>
{#if hasMore}<div class="actions">
    <button onclick={() => void more()}>Load more crawl requests</button>
  </div>{/if}
<p class="note">
  Accepted means the collection directory took the request; it is not
  completion. Finished means the repository listing ended; repositories that
  could not be described are counted separately. Outcomes are lost if the
  collection directory restarts.
</p>

<style>
  .crawl-confirmation {
    margin-top: 1.5rem;
    padding-top: 1.5rem;
    border-top: 1px solid var(--color-ui-separator);
  }
</style>
