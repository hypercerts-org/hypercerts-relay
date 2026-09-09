<script lang="ts">
  import { onMount } from "svelte";
  import Administrators from "./Administrators.svelte";
  import Sources from "./Sources.svelte";
  import Collections from "./Collections.svelte";
  import Operations from "./Operations.svelte";
  import Limits from "./Limits.svelte";
  import State from "./State.svelte";
  import logo from "./brand/assets/logo/hypercerts.svg";
  import {
    api,
    target,
    type Command,
    type Operation,
    type Source,
    type Policy,
    type Job,
    type Coverage,
    type Audit,
    type Limit,
    type Administrator,
  } from "./api";
  const navigation = [
    ["overview", "Overview"],
    ["sources", "PDS sources"],
    ["collections", "Collections"],
    ["jobs", "Backfill jobs"],
    ["coverage", "Coverage"],
    ["limits", "Rate limits"],
    ["changes", "Requested changes"],
    ["audit", "Audit history"],
    ["administrators", "Administrators"],
  ];
  const route = location.pathname.split("/")[1] || "overview";
  const screen = navigation.some(([key]) => key === route) ? route : "overview";
  let session: (Administrator & { csrf: string; expires: number }) | null =
      null,
    ready = false,
    loading = false,
    busy = false,
    error = "",
    notice = "";
  let administrators: Administrator[] = [];
  let policyLoaded = false;
  let sources: Source[] = [],
    policy: Policy = { revision: 1, collections: [] },
    jobs: Job[] = [],
    changes: Operation[] = [],
    coverage: Coverage[] = [],
    audit: Audit[] = [],
    limits: Limit[] = [];
  let status: { relay: string; jetstream: string; observedAt: string } | null =
      null,
    operation: Operation | null = null;
  const pdsFilter = new URLSearchParams(location.search).get("pds") ?? "";
  let cursor = "",
    next: string | null = null,
    previous: string[] = [];
  async function load() {
    loading = true;
    try {
      switch (screen) {
        case "administrators": {
          const r = await api<{
            items: Administrator[];
            next: string | null;
          }>(`/administrators?after=${encodeURIComponent(cursor)}`);
          administrators = r.items;
          next = r.next;
          break;
        }
        case "sources": {
          const r = await api<{ Sources: Source[]; NextAfterHostID: number }>(
            `/sources?after=${encodeURIComponent(cursor)}`,
          );
          sources = r.Sources;
          next = r.NextAfterHostID ? String(r.NextAfterHostID) : null;
          break;
        }
        case "collections":
          policy = await api<Policy>("/policy");
          policyLoaded = true;
          break;
        case "jobs": {
          const r = await api<{ jobs: Job[]; nextCursor?: string }>(
            `/jobs?after=${encodeURIComponent(cursor)}&pds=${encodeURIComponent(pdsFilter)}`,
          );
          jobs = r.jobs;
          next = r.nextCursor ?? null;
          break;
        }
        case "coverage": {
          const r = await api<{ items: Coverage[]; next: string | null }>(
            `/coverage?after=${encodeURIComponent(cursor)}&pds=${encodeURIComponent(pdsFilter)}`,
          );
          coverage = r.items;
          next = r.next;
          break;
        }
        case "limits": {
          const r = await api<{ items: Limit[]; next: string | null }>(
            `/limits?after=${encodeURIComponent(cursor)}`,
          );
          limits = r.items;
          next = r.next;
          break;
        }
        case "changes": {
          const r = await api<{ items: Operation[]; next: string | null }>(
            `/operations?after=${encodeURIComponent(cursor)}`,
          );
          changes = r.items;
          next = r.next;
          break;
        }
        case "audit": {
          const r = await api<{ items: Audit[]; next: string | null }>(
            `/audit?after=${encodeURIComponent(cursor)}`,
          );
          audit = r.items;
          next = r.next;
          break;
        }
        default:
          status = await api("/status");
      }
      if (operation)
        operation = await api<Operation>(`/operations/${operation.id}`);
    } catch (e) {
      error = (e as Error).message;
    } finally {
      loading = false;
    }
  }
  async function submit(command: Command) {
    busy = true;
    error = "";
    notice = "";
    try {
      operation = await api<Operation>(
        "/operations",
        command,
        session!.csrf,
        crypto.randomUUID(),
      );
      notice = `Change recorded: ${target(command)}`;
      await load();
    } catch (e) {
      error = (e as Error).message;
    } finally {
      busy = false;
    }
  }
  async function action(id: string, action: "retry" | "cancel") {
    busy = true;
    error = "";
    try {
      operation = await api<Operation>(
        `/operations/${id}/${action}`,
        {},
        session!.csrf,
      );
      await load();
    } catch (e) {
      error = (e as Error).message;
    } finally {
      busy = false;
    }
  }
  async function login(event: SubmitEvent) {
    event.preventDefault();
    busy = true;
    error = "";
    try {
      const form = event.currentTarget as HTMLFormElement;
      const response = await fetch("/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ handle: new FormData(form).get("handle") }),
      });
      const result = await response.json();
      if (!response.ok)
        throw new Error(result.error?.replaceAll("_", " ") ?? "Sign-in failed");
      location.assign(result.redirectUrl);
    } catch (e) {
      error = (e as Error).message;
      busy = false;
    }
  }
  async function signout(all = false) {
    try {
      await api(all ? "/revoke-sessions" : "/signout", {}, session!.csrf);
      session = null;
      operation = null;
    } catch (e) {
      error = (e as Error).message;
    }
  }
  onMount(() => {
    let active = true;
    void api<typeof session>("/session")
      .then(async (value) => {
        if (active) {
          session = value;
          ready = true;
          await load();
        }
      })
      .catch(() => {
        if (active) ready = true;
      });
    const timer = setInterval(() => {
      if (session && !loading && !busy) void load();
    }, 5000);
    return () => {
      active = false;
      clearInterval(timer);
    };
  });
</script>

<svelte:head
  ><title
    >{navigation.find(([key]) => key === screen)?.[1]} · Hypercerts Relay</title
  ></svelte:head
>
<a class="skip" href="#main">Skip to content</a>
{#if !ready}<main id="main" class="login">
    <p role="status">Checking your session…</p>
  </main>
{:else if !session}<main id="main" class="login">
    <div class="brand">
      <img src={logo} alt="Hypercerts" width="170" height="30" />
    </div>
    <p class="eyebrow">Operator access</p>
    <h1>Relay <em>administration</em></h1>
    <p>
      Sign in with your AT Protocol account. Access is limited to authorized
      administrators.
    </p>
    <form method="post" action="/auth/login" onsubmit={login}>
      <label for="handle">Handle or DID</label><input
        id="handle"
        name="handle"
        autocomplete="username"
        placeholder="operator.example"
        required
        maxlength="300"
      /><button class="primary" disabled={busy}
        >{busy ? "Opening sign-in…" : "Sign in with AT Protocol"}</button
      >
    </form>
    <p class="note">
      Use your account’s own authorization server. Service credentials stay on
      the server.
    </p>
    {#if error}<p role="alert" class="error">{error}</p>{/if}
  </main>
{:else}
  <div class="shell">
    <aside>
      <a class="brand" href="/" aria-label="Hypercerts Relay overview"
        ><img src={logo} alt="Hypercerts" width="170" height="30" /></a
      >
      <p class="product-label">Relay administration</p>
      <nav aria-label="Administration">
        {#each navigation as [key, label]}<a
            href={key === "overview" ? "/" : `/${key}`}
            aria-current={screen === key ? "page" : undefined}>{label}</a
          >{/each}
      </nav>
      <div class="account" aria-label="Signed-in account">
        <div class="account-identity">
          <small>Signed in as</small>
          <strong
            >{session.displayName ||
              (session.handle ? `@${session.handle}` : session.did)}</strong
          >
          {#if session.displayName && session.handle}<span
              >@{session.handle}</span
            >
          {:else if session.displayName}<span>{session.did}</span>{/if}
        </div>
        <div class="account-actions">
          <button onclick={() => signout()}>Sign out</button><button
            class="text-button"
            onclick={() => signout(true)}>Revoke my sessions</button
          >
        </div>
      </div>
    </aside>
    <main id="main">
      <header class="toolbar">
        <span>Administration</span>
        <div class="actions">
          <span class="refresh-status" role="status"
            >{loading ? "Refreshing…" : "Refreshes every 5 seconds"}</span
          ><button
            disabled={loading}
            onclick={() => {
              error = "";
              void load();
            }}>Refresh</button
          >
        </div>
      </header>
      {#if error}<div class="error" role="alert">
          {error}. Check the service connection or refresh to try again.
        </div>{/if}
      {#if notice || operation}<div class="notice" role="status">
          {notice}{#if operation}<div>
              <State value={operation.state} />{operation.error?.replaceAll(
                "_",
                " ",
              ) ?? ""} <a href="/changes">View requested changes</a>
            </div>{/if}
        </div>{/if}
      {#if screen === "administrators"}<Administrators
          rows={administrators}
          currentDid={session.did}
          csrf={session.csrf}
          onChanged={load}
        />
      {:else if screen === "sources"}<Sources rows={sources} {submit} {busy} />
      {:else if screen === "collections"}{#if policyLoaded}<Collections
            {policy}
            {submit}
            {busy}
          />{/if}
      {:else if screen === "limits"}<Limits rows={limits} {submit} {busy} />
      {:else if ["jobs", "coverage", "changes", "audit"].includes(screen)}<Operations
          {screen}
          {jobs}
          {changes}
          {coverage}
          {audit}
          {submit}
          {action}
          {busy}
        />
      {:else}<div class="intro">
          <p class="eyebrow">Service health</p>
          <h1>Relay <em>operations</em></h1>
          <p>
            Manage acquisition across Relay and Jetstream. Start with source
            health, then inspect backfills and coverage.
          </p>
        </div>
        <section class="overview">
          <h2>Service <em>connections</em></h2>
          <dl>
            <div>
              <dt>Indigo Relay</dt>
              <dd><State value={status?.relay ?? "unknown"} /></dd>
            </div>
            <div>
              <dt>Jetstream</dt>
              <dd><State value={status?.jetstream ?? "unknown"} /></dd>
            </div>
          </dl>
          <p class="note">
            {status
              ? `Observed ${new Date(status.observedAt).toLocaleString()}`
              : "Waiting for the private service APIs."}
          </p>
        </section>
        <section class="overview">
          <h2>Operate <em>the relay</em></h2>
          <a class="workflow" href="/sources"
            ><strong>Inspect PDS sources</strong><span
              >Connection state, validation, account quota and the last durable
              cursor.</span
            ></a
          ><a class="workflow" href="/changes"
            ><strong>Follow requested changes</strong><span
              >Separate requested policy from acknowledged service results.</span
            ></a
          ><a class="workflow" href="/coverage"
            ><strong>Check collection coverage</strong><span
              >See exact source and policy revisions, progress and incomplete
              conditions.</span
            ></a
          >
        </section>
      {/if}
      {#if !["overview", "collections"].includes(screen)}<nav
          class="pagination"
          aria-label="Result pages"
        >
          <button
            disabled={!previous.length || loading}
            onclick={() => {
              cursor = previous[previous.length - 1];
              previous = previous.slice(0, -1);
              void load();
            }}>Previous page</button
          ><span>Page {previous.length + 1}</span><button
            disabled={!next || loading}
            onclick={() => {
              previous = [...previous, cursor];
              cursor = next!;
              void load();
            }}>Next page</button
          >
        </nav>{/if}
      <footer>
        Raw stream: Relay · Collection archive and backfill: Jetstream
      </footer>
    </main>
  </div>
{/if}
