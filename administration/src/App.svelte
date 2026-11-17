<script lang="ts">
  import { onMount } from "svelte";
  import Administrators from "./Administrators.svelte";
  import Sources from "./Sources.svelte";
  import CollectionDirectory from "./CollectionDirectory.svelte";
  import Collections from "./Collections.svelte";
  import Operations from "./Operations.svelte";
  import Limits from "./Limits.svelte";
  import ArchiveKeys from "./ArchiveKeys.svelte";
  import DiscoverySettings from "./DiscoverySettings.svelte";
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
    type CoveragePage,
    type Audit,
    type Limit,
    type Administrator,
    type CollectionCrawl,
    type CollectionDirectoryStatus,
    type CollectionBootstrapView,
  } from "./api";
  import {
    bootstrapNotice,
    createDirectoryPoller,
    crawlNotice,
    crawlsInProgress,
    refreshCrawls,
    type CrawlPage,
    type DirectoryPoller,
  } from "./collectionDirectory";
  const navigation = [
    { label: "", items: [["overview", "Overview"]] },
    {
      label: "Relay",
      items: [
        ["sources", "PDS sources"],
        ["limits", "Rate limits"],
      ],
    },
    {
      label: "Jetstream",
      items: [
        ["collections", "Collections"],
        ["jobs", "Backfill jobs"],
        ["coverage", "Coverage"],
        ["archive-keys", "Consumer API keys"],
      ],
    },
    {
      label: "Discovery",
      items: [["collection-directory", "Collection directory"]],
    },
    {
      label: "Administration",
      items: [
        ["audit", "Audit log"],
        ["manage-adminis", "Administrators"],
      ],
    },
  ];
  const navigationItems = navigation.flatMap((group) => group.items);
  const route = location.pathname.split("/")[1] || "overview";
  const screen = ["administrators", "administrations"].includes(route)
    ? "manage-adminis"
    : route === "changes"
      ? "audit"
      : navigationItems.some(([key]) => key === route)
        ? route
        : "overview";
  let session: (Administrator & { csrf: string; expires: number }) | null =
      null,
    ready = false,
    loading = false,
    busy = false,
    error = "",
    loadError = "",
    notice = "";
  let administrators: Administrator[] = [];
  let sourcesComponent: {
    refreshDetails: () => Promise<void>;
    refreshCrawlDetail: () => Promise<void>;
  } | null = null;
  let policyLoaded = false;
  let sources: Source[] = [],
    policy: Policy = { revision: 1, collections: [] },
    jobs: Job[] = [],
    changes: Operation[] = [],
    coverage: (CoveragePage & { next: string | null }) | null = null,
    audit: Audit[] = [],
    limits: Limit[] = [],
    globalLimits: Limit[] = [];
  let nextOperationAudit: string | null = null,
    nextHistoryAudit: string | null = null;
  let directory: CollectionDirectoryStatus | null = null,
    crawls: CollectionCrawl[] = [],
    bootstraps: CollectionBootstrapView[] = [],
    nextCrawls: string | null = null,
    olderCrawlsLoaded = false,
    poller: DirectoryPoller | null = null;
  const directoryScreen = ["sources", "collection-directory"].includes(screen);
  let status: {
      relay: string;
      jetstream: string;
      collectiondir?: string;
      publicServiceOrigins: {
        relay?: string;
        rainbow?: string;
        jetstream?: string;
      };
      observedAt: string;
    } | null = null,
    operation: Operation | null = null;
  const pdsFilter = new URLSearchParams(location.search).get("pds") ?? "";
  let cursor = "",
    next: string | null = null,
    previous: string[] = [];
  async function load() {
    loading = true;
    try {
      switch (screen) {
        case "manage-adminis": {
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
          sources = r.Sources.map((source) => ({ ...source }));
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
          const r = await api<CoveragePage & { next: string | null }>(
            `/coverage?after=${encodeURIComponent(cursor)}&pds=${encodeURIComponent(pdsFilter)}`,
          );
          coverage = r;
          next = r.next;
          break;
        }
        case "limits": {
          const [limitPage, sourcePage] = await Promise.all([
            api<{ items: Limit[]; next: string | null }>(
              `/limits?after=${encodeURIComponent(cursor)}`,
            ),
            api<{ Sources: Source[] }>("/sources"),
          ]);
          limits = limitPage.items;
          const pageGlobals = limitPage.items.filter(
            (limit) => limit.scope === "global",
          );
          if (!cursor || pageGlobals.length) globalLimits = pageGlobals;
          sources = sourcePage.Sources;
          next = limitPage.next;
          break;
        }
        case "collection-directory":
          next = null;
          break;
        case "audit": {
          const [operationPage, auditPage] = await Promise.all([
            api<{ items: Operation[]; next: string | null }>("/operations"),
            api<{ items: Audit[]; next: string | null }>("/audit"),
          ]);
          changes = operationPage.items;
          audit = auditPage.items;
          nextOperationAudit = operationPage.next;
          nextHistoryAudit = auditPage.next;
          next = null;
          break;
        }
        default:
          status = await api("/status");
      }
      if (screen === "sources") await sourcesComponent?.refreshDetails();
      if (operation)
        operation = await api<Operation>(`/operations/${operation.id}`);
      loadError = "";
    } catch (e) {
      loadError = (e as Error).message;
    } finally {
      loading = false;
    }
    // Not awaited: actions and page loads never wait on the directory.
    if (directoryScreen) void refreshDirectoryQuietly();
  }
  // Collection directory problems are shown on the page, never as a load
  // failure: Relay source management must keep working without it.
  async function refreshDirectoryQuietly() {
    try {
      await refreshDirectory(true);
    } catch {}
    poller?.update();
  }
  // Page loads and the poller can refresh concurrently; only the newest
  // refresh may change state, so a slower, older response (or failure) is
  // dropped instead of overwriting it.
  let directoryRequest = 0;
  async function refreshDirectory(showFailure = false) {
    const request = ++directoryRequest;
    const keepOlder = olderCrawlsLoaded;
    let directoryStatus: CollectionDirectoryStatus;
    let crawlPage: CrawlPage;
    let bootstrapPage: { items: CollectionBootstrapView[]; next: string | null };
    try {
      [directoryStatus, crawlPage, bootstrapPage] = await Promise.all([
        api<CollectionDirectoryStatus>("/collection-directory/status"),
        refreshCrawls(crawls, keepOlder, (path) => api<CrawlPage>(path)),
        api<{ items: CollectionBootstrapView[]; next: string | null }>(
          "/collection-directory/bootstraps?limit=100",
        ),
      ]);
    } catch (error) {
      if (request !== directoryRequest) return;
      if (showFailure)
        directory = {
          configured: true,
          available: false,
          error: "status_request_failed",
          observedAt: new Date().toISOString(),
          processStartedAt: null,
          historySupported: false,
          active: [],
          recent: [],
        };
      throw error;
    }
    if (request !== directoryRequest) return;
    directory = directoryStatus;
    bootstraps = bootstrapPage.items;
    crawls = crawlPage.items;
    if (!keepOlder) nextCrawls = crawlPage.next;
  }
  async function moreCrawls() {
    if (!nextCrawls) return;
    const request = ++directoryRequest;
    try {
      const olderPage = await api<{
        items: CollectionCrawl[];
        next: string | null;
      }>(
        `/collection-directory/crawls?limit=100&after=${encodeURIComponent(nextCrawls)}`,
      );
      if (request !== directoryRequest) return;
      const known = new Set(crawls.map((row) => row.id));
      crawls = [
        ...crawls,
        ...olderPage.items.filter((row) => !known.has(row.id)),
      ];
      nextCrawls = olderPage.next;
      olderCrawlsLoaded = true;
      poller?.update();
    } catch (e) {
      if (request === directoryRequest) error = (e as Error).message;
    }
  }
  function crawlActive() {
    return (
      directoryScreen &&
      crawlsInProgress(directory, crawls, operation, bootstraps)
    );
  }
  $: crawlMessage = crawlNotice(operation, crawls);
  $: bootstrapMessage = bootstrapNotice(operation);
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
  async function loadMoreAudit(kind: "operations" | "history") {
    if (loading) return;
    const after = kind === "operations" ? nextOperationAudit : nextHistoryAudit;
    if (!after) return;
    loading = true;
    try {
      if (kind === "operations") {
        const page = await api<{ items: Operation[]; next: string | null }>(
          `/operations?after=${encodeURIComponent(after)}`,
        );
        changes = [...changes, ...page.items];
        nextOperationAudit = page.next;
      } else {
        const page = await api<{ items: Audit[]; next: string | null }>(
          `/audit?after=${encodeURIComponent(after)}`,
        );
        audit = [...audit, ...page.items];
        nextHistoryAudit = page.next;
      }
      loadError = "";
    } catch (e) {
      loadError = (e as Error).message;
    } finally {
      loading = false;
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
    poller = createDirectoryPoller(async () => {
      // Follow a pending crawl request or source enable first, so one that
      // finishes during this poll is reflected in the directory snapshot read
      // below (a source enable queues the initial crawl).
      const watched = operation;
      if (
        (watched?.command.kind === "collection_crawl" ||
          watched?.command.kind === "source") &&
        ["requested", "applying"].includes(watched.state)
      ) {
        const latest = await api<Operation>(`/operations/${watched.id}`);
        // Keep an operation the operator submitted while this was in flight.
        if (operation?.id === watched.id) operation = latest;
      }
      await refreshDirectory();
      await sourcesComponent?.refreshCrawlDetail();
    }, crawlActive);
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
    return () => {
      active = false;
      poller?.stop();
    };
  });
</script>

<svelte:head
  ><title
    >{navigationItems.find(([key]) => key === screen)?.[1]} · Hypercerts Relay</title
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
    <h1>Relay administration</h1>
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
        {#each navigation as group}
          <div class:nav-group={Boolean(group.label)}>
            {#if group.label}<p>{group.label}</p>{/if}
            {#each group.items as [key, label]}<a
                href={key === "overview" ? "/" : `/${key}`}
                aria-current={screen === key ? "page" : undefined}>{label}</a
              >{/each}
          </div>
        {/each}
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
          {#if loading}<span class="refresh-status" role="status"
              >Refreshing…</span
            >{/if}<button
            disabled={loading}
            onclick={() => {
              error = "";
              void load();
            }}>Refresh</button
          >
        </div>
      </header>
      {#if error || loadError}<div class="error" role="alert">
          {error || loadError}. Check the service connection or refresh to try
          again.
        </div>{/if}
      {#if notice || operation}<div class="notice" role="status">
          {notice}{#if operation}<div>
              <State value={operation.state} />{operation.error?.replaceAll(
                "_",
                " ",
              ) ?? ""} <a href="/audit">View audit log</a>
            </div>{#if crawlMessage}<p>{crawlMessage}</p>{/if}{#if bootstrapMessage}<p
                >{bootstrapMessage.text}{#if bootstrapMessage.queued}
                  <a href="/collection-directory">View collection directory</a
                  >{/if}
              </p>{/if}{/if}
        </div>{/if}
      {#if screen === "manage-adminis"}<Administrators
          rows={administrators}
          currentDid={session.did}
          csrf={session.csrf}
          onChanged={load}
        />
      {:else if screen === "sources"}<Sources
          bind:this={sourcesComponent}
          rows={sources}
          {submit}
          {busy}
          {directory}
          {crawls}
          {action}
        /><DiscoverySettings csrf={session.csrf} />
      {:else if screen === "collection-directory"}<CollectionDirectory
          {directory}
          {crawls}
          {bootstraps}
          hasMore={!!nextCrawls}
          more={moreCrawls}
          {submit}
          {busy}
        />
      {:else if screen === "collections"}{#if policyLoaded}<Collections
            {policy}
            {submit}
            {busy}
          />{/if}
      {:else if screen === "archive-keys"}<ArchiveKeys csrf={session.csrf} />
      {:else if screen === "limits"}<Limits
          rows={limits}
          {globalLimits}
          {sources}
          {submit}
          {busy}
        />
      {:else if ["jobs", "coverage", "audit"].includes(screen)}<Operations
          {screen}
          {jobs}
          {changes}
          {coverage}
          {audit}
          {submit}
          {action}
          moreChanges={() => loadMoreAudit("operations")}
          moreAudit={() => loadMoreAudit("history")}
          hasMoreChanges={!!nextOperationAudit}
          hasMoreAudit={!!nextHistoryAudit}
          {busy}
        />
      {:else}<div class="intro">
          <p class="eyebrow">Service health</p>
          <h1>Relay operations</h1>
          <p>
            Manage acquisition across Relay and Jetstream. Start with source
            health, then inspect backfills and coverage.
          </p>
        </div>
        <section class="overview">
          <h2>Service connections</h2>
          <dl>
            <div>
              <dt>Indigo Relay</dt>
              <dd><State value={status?.relay ?? "unknown"} /></dd>
            </div>
            <div>
              <dt>Jetstream</dt>
              <dd><State value={status?.jetstream ?? "unknown"} /></dd>
            </div>
            <div>
              <dt>Collection directory</dt>
              <dd><State value={status?.collectiondir ?? "unknown"} /></dd>
            </div>
            <div>
              <dt>Relay public endpoint</dt>
              <dd>
                {#if status?.publicServiceOrigins.relay}
                  <a href={status.publicServiceOrigins.relay}
                    >{status.publicServiceOrigins.relay}</a
                  >
                {:else}Not configured{/if}
              </dd>
            </div>
            <div>
              <dt>Rainbow public endpoint</dt>
              <dd>
                {#if status?.publicServiceOrigins.rainbow}
                  <a href={status.publicServiceOrigins.rainbow}
                    >{status.publicServiceOrigins.rainbow}</a
                  >
                {:else}Not configured{/if}
              </dd>
            </div>
            <div>
              <dt>Jetstream public endpoint</dt>
              <dd>
                {#if status?.publicServiceOrigins.jetstream}
                  <a href={status.publicServiceOrigins.jetstream}
                    >{status.publicServiceOrigins.jetstream}</a
                  >
                {:else}Not configured{/if}
              </dd>
            </div>
          </dl>
          <p class="note">
            {status
              ? `Observed ${new Date(status.observedAt).toLocaleString()}`
              : "Waiting for the private service APIs."}
          </p>
        </section>
        <section class="overview">
          <h2>Operate the relay</h2>
          <a class="workflow" href="/sources"
            ><strong>Inspect PDS sources</strong><span
              >Connection state, validation, account quota and the last durable
              cursor.</span
            ></a
          ><a class="workflow" href="/audit"
            ><strong>Open audit log</strong><span
              >Review requested policy and acknowledged service results.</span
            ></a
          ><a class="workflow" href="/coverage"
            ><strong>Check collection coverage</strong><span
              >See exact source and policy revisions, progress and incomplete
              conditions.</span
            ></a
          >
        </section>
      {/if}
      {#if !["overview", "collections", "archive-keys", "audit", "collection-directory"].includes(screen)}<nav
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
