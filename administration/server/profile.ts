import { isValidHandle } from "@atproto/syntax";

export interface AdministratorProfile {
  handle?: string | null;
  displayName?: string | null;
}
export interface IdentitySession {
  did: string;
  fetchHandler?(path: string, init?: RequestInit): Promise<Response>;
}
interface Resolver {
  resolveIdentity(
    did: string,
    options: { signal: AbortSignal; noCache: boolean },
  ): Promise<{ did: string; handle: string }>;
}

async function record(response: Response): Promise<{
  error?: string;
  uri?: string;
  value?: { displayName?: unknown };
}> {
  const reader = response.body?.getReader();
  if (!reader) throw new Error("Profile response has no body");
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.length;
      if (size > 64 * 1024) throw new Error("Profile response exceeds limit");
      chunks.push(value);
    }
    return JSON.parse(Buffer.concat(chunks).toString());
  } finally {
    await reader.cancel();
  }
}

// Public presentation only. The OAuth DID remains the authorization identity.
export async function administratorProfile(
  resolver: Resolver,
  session: IdentitySession,
): Promise<AdministratorProfile> {
  const controller = new AbortController();
  const signal = controller.signal;
  const profile: AdministratorProfile = {};
  const lookup = Promise.all([
    (async () => {
      try {
        const identity = await resolver.resolveIdentity(session.did, {
          signal,
          noCache: true,
        });
        if (identity.did === session.did)
          profile.handle =
            identity.handle !== "handle.invalid" &&
            isValidHandle(identity.handle)
              ? identity.handle
              : null;
      } catch {
        /* Keep previously saved presentation when identity resolution is unavailable. */
      }
    })(),
    (async () => {
      if (!session.fetchHandler) return;
      let missing = 0;
      for (const collection of [
        "app.certified.actor.profile",
        "app.bsky.actor.profile",
      ]) {
        try {
          const query = new URLSearchParams({
            repo: session.did,
            collection,
            rkey: "self",
          });
          const response = await session.fetchHandler(
            `/xrpc/com.atproto.repo.getRecord?${query}`,
            { signal, redirect: "error" },
          );
          if (!response.ok) {
            if (response.status === 400) {
              if ((await record(response)).error === "RecordNotFound")
                missing++;
            } else {
              await response.body?.cancel();
              if (response.status === 404) missing++;
            }
            continue;
          }
          const data = await record(response);
          if (data.uri !== `at://${session.did}/${collection}/self`) continue;
          if (!data.value || typeof data.value !== "object") continue;
          const name = data.value?.displayName;
          if (typeof name === "string" && name.trim()) {
            profile.displayName = name.trim().slice(0, 640);
            return;
          }
          if (name === undefined || typeof name === "string") missing++;
        } catch {
          /* Profile lookup must not prevent administrator sign-in. */
        }
      }
      if (missing === 2) profile.displayName = null;
    })(),
  ]);
  let timer: ReturnType<typeof setTimeout>;
  const deadline = new Promise<void>((resolve) => {
    timer = setTimeout(() => {
      controller.abort();
      resolve();
    }, 4000);
  });
  await Promise.race([lookup, deadline]);
  clearTimeout(timer!);
  return { ...profile };
}
