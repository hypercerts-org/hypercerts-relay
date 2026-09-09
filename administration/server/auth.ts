import {
  createHash,
  randomBytes,
  createCipheriv,
  createDecipheriv,
  timingSafeEqual,
} from "node:crypto";
import {
  NodeOAuthClient,
  requestLocalLock,
  type NodeSavedSession,
  type NodeSavedState,
} from "@atproto/oauth-client-node";
import type { Request, Response, NextFunction } from "express";
import { Store } from "./store.ts";
import { ApiError } from "./contracts.ts";

const hash = (v: string) => createHash("sha256").update(v).digest("hex");
const random = () => randomBytes(32).toString("base64url");
export interface Session {
  id: string;
  did: string;
  csrf: string;
  expires: number;
}
export interface OAuthProvider {
  authorize(
    input: string,
    options: { state: string; scope: string },
  ): Promise<URL>;
  callback(
    params: URLSearchParams,
  ): Promise<{ session: { did: string }; state?: string | null }>;
  revoke(did: string): Promise<void>;
}
export function oauthProvider(store: Store, base: string, key: Buffer) {
  const seal = (value: unknown) => {
    const iv = randomBytes(12);
    const cipher = createCipheriv("aes-256-gcm", key, iv);
    const data = Buffer.concat([
      cipher.update(JSON.stringify(value)),
      cipher.final(),
    ]);
    return Buffer.concat([iv, cipher.getAuthTag(), data]).toString("base64");
  };
  const unseal = <T>(raw: string): T => {
    const b = Buffer.from(raw, "base64");
    const cipher = createDecipheriv("aes-256-gcm", key, b.subarray(0, 12));
    cipher.setAuthTag(b.subarray(12, 28));
    return JSON.parse(
      Buffer.concat([cipher.update(b.subarray(28)), cipher.final()]).toString(),
    );
  };
  const storage = <T>(namespace: string) => ({
    async get(key: string) {
      const row = store.get<{ value: string; expires: number }>(namespace, key);
      if (!row) return undefined;
      if (row.expires < Date.now()) {
        store.del(namespace, key);
        return undefined;
      }
      return unseal<T>(row.value);
    },
    async set(key: string, value: T) {
      store.set(namespace, key, {
        value: seal(value),
        expires:
          Date.now() + (namespace === "oauth-state" ? 600_000 : 30 * 86400_000),
      });
    },
    async del(key: string) {
      store.del(namespace, key);
    },
  });
  const redirect = `${base}/auth/callback`;
  const local = new URL(base).protocol === "http:";
  const metadata = {
    client_id: local
      ? `http://localhost?redirect_uri=${encodeURIComponent(redirect)}&scope=atproto`
      : `${base}/oauth-client-metadata.json`,
    client_name: "Hypercerts Relay Administration",
    client_uri: base,
    redirect_uris: [redirect] as [string],
    scope: "atproto",
    grant_types: ["authorization_code", "refresh_token"] as [
      "authorization_code",
      "refresh_token",
    ],
    response_types: ["code"] as ["code"],
    token_endpoint_auth_method: "none" as const,
    application_type: "web" as const,
    dpop_bound_access_tokens: true,
  };
  return {
    provider: new NodeOAuthClient({
      requestLock: requestLocalLock,
      clientMetadata: metadata,
      stateStore: storage<NodeSavedState>("oauth-state"),
      sessionStore: storage<NodeSavedSession>("oauth-session"),
    }),
    metadata,
  };
}
export class Auth {
  constructor(
    readonly store: Store,
    readonly base: string,
    readonly provider: OAuthProvider,
  ) {}
  cookie(req: Request, name: string) {
    return req.headers.cookie
      ?.split(";")
      .map((x) => x.trim())
      .find((x) => x.startsWith(`${name}=`))
      ?.slice(name.length + 1);
  }
  options(maxAge: number) {
    return {
      httpOnly: true,
      secure: this.base.startsWith("https:"),
      sameSite: "lax" as const,
      path: "/",
      maxAge,
    };
  }
  session(req: Request): Session | undefined {
    const token = this.cookie(req, "relay_session");
    if (!token) return;
    const row = this.store.db
      .prepare("SELECT * FROM sessions WHERE id=? AND expires>?")
      .get(hash(token), Date.now()) as unknown as Session | undefined;
    if (
      !row ||
      !this.store.db
        .prepare("SELECT did FROM administrators WHERE did=?")
        .get(row.did)
    )
      return;
    return row;
  }
  create(did: string, res: Response) {
    if (
      !this.store.db
        .prepare("SELECT did FROM administrators WHERE did=?")
        .get(did)
    )
      throw new ApiError(403, "administrator_required");
    const token = random(),
      csrf = random();
    const session = {
      id: hash(token),
      did,
      csrf,
      expires: Date.now() + 8 * 3600_000,
    };
    this.store.transaction(() => {
      this.store.db
        .prepare("DELETE FROM sessions WHERE expires<=?")
        .run(Date.now());
      this.store.db
        .prepare("INSERT INTO sessions VALUES(?,?,?,?)")
        .run(session.id, did, csrf, session.expires);
      this.store.audit(did, "signed_in", {});
    });
    res.cookie("relay_session", token, this.options(8 * 3600_000));
    return session;
  }
  require = (req: Request, res: Response, next: NextFunction) => {
    try {
      const session = this.session(req);
      if (!session) throw new ApiError(401, "administrator_session_required");
      if (!["GET", "HEAD"].includes(req.method)) {
        const token = req.get("X-CSRF-Token") ?? "";
        if (
          req.get("Origin") !== this.base ||
          token.length !== session.csrf.length ||
          !timingSafeEqual(Buffer.from(token), Buffer.from(session.csrf))
        )
          throw new ApiError(403, "invalid_csrf");
      }
      res.locals.session = session;
      next();
    } catch (e) {
      next(e);
    }
  };
  async login(req: Request, res: Response) {
    if (req.get("Origin") !== this.base)
      throw new ApiError(403, "invalid_origin");
    const input = req.body?.handle;
    if (typeof input !== "string" || input.length > 300 || !input.trim())
      throw new ApiError(400, "handle_required");
    const binding = random();
    const url = await this.provider.authorize(input.trim(), {
      state: hash(binding),
      scope: "atproto",
    });
    res.cookie("relay_oauth", binding, this.options(600_000));
    res.json({ redirectUrl: url.toString() });
  }
  async callback(req: Request, res: Response) {
    const binding = this.cookie(req, "relay_oauth");
    if (!binding) throw new ApiError(400, "oauth_browser_binding_missing");
    const { session, state } = await this.provider.callback(
      new URL(req.originalUrl, this.base).searchParams,
    );
    res.clearCookie("relay_oauth", this.options(0));
    if (!state || state !== hash(binding))
      throw new ApiError(403, "oauth_browser_binding_invalid");
    this.create(session.did, res);
    res.redirect(303, "/");
  }
  logout(req: Request, res: Response) {
    const session = res.locals.session as Session;
    this.store.transaction(() => {
      this.store.db.prepare("DELETE FROM sessions WHERE id=?").run(session.id);
      this.store.audit(session.did, "signed_out", {});
    });
    res.clearCookie("relay_session", this.options(0));
    res.status(204).end();
  }
}
