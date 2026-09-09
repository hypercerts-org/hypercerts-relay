import { isValidDid } from "@atproto/syntax";
import { DatabaseSync } from "node:sqlite";
import { mkdirSync, chmodSync, openSync, closeSync, existsSync } from "node:fs";
import { dirname } from "node:path";
import { randomUUID } from "node:crypto";
import type { Command, Operation, OperationState } from "./contracts.ts";
import { ApiError } from "./contracts.ts";

export class Store {
  readonly db: DatabaseSync;
  constructor(path: string) {
    if (path !== ":memory:") {
      mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
      // Precreate privately before SQLite can write sessions or OAuth material.
      closeSync(openSync(path, "a", 0o600));
      secureDatabaseFiles(path);
    }
    this.db = new DatabaseSync(path);
    this.db
      .exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, actor TEXT NOT NULL, command TEXT NOT NULL, state TEXT NOT NULL, createdAt TEXT NOT NULL, updatedAt TEXT NOT NULL, result TEXT, error TEXT);
      CREATE TABLE IF NOT EXISTS audit (seq INTEGER PRIMARY KEY, operation TEXT, actor TEXT NOT NULL, action TEXT NOT NULL, time TEXT NOT NULL, detail TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS kv (namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(namespace,key));
      CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, did TEXT NOT NULL, csrf TEXT NOT NULL, expires INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS administrators (did TEXT PRIMARY KEY);
      CREATE INDEX IF NOT EXISTS operations_state ON operations(state,createdAt,id);
      CREATE INDEX IF NOT EXISTS operations_created ON operations(createdAt,id);`);
    if (path !== ":memory:") secureDatabaseFiles(path);
  }
  seedAdministrator(did?: string) {
    if (did !== undefined && !isValidDid(did))
      throw new Error("ADMIN_SEED_DID must be a valid DID");
    this.transaction(() => {
      if (this.get("bootstrap", "administrator")) return;
      // Existing membership or its audit history must never be overwritten by
      // first-run configuration, even if the last administrator was removed.
      const existing =
        this.db.prepare("SELECT did FROM administrators LIMIT 1").get() ||
        this.db
          .prepare(
            "SELECT seq FROM audit WHERE action IN ('administrator_grant', 'administrator_remove', 'administrator_seed') LIMIT 1",
          )
          .get();
      if (existing) {
        this.set("bootstrap", "administrator", { initialized: true });
      } else if (did) {
        this.db.prepare("INSERT INTO administrators VALUES(?)").run(did);
        this.audit("system:bootstrap", "administrator_seed", { did });
        this.set("bootstrap", "administrator", { initialized: true });
      }
    });
  }
  administrators(after: string, limit: number) {
    const rows = this.db
      .prepare(
        "SELECT did FROM administrators WHERE did>? ORDER BY did LIMIT ?",
      )
      .all(after, limit + 1) as { did: string }[];
    return {
      items: rows.slice(0, limit).map((row) => ({
        ...this.get<{ handle?: string | null; displayName?: string | null }>(
          "administrator-profile",
          row.did,
        ),
        did: row.did,
      })),
      next: rows.length > limit ? rows[limit - 1].did : null,
    };
  }
  changeAdministrator(actor: string, did: string, action: "grant" | "remove") {
    if (!isValidDid(did)) throw new ApiError(400, "invalid_administrator_did");
    this.transaction(() => {
      if (
        !this.db
          .prepare("SELECT did FROM administrators WHERE did=?")
          .get(actor)
      )
        throw new ApiError(403, "administrator_required");
      if (action === "remove" && did === actor)
        throw new ApiError(
          400,
          "ask_another_administrator_to_remove_your_access",
        );
      const result =
        action === "grant"
          ? this.db
              .prepare("INSERT OR IGNORE INTO administrators VALUES(?)")
              .run(did)
          : this.removeAdministrator(did);
      this.set("bootstrap", "administrator", { initialized: true });
      if (result.changes) this.audit(actor, `administrator_${action}`, { did });
    });
  }
  transaction<T>(f: () => T): T {
    this.db.exec("BEGIN IMMEDIATE");
    try {
      const value = f();
      this.db.exec("COMMIT");
      return value;
    } catch (e) {
      this.db.exec("ROLLBACK");
      throw e;
    }
  }
  // API and local recovery callers provide their own authorization, audit and transaction.
  removeAdministrator(did: string) {
    const result = this.db
      .prepare("DELETE FROM administrators WHERE did=?")
      .run(did);
    this.revokeSessions(did);
    this.del("administrator-profile", did);
    return result;
  }
  audit(
    actor: string,
    action: string,
    detail: unknown,
    operation: string | null = null,
  ) {
    this.db
      .prepare(
        "INSERT INTO audit(operation,actor,action,time,detail) VALUES(?,?,?,?,?)",
      )
      .run(
        operation,
        actor,
        action,
        new Date().toISOString(),
        JSON.stringify(detail),
      );
  }
  get<T>(namespace: string, key: string): T | undefined {
    const r = this.db
      .prepare("SELECT value FROM kv WHERE namespace=? AND key=?")
      .get(namespace, key);
    return r ? JSON.parse(r.value as string) : undefined;
  }
  set(namespace: string, key: string, value: unknown) {
    this.db
      .prepare(
        "INSERT INTO kv VALUES(?,?,?) ON CONFLICT(namespace,key) DO UPDATE SET value=excluded.value",
      )
      .run(namespace, key, JSON.stringify(value));
  }
  del(namespace: string, key: string) {
    this.db
      .prepare("DELETE FROM kv WHERE namespace=? AND key=?")
      .run(namespace, key);
  }
  request(actor: string, body: Command, id: string = randomUUID()): Operation {
    return this.transaction(() => {
      const previous = this.operation(id);
      if (previous) {
        if (
          previous.actor !== actor ||
          JSON.stringify(previous.command) !== JSON.stringify(body)
        )
          throw new ApiError(409, "idempotency_conflict");
        return previous;
      }
      const time = new Date().toISOString();
      this.db
        .prepare("INSERT INTO operations VALUES(?,?,?,?,?,?,NULL,NULL)")
        .run(id, actor, JSON.stringify(body), "requested", time, time);
      this.audit(actor, "requested", body, id);
      return this.operation(id)!;
    });
  }
  operation(id: string): Operation | undefined {
    const r = this.db.prepare("SELECT * FROM operations WHERE id=?").get(id);
    return r
      ? ({
          ...r,
          command: JSON.parse(r.command as string),
          result: r.result ? JSON.parse(r.result as string) : null,
        } as unknown as Operation)
      : undefined;
  }
  transition(
    id: string,
    state: OperationState,
    result: unknown = null,
    error: string | null = null,
    actor?: string,
    expected?: readonly OperationState[],
  ): boolean {
    return this.transaction(() => {
      const op = this.operation(id);
      if (!op) throw new ApiError(404, "operation_not_found");
      if (expected && !expected.includes(op.state)) return false;
      this.db
        .prepare(
          "UPDATE operations SET state=?,updatedAt=?,result=?,error=? WHERE id=?",
        )
        .run(
          state,
          new Date().toISOString(),
          JSON.stringify(result),
          error,
          id,
        );
      this.audit(actor ?? op.actor, state, { result, error }, id);
      return true;
    });
  }
  page(table: "operations" | "audit", after: string, limit: number) {
    if (table === "operations") {
      const [createdAt, id] = operationCursor(after);
      const rows = this.db
        .prepare(
          "SELECT id,createdAt FROM operations WHERE (createdAt,id)>(?,?) ORDER BY createdAt,id LIMIT ?",
        )
        .all(createdAt, id, limit + 1);
      return {
        items: rows.slice(0, limit).map((r) => this.operation(r.id as string)!),
        next:
          rows.length > limit
            ? `${rows[limit - 1].createdAt}|${rows[limit - 1].id}`
            : null,
      };
    }
    const rows = this.db
      .prepare("SELECT * FROM audit WHERE seq>? ORDER BY seq LIMIT ?")
      .all(Number(after || 0), limit + 1);
    return {
      items: rows
        .slice(0, limit)
        .map((r) => ({ ...r, detail: JSON.parse(r.detail as string) })),
      next: rows.length > limit ? String(rows[limit - 1].seq) : null,
    };
  }
  revokeSessions(did: string) {
    this.db.prepare("DELETE FROM sessions WHERE did=?").run(did);
    this.del("oauth-session", did);
  }
  close() {
    this.db.close();
  }
}

function secureDatabaseFiles(path: string) {
  // Existing parents may be shared mounts; never change their permissions.
  for (const file of [path, `${path}-wal`, `${path}-shm`])
    if (existsSync(file)) chmodSync(file, 0o600);
}
function operationCursor(after: string): [string, string] {
  if (!after) return ["", ""];
  const parts = after.split("|");
  if (
    parts.length !== 2 ||
    !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(parts[0]) ||
    !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(
      parts[1],
    ) ||
    !Number.isFinite(Date.parse(parts[0]))
  )
    throw new ApiError(400, "invalid_cursor");
  return [parts[0], parts[1]];
}
