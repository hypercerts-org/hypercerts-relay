import { userInfo } from "node:os";
import { isValidDid } from "@atproto/syntax";
import { Store } from "./store.ts";

const [action, did] = process.argv.slice(2);
if (
  !["grant", "remove", "revoke"].includes(action) ||
  !isValidDid(did ?? "") ||
  !process.env.ADMIN_DATABASE
)
  throw Error(
    "Usage: ADMIN_DATABASE=/path/control.db npx tsx server/access.ts grant|remove|revoke did:...",
  );
const store = new Store(process.env.ADMIN_DATABASE);
store.transaction(() => {
  if (action === "grant")
    store.db.prepare("INSERT OR IGNORE INTO administrators VALUES(?)").run(did);
  else {
    if (action === "remove")
      store.db.prepare("DELETE FROM administrators WHERE did=?").run(did);
    store.revokeSessions(did);
  }
  store.audit(`local:${userInfo().username}`, `administrator_${action}`, {
    did,
  });
});
store.close();
console.log(`Administrator access ${action} recorded.`);
