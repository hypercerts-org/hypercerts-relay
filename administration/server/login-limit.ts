import type { RequestHandler } from "express";
import { isIP } from "node:net";
import { ApiError } from "./contracts.ts";

export interface ProxyOptions {
  trustProxy?: string[];
  railway?: boolean;
}
// Railway overwrites X-Real-IP at its public edge. Only accept it from a trusted
// immediate proxy, never from a direct connection or an arbitrary forwarded chain.
export function railwayClientIP(options: ProxyOptions): RequestHandler {
  return (req, _res, next) => {
    if (
      options.railway &&
      req.app.get("trust proxy fn")(req.socket.remoteAddress, 0)
    ) {
      const ip = req.get("X-Real-IP");
      delete req.headers["x-forwarded-for"];
      if (ip && isIP(ip)) req.headers["x-forwarded-for"] = ip;
    }
    next();
  };
}
export function loginLimit(capacity = 10_000, now = Date.now): RequestHandler {
  const clients = new Map<string, { count: number; until: number }>();
  return (req, _res, next) => {
    const time = now();
    for (const [key, value] of clients)
      if (value.until <= time) clients.delete(key);
    const key = req.ip ?? "unknown";
    let entry = clients.get(key);
    if (!entry) {
      if (clients.size >= capacity)
        clients.delete(clients.keys().next().value!);
      entry = { count: 0, until: time + 60_000 };
      clients.set(key, entry);
    }
    if (++entry.count > 10)
      return next(new ApiError(429, "try_sign_in_again_later"));
    next();
  };
}
