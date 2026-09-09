// Switzer is self-hosted for this app, but must not be redistributed in Git.
// See src/brand/README.md for the upstream license and update procedure.
import { createHash } from "node:crypto";
import { readFile, writeFile, mkdir } from "node:fs/promises";

const file = new URL(
  "../src/brand/assets/fonts/Switzer-Variable.woff2",
  import.meta.url,
);
const source =
  "https://cdn.fontshare.com/wf/HJHZ26OECMTXRH7JXPFC7EVIHDSLT2RA/LJRNLR7WCPF3PY3SZ7B2LHNUTQMFNCHL/4MCJYGQDIOOXHWSIIB2OYNDBEALJSOGN.woff2";
const checksum =
  "d1bf801ffb1a6096def70a7c532255722ad87d948b13a8a586e342f7091f8ee4";
const matches = (data) =>
  createHash("sha256").update(data).digest("hex") === checksum;
let cached;
try {
  cached = await readFile(file);
} catch (error) {
  if (error.code !== "ENOENT") throw error;
}
if (!cached || !matches(cached)) {
  const response = await fetch(source, { signal: AbortSignal.timeout(30000) });
  if (!response.ok)
    throw new Error(`Switzer download failed: HTTP ${response.status}`);
  const data = Buffer.from(await response.arrayBuffer());
  if (!matches(data))
    throw new Error(
      "Switzer checksum changed; review the font and license before updating the pin.",
    );
  await mkdir(new URL(".", file), { recursive: true });
  await writeFile(file, data);
}
