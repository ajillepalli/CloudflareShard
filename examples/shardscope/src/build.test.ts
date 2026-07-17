/** build.test.ts — Shardscope "Build on it" panel backend tests. Two
 * layers, mirroring this repo's existing split (see play.test.ts's header
 * comment for the full rationale):
 *
 *   1. Unit tests against src/build.ts's pure generateScaffoldFiles()/
 *      crc32()/buildZip() functions directly — no Env, no gate, no worker —
 *      covering file-set accuracy (real service-binding block, real RPC
 *      method names, no secret substrings) and zip structural validity.
 *
 *   2. Route-level tests calling src/index.ts's default export's fetch()
 *      directly (no cloudflare:test/SELF) — see play.test.ts's header
 *      comment for why this is the correct in-process equivalent here too.
 */
import { describe, expect, it } from "vitest";
import worker from "./index";
import { buildZip, crc32, generateScaffoldFiles, type ScaffoldFile } from "./build";
import type { Env } from "./env";

function fakeEnv(): Env {
  return {
    ADMIN_TOKEN: "test-admin-token",
    SHARDSCOPE_GATE_TOKEN: "test-gate-token",
  } as unknown as Env;
}

function buildRequest(path: string, opts: { authorized?: boolean } = {}): Request<unknown, IncomingRequestCfProperties> {
  const headers: Record<string, string> = {};
  if (opts.authorized !== false) headers.authorization = "Bearer test-gate-token";
  return new Request(`https://shardscope.internal${path}`, { method: "GET", headers }) as Request<unknown, IncomingRequestCfProperties>;
}

// ============================================================================
// Zip parsing helper (test-only) — walks the End Of Central Directory record
// backward, then the central directory, then each local file header, to
// recover {path, content} pairs and verify every stored CRC-32 matches the
// extracted bytes. This is a real structural parse (signatures, offsets,
// sizes checked), not a trust-the-generator round-trip.
// ============================================================================

function readUint16LE(bytes: Uint8Array, offset: number): number {
  return bytes[offset] | (bytes[offset + 1] << 8);
}
function readUint32LE(bytes: Uint8Array, offset: number): number {
  return (bytes[offset] | (bytes[offset + 1] << 8) | (bytes[offset + 2] << 16) | (bytes[offset + 3] << 24)) >>> 0;
}

function parseZip(bytes: Uint8Array): Array<{ path: string; content: string; storedCrc: number; computedCrc: number }> {
  // EOCD is the last 22 bytes here (this generator never writes a zip
  // comment), signature 0x06054b50.
  const eocdOffset = bytes.length - 22;
  expect(readUint32LE(bytes, eocdOffset)).toBe(0x06054b50);
  const entryCount = readUint16LE(bytes, eocdOffset + 10);
  const centralDirSize = readUint32LE(bytes, eocdOffset + 12);
  const centralDirOffset = readUint32LE(bytes, eocdOffset + 16);
  expect(eocdOffset).toBe(centralDirOffset + centralDirSize);

  const decoder = new TextDecoder();
  const results: Array<{ path: string; content: string; storedCrc: number; computedCrc: number }> = [];
  let pos = centralDirOffset;
  for (let i = 0; i < entryCount; i++) {
    expect(readUint32LE(bytes, pos)).toBe(0x02014b50);
    const storedCrc = readUint32LE(bytes, pos + 16);
    const compressedSize = readUint32LE(bytes, pos + 20);
    const nameLen = readUint16LE(bytes, pos + 28);
    const extraLen = readUint16LE(bytes, pos + 30);
    const commentLen = readUint16LE(bytes, pos + 32);
    const localHeaderOffset = readUint32LE(bytes, pos + 42);
    const name = decoder.decode(bytes.slice(pos + 46, pos + 46 + nameLen));
    pos += 46 + nameLen + extraLen + commentLen;

    // Follow the local header to extract the actual stored bytes.
    expect(readUint32LE(bytes, localHeaderOffset)).toBe(0x04034b50);
    expect(readUint16LE(bytes, localHeaderOffset + 8)).toBe(0); // compression method: stored
    const localNameLen = readUint16LE(bytes, localHeaderOffset + 26);
    const localExtraLen = readUint16LE(bytes, localHeaderOffset + 28);
    const dataStart = localHeaderOffset + 30 + localNameLen + localExtraLen;
    const contentBytes = bytes.slice(dataStart, dataStart + compressedSize);
    const computedCrc = crc32(contentBytes);

    results.push({ path: name, content: decoder.decode(contentBytes), storedCrc, computedCrc });
  }
  return results;
}

// ============================================================================
// Layer 1: pure generation + zip functions
// ============================================================================

describe("build.ts — generateScaffoldFiles()", () => {
  it("emits the expected file set", () => {
    const files = generateScaffoldFiles();
    const paths = files.map((f) => f.path).sort();
    expect(paths).toEqual(["README.md", "package.json", "schema.sql", "src/index.ts", "tsconfig.json", "wrangler.toml"].sort());
  });

  it("wrangler.toml contains the real service-binding block, copied from rpc-consumer's pattern", () => {
    const wrangler = generateScaffoldFiles().find((f) => f.path === "wrangler.toml")!.content;
    expect(wrangler).toContain('binding = "SHARD_API"');
    expect(wrangler).toContain('service = "cloudflare-shard-mvp"');
    expect(wrangler).toContain('entrypoint = "CloudflareShardRpc"');
    expect(wrangler).toContain("[[services]]");
  });

  it("src/index.ts references the real RPC method names with the real service-binding call shape", () => {
    const src = generateScaffoldFiles().find((f) => f.path === "src/index.ts")!.content;
    expect(src).toContain("env.SHARD_API.mutate(");
    expect(src).toContain("env.SHARD_API.tx(");
    expect(src).toContain("env.SHARD_API.tableScan(");
    expect(src).toContain("env.SHARD_API.adminCreateTable(");
    expect(src).toContain("env.SHARD_API.adminRegisterTenant(");
    // Real StructuredMutation/StructuredOperation field names (repo-root
    // src/structured-op.ts) — not invented ones.
    expect(src).toContain("partitionKey");
    expect(src).toContain("tenantId");
  });

  it("schema.sql matches the table + column shape src/index.ts's adminCreateTable call sends", () => {
    const schema = generateScaffoldFiles().find((f) => f.path === "schema.sql")!.content;
    const src = generateScaffoldFiles().find((f) => f.path === "src/index.ts")!.content;
    expect(schema).toContain("CREATE TABLE inventory_items");
    expect(schema).toContain("id TEXT PRIMARY KEY");
    expect(src).toContain("CREATE TABLE ${TABLE} (id TEXT PRIMARY KEY, sku TEXT, name TEXT, quantity INTEGER)");
  });

  it("contains no secret/token string in any generated file", () => {
    const files = generateScaffoldFiles();
    // The literal values this repo's own tests/dev setup use for
    // ADMIN_TOKEN / SHARDSCOPE_GATE_TOKEN — a regression that ever threaded
    // real Env into this pure generator would leak one of these into the
    // download; generateScaffoldFiles() takes no Env argument at all, so
    // this is also a structural (not just string-match) guarantee, but the
    // string check catches a future refactor that adds Env access.
    const forbidden = ["test-admin-token", "test-gate-token"];
    for (const file of files) {
      for (const secret of forbidden) {
        expect(file.content).not.toContain(secret);
      }
      // No hardcoded bearer-token-shaped literal (a real token, not the
      // word "tenantToken"/"adminToken" as an identifier/JSON key).
      expect(file.content).not.toMatch(/Bearer [A-Za-z0-9_-]{16,}/);
    }
  });

  it("is a pure function — calling it twice yields identical content (deterministic, no Env/clock coupling)", () => {
    expect(generateScaffoldFiles()).toEqual(generateScaffoldFiles());
  });
});

describe("build.ts — crc32()", () => {
  it("matches the standard CRC-32 check value for the ASCII string \"123456789\"", () => {
    // The canonical test vector every CRC-32 (polynomial 0xEDB88320)
    // implementation is checked against.
    const bytes = new TextEncoder().encode("123456789");
    expect(crc32(bytes)).toBe(0xcbf43926);
  });

  it("returns 0 for empty input", () => {
    expect(crc32(new Uint8Array(0))).toBe(0);
  });
});

describe("build.ts — buildZip()", () => {
  it("produces a structurally valid zip whose entries round-trip to the original files with matching CRC-32s", () => {
    const files = generateScaffoldFiles();
    const zipBytes = buildZip(files);
    const parsed = parseZip(zipBytes);

    expect(parsed.map((p) => p.path).sort()).toEqual(files.map((f) => f.path).sort());
    for (const entry of parsed) {
      const original = files.find((f) => f.path === entry.path) as ScaffoldFile;
      expect(entry.content).toBe(original.content);
      expect(entry.storedCrc).toBe(entry.computedCrc);
    }
  });

  it("starts with the local file header signature (PK\\x03\\x04)", () => {
    const zipBytes = buildZip(generateScaffoldFiles());
    expect(zipBytes[0]).toBe(0x50);
    expect(zipBytes[1]).toBe(0x4b);
    expect(zipBytes[2]).toBe(0x03);
    expect(zipBytes[3]).toBe(0x04);
  });

  it("handles an empty file list (zero entries, still a valid EOCD-only archive)", () => {
    const zipBytes = buildZip([]);
    expect(zipBytes.length).toBe(22);
    expect(parseZip(zipBytes)).toEqual([]);
  });
});

// ============================================================================
// Layer 2: route-level — the gate + wiring
// ============================================================================

describe("index.ts — /api/build/* gate + wiring", () => {
  it("GET /api/build/manifest rejects an unauthenticated request with 401", async () => {
    const res = await worker.fetch(buildRequest("/api/build/manifest", { authorized: false }), fakeEnv());
    expect(res.status).toBe(401);
  });

  it("GET /api/build/scaffold rejects an unauthenticated request with 401", async () => {
    const res = await worker.fetch(buildRequest("/api/build/scaffold", { authorized: false }), fakeEnv());
    expect(res.status).toBe(401);
  });

  it("authorized GET /api/build/manifest returns the same file set as generateScaffoldFiles()", async () => {
    const res = await worker.fetch(buildRequest("/api/build/manifest"), fakeEnv());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { files: ScaffoldFile[] };
    expect(body.files).toEqual(generateScaffoldFiles());
  });

  it("authorized GET /api/build/scaffold returns a downloadable zip with the right headers and a valid body", async () => {
    const res = await worker.fetch(buildRequest("/api/build/scaffold"), fakeEnv());
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("application/zip");
    expect(res.headers.get("content-disposition")).toBe('attachment; filename="cloudflareshard-inventory-starter.zip"');
    const bytes = new Uint8Array(await res.arrayBuffer());
    expect(bytes[0]).toBe(0x50);
    expect(bytes[1]).toBe(0x4b);
    const parsed = parseZip(bytes);
    expect(parsed.length).toBe(generateScaffoldFiles().length);
    for (const entry of parsed) expect(entry.storedCrc).toBe(entry.computedCrc);
  });
});
