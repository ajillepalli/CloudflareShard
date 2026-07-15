import { env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import { hashKey } from "./hash";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";
import { decodeTableScanCursor, encodeTableScanCursor, mergeTableScanPages } from "./index";
import { isInternalTableName } from "./sql-safety";
import {
  AUTH,
  createIndexTestTable,
  findPartitionKeyPairOnDifferentShards,
  insertRowBypassingProvenance,
  post,
  registerTenant,
  shardExecute,
  tenantForCatalogShard,
} from "./index.test-helpers";

// This file is one of several index.*.test.ts files split out of a single
// index.test.ts (see index.test-helpers.ts's header comment for why). DO
// storage persists across `it` blocks within a file, so afterEach(reset())
// gives every test clean storage — the same isolation the pre-split file used.
afterEach(async () => {
  await reset();
});

/** Looks up which shard (table, tenantId, partitionKey) currently routes to,
 * via /v1/sql's route echo — the same established pattern
 * findPartitionKeyPairOnDifferentShards already uses (AUTH() bypasses the
 * tenant-token check; tenantId is still used for the actual hash/routing). */
async function probeShardForKey(tenantId: string, table: string, partitionKey: string): Promise<string> {
  const res = await post("/v1/sql", { sql: "SELECT 1", table, tenantId, partitionKey }, AUTH());
  const body = (await res.json()) as { route: { shardId: string } };
  return body.route.shardId;
}

/** Finds at least `minTotal` partition keys (zero-padded sequential
 * candidates "pk-00000", "pk-00001", ... — so their relative sort order is
 * known up front, independent of which shard each lands on) that collectively
 * span at least `minShards` distinct shards within tenantId's catalog-shard
 * pool. Used to build a genuinely multi-shard row set for pagination/E2E
 * tests without depending on a specific hash outcome — only on some split
 * across shards existing, which it keeps probing for rather than assuming. */
async function findKeysSpanningShards(tenantId: string, table: string, minTotal: number, minShards = 2): Promise<string[]> {
  const keys: string[] = [];
  const byShard = new Map<string, number>();
  for (let i = 0; keys.length < minTotal || byShard.size < minShards; i += 1) {
    if (i > 5000) throw new Error(`could not find ${minTotal} keys spanning ${minShards} shards within 5000 probes`);
    const candidate = `pk-${String(i).padStart(5, "0")}`;
    const shardId = await probeShardForKey(tenantId, table, candidate);
    keys.push(candidate);
    byShard.set(shardId, (byShard.get(shardId) ?? 0) + 1);
  }
  return keys;
}

describe("table-scan cursor + merge helpers (unit)", () => {
  it("round-trips a cursor through encode/decode, including non-ASCII partition keys", () => {
    const cursor = { shardCursors: { "shard-a": "row-042", "shard-b": "row-日本語-9" } };
    const encoded = encodeTableScanCursor(cursor);
    expect(typeof encoded).toBe("string");
    expect(decodeTableScanCursor(encoded)).toEqual(cursor);
  });

  it("rejects a malformed cursor (non-base64, non-JSON, or wrong shape) by decoding to null rather than throwing or guessing", () => {
    expect(decodeTableScanCursor("not valid base64!!!")).toBeNull();
    expect(decodeTableScanCursor(btoa("not json"))).toBeNull();
    expect(decodeTableScanCursor(btoa(JSON.stringify({ notShardCursors: {} })))).toBeNull();
    expect(decodeTableScanCursor(btoa(JSON.stringify({ shardCursors: { a: 1 } })))).toBeNull();
    expect(decodeTableScanCursor(btoa(JSON.stringify({ shardCursors: ["a"] })))).toBeNull();
  });

  it("advances a shard's cursor only to the last row from that shard actually kept after truncation — never to a row that was fetched but cut (the data-loss-avoiding invariant)", () => {
    // Two shards each return 2 rows; the overall limit (3) truncates the
    // merged 4 down to 3, so shard-b's second row (b2) gets cut. shard-a's
    // cursor should advance to its last KEPT row (a2, fully included);
    // shard-b's cursor should advance only to b1 (the one kept), never to b2
    // (fetched this call but truncated away) — a subsequent call must
    // re-fetch b2, not skip it.
    const pages = [
      { shardId: "shard-a", rows: [{ partitionKey: "a1", row: { id: "a1" } }, { partitionKey: "a2", row: { id: "a2" } }] },
      { shardId: "shard-b", rows: [{ partitionKey: "b1", row: { id: "b1" } }, { partitionKey: "b2", row: { id: "b2" } }] },
    ];
    const { rows, nextShardCursors } = mergeTableScanPages(pages, 3, 2, {});
    expect(rows.map((r) => r.id)).toEqual(["a1", "a2", "b1"]);
    expect(nextShardCursors).toEqual({ "shard-a": "a2", "shard-b": "b1" });
  });

  // Regression test for a real bug found while implementing this feature:
  // both shards can return FEWER rows than their own perShardLimit (each
  // correctly signalling "nothing further behind what I fetched"), while the
  // OVERALL merge still truncates away a row one of them DID fetch, because
  // another shard's rows happened to sort earlier. nextCursor must still be
  // present in that case, or the truncated-away row is silently dropped
  // forever (no shard's cursor ever passes it, and the client never calls
  // again to pick it up).
  it("still returns a nextCursor when every shard's fetch came back under its own per-shard limit, if the OVERALL truncation still cut a fetched row (no data loss even without any shard hitting its own cap)", () => {
    const pages = [
      { shardId: "shard-a", rows: [{ partitionKey: "a3", row: { id: "a3" } }, { partitionKey: "a5", row: { id: "a5" } }] },
      { shardId: "shard-b", rows: [{ partitionKey: "a4", row: { id: "a4" } }, { partitionKey: "a6", row: { id: "a6" } }] },
    ];
    // Both pages have length 2, under perShardLimit(3) — neither shard signals
    // "may have more" on its own. But merged+sorted is [a3, a4, a5, a6];
    // truncating to overallLimit(3) cuts a6, which WAS fetched from shard-b.
    const { rows, nextShardCursors } = mergeTableScanPages(pages, 3, 3, {});
    expect(rows.map((r) => r.id)).toEqual(["a3", "a4", "a5"]);
    expect(nextShardCursors).not.toBeNull();
    // shard-b's cursor stays at a4 (a6 was fetched but cut) so the next call
    // re-fetches a6 rather than skipping it.
    expect(nextShardCursors).toEqual({ "shard-a": "a5", "shard-b": "a4" });
  });

  it("omits nextCursor once every shard's page came back under its per-shard limit, and leaves an untouched shard's prior cursor unchanged", () => {
    const pages = [{ shardId: "shard-a", rows: [{ partitionKey: "a1", row: { id: "a1" } }] }];
    // shard-a returned 1 row < perShardLimit(5) -> exhausted; shard-b wasn't
    // fetched this call at all (e.g. a stale/omitted key) -> nextShardCursors
    // is null regardless, since NO shard signalled "may have more".
    const { rows, nextShardCursors } = mergeTableScanPages(pages, 10, 5, { "shard-b": "b-prior" });
    expect(rows.map((r) => r.id)).toEqual(["a1"]);
    expect(nextShardCursors).toBeNull();
  });

  // Codex round-4 fix: a shard's own owner-row query (__cf_row_owners, see
  // /tenant-scan-page) can fully consume its LIMIT while still resolving
  // FEWER than `limit` actual rows, because one or more owner rows' base rows
  // were deleted between the shard's two queries (an accepted race — see
  // shard.ts) and got silently skipped. `rows.length < perShardLimit` alone
  // can no longer distinguish "genuinely exhausted" from "hit LIMIT, but a
  // skip pulled the delivered count down" — only `ownerRowsScanned` (the
  // count from query 1, before any skips) can. This regression-tests the
  // fix directly at the merge-helper level: rows.length(1) < perShardLimit(3)
  // would have signalled exhaustion under the old rows.length-only check, but
  // ownerRowsScanned(3) === perShardLimit(3) correctly signals "may have more".
  it("still signals nextCursor for a shard whose owner-row query fully consumed its LIMIT, even though a skipped owner row pulled its delivered row count below that LIMIT", () => {
    const pages = [{ shardId: "shard-a", rows: [{ partitionKey: "a1", row: { id: "a1" } }], ownerRowsScanned: 3, lastOwnerKeyScanned: "a3" }];
    const { rows, nextShardCursors } = mergeTableScanPages(pages, 10, 3, {});
    expect(rows.map((r) => r.id)).toEqual(["a1"]);
    expect(nextShardCursors).not.toBeNull();
  });

  // Control for the fix above: when the owner-row query genuinely came back
  // under its LIMIT (ownerRowsScanned < perShardLimit — no more owner rows
  // exist at all, not even skipped ones), nextCursor must still correctly be
  // omitted. Confirms the new ownerRowsScanned-based check doesn't
  // over-signal "may have more" and break the true end-of-data case.
  it("still omits nextCursor when a shard's owner-row query genuinely came back under its LIMIT (true end-of-data, no regression)", () => {
    const pages = [{ shardId: "shard-a", rows: [{ partitionKey: "a1", row: { id: "a1" } }], ownerRowsScanned: 1, lastOwnerKeyScanned: "a1" }];
    const { rows, nextShardCursors } = mergeTableScanPages(pages, 10, 3, {});
    expect(rows.map((r) => r.id)).toEqual(["a1"]);
    expect(nextShardCursors).toBeNull();
  });

  // Codex round-4 fix: the new owner-scan cursor signal (lastOwnerKeyScanned)
  // must never override the pre-existing cross-shard-truncation invariant --
  // when cross-shard truncation cuts SOME of a shard's own fetched rows away,
  // the "how far scanned" signal must not push the cursor past a row that was
  // fetched but then cut by the overall-limit truncation. Here shard-a's
  // owner query scanned as far as "a9" (not exhausted, limit fully
  // consumed), but the overall truncation to 1 row cuts shard-a's own
  // fetched "a2" away entirely (0 of its 1 fetched rows survived) —
  // nextShardCursors must stay at "" (never fetched "a2"), not jump to "a9".
  it("never advances a shard's cursor past a row it fetched but the cross-shard merge truncated away, even when that shard's owner-row scan went further", () => {
    const pages = [
      { shardId: "shard-a", rows: [{ partitionKey: "a2", row: { id: "a2" } }], ownerRowsScanned: 3, lastOwnerKeyScanned: "a9" },
      { shardId: "shard-b", rows: [{ partitionKey: "a1", row: { id: "a1" } }], ownerRowsScanned: 1, lastOwnerKeyScanned: "a1" },
    ];
    // Merged+sorted: [a1 (shard-b), a2 (shard-a)]; overallLimit(1) keeps only
    // a1, cutting shard-a's a2 away entirely.
    const { rows, nextShardCursors } = mergeTableScanPages(pages, 1, 3, {});
    expect(rows.map((r) => r.id)).toEqual(["a1"]);
    expect(nextShardCursors).toEqual({ "shard-a": "", "shard-b": "a1" });
  });

  // Codex round-5 fix: a shard whose ENTIRE batch of owner keys resolves to
  // zero delivered rows (e.g. every one of their base rows was deleted
  // between /tenant-scan-page's two queries -- shard.ts's accepted race) has
  // no entry at all in `truncated`, so invariant (b) never touches it and its
  // cursor is left at whatever it was BEFORE this call. Since
  // lastOwnerKeyScanned is always >= that prior position (the owner query is
  // `partition_key > prior`), the old "take whichever of (a)/(b) is EARLIER"
  // composition always picked the untouched prior position here -- the
  // cursor could never advance, so a client would re-issue the identical
  // query forever. rows.length(0) must instead let (a) advance the cursor
  // all the way to lastOwnerKeyScanned unconditionally.
  it("advances a shard's cursor to lastOwnerKeyScanned when its entire batch resolved to zero delivered rows, rather than stalling at the untouched prior cursor forever", () => {
    const pages = [{ shardId: "shard-a", rows: [], ownerRowsScanned: 3, lastOwnerKeyScanned: "a3" }];
    const { rows, nextShardCursors } = mergeTableScanPages(pages, 10, 3, { "shard-a": "" });
    expect(rows).toEqual([]);
    expect(nextShardCursors).toEqual({ "shard-a": "a3" });
  });

  // Companion to the fix above: when every row a shard DID fetch survived
  // into the truncated response (no cross-shard truncation touched it at
  // all), (a) is also free to advance past a trailing skipped-owner-row gap
  // beyond the last delivered row -- not just hold at invariant (b)'s bound.
  it("advances a shard's cursor past a trailing skipped-owner-row gap when every row it fetched was delivered (no cross-shard truncation involved)", () => {
    const pages = [{ shardId: "shard-a", rows: [{ partitionKey: "a1", row: { id: "a1" } }], ownerRowsScanned: 3, lastOwnerKeyScanned: "a3" }];
    // overallLimit(10) keeps a1 -- nothing truncated away from shard-a.
    const { rows, nextShardCursors } = mergeTableScanPages(pages, 10, 3, {});
    expect(rows.map((r) => r.id)).toEqual(["a1"]);
    expect(nextShardCursors).toEqual({ "shard-a": "a3" });
  });

  it("breaks a tied partition_key across shards by shardId ascending, and isInternalTableName correctly identifies internal tables at this new gate", () => {
    const pages = [
      { shardId: "shard-z", rows: [{ partitionKey: "dup", row: { id: "from-z" } }] },
      { shardId: "shard-a", rows: [{ partitionKey: "dup", row: { id: "from-a" } }] },
    ];
    const { rows } = mergeTableScanPages(pages, 10, 5, {});
    expect(rows.map((r) => r.id)).toEqual(["from-a", "from-z"]);

    expect(isInternalTableName("__cf_row_owners")).toBe(true);
    expect(isInternalTableName("__cf_indexes")).toBe(true);
    expect(isInternalTableName("sqlite_master")).toBe(true);
    expect(isInternalTableName("events")).toBe(false);
  });
});

describe("Worker /v1/table-scan (Milestone 4)", () => {
  it("returns every row of a tenant's rows in one call when they all fit on a single shard (single-shard scan)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("scan_single_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    await post("/v1/mutate", { op: "insert", table: "scan_single_evt", tenantId, partitionKey: "row-1", values: { v: "a" } }, token);
    await post("/v1/mutate", { op: "insert", table: "scan_single_evt", tenantId, partitionKey: "row-2", values: { v: "b" } }, token);
    await post("/v1/mutate", { op: "insert", table: "scan_single_evt", tenantId, partitionKey: "row-3", values: { v: "c" } }, token);

    const res = await post("/v1/table-scan", { tenantId, table: "scan_single_evt" }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      rows: Array<{ id: string; v: string }>;
      nextCursor?: string;
      provenance: { complete: boolean };
      scan: { catalogShardId: string; shardCount: number; successCount: number; scanMs: number };
    };
    expect(body.rows.map((r) => r.id).sort()).toEqual(["row-1", "row-2", "row-3"]);
    expect(body.nextCursor).toBeUndefined();
    expect(body.provenance.complete).toBe(true); // /admin/create-table sets this fresh
    expect(body.scan.shardCount).toBe(1); // numShards: 1 -> one physical shard in this tenant's pool
    expect(body.scan.successCount).toBe(1);
    expect(typeof body.scan.scanMs).toBe("number");
  });

  it("fans out across the tenant's full catalog-shard pool and returns rows from every shard in one call (multi-shard scan)", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("scan_multi_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const [keyA, keyB] = await findPartitionKeyPairOnDifferentShards(token, tenantId, "scan_multi_evt");
    await post("/v1/mutate", { op: "insert", table: "scan_multi_evt", tenantId, partitionKey: keyA, values: { v: "a" } }, token);
    await post("/v1/mutate", { op: "insert", table: "scan_multi_evt", tenantId, partitionKey: keyB, values: { v: "b" } }, token);

    const res = await post("/v1/table-scan", { tenantId, table: "scan_multi_evt" }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: Array<{ id: string }>; nextCursor?: string; scan: { shardCount: number } };
    expect(body.rows.map((r) => r.id).sort()).toEqual([keyA, keyB].sort());
    expect(body.nextCursor).toBeUndefined();
    expect(body.scan.shardCount).toBe(2);
  });

  // Codex P3 fix: /tenant-scan-page used to SELECT b.*, ro.partition_key AS
  // __cf_scan_pk -- if a tenant's table had a real column literally named
  // __cf_scan_pk, the alias collided with it and the destructure that pulled
  // the alias back out silently dropped that column's real data. The fix
  // reads the partition key directly off the row by its known
  // partitionKeyColumn name instead of using any synthetic alias.
  it("returns a row's real __cf_scan_pk column value intact, even though that name used to collide with the (now-removed) synthetic partition-key alias", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      {
        table: "scan_collide_evt",
        schema: "CREATE TABLE IF NOT EXISTS scan_collide_evt (id TEXT PRIMARY KEY, __cf_scan_pk TEXT, v TEXT)",
        partitionKeyColumn: "id",
      },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const insertRes = await post(
      "/v1/mutate",
      { op: "insert", table: "scan_collide_evt", tenantId, partitionKey: "row-1", values: { __cf_scan_pk: "distinct-real-value", v: "x" } },
      token,
    );
    expect(insertRes.status).toBe(200);

    const res = await post("/v1/table-scan", { tenantId, table: "scan_collide_evt" }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: Array<{ id: string; __cf_scan_pk: string; v: string }> };
    expect(body.rows).toHaveLength(1);
    expect(body.rows[0].id).toBe("row-1");
    expect(body.rows[0].__cf_scan_pk).toBe("distinct-real-value");
    expect(body.rows[0].v).toBe("x");
  });

  // Codex-found P2 regression (from the /tenant-scan-page P3 fix above): that
  // fix started reading the partition key directly off the base row's own
  // value (row[partitionKeyColumn]) instead of __cf_row_owners' copy. That's
  // wrong whenever partitionKeyColumn has SQLite type affinity that coerces
  // values -- an INTEGER PRIMARY KEY column stores tenant keys "9"/"10" as the
  // integers 9/10, while __cf_row_owners.partition_key keeps the original TEXT
  // form. A cursor built from the coerced value would then be compared,
  // lexicographically, against __cf_row_owners' text form on the NEXT call --
  // string "10" sorts before "9" even though numeric 9 < 10 -- silently
  // skipping rows. Keys below are chosen so the two orderings genuinely
  // diverge ("10" < "2" < "9" lexicographically vs. 2 < 9 < 10 numerically).
  it("regression (Codex-found P2, coercion divergence): paginates an INTEGER PRIMARY KEY partitionKeyColumn with no skips, in __cf_row_owners' canonical string order, even though its numeric sort order differs", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "scan_coerce_evt";
    const createRes = await post(
      "/admin/create-table",
      { table, schema: `CREATE TABLE IF NOT EXISTS ${table} (pk INTEGER PRIMARY KEY, v TEXT)`, partitionKeyColumn: "pk" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const keys = ["01", "02", "10"];
    for (const k of keys) {
      const res = await post("/v1/mutate", { op: "insert", table, tenantId, partitionKey: k, values: { v: k } }, token);
      expect(res.status).toBe(200);
    }

    const collected: string[] = [];
    let cursor: string | undefined;
    let calls = 0;
    for (;;) {
      calls += 1;
      expect(calls).toBeLessThanOrEqual(10); // sanity bound -- must terminate
      const res = await post("/v1/table-scan", { tenantId, table, limit: 1, cursor: cursor ?? null }, token);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { rows: Array<{ pk: number; v: string }>; nextCursor?: string };
      collected.push(...body.rows.map((r) => r.v));
      if (!body.nextCursor) break;
      cursor = body.nextCursor;
    }

    // Every key returned exactly once, with no skips, in __cf_row_owners'
    // canonical (string) ascending order -- "01" < "02" < "10" lexicographically.
    expect(collected).toEqual(["01", "02", "10"]);
  });

  it("paginates to exhaustion with no duplicates and no gaps, even when a page truncates a shard's contribution mid-list (criterion 3)", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("scan_page_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const keys = await findKeysSpanningShards(tenantId, "scan_page_evt", 7);
    for (const k of keys) {
      const res = await post("/v1/mutate", { op: "insert", table: "scan_page_evt", tenantId, partitionKey: k, values: { v: k } }, token);
      expect(res.status).toBe(200);
    }

    const collected: string[] = [];
    let cursor: string | undefined;
    let calls = 0;
    for (;;) {
      calls += 1;
      expect(calls).toBeLessThanOrEqual(20); // sanity bound — must terminate
      const res = await post("/v1/table-scan", { tenantId, table: "scan_page_evt", limit: 3, cursor: cursor ?? null }, token);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { rows: Array<{ id: string }>; nextCursor?: string };
      collected.push(...body.rows.map((r) => r.id));
      if (!body.nextCursor) break;
      cursor = body.nextCursor;
    }

    expect(collected.sort()).toEqual([...keys].sort());
    expect(new Set(collected).size).toBe(keys.length);
    expect(calls).toBeGreaterThan(1); // genuinely paginated across multiple calls
  });

  it("never leaks tenant B's row to tenant A, even when both use the identical partition key and it collides on the same shard (criterion 2)", async () => {
    // numShards: 1 -> catalog-0 owns exactly one physical shard, so any two
    // tenants bound to catalog-0 inevitably collide on THAT shard for an
    // identical partition key — the exact "hashes to the same shard"
    // scenario criterion 2 requires, made deterministic rather than probed.
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("scan_isolation_evt");
    const tenantA = tenantForCatalogShard(0, 4);
    let tenantB = "";
    for (let i = 0; ; i += 1) {
      if (i > 5000) throw new Error("could not find a colliding tenantId within 5000 probes");
      const candidate = `${tenantA}-collide-${i}`;
      if (candidate !== tenantA && hashKey(candidate) % 4 === 0) {
        tenantB = candidate;
        break;
      }
    }
    const tokenA = await registerTenant(tenantA);
    const tokenB = await registerTenant(tenantB);

    await post("/v1/mutate", { op: "insert", table: "scan_isolation_evt", tenantId: tenantA, partitionKey: "a-only", values: { v: "a-value" } }, tokenA);
    await post("/v1/mutate", { op: "insert", table: "scan_isolation_evt", tenantId: tenantA, partitionKey: "shared-key", values: { v: "a-shared-value" } }, tokenA);
    // Tenant B upserts the SAME physical row (same base-table PK, no tenant
    // in that key either) — the documented §14 collision, last writer wins.
    // __cf_row_owners now attributes "shared-key" to tenant B.
    const upsertRes = await post(
      "/v1/mutate",
      { op: "upsert", table: "scan_isolation_evt", tenantId: tenantB, partitionKey: "shared-key", values: { v: "b-shared-value" } },
      tokenB,
    );
    expect(upsertRes.status).toBe(200);

    const resA = await post("/v1/table-scan", { tenantId: tenantA, table: "scan_isolation_evt" }, tokenA);
    expect(resA.status).toBe(200);
    const bodyA = (await resA.json()) as { rows: Array<{ id: string; v: string }> };
    // A must see ONLY its own remaining row — never B's data, and no longer
    // "shared-key" (that row_owners entry no longer names A).
    expect(bodyA.rows.map((r) => r.id)).toEqual(["a-only"]);

    const resB = await post("/v1/table-scan", { tenantId: tenantB, table: "scan_isolation_evt" }, tokenB);
    expect(resB.status).toBe(200);
    const bodyB = (await resB.json()) as { rows: Array<{ id: string; v: string }> };
    expect(bodyB.rows.map((r) => r.id)).toEqual(["shared-key"]);
    expect(bodyB.rows[0].v).toBe("b-shared-value");
  });

  it("rejects a table-scan against an unregistered table with 404 TABLE_NOT_REGISTERED", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post("/v1/table-scan", { tenantId, table: "scan_nonexistent_evt" }, token);
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TABLE_NOT_REGISTERED");
  });

  it("rejects a table-scan against a table with an UNSET partition-key column with 409 PARTITION_KEY_COLUMN_UNSET", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    // Directly seed a table_rules row still carrying the '__unset__'
    // sentinel, simulating a table registered before the partition-key-column
    // upgrade — same established pattern as index.core.test.ts's equivalent
    // /v1/mutate test.
    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO table_rules (table_name, partitioning, partition_key_column, created_at) VALUES (?, ?, ?, ?)",
        "scan_legacy_evt",
        "hash",
        "__unset__",
        new Date().toISOString(),
      );
    });
    const token = await registerTenant(tenantId);
    const res = await post("/v1/table-scan", { tenantId, table: "scan_legacy_evt" }, token);
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("PARTITION_KEY_COLUMN_UNSET");
  });

  it("rejects a table-scan against an internal (__cf_*) table name with 403 INTERNAL_TABLE_ACCESS_FORBIDDEN", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post("/v1/table-scan", { tenantId, table: "__cf_row_owners" }, token);
    expect(res.status).toBe(403);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("INTERNAL_TABLE_ACCESS_FORBIDDEN");
  });

  // Codex P2 fix: isInternalTableName expects a normalized (unquoted,
  // lowercased) name -- this route used to pass the raw request value
  // straight through, so a case variant of an internal table name slipped
  // past the guard. Same bug class as the case-sensitivity bypass that took
  // several review rounds to fully close on /v1/sql earlier in this project.
  it.each(["SQLite_master", "sqlite_MASTER", "__CF_row_owners", "__Cf_Row_Owners"])(
    "rejects a case-variant internal table name (%s) with 403 INTERNAL_TABLE_ACCESS_FORBIDDEN, same as the lowercase form",
    async (tableVariant) => {
      await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
      const tenantId = tenantForCatalogShard(0, 4);
      const token = await registerTenant(tenantId);
      const res = await post("/v1/table-scan", { tenantId, table: tableVariant }, token);
      expect(res.status).toBe(403);
      const body = (await res.json()) as { error: { code: string } };
      expect(body.error.code).toBe("INTERNAL_TABLE_ACCESS_FORBIDDEN");
    },
  );

  it("rejects a tenant token that doesn't match the claimed tenantId with 401, identically to /v1/index-query's existing check", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("scan_authmismatch_evt");
    const tenantA = tenantForCatalogShard(0, 4);
    await registerTenant(tenantA);
    const otherToken = await registerTenant("scan-authmismatch-other-tenant");
    const res = await post("/v1/table-scan", { tenantId: tenantA, table: "scan_authmismatch_evt" }, otherToken);
    expect(res.status).toBe(401);
  });

  it("rejects with no tenant token at all with 401", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("scan_notoken_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const res = await post("/v1/table-scan", { tenantId, table: "scan_notoken_evt" });
    expect(res.status).toBe(401);
  });

  it("fails the whole request 502 SHARD_UNREACHABLE, naming the shard, when one shard in the pool doesn't respond (stub that shard's route to fail, the established catalog.test.ts/shard.test.ts monkey-patch pattern)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("scan_unreachable_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    await post("/v1/mutate", { op: "insert", table: "scan_unreachable_evt", tenantId, partitionKey: "row-1", values: { v: "a" } }, token);

    const shardId = "catalog-0-shard-0";
    const stub = env.SHARD.get(env.SHARD.idFromName(shardId));
    await runInDurableObject(stub, async (instance: ShardDO) => {
      const inst = instance as unknown as { routes: Record<string, (request: Request) => Promise<Response>> };
      inst.routes["/tenant-scan-page"] = async () =>
        new Response(JSON.stringify({ error: "simulated shard failure for the regression test" }), {
          status: 500,
          headers: { "content-type": "application/json" },
        });
    });

    const res = await post("/v1/table-scan", { tenantId, table: "scan_unreachable_evt" }, token);
    expect(res.status).toBe(502);
    const body = (await res.json()) as { error: { code: string; shardId: string } };
    expect(body.error.code).toBe("SHARD_UNREACHABLE");
    expect(body.error.shardId).toBe(shardId);
  });

  // Codex P2 fix (defense in depth): a fractional/non-integer limit used to
  // be able to reach a shard and trip a genuine SQLite error there, which the
  // shard route then silently swallowed as {rows: []} instead of failing the
  // request -- see shard.test.ts's "/tenant-scan-page error handling" tests
  // for the shard-level half of this fix. Rejecting it at the Worker layer
  // means it never even reaches a shard in the first place.
  it("rejects a fractional/non-integer limit with 400 LIMIT_EXCEEDED before it ever reaches a shard", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("scan_fraclimit_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post("/v1/table-scan", { tenantId, table: "scan_fraclimit_evt", limit: 2.5 }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("LIMIT_EXCEEDED");
  });

  // Coverage gap: neither tenantId nor table has any test at all -- both are
  // required fields checked before anything else in handleV1TableScan (the
  // sibling /v1/index-query route has an equivalent MISSING_FIELDS test in
  // index.indexing.test.ts; /v1/table-scan's own copy of this check had none).
  it("rejects a table-scan missing tenantId or table with 400 MISSING_FIELDS", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const missingTable = await post("/v1/table-scan", { tenantId }, token);
    expect(missingTable.status).toBe(400);
    expect(((await missingTable.json()) as { error: { code: string } }).error.code).toBe("MISSING_FIELDS");

    const missingTenant = await post("/v1/table-scan", { table: "scan_missingfields_evt" }, token);
    expect(missingTenant.status).toBe(400);
    expect(((await missingTenant.json()) as { error: { code: string } }).error.code).toBe("MISSING_FIELDS");
  });

  // Coverage gap: the fractional-limit test above only exercises the
  // `!Number.isInteger` disjunct of this route's compound limit validation.
  // The `< 1` and `> MAX_TABLE_SCAN_LIMIT` disjuncts had zero coverage of
  // their own -- each is a genuinely different reason the same 400 fires.
  it.each([0, -1, 501])("rejects an out-of-range limit (%d) with 400 LIMIT_EXCEEDED", async (limit) => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("scan_limitrange_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post("/v1/table-scan", { tenantId, table: "scan_limitrange_evt", limit }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("LIMIT_EXCEEDED");
  });

  it("rejects a malformed cursor with 400 INVALID_CURSOR rather than 500ing or silently restarting", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("scan_badcursor_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post("/v1/table-scan", { tenantId, table: "scan_badcursor_evt", cursor: "not-a-valid-cursor!!" }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("INVALID_CURSOR");
  });

  it("rejects a cursor naming a shard no longer in the active set with 400 INVALID_CURSOR (topology changed between calls)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("scan_stalecursor_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const staleCursor = encodeTableScanCursor({ shardCursors: { "catalog-0-shard-does-not-exist": "row-9" } });
    const res = await post("/v1/table-scan", { tenantId, table: "scan_stalecursor_evt", cursor: staleCursor }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("INVALID_CURSOR");
  });

  it("hides an unattributed row from every tenant and reports provenance.complete=false until a full backfill run clears it, then flips true and the row becomes visible (criterion 4)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("scan_prov_evt");

    // Clear tenant_auth on every catalog shard first, so /admin/backfill-
    // provenance's "exactly one candidate tenant" attribution below is
    // deterministic (mirrors index.provenance.test.ts's beforeEach).
    for (let i = 0; i < 4; i += 1) {
      const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
      await stub.fetch(
        new Request("https://catalog.internal/list-shards", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }),
      );
      await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec("DELETE FROM tenant_auth");
      });
    }

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/mutate", { op: "insert", table: "scan_prov_evt", tenantId, partitionKey: "attributed-row", values: { v: "x" } }, token);
    await insertRowBypassingProvenance("catalog-0-shard-0", "scan_prov_evt", "orphan-row", "y");

    // /admin/create-table already set provenance_complete = 1 at creation —
    // reset it directly to simulate a table that predates completeness
    // tracking and genuinely has a legacy unattributed row (this test's
    // premise), on every catalog shard (table_rules is fanned to all of them).
    for (let i = 0; i < 4; i += 1) {
      const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
      await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec("UPDATE table_rules SET provenance_complete = 0 WHERE table_name = ?", "scan_prov_evt");
      });
    }

    const before = await post("/v1/table-scan", { tenantId, table: "scan_prov_evt" }, token);
    expect(before.status).toBe(200);
    const beforeBody = (await before.json()) as { rows: Array<{ id: string }>; provenance: { complete: boolean; fix?: string } };
    // The orphan is invisible to the JOIN, regardless of who's scanning.
    expect(beforeBody.rows.map((r) => r.id)).toEqual(["attributed-row"]);
    expect(beforeBody.provenance.complete).toBe(false);
    expect(beforeBody.provenance.fix).toBeDefined();

    const backfillRes = await post("/admin/backfill-provenance", {}, AUTH());
    expect(backfillRes.status).toBe(200);
    const backfillBody = (await backfillRes.json()) as { attributed: number; orphaned: unknown[]; ambiguous: unknown[] };
    expect(backfillBody.orphaned).toHaveLength(0);
    expect(backfillBody.ambiguous).toHaveLength(0);

    const after = await post("/v1/table-scan", { tenantId, table: "scan_prov_evt" }, token);
    expect(after.status).toBe(200);
    const afterBody = (await after.json()) as { rows: Array<{ id: string }>; provenance: { complete: boolean; fix?: string } };
    expect(afterBody.rows.map((r) => r.id).sort()).toEqual(["attributed-row", "orphan-row"]);
    expect(afterBody.provenance.complete).toBe(true);
    expect(afterBody.provenance.fix).toBeUndefined();
  });

  // Coverage gap: the criterion-4 test above only exercises the `if
  // (!body.catalogShardId)` branch's TRUE path (a full-cluster run). The
  // FALSE path -- a scoped run (catalogShardId given) must NEVER mark
  // provenance_complete, even when ITS OWN scan reports zero orphaned/
  // ambiguous rows -- had zero coverage. A scoped run only ever sees one
  // catalog shard's own shard pool, so "clean" there doesn't mean clean
  // cluster-wide; incorrectly marking complete here would permanently hide
  // this table's provenance status from ever being re-checked honestly.
  it("does NOT mark a table provenance-complete after a scoped (catalogShardId-given) backfill run, even when that run itself finds zero orphaned/ambiguous rows", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("scan_scopedprov_evt");

    for (let i = 0; i < 4; i += 1) {
      const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
      await stub.fetch(
        new Request("https://catalog.internal/list-shards", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }),
      );
      await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec("DELETE FROM tenant_auth");
      });
    }

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    await post("/v1/mutate", { op: "insert", table: "scan_scopedprov_evt", tenantId, partitionKey: "row-1", values: { v: "x" } }, token);

    // Simulate a table that predates completeness tracking, same as the
    // criterion-4 test above.
    for (let i = 0; i < 4; i += 1) {
      const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
      await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec("UPDATE table_rules SET provenance_complete = 0 WHERE table_name = ?", "scan_scopedprov_evt");
      });
    }

    // Scoped run against just catalog-0: the single attributed row here is
    // fully clean (zero orphaned, zero ambiguous) FOR THIS SHARD POOL, but
    // that must not be enough to certify the table complete.
    const scopedRes = await post("/admin/backfill-provenance", { catalogShardId: "catalog-0" }, AUTH());
    expect(scopedRes.status).toBe(200);
    const scopedBody = (await scopedRes.json()) as { orphaned: unknown[]; ambiguous: unknown[] };
    expect(scopedBody.orphaned).toHaveLength(0);
    expect(scopedBody.ambiguous).toHaveLength(0);

    const afterScoped = await post("/v1/table-scan", { tenantId, table: "scan_scopedprov_evt" }, token);
    expect(afterScoped.status).toBe(200);
    const afterScopedBody = (await afterScoped.json()) as { provenance: { complete: boolean } };
    expect(afterScopedBody.provenance.complete).toBe(false);
  });
});

// Codex P1 fix: __cf_row_owners keys a row's owner by (table, partition key
// VALUE) alone, so /tenant-scan-page's JOIN is only safe if partitionKeyColumn
// is guaranteed unique. These tests exercise the new table_rules.
// partition_key_unique verification/gate at both places a partitionKeyColumn
// is established (/admin/create-table and /admin/set-partition-key-column).
describe("/v1/table-scan partitionKeyColumn uniqueness gate (Codex P1 fix)", () => {
  it("allows table-scan normally when partitionKeyColumn is the table's sole PRIMARY KEY (regression, existing behavior)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      { table: "scan_pku_pk_evt", schema: "CREATE TABLE IF NOT EXISTS scan_pku_pk_evt (id TEXT PRIMARY KEY, v TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post("/v1/table-scan", { tenantId, table: "scan_pku_pk_evt" }, token);
    expect(res.status).toBe(200);
  });

  it("allows table-scan normally when partitionKeyColumn is backed by a UNIQUE constraint rather than being the PRIMARY KEY (exercises the PRAGMA index_list/index_info branch)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      {
        table: "scan_pku_uniq_evt",
        schema: "CREATE TABLE IF NOT EXISTS scan_pku_uniq_evt (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT UNIQUE, v TEXT)",
        partitionKeyColumn: "user_id",
      },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post("/v1/table-scan", { tenantId, table: "scan_pku_uniq_evt" }, token);
    expect(res.status).toBe(200);
  });

  // Codex-found P1 (re-review of the P1 fix above): PRAGMA index_list reports
  // unique=1 for a PARTIAL unique index too (CREATE UNIQUE INDEX ... WHERE
  // <predicate>), and PRAGMA index_info never exposes the predicate at all —
  // so checkPartitionKeyUnique used to treat a partial unique index as
  // full-table uniqueness, leaving the cross-tenant leak reachable via this
  // path (duplicate values ARE allowed for rows outside the predicate).
  it("rejects a PARTIAL unique index on partitionKeyColumn as insufficient uniqueness, but still accepts a genuine FULL unique index on the same column (no regression)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      {
        table: "scan_pku_partial_evt",
        schema: "CREATE TABLE IF NOT EXISTS scan_pku_partial_evt (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, active INTEGER, v TEXT)",
        partitionKeyColumn: "user_id",
      },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // No unique index at all yet -> gated 409.
    const beforeAnyIndex = await post("/v1/table-scan", { tenantId, table: "scan_pku_partial_evt" }, token);
    expect(beforeAnyIndex.status).toBe(409);

    // Add a PARTIAL unique index directly on the shard's own SQLite storage —
    // this can't be expressed via /admin/create-table's single-CREATE-TABLE
    // schema field, so we reach into the ShardDO directly (established
    // pattern elsewhere in this file/catalog.test.ts).
    const shardStub = env.SHARD.get(env.SHARD.idFromName("catalog-0-shard-0"));
    await runInDurableObject(shardStub, async (_instance: ShardDO, state: DurableObjectState) => {
      state.storage.sql.exec("CREATE UNIQUE INDEX ux_scan_pku_partial ON scan_pku_partial_evt(user_id) WHERE active = 1");
    });

    // Re-trigger verification for the same column via /admin/set-partition-key-column
    // — must STILL be gated 409: a partial unique index does not guarantee
    // uniqueness for rows outside its predicate.
    const setWithPartial = await post(
      "/admin/set-partition-key-column",
      { table: "scan_pku_partial_evt", partitionKeyColumn: "user_id" },
      AUTH(),
    );
    expect(setWithPartial.status).toBe(200);
    const scanWithPartial = await post("/v1/table-scan", { tenantId, table: "scan_pku_partial_evt" }, token);
    expect(scanWithPartial.status).toBe(409);
    const partialBody = (await scanWithPartial.json()) as { error: { code: string } };
    expect(partialBody.error.code).toBe("PARTITION_KEY_NOT_UNIQUE");

    // Replace it with a genuine FULL (non-partial) unique index on the same
    // column — must now be accepted.
    await runInDurableObject(shardStub, async (_instance: ShardDO, state: DurableObjectState) => {
      state.storage.sql.exec("DROP INDEX ux_scan_pku_partial");
      state.storage.sql.exec("CREATE UNIQUE INDEX ux_scan_pku_full ON scan_pku_partial_evt(user_id)");
    });
    const setWithFull = await post(
      "/admin/set-partition-key-column",
      { table: "scan_pku_partial_evt", partitionKeyColumn: "user_id" },
      AUTH(),
    );
    expect(setWithFull.status).toBe(200);
    const scanWithFull = await post("/v1/table-scan", { tenantId, table: "scan_pku_partial_evt" }, token);
    expect(scanWithFull.status).toBe(200);
  });

  it("lets /admin/create-table succeed for a schema whose partitionKeyColumn is NOT unique (schema creation isn't blocked), but rejects /v1/table-scan on it with 409 PARTITION_KEY_NOT_UNIQUE", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      {
        table: "scan_pku_notuniq_evt",
        schema: "CREATE TABLE IF NOT EXISTS scan_pku_notuniq_evt (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, v TEXT)",
        partitionKeyColumn: "user_id",
      },
      AUTH(),
    );
    // Schema creation itself is never blocked by the uniqueness check.
    expect(createRes.status).toBe(200);

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post("/v1/table-scan", { tenantId, table: "scan_pku_notuniq_evt" }, token);
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string; fix?: string } };
    expect(body.error.code).toBe("PARTITION_KEY_NOT_UNIQUE");
    expect(body.error.fix).toBeDefined();
  });

  it("re-verifies uniqueness at /admin/set-partition-key-column time too — switching to a non-unique column gates the scan 409, switching to a unique one clears it", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      {
        table: "scan_spkc_evt",
        schema: "CREATE TABLE IF NOT EXISTS scan_spkc_evt (id TEXT PRIMARY KEY, user_id TEXT, v TEXT)",
        partitionKeyColumn: "id",
      },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    // Simulate a table registered before this validation existed: reset to
    // the __unset__ sentinel and an unverified (0) uniqueness flag, on every
    // catalog shard (table_rules is fanned out to all of them).
    for (let i = 0; i < 4; i += 1) {
      const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
      await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec(
          "UPDATE table_rules SET partition_key_column = ?, partition_key_unique = 0 WHERE table_name = ?",
          "__unset__",
          "scan_spkc_evt",
        );
      });
    }

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // Upgrade to a NON-unique column: the endpoint itself succeeds (it only
    // validates the column exists), but the scan must be gated 409.
    const setNonUniqueRes = await post(
      "/admin/set-partition-key-column",
      { table: "scan_spkc_evt", partitionKeyColumn: "user_id" },
      AUTH(),
    );
    expect(setNonUniqueRes.status).toBe(200);
    const scanAfterNonUnique = await post("/v1/table-scan", { tenantId, table: "scan_spkc_evt" }, token);
    expect(scanAfterNonUnique.status).toBe(409);
    const nonUniqueBody = (await scanAfterNonUnique.json()) as { error: { code: string } };
    expect(nonUniqueBody.error.code).toBe("PARTITION_KEY_NOT_UNIQUE");

    // Upgrade to a genuinely unique (PRIMARY KEY) column: the flag must be
    // freshly re-verified for the NEW column (not stuck from the old one),
    // and the scan must now be allowed.
    const setUniqueRes = await post(
      "/admin/set-partition-key-column",
      { table: "scan_spkc_evt", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(setUniqueRes.status).toBe(200);
    const scanAfterUnique = await post("/v1/table-scan", { tenantId, table: "scan_spkc_evt" }, token);
    expect(scanAfterUnique.status).toBe(200);
  });
});

describe("Worker /v1/table-scan E2E (Milestone 4)", () => {
  it("register table -> tenant writes rows across multiple shards via /v1/mutate -> /v1/table-scan paginates to completion, returning every row exactly once", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("scan_e2e_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const keys = await findKeysSpanningShards(tenantId, "scan_e2e_evt", 6);
    for (const k of keys) {
      const res = await post("/v1/mutate", { op: "insert", table: "scan_e2e_evt", tenantId, partitionKey: k, values: { v: k } }, token);
      expect(res.status).toBe(200);
    }

    const collected: Array<{ id: string; v: string }> = [];
    let cursor: string | undefined;
    for (let call = 0; call < 20; call += 1) {
      const res = await post("/v1/table-scan", { tenantId, table: "scan_e2e_evt", limit: 2, cursor: cursor ?? null }, token);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { rows: Array<{ id: string; v: string }>; nextCursor?: string };
      collected.push(...body.rows);
      if (!body.nextCursor) break;
      cursor = body.nextCursor;
    }

    expect(collected.map((r) => r.id).sort()).toEqual([...keys].sort());
    expect(new Set(collected.map((r) => r.id)).size).toBe(keys.length);
    for (const row of collected) {
      expect(row.v).toBe(row.id);
    }
  });

  // Codex round-4 fix, reproduced end-to-end: /tenant-scan-page's owner-row
  // query (query 1, __cf_row_owners) can fully consume its LIMIT while the
  // per-key base-row lookup (query 2) skips one entry whose base row was
  // deleted between the two queries (the accepted race documented in
  // shard.ts). Before this fix, the Worker inferred shard exhaustion from
  // `rows.length < perShardLimit` -- the skip alone made that true even
  // though __cf_row_owners still held more real keys beyond this batch,
  // so the scan silently stopped and permanently dropped them.
  //
  // Uses numShards:1 (one physical shard per catalog shard) so every row
  // lands on one known shard ("catalog-0-shard-0"), and a small
  // limit(3) so a handful of rows is enough to force a batch boundary right
  // where the deleted row sits -- no need for TENANT_SCAN_PAGE_SIZE(100) rows.
  it("regression (Codex round-4): a base row deleted out from under its __cf_row_owners entry (simulating the query-1/query-2 race) never truncates the scan early -- pagination continues and still returns every other real row", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "scan_owner_race_evt";
    await createIndexTestTable(table);
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const shardId = "catalog-0-shard-0"; // numShards:1 -> exactly one physical shard for this catalog shard

    // Zero-padded, same-length keys so string order == insertion order:
    // row-1 < row-2 < row-3 < row-4 < row-5.
    const keys = ["row-1", "row-2", "row-3", "row-4", "row-5"];
    for (const k of keys) {
      const res = await post("/v1/mutate", { op: "insert", table, tenantId, partitionKey: k, values: { v: k } }, token);
      expect(res.status).toBe(200);
    }

    // Simulate the race directly at the storage layer: delete JUST row-3's
    // physical base row via the same /execute-backed test helper used
    // elsewhere in this codebase (shardExecute), leaving its
    // __cf_row_owners entry in place untouched -- exactly the state
    // /tenant-scan-page's comment says query 2 must tolerate.
    const deleteResult = await shardExecute(shardId, `DELETE FROM "${table}" WHERE "id" = ?`, ["row-3"]);
    expect(deleteResult.status).toBe(200);

    // limit:3 -> perShardLimit = min(TENANT_SCAN_PAGE_SIZE, 3) = 3. The first
    // page's owner-row query (LIMIT 3) scans [row-1, row-2, row-3]; row-3's
    // base row is gone, so only [row-1, row-2] (2 rows) come back --
    // fewer than perShardLimit(3), purely because of the skip, while
    // row-4/row-5 genuinely still exist beyond this batch.
    const collected: Array<{ id: string; v: string }> = [];
    let cursor: string | undefined;
    let calls = 0;
    for (let call = 0; call < 10; call += 1) {
      calls += 1;
      const res = await post("/v1/table-scan", { tenantId, table, limit: 3, cursor: cursor ?? null }, token);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { rows: Array<{ id: string; v: string }>; nextCursor?: string };
      collected.push(...body.rows);
      if (!body.nextCursor) break;
      cursor = body.nextCursor;
    }

    // The scan must NOT have terminated after the first (short, skip-caused)
    // page -- row-4 and row-5 are real and must still surface via
    // nextCursor-driven pagination.
    expect(calls).toBeGreaterThan(1);
    expect(collected.map((r) => r.id).sort()).toEqual(["row-1", "row-2", "row-4", "row-5"]);
    expect(new Set(collected.map((r) => r.id)).size).toBe(4); // no duplicate delivery either
  });

  // Codex round-5 fix, reproduced end-to-end: the round-4 fix above handles a
  // shard's owner-row batch delivering SOME rows with one skip mixed in. It
  // has a blind spot when a shard's ENTIRE batch resolves to zero delivered
  // rows -- e.g. every owner key in the first LIMIT-sized batch has its base
  // row deleted. Before this fix, mergeTableScanPages's invariant (a) never
  // advances a shard's cursor in that case (lastOwnerKeyScanned is always >=
  // the untouched prior cursor, so "take whichever of (a)/(b) is earlier"
  // always picked the prior, unmoved position) -- the client would re-issue
  // the identical /tenant-scan-page query forever.
  //
  // Uses numShards:1 so all rows land on one known shard, and limit:3 so the
  // whole tenant's data (exactly 3 rows) is consumed by the FIRST batch, with
  // nothing left beyond it -- a scan that stalls will never see a null
  // nextCursor; a correctly-fixed scan advances past the fully-skipped batch
  // on the very next call and terminates immediately.
  it("regression (Codex round-5): a shard whose entire first batch resolves to zero delivered rows still terminates the scan, instead of re-issuing the identical empty query forever", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "scan_owner_allskip_evt";
    await createIndexTestTable(table);
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const shardId = "catalog-0-shard-0";

    const keys = ["row-1", "row-2", "row-3"];
    for (const k of keys) {
      const res = await post("/v1/mutate", { op: "insert", table, tenantId, partitionKey: k, values: { v: k } }, token);
      expect(res.status).toBe(200);
    }
    // Delete every physical base row, leaving all three __cf_row_owners
    // entries in place -- the whole first (and only) batch resolves to zero
    // delivered rows.
    for (const k of keys) {
      const deleteResult = await shardExecute(shardId, `DELETE FROM "${table}" WHERE "id" = ?`, [k]);
      expect(deleteResult.status).toBe(200);
    }

    const collected: Array<{ id: string; v: string }> = [];
    let cursor: string | undefined;
    let calls = 0;
    // A stalled scan would never see a null nextCursor; cap well above the 2
    // calls a correct fix needs (1: empty page + cursor advanced past
    // row-3; 2: owner query past row-3 finds nothing, scan ends) so a
    // regression fails loudly on the `calls` assertion below rather than
    // hanging the test suite.
    for (let call = 0; call < 5; call += 1) {
      calls += 1;
      const res = await post("/v1/table-scan", { tenantId, table, limit: 3, cursor: cursor ?? null }, token);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { rows: Array<{ id: string; v: string }>; nextCursor?: string };
      collected.push(...body.rows);
      if (!body.nextCursor) break;
      cursor = body.nextCursor;
    }

    expect(calls).toBeLessThanOrEqual(2);
    expect(collected).toEqual([]);
  });

  // Companion to the regression above: proves the fix doesn't just terminate
  // the stalled scan, but actually makes PROGRESS past the fully-skipped
  // batch -- real data seeded beyond it must still surface.
  it("regression (Codex round-5): real rows beyond a shard's fully-skipped first batch are still returned, proving the cursor advanced past it (not just that the scan terminated)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "scan_owner_allskip_progress_evt";
    await createIndexTestTable(table);
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const shardId = "catalog-0-shard-0";

    // Zero-padded so string order == insertion order: row-1..row-3 form the
    // fully-skipped first batch (limit:3); row-4/row-5 are real data sorting
    // strictly after it.
    const skippedKeys = ["row-1", "row-2", "row-3"];
    const realKeys = ["row-4", "row-5"];
    for (const k of [...skippedKeys, ...realKeys]) {
      const res = await post("/v1/mutate", { op: "insert", table, tenantId, partitionKey: k, values: { v: k } }, token);
      expect(res.status).toBe(200);
    }
    for (const k of skippedKeys) {
      const deleteResult = await shardExecute(shardId, `DELETE FROM "${table}" WHERE "id" = ?`, [k]);
      expect(deleteResult.status).toBe(200);
    }

    const collected: Array<{ id: string; v: string }> = [];
    let cursor: string | undefined;
    let calls = 0;
    for (let call = 0; call < 5; call += 1) {
      calls += 1;
      const res = await post("/v1/table-scan", { tenantId, table, limit: 3, cursor: cursor ?? null }, token);
      expect(res.status).toBe(200);
      const body = (await res.json()) as { rows: Array<{ id: string; v: string }>; nextCursor?: string };
      collected.push(...body.rows);
      if (!body.nextCursor) break;
      cursor = body.nextCursor;
    }

    expect(calls).toBeGreaterThan(1);
    expect(calls).toBeLessThanOrEqual(5);
    expect(collected.map((r) => r.id).sort()).toEqual(["row-4", "row-5"]);
    expect(new Set(collected.map((r) => r.id)).size).toBe(2);
  });
});
