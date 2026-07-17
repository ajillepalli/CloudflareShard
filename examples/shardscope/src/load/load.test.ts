import { describe, expect, it } from "vitest";
import { hashKey } from "../../../../src/hash";
import { generateSkewedKeys, ownedVBuckets, routesToShard, type VBucketOwnership } from "./skew";
import { TRANSACTION_MIX, pickTransactionType, type TransactionType } from "./transactions";

// ----------------------------------------------------------------------------
// Test fixtures: a synthetic vbucket map spread across a few shards, built
// the same way a real CatalogDO's /vbucket-map response would be (a flat
// array of {vbucket, shardId} rows covering [0, totalVBuckets)).
// ----------------------------------------------------------------------------

const TOTAL_VBUCKETS = 64;
const SHARD_IDS = ["shard-0", "shard-1", "shard-2", "shard-3"];

function buildVbucketMap(totalVBuckets: number, shardIds: string[]): VBucketOwnership[] {
  return Array.from({ length: totalVBuckets }, (_, vbucket) => ({
    vbucket,
    shardId: shardIds[vbucket % shardIds.length],
  }));
}

describe("skew.ts — ownedVBuckets", () => {
  const map = buildVbucketMap(TOTAL_VBUCKETS, SHARD_IDS);

  it("returns exactly the vBuckets whose current shardId matches the target", () => {
    const owned = ownedVBuckets(map, "shard-1");
    for (const v of owned) {
      expect(map.find((r) => r.vbucket === v)?.shardId).toBe("shard-1");
    }
    // Every vbucket in the map is accounted for across all 4 shards.
    const totalOwned = SHARD_IDS.reduce((sum, id) => sum + ownedVBuckets(map, id).size, 0);
    expect(totalOwned).toBe(TOTAL_VBUCKETS);
  });

  it("returns an empty set for a shard id that owns nothing", () => {
    expect(ownedVBuckets(map, "shard-does-not-exist").size).toBe(0);
  });
});

describe("skew.ts — generateSkewedKeys: the core routing guarantee", () => {
  const map = buildVbucketMap(TOTAL_VBUCKETS, SHARD_IDS);
  const tenantId = "tpcc-w0001";
  const table = "tpcc_stock";

  // Real stockKey format ("s-0001-000042"), duplicated inline rather than
  // imported from transactions.ts, so this test doesn't silently pass just
  // because both sides share one (possibly wrong) implementation.
  function stockKeyLike(w: number, i: number): string {
    return `s-${String(w).padStart(4, "0")}-${String(i).padStart(6, "0")}`;
  }

  for (const targetShardId of SHARD_IDS) {
    it(`every generated key for target ${targetShardId} actually routes there via the real hashKey formula`, () => {
      const results = generateSkewedKeys<number>({
        targetShardId,
        vbucketMap: map,
        totalVBuckets: TOTAL_VBUCKETS,
        tenantId,
        table,
        count: 20,
        candidateToKey: (i) => ({ value: i + 1, partitionKey: stockKeyLike(1, i + 1) }),
      });

      // The skew driver is worthless if this doesn't hold: every key it
      // hands back must independently re-verify as routing to the target
      // shard, using the exact same formula production routing uses
      // (hashKey(`${tenantId}:${table}:${partitionKey}`) % totalVBuckets).
      expect(results.length).toBeGreaterThan(0);
      for (const r of results) {
        const vbucket = hashKey(`${tenantId}:${table}:${r.partitionKey}`) % TOTAL_VBUCKETS;
        expect(vbucket).toBe(r.vbucket);
        expect(map[vbucket].shardId).toBe(targetShardId);
        expect(routesToShard(tenantId, table, r.partitionKey, TOTAL_VBUCKETS, map, targetShardId)).toBe(true);
      }
    });
  }

  it("returns keys whose VALUES also round-trip through candidateToKey correctly", () => {
    const results = generateSkewedKeys<number>({
      targetShardId: "shard-0",
      vbucketMap: map,
      totalVBuckets: TOTAL_VBUCKETS,
      tenantId,
      table,
      count: 5,
      candidateToKey: (i) => ({ value: i + 1, partitionKey: stockKeyLike(1, i + 1) }),
    });
    for (const r of results) {
      expect(r.partitionKey).toBe(stockKeyLike(1, r.value));
    }
  });

  it("respects `count`: never returns more than requested", () => {
    const results = generateSkewedKeys<number>({
      targetShardId: "shard-0",
      vbucketMap: map,
      totalVBuckets: TOTAL_VBUCKETS,
      tenantId,
      table,
      count: 3,
      candidateToKey: (i) => ({ value: i + 1, partitionKey: stockKeyLike(1, i + 1) }),
      maxAttempts: 5000,
    });
    expect(results.length).toBeLessThanOrEqual(3);
  });
});

describe("skew.ts — bounded search terminates", () => {
  const tenantId = "tpcc-w0001";
  const table = "tpcc_stock";

  function stockKeyLike(w: number, i: number): string {
    return `s-${String(w).padStart(4, "0")}-${String(i).padStart(6, "0")}`;
  }

  it("terminates immediately when the target shard owns zero vBuckets (no infinite loop)", () => {
    // A map where the target shard has NO rows at all -- e.g. mid-drain, or
    // a catalog it has no presence in.
    const mapWithoutTarget = buildVbucketMap(TOTAL_VBUCKETS, ["shard-a", "shard-b"]);
    const start = performance.now();
    const results = generateSkewedKeys<number>({
      targetShardId: "shard-ghost",
      vbucketMap: mapWithoutTarget,
      totalVBuckets: TOTAL_VBUCKETS,
      tenantId,
      table,
      count: 10,
      candidateToKey: (i) => ({ value: i + 1, partitionKey: stockKeyLike(1, i + 1) }),
      maxAttempts: 1_000_000,
    });
    const elapsedMs = performance.now() - start;
    expect(results).toEqual([]);
    // The size-0-owned-set short-circuit means this returns immediately,
    // well before scanning anywhere near maxAttempts candidates.
    expect(elapsedMs).toBeLessThan(1000);
  });

  it("respects maxAttempts even when the target shard owns very few vBuckets", () => {
    // Only ONE vbucket (out of a much larger space) belongs to the target --
    // a worst-case-but-legitimate topology (e.g. a shard that just took over
    // a single vbucket mid-reshard).
    const totalVBuckets = 10_000;
    const sparseMap: VBucketOwnership[] = Array.from({ length: totalVBuckets }, (_, v) => ({
      vbucket: v,
      shardId: v === 4242 ? "shard-rare" : "shard-common",
    }));

    const start = performance.now();
    const results = generateSkewedKeys<number>({
      targetShardId: "shard-rare",
      vbucketMap: sparseMap,
      totalVBuckets,
      tenantId,
      table,
      count: 5,
      // A tiny maxAttempts relative to the search space -- almost certainly
      // won't find all 5 (may find zero), but MUST come back promptly
      // instead of scanning forever looking for a 5th match that may not be
      // reachable within budget.
      maxAttempts: 50,
      candidateToKey: (i) => ({ value: i + 1, partitionKey: stockKeyLike(1, i + 1) }),
    });
    const elapsedMs = performance.now() - start;

    expect(results.length).toBeLessThanOrEqual(5);
    // Every match found (even if fewer than requested) must still be
    // correct -- a bounded search that gives up early must never fabricate
    // an incorrect result to hit its count.
    for (const r of results) {
      const vbucket = hashKey(`${tenantId}:${table}:${r.partitionKey}`) % totalVBuckets;
      expect(vbucket).toBe(4242);
    }
    expect(elapsedMs).toBeLessThan(2000);
  });

  it("a huge count with a small maxAttempts still returns promptly instead of hanging", () => {
    const map = buildVbucketMap(TOTAL_VBUCKETS, SHARD_IDS);
    const start = performance.now();
    const results = generateSkewedKeys<number>({
      targetShardId: "shard-0",
      vbucketMap: map,
      totalVBuckets: TOTAL_VBUCKETS,
      tenantId,
      table,
      count: 1_000_000, // structurally unreachable
      maxAttempts: 200,
      candidateToKey: (i) => ({ value: i + 1, partitionKey: stockKeyLike(1, i + 1) }),
    });
    const elapsedMs = performance.now() - start;
    expect(results.length).toBeLessThan(1_000_000);
    expect(elapsedMs).toBeLessThan(1000);
  });
});

describe("transactions.ts — weighted transaction mix", () => {
  it("TRANSACTION_MIX matches the standard TPC-C weights", () => {
    const weights = Object.fromEntries(TRANSACTION_MIX.map((t) => [t.type, t.weight]));
    expect(weights["new-order"]).toBe(45);
    expect(weights["payment"]).toBe(43);
    expect(weights["order-status"]).toBe(4);
    expect(weights["delivery"]).toBe(4);
    expect(weights["stock-level"]).toBe(4);
    expect(Object.values(weights).reduce((a, b) => a + b, 0)).toBe(100);
  });

  it("pickTransactionType produces the standard distribution within tolerance over many samples", () => {
    const N = 50_000;
    const counts: Record<TransactionType, number> = {
      "new-order": 0,
      payment: 0,
      "order-status": 0,
      delivery: 0,
      "stock-level": 0,
    };

    // Deterministic seeded PRNG (mulberry32) instead of Math.random so this
    // test is reproducible.
    let seed = 0x1234abcd;
    const rng = () => {
      seed |= 0;
      seed = (seed + 0x6d2b79f5) | 0;
      let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
      t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };

    for (let i = 0; i < N; i++) {
      counts[pickTransactionType(rng).type] += 1;
    }

    const expected: Record<TransactionType, number> = {
      "new-order": 0.45,
      payment: 0.43,
      "order-status": 0.04,
      delivery: 0.04,
      "stock-level": 0.04,
    };

    for (const type of Object.keys(expected) as TransactionType[]) {
      const observed = counts[type] / N;
      // Generous absolute tolerance (+/-1.5 percentage points) -- this is a
      // distributional sanity check, not a statistical precision test.
      expect(Math.abs(observed - expected[type])).toBeLessThan(0.015);
    }
  });

  it("always returns one of the 5 defined transaction types", () => {
    const seen = new Set<TransactionType>();
    for (let i = 0; i < 2000; i++) {
      seen.add(pickTransactionType(Math.random).type);
    }
    expect([...seen].sort()).toEqual(["delivery", "new-order", "order-status", "payment", "stock-level"]);
  });
});
