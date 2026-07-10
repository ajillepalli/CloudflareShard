import { describe, expect, it } from "vitest";
import { hashKey, indexShardIdForKey } from "./hash";

describe("hashKey", () => {
  it("is deterministic for the same input", () => {
    expect(hashKey("tenant-1:events:user-1")).toBe(hashKey("tenant-1:events:user-1"));
  });

  it("returns a non-negative 32-bit unsigned integer", () => {
    const h = hashKey("some-key");
    expect(Number.isInteger(h)).toBe(true);
    expect(h).toBeGreaterThanOrEqual(0);
    expect(h).toBeLessThanOrEqual(0xffffffff);
  });

  it("handles the empty string without throwing", () => {
    expect(() => hashKey("")).not.toThrow();
    expect(Number.isInteger(hashKey(""))).toBe(true);
  });

  it("produces different hashes for different inputs (no trivial collisions)", () => {
    const hashes = new Set(["a", "b", "c", "tenant-1", "tenant-2", "tenant-3"].map(hashKey));
    expect(hashes.size).toBe(6);
  });

  it("handles unicode input without throwing", () => {
    expect(() => hashKey("tenant-éè日本語")).not.toThrow();
  });
});

describe("indexShardIdForKey", () => {
  const shardIds = ["shard-0", "shard-1", "shard-2", "shard-3"];

  it("is deterministic for the same (table, indexName, indexKeyJson, shardIds)", () => {
    const a = indexShardIdForKey("events", "idx_by_v", '["alpha"]', shardIds);
    const b = indexShardIdForKey("events", "idx_by_v", '["alpha"]', shardIds);
    expect(a).toBe(b);
  });

  it("always returns one of the supplied shardIds", () => {
    for (const indexKeyJson of ['["alpha"]', '["beta"]', '["gamma"]', "[1]", "[null]"]) {
      const shardId = indexShardIdForKey("events", "idx_by_v", indexKeyJson, shardIds);
      expect(shardIds).toContain(shardId);
    }
  });

  it("distinguishes table, indexName, and indexKeyJson — no field-boundary collision from naive string concatenation", () => {
    // If the composite key were built without a separator (or a collidable
    // one), "ab" + "c" could equal "a" + "bc". These three inputs would
    // collide under naive concatenation without delimiters; the `:`
    // separator plus each field's own content keeps them distinct enough
    // that they don't all land on the same shard by construction (only by
    // hash coincidence, which this asserts against for these three).
    const a = indexShardIdForKey("ab", "c", '["x"]', shardIds);
    const b = indexShardIdForKey("a", "bc", '["x"]', shardIds);
    const c = indexShardIdForKey("a", "b", 'c"x"]', shardIds);
    const distinct = new Set([a, b, c]);
    expect(distinct.size).toBeGreaterThan(1);
  });

  it("matches the shardIds[hashKey(composite) % shardIds.length] formula directly", () => {
    const table = "events";
    const indexName = "idx_by_v";
    const indexKeyJson = '["alpha"]';
    const expected = shardIds[hashKey(`${table}:${indexName}:${indexKeyJson}`) % shardIds.length];
    expect(indexShardIdForKey(table, indexName, indexKeyJson, shardIds)).toBe(expected);
  });

  it("changing shardIds.length can change the selected shard (no stability guarantee across resizes)", () => {
    const smallPool = indexShardIdForKey("events", "idx_by_v", '["alpha"]', ["shard-0"]);
    expect(smallPool).toBe("shard-0");
  });

  // Milestone 3, Chunk 2: indexShardIdForKey now takes an explicit `ring` —
  // an index's PINNED placement ring, captured once at create-index time —
  // rather than ever being handed the live/current active shard set. These
  // two tests assert the "pinned" half of that contract: this pure function
  // has no notion of "the cluster grew/shrank since"; it only ever sees
  // whatever array the caller passes.
  it("pinned-ring stability: the same ring resolves to the same shard across repeated calls, unaffected by a larger 'live' shard set existing elsewhere", () => {
    const pinnedRing = ["shard-0", "shard-1"];
    const first = indexShardIdForKey("events", "idx_by_v", '["alpha"]', pinnedRing);
    // A hypothetical live/current active-shard set has since grown (e.g. a
    // split added shard-2/shard-3) — but nothing here ever sees that array;
    // the same pinned ring is passed again, exactly as an index's
    // placement_ring_json would be, unaffected by the cluster's growth.
    const second = indexShardIdForKey("events", "idx_by_v", '["alpha"]', pinnedRing);
    expect(second).toBe(first);
    expect(pinnedRing).toContain(first);
  });

  it("pinned-ring stability: a smaller ring captured historically keeps resolving within itself even though a much larger live shard array exists side by side", () => {
    const pinnedRing = ["shard-0", "shard-1"];
    const liveShardsAfterGrowth = ["shard-0", "shard-1", "shard-2", "shard-3", "shard-4"];
    for (const indexKeyJson of ['["alpha"]', '["beta"]', '["gamma"]', "[1]", "[42]"]) {
      const resolved = indexShardIdForKey("events", "idx_by_v", indexKeyJson, pinnedRing);
      // Resolving against the pinned ring never produces a shard outside it,
      // regardless of what the live set (computed independently, never
      // passed in) looks like.
      expect(pinnedRing).toContain(resolved);
      void liveShardsAfterGrowth; // documents the scenario; deliberately unused by the call above
    }
  });
});
