export function hashKey(input: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i += 1) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/** Deterministic shard selection for a Milestone 2 index entry — hashes
 * (table, indexName, indexKeyJson) into a shard pool, independent of which
 * shard the entry's base row lives on. This is what lets /v1/index-query
 * resolve a lookup on one shard instead of scattering.
 *
 * Milestone 3, Chunk 2: `ring` must be the index's *pinned* placement ring
 * (`index_rules.placement_ring_json`, captured once at /admin/create-index
 * time from the then-active shard set), never the live/current active shard
 * set. Every caller of this function (index-write maintenance, index-query
 * lookup, backfill) must pass the SAME ring for a given index for as long as
 * it exists, or entries silently split across two different modulos and
 * become unfindable. This is what makes /admin/split-vbucket (which grows
 * the active shard set) and /admin/drain-shard (which shrinks it, modulo
 * Chunk 5's ring-evacuation rule) safe to run without recomputing index
 * placement out from under existing entries. */
export function indexShardIdForKey(
  table: string,
  indexName: string,
  indexKeyJson: string,
  ring: string[],
): string {
  const composite = `${table}:${indexName}:${indexKeyJson}`;
  return ring[hashKey(composite) % ring.length];
}

/** Milestone 3, Chunk 5: deterministic replacement choice when a shard in an
 * index's pinned ring is drained — the candidate with the smallest
 * hashKey(indexName + ":" + shardId), per the spec's substitution rule.
 * Deterministic on (indexName, candidates) alone, so any orchestrator (or a
 * re-run after a crash) picks the identical substitute; hash ties break
 * lexicographically for the same reason. Returns null when no candidate
 * exists (the caller rejects 409 RING_EVACUATION_NO_CANDIDATE). */
export function pickRingSubstitute(indexName: string, candidates: string[]): string | null {
  let best: string | null = null;
  let bestHash = Number.POSITIVE_INFINITY;
  for (const shardId of candidates) {
    const h = hashKey(`${indexName}:${shardId}`);
    if (h < bestHash || (h === bestHash && best !== null && shardId < best)) {
      best = shardId;
      bestHash = h;
    }
  }
  return best;
}

