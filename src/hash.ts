export function hashKey(input: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i += 1) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

/** Deterministic shard selection for a Milestone 2 index entry — hashes
 * (table, indexName, indexKeyJson) into the existing shard pool, independent
 * of which shard the entry's base row lives on. This is what lets
 * /v1/index-query resolve a lookup on one shard instead of scattering. */
export function indexShardIdForKey(
  table: string,
  indexName: string,
  indexKeyJson: string,
  shardIds: string[],
): string {
  const composite = `${table}:${indexName}:${indexKeyJson}`;
  return shardIds[hashKey(composite) % shardIds.length];
}

