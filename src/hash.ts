export function hashKey(input: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < input.length; i += 1) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

const DEFAULT_COORDINATOR_SHARD_COUNT = 4;

/** Mirrors catalogShardCount()'s env-var-with-fallback pattern (index.ts) —
 * shared here since both the Worker (Chunk 3, generating txIds) and ShardDO
 * (Chunk 2, resolving where an existing txId's coordinator lives) need the
 * identical mapping function, or a shard's alarm-driven recovery query would
 * land on the wrong CoordinatorDO instance. */
export function coordinatorShardCount(env: { COORDINATOR_SHARD_COUNT?: string }): number {
  const parsed = env.COORDINATOR_SHARD_COUNT ? Number.parseInt(env.COORDINATOR_SHARD_COUNT, 10) : NaN;
  return Number.isInteger(parsed) && parsed > 0 ? parsed : DEFAULT_COORDINATOR_SHARD_COUNT;
}

export function coordinatorShardIdForTx(txId: string, count: number): string {
  return `coordinator-${hashKey(txId) % count}`;
}
