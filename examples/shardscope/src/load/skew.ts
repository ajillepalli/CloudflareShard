/** skew.ts — the deterministic hot-shard skew driver.
 *
 * Vanilla TPC-C hashes each row's partition key through hashKey() into one of
 * totalVBuckets buckets, uniformly distributed across whichever shards those
 * buckets currently belong to — across a realistic run, every shard gets a
 * roughly even share of writes and none of them reliably "lights up" as hot.
 * That's fine for a load GENERATOR but useless for a DEMO whose whole point
 * (DESIGN.md's "healed itself under fire, lost nothing") is watching one
 * shard visibly overload under load and then get relieved by a reshard.
 *
 * This module inverts the exact routing formula CloudflareShard's gateway
 * itself uses (see src/hash.ts's hashKey and src/index.ts's mutate/tx
 * routing path — grep `catalogShardIdForTenant` and the literal template
 * `hashKey(\`${tenantId}:${table}:${partitionKey}\`) % totalVBuckets`):
 * given a target shard id and the *current* vbucket-\>shard map for one
 * catalog, it computes the set of vBuckets that shard currently owns, then
 * brute-force-searches a caller-supplied candidate space for partition keys
 * whose hash lands in that set. There is no closed-form inverse for an
 * FNV-1a hash (the same hash construction as hashKey) — a bounded scan is
 * the correct, and only, approach, exactly as it would be for a real
 * consistent-hash ring.
 *
 * This module is deliberately schema-agnostic: it knows nothing about
 * warehouses, items, or TPC-C tables. Callers (see ./load-driver.ts) supply
 * a `candidateToKey` function that maps a plain increasing integer index
 * onto whatever domain value + partition-key string they actually care
 * about (e.g. a TPC-C item id -> stockKey(warehouseId, itemId)) — this file
 * only verifies where that key would route and reports which candidates
 * matched.
 */
import { hashKey } from "../../../../src/hash";

/** The subset of a vbucket-map row this module needs — structurally
 * compatible with a `TopologySnapshot.catalogs[].vbuckets` entry
 * (aggregator.ts's VbucketMapRow) without importing that file, so this stays
 * a standalone, dependency-free routing utility. */
export interface VBucketOwnership {
  vbucket: number;
  shardId: string;
}

/** The exact routing formula CloudflareShard's gateway uses to place a key
 * into a vBucket — see this file's header comment for the source-of-truth
 * pointer (src/hash.ts's hashKey + src/index.ts's mutate/tx routing path).
 * Exported so callers OUTSIDE this file (e.g. the Playground room's routing
 * inspector, ../play.ts) can compute the same vbucket number this module's
 * own routesToShard/generateSkewedKeys use internally, without re-deriving
 * the modulo themselves — a single formula, never two copies that could
 * drift apart. */
export function vbucketForKey(tenantId: string, table: string, partitionKey: string, totalVBuckets: number): number {
  return hashKey(`${tenantId}:${table}:${partitionKey}`) % totalVBuckets;
}

/** Computes the set of vBucket numbers currently owned by `targetShardId`
 * within one catalog's map. A vbucket mid-migration (its row's
 * `targetShardId` set, cutover not yet committed) is deliberately NOT
 * counted as owned by that migration target here — only the CURRENT
 * `shardId` is, matching where a write issued right now actually lands. */
export function ownedVBuckets(vbucketMap: VBucketOwnership[], targetShardId: string): Set<number> {
  const owned = new Set<number>();
  for (const row of vbucketMap) {
    if (row.shardId === targetShardId) owned.add(row.vbucket);
  }
  return owned;
}

export interface SkewCandidate<T> {
  value: T;
  partitionKey: string;
}

export interface SkewedKey<T> {
  value: T;
  partitionKey: string;
  vbucket: number;
}

export interface SkewParams<T> {
  targetShardId: string;
  vbucketMap: VBucketOwnership[];
  totalVBuckets: number;
  tenantId: string;
  table: string;
  /** Maps a monotonically increasing candidate index (0, 1, 2, ...) to a
   * domain value and the partition key that value would actually be written
   * under. */
  candidateToKey: (candidateIndex: number) => SkewCandidate<T>;
  /** How many matching keys to return. */
  count: number;
  /** Upper bound on candidate indices scanned, regardless of how many
   * matches were found so far — the reliability guarantee this module
   * exists to provide: `generateSkewedKeys` ALWAYS terminates, even when the
   * target shard owns very few (or zero) vBuckets and a match is rare or
   * nonexistent. Defaults to a bound generous enough for a realistic
   * totalVBuckets/shard-count ratio without risking a runaway scan against a
   * target shard that (transiently) owns no vBuckets at all. */
  maxAttempts?: number;
}

const DEFAULT_MAX_ATTEMPTS = 20000;

/** Deterministic hot-shard key generation ("skew" mode). Scans
 * candidateToKey(0), candidateToKey(1), ... until `count` candidates whose
 * `hashKey(\`${tenantId}:${table}:${partitionKey}\`) % totalVBuckets` falls
 * in a vBucket owned by targetShardId have been found, or `maxAttempts`
 * candidates have been scanned — whichever comes first. This bounded-scan
 * behavior is the core correctness guarantee this module exists for: every
 * key it returns is VERIFIED (not assumed) to route to the target shard, by
 * applying the exact same formula production routing uses.
 *
 * Returns fewer than `count` entries (possibly zero) if the candidate space
 * or the maxAttempts budget is exhausted first — e.g. a target shard that
 * currently owns no vBuckets in this catalog (mid-drain, or a catalog it
 * simply has no presence in). Callers must handle a short/empty result; this
 * function deliberately never throws or loops unboundedly to "guarantee" a
 * count it cannot structurally promise. */
export function generateSkewedKeys<T>(params: SkewParams<T>): SkewedKey<T>[] {
  const { targetShardId, vbucketMap, totalVBuckets, tenantId, table, candidateToKey, count } = params;
  const maxAttempts = params.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
  const results: SkewedKey<T>[] = [];
  if (totalVBuckets <= 0 || count <= 0) return results;

  const owned = ownedVBuckets(vbucketMap, targetShardId);
  if (owned.size === 0) return results;

  for (let attempt = 0; attempt < maxAttempts && results.length < count; attempt++) {
    const { value, partitionKey } = candidateToKey(attempt);
    const vbucket = vbucketForKey(tenantId, table, partitionKey, totalVBuckets);
    if (owned.has(vbucket)) {
      results.push({ value, partitionKey, vbucket });
    }
  }
  return results;
}

/** "Uniform" mode counterpart — normal, unskewed keys, matching real TPC-C's
 * own uniform-random ID sampling (world.mjs's randomItemId() and friends in
 * the Node reference harness, which also sample with replacement). Takes the
 * SAME candidateToKey shape as generateSkewedKeys so a caller can flip
 * between modes without restructuring anything else, per this driver's
 * "pluggable normal-vs-skew" requirement. */
export function generateUniformKeys<T>(
  poolSize: number,
  count: number,
  candidateToKey: (candidateIndex: number) => SkewCandidate<T>,
  rng: () => number = Math.random,
): SkewCandidate<T>[] {
  if (poolSize <= 0 || count <= 0) return [];
  const results: SkewCandidate<T>[] = [];
  for (let i = 0; i < count; i++) {
    const candidateIndex = Math.floor(rng() * poolSize);
    results.push(candidateToKey(candidateIndex));
  }
  return results;
}

/** Verifies a single (tenantId, table, partitionKey) actually routes to
 * `targetShardId` under the current vbucket map — the same check
 * `generateSkewedKeys` applies internally per-candidate, exposed standalone
 * so callers (and tests) can spot-check one key without re-deriving the
 * formula themselves. */
export function routesToShard(
  tenantId: string,
  table: string,
  partitionKey: string,
  totalVBuckets: number,
  vbucketMap: VBucketOwnership[],
  targetShardId: string,
): boolean {
  if (totalVBuckets <= 0) return false;
  const vbucket = vbucketForKey(tenantId, table, partitionKey, totalVBuckets);
  return ownedVBuckets(vbucketMap, targetShardId).has(vbucket);
}
