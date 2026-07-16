/** Env bindings for the Shardscope demo Worker.
 *
 * Two-tier auth model (see src/index.ts header comment for the full story):
 *   - ADMIN_TOKEN gates every /admin/* RPC call this Worker makes on the
 *     operator's behalf (topology reads today; topology-mutating controls,
 *     once built, must be gated the same way).
 *   - SHARDSCOPE_GATE_TOKEN gates *this Worker's own* routes (e.g.
 *     /api/stream) so that Shardscope itself isn't a public, unauthenticated
 *     window into cluster topology.
 * Both are secrets (`wrangler secret put ...`), never sent to or read by the
 * browser — the browser only ever talks to this Worker, never to
 * cloudflare-shard-mvp directly.
 */

// Hand-mirrored RPC contract (mirrors examples/rpc-consumer/src/index.ts's
// ShardApiBinding pattern, which hand-mirrors CloudflareShardRpc — see
// src/index.ts's CloudflareShardRpc class there for the source of truth).
// Only the three admin methods TopologyAggregator actually calls are
// declared here; add more as Shardscope grows into mutating admin controls
// (Reshard/Chaos rooms per DESIGN.md).
//
// Return types are intentionally `unknown`, exactly matching
// CloudflareShardRpc's own declared signatures in src/index.ts
// (`adminStatus(adminToken: string): Promise<unknown>`, etc. — the RPC
// entrypoint doesn't narrow these itself). aggregator.ts casts each result
// to its own local response-shape interfaces (mirroring adminStatusCore /
// adminVbucketMapCore / handleStats's actual JSON bodies in src/index.ts and
// src/shard.ts) after receiving it, rather than trusting the binding type to
// do that narrowing.
export interface ShardApiBinding {
  /** Cluster-level counters, fanned out + merged across every catalog shard.
   * See adminStatusCore in src/index.ts. */
  adminStatus(adminToken: string): Promise<unknown>;
  /** Per-vBucket -> shard ownership + in-flight migration state, fanned out
   * + merged across every catalog shard. See adminVbucketMapCore in
   * src/index.ts. CATALOG-AWARE: vbucket ids are only unique within a single
   * catalog's map. */
  adminVbucketMap(adminToken: string): Promise<unknown>;
  /** Per-shard load/table stats for exactly one shard. See
   * adminShardStatsCore in src/index.ts (body.shardId is required — the core
   * function 400s without it) and ShardDO.handleStats in src/shard.ts for
   * the actual JSON body shape. */
  adminShardStats(adminToken: string, body: { shardId: string }): Promise<unknown>;
}

export interface Env {
  /** Service binding to cloudflare-shard-mvp's CloudflareShardRpc entrypoint.
   * See wrangler.toml's [[services]] block for the binding + explanation. */
  SHARD_API: ShardApiBinding;

  /** Durable Object namespace for the single shared topology poller/fan-out.
   * See src/aggregator.ts. Always addressed via idFromName("singleton") —
   * there is exactly one aggregator instance for the whole Worker. */
  AGGREGATOR: DurableObjectNamespace;

  /** Bearer token this Worker presents to cloudflare-shard-mvp's /admin/*
   * surface (HTTP today; RPC methods that take an explicit adminToken
   * argument, once used). Server-side secret only — the browser never sees
   * this value under any circumstance. */
  ADMIN_TOKEN: string;

  /** Bearer/cookie token that gates Shardscope's *own* routes (this Worker's
   * /api/stream, and eventually any topology-mutating control routes it
   * exposes). Distinct from ADMIN_TOKEN: this is "is this browser allowed to
   * watch/operate Shardscope at all", not "is this call allowed to touch
   * cloudflare-shard-mvp's admin API". Server-side secret; the browser holds
   * a session artifact derived from it (e.g. a cookie set after a login
   * step), never the raw token itself. */
  SHARDSCOPE_GATE_TOKEN: string;
}
