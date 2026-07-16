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

// TODO(shardscope): replace this with the real hand-mirrored RPC contract
// once the admin/topology RPC surface this dashboard actually needs is
// settled (mirrors examples/rpc-consumer/src/index.ts's ShardApiBinding
// interface, which hand-mirrors CloudflareShardRpc — see src/index.ts there
// for the pattern). In particular this needs an `adminVBucketMap` method,
// which doesn't exist on CloudflareShardRpc / as an HTTP /admin/* route yet
// (tracked as a separate in-flight task) — today's confirmed methods this
// dashboard will use are `adminListTables`/`adminTopologyLockStatus`-shaped
// calls plus the not-yet-RPC'd HTTP routes /admin/status and
// /admin/shard-stats. Typed loosely as `any` until that contract lands so
// this skeleton isn't blocked on it.
export type ShardApiBinding = any;

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
