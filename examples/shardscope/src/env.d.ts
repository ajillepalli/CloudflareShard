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
  /** Registers (or, with `rotate: true`, re-issues) a tenant's bearer token.
   * See adminRegisterTenantCore + CatalogDO.handleRegisterTenant in the main
   * repo's src/index.ts / src/catalog.ts. Success response body:
   * `{ ok: true, tenantId, token }` — the token is returned ONLY on this
   * call; the catalog stores just its hash from then on. Without
   * `rotate: true`, registering an already-registered tenantId rejects (the
   * underlying RPC throws, since CloudflareShardRpc's adminRegisterTenant
   * unwraps a non-2xx HTTP response into a thrown Error) with a message
   * containing `TENANT_ALREADY_REGISTERED` rather than returning a new
   * token — see src/load/tenant-token-store.ts, whose entire get-or-create
   * design exists to never pass `rotate: true` for a tenant it didn't
   * itself just create. */
  adminRegisterTenant(adminToken: string, body: { tenantId: string; rotate?: boolean }): Promise<unknown>;

  // ---- Reshard console (T8) — topology-mutating admin controls ------------
  // Every method below is a topology-op RPC that ALREADY EXISTS on
  // CloudflareShardRpc in the main repo's src/index.ts (see that class's
  // "Admin/topology methods" section) — declared here only so this Worker's
  // TypeScript build knows the shape of the SHARD_API service binding call.
  // Like every method above, all validation/auth happens on the OTHER side
  // of this binding (assertAdminRpcAuth checks adminToken there); a rejected
  // promise here means the underlying RPC's unwrapForRpc turned a non-2xx
  // HTTP response into a thrown Error (see CloudflareShardRpc's own
  // unwrapForRpc helper) — src/reshard.ts's callers are responsible for
  // catching that and turning it into a calm inline error for the browser.
  //
  // CATALOG-AWARE: vbucket ids are local to a single catalog shard's own
  // [0, totalVBuckets) range (same caveat as adminVbucketMap above) — every
  // one of these calls that takes a vbucket also takes catalogShardId to
  // disambiguate it.

  /** Starts a vbucket split: creates a fresh target shard (or migrates onto
   * `newShardId` if given) and begins a real data migration off the vbucket's
   * current shard. See adminSplitVbucketCore (main repo's src/index.ts,
   * ~line 1265) and CatalogDO.handleSplitVbucket (src/catalog.ts) for the
   * exact payload/response shape. Success body:
   * `{ ok: true, vbucket, fromShard, toShard, metadataVersion, migrationStarted: true }`. */
  adminSplitVbucket(adminToken: string, payload: { catalogShardId?: string; vbucket: number; newShardId?: string }): Promise<unknown>;

  /** Starts a vbucket migration onto an explicit `targetShardId` (or a fresh
   * shard if omitted — same underlying primitive as adminSplitVbucket). See
   * adminMigrateVbucketCore (~line 1302) and CatalogDO.handleMigrateVbucket.
   * Success body: `{ ok: true, vbucket, fromShard, toShard, status: "backfilling" }`. */
  adminMigrateVbucket(adminToken: string, payload: { catalogShardId?: string; vbucket?: number; targetShardId?: string }): Promise<unknown>;

  /** Progress of an in-flight (or just-finished) migrate/split for one
   * vbucket. See CatalogDO.handleMigrateVbucketStatus (src/catalog.ts).
   * Response body: `{ vbucket, status, fromShard, toShard, rowsCopied,
   * mirrorQueueDepth, startedAt, blockedTxIds? }` — `status` is one of
   * "none" | "backfilling" | "mirroring" | "cutover" |
   * "cutover-blocked-on-prepared-intents" | "aborting" (see the migration
   * state machine in src/catalog.ts); "none" after having been non-"none" is
   * terminal (either committed via cutover or cleaned up via abort). */
  adminMigrateVbucketStatus(adminToken: string, payload: { catalogShardId?: string; vbucket?: number }): Promise<unknown>;

  /** Aborts an in-flight migrate/split for one vbucket — safe at any point
   * before the ownership flip (the source never stopped being authoritative).
   * See CatalogDO.handleMigrateVbucketAbort. Success body:
   * `{ ok: true, vbucket, status: "aborted" }`; 409 MIGRATION_ALREADY_COMMITTED
   * if there's nothing active to abort. */
  adminMigrateVbucketAbort(adminToken: string, payload: { catalogShardId?: string; vbucket?: number }): Promise<unknown>;

  /** Starts draining a shard (evacuates every vbucket it owns + any index
   * ring it's pinned into, within the given catalog). See adminDrainShardCore
   * (~line 1833) and CatalogDO.handleDrainShard. Success body:
   * `{ ok: true, shardId, metadataVersion, evacuationStarted: true }`; can
   * 409 (SHARD_HAS_IN_FLIGHT_TRANSACTIONS, SHARD_HAS_PENDING_INDEX_JOBS,
   * RING_EVACUATION_NO_CANDIDATE) before anything durable is marked. */
  adminDrainShard(adminToken: string, payload: { shardId: string; catalogShardId?: string }): Promise<unknown>;

  /** Progress of an in-flight (or just-finished) shard drain. See
   * CatalogDO.handleDrainShardStatus. Response body: `{ shardId,
   * vbucketsRemaining, ringsRemaining, status, stallReason }` — `status` is
   * one of "active" | "migrating-vbuckets" | "evacuating-rings" |
   * "stalled-provenance" | "stalled" | "complete". */
  adminDrainShardStatus(adminToken: string, payload: { catalogShardId?: string; shardId?: string }): Promise<unknown>;

  /** Current holder (if any) of the cluster-wide topology-operation lock —
   * every split/migrate/drain above acquires this lock for its whole
   * multi-tick duration, so at most one topology op runs cluster-wide at a
   * time. See adminTopologyLockStatusCore (~line 2945). No payload — the
   * lock lives on one canonical physical DO ("catalog-0"), not per catalog.
   * Response body: `{ held: false }` or `{ held: true, operationId,
   * operationType, acquiredAt, heartbeatAt, expiresAt, expired }`. */
  adminTopologyLockStatus(adminToken: string): Promise<unknown>;

  /** Operator escape hatch: force-clears the topology lock IFF `operationId`
   * currently matches its holder (idempotent no-op otherwise) — for a
   * crashed operation that never released it. See
   * adminForceReleaseTopologyLockCore (~line 2962). Read
   * adminTopologyLockStatus first to find the current operationId; this call
   * 400s without one. Response body: `{ ok: true, released: boolean }`. */
  adminForceReleaseTopologyLock(adminToken: string, payload: { operationId?: string }): Promise<unknown>;

  // ---- Demo-only fault injection (Shardscope chaos mode, T9's blip-shard-
  // offline attack) — see the main repo's src/index.ts
  // (requireFaultInjectionEnabled, adminFaultInjectCore/adminFaultClearCore)
  // and src/shard.ts (ShardDO.handleFaultInject/handleFaultClear, FAULT_MAX_MS)
  // for the real primitive these two methods call. Off unless the core
  // Worker's FAULT_INJECTION_ENABLED env var is exactly "true" — a disabled
  // deployment rejects BEFORE auth, BEFORE routing, with 403 (the exact
  // string "Fault injection is disabled" always appears in that rejection's
  // `error` field — see ../src/chaos.ts's classifyBlipFaultInjectError,
  // which matches on it). An unknown shardId (not currently in the live
  // vbucket map) rejects with 404 `{ error: { code: "UNKNOWN_SHARD", ... } }`
  // rather than ever materializing a cold Durable Object.

  /** Makes ONE named shard's Durable Object genuinely return 503 for up to
   * `durationMs` (server clamps to a HARD, absolute 30s cap — FAULT_MAX_MS in
   * src/shard.ts — regardless of how this is called or how many times).
   * `catalogShardId` is accepted for shape-consistency with every other
   * shard-targeted admin call here but is NOT used for routing — this
   * targets exactly one shard by `shardId`, never a fan-out. Success body:
   * `{ ok: true, mode: "unreachable", faultExpiresAt: <epoch ms> }`. */
  adminFaultInject(adminToken: string, body: { shardId: string; catalogShardId?: string; mode?: "unreachable"; durationMs?: number }): Promise<unknown>;

  /** Clears an active fault on one shard before its window elapses on its
   * own. Idempotent — succeeds even if no fault is currently active. Success
   * body: `{ ok: true }`. */
  adminFaultClear(adminToken: string, body: { shardId: string; catalogShardId?: string }): Promise<unknown>;

  // ---- Playground (src/play.ts) — tenant data-plane + operator primitive
  // RPC methods. Every one of these ALREADY EXISTS on CloudflareShardRpc in
  // the main repo's src/index.ts (the "RPC / Worker-service-binding
  // entrypoint for the tenant data path" section for mutate/tx/indexQuery/
  // tableScan, and the "Operator-only raw SQL"/"Cross-tenant fan-out read"
  // methods for sql/scatter) — declared here only so this Worker's
  // TypeScript build knows the shape of the SHARD_API service binding call,
  // same convention as every method above.

  /** Tenant-scoped structured mutate — RPC counterpart of /v1/mutate, taking
   * a TENANT bearer token (not adminToken) as its first argument. See
   * CloudflareShardRpc.mutate + structured-op.ts's StructuredMutation (main
   * repo) for the authoritative shape. Success body: `{ ok: true,
   * rowsAffected }`; a requestId reused with a different (sql/params) hash
   * rejects with 409 (see src/play.ts's playMutate doc comment for the exact
   * contract this demo exercises). */
  mutate(
    tenantToken: string,
    body: {
      op: "insert" | "update" | "delete" | "upsert";
      table: string;
      tenantId: string;
      partitionKey: string;
      where?: Record<string, unknown>;
      values?: Record<string, unknown>;
      requestId?: string;
    },
  ): Promise<unknown>;

  /** Tenant-scoped cross-shard atomic write (2PC) — RPC counterpart of
   * /v1/tx. See CloudflareShardRpc.tx + structured-op.ts's StructuredOperation. */
  tx(
    tenantToken: string,
    body: {
      mutations: Array<{
        op: "insert" | "update" | "delete" | "upsert";
        table: string;
        tenantId: string;
        partitionKey: string;
        where?: Record<string, unknown>;
        values?: Record<string, unknown>;
      }>;
      requestId?: string;
    },
  ): Promise<unknown>;

  /** Tenant-scoped secondary-index lookup — RPC counterpart of
   * /v1/index-query. See CloudflareShardRpc.indexQuery. */
  indexQuery(
    tenantToken: string,
    body: { table: string; indexName: string; tenantId: string; values: Record<string, unknown>; limit?: number },
  ): Promise<unknown>;

  /** Tenant-scoped full table scan — RPC counterpart of /v1/table-scan. See
   * CloudflareShardRpc.tableScan. */
  tableScan(tenantToken: string, body: { tenantId: string; table: string; limit?: number; cursor?: string | null }): Promise<unknown>;

  /** Operator-only raw SQL — see the main repo's src/index.ts's sqlCore doc
   * comment for why this is admin-gated rather than tenant-gated (the
   * per-tenant SQL guard was structurally unwinnable and was removed).
   * Requires table+tenantId+partitionKey for deterministic single-shard
   * routing; a SELECT without partitionKey is rejected by core itself (use
   * scatter for fan-out reads). src/play.ts's playSql additionally enforces
   * read-only (SELECT/EXPLAIN only) before this is ever called. */
  sql(adminToken: string, body: { sql: string; params?: unknown[]; table: string; tenantId: string; partitionKey: string; requestId?: string }): Promise<unknown>;

  /** Operator-only cross-tenant fan-out read — admin-gated for the same
   * reason sql() is (reads across every tenant on every shard
   * indiscriminately). Core's scatterCore itself rejects a mutation with
   * 400 (SELECT only); src/play.ts's playScatter additionally scopes the
   * query to one demo table (see that file's extractScatterFromTable). */
  scatter(adminToken: string, body: { sql: string; params?: unknown[]; limit?: number }): Promise<unknown>;
}

export interface Env {
  /** Service binding to cloudflare-shard-mvp's CloudflareShardRpc entrypoint.
   * See wrangler.toml's [[services]] block for the binding + explanation. */
  SHARD_API: ShardApiBinding;

  /** Durable Object namespace for the single shared topology poller/fan-out.
   * See src/aggregator.ts. Always addressed via idFromName("singleton") —
   * there is exactly one aggregator instance for the whole Worker. */
  AGGREGATOR: DurableObjectNamespace;

  /** Durable Object namespace for the single shared load driver (Shardscope
   * T3). See src/load/load-driver.ts — a Worker-native TPC-C-style load engine
   * with a deterministic hot-shard skew mode. Always addressed via
   * idFromName("singleton"): one shared load run for the whole Worker, not one
   * per caller. */
  LOAD_DRIVER: DurableObjectNamespace;

  /** Durable Object namespace for the single shared tenant-token store
   * (Shardscope T5). See src/load/tenant-token-store.ts — durable
   * get-or-create storage for the per-warehouse tenant bearer tokens
   * LoadDriver needs to issue real /v1/* transactions. Always addressed via
   * idFromName("singleton"), same reasoning as AGGREGATOR/LOAD_DRIVER above.
   * A Durable Object (not a KV namespace) was chosen specifically because
   * get-or-create needs real atomicity — see tenant-token-store.ts's header
   * comment for why. */
  TENANT_TOKEN_STORE: DurableObjectNamespace;

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
