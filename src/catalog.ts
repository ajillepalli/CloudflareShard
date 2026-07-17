import { DurableObject } from "cloudflare:workers";
import { json } from "./http";
import { hashKey, pickRingSubstitute, indexShardIdForKey } from "./hash";
import { checkAdminAuth, sha256Hex, timingSafeEqual } from "./auth";
import { log } from "./log";
import { MIGRATE_PAGE_SIZE } from "./shard";
import { ensureCreateTableIfNotExists } from "./sql-safety";
import { IDENTIFIER_RE, UNSET_PARTITION_KEY_COLUMN } from "./structured-op";

// Review Tier 1 #6: how many reconcile passes ring evacuation makes to catch
// entries that raced onto the draining shard after the ring repoint before
// giving up (leaving them in place, unreachable-but-not-lost).
const RING_EVAC_RECONCILE_MAX_PASSES = 10;

const ADMIN_GATED_ROUTES = new Set([
  "/status",
  "/list-tables",
  "/drain-shard",
  "/init",
  "/register-table",
  "/split-vbucket",
  "/audit-log",
  "/register-tenant",
  "/revoke-tenant",
  "/set-partition-key-column",
  "/create-index",
  "/list-indexes",
  "/drop-index",
  "/list-tenants",
  "/vbucket-map",
  "/migrate-vbucket",
  "/migrate-vbucket-status",
  "/migrate-vbucket-abort",
  "/drain-shard-status",
  "/mark-table-provenance-complete",
  "/start-index-backfill",
  "/index-backfill-status",
]);

// Milestone 3, Chunk 4: cadence of the alarm-driven migration orchestration
// loop — each tick advances every in-flight migration one step (a bounded
// backfill slice, or one cutover attempt that may be waiting on the mirror
// queue to drain).
const MIGRATION_TICK_MS = 250;
// Topology-operation lock lease. Comfortably larger than one alarm tick
// (MIGRATION_TICK_MS) so a long operation heartbeating each tick never lets its
// own lease lapse, while a crashed operation's lock still auto-expires quickly
// enough that the cluster isn't wedged.
const TOPOLOGY_LOCK_TTL_MS = 30_000;
// Issue #26: /admin/register-tenant {rotate: true} used to invalidate the old
// token the instant the new one was issued, with zero overlap window -- a
// request already in flight with the old token, or a caller whose config/
// secret propagation hasn't picked up the freshly-rotated token yet, would
// hard-401. This window lets the OLD token keep working for a bounded time
// after rotation instead of indefinitely; short enough to still meaningfully
// bound how long a compromised/leaked old token stays valid post-rotation,
// long enough to cover in-flight requests and ordinary propagation delay.
// Explicit /admin/revoke-tenant is NOT softened by this -- it still
// invalidates everything (current AND any in-grace previous token)
// immediately, checked first in checkTenantAuth regardless of which hash a
// caller's token matches.
const TENANT_TOKEN_ROTATION_GRACE_MS = 5 * 60_000;
// Issue #33: /v1/table-scan is the only tenant-facing route that fans out to
// every shard in a tenant's catalog-shard pool (every other tenant route
// touches exactly one shard) -- an unbounded caller degrades every OTHER
// tenant sharing that shard pool, not just their own requests. Token bucket:
// TABLE_SCAN_RATE_LIMIT_CAPACITY bounds how many calls a tenant can burst
// before being throttled, TABLE_SCAN_RATE_LIMIT_REFILL_PER_SECOND bounds
// their sustained rate afterward. A tenant scanning at or below the refill
// rate is never limited; one bursting past capacity gets 429s until tokens
// refill. Chosen generously for a legitimate paginated-scan client (a
// multi-page scan loop looks like a short burst, not sustained abuse) while
// still bounding a runaway/malicious loop.
const TABLE_SCAN_RATE_LIMIT_CAPACITY = 20;
const TABLE_SCAN_RATE_LIMIT_REFILL_PER_SECOND = 2;
// Review Tier 2 #8: cap the backfill work per tick and back off the alarm
// re-arm when a tick throws, so a large migration resumes from its cursor
// rather than restarting-and-throwing at 4Hz forever.
const MIGRATION_BACKFILL_PAGES_PER_TICK = 8;
const MIGRATION_TICK_MAX_MS = 30000;
// Re-review: bound the cutover wait for the source's prepared 2PC intents to
// drain. A tx prepared-but-never-resolved (coordinator wedged on an
// unreachable participant, so the sweep sees it 'prepared' forever) would
// otherwise block this vbucket's cutover indefinitely with no operator
// signal. After this long, the migration surfaces a distinct
// 'cutover-blocked-on-prepared-intents' status via /migrate-vbucket-status
// (naming the txId) so an operator can /admin/tx-force-abort it. The tick
// keeps polling rather than aborting — a genuinely slow-but-live tx still
// completes on its own and clears the marker.
const CUTOVER_PREPARED_WAIT_MAX_MS = 30000;

// Codex live-deployment finding: bounds one index-backfill tick's work. Each
// row costs one subrequest to refresh its current value immediately before
// writing (Codex round 6: this can no longer be batched once per page --
// see advanceIndexBackfill's doc comment there) plus one subrequest to WRITE
// its index entry, plus one subrequest to scan the page itself -- so a
// tick's worst case is 2 * INDEX_BACKFILL_PAGE_SIZE + 1 subrequests,
// comfortably under Cloudflare's per-invocation cap with headroom for
// INDEX_RING_FENCED retries on a handful of rows.
const INDEX_BACKFILL_PAGE_SIZE = 250;
// Codex review P2 fix: re-heartbeat the topology lock partway through a full
// page's write loop, not just once before the tick starts. A full 250-row
// page's writes run serially (each one its own subrequest, some possibly
// retried on INDEX_RING_FENCED) -- if that genuinely takes longer than
// TOPOLOGY_LOCK_TTL_MS (30s), the lease could expire and another topology
// operation acquire it while this tick is still writing entries, reopening
// the exact race the lock exists to prevent. Mirrors the retired synchronous
// backfill's BACKFILL_HEARTBEAT_ROW_INTERVAL, just scoped to one tick's page
// instead of a whole shard's unbounded scan.
const INDEX_BACKFILL_HEARTBEAT_ROW_INTERVAL = 50;

/** Codex review P1 fix: distinguishes a backfill failure that CANNOT resolve
 * itself no matter how many times the alarm retries (e.g. a row with no
 * __cf_row_owners provenance entry -- only an explicit
 * /admin/backfill-provenance run, a genuine operator action, ever fixes
 * that) from an ordinary transient one (a network blip, contention, a
 * momentarily-fenced ring position) that's safe to retry forever with
 * backoff. Treating BOTH the same way -- which the alarm loop's shared
 * per-item catch, copied from migration's, originally did -- meant a
 * permanent failure left the topology lock held indefinitely: every later
 * tick would heartbeat the SAME lock, throw the SAME error, and never
 * release it, wedging every other create-index/drop-index/drain-shard/
 * split-vbucket/migrate-vbucket call behind TOPOLOGY_OPERATION_IN_PROGRESS
 * until an operator noticed and force-released it by hand -- strictly worse
 * than the retired synchronous path, which surfaced this exact condition as
 * an immediate, visible 409 and released its lock right away. */
class PermanentIndexBackfillError extends Error {}

type MigrationRow = {
  vbucket: number;
  shard_id: string;
  target_shard_id: string | null;
  migration_status: string;
  migration_rows_copied: number;
  topology_lock_operation_id?: string | null;
};

type IndexBackfillRow = {
  index_name: string;
  table_name: string;
  columns_json: string;
  placement_ring_json: string;
  backfill_shard_ids_json: string;
  backfill_shard_idx: number;
  backfill_after_pk: string;
  backfill_rows_copied: number;
  topology_lock_operation_id: string | null;
};

export class CatalogDO extends DurableObject {
  private readonly sql: SqlStorage;
  private readonly adminToken?: string;
  private readonly catalogEnv: Cloudflare.Env;
  private readonly routes: Record<string, (request: Request) => Promise<Response>>;
  /** See ShardDO.schemaEnsured — one schema pass per in-memory instance. */
  private schemaEnsured = false;
  /** Milestone 3, Chunk 4: DO handlers interleave at await points, so a
   * scheduled alarm() and any other concurrent invocation could both run
   * advanceMigration against the same vbucket on stale row snapshots —
   * worst case, a stale cutover tick observing post-flip state "detects" a
   * checksum mismatch and wipes just-migrated data. One tick at a time per
   * instance; a tick that finds the latch held just reschedules. */
  private migrationTickInFlight = false;
  /** Review Tier 2 #8: consecutive throwing orchestration ticks, for the
   * alarm re-arm backoff. Reset on a clean tick or when nothing's active. */
  private migrationTickFailureStreak = 0;

  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.catalogEnv = env;
    this.adminToken =
      typeof (env as { ADMIN_TOKEN?: unknown }).ADMIN_TOKEN === "string"
        ? (env as { ADMIN_TOKEN: string }).ADMIN_TOKEN
        : undefined;
    this.routes = {
      "/init": this.handleInit.bind(this),
      "/register-table": this.handleRegisterTable.bind(this),
      "/route": this.handleRoute.bind(this),
      "/route-batch": this.handleRouteBatch.bind(this),
      "/list-shards": this.handleListShards.bind(this),
      "/status": this.handleStatus.bind(this),
      "/list-tables": this.handleListTables.bind(this),
      "/audit-log": this.handleAuditLog.bind(this),
      "/drain-shard": this.handleDrainShard.bind(this),
      "/split-vbucket": this.handleSplitVbucket.bind(this),
      "/register-tenant": this.handleRegisterTenant.bind(this),
      "/revoke-tenant": this.handleRevokeTenant.bind(this),
      "/set-partition-key-column": this.handleSetPartitionKeyColumn.bind(this),
      "/create-index": this.handleCreateIndex.bind(this),
      "/list-indexes": this.handleListIndexes.bind(this),
      "/lookup-index": this.handleLookupIndex.bind(this),
      "/lookup-table-scan": this.handleLookupTableScan.bind(this),
      "/mark-table-provenance-complete": this.handleMarkTableProvenanceComplete.bind(this),
      "/drop-index": this.handleDropIndex.bind(this),
      "/mark-index-ready": this.handleMarkIndexReady.bind(this),
      "/start-index-backfill": this.handleStartIndexBackfill.bind(this),
      "/index-backfill-status": this.handleIndexBackfillStatus.bind(this),
      "/list-tenants": this.handleListTenants.bind(this),
      "/vbucket-map": this.handleVbucketMap.bind(this),
      "/migrate-vbucket": this.handleMigrateVbucket.bind(this),
      "/migrate-vbucket-status": this.handleMigrateVbucketStatus.bind(this),
      "/migrate-vbucket-abort": this.handleMigrateVbucketAbort.bind(this),
      "/drain-shard-status": this.handleDrainShardStatus.bind(this),
      "/update-index-ring": this.handleUpdateIndexRing.bind(this),
      "/index-ring": this.handleIndexRing.bind(this),
      "/acquire-topology-lock": this.handleAcquireTopologyLock.bind(this),
      "/heartbeat-topology-lock": this.handleHeartbeatTopologyLock.bind(this),
      "/release-topology-lock": this.handleReleaseTopologyLock.bind(this),
      "/topology-lock-status": this.handleTopologyLockStatus.bind(this),
      "/holds-topology-lock": this.handleHoldsTopologyLock.bind(this),
    };
  }

  /** Milestone 3, Chunk 4: CatalogDO drives ShardDO's internal migration
   * endpoints directly (export/import/fence/checksum) — a deliberate,
   * spec'd exception to the earlier "CatalogDO and ShardDO never call each
   * other" convention: the catalog owns the migration state machine (it
   * already owns vbucket_map), and orchestration from a stateless Worker
   * request would die with the request. */
  private async callShard(shardId: string, path: string, payload: unknown): Promise<Response> {
    const id = this.catalogEnv.SHARD.idFromName(shardId);
    const stub = this.catalogEnv.SHARD.get(id);
    return stub.fetch(`https://shard.internal${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
  }

  /** Add a column if a table predating it doesn't have it yet — CREATE TABLE IF
   * NOT EXISTS doesn't retroactively alter already-provisioned tables, so schema
   * additions need an explicit migration step. */
  private ensureColumn(table: string, column: string, definition: string): void {
    const existing = this.many<{ name: string }>(`PRAGMA table_info(${table})`);
    if (!existing.some((col) => col.name === column)) {
      this.sql.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    }
  }

  private ensureSchema(): void {
    if (this.schemaEnsured) return;
    this.schemaEnsured = true;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS cluster_config (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        total_vbuckets INTEGER NOT NULL,
        metadata_version INTEGER NOT NULL DEFAULT 1,
        initialized_at TEXT NOT NULL
      )
    `);
    this.ensureColumn("cluster_config", "catalog_shard_id", "TEXT");
    this.ensureColumn("cluster_config", "catalog_shard_count", "INTEGER");

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS shards (
        shard_id TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL
      )
    `);
    // Review Tier 2 #10: when a drain's vbucket migration is blocked by the
    // provenance gate, the drain parks here rather than re-scanning every
    // table on the source at the 250ms tick cadence forever. Cleared when the
    // operator re-invokes /admin/drain-shard (after /admin/backfill-provenance).
    this.ensureColumn("shards", "drain_stall_reason", "TEXT");
    // Approved design (Stage 3): the topology-lock operationId a drain of this
    // shard is holding for its ENTIRE multi-tick duration (heartbeated each
    // tick by advanceDrain, released only on full completion). NULL for a
    // drain never mediated by the Worker's /admin/drain-shard (e.g. tests that
    // call this DO's /drain-shard directly) — those keep behaving exactly as
    // before Stage 3 (no lock tracking).
    this.ensureColumn("shards", "topology_lock_operation_id", "TEXT");

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS vbucket_map (
        vbucket INTEGER PRIMARY KEY,
        shard_id TEXT NOT NULL,
        map_version INTEGER NOT NULL,
        updated_at TEXT NOT NULL
      )
    `);
    // Milestone 3, Chunk 3: per-vbucket migration state machine
    // (none|backfilling|cutover) plus the migration's target shard. While
    // status != 'none', /route returns {targetShardId, migrationStatus}
    // alongside the authoritative source shardId, and the gateway mirrors
    // every write to the target after source success (same requestId — the
    // target's applied_requests dedupe makes mirror + backfill + retry all
    // safely re-appliable in any order). Reads stay on source until Chunk
    // 4's cutover flips shard_id.
    this.ensureColumn("vbucket_map", "migration_status", "TEXT NOT NULL DEFAULT 'none'");
    this.ensureColumn("vbucket_map", "target_shard_id", "TEXT");
    // Chunk 4's status endpoint reports rowsCopied/startedAt; kept on the
    // same row rather than a separate migrations table — one migration per
    // vbucket at a time is an invariant (409 MIGRATION_IN_PROGRESS).
    this.ensureColumn("vbucket_map", "migration_rows_copied", "INTEGER NOT NULL DEFAULT 0");
    this.ensureColumn("vbucket_map", "migration_started_at", "TEXT");
    // Review Tier 2 #8: a persisted backfill cursor (current table + last
    // partition key copied) so a large vbucket's backfill resumes across
    // alarm ticks instead of restarting from page zero and re-exceeding the
    // per-invocation subrequest cap every 250ms forever.
    this.ensureColumn("vbucket_map", "backfill_table", "TEXT");
    this.ensureColumn("vbucket_map", "backfill_after_pk", "TEXT");
    // Re-review: cutover-entry timestamp and a stall marker, so the
    // prepared-2PC-intent cutover wait can be bounded and surfaced via
    // /migrate-vbucket-status instead of livelocking silently.
    this.ensureColumn("vbucket_map", "cutover_started_at", "TEXT");
    this.ensureColumn("vbucket_map", "cutover_stall_reason", "TEXT");
    // Codex review P1 (correctness): set only when a migration's target shard
    // is FRESHLY created (a split target that never received the create-table
    // fan-out). Its first backfill tick then provisions every registered
    // table's schema on it — including tables with zero rows in this vbucket,
    // which the row-export path alone would skip. A drain to an existing shard
    // (which already has every table) leaves this 0, so the backfill does NOT
    // re-issue a provision call per registered table per vbucket (that would
    // be O(tables x vbuckets) subrequests and swamp a single alarm tick).
    this.ensureColumn("vbucket_map", "provision_pending", "INTEGER NOT NULL DEFAULT 0");
    // Codex full-PR review P2: post-flip source cleanup (delete rows + unfence)
    // must be RETRYABLE. Set atomically with the map flip (status back to
    // 'none'); the alarm processes cleanup_pending=1 rows independently of
    // migration_status and clears the flag only once BOTH shard calls succeed,
    // so a failed unfence/delete can't leave the source permanently
    // stale-fenced or with undeleted rows while status reports 'complete'.
    // cleanup_source_shard_id remembers the old source (the flip nulls
    // target_shard_id and moves shard_id to the target).
    this.ensureColumn("vbucket_map", "cleanup_pending", "INTEGER NOT NULL DEFAULT 0");
    this.ensureColumn("vbucket_map", "cleanup_source_shard_id", "TEXT");
    // Approved design (Stage 3): the topology-lock operationId this migration
    // is holding from /admin/migrate-vbucket's (or /admin/split-vbucket's, or
    // a drain's phase-1 sub-migration's) START all the way through cutover AND
    // post-flip cleanup — heartbeated each tick, released only once cleanup
    // fully completes (or on /admin/migrate-vbucket-abort). NULL for a
    // migration never mediated by the Worker (e.g. tests that call this DO's
    // /migrate-vbucket or /split-vbucket directly) — those keep behaving
    // exactly as before Stage 3 (no lock tracking).
    this.ensureColumn("vbucket_map", "topology_lock_operation_id", "TEXT");

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS table_rules (
        table_name TEXT PRIMARY KEY,
        partitioning TEXT NOT NULL,
        created_at TEXT NOT NULL
      )
    `);
    // Mandatory going forward (enforced in handleRegisterTable), but existing
    // rows need a non-NULL default to backfill against — a bare NOT NULL with
    // no default fails immediately on ALTER TABLE against a table with rows.
    this.ensureColumn("table_rules", "partition_key_column", `TEXT NOT NULL DEFAULT '${UNSET_PARTITION_KEY_COLUMN}'`);
    // Milestone 3, Chunk 5: the table's CREATE TABLE statement, captured at
    // /admin/create-table — a shard created mid-life (split target) has none
    // of the tables that were fanned out at create-table time, so migration
    // backfill applies this to the target before importing. Nullable: a
    // table registered before this column existed simply can't be
    // auto-provisioned on new shards (operator applies schema manually).
    //
    // PR review round 11 (fundamental fix, replacing rounds 6-10's escalating
    // guard patches): schema_sql can only ever be trustworthy alongside a
    // probe-verified partition_key_unique=1 (see that column's comment below)
    // when the two were established TOGETHER, atomically, by
    // /admin/create-table's own push-then-verify flow (handleAdminCreateTable
    // in index.ts) — it pushes this exact DDL to every shard, THEN verifies
    // uniqueness against a shard that just received it, so the two are
    // structurally guaranteed to correspond. Every OTHER path that can set
    // partition_key_unique=1 — /admin/register-table's own live probe
    // (handleRegisterTable below) and /admin/set-partition-key-column
    // (handleSetPartitionKeyColumn below) — verifies against whatever
    // ALREADY physically exists on a live shard, completely disconnected
    // from whatever schema_sql text happens to be on file, and so NULLS
    // schema_sql in the same operation whenever it sets partition_key_unique
    // = 1. Trade-off: a table verified via one of those two routes (not
    // /admin/create-table) can be table-scan-eligible but has schema_sql =
    // NULL — a future split/migration backfill can't auto-provision it on a
    // new target shard from stored DDL; the table must already exist there
    // some other way, or an operator handles it manually.
    this.ensureColumn("table_rules", "schema_sql", "TEXT");
    // Tenant-scoped table scan (POST /v1/table-scan): cached provenance-
    // completeness flag, mirroring index_rules.status's building/ready cache.
    // 1 once a full-cluster (catalogShardId omitted) /admin/backfill-
    // provenance run has reported zero orphaned and zero ambiguous rows for
    // this table across every catalog shard and shard it touched; never reset
    // to 0 automatically (a completed table stays complete — only pre-
    // existing legacy rows can be unattributed, and every write since Chunk 0
    // is already provenance-tracked). Always starts at 0, including for a
    // table just created via /admin/create-table (PR review round 11, P2
    // fix): that route's DDL is CREATE TABLE IF NOT EXISTS, which silently
    // no-ops if the table name already physically existed with pre-existing
    // legacy rows never covered by __cf_row_owners — auto-certifying
    // provenance_complete=1 at creation time regardless (the old behavior)
    // would hide that collision behind a false `provenance.complete: true`
    // on /v1/table-scan. A genuinely brand-new (empty) table's first
    // /admin/backfill-provenance run trivially finds zero orphaned/ambiguous
    // rows and certifies it complete through this normal mechanism instead —
    // same end state, one extra (cheap, near-no-op) admin call.
    this.ensureColumn("table_rules", "provenance_complete", "INTEGER NOT NULL DEFAULT 0");
    // Codex P1 fix (cross-tenant table-scan leak): cached "is partitionKeyColumn
    // actually UNIQUE or the table's sole PRIMARY KEY" flag. __cf_row_owners
    // stores one owner per (table, partition_key VALUE); /tenant-scan-page's
    // JOIN matches on that value alone, so if two physical rows (any tenants)
    // ever share a partition-key value, the join — and thus /v1/table-scan —
    // would return both under whichever tenant currently owns that key. Set by
    // /admin/create-table and /admin/set-partition-key-column (the only two
    // places a table's partitionKeyColumn is established), each of which
    // verifies via PRAGMA introspection before setting this to 1.
    // Defaults to 0 (fail closed) — a table registered before this flag
    // existed is unverified and /v1/table-scan on it is rejected until
    // re-verified (no automatic backfill; out of scope for this fix).
    this.ensureColumn("table_rules", "partition_key_unique", "INTEGER NOT NULL DEFAULT 0");

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        endpoint TEXT NOT NULL,
        request_summary TEXT NOT NULL,
        created_at TEXT NOT NULL
      )
    `);

    // Tenant data-plane authorization. Isolates apps/environments within one
    // self-hosted deployment — not a multi-customer-SaaS boundary, since the
    // operator (ADMIN_TOKEN holder) and tenants both belong to the same
    // deploying developer in this milestone's distribution model.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS tenant_auth (
        tenant_id TEXT PRIMARY KEY,
        token_hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        revoked_at TEXT
      )
    `);
    // Issue #26: the previous token (and when its grace window expires),
    // populated only by a rotate: true call against an already-registered
    // tenant -- see TENANT_TOKEN_ROTATION_GRACE_MS's doc comment. Both NULL
    // for a tenant that's never been rotated.
    this.ensureColumn("tenant_auth", "previous_token_hash", "TEXT");
    this.ensureColumn("tenant_auth", "previous_token_expires_at", "TEXT");

    // Issue #33: per-tenant token-bucket state for /v1/table-scan's rate
    // limit -- the only fan-out-shaped tenant route (every other tenant
    // route touches exactly one shard). tokens is a float (fractional
    // refill accrues between calls, not just whole tokens); one row per
    // tenant that has ever called table-scan, created lazily on first use.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS tenant_scan_rate_limit (
        tenant_id TEXT PRIMARY KEY,
        tokens REAL NOT NULL,
        last_refill_at TEXT NOT NULL
      )
    `);

    // Milestone 2 (Index Service). One row per registered secondary index.
    // No tenant scoping here — matches table_rules/base-table rows, which
    // also carry no tenant_id (see docs/SPEC.md §14's documented trust
    // model). columns_json is a JSON array to support composite indexes.
    // status starts 'building' (set the moment the Worker registers, before
    // backfill runs) and flips to 'ready' only once backfill has fully
    // completed (see handleMarkIndexReady) — /lookup-index (and therefore
    // /v1/index-query) rejects reads against a 'building' index rather than
    // silently returning partial results for rows backfill hasn't reached
    // yet. Write-path maintenance (async /v1/mutate, /v1/tx piggyback) is
    // NOT gated on status — it's supposed to be live from the moment of
    // registration, that's what makes registering before backfill correct.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS index_rules (
        index_name TEXT PRIMARY KEY,
        table_name TEXT NOT NULL,
        columns_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'building',
        created_at TEXT NOT NULL
      )
    `);
    // Milestone 3, Chunk 2: the ordered shard-id array active at the moment
    // this index was created (/admin/create-index), pinned for the index's
    // entire lifetime — indexShardIdForKey (hash.ts) hashes over THIS array,
    // never the live/current active shard set, so /admin/split-vbucket
    // (grows the active set) and /admin/drain-shard (shrinks it, modulo
    // Chunk 5's ring-evacuation rule) can't silently orphan existing
    // __cf_indexes entries by changing the modulo out from under them.
    // Default '[]' only matters for a row that predates this column (would
    // need one of the old blockIfIndexesExist-era indexes recreated via the
    // documented drop-index/create-index upgrade flow anyway).
    this.ensureColumn("index_rules", "placement_ring_json", "TEXT NOT NULL DEFAULT '[]'");
    // Codex round-14 P2 (read-visibility during evacuation): the draining
    // shard(s) whose ring position is mid-evacuation for this index. Set
    // ATOMICALLY with placement_ring_json by /update-index-ring, so wherever the
    // ring is repointed to the substitute this shadow marks the old shard whose
    // not-yet-copied entries must ALSO be read. /v1/index-query dual-looks-up
    // both while it is non-empty; cleared on evacuation completion. Replicated
    // to every catalog shard (via the same fan-out) so /lookup-index — which the
    // gateway routes to the TENANT's catalog, not necessarily the draining one —
    // can always answer it.
    this.ensureColumn("index_rules", "evac_from_shards_json", "TEXT NOT NULL DEFAULT '[]'");

    // Codex live-deployment finding: /admin/create-index used to backfill
    // every existing row SYNCHRONOUSLY, inside the one HTTP invocation that
    // registered the index -- 2+ subrequests per row (a fresh re-read plus
    // an index-entry write), no batching, no chunking. A moderate table
    // (a few thousand rows) exceeds Cloudflare's per-invocation subrequest
    // cap outright, leaving the index wedged at status='building' forever
    // with no way to retry past the same wall. Converted to the SAME
    // alarm-driven, persisted-cursor pattern vbucket migration already uses
    // (see vbucket_map's backfill_table/backfill_after_pk and this file's
    // MIGRATION_TICK_MS-driven alarm()) -- only CATALOG-0's copy of a given
    // index's row ever has non-empty backfill_shard_ids_json (see
    // handleStartIndexBackfill); every other catalog shard's replica keeps
    // these at their defaults, so the alarm's backfill loop naturally only
    // ever does work on catalog-0, the single driver, exactly mirroring how
    // /create-index's OWN backfill was always a single global operation
    // before this fix, never a per-catalog one.
    this.ensureColumn("index_rules", "backfill_shard_ids_json", "TEXT NOT NULL DEFAULT '[]'");
    this.ensureColumn("index_rules", "backfill_shard_idx", "INTEGER NOT NULL DEFAULT 0");
    this.ensureColumn("index_rules", "backfill_after_pk", "TEXT NOT NULL DEFAULT ''");
    this.ensureColumn("index_rules", "backfill_rows_copied", "INTEGER NOT NULL DEFAULT 0");
    this.ensureColumn("index_rules", "topology_lock_operation_id", "TEXT");

    // Codex round-13 fix: per-(draining shard, index) ring-evacuation progress.
    // A row means "index_name's ring is being evacuated off shard_id onto
    // substitute; not yet complete." Ring evacuation now FENCES the index on the
    // draining shard and repoints EARLY (the fence stops new writes to that
    // shard), so the ring no longer contains the draining shard while evacuation
    // is still in flight — ringsToEvacuate (ring membership) can no longer be
    // the source of truth for "revisit this index next tick". This marker is:
    // advanceDrain revisits every marker for the shard until it deletes the row
    // on completion. Survives ticks; drives /drain-shard-status too.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS drain_ring_evac (
        shard_id   TEXT NOT NULL,
        index_name TEXT NOT NULL,
        substitute TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (shard_id, index_name)
      )
    `);

    // Durable cluster-wide TOPOLOGY-OPERATION LOCK (approved design). A single
    // row, held on catalog-0 (the canonical cluster-wide catalog — index_rules
    // replication and ring re-resolution already treat it as authoritative),
    // serializes every topology mutation (split, migrate, create/drop index,
    // drain) so concurrent operations can't corrupt shared ring/shadow/map
    // state. Lease-based with a TTL refreshed by heartbeat, so a crashed
    // operation's lock auto-expires instead of blocking forever.
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS topology_lock (
        singleton      INTEGER PRIMARY KEY CHECK (singleton = 1),
        operation_id   TEXT NOT NULL,
        operation_type TEXT NOT NULL,
        acquired_at    TEXT NOT NULL,
        expires_at     TEXT NOT NULL,
        heartbeat_at   TEXT NOT NULL
      )
    `);
  }

  private audit(endpoint: string, requestSummary: Record<string, unknown>): void {
    log("catalog.admin_action", { endpoint, ...requestSummary });
    this.sql.exec(
      `INSERT INTO audit_log (endpoint, request_summary, created_at) VALUES (?, ?, ?)`,
      endpoint,
      JSON.stringify(requestSummary),
      new Date().toISOString(),
    );
  }

  private one<T extends object>(sql: string, ...params: unknown[]): T | null {
    const cursor = this.sql.exec(sql, ...params);
    for (const row of cursor) {
      return row as T;
    }
    return null;
  }

  private many<T extends object>(sql: string, ...params: unknown[]): T[] {
    return Array.from(this.sql.exec(sql, ...params)) as T[];
  }

  private metadataVersion(): number {
    const config = this.one<{ metadata_version: number }>(
      "SELECT metadata_version FROM cluster_config WHERE singleton = 1",
    );
    return config?.metadata_version ?? 1;
  }

  private bumpMetadataVersion(): number {
    this.sql.exec(
      `
      UPDATE cluster_config
      SET metadata_version = metadata_version + 1
      WHERE singleton = 1
      `,
    );
    return this.metadataVersion();
  }

  async fetch(request: Request): Promise<Response> {
    try {
      return await this.handle(request);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log("catalog.unhandled_error", { path: new URL(request.url).pathname, message });
      return json({ error: "Internal error." }, 500);
    }
  }

  private async handle(request: Request): Promise<Response> {
    this.ensureSchema();

    const url = new URL(request.url);
    const method = request.method.toUpperCase();

    if (method !== "POST") {
      return json({ error: "Only POST allowed for catalog endpoints." }, 405);
    }

    if (ADMIN_GATED_ROUTES.has(url.pathname)) {
      const authError = checkAdminAuth(this.adminToken, request);
      if (authError) {
        return json({ error: authError.error }, authError.status);
      }
    }

    const handler = this.routes[url.pathname];
    if (handler) {
      return handler(request);
    }

    return json({ error: `Unknown catalog route: ${url.pathname}` }, 404);
  }

  private async handleInit(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      numShards?: number;
      totalVBuckets?: number;
      force?: boolean;
      catalogShardId?: string;
      catalogShardCount?: number;
    };

    // Reject non-finite values before clamping — Math.max/min silently
    // propagate NaN (e.g. a non-numeric JSON value), which would zero out
    // the shard-creation loop below while the vbucket loop still assigns
    // every vbucket to "shard-NaN", corrupting the cluster with a 200 OK.
    if (body.numShards !== undefined && !Number.isFinite(body.numShards)) {
      return json({ error: "numShards must be a finite number." }, 400);
    }
    if (body.totalVBuckets !== undefined && !Number.isFinite(body.totalVBuckets)) {
      return json({ error: "totalVBuckets must be a finite number." }, 400);
    }

    // Ceilings prevent a single admin call from creating a pathologically
    // large cluster that exhausts this DO's CPU/time budget mid-loop (the
    // shard/vbucket population loops below have no batching or rollback).
    const numShards = Math.min(256, Math.max(1, body.numShards ?? 8));
    const totalVBuckets = Math.min(65536, Math.max(64, body.totalVBuckets ?? 1024));
    const force = body.force === true;
    const catalogShardId = body.catalogShardId ?? null;
    const catalogShardCount = body.catalogShardCount ?? null;
    const shardPrefix = catalogShardId ? `${catalogShardId}-` : "";

    this.audit("/init", { numShards, totalVBuckets, force, catalogShardId });

    const existing = this.one<{ total_vbuckets: number }>(
      "SELECT total_vbuckets FROM cluster_config WHERE singleton = 1",
    );
    if (existing && !force) {
      return json({
        ok: true,
        alreadyInitialized: true,
        totalVBuckets: existing.total_vbuckets,
      });
    }

    if (force) {
      this.sql.exec("DELETE FROM vbucket_map");
      this.sql.exec("DELETE FROM shards");
      this.sql.exec("DELETE FROM cluster_config");
    }

    this.sql.exec(
      `
      INSERT OR REPLACE INTO cluster_config (singleton, total_vbuckets, metadata_version, initialized_at, catalog_shard_id, catalog_shard_count)
      VALUES (1, ?, 1, ?, ?, ?)
      `,
      totalVBuckets,
      new Date().toISOString(),
      catalogShardId,
      catalogShardCount,
    );

    for (let i = 0; i < numShards; i += 1) {
      const shardId = `${shardPrefix}shard-${i}`;
      this.sql.exec(
        `
        INSERT OR IGNORE INTO shards (shard_id, status, created_at)
        VALUES (?, 'active', ?)
        `,
        shardId,
        new Date().toISOString(),
      );
    }

    for (let vb = 0; vb < totalVBuckets; vb += 1) {
      const shardId = `${shardPrefix}shard-${vb % numShards}`;
      this.sql.exec(
        `
        INSERT OR REPLACE INTO vbucket_map (vbucket, shard_id, map_version, updated_at)
        VALUES (?, ?, 1, ?)
        `,
        vb,
        shardId,
        new Date().toISOString(),
      );
    }

    return json({ ok: true, numShards, totalVBuckets, catalogShardId });
  }

  private async handleRegisterTable(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      table: string;
      partitioning?: string;
      partitionKeyColumn?: string;
      // PR review round 11: string | null | undefined, not a bare
      // `schemaSql?: string` — the three states are distinguishable and each
      // means something different below. See schemaSqlToStore's comment for
      // the full contract (omitted = preserve, null = explicit clear, string
      // = store this).
      schemaSql?: string | null;
      provenanceComplete?: boolean;
      partitionKeyUnique?: boolean;
    };

    if (!body.table) {
      return json({ error: "Missing table" }, 400);
    }
    if (!body.partitionKeyColumn) {
      return json(
        {
          error: {
            code: "MISSING_PARTITION_KEY_COLUMN",
            message: "Missing partitionKeyColumn.",
            fix: "Provide the column name that holds each row's partition key.",
          },
        },
        400,
      );
    }

    this.audit("/register-table", {
      table: body.table,
      partitioning: body.partitioning,
      partitionKeyColumn: body.partitionKeyColumn,
    });

    // provenance_complete is monotonic (see ensureSchema's comment above) —
    // INSERT OR REPLACE below would otherwise silently reset an
    // already-completed table back to 0 if /register-table is ever called
    // again for it (e.g. a manual metadata-only re-registration). Preserve
    // "already complete" across a re-register; only handleAdminCreateTable's
    // fan-out (via provenanceComplete: true, a brand-new table) sets it fresh.
    const existing = this.one<{ provenance_complete: number; partition_key_column: string; schema_sql: string | null; partition_key_unique: number }>(
      "SELECT provenance_complete, partition_key_column, schema_sql, partition_key_unique FROM table_rules WHERE table_name = ?",
      body.table,
    );
    const provenanceComplete = existing?.provenance_complete === 1 || body.provenanceComplete === true ? 1 : 0;
    // PR review round 7: INSERT OR REPLACE below would otherwise let this
    // endpoint silently repoint an already-configured table's
    // partition_key_column to a different value — the SAME vulnerability
    // round 6 closed for /admin/set-partition-key-column (see that handler's
    // comment for the full stale-__cf_row_owners-provenance/cross-tenant
    // rationale). Allow the call through when there's no existing row yet
    // (first-ever registration), when the existing value is still the
    // '__unset__' sentinel (the legitimate upgrade path), or when the new
    // value is IDENTICAL to what's already set (idempotent re-registration,
    // e.g. metadata-only re-sync). Only reject when a real, differing value
    // would overwrite an existing real value.
    if (
      existing &&
      existing.partition_key_column !== UNSET_PARTITION_KEY_COLUMN &&
      existing.partition_key_column !== body.partitionKeyColumn
    ) {
      return json(
        {
          error: {
            code: "PARTITION_KEY_ALREADY_SET",
            message: `Table ${body.table} already has a configured partition key column (${existing.partition_key_column}).`,
            fix: "Registering again with the same partitionKeyColumn is fine, but repointing to a different column is not supported: it would leave existing row-ownership provenance keyed under the old column's values.",
          },
        },
        409,
      );
    }
    // partition_key_unique is NOT preserved like provenance_complete above —
    // it's a property of the CURRENT partitionKeyColumn, which can itself
    // change (via /set-partition-key-column), so a re-register must take
    // whatever verification result the caller just computed for the
    // partitionKeyColumn it's registering, defaulting to 0 (unverified) if
    // the caller didn't verify (e.g. /admin/register-table's raw passthrough).
    const partitionKeyUnique = body.partitionKeyUnique === true ? 1 : 0;

    // PR review round 11 (fundamental fix, replacing rounds 8/9's reject/
    // preserve guard pair): schema_sql is exactly what a future split/
    // migration backfill executes verbatim to provision this table on a
    // freshly-created target shard (see ensureSchema's comment above the
    // schema_sql column) — it can only ever be trustworthy alongside a
    // probe-verified partition_key_unique=1 when the two were established
    // TOGETHER, atomically, by /admin/create-table's own push-then-verify
    // flow (handleAdminCreateTable, which always sends its own just-pushed
    // schema string here). This route's partitionKeyUnique (computed by the
    // Worker's handleAdminRegisterTable, whenever it runs its own live-shard
    // probe) verifies against whatever ALREADY physically exists right now —
    // structurally disconnected from whatever schema_sql text this or a
    // PRIOR call stored. Rounds 8/9 tried to protect that gap by rejecting a
    // differing schema_sql once verified, and preserving it across an
    // omitted one — but that still let a probe-verified partition_key_unique
    // = 1 end up paired with a schema_sql from an earlier, unrelated call
    // (finding 1) and let an empty-string schemaSql slip through ungated
    // (finding 2). Simpler invariant: schema_sql can never be non-null
    // alongside a partition_key_unique=1 that THIS route's own probe just
    // produced — handleAdminRegisterTable is responsible for signaling which
    // of three things it wants:
    //   - omit the "schemaSql" property entirely → preserve whatever's
    //     already stored, untouched (used when this call didn't just verify
    //     uniqueness, so there's nothing unsafe about leaving it as-is);
    //   - "schemaSql": null → explicitly clear it (used when this call's own
    //     probe just verified partition_key_unique=1 — the caller has no
    //     opinion on whether the stored text corresponds, so it must not be
    //     preserved);
    //   - "schemaSql": "<a string>" → store it as submitted (this call
    //     supplied real schema text of its own).
    // A bare `??` can't tell "omitted" apart from "explicitly null" (both
    // are nullish), hence the explicit `in` presence check rather than a
    // fallback chain.
    const schemaSqlToStore = "schemaSql" in body ? (body.schemaSql ?? null) : (existing?.schema_sql ?? null);

    this.sql.exec(
      `
      INSERT OR REPLACE INTO table_rules (table_name, partitioning, partition_key_column, created_at, schema_sql, provenance_complete, partition_key_unique)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      `,
      body.table,
      body.partitioning ?? "hash",
      body.partitionKeyColumn,
      new Date().toISOString(),
      schemaSqlToStore,
      provenanceComplete,
      partitionKeyUnique,
    );

    const version = this.bumpMetadataVersion();
    return json({ ok: true, table: body.table, metadataVersion: version });
  }

  private async handleSetPartitionKeyColumn(request: Request): Promise<Response> {
    const body = (await request.json()) as { table?: string; partitionKeyColumn?: string; partitionKeyUnique?: boolean };
    if (!body.table || !body.partitionKeyColumn) {
      return json(
        {
          error: {
            code: "MISSING_FIELDS",
            message: "Missing table or partitionKeyColumn.",
            fix: "Provide both table and partitionKeyColumn.",
          },
        },
        400,
      );
    }

    const existing = this.one<{ table_name: string; partition_key_column: string }>(
      "SELECT table_name, partition_key_column FROM table_rules WHERE table_name = ?",
      body.table,
    );
    if (!existing) {
      return json(
        { error: { code: "TABLE_NOT_REGISTERED", message: `Table ${body.table} is not registered.`, fix: "Call /register-table first." } },
        404,
      );
    }
    // PR review round 6: this endpoint is a ONE-TIME unset->set upgrade path
    // only (for tables carrying the __unset__ sentinel from before
    // partition-key-column validation existed), never a general "repoint an
    // already-working table's partition key column" operation. Re-invoking it
    // on a table that already has a real partition_key_column would silently
    // repoint table_rules while leaving __cf_row_owners' existing entries
    // keyed under the OLD column's values — /tenant-scan-page would then
    // enumerate those stale partition_key values but look up base rows via
    // `WHERE "<NEW_column>" = ?`, a cross-tenant data leak if an unrelated row
    // happens to share that value under the new column. There is no
    // legitimate use case for repointing an already-configured column, so
    // reject outright rather than attempting to safely migrate/clear
    // provenance.
    if (existing.partition_key_column !== UNSET_PARTITION_KEY_COLUMN) {
      return json(
        {
          error: {
            code: "PARTITION_KEY_ALREADY_SET",
            message: `Table ${body.table} already has a configured partition key column (${existing.partition_key_column}).`,
            fix: "This endpoint only upgrades a table from the '__unset__' sentinel; it cannot repoint an already-configured partition key column.",
          },
        },
        409,
      );
    }

    this.audit("/set-partition-key-column", { table: body.table, partitionKeyColumn: body.partitionKeyColumn });
    // Changing the partitionKeyColumn invalidates any previously-cached
    // uniqueness verification (it was for the OLD column) — always take the
    // caller's freshly-computed value for the NEW column, defaulting to 0
    // (unverified) if it didn't verify.
    const partitionKeyUnique = body.partitionKeyUnique === true ? 1 : 0;
    // PR review round 11: this is the OTHER route (besides
    // handleRegisterTable's own probe) that can independently set
    // partition_key_unique=1 via a live-shard check — same as that route,
    // there's no way to guarantee whatever schema_sql happens to already be
    // on file corresponds to the column THIS call just verified, so it must
    // be nulled in the SAME statement whenever partitionKeyUnique verifies
    // true. Left untouched when it's 0 — nothing unsafe about an unverified
    // table keeping whatever schema text is on file, since
    // /admin/create-table's push-then-verify flow is the only path allowed
    // to pair a real schema_sql with partition_key_unique=1.
    if (partitionKeyUnique === 1) {
      this.sql.exec(
        "UPDATE table_rules SET partition_key_column = ?, partition_key_unique = ?, schema_sql = NULL WHERE table_name = ?",
        body.partitionKeyColumn,
        partitionKeyUnique,
        body.table,
      );
    } else {
      this.sql.exec(
        "UPDATE table_rules SET partition_key_column = ?, partition_key_unique = ? WHERE table_name = ?",
        body.partitionKeyColumn,
        partitionKeyUnique,
        body.table,
      );
    }

    const version = this.bumpMetadataVersion();
    return json({ ok: true, table: body.table, partitionKeyColumn: body.partitionKeyColumn, metadataVersion: version });
  }

  /** Registers index metadata only — does not touch physical shard data.
   * The Worker orchestrates the physical side (creating __cf_indexes on
   * every shard, running backfill) before calling this, mirroring how
   * /admin/create-table applies the shard-level schema before registering
   * in table_rules (src/index.ts's handleAdminCreateTable). */
  private async handleCreateIndex(request: Request): Promise<Response> {
    const body = (await request.json()) as { indexName?: string; table?: string; columns?: string[]; placementRing?: string[] };
    if (!body.indexName || !body.table || !body.columns || body.columns.length === 0) {
      return json(
        {
          error: {
            code: "MISSING_FIELDS",
            message: "Missing indexName, table, or columns.",
            fix: "Provide indexName, table, and a non-empty columns array.",
          },
        },
        400,
      );
    }
    if (!IDENTIFIER_RE.test(body.indexName)) {
      return json(
        { error: { code: "UNSAFE_IDENTIFIER", message: "indexName is not a valid identifier." } },
        400,
      );
    }
    for (const col of body.columns) {
      if (!IDENTIFIER_RE.test(col)) {
        return json(
          { error: { code: "UNSAFE_IDENTIFIER", message: `Unsafe identifier in columns: ${col}` } },
          400,
        );
      }
    }

    const table = this.one<{ table_name: string }>("SELECT table_name FROM table_rules WHERE table_name = ?", body.table);
    if (!table) {
      return json(
        { error: { code: "TABLE_NOT_REGISTERED", message: `Table ${body.table} is not registered.`, fix: "Call /admin/create-table first." } },
        404,
      );
    }

    // Idempotent on retry with the SAME table+columns — the Worker registers
    // BEFORE backfilling (not after), specifically so a retry after a
    // partial backfill failure can call this again rather than getting
    // stuck behind a 409 for an index that's already (partially) there.
    // Genuinely different table/columns for the same indexName is still a
    // real conflict, not a retry.
    const existing = this.one<{ table_name: string; columns_json: string }>(
      "SELECT table_name, columns_json FROM index_rules WHERE index_name = ?",
      body.indexName,
    );
    if (existing) {
      const sameDefinition = existing.table_name === body.table && existing.columns_json === JSON.stringify(body.columns);
      if (sameDefinition) {
        return json({ ok: true, indexName: body.indexName, table: body.table, columns: body.columns });
      }
      return json(
        { error: { code: "INDEX_ALREADY_REGISTERED", message: `Index ${body.indexName} is already registered with a different table/columns.` } },
        409,
      );
    }

    this.audit("/create-index", { indexName: body.indexName, table: body.table, columns: body.columns, placementRing: body.placementRing });
    this.sql.exec(
      "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'building', ?, ?)",
      body.indexName,
      body.table,
      JSON.stringify(body.columns),
      new Date().toISOString(),
      JSON.stringify(body.placementRing ?? []),
    );

    return json({ ok: true, indexName: body.indexName, table: body.table, columns: body.columns });
  }

  /** Shared by handleMarkIndexReady and handleDropIndex — both need to
   * confirm an index is registered before acting, and return the identical
   * 404 shape if it isn't. Returns null (not a Response) when found, so
   * callers can `if (!existing) return ...` on a real row without an extra
   * unwrap. */
  private requireIndexRule(indexName: string): { found: true } | { found: false; response: Response } {
    const existing = this.one<{ index_name: string }>("SELECT index_name FROM index_rules WHERE index_name = ?", indexName);
    if (!existing) {
      return {
        found: false,
        response: json({ error: { code: "INDEX_NOT_REGISTERED", message: `Index ${indexName} is not registered.` } }, 404),
      };
    }
    return { found: true };
  }

  /** Eng-review fix: flips an index from 'building' to 'ready' once its
   * backfill has fully completed. Originally called by the Worker (as the
   * last step of a synchronous /admin/create-index, admin-gated like any
   * other Worker-facing route) — now called ONLY catalog-to-catalog, by
   * fanMarkIndexReady, once catalog-0's alarm-driven backfill (see
   * advanceIndexBackfill) finishes every shard. Codex live-deployment
   * finding: removed from ADMIN_GATED_ROUTES accordingly — a DO-to-DO fetch
   * has no admin token to present, and (now that the Worker never calls this
   * route at all) there's nothing left for that gate to protect; same
   * DO-binding-only trust model handleUpdateIndexRing's sibling fan-out
   * already uses. Before this flips, /lookup-index (and therefore
   * /v1/index-query) rejects reads against the index rather than silently
   * returning partial results for rows backfill hasn't reached yet. */
  private async handleMarkIndexReady(request: Request): Promise<Response> {
    const body = (await request.json()) as { indexName?: string };
    if (!body.indexName) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing indexName." } }, 400);
    }
    const rule = this.requireIndexRule(body.indexName);
    if (!rule.found) return rule.response;
    this.sql.exec("UPDATE index_rules SET status = 'ready' WHERE index_name = ?", body.indexName);
    return json({ ok: true, indexName: body.indexName });
  }

  /** Codex live-deployment finding: starts (or resumes, if already stamped —
   * idempotent, matching /create-index's own retry contract) catalog-0's
   * alarm-driven backfill for an already-registered index. Only ever called
   * by the Worker against catalog-0 specifically (never fanned out) — see
   * this file's index_rules schema comment for why only catalog-0's row
   * ever carries non-empty backfill_shard_ids_json. `backfillShardIds` is
   * the data-holding shard set (active + draining) the Worker captured at
   * /admin/create-index time; `operationId` is the topology lock the
   * Worker acquired for "create-index" and is HANDING OFF here (mirroring
   * split-vbucket's Stage 3 hand-off) — this DO now owns heartbeating and
   * eventually releasing it, across as many alarm ticks as backfill needs. */
  private async handleStartIndexBackfill(request: Request): Promise<Response> {
    const body = (await request.json()) as { indexName?: string; backfillShardIds?: string[]; operationId?: string };
    if (!body.indexName || !body.backfillShardIds) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing indexName or backfillShardIds." } }, 400);
    }
    const rule = this.requireIndexRule(body.indexName);
    if (!rule.found) return rule.response;
    // Live-deployment finding (round 2): a retry of an already-'ready' index
    // (idempotent registration matching an index whose backfill already
    // finished) used to still report handedOff-equivalent success
    // unconditionally, which made the WORKER's wrapper (adminCreateIndexCore)
    // treat it as "backfill just started, catalog-0 now owns the lock" and
    // never release the lock IT had just acquired for THIS call -- since
    // there's nothing left to hand off to (backfill is already done), that
    // lock then leaked forever, wedging every future topology operation
    // until an operator manually /admin/force-release-topology-lock'd it.
    // Checking status FIRST and skipping the UPDATE/alarm-arm entirely for an
    // already-'ready' index closes this: handedOff:false tells the Worker
    // there's nothing to hand off, so IT releases the lock it just acquired,
    // exactly like split-vbucket's own "release if nothing was actually
    // started" convention.
    const current = this.one<{ status: string }>("SELECT status FROM index_rules WHERE index_name = ?", body.indexName);
    if (current?.status === "ready") {
      return json({ ok: true, indexName: body.indexName, status: "ready", handedOff: false });
    }
    // Codex review P1 fix: a 'failed' index (backfill hit a PermanentIndex-
    // BackfillError -- e.g. a provenance gap -- and gave up, see alarm()'s
    // catch) is a genuine RETRY target once the operator fixes the
    // underlying issue, not a dead end. Resetting status back to 'building'
    // re-arms the alarm.
    //
    // Codex round-3/5 fix: the failed attempt's backfill_shard_idx/
    // backfill_after_pk cursor was ORIGINALLY preserved here so a retry could
    // resume instead of rescanning every shard -- but the topology lock is
    // released the moment a backfill gives up (see alarm()'s catch), and the
    // gap between that release and this retry is exactly when an operator
    // (or anyone else) is free to run /admin/split-vbucket, /admin/drain-
    // shard, or /admin/migrate-vbucket. Round 3 caught the case where that
    // changes WHICH shards exist (comparing the old vs. new backfillShardIds
    // list); round 5 found a narrower one it missed -- migrating a vbucket
    // moves rows between shards that were BOTH already in the list, with the
    // list itself unchanged, so the list-comparison saw nothing to reset for.
    // Reliably distinguishing "safe to resume" from "unsafe" would mean
    // tracking a full topology/vbucket-map version stamp across the release
    // window, just to save re-scanning a few already-completed shards on an
    // operator-driven, rare, non-hot-path retry. Not worth the complexity or
    // the risk of missing a THIRD variant of the same problem: always start
    // a retried-from-'failed' backfill over from shard 0. Every write here is
    // idempotent (INSERT OR REPLACE index entries), so re-scanning already-
    // indexed shards costs some redundant work, never incorrect results.
    this.sql.exec(
      "UPDATE index_rules SET status = 'building', backfill_shard_ids_json = ?, backfill_shard_idx = 0, backfill_after_pk = '', topology_lock_operation_id = ? WHERE index_name = ? AND status IN ('building', 'failed')",
      JSON.stringify(body.backfillShardIds),
      body.operationId ?? null,
      body.indexName,
    );
    const soon = Date.now() + MIGRATION_TICK_MS;
    const existingAlarm = await this.ctx.storage.getAlarm();
    if (existingAlarm === null || existingAlarm > soon) {
      await this.ctx.storage.setAlarm(soon);
    }
    return json({ ok: true, indexName: body.indexName, status: "building", handedOff: true });
  }

  /** Observability for the alarm-driven backfill, modeled directly on
   * handleMigrateVbucketStatus. Status stays 'building' (index_rules'
   * existing status column) for the whole backfill; this endpoint adds the
   * progress detail migrate-vbucket-status already gives migrations
   * (rowsCopied, current shard cursor) so an operator isn't left guessing
   * whether a 'building' index is progressing or wedged. */
  private async handleIndexBackfillStatus(request: Request): Promise<Response> {
    const body = (await request.json()) as { indexName?: string };
    if (!body.indexName) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing indexName." } }, 400);
    }
    const row = this.one<IndexBackfillRow & { status: string }>(
      "SELECT index_name, table_name, columns_json, placement_ring_json, backfill_shard_ids_json, backfill_shard_idx, backfill_after_pk, backfill_rows_copied, topology_lock_operation_id, status FROM index_rules WHERE index_name = ?",
      body.indexName,
    );
    if (!row) {
      return json({ error: { code: "INDEX_NOT_REGISTERED", message: `Index ${body.indexName} is not registered.` } }, 404);
    }
    const shardIds = JSON.parse(row.backfill_shard_ids_json) as string[];
    return json({
      indexName: row.index_name,
      table: row.table_name,
      status: row.status,
      rowsCopied: row.backfill_rows_copied,
      totalShards: shardIds.length,
      currentShardIndex: row.backfill_shard_idx,
      currentShardId: shardIds[row.backfill_shard_idx] ?? null,
    });
  }

  /** Milestone 2, Chunk 6. Unregisters the index — the Worker calls this
   * BEFORE fanning out physical __cf_indexes cleanup, so any /v1/index-query
   * or /lookup-index call that starts after this returns sees the index as
   * gone immediately (404 INDEX_NOT_REGISTERED), even while physical
   * cleanup is still in flight across shards. A write already in progress
   * when this runs may still land one last __cf_indexes row after physical
   * cleanup passes over it — a known, accepted eventual-consistency window,
   * not a correctness gap this milestone closes (DROP INDEX is a rare admin
   * operation, not a hot path). */
  private async handleDropIndex(request: Request): Promise<Response> {
    const body = (await request.json()) as { indexName?: string };
    if (!body.indexName) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing indexName." } }, 400);
    }
    const rule = this.requireIndexRule(body.indexName);
    if (!rule.found) return rule.response;
    this.audit("/drop-index", { indexName: body.indexName });
    this.sql.exec("DELETE FROM index_rules WHERE index_name = ?", body.indexName);
    return json({ ok: true, indexName: body.indexName });
  }

  /** Codex round-13 fix (internal, DO-binding only, un-gated like /list-shards):
   * returns one index's CURRENT pinned placement ring. Used to RE-RESOLVE an
   * index write's target after an INDEX_RING_FENCED rejection — the gateway's
   * best-effort index write and a base shard's index_pending_jobs retry both
   * fetch the live ring here (index_rules is replicated identically to every
   * catalog shard, so any shard — canonically catalog-0 — can answer) and
   * recompute placement onto the substitute. Returns an empty ring for an
   * unknown index rather than erroring, so a caller can fall back safely. */
  private async handleIndexRing(request: Request): Promise<Response> {
    const body = (await request.json()) as { indexName?: string };
    if (!body.indexName) {
      return json({ error: "Missing indexName" }, 400);
    }
    const row = this.one<{ placement_ring_json: string }>(
      "SELECT placement_ring_json FROM index_rules WHERE index_name = ?",
      body.indexName,
    );
    return json({ indexName: body.indexName, ring: row ? (JSON.parse(row.placement_ring_json) as string[]) : [] });
  }

  // ─── Topology-operation lock (held on catalog-0) ───────────────────────────

  private topologyLockRow(): { operation_id: string; operation_type: string; acquired_at: string; expires_at: string; heartbeat_at: string } | null {
    return this.one("SELECT operation_id, operation_type, acquired_at, expires_at, heartbeat_at FROM topology_lock WHERE singleton = 1");
  }

  private topologyLockExpired(row: { expires_at: string }, nowMs: number): boolean {
    return nowMs > new Date(row.expires_at).getTime();
  }

  /** CAS acquire: take the lock if none held or the current one has expired.
   * Returns {ok, operationId} or 409 TOPOLOGY_OPERATION_IN_PROGRESS. */
  private async handleAcquireTopologyLock(request: Request): Promise<Response> {
    const body = (await request.json()) as { operationType?: string };
    if (!body.operationType) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing operationType." } }, 400);
    }
    const now = Date.now();
    const existing = this.topologyLockRow();
    if (existing && !this.topologyLockExpired(existing, now)) {
      return json(
        {
          error: {
            code: "TOPOLOGY_OPERATION_IN_PROGRESS",
            operationType: existing.operation_type,
            operationId: existing.operation_id,
            message: `A topology operation (${existing.operation_type}) is already in progress.`,
            fix: "Retry after the in-progress operation completes, or /admin/force-release-topology-lock if it is stuck.",
          },
        },
        409,
      );
    }
    const operationId = crypto.randomUUID();
    const nowIso = new Date(now).toISOString();
    const expiresIso = new Date(now + TOPOLOGY_LOCK_TTL_MS).toISOString();
    this.sql.exec(
      `INSERT OR REPLACE INTO topology_lock (singleton, operation_id, operation_type, acquired_at, expires_at, heartbeat_at)
       VALUES (1, ?, ?, ?, ?, ?)`,
      operationId,
      body.operationType,
      nowIso,
      expiresIso,
      nowIso,
    );
    this.audit("/acquire-topology-lock", { operationType: body.operationType, operationId });
    return json({ ok: true, operationId, operationType: body.operationType, expiresAt: expiresIso });
  }

  /** Refresh the lease iff this operationId still holds the (non-expired) lock;
   * else 409 LOCK_LOST — the operation must stop mutating. */
  private async handleHeartbeatTopologyLock(request: Request): Promise<Response> {
    const body = (await request.json()) as { operationId?: string };
    if (!body.operationId) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing operationId." } }, 400);
    }
    const now = Date.now();
    const existing = this.topologyLockRow();
    if (!existing || existing.operation_id !== body.operationId || this.topologyLockExpired(existing, now)) {
      return json(
        { error: { code: "LOCK_LOST", message: "This operation no longer holds the topology lock (released, force-released, or expired).", fix: "Stop the operation; it must not keep mutating topology." } },
        409,
      );
    }
    const expiresIso = new Date(now + TOPOLOGY_LOCK_TTL_MS).toISOString();
    this.sql.exec(
      "UPDATE topology_lock SET heartbeat_at = ?, expires_at = ? WHERE singleton = 1 AND operation_id = ?",
      new Date(now).toISOString(),
      expiresIso,
      body.operationId,
    );
    return json({ ok: true, operationId: body.operationId, expiresAt: expiresIso });
  }

  /** Release iff this operationId holds it. Idempotent no-op otherwise (a
   * stale releaser must never drop a lock a newer operation now holds). */
  private async handleReleaseTopologyLock(request: Request): Promise<Response> {
    const body = (await request.json()) as { operationId?: string };
    if (!body.operationId) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing operationId." } }, 400);
    }
    const existing = this.topologyLockRow();
    const held = existing?.operation_id === body.operationId;
    if (held) {
      this.sql.exec("DELETE FROM topology_lock WHERE singleton = 1 AND operation_id = ?", body.operationId);
      this.audit("/release-topology-lock", { operationId: body.operationId });
    }
    return json({ ok: true, released: held });
  }

  /** Current holder (or none) + whether it is expired. */
  private async handleTopologyLockStatus(): Promise<Response> {
    const existing = this.topologyLockRow();
    if (!existing) {
      return json({ held: false });
    }
    return json({
      held: true,
      operationId: existing.operation_id,
      operationType: existing.operation_type,
      acquiredAt: existing.acquired_at,
      heartbeatAt: existing.heartbeat_at,
      expiresAt: existing.expires_at,
      expired: this.topologyLockExpired(existing, Date.now()),
    });
  }

  /** True iff this operationId still holds the non-expired lock — the
   * pre-destructive-step gate. */
  private async handleHoldsTopologyLock(request: Request): Promise<Response> {
    const body = (await request.json()) as { operationId?: string };
    if (!body.operationId) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing operationId." } }, 400);
    }
    const existing = this.topologyLockRow();
    const holds = !!existing && existing.operation_id === body.operationId && !this.topologyLockExpired(existing, Date.now());
    return json({ holds });
  }

  private async handleListIndexes(): Promise<Response> {
    const indexes = this.many<{
      index_name: string;
      table_name: string;
      columns_json: string;
      status: string;
      created_at: string;
      placement_ring_json: string;
    }>(
      "SELECT index_name, table_name, columns_json, status, created_at, placement_ring_json FROM index_rules ORDER BY index_name ASC",
    );
    return json({
      indexes: indexes.map((i) => ({
        indexName: i.index_name,
        table: i.table_name,
        columns: JSON.parse(i.columns_json) as string[],
        status: i.status,
        createdAt: i.created_at,
        placementRing: JSON.parse(i.placement_ring_json) as string[],
      })),
    });
  }

  /** Milestone 2, Chunk 4. Tenant-auth-gated (not admin-gated, unlike
   * /list-indexes) — /v1/index-query is a tenant-facing data-plane route, so
   * this checks the caller's token the same way /route does, without
   * requiring a partitionKey (nothing to route to a specific row yet —
   * that's what the index lookup itself resolves). */
  private async handleLookupIndex(request: Request): Promise<Response> {
    const body = (await request.json()) as { table?: string; indexName?: string; tenantId?: string };
    if (!body.table || !body.indexName || !body.tenantId) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing table, indexName, or tenantId." } }, 400);
    }
    const authError = await this.checkTenantAuth(body.tenantId, request);
    if (authError) return authError;

    const index = this.one<{ columns_json: string; partition_key_column: string; status: string; placement_ring_json: string; evac_from_shards_json: string }>(
      `
      SELECT ir.columns_json AS columns_json, tr.partition_key_column AS partition_key_column, ir.status AS status, ir.placement_ring_json AS placement_ring_json, ir.evac_from_shards_json AS evac_from_shards_json
      FROM index_rules ir
      JOIN table_rules tr ON tr.table_name = ir.table_name
      WHERE ir.index_name = ? AND ir.table_name = ?
      `,
      body.indexName,
      body.table,
    );
    if (!index) {
      return json(
        { error: { code: "INDEX_NOT_REGISTERED", message: `Index ${body.indexName} is not registered on table ${body.table}.` } },
        404,
      );
    }
    if (index.status !== "ready") {
      return json(
        {
          error: {
            code: "INDEX_BUILDING",
            message: `Index ${body.indexName} is still backfilling and not yet queryable.`,
            fix: "Retry once /admin/create-index for this index has returned successfully.",
          },
        },
        425,
      );
    }
    return json({
      columns: JSON.parse(index.columns_json) as string[],
      partitionKeyColumn: index.partition_key_column,
      ring: JSON.parse(index.placement_ring_json) as string[],
      // Codex round-14 P2: draining shard(s) whose entries for this index are
      // mid-evacuation (repointed ring, not-yet-copied) — /v1/index-query
      // dual-looks-up these alongside the current ring shard.
      evacFromShards: JSON.parse(index.evac_from_shards_json ?? "[]") as string[],
    });
  }

  /** Tenant-scoped table scan (POST /v1/table-scan). Tenant-auth-gated (not
   * admin-gated) the same way /lookup-index is — checks the caller's token
   * exactly like handleLookupIndex does, reusing checkTenantAuth rather
   * than re-implementing it, per the spec's explicit instruction. No
   * partitionKey is available yet (a scan has none), so this can't reuse
   * /route itself; it plays the combined "auth + table_rules gate" role
   * /lookup-index plays for /v1/index-query instead. */
  private async handleLookupTableScan(request: Request): Promise<Response> {
    const body = (await request.json()) as { table?: string; tenantId?: string };
    if (!body.table || !body.tenantId) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing table or tenantId." } }, 400);
    }
    const authError = await this.checkTenantAuth(body.tenantId, request);
    if (authError) return authError;

    // Issue #33: gated here, the same choke point auth already runs at --
    // BEFORE the Worker fans out to every shard in this tenant's pool
    // (tableScanCore, back in index.ts, only reaches that fan-out once this
    // whole call succeeds).
    const rateLimitError = this.checkAndConsumeTableScanRateLimit(body.tenantId);
    if (rateLimitError) return rateLimitError;

    const table = this.one<{ partition_key_column: string; provenance_complete: number; partition_key_unique: number }>(
      "SELECT partition_key_column, provenance_complete, partition_key_unique FROM table_rules WHERE table_name = ?",
      body.table,
    );
    if (!table) {
      return json(
        {
          error: {
            code: "TABLE_NOT_REGISTERED",
            message: `Table ${body.table} is not registered.`,
            fix: "Call /admin/create-table first.",
          },
        },
        404,
      );
    }
    if (table.partition_key_column === UNSET_PARTITION_KEY_COLUMN) {
      return json(
        {
          error: {
            code: "PARTITION_KEY_COLUMN_UNSET",
            message: `Table ${body.table} has not been upgraded with a partition key column.`,
            fix: "Call /admin/set-partition-key-column first.",
          },
        },
        409,
      );
    }
    // Codex P1 fix: __cf_row_owners keys a row's owner by (table, partition
    // key VALUE) alone. If partitionKeyColumn isn't verified UNIQUE/PRIMARY
    // KEY, two different tenants' rows could share a value and the
    // /tenant-scan-page JOIN would attribute both to whichever tenant
    // currently owns that key — a cross-tenant read leak. Reject the scan
    // rather than risk that; this check is separate from (and after) the
    // UNSET check above since an unset column is the more fundamental issue.
    if (table.partition_key_unique !== 1) {
      return json(
        {
          error: {
            code: "PARTITION_KEY_NOT_UNIQUE",
            message: `Table ${body.table}'s partitionKeyColumn (${table.partition_key_column}) is not verified UNIQUE or PRIMARY KEY.`,
            fix: "The table's partitionKeyColumn must be UNIQUE or PRIMARY KEY to support tenant-scoped scanning; recreate the table with that constraint, or contact an operator.",
          },
        },
        409,
      );
    }
    return json({
      partitionKeyColumn: table.partition_key_column,
      provenanceComplete: table.provenance_complete === 1,
    });
  }

  /** /admin/backfill-provenance (internal, admin-gated at the Worker level and
   * re-gated here — see ADMIN_GATED_ROUTES) flips a table's cached
   * provenance_complete flag once a full-cluster run reports zero orphaned
   * and zero ambiguous rows for it. Monotonic (see ensureSchema's comment):
   * an UPDATE ... SET provenance_complete = 1 can only ever set it, never
   * clear it, so calling this again for an already-complete table is a no-op,
   * not a regression. */
  private async handleMarkTableProvenanceComplete(request: Request): Promise<Response> {
    const body = (await request.json()) as { table?: string };
    if (!body.table) {
      return json({ error: "Missing table" }, 400);
    }
    this.sql.exec("UPDATE table_rules SET provenance_complete = 1 WHERE table_name = ?", body.table);
    return json({ ok: true, table: body.table });
  }

  /** Data-plane tenant auth check: does the caller's bearer token match the
   * hash on file for the claimed tenantId? A per-claimed-tenant check, not an
   * identity primitive — it answers "does this token match tenantId X" for a
   * caller-supplied X, not "which tenant does this token belong to". */
  private async checkTenantAuth(tenantId: string, request: Request): Promise<Response | null> {
    const authHeader = request.headers.get("authorization");
    const token = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : null;
    if (!token) {
      return json(
        {
          error: {
            code: "TENANT_TOKEN_MISSING",
            message: "Missing tenant bearer token.",
            fix: "Include 'authorization: Bearer <token>' from /register-tenant.",
          },
        },
        401,
      );
    }
    // Admin bypass: the operator (ADMIN_TOKEN) may /route as ANY tenant — used
    // by the now-admin-only /v1/sql path (which still passes body.tenantId for
    // routing/hashing but is authenticated as the operator, not the tenant).
    // Harmless for the tenant-facing endpoints (admin is a superuser) and does
    // not weaken tenant auth: a tenant token must still match its own hash.
    if (this.adminToken && timingSafeEqual(token, this.adminToken)) {
      return null;
    }

    const row = this.one<{
      token_hash: string;
      revoked_at: string | null;
      previous_token_hash: string | null;
      previous_token_expires_at: string | null;
    }>(
      "SELECT token_hash, revoked_at, previous_token_hash, previous_token_expires_at FROM tenant_auth WHERE tenant_id = ?",
      tenantId,
    );
    if (!row) {
      return json(
        {
          error: {
            code: "TENANT_NOT_REGISTERED",
            message: `Tenant ${tenantId} is not registered.`,
            fix: "Call /register-tenant first.",
          },
        },
        401,
      );
    }
    if (row.revoked_at) {
      // Issue #26: explicit revocation is NOT softened by the rotation grace
      // period below -- checked first, unconditionally, so a revoked tenant's
      // in-grace previous token (if any) is also rejected immediately.
      return json(
        {
          error: {
            code: "TENANT_TOKEN_REVOKED",
            message: `Tenant ${tenantId}'s token has been revoked.`,
            fix: "Call /register-tenant with rotate: true to get a new token.",
          },
        },
        401,
      );
    }

    const tokenHash = await sha256Hex(token);
    if (timingSafeEqual(tokenHash, row.token_hash)) {
      return null;
    }
    // Issue #26: a rotate: true call keeps the OLD token valid for a bounded
    // grace window (see TENANT_TOKEN_ROTATION_GRACE_MS) instead of
    // invalidating it the instant the new one is issued.
    if (
      row.previous_token_hash &&
      row.previous_token_expires_at &&
      Date.now() < Date.parse(row.previous_token_expires_at) &&
      timingSafeEqual(tokenHash, row.previous_token_hash)
    ) {
      return null;
    }
    return json(
      {
        error: {
          code: "TENANT_TOKEN_INVALID",
          message: "Invalid tenant token.",
          fix: "Check the token, or re-register via /register-tenant.",
        },
      },
      401,
    );
  }

  /** Issue #33: token-bucket rate limit for /v1/table-scan, the only
   * fan-out-shaped tenant route -- called from handleLookupTableScan, the
   * same choke point that already gates auth + table-registry validation
   * before the Worker fans out to every shard. Returns a 429 Response if
   * the tenant is over their rate, null if the call may proceed (and has
   * already consumed one token). Lazily creates a full bucket for a
   * tenant's first-ever table-scan call. */
  private checkAndConsumeTableScanRateLimit(tenantId: string): Response | null {
    const now = Date.now();
    const row = this.one<{ tokens: number; last_refill_at: string }>(
      "SELECT tokens, last_refill_at FROM tenant_scan_rate_limit WHERE tenant_id = ?",
      tenantId,
    );
    let tokens = TABLE_SCAN_RATE_LIMIT_CAPACITY;
    if (row) {
      const elapsedSeconds = Math.max(0, (now - Date.parse(row.last_refill_at)) / 1000);
      tokens = Math.min(TABLE_SCAN_RATE_LIMIT_CAPACITY, row.tokens + elapsedSeconds * TABLE_SCAN_RATE_LIMIT_REFILL_PER_SECOND);
    }
    if (tokens < 1) {
      const retryAfterMs = Math.ceil(((1 - tokens) / TABLE_SCAN_RATE_LIMIT_REFILL_PER_SECOND) * 1000);
      return json(
        {
          error: {
            code: "RATE_LIMITED",
            message: `Tenant ${tenantId} is issuing /v1/table-scan calls too quickly.`,
            fix: `Retry after ${retryAfterMs}ms, or space out table-scan calls to at most ${TABLE_SCAN_RATE_LIMIT_REFILL_PER_SECOND}/sec sustained.`,
            retryAfterMs,
          },
        },
        429,
      );
    }
    this.sql.exec(
      "INSERT OR REPLACE INTO tenant_scan_rate_limit (tenant_id, tokens, last_refill_at) VALUES (?, ?, ?)",
      tenantId,
      tokens - 1,
      new Date(now).toISOString(),
    );
    return null;
  }

  private async handleRegisterTenant(request: Request): Promise<Response> {
    const body = (await request.json()) as { tenantId?: string; rotate?: boolean };
    if (!body.tenantId) {
      return json(
        { error: { code: "MISSING_TENANT_ID", message: "Missing tenantId.", fix: "Provide a tenantId in the request body." } },
        400,
      );
    }

    const existing = this.one<{ tenant_id: string; token_hash: string; revoked_at: string | null }>(
      "SELECT tenant_id, token_hash, revoked_at FROM tenant_auth WHERE tenant_id = ?",
      body.tenantId,
    );
    if (existing && body.rotate !== true) {
      return json(
        {
          error: {
            code: "TENANT_ALREADY_REGISTERED",
            message: `Tenant ${body.tenantId} is already registered.`,
            fix: "Pass rotate: true to issue a new token for this tenant.",
          },
        },
        409,
      );
    }

    const token = crypto.randomUUID();
    const tokenHash = await sha256Hex(token);
    const now = new Date();

    // Log only {tenantId, rotate} — never the token or its hash, unlike other
    // audit() call sites in this file that log their full parsed body by
    // convention. This one must not, since audit_log is durably persisted
    // and readable via /admin/audit-log.
    this.audit("/register-tenant", { tenantId: body.tenantId, rotate: body.rotate === true });

    // Issue #26: a rotate: true call against an EXISTING, non-revoked tenant
    // grants the old token a bounded grace period instead of killing it
    // immediately (INSERT OR REPLACE previously wiped it outright). An
    // explicitly-revoked tenant rotating back in gets no grace for its old
    // token -- that token was already killed by deliberate operator action,
    // and un-revoking via rotate must not resurrect it. A fresh registration
    // has nothing to grace either way.
    if (existing && !existing.revoked_at) {
      const graceExpiresAt = new Date(now.getTime() + TENANT_TOKEN_ROTATION_GRACE_MS).toISOString();
      this.sql.exec(
        `
        UPDATE tenant_auth
        SET token_hash = ?, revoked_at = NULL, previous_token_hash = ?, previous_token_expires_at = ?
        WHERE tenant_id = ?
        `,
        tokenHash,
        existing.token_hash,
        graceExpiresAt,
        body.tenantId,
      );
    } else {
      this.sql.exec(
        `
        INSERT OR REPLACE INTO tenant_auth (tenant_id, token_hash, created_at, revoked_at, previous_token_hash, previous_token_expires_at)
        VALUES (?, ?, ?, NULL, NULL, NULL)
        `,
        body.tenantId,
        tokenHash,
        now.toISOString(),
      );
    }

    return json({ ok: true, tenantId: body.tenantId, token });
  }

  private async handleRevokeTenant(request: Request): Promise<Response> {
    const body = (await request.json()) as { tenantId?: string };
    if (!body.tenantId) {
      return json(
        { error: { code: "MISSING_TENANT_ID", message: "Missing tenantId.", fix: "Provide a tenantId in the request body." } },
        400,
      );
    }

    const existing = this.one<{ tenant_id: string }>(
      "SELECT tenant_id FROM tenant_auth WHERE tenant_id = ?",
      body.tenantId,
    );
    if (!existing) {
      return json(
        {
          error: {
            code: "TENANT_NOT_FOUND",
            message: `Tenant ${body.tenantId} is not registered.`,
            fix: "Call /register-tenant first.",
          },
        },
        404,
      );
    }

    this.audit("/revoke-tenant", { tenantId: body.tenantId });
    this.sql.exec(
      "UPDATE tenant_auth SET revoked_at = ? WHERE tenant_id = ?",
      new Date().toISOString(),
      body.tenantId,
    );

    return json({ ok: true, tenantId: body.tenantId, revoked: true });
  }

  private async handleRoute(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      table: string;
      tenantId: string;
      partitionKey: string;
    };

    if (!body.table || !body.tenantId || !body.partitionKey) {
      return json({ error: "Missing table, tenantId, or partitionKey" }, 400);
    }

    const authError = await this.checkTenantAuth(body.tenantId, request);
    if (authError) return authError;

    const config = this.one<{
      total_vbuckets: number;
      metadata_version: number;
      catalog_shard_count: number | null;
    }>(
      "SELECT total_vbuckets, metadata_version, catalog_shard_count FROM cluster_config WHERE singleton = 1",
    );
    if (!config) {
      return json({ error: "Cluster not initialized. Call /admin/init first." }, 400);
    }

    const composite = `${body.tenantId}:${body.table}:${body.partitionKey}`;
    const vbucket = hashKey(composite) % config.total_vbuckets;

    const mapped = this.one<{
      table_registered: string | null;
      partition_key_column: string | null;
      shard_id: string;
      status: string;
      migration_status: string;
      target_shard_id: string | null;
    }>(
      `
      SELECT
        (SELECT table_name FROM table_rules WHERE table_name = ?) AS table_registered,
        (SELECT partition_key_column FROM table_rules WHERE table_name = ?) AS partition_key_column,
        vm.shard_id AS shard_id,
        s.status AS status,
        vm.migration_status AS migration_status,
        vm.target_shard_id AS target_shard_id
      FROM vbucket_map vm
      JOIN shards s ON s.shard_id = vm.shard_id
      WHERE vm.vbucket = ?
      `,
      body.table,
      body.table,
      vbucket,
    );
    if (!mapped) {
      return json({ error: `No shard mapping for vbucket ${vbucket}` }, 500);
    }
    if (!mapped.table_registered) {
      return json(
        { error: `Table ${body.table} is not registered. Call /admin/register-table first.` },
        400,
      );
    }
    if (mapped.status !== "active") {
      log("catalog.route_rejected_draining", { table: body.table, vbucket, shardId: mapped.shard_id, status: mapped.status });
      return json(
        { error: `Mapped shard ${mapped.shard_id} is ${mapped.status}. Reassign this vbucket before routing.` },
        503,
      );
    }

    // Milestone 2: lets the Worker reject a raw /v1/sql mutation against a
    // table carrying a registered index (ShardDO has no CatalogDO access to
    // check this itself — see the Milestone 2 eng review's correction), and
    // lets /v1/mutate's async index maintenance (Chunk 2) know which columns
    // each registered index actually covers.
    const indexRows = this.many<{ index_name: string; columns_json: string; placement_ring_json: string }>(
      "SELECT index_name, columns_json, placement_ring_json FROM index_rules WHERE table_name = ?",
      body.table,
    );
    const indexes = indexRows.map((r) => ({
      indexName: r.index_name,
      columns: JSON.parse(r.columns_json) as string[],
      ring: JSON.parse(r.placement_ring_json) as string[],
    }));

    return json({
      shardId: mapped.shard_id,
      vbucket,
      metadataVersion: config.metadata_version,
      catalogShardCount: config.catalog_shard_count,
      partitionKeyColumn: mapped.partition_key_column,
      indexNames: indexes.map((i) => i.indexName),
      indexes,
      // Milestone 3, Chunk 3: while this vbucket is migrating, the gateway
      // applies every write to the source (shardId above — still
      // authoritative) and then mirrors it to targetShardId with the same
      // requestId. 'none' status omits both fields, keeping the pre-M3
      // response shape byte-identical for non-migrating vbuckets.
      ...(mapped.migration_status !== "none" && mapped.target_shard_id
        ? { migrationStatus: mapped.migration_status, targetShardId: mapped.target_shard_id }
        : {}),
    });
  }

  /** Review Tier 2 #12: resolve MANY (tenant, table, partitionKey) tuples to
   * their current shards in ONE tenant-authenticated call — /v1/index-query
   * hydration used to make one /route subrequest per matched entry (up to
   * 100+ per query, all serialized through this one CatalogDO). Auth is
   * checked once for the whole batch (all tuples share the caller's tenant).
   * Only the shard mapping is returned (no per-tuple index/metadata payload),
   * since the caller already resolved the index. */
  private async handleRouteBatch(request: Request): Promise<Response> {
    const body = (await request.json()) as { table?: string; tenantId?: string; partitionKeys?: string[] };
    if (!body.table || !body.tenantId || !body.partitionKeys) {
      return json({ error: { code: "MISSING_FIELDS", message: "Missing table, tenantId, or partitionKeys." } }, 400);
    }
    const authError = await this.checkTenantAuth(body.tenantId, request);
    if (authError) return authError;

    const config = this.one<{ total_vbuckets: number }>("SELECT total_vbuckets FROM cluster_config WHERE singleton = 1");
    if (!config) {
      return json({ error: "Cluster not initialized. Call /admin/init first." }, 400);
    }
    const vbucketToShard = new Map(
      this.many<{ vbucket: number; shard_id: string }>("SELECT vbucket, shard_id FROM vbucket_map").map((r) => [r.vbucket, r.shard_id]),
    );
    const routes = body.partitionKeys.map((pk) => {
      const vbucket = hashKey(`${body.tenantId}:${body.table}:${pk}`) % config.total_vbuckets;
      return { partitionKey: pk, shardId: vbucketToShard.get(vbucket) ?? null };
    });
    return json({ routes });
  }

  /** Default: ACTIVE shards only — the placement-ring / substitution pool. A
   * draining shard must never be pinned into a new ring or chosen as a
   * substitute target, so ring-building callers (clusterActiveShards, the
   * create-index placement ring) rely on this default and pass no body.
   *
   * With `{ includeDraining: true }`: also returns DRAINING shards — a
   * draining shard still physically holds its base rows until its vbuckets
   * finish migrating, so any operation that must SEE every existing row
   * (backfill-provenance's re-attribution scan, create-index's backfill scan)
   * needs it. This is purely about seeing a draining shard's DATA; it does not
   * make the shard a candidate for placement. */
  private async handleListShards(request: Request): Promise<Response> {
    let includeDraining = false;
    try {
      const body = (await request.json()) as { includeDraining?: boolean };
      includeDraining = body?.includeDraining === true;
    } catch {
      // No/blank body — default to active-only.
    }
    const statuses = includeDraining ? ["active", "draining"] : ["active"];
    const placeholders = statuses.map(() => "?").join(", ");
    const shards = this.many<{ shard_id: string }>(
      `SELECT shard_id FROM shards WHERE status IN (${placeholders}) ORDER BY shard_id ASC`,
      ...statuses,
    );
    return json({ shardIds: shards.map((s) => s.shard_id) });
  }

  /** Milestone 3, Chunk 1: candidate tenant identities for this catalog
   * shard's row-provenance re-attribution — /admin/backfill-provenance tries
   * every one of these against every unattributed row's hash to find which
   * tenant(s) could have written it. Returns tenantId only, never
   * token_hash — this route exists purely to enumerate identities, not to
   * authenticate anything. */
  private async handleListTenants(): Promise<Response> {
    const tenants = this.many<{ tenant_id: string }>("SELECT tenant_id FROM tenant_auth ORDER BY tenant_id ASC");
    return json({ tenantIds: tenants.map((t) => t.tenant_id) });
  }

  /** Milestone 3, Chunk 1: the full vbucket -> shard_id map for this catalog
   * shard, plus total_vbuckets — /admin/backfill-provenance and
   * /admin/set-row-owner fetch this once per catalog shard (rather than one
   * round trip per candidate tenant per row) to test "does this candidate
   * tenant's hash land on this specific shard" locally. */
  private async handleVbucketMap(): Promise<Response> {
    const config = this.one<{ total_vbuckets: number }>("SELECT total_vbuckets FROM cluster_config WHERE singleton = 1");
    if (!config) {
      return json({ error: "Cluster not initialized. Call /admin/init first." }, 400);
    }
    // Shardscope T1: migration_status/target_shard_id/cutover_started_at are
    // additive fields (existing consumers — /admin/backfill-provenance,
    // /admin/set-row-owner — only ever read vbucket/shardId, so those two
    // stay byte-identical) that let a dashboard render in-flight vbucket
    // migrations, not just the steady-state map.
    const rows = this.many<{
      vbucket: number;
      shard_id: string;
      migration_status: string;
      target_shard_id: string | null;
      cutover_started_at: string | null;
    }>(
      "SELECT vbucket, shard_id, migration_status, target_shard_id, cutover_started_at FROM vbucket_map ORDER BY vbucket ASC",
    );
    return json({
      totalVBuckets: config.total_vbuckets,
      map: rows.map((r) => ({
        vbucket: r.vbucket,
        shardId: r.shard_id,
        migrationStatus: r.migration_status,
        targetShardId: r.target_shard_id,
        cutoverStartedAt: r.cutover_started_at,
      })),
    });
  }

  private async handleStatus(): Promise<Response> {
    const config = this.one<{
      total_vbuckets: number;
      metadata_version: number;
      initialized_at: string;
    }>("SELECT total_vbuckets, metadata_version, initialized_at FROM cluster_config WHERE singleton = 1");

    if (!config) {
      return json({ initialized: false });
    }

    const shardRows = this.many<{ shard_id: string; status: string }>(
      "SELECT shard_id, status FROM shards ORDER BY shard_id ASC",
    );
    const activeShards = shardRows.filter((s) => s.status === "active").length;
    const drainingShards = shardRows.filter((s) => s.status === "draining").length;

    return json({
      initialized: true,
      totalVBuckets: config.total_vbuckets,
      metadataVersion: config.metadata_version,
      initializedAt: config.initialized_at,
      shards: {
        total: shardRows.length,
        active: activeShards,
        draining: drainingShards,
      },
    });
  }

  private async handleListTables(): Promise<Response> {
    const tables = this.many<{ table_name: string; partitioning: string; partition_key_column: string; created_at: string }>(
      "SELECT table_name, partitioning, partition_key_column, created_at FROM table_rules ORDER BY table_name ASC",
    );
    return json({ tables });
  }

  private async handleAuditLog(): Promise<Response> {
    const entries = this.many<{ endpoint: string; request_summary: string; created_at: string }>(
      "SELECT endpoint, request_summary, created_at FROM audit_log ORDER BY id DESC LIMIT 100",
    );
    return json({
      entries: entries.map((e) => ({
        endpoint: e.endpoint,
        request: JSON.parse(e.request_summary) as unknown,
        createdAt: e.created_at,
      })),
    });
  }

  /** Milestone 3, Chunk 5 (drain v2): marking a shard draining now also
   * kicks off full evacuation — every vbucket mapped to it is migrated off
   * sequentially via Chunk 4's primitive, then any index whose pinned
   * placement ring contains it gets the shard substituted out (deterministic
   * rule: the active shard not already in that ring with the smallest
   * hashKey(indexName + ":" + shardId), entries copied before the ring
   * repoints, source copies deleted after). Both loops run from this
   * catalog's alarm; /drain-shard-status exposes progress. The old blanket
   * SHARD_DRAIN_BLOCKED_BY_INDEXES 409 stays removed (Chunk 2): pinned
   * rings make draining a non-ring shard trivially safe, and ring
   * evacuation now covers the rest. */
  private async handleDrainShard(request: Request): Promise<Response> {
    const body = (await request.json()) as { shardId: string; operationId?: string };
    if (!body.shardId) {
      return json({ error: "Missing shardId" }, 400);
    }

    const existing = this.one<{ shard_id: string; status: string }>(
      "SELECT shard_id, status FROM shards WHERE shard_id = ?",
      body.shardId,
    );
    if (!existing) {
      return json({ error: `Shard ${body.shardId} not found` }, 404);
    }

    // Ring-evacuation feasibility, BEFORE durably marking anything:
    // rejecting up front beats discovering mid-drain (from an alarm with no
    // caller to answer to) that the evacuation can't finish. Candidates are
    // gathered CLUSTER-wide (rings span every catalog shard's pool, and an
    // index's ring pins ALL shards active at its creation — the only viable
    // substitutes are shards added afterwards, e.g. by a split). A shard
    // that still owns vbuckets but has no local migration target is NOT
    // rejected here: marking it draining (503 for new work) without moving
    // data yet is exactly the pre-M3 behavior, and the vbucket loop resumes
    // as soon as capacity exists.
    const indexRules = this.many<{ index_name: string; placement_ring_json: string }>(
      "SELECT index_name, placement_ring_json FROM index_rules",
    );
    const ringsContaining = indexRules.filter((r) => (JSON.parse(r.placement_ring_json) as string[]).includes(body.shardId));
    if (ringsContaining.length > 0) {
      const clusterActive = await this.clusterActiveShards(body.shardId);
      for (const rule of ringsContaining) {
        const ring = JSON.parse(rule.placement_ring_json) as string[];
        const candidates = clusterActive.filter((s) => !ring.includes(s));
        if (candidates.length === 0) {
          return json(
            {
              error: {
                code: "RING_EVACUATION_NO_CANDIDATE",
                message: `Index ${rule.index_name}'s placement ring contains ${body.shardId} and no active shard outside that ring exists to substitute in.`,
                fix: "Add an active shard (/admin/split-vbucket) or drop and recreate the index, then retry the drain.",
              },
            },
            409,
          );
        }
      }
    }

    this.audit("/drain-shard", { shardId: body.shardId });

    // Clear any provenance stall (review Tier 2 #10): re-invoking /drain-shard
    // is the operator's "I've backfilled provenance, resume" signal.
    this.sql.exec("UPDATE shards SET status = 'draining', drain_stall_reason = NULL WHERE shard_id = ?", body.shardId);

    // Approved design (Stage 3): a FRESH drain start (this shard wasn't
    // already draining) records the topology-lock operationId the Worker
    // acquired for this call — advanceDrain heartbeats it every tick and holds
    // it for the drain's ENTIRE multi-tick duration, releasing only on full
    // completion. A RE-INVOKE of an already-draining shard (the "I've fixed
    // the stall, resume" signal above) must NOT overwrite it: the original
    // drain's lock is still held and heartbeating; the Worker's re-invoke
    // call did not (and structurally could not, since the lock only allows
    // one holder) acquire a new one for this same drain.
    if (existing.status !== "draining" && body.operationId) {
      this.sql.exec("UPDATE shards SET topology_lock_operation_id = ? WHERE shard_id = ?", body.operationId, body.shardId);
    }

    const version = this.bumpMetadataVersion();

    // Kick the evacuation loop; re-calling /drain-shard on an
    // already-draining shard is an idempotent way to re-arm it.
    const soon = Date.now() + MIGRATION_TICK_MS;
    const existingAlarm = await this.ctx.storage.getAlarm();
    if (existingAlarm === null || existingAlarm > soon) {
      await this.ctx.storage.setAlarm(soon);
    }

    return json({ ok: true, shardId: body.shardId, metadataVersion: version, evacuationStarted: true });
  }

  /** Milestone 3, Chunk 5 (POST /drain-shard-status {shardId}). */
  private async handleDrainShardStatus(request: Request): Promise<Response> {
    const body = (await request.json()) as { shardId?: string };
    if (!body.shardId) {
      return json({ error: "Missing shardId" }, 400);
    }
    const shard = this.one<{ status: string; drain_stall_reason: string | null }>(
      "SELECT status, drain_stall_reason FROM shards WHERE shard_id = ?",
      body.shardId,
    );
    if (!shard) {
      return json({ error: `Shard ${body.shardId} not found` }, 404);
    }
    const vbucketsRemaining = this.one<{ n: number }>("SELECT COUNT(*) AS n FROM vbucket_map WHERE shard_id = ?", body.shardId)?.n ?? 0;
    // Codex round-13 fix: "rings remaining" is the UNION of indexes still
    // ring-resident on this shard (not yet fenced/repointed) and indexes with an
    // open evacuation MARKER (fenced + repointed EARLY, still copying/deleting).
    // Ring membership alone would read 0 the instant the early repoint lands and
    // wrongly report 'complete' while the source entries are still being moved.
    const ringResident = new Set(
      this.many<{ index_name: string; placement_ring_json: string }>("SELECT index_name, placement_ring_json FROM index_rules")
        .filter((r) => (JSON.parse(r.placement_ring_json) as string[]).includes(body.shardId!))
        .map((r) => r.index_name),
    );
    for (const m of this.many<{ index_name: string }>("SELECT index_name FROM drain_ring_evac WHERE shard_id = ?", body.shardId)) {
      ringResident.add(m.index_name);
    }
    const ringsRemaining = ringResident.size;
    const status =
      shard.status !== "draining"
        ? shard.status
        : // Review Tier 2 #10: a drain parked on the provenance gate reports a
          // distinct status so the operator knows to run
          // /admin/backfill-provenance and re-invoke /admin/drain-shard.
          shard.drain_stall_reason === "provenance"
          ? "stalled-provenance"
          : // Re-review item E: any other stall reason (e.g. a vbucket wedged
            // in 'aborting', or an unreachable shard) is also parked — report
            // it as generically stalled (stallReason carries the specifics)
            // rather than the misleading 'migrating-vbuckets'.
            shard.drain_stall_reason
            ? "stalled"
            : vbucketsRemaining > 0
              ? "migrating-vbuckets"
              : ringsRemaining > 0
                ? "evacuating-rings"
                : "complete";
    return json({ shardId: body.shardId, vbucketsRemaining, ringsRemaining, status, stallReason: shard.drain_stall_reason });
  }

  // ─── Topology-operation lock client (Stage 3: long-running operations) ────
  // The lock lives on ONE physical DO — "catalog-0" — regardless of which
  // catalog shard THIS instance is; the Worker's acquireTopologyLock always
  // acquires against that same physical DO (see index.ts). These helpers let
  // a long-running migration/drain (which may be advancing on ANY catalog
  // shard's own alarm) heartbeat/release that lock from wherever it's ticking.

  /** True iff THIS CatalogDO instance IS the physical "catalog-0" — the
   * lock's canonical home. Lets the heartbeat/release helpers below call the
   * route handlers DIRECTLY (plain in-process method calls) instead of
   * self-fetching a stub for its own identity, which is both unnecessary
   * overhead and — observed empirically — unreliable when the caller is
   * itself already executing inside a non-standard dispatch context (e.g. the
   * test harness's runInDurableObject, which invokes DO methods directly
   * rather than through a normal request; a self-fetch issued from within
   * that context does not reliably reach this same instance's storage).
   *
   * Compares DO IDENTITY (this.ctx.id vs. idFromName("catalog-0")), not
   * cluster_config.catalog_shard_id — that config field is only ever set when
   * /init is called through the Worker's multi-catalog fan-out (which always
   * passes catalogShardId), so a catalog-0 instance /init'd directly (bypassing
   * the Worker, e.g. many catalog.test.ts unit tests) would otherwise be
   * mis-detected as NOT itself, triggering the very self-fetch this exists to
   * avoid. */
  private isCatalogZero(): boolean {
    const catalogZeroId = this.catalogEnv.CATALOG.idFromName("catalog-0");
    return this.ctx.id.toString() === catalogZeroId.toString();
  }

  /** POSTs to catalog-0's internal topology-lock route (used only when THIS
   * instance is NOT catalog-0 — see isCatalogZero). */
  private async topologyLockZeroFetch(path: string, body: unknown): Promise<Response> {
    const stub = this.catalogEnv.CATALOG.get(this.catalogEnv.CATALOG.idFromName("catalog-0"));
    return stub.fetch(`https://catalog.internal${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  /** True iff the lock is still held for `operationId` (heartbeating/refreshing
   * its lease as a side effect) — or `operationId` is null/undefined, meaning
   * this migration/drain was never mediated by the Worker's lock-acquiring
   * admin handlers (e.g. a test driving this DO's routes directly), in which
   * case there is nothing to check and the caller proceeds exactly as it did
   * before Stage 3. Catalog-0 being unreachable is treated as LOST (fail
   * safe: stop mutating rather than risk racing a topology op we can no
   * longer confirm exclusivity against). */
  private async heartbeatTopologyLockOrPark(operationId: string | null | undefined): Promise<boolean> {
    if (!operationId) return true;
    try {
      if (this.isCatalogZero()) {
        const res = await this.handleHeartbeatTopologyLock(
          new Request("https://catalog.internal/heartbeat-topology-lock", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ operationId }),
          }),
        );
        return res.ok;
      }
      const res = await this.topologyLockZeroFetch("/heartbeat-topology-lock", { operationId });
      return res.ok;
    } catch {
      return false;
    }
  }

  /** Codex final-review P1 #2: a CHECK-ONLY re-verification (does not renew
   * the lease — that's heartbeatTopologyLockOrPark's job, once per tick) for
   * gating the cutover's destructive map-flip immediately before it runs. The
   * alarm heartbeats once per tick, BEFORE advanceMigration is called — but
   * the cutover branch then awaits several more round trips (fence check,
   * mirror-drain, prepared-intent poll, checksum verify) before reaching the
   * flip. If the lease expired or was force-released during those awaits,
   * another operation could have acquired the lock and be acting on `target`
   * already; flipping onto it now would be unsafe. Same null/unreachable
   * conventions as heartbeatTopologyLockOrPark: no operationId means this
   * migration was never lock-mediated (proceed as before Stage 3); catalog-0
   * unreachable is treated as NOT held (fail safe). */
  private async holdsTopologyLockNow(operationId: string | null | undefined): Promise<boolean> {
    if (!operationId) return true;
    try {
      if (this.isCatalogZero()) {
        const res = await this.handleHoldsTopologyLock(
          new Request("https://catalog.internal/holds-topology-lock", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ operationId }),
          }),
        );
        if (!res.ok) return false;
        return ((await res.json()) as { holds: boolean }).holds;
      }
      const res = await this.topologyLockZeroFetch("/holds-topology-lock", { operationId });
      if (!res.ok) return false;
      return ((await res.json()) as { holds: boolean }).holds;
    } catch {
      return false;
    }
  }

  /** Best-effort release — called once a migration/drain this lock was held
   * for is FULLY complete (or aborted). Never throws; a failed release just
   * leaves the lease to expire on its own TTL. No-op if operationId is
   * null/undefined (nothing was ever held). */
  private async releaseTopologyLockRemote(operationId: string | null | undefined): Promise<void> {
    if (!operationId) return;
    try {
      if (this.isCatalogZero()) {
        await this.handleReleaseTopologyLock(
          new Request("https://catalog.internal/release-topology-lock", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ operationId }),
          }),
        );
        return;
      }
      await this.topologyLockZeroFetch("/release-topology-lock", { operationId });
    } catch {
      // Lease expires on its own.
    }
  }

  /** True iff `operationId` is currently recorded as an ACTIVE drain's lock —
   * i.e. this vbucket's migration is a SUB-migration a drain's phase-1
   * started (inheriting the drain's own operationId, see advanceDrain), and
   * the enclosing drain — not this one vbucket completing — owns the lock's
   * lifecycle. */
  private isOperationOwnedByActiveDrain(operationId: string): boolean {
    const row = this.one<{ n: number }>(
      "SELECT COUNT(*) AS n FROM shards WHERE topology_lock_operation_id = ?",
      operationId,
    );
    return (row?.n ?? 0) > 0;
  }

  /** Release-on-MIGRATION-completion. Approved design (Stage 3) hand-off
   * nuance: a drain's phase-1 sub-migrations inherit the DRAIN's own
   * operationId (so alarm()'s per-row heartbeat still gates their
   * advancement) — but that migration completing is NOT the whole drain
   * completing (there may be more vbuckets, then ring evacuation, still to
   * go). Releasing here unconditionally would drop the drain's lock the
   * moment its FIRST sub-migration finishes. Skip the release when the
   * operationId is still an active drain's lock; only a genuinely
   * standalone migration (/admin/migrate-vbucket, /admin/split-vbucket) or
   * one already vacated by its drain releases here. */
  private async releaseMigrationTopologyLock(operationId: string | null | undefined): Promise<void> {
    if (!operationId) return;
    if (this.isOperationOwnedByActiveDrain(operationId)) return;
    await this.releaseTopologyLockRemote(operationId);
  }

  /** Milestone 3, Chunk 5: the union of active shard ids across EVERY
   * catalog shard (self + siblings via each one's /list-shards) — ring
   * evacuation needs cluster-wide candidates because placement rings span
   * all catalogs' shard pools, while this CatalogDO's own `shards` table
   * only knows its own. */
  private async clusterActiveShards(excludeShardId: string): Promise<string[]> {
    const own = this.many<{ shard_id: string }>(
      "SELECT shard_id FROM shards WHERE status = 'active' AND shard_id != ? ORDER BY shard_id ASC",
      excludeShardId,
    ).map((s) => s.shard_id);
    const all = new Set(own);
    const config = this.one<{ catalog_shard_count: number | null; catalog_shard_id: string | null }>(
      "SELECT catalog_shard_count, catalog_shard_id FROM cluster_config WHERE singleton = 1",
    );
    const siblingCount = config?.catalog_shard_count ?? 0;
    for (let i = 0; i < siblingCount; i += 1) {
      const siblingId = `catalog-${i}`;
      if (config?.catalog_shard_id === siblingId) continue; // self, already counted
      try {
        const stub = this.catalogEnv.CATALOG.get(this.catalogEnv.CATALOG.idFromName(siblingId));
        const res = await stub.fetch("https://catalog.internal/list-shards", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: "{}",
        });
        if (!res.ok) continue;
        const body = (await res.json()) as { shardIds: string[] };
        for (const s of body.shardIds) {
          if (s !== excludeShardId) all.add(s);
        }
      } catch {
        // A sibling being unreachable narrows the candidate pool; it never
        // invents a wrong substitute.
      }
    }
    return Array.from(all).sort();
  }

  /** Every shard id in the cluster, ALL statuses (active + draining), across
   * this catalog's own pool and every sibling's. Unlike clusterActiveShards
   * this includes the draining shard and is used only to reach a shard's DATA
   * (here: to flush its queued index jobs), never for placement/substitution. */
  private async clusterAllShardIds(): Promise<string[]> {
    const own = this.many<{ shard_id: string }>("SELECT shard_id FROM shards ORDER BY shard_id ASC").map((s) => s.shard_id);
    const all = new Set(own);
    const config = this.one<{ catalog_shard_count: number | null; catalog_shard_id: string | null }>(
      "SELECT catalog_shard_count, catalog_shard_id FROM cluster_config WHERE singleton = 1",
    );
    const siblingCount = config?.catalog_shard_count ?? 0;
    for (let i = 0; i < siblingCount; i += 1) {
      const siblingId = `catalog-${i}`;
      if (config?.catalog_shard_id === siblingId) continue;
      try {
        const stub = this.catalogEnv.CATALOG.get(this.catalogEnv.CATALOG.idFromName(siblingId));
        const res = await stub.fetch("https://catalog.internal/list-shards", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ includeDraining: true }),
        });
        if (!res.ok) continue;
        const body = (await res.json()) as { shardIds: string[] };
        for (const s of body.shardIds) all.add(s);
      } catch {
        // A sibling being unreachable means we may miss some base shards' jobs;
        // callers treat any shortfall conservatively (block completion).
      }
    }
    return Array.from(all).sort();
  }

  /** Codex full-PR review P1 E: force-flush every queued index_pending_job
   * across ALL base shards whose target is the draining shard, and return how
   * many still remain cluster-wide. A job's target_shard_id is fixed at enqueue
   * time, so a job that resolved the OLD ring before the repoint would, if left
   * queued, fire AFTER the source delete and write an index entry onto the now
   * drained shard (outside the ring) — a silent /v1/index-query miss. Flushing
   * them while the ring still targets the draining shard makes their entries
   * land there, where ring evacuation's reconcile then copies them to the
   * substitute. An unreachable base shard is counted as "still pending" so the
   * caller never completes evacuation while a late write could still arrive. */
  private async flushIndexJobsTargeting(drainingShardId: string): Promise<number> {
    const shardIds = await this.clusterAllShardIds();
    let remaining = 0;
    for (const shardId of shardIds) {
      try {
        const res = await this.callShard(shardId, "/flush-index-jobs-for-target", { targetShardId: drainingShardId });
        if (!res.ok) {
          remaining += 1; // treat an unreachable/erroring base shard as pending
          continue;
        }
        remaining += ((await res.json()) as { remaining: number }).remaining;
      } catch {
        remaining += 1;
      }
    }
    return remaining;
  }

  /** Milestone 3, Chunk 5 (internal, catalog-to-catalog): repoints one
   * index's pinned placement ring — the draining catalog fans a completed
   * ring substitution to every sibling catalog shard, since index_rules is
   * replicated identically to all of them. DO-binding-only route, no admin
   * gate: it's never exposed through the Worker, the same trust model every
   * ShardDO internal route already uses. */
  private async handleUpdateIndexRing(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      indexName?: string;
      ring?: string[];
      evacFromShards?: string[];
      evacAdd?: string;
      evacRemove?: string;
    };
    if (!body.indexName || !body.ring) {
      return json({ error: "Missing indexName or ring" }, 400);
    }
    this.applyIndexRingUpdate(body.indexName, body.ring, {
      set: body.evacFromShards,
      add: body.evacAdd,
      remove: body.evacRemove,
    });
    return json({ ok: true, indexName: body.indexName });
  }

  /** Applies a ring repoint and a read-shadow (evac_from_shards_json) change
   * ATOMICALLY on THIS catalog. The shadow op is one of:
   *  - set:    replace the whole shadow (legacy/full-set callers);
   *  - add:    MERGE a draining shard into the existing shadow;
   *  - remove: drop only that shard, leaving any others;
   *  - none:   leave the shadow unchanged.
   * Codex round-15 P1 #1: add/remove (not a singleton set) is what keeps the
   * shadow the UNION of ALL concurrent same-index evacuations' draining shards —
   * a second drain's repoint must not overwrite (and drop) the first's shadow,
   * or /v1/index-query silently stops dual-reading the first draining shard. The
   * read-modify-write is atomic on a single-threaded DO (no awaits here). */
  private applyIndexRingUpdate(
    indexName: string,
    ring: string[],
    shadow: { set?: string[]; add?: string; remove?: string },
  ): void {
    this.sql.exec("UPDATE index_rules SET placement_ring_json = ? WHERE index_name = ?", JSON.stringify(ring), indexName);
    if (shadow.set !== undefined) {
      this.sql.exec("UPDATE index_rules SET evac_from_shards_json = ? WHERE index_name = ?", JSON.stringify(shadow.set), indexName);
    } else if (shadow.add !== undefined || shadow.remove !== undefined) {
      const row = this.one<{ evac_from_shards_json: string }>("SELECT evac_from_shards_json FROM index_rules WHERE index_name = ?", indexName);
      const cur = new Set<string>((row ? (JSON.parse(row.evac_from_shards_json) as string[]) : []));
      if (shadow.add !== undefined) cur.add(shadow.add);
      if (shadow.remove !== undefined) cur.delete(shadow.remove);
      this.sql.exec("UPDATE index_rules SET evac_from_shards_json = ? WHERE index_name = ?", JSON.stringify([...cur].sort()), indexName);
    }
    // else: leave the shadow untouched.
  }

  /** Codex round-14 P2 / round-15 P1 #1: fan an index's ring repoint and a
   * read-shadow change to every SIBLING catalog, then apply locally — the same
   * replication path so ring + shadow stay atomic on every catalog. `shadow`
   * carries the merge op (add/remove/set) so concurrent same-index evacuations
   * union their draining shards rather than clobber each other. Throws on a
   * sibling failure so the drain retries the whole (idempotent) fan-out. */
  private async fanUpdateIndexRing(
    indexName: string,
    ring: string[],
    shadow: { set?: string[]; add?: string; remove?: string },
  ): Promise<void> {
    const config = this.one<{ catalog_shard_count: number | null; catalog_shard_id: string | null }>(
      "SELECT catalog_shard_count, catalog_shard_id FROM cluster_config WHERE singleton = 1",
    );
    const siblingCount = config?.catalog_shard_count ?? 0;
    for (let i = 0; i < siblingCount; i += 1) {
      const siblingId = `catalog-${i}`;
      if (config?.catalog_shard_id === siblingId) continue; // self applied locally below
      const stub = this.catalogEnv.CATALOG.get(this.catalogEnv.CATALOG.idFromName(siblingId));
      const res = await stub.fetch("https://catalog.internal/update-index-ring", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ indexName, ring, evacFromShards: shadow.set, evacAdd: shadow.add, evacRemove: shadow.remove }),
      });
      if (!res.ok) throw new Error(`update-index-ring failed on ${siblingId}: ${res.status}`);
    }
    // Every sibling applied — now locally (never via a DO self-fetch).
    this.applyIndexRingUpdate(indexName, ring, shadow);
  }

  /** Codex live-deployment finding: fans an index's building→ready flip to
   * every SIBLING catalog (each via its own already-existing
   * /mark-index-ready route — no new sibling-side route needed, unlike
   * fanUpdateIndexRing's /update-index-ring), then applies locally.
   * Idempotent per-sibling exactly like /mark-index-ready itself; a thrown
   * error here (a sibling unreachable) propagates to advanceIndexBackfill's
   * caller, which treats it as a normal throwing tick — retried, with
   * backoff, next alarm, leaving backfill_shard_idx already past every
   * shard (so the retried fan-out is the ONLY remaining work, not a
   * redundant re-scan). */
  private async fanMarkIndexReady(indexName: string): Promise<void> {
    const config = this.one<{ catalog_shard_count: number | null; catalog_shard_id: string | null }>(
      "SELECT catalog_shard_count, catalog_shard_id FROM cluster_config WHERE singleton = 1",
    );
    const siblingCount = config?.catalog_shard_count ?? 0;
    for (let i = 0; i < siblingCount; i += 1) {
      const siblingId = `catalog-${i}`;
      if (config?.catalog_shard_id === siblingId) continue; // self applied locally below
      const stub = this.catalogEnv.CATALOG.get(this.catalogEnv.CATALOG.idFromName(siblingId));
      const res = await stub.fetch("https://catalog.internal/mark-index-ready", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ indexName }),
      });
      if (!res.ok) throw new Error(`mark-index-ready failed on ${siblingId}: ${res.status}`);
    }
    this.sql.exec("UPDATE index_rules SET status = 'ready' WHERE index_name = ?", indexName);
  }

  /** Codex live-deployment finding: one alarm-tick's worth of index backfill
   * work for ONE index (catalog-0 only — see index_rules schema comment).
   * Returns true while more ticks are needed, false once this index is
   * fully backfilled and flipped to 'ready'.
   *
   * Bounded to INDEX_BACKFILL_PAGE_SIZE rows per tick: scans the current
   * shard's next page (one subrequest), then for each row re-reads its
   * CURRENT values plus its __cf_row_owners tenant_id (one subrequest) and
   * writes its index entry (one subrequest — this step can't batch, since
   * different rows can hash to DIFFERENT index shards). The per-row refresh
   * was briefly batched into one query for the whole page (an "Eng-review
   * fix" for subrequest cost) but Codex round 6 found that widened the
   * staleness window this refresh exists to close: a page can take up to
   * INDEX_BACKFILL_PAGE_SIZE write round trips, and a concurrent /v1/mutate
   * on a row not yet written can land anywhere in that window, not just
   * between the scan and a single page-level read. Reverted to reading each
   * row immediately before ITS OWN write narrows the race back to one
   * read-then-write round trip per row, matching the original synchronous
   * version's own reasoning here. */
  private async advanceIndexBackfill(row: IndexBackfillRow): Promise<boolean> {
    const shardIds = JSON.parse(row.backfill_shard_ids_json) as string[];
    if (row.backfill_shard_idx >= shardIds.length) {
      // Every shard already scanned (a prior tick advanced the cursor but a
      // later step — the ready-flip fan-out — threw and left this
      // unfinished). Just retry completion; no scanning work left.
      await this.fanMarkIndexReady(row.index_name);
      this.sql.exec(
        "UPDATE index_rules SET backfill_shard_ids_json = '[]', backfill_shard_idx = 0, backfill_after_pk = '', topology_lock_operation_id = NULL WHERE index_name = ?",
        row.index_name,
      );
      await this.releaseTopologyLockRemote(row.topology_lock_operation_id);
      return false;
    }

    const columns = JSON.parse(row.columns_json) as string[];
    const table = this.one<{ partition_key_column: string }>("SELECT partition_key_column FROM table_rules WHERE table_name = ?", row.table_name);
    if (!table) throw new Error(`table ${row.table_name} no longer registered mid-backfill`);
    const pkCol = table.partition_key_column;
    const safeTable = `"${row.table_name}"`;
    const safePk = `"${pkCol}"`;
    const selectCols = [pkCol, ...columns].map((c) => `"${c}"`).join(", ");
    const shardId = shardIds[row.backfill_shard_idx];

    // Codex review P1 fix: this shard's FIRST page (backfill_after_pk still
    // at its '' default) must not filter with `WHERE pk > ''` at all. That
    // predicate is only safe once a REAL previously-seen key is bound --
    // SQLite's affinity rules convert a numeric-looking TEXT parameter (e.g.
    // "251") for comparison against an INTEGER-affinity column, so later
    // pages compare correctly (proven directly: `WHERE id > '250'` against
    // an INTEGER PRIMARY KEY column correctly returns only rows > 250). An
    // EMPTY string parameter doesn't convert the same way: SQLite's type
    // ordering ranks any INTEGER below any TEXT, so `id > ''` is FALSE for
    // every existing integer value, not true -- the first page would
    // silently scan zero rows on any table using a non-TEXT-affinity
    // partition key column, and the shard-exhausted branch below would then
    // advance past it as if it had no data at all, permanently skipping
    // every pre-existing row. Omitting the predicate entirely for the
    // sentinel case sidesteps the whole affinity question -- ORDER BY +
    // LIMIT alone correctly return "the first page," for any column type.
    const hasCursor = row.backfill_after_pk !== "";
    const scanRes = await this.callShard(shardId, "/execute", {
      sql: hasCursor
        ? `SELECT ${selectCols} FROM ${safeTable} WHERE ${safePk} > ? ORDER BY ${safePk} ASC LIMIT ?`
        : `SELECT ${selectCols} FROM ${safeTable} ORDER BY ${safePk} ASC LIMIT ?`,
      params: hasCursor ? [row.backfill_after_pk, INDEX_BACKFILL_PAGE_SIZE] : [INDEX_BACKFILL_PAGE_SIZE],
      requestId: `create-index-backfill-scan-${row.index_name}-${shardId}-${crypto.randomUUID()}`,
      isMutation: false,
    });
    // Codex round-4 fix: callShard resolves (doesn't reject) for a
    // well-formed HTTP error response -- a genuinely transient failure
    // (the shard DO unavailable, a network blip) throws instead and is
    // caught by alarm()'s own try/catch below as an ordinary retryable
    // error. A resolved !ok response here means the shard's /execute
    // handler itself rejected this exact query (shard.ts's only non-2xx
    // path for a read-only scan is its catch-all "SQL execution failed",
    // e.g. the table or an indexed column is missing on THIS shard) --
    // that can never self-resolve by retrying the same query against the
    // same shard schema, so (like PROVENANCE_MISSING_FOR_INDEX) it must
    // give up and release the lock rather than heartbeat it forever.
    if (!scanRes.ok) {
      const errorText = await scanRes.text().catch(() => "");
      throw new PermanentIndexBackfillError(
        `backfill scan failed on shard ${shardId} for index ${row.index_name}, table ${row.table_name} (HTTP ${scanRes.status}): ${errorText || "no response body"} -- likely a schema mismatch (the table or an indexed column may be missing on this shard); an operator must reconcile the shard's schema, then retry`,
      );
    }
    const scanBody = (await scanRes.json()) as { rows: Array<Record<string, unknown>> };
    const page = scanBody.rows;

    if (page.length === 0) {
      // This shard is exhausted -- advance to the next one and keep ticking
      // (or finish, if that was the last shard).
      this.sql.exec(
        "UPDATE index_rules SET backfill_shard_idx = ?, backfill_after_pk = '' WHERE index_name = ? AND status = 'building'",
        row.backfill_shard_idx + 1,
        row.index_name,
      );
      // Always one more tick: either to scan the next shard, or (if that was
      // the last one) to run this same method's completion branch above.
      return true;
    }

    const pageKeys = page.map((r) => String(r[pkCol]));

    // Codex round-6 fix: refresh each row IMMEDIATELY before writing its
    // index entry, not once for the whole page up front. A page can take up
    // to INDEX_BACKFILL_PAGE_SIZE write round trips; a live /v1/mutate can
    // update this exact row's indexed column(s) at any point during that
    // window. A page-level batched read (the prior "Eng-review fix",
    // reverted here) snapshots every row's value ONCE before any writes
    // start -- if a row changes between that snapshot and this row's turn in
    // the loop, the backfill would write a now-STALE index_key_json entry
    // that the live write's own index maintenance has no way to know about
    // or clean up (it only touches the key pair for ITS OWN write).
    // __cf_indexes is keyed by index_key_json, so that stale entry can
    // persist forever once the index reaches 'ready', and enough of them for
    // the same logical key can exhaust /v1/index-query's rawScanCap and hide
    // later live matches. Costs one extra subrequest per row (matching
    // INDEX_BACKFILL_PAGE_SIZE's doc comment, updated below) -- still
    // comfortably under Cloudflare's per-invocation cap per tick.
    const placementRing = JSON.parse(row.placement_ring_json) as string[];
    let rowsWritten = 0;
    let rowsSinceHeartbeat = 0;
    for (const pk of pageKeys) {
      const freshRes = await this.callShard(shardId, "/execute", {
        sql: `SELECT ${selectCols}, ro.tenant_id AS __cf_tenant_id FROM ${safeTable} b LEFT JOIN __cf_row_owners ro ON ro.table_name = ? AND ro.partition_key = b.${safePk} WHERE b.${safePk} = ?`,
        params: [row.table_name, pk],
        requestId: `create-index-backfill-refresh-${row.index_name}-${shardId}-${crypto.randomUUID()}`,
        isMutation: false,
      });
      if (!freshRes.ok) throw new Error(`backfill refresh failed for partitionKey ${pk} on shard ${shardId}, index ${row.index_name}: ${freshRes.status}`);
      const freshBody = (await freshRes.json()) as { rows: Array<Record<string, unknown> & { __cf_tenant_id: string | null }> };
      const freshRow = freshBody.rows[0];
      if (!freshRow) continue; // deleted since the scan -- skip rather than index stale data
      if (!freshRow.__cf_tenant_id) {
        throw new PermanentIndexBackfillError(
          `row ${pk} on shard ${shardId}, table ${row.table_name} has no row-provenance entry (__cf_row_owners) -- run /admin/backfill-provenance for this shard, then retry`,
        );
      }
      rowsSinceHeartbeat += 1;
      if (rowsSinceHeartbeat >= INDEX_BACKFILL_HEARTBEAT_ROW_INTERVAL) {
        rowsSinceHeartbeat = 0;
        if (!(await this.heartbeatTopologyLockOrPark(row.topology_lock_operation_id))) {
          throw new Error(`topology lock lost partway through a backfill page for index ${row.index_name}`);
        }
      }
      const tenantId = freshRow.__cf_tenant_id;
      const indexKeyJson = JSON.stringify(columns.map((c) => freshRow[c] ?? null));
      const sql =
        "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)";
      const params = [row.table_name, row.index_name, indexKeyJson, pk, shardId, tenantId, new Date().toISOString()];
      const requestId = `create-index-backfill-write-${row.index_name}-${crypto.randomUUID()}`;
      const target = indexShardIdForKey(row.table_name, row.index_name, indexKeyJson, placementRing);
      let wrote = await this.callShard(target, "/execute", { sql, params, requestId, isMutation: true, indexName: row.index_name });
      if (!wrote.ok) {
        // Codex round-14 P2 equivalent (see the retired Worker-side
        // backfillWriteIndexEntry this replaces): on INDEX_RING_FENCED,
        // re-resolve against THIS instance's own (always up to date —
        // catalog-0 is the ring's canonical home) placement_ring_json
        // instead of the possibly-stale copy captured when this tick
        // started, and retry once against the substitute.
        const errBody = (await wrote.clone().json().catch(() => ({}))) as { error?: { code?: string } };
        if (errBody.error?.code === "INDEX_RING_FENCED") {
          const freshRingRow = this.one<{ placement_ring_json: string }>("SELECT placement_ring_json FROM index_rules WHERE index_name = ?", row.index_name);
          const freshRing = freshRingRow ? (JSON.parse(freshRingRow.placement_ring_json) as string[]) : [];
          if (freshRing.length > 0) {
            const resolved = indexShardIdForKey(row.table_name, row.index_name, indexKeyJson, freshRing);
            if (resolved !== target) {
              wrote = await this.callShard(resolved, "/execute", { sql, params, requestId, isMutation: true, indexName: row.index_name });
            }
          }
        }
        if (!wrote.ok) throw new Error(`backfill write failed for partitionKey ${pk} on index ${row.index_name}: ${wrote.status}`);
      }
      rowsWritten += 1;
    }

    const lastPk = pageKeys[pageKeys.length - 1];
    const exhausted = page.length < INDEX_BACKFILL_PAGE_SIZE;
    this.sql.exec(
      exhausted
        ? "UPDATE index_rules SET backfill_shard_idx = backfill_shard_idx + 1, backfill_after_pk = '', backfill_rows_copied = backfill_rows_copied + ? WHERE index_name = ? AND status = 'building'"
        : "UPDATE index_rules SET backfill_after_pk = ?, backfill_rows_copied = backfill_rows_copied + ? WHERE index_name = ? AND status = 'building'",
      ...(exhausted ? [rowsWritten, row.index_name] : [lastPk, rowsWritten, row.index_name]),
    );
    return true;
  }

  /** Milestone 3, Chunk 5: one drain-orchestration step for one draining
   * shard, called from alarm(). Returns true while more ticks are needed.
   *
   * Phase 1 — vbuckets: migrate every vbucket mapped to the draining shard
   * off it, strictly sequentially (one in-flight migration at a time),
   * targets rotated deterministically across the remaining active shards.
   * Phase 2 — rings: for each index whose pinned ring contains the shard,
   * substitute deterministically (pickRingSubstitute), copy the draining
   * shard's entries for that index to the substitute, repoint the ring on
   * every catalog shard, then delete the source copies. */
  private async advanceDrain(shardId: string): Promise<boolean> {
    // Review Tier 2 #10: a drain parked on the provenance gate does NOT keep
    // re-scanning every table on the source at the tick cadence — it stays
    // parked until the operator re-invokes /admin/drain-shard (which clears
    // this after they run /admin/backfill-provenance).
    //
    // Codex full-PR review P1 D: 'ring-reconcile-unstable' is the ONE
    // exception — it is a SOFT stall. Ring evacuation now converges (copies
    // every straggler) BEFORE it repoints, so while unstable the ring still
    // points at the draining shard (reads resolve, nothing stranded) and the
    // next tick must REVISIT this index to retry, not hard-park behind an
    // operator re-invoke. It clears itself once the write churn stops and the
    // reconcile converges. All other reasons are genuine hard parks.
    const stall = this.one<{ drain_stall_reason: string | null; topology_lock_operation_id: string | null }>(
      "SELECT drain_stall_reason, topology_lock_operation_id FROM shards WHERE shard_id = ?",
      shardId,
    );
    if (stall?.drain_stall_reason && stall.drain_stall_reason !== "ring-reconcile-unstable" && stall.drain_stall_reason !== "topology-lock-lost") {
      return false;
    }

    // Approved design (Stage 3): a long-running drain heartbeats the topology
    // lock it acquired at start on every tick. If the lock is lost (force-
    // released, or its lease somehow lapsed), STOP mutating immediately —
    // every destructive step below (migration flips, source deletes, ring
    // deletes) lives inside the calls this early-return skips — and park with
    // a distinguishable stall reason rather than risk racing a topology
    // operation that may now be running against the same shards. A
    // 'topology-lock-lost' park is a SOFT stall like ring-reconcile-unstable
    // (see the check above): the alarm keeps revisiting it in case the lock
    // situation resolves, rather than requiring an operator re-invoke.
    const drainOperationId = stall?.topology_lock_operation_id ?? null;
    const stillHeld = await this.heartbeatTopologyLockOrPark(drainOperationId);
    if (!stillHeld) {
      this.sql.exec("UPDATE shards SET drain_stall_reason = 'topology-lock-lost' WHERE shard_id = ?", shardId);
      log("catalog.drain_topology_lock_lost", { shardId, operationId: drainOperationId });
      return true; // keep ticking — revisit next tick in case the lock is restored
    }
    if (stall?.drain_stall_reason === "topology-lock-lost") {
      // The lock came back (heartbeat above succeeded) — clear the soft stall
      // and fall through to resume normal evacuation this same tick.
      this.sql.exec("UPDATE shards SET drain_stall_reason = NULL WHERE shard_id = ?", shardId);
    }

    const vbuckets = this.many<{ vbucket: number; migration_status: string }>(
      "SELECT vbucket, migration_status FROM vbucket_map WHERE shard_id = ? ORDER BY vbucket ASC",
      shardId,
    );
    if (vbuckets.length > 0) {
      const inFlight = vbuckets.filter((v) => v.migration_status !== "none");
      if (inFlight.length > 0) {
        // Only 'backfilling'/'cutover' rows are advanced by the alarm's
        // migration loop each tick. Codex full-PR review P1 C: a vbucket wedged
        // in 'aborting' (an abort whose cleanup failed) — or any other
        // unexpected status — is NEVER advanced there, so blanket-returning true
        // spun the 250ms alarm forever with /drain-shard-status stuck on
        // 'migrating-vbuckets'. Keep ticking only while a genuinely-advancing
        // migration is in flight; otherwise PARK with a distinct stall reason so
        // the alarm stops and the operator knows to retry the abort.
        const advancing = inFlight.some((v) => v.migration_status === "backfilling" || v.migration_status === "cutover");
        if (advancing) {
          return true; // an active migration is progressing via the migration loop
        }
        const stuck = inFlight[0];
        this.sql.exec("UPDATE shards SET drain_stall_reason = 'aborting-migration' WHERE shard_id = ?", shardId);
        log("catalog.drain_stalled_aborting", { shardId, vbucket: stuck.vbucket, status: stuck.migration_status });
        return false;
      }
      const activeOthers = this.many<{ shard_id: string }>(
        "SELECT shard_id FROM shards WHERE status = 'active' AND shard_id != ? ORDER BY shard_id ASC",
        shardId,
      ).map((s) => s.shard_id);
      if (activeOthers.length === 0) {
        // Nowhere to move data within this catalog's pool — behave like the
        // pre-M3 drain (marked draining, no data moved) rather than spinning
        // the alarm forever. Re-calling /admin/drain-shard after adding
        // capacity resumes the evacuation.
        log("catalog.drain_stalled_no_target", { shardId, vbucketsRemaining: vbuckets.length });
        return false;
      }
      const next = vbuckets[0].vbucket;
      const target = activeOthers[next % activeOthers.length];
      // The sub-migration inherits the DRAIN's own topology-lock operationId
      // (not a fresh acquire — the drain already holds the one lock for its
      // whole duration) so alarm()'s migration loop heartbeats the same lock
      // while this vbucket's migration is advancing.
      const started = await this.startMigration(next, target, "/drain-shard-migrate", drainOperationId ?? undefined);
      if (started instanceof Response) {
        // Park the drain so the alarm stops re-running the full-table
        // provenance scan every tick (review Tier 2 #10). Re-review item E:
        // record the ACTUAL reason rather than always claiming 'provenance' —
        // an incomplete-provenance rejection is fixed by
        // /admin/backfill-provenance, but a MIGRATION_IN_PROGRESS (e.g. a
        // vbucket wedged in 'aborting') or an unreachable-shard 502 is not, so
        // mislabeling it sends the operator down the wrong path.
        let code: string | undefined;
        try {
          code = ((await started.clone().json()) as { error?: { code?: string } }).error?.code;
        } catch {
          // Non-JSON body (e.g. a plain-string error) — leave code undefined.
        }
        // Codex full-PR review P1 A: a MIGRATION_CLEANUP_PENDING rejection is
        // TRANSIENT — the same alarm's post-flip cleanup loop is already
        // retrying that vbucket's source delete/unfence and will clear
        // cleanup_pending on its own, after which the next tick's startMigration
        // succeeds. Keep ticking (return true) rather than parking the whole
        // drain behind an operator re-invoke.
        if (code === "MIGRATION_CLEANUP_PENDING") {
          log("catalog.drain_waiting_on_cleanup", { shardId, vbucket: next });
          return true;
        }
        const reason = code === "VBUCKET_PROVENANCE_INCOMPLETE" ? "provenance" : "migration-blocked";
        this.sql.exec("UPDATE shards SET drain_stall_reason = ? WHERE shard_id = ?", reason, shardId);
        log("catalog.drain_stalled", { shardId, vbucket: next, reason, code });
        return false;
      }
      // Advance it immediately — a quiet vbucket completes this same tick.
      const row = this.one<MigrationRow>(
        "SELECT vbucket, shard_id, target_shard_id, migration_status, migration_rows_copied, topology_lock_operation_id FROM vbucket_map WHERE vbucket = ?",
        next,
      );
      if (row && row.migration_status !== "none") {
        await this.advanceMigration(row);
      }
      return true;
    }

    // Phase 2: ring evacuation — FENCE-FIRST, marker-driven (Codex round-13).
    // The prior converge-before-repoint design could not close the race: an
    // in-flight index write that resolved the OLD ring before the repoint is
    // delayed in the gateway's ctx.waitUntil — not yet in __cf_indexes or any
    // queue — so no "copy existing entries" pass can see it, and it lands on the
    // drained shard after the source delete. The fix fences the index's ring on
    // the draining shard (so such a write 409s → the writer re-resolves to the
    // substitute), repoints EARLY (so re-resolution finds the substitute), then
    // flushes queued retries, copies, and deletes. Progress is tracked by the
    // drain_ring_evac MARKER (not ring membership, which the early repoint
    // clears), so a not-yet-finished index is revisited on later ticks.
    const indexRules = this.many<{ index_name: string; placement_ring_json: string }>(
      "SELECT index_name, placement_ring_json FROM index_rules ORDER BY index_name ASC",
    );
    const ringContains = indexRules.filter((r) => (JSON.parse(r.placement_ring_json) as string[]).includes(shardId));
    const markers = this.many<{ index_name: string; substitute: string }>(
      "SELECT index_name, substitute FROM drain_ring_evac WHERE shard_id = ? ORDER BY index_name ASC",
      shardId,
    );
    const markerByIndex = new Map(markers.map((m) => [m.index_name, m.substitute]));

    if (ringContains.length === 0 && markers.length === 0) {
      // Nothing to evacuate — clear any lingering soft stall and finish.
      this.sql.exec(
        "UPDATE shards SET drain_stall_reason = NULL WHERE shard_id = ? AND drain_stall_reason IN ('ring-reconcile-unstable', 'topology-lock-lost')",
        shardId,
      );
      // Fully complete (vbuckets evacuated, no rings ever pinned this shard) —
      // release the topology lock this drain has held since start.
      await this.releaseDrainTopologyLock(shardId);
      return false;
    }

    // Candidate substitutes are only needed for indexes not already fenced (a
    // marker pins its substitute for the index's whole evacuation).
    const needSubstitute = ringContains.some((r) => !markerByIndex.has(r.index_name));
    const clusterActive = needSubstitute ? await this.clusterActiveShards(shardId) : [];

    // Union of ring-resident (not yet started) and marked (in flight) indexes.
    const workIndexNames = Array.from(
      new Set<string>([...ringContains.map((r) => r.index_name), ...markers.map((m) => m.index_name)]),
    ).sort();

    let anyIncomplete = false;
    for (const indexName of workIndexNames) {
      const rule = indexRules.find((r) => r.index_name === indexName);
      let substitute = markerByIndex.get(indexName);

      // (a)/(b) Not yet fenced: pick the substitute, FENCE the index on the
      // draining shard, and record the marker. From this instant any new write
      // for this index arriving on the draining shard 409s INDEX_RING_FENCED, so
      // an in-flight write that resolved the old ring can only be turned away
      // (→ re-resolved to the substitute), never stranded here.
      if (substitute === undefined) {
        const ring = rule ? (JSON.parse(rule.placement_ring_json) as string[]) : [];
        if (ring.indexOf(shardId) === -1) continue; // not resident, no marker
        const picked = pickRingSubstitute(indexName, clusterActive.filter((s) => !ring.includes(s)));
        if (picked === null) {
          // No capacity to substitute — leave the ring intact (reads keep
          // working against the draining shard) for operator action.
          log("catalog.ring_evacuation_no_candidate", { shardId, indexName });
          continue;
        }
        const fenceRes = await this.callShard(shardId, "/fence-index-ring", { indexName });
        if (!fenceRes.ok) {
          anyIncomplete = true; // couldn't fence — retry next tick
          continue;
        }
        substitute = picked;
        this.sql.exec(
          "INSERT OR REPLACE INTO drain_ring_evac (shard_id, index_name, substitute, created_at) VALUES (?, ?, ?, ?)",
          shardId,
          indexName,
          substitute,
          new Date().toISOString(),
        );
      }

      try {
        // (c) Repoint the ring S→substitute (idempotent — only if the current
        // ring still contains the draining shard). Codex round-14 P2: repoint
        // the ring AND set the read-shadow (evacFromShards=[shardId]) atomically
        // on every catalog via the same fan-out, so wherever a query sees the
        // repointed ring it also knows to dual-look-up the draining shard's
        // not-yet-copied entries. Siblings first, then local — a sibling failure
        // throws with local un-repointed and the marker + fence persisting →
        // retried next tick.
        //
        // Codex round-16 defense-in-depth: RE-READ placement_ring_json HERE,
        // at write time — not `rule` (a snapshot captured at the TOP of this
        // function, before every await since). A DIFFERENT evacuation of
        // ANOTHER position in this SAME ring (a sibling catalog's own drain,
        // or — within this catalog — a later-in-the-same-tick advanceDrain
        // call for a different draining shard) can apply its own
        // /update-index-ring during one of those awaits; substituting into a
        // stale full-array snapshot would silently REVERT that concurrent
        // substitution back to its old (possibly already-decommissioned)
        // shard the moment this call's repoint fans out. Re-reading and
        // substituting only THIS shard's position is safe regardless: this
        // drain only ever touches the one position it owns.
        const freshRingRow = this.one<{ placement_ring_json: string }>(
          "SELECT placement_ring_json FROM index_rules WHERE index_name = ?",
          indexName,
        );
        const currentRing = freshRingRow ? (JSON.parse(freshRingRow.placement_ring_json) as string[]) : [];
        const pos = currentRing.indexOf(shardId);
        if (pos !== -1) {
          const newRing = [...currentRing];
          newRing[pos] = substitute;
          // Codex round-15 P1 #1: MERGE this draining shard into the shadow
          // (never overwrite) so a concurrent evacuation of another shard in the
          // same index's ring keeps its own shadow entry.
          await this.fanUpdateIndexRing(indexName, newRing, { add: shardId });
        }

        // (d) Flush queued index_pending_jobs targeting the draining shard
        // (cluster-wide). With the fence + repoint in place, a delivered job
        // 409s → re-resolves to the substitute; an unreachable base shard leaves
        // remaining > 0 → not done this tick (fence backstops safety meanwhile).
        const remainingJobs = await this.flushIndexJobsTargeting(shardId);
        if (remainingJobs > 0) {
          anyIncomplete = true;
          continue; // keep marker + fence, retry next tick
        }

        // (e) Copy the draining shard's entries for this index → substitute,
        // reconcile to zero-new. The fence guarantees no NEW entry lands on the
        // draining shard, so this converges (bounded pass budget as a guard).
        let afterRowid = 0;
        let converged = false;
        for (let pass = 0; pass < RING_EVAC_RECONCILE_MAX_PASSES; pass += 1) {
          const before = afterRowid;
          afterRowid = await this.copyIndexEntries(shardId, substitute, indexName, afterRowid);
          if (afterRowid === before) {
            converged = true;
            break;
          }
        }
        if (!converged) {
          anyIncomplete = true;
          continue; // retry next tick (marker persists, nothing deleted)
        }

        // (f) Delete the source copies — everything is on the substitute and no
        // new write can arrive (fenced + repointed).
        const deleteRes = await this.callShard(shardId, "/execute", {
          sql: "DELETE FROM __cf_indexes WHERE index_name = ?",
          params: [indexName],
          requestId: `ring-evacuate-${indexName}-${shardId}-${crypto.randomUUID()}`,
          isMutation: true,
        });
        if (!deleteRes.ok) {
          log("catalog.ring_evacuation_source_cleanup_failed", { shardId, indexName });
          anyIncomplete = true;
          continue; // retry the delete next tick
        }

        // Codex round-14 P2: the source entries are gone, so CLEAR the
        // read-shadow (evacFromShards=[]) on every catalog via the same fan-out
        // — reads now hit only the substitute. Done BEFORE deleting the marker
        // and inside the try, so a sibling failure throws → marker persists →
        // the whole (idempotent) clear retries next tick (never leaving a stale
        // shadow pointing at a decommissioned shard).
        const currentRow = this.one<{ placement_ring_json: string }>("SELECT placement_ring_json FROM index_rules WHERE index_name = ?", indexName);
        const finalRing = currentRow ? (JSON.parse(currentRow.placement_ring_json) as string[]) : [];
        // Codex round-15 P1 #1: REMOVE only this drain's shard from the shadow,
        // leaving any concurrent evacuation's shard still shadowed.
        await this.fanUpdateIndexRing(indexName, finalRing, { remove: shardId });

        // Complete — clear the marker. The fence is LEFT in place: the shard is
        // being decommissioned, and a stray late write to it for this index
        // 409s → re-resolves to the substitute (a safe backstop). It is only
        // explicitly released on an abort/failure path.
        this.sql.exec("DELETE FROM drain_ring_evac WHERE shard_id = ? AND index_name = ?", shardId, indexName);
        this.audit("/drain-shard-ring-evacuated", { shardId, indexName, substitute });
      } catch (error) {
        // Repoint/copy/delete failure — marker + fence persist; retry next tick.
        log("catalog.ring_evacuation_tick_failed", { shardId, indexName, message: error instanceof Error ? error.message : String(error) });
        anyIncomplete = true;
      }
    }

    if (anyIncomplete) {
      // At least one index isn't fully evacuated. Record the soft stall so
      // /drain-shard-status reports NON-'complete', and keep ticking so a later
      // tick retries (the marker drives the revisit, not ring membership).
      this.sql.exec("UPDATE shards SET drain_stall_reason = 'ring-reconcile-unstable' WHERE shard_id = ?", shardId);
      return true;
    }
    // Every ring evacuated — clear any prior soft stall.
    this.sql.exec(
      "UPDATE shards SET drain_stall_reason = NULL WHERE shard_id = ? AND drain_stall_reason IN ('ring-reconcile-unstable', 'topology-lock-lost')",
      shardId,
    );
    // Fully complete (vbuckets evacuated AND every ring evacuated) — release
    // the topology lock this drain has held since start.
    await this.releaseDrainTopologyLock(shardId);
    return false;
  }

  /** Releases (best-effort) and clears the topology-lock operationId a NOW-
   * COMPLETE drain of `shardId` was holding. No-op if none was ever recorded
   * (e.g. a drain never mediated by the Worker). */
  private async releaseDrainTopologyLock(shardId: string): Promise<void> {
    const row = this.one<{ topology_lock_operation_id: string | null }>(
      "SELECT topology_lock_operation_id FROM shards WHERE shard_id = ?",
      shardId,
    );
    if (!row?.topology_lock_operation_id) return;
    await this.releaseTopologyLockRemote(row.topology_lock_operation_id);
    this.sql.exec("UPDATE shards SET topology_lock_operation_id = NULL WHERE shard_id = ?", shardId);
  }

  /** Copies one index's __cf_indexes entries from `fromShard` to `toShard`
   * with rowid > afterRowid, paged, and returns the highest rowid copied (or
   * afterRowid unchanged if nothing). Used by ring evacuation's initial copy
   * and its reconcile loop. */
  private async copyIndexEntries(fromShard: string, toShard: string, indexName: string, afterRowid: number): Promise<number> {
    let cursor = afterRowid;
    for (;;) {
      const exportRes = await this.callShard(fromShard, "/index-entries-export", { indexName, afterRowid: cursor, limit: MIGRATE_PAGE_SIZE });
      if (!exportRes.ok) throw new Error(`index-entries-export failed on ${fromShard}: ${exportRes.status}`);
      const rows = ((await exportRes.json()) as { rows: Array<{ rowid: number }> }).rows;
      if (rows.length === 0) break;
      const importRes = await this.callShard(toShard, "/index-entries-import", { rows });
      if (!importRes.ok) throw new Error(`index-entries-import failed on ${toShard}: ${importRes.status}`);
      cursor = rows[rows.length - 1].rowid;
      if (rows.length < MIGRATE_PAGE_SIZE) break;
    }
    return cursor;
  }

  /** Milestone 3, Chunk 4: /admin/split-vbucket keeps its name and request
   * shape, but "split" now means "create the target shard and start a real
   * data migration" instead of repointing vbucket_map and stranding every
   * row already on the source (the pre-M3 behavior). The response gains
   * migrationStarted: true; routing flips only when the migration's fenced
   * cutover completes (steps 1-5 in advanceMigration). */
  private async handleSplitVbucket(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      vbucket: number;
      newShardId?: string;
      operationId?: string;
    };

    if (!Number.isInteger(body.vbucket) || body.vbucket < 0) {
      return json({ error: "vbucket must be a non-negative integer" }, 400);
    }

    // Milestone 3, Chunk 2: splitting no longer blocks on index presence
    // (the previous 409 SPLIT_BLOCKED_BY_INDEXES is removed) — index
    // placement hashes over each index's own pinned placement_ring_json,
    // so adding a new active shard never changes existing index placement.
    const started = await this.startMigration(body.vbucket, body.newShardId, "/split-vbucket", body.operationId);
    if (started instanceof Response) return started;

    return json({
      ok: true,
      vbucket: body.vbucket,
      fromShard: started.fromShard,
      toShard: started.toShard,
      metadataVersion: started.metadataVersion,
      migrationStarted: true,
    });
  }

  /** Milestone 3, Chunk 4 (POST /migrate-vbucket {vbucket, targetShardId?}).
   * Same primitive /split-vbucket now builds on, with the target shard
   * explicit/optional rather than always fresh. */
  private async handleMigrateVbucket(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number; targetShardId?: string; operationId?: string };
    if (body.vbucket === undefined || !Number.isInteger(body.vbucket) || body.vbucket < 0) {
      return json({ error: "vbucket must be a non-negative integer" }, 400);
    }
    const started = await this.startMigration(body.vbucket, body.targetShardId, "/migrate-vbucket", body.operationId);
    if (started instanceof Response) return started;
    return json({
      ok: true,
      vbucket: body.vbucket,
      fromShard: started.fromShard,
      toShard: started.toShard,
      status: "backfilling",
    });
  }

  /** Registered tables eligible for migration — a table still carrying the
   * UNSET sentinel can't be exported (no partition-key column to page on). */
  private migratableTables(): Array<{ table: string; partitionKeyColumn: string; schemaSql: string | null }> {
    return this.many<{ table_name: string; partition_key_column: string; schema_sql: string | null }>(
      "SELECT table_name, partition_key_column, schema_sql FROM table_rules ORDER BY table_name ASC",
    )
      .filter((t) => t.partition_key_column !== UNSET_PARTITION_KEY_COLUMN)
      .map((t) => ({ table: t.table_name, partitionKeyColumn: t.partition_key_column, schemaSql: t.schema_sql }));
  }

  /** Shared migration-start guard + state transition for /split-vbucket and
   * /migrate-vbucket. Returns a Response on rejection. */
  private async startMigration(
    vbucket: number,
    requestedTargetShardId: string | undefined,
    endpoint: string,
    operationId?: string,
  ): Promise<Response | { fromShard: string; toShard: string; metadataVersion: number }> {
    const existingMap = this.one<{ shard_id: string; migration_status: string; cleanup_pending: number }>(
      "SELECT shard_id, migration_status, cleanup_pending FROM vbucket_map WHERE vbucket = ?",
      vbucket,
    );
    if (!existingMap) {
      return json({ error: `vbucket ${vbucket} has no mapping` }, 404);
    }
    if (existingMap.migration_status !== "none") {
      return json(
        {
          error: {
            code: "MIGRATION_IN_PROGRESS",
            message: `vbucket ${vbucket} already has a migration in progress (status: ${existingMap.migration_status}).`,
            fix: "Wait for it to finish (/admin/migrate-vbucket-status) or abort it (/admin/migrate-vbucket-abort).",
          },
        },
        409,
      );
    }
    // Codex full-PR review P1 A: a prior cutover can flip migration_status to
    // 'none' while post-flip cleanup (delete source rows + unfence source) is
    // still retrying (cleanup_pending=1). Starting a NEW migration for this
    // vbucket now would overwrite cleanup_source_shard_id on the flip, orphaning
    // the old source's still-set __cf_fenced_vbuckets entry — and if the new
    // target happens to equal that old source, it would stay fenced forever
    // (503/VBUCKET_FENCED on every write). The alarm-driven cleanup is
    // idempotent and self-healing, so reject until it clears rather than racing
    // it.
    if (existingMap.cleanup_pending === 1) {
      return json(
        {
          error: {
            code: "MIGRATION_CLEANUP_PENDING",
            message: `vbucket ${vbucket} has post-flip cleanup still pending from a prior migration (source rows delete / unfence not yet confirmed).`,
            fix: "Wait for the alarm-driven cleanup to finish (it retries automatically; watch cleanup via /admin/migrate-vbucket-status), then retry.",
          },
        },
        409,
      );
    }

    // Provenance gate: every row on the source shard must be attributable
    // to a (tenant, vbucket) before ANY vbucket can migrate off it —
    // /migrate-export selects rows via __cf_row_owners, so an unattributed
    // row would silently be left behind rather than fail loudly here.
    const tables = this.migratableTables();
    if (tables.length > 0) {
      const unattributedRes = await this.callShard(existingMap.shard_id, "/unattributed-count", { tables });
      if (!unattributedRes.ok) {
        return json({ error: `Failed to check provenance completeness on shard ${existingMap.shard_id}.` }, 502);
      }
      const unattributed = ((await unattributedRes.json()) as { count: number }).count;
      if (unattributed > 0) {
        return json(
          {
            error: {
              code: "VBUCKET_PROVENANCE_INCOMPLETE",
              message: `Source shard ${existingMap.shard_id} has ${unattributed} row(s) with no provenance entry — migration would leave them behind.`,
              unattributedRows: unattributed,
              fix: "Run /admin/backfill-provenance (and /admin/set-row-owner for any ambiguous rows), then retry.",
            },
          },
          409,
        );
      }
    }

    const config = this.one<{ catalog_shard_id: string | null }>(
      "SELECT catalog_shard_id FROM cluster_config WHERE singleton = 1",
    );
    const shardPrefix = config?.catalog_shard_id ? `${config.catalog_shard_id}-` : "";
    const targetShard = requestedTargetShardId ?? `${shardPrefix}shard-split-${Date.now()}`;
    if (targetShard === existingMap.shard_id) {
      return json({ error: `targetShardId must differ from the vbucket's current shard (${existingMap.shard_id}).` }, 400);
    }
    // Codex full-PR review P1 B: never migrate ONTO a non-active shard. If the
    // explicit target ALREADY exists it must be 'active' — a 'draining' (or
    // otherwise non-active) target would, once the map flips to it at cutover,
    // make /route (which joins shards and rejects a non-active mapping) return
    // 503 for this vbucket forever. INSERT OR IGNORE below would leave the
    // pre-existing draining row untouched and let the migration proceed. A
    // freshly-created target (no row yet) is created 'active' below and is
    // fine. This runs AFTER the awaited provenance scan above deliberately: a
    // concurrent drain could have flipped the target to 'draining' during that
    // await, and there are no further awaits before the claim/INSERT, so this
    // check is race-safe.
    const existingTarget = this.one<{ status: string }>("SELECT status FROM shards WHERE shard_id = ?", targetShard);
    if (existingTarget && existingTarget.status !== "active") {
      return json(
        {
          error: {
            code: "TARGET_SHARD_NOT_ACTIVE",
            message: `Target shard ${targetShard} is '${existingTarget.status}', not active — migrating vbucket ${vbucket} onto it would leave the vbucket permanently unroutable after cutover.`,
            fix: "Choose an active target shard, or wait for / cancel the target's own drain first.",
          },
        },
        409,
      );
    }

    // Codex review P2 (TOCTOU): the migration_status !== 'none' check above and
    // this state transition are separated by the awaited provenance check, and
    // DO handlers interleave at await points — so two concurrent
    // /admin/migrate-vbucket (or /split-vbucket) calls for the same vbucket can
    // BOTH pass the check. Claim the migration with a CONDITIONAL update (only
    // from 'none') and check changes(); if we lost the race, bail 409 BEFORE
    // creating the target shard, so the loser leaves no orphaned target state
    // and never overwrites the winner's target_shard_id.
    const now = new Date().toISOString();
    this.sql.exec(
      `
      UPDATE vbucket_map
      SET migration_status = 'backfilling', target_shard_id = ?, migration_rows_copied = 0, migration_started_at = ?,
          backfill_table = NULL, backfill_after_pk = NULL, updated_at = ?, topology_lock_operation_id = ?
      WHERE vbucket = ? AND migration_status = 'none' AND cleanup_pending = 0
      `,
      targetShard,
      now,
      now,
      operationId ?? null,
      vbucket,
    );
    const claimed = this.one<{ n: number }>("SELECT changes() AS n");
    if ((claimed?.n ?? 0) === 0) {
      return json(
        {
          error: {
            code: "MIGRATION_IN_PROGRESS",
            message: `vbucket ${vbucket} already has a migration in progress (a concurrent request won the race).`,
            fix: "Wait for it to finish (/admin/migrate-vbucket-status) or abort it (/admin/migrate-vbucket-abort).",
          },
        },
        409,
      );
    }

    this.audit(endpoint, { vbucket, fromShard: existingMap.shard_id, toShard: targetShard });
    // Only the winner creates the target shard and bumps the map version.
    this.sql.exec(
      "INSERT OR IGNORE INTO shards (shard_id, status, created_at) VALUES (?, 'active', ?)",
      targetShard,
      now,
    );
    // P1 (correctness): a brand-new target shard (INSERT actually inserted)
    // never got the create-table fan-out — mark it for full schema
    // provisioning on the first backfill tick; an existing target (INSERT
    // ignored) already has every table, so it's left 0.
    const targetIsFresh = (this.one<{ n: number }>("SELECT changes() AS n")?.n ?? 0) > 0;
    this.sql.exec("UPDATE vbucket_map SET provision_pending = ? WHERE vbucket = ?", targetIsFresh ? 1 : 0, vbucket);
    const version = this.bumpMetadataVersion();

    const soon = Date.now() + MIGRATION_TICK_MS;
    const existingAlarm = await this.ctx.storage.getAlarm();
    if (existingAlarm === null || existingAlarm > soon) {
      await this.ctx.storage.setAlarm(soon);
    }

    return { fromShard: existingMap.shard_id, toShard: targetShard, metadataVersion: version };
  }

  /** Milestone 3, Chunk 4: alarm-driven migration orchestration. Each tick
   * advances every in-flight migration one step; the alarm re-arms while any
   * remains active. */
  async alarm(): Promise<void> {
    this.ensureSchema();
    if (this.migrationTickInFlight) {
      // Another tick (scheduled alarm vs. a concurrent invocation) is
      // already advancing migrations — don't interleave with it on stale
      // row snapshots; just make sure a future tick happens.
      await this.ctx.storage.setAlarm(Date.now() + MIGRATION_TICK_MS);
      return;
    }
    this.migrationTickInFlight = true;
    let anyActive = false;
    let anyThrew = false;
    try {
      const migrating = this.many<MigrationRow>(
        // Review Tier 1 #4: the alarm's migration loop drives only active
        // migrations ('backfilling'/'cutover'); an 'aborting' row is finished
        // by a retried /admin/migrate-vbucket-abort, not here.
        "SELECT vbucket, shard_id, target_shard_id, migration_status, migration_rows_copied, topology_lock_operation_id FROM vbucket_map WHERE migration_status IN ('backfilling', 'cutover') ORDER BY vbucket ASC",
      );
      for (const m of migrating) {
        try {
          // Approved design (Stage 3): a migration mediated by the Worker
          // holds the topology lock from start through post-flip cleanup,
          // heartbeated every tick. If the lock is lost, STOP mutating this
          // tick — every destructive step (checksum-mismatch rewind, the map
          // flip, post-flip cleanup) lives inside advanceMigration, so skipping
          // the call protects all of them — and keep ticking so a later tick
          // notices if the lock situation resolves.
          const stillHeld = await this.heartbeatTopologyLockOrPark(m.topology_lock_operation_id);
          if (!stillHeld) {
            log("catalog.migration_topology_lock_lost", { vbucket: m.vbucket });
            anyActive = true;
            continue;
          }
          const stillActive = await this.advanceMigration(m);
          anyActive = anyActive || stillActive;
        } catch (error) {
          // Leave the migration in its current state and retry next tick —
          // every step is idempotent (INSERT OR REPLACE imports,
          // re-assertable fence, re-comparable checksums; the backfill cursor
          // resumes rather than restarts, review Tier 2 #8).
          log("catalog.migration_tick_failed", {
            vbucket: m.vbucket,
            status: m.migration_status,
            message: error instanceof Error ? error.message : String(error),
          });
          anyActive = true;
          anyThrew = true;
        }
      }

      // Codex full-PR review P2: retry post-flip source cleanup independently
      // of migration_status. After a flip the row is 'none' (not in the
      // migrating set above), so a failed step-5 delete/unfence would otherwise
      // never be retried, leaving the source stale-fenced or with undeleted
      // rows while status reports 'complete'. cleanup_pending drives the retry.
      const cleanups = this.many<{ vbucket: number; cleanup_source_shard_id: string | null; topology_lock_operation_id: string | null }>(
        "SELECT vbucket, cleanup_source_shard_id, topology_lock_operation_id FROM vbucket_map WHERE cleanup_pending = 1 ORDER BY vbucket ASC",
      );
      const cleanupTables = cleanups.length > 0 ? this.migratableTables() : [];
      for (const c of cleanups) {
        if (!c.cleanup_source_shard_id) {
          // Nothing to clean against — clear the flag so it can't loop forever.
          // The migration is effectively done — release its topology lock too
          // (unless an enclosing drain still owns it — see
          // releaseMigrationTopologyLock).
          await this.releaseMigrationTopologyLock(c.topology_lock_operation_id);
          this.sql.exec("UPDATE vbucket_map SET cleanup_pending = 0, topology_lock_operation_id = NULL WHERE vbucket = ?", c.vbucket);
          continue;
        }
        try {
          const stillPending = await this.runPostFlipCleanup(c.vbucket, c.cleanup_source_shard_id, cleanupTables, c.topology_lock_operation_id);
          anyActive = anyActive || stillPending;
        } catch (error) {
          log("catalog.cleanup_tick_failed", { vbucket: c.vbucket, message: error instanceof Error ? error.message : String(error) });
          anyActive = true;
          anyThrew = true;
        }
      }

      // Milestone 3, Chunk 5: drive shard drains (vbucket evacuation, then
      // ring evacuation) for every draining shard.
      const draining = this.many<{ shard_id: string }>("SELECT shard_id FROM shards WHERE status = 'draining' ORDER BY shard_id ASC");
      for (const d of draining) {
        try {
          const stillActive = await this.advanceDrain(d.shard_id);
          anyActive = anyActive || stillActive;
        } catch (error) {
          log("catalog.drain_tick_failed", {
            shardId: d.shard_id,
            message: error instanceof Error ? error.message : String(error),
          });
          anyActive = true;
          anyThrew = true;
        }
      }

      // Codex live-deployment finding: drive alarm-based index backfills.
      // Only ever non-empty on catalog-0 — see index_rules schema comment —
      // so this loop is a genuine no-op on every other catalog shard's own
      // alarm firings (an empty result set, not wasted work).
      const backfilling = this.many<IndexBackfillRow>(
        "SELECT index_name, table_name, columns_json, placement_ring_json, backfill_shard_ids_json, backfill_shard_idx, backfill_after_pk, backfill_rows_copied, topology_lock_operation_id FROM index_rules WHERE status = 'building' AND backfill_shard_ids_json != '[]' ORDER BY index_name ASC",
      );
      for (const b of backfilling) {
        try {
          // Same Stage 3 lock-loss handling as migrations above: stop
          // mutating this tick (every destructive step -- the ready-flip,
          // the lock release -- lives inside advanceIndexBackfill) and keep
          // ticking so a later tick notices if the lock situation resolves.
          const stillHeld = await this.heartbeatTopologyLockOrPark(b.topology_lock_operation_id);
          if (!stillHeld) {
            log("catalog.index_backfill_topology_lock_lost", { indexName: b.index_name });
            anyActive = true;
            continue;
          }
          const stillActive = await this.advanceIndexBackfill(b);
          anyActive = anyActive || stillActive;
        } catch (error) {
          if (error instanceof PermanentIndexBackfillError) {
            // Codex review P1 fix: this specific error can NEVER resolve by
            // simply retrying -- only an explicit operator action
            // (/admin/backfill-provenance) can fix it. Retrying forever
            // (the ordinary-error branch below) would hold this index's
            // topology lock indefinitely, wedging every OTHER topology
            // operation until someone happened to force-release it by
            // hand. Give up instead: mark 'failed' (still non-'ready', so
            // /v1/index-query keeps rejecting reads against it -- see
            // handleLookupIndex's `status !== "ready"` gate), release the
            // lock, and stop the alarm from matching this row again (the
            // 'backfilling' query below also requires status = 'building',
            // which this row no longer has, so leaving
            // backfill_shard_ids_json populated can't make the alarm loop
            // pick it back up). backfill_shard_idx/backfill_after_pk/
            // backfill_shard_ids_json are left as-is purely for operator
            // diagnostics (how far did the failed attempt get) -- Codex
            // round 5 finding: handleStartIndexBackfill's 'failed'-retry
            // branch does NOT resume this cursor (a same-shard-set vbucket
            // migration in the release-to-retry window can move rows onto
            // an already-scanned shard with no change to backfillShardIds
            // at all, which a "did the shard list change" check can't catch)
            // -- it always restarts the retried backfill from shard 0.
            log("catalog.index_backfill_permanently_failed", {
              indexName: b.index_name,
              message: error.message,
            });
            this.sql.exec(
              "UPDATE index_rules SET status = 'failed', topology_lock_operation_id = NULL WHERE index_name = ?",
              b.index_name,
            );
            await this.releaseTopologyLockRemote(b.topology_lock_operation_id);
            continue;
          }
          // Leave the cursor where it is and retry next tick -- every step
          // is idempotent (INSERT OR REPLACE index-entry writes, a
          // re-resolvable INDEX_RING_FENCED retry, a resumable scan
          // cursor), the same reasoning migration ticks already rely on.
          log("catalog.index_backfill_tick_failed", {
            indexName: b.index_name,
            message: error instanceof Error ? error.message : String(error),
          });
          anyActive = true;
          anyThrew = true;
        }
      }
    } finally {
      this.migrationTickInFlight = false;
    }
    if (anyActive) {
      // Review Tier 2 #8: exponential backoff on a throwing tick (e.g. a
      // shard transiently over its subrequest budget) instead of hammering
      // at the 250ms base cadence; reset the streak on a clean tick.
      if (anyThrew) {
        this.migrationTickFailureStreak += 1;
      } else {
        this.migrationTickFailureStreak = 0;
      }
      const delay =
        this.migrationTickFailureStreak === 0
          ? MIGRATION_TICK_MS
          : Math.min(MIGRATION_TICK_MAX_MS, MIGRATION_TICK_MS * 2 ** this.migrationTickFailureStreak);
      await this.ctx.storage.setAlarm(Date.now() + delay);
    } else {
      this.migrationTickFailureStreak = 0;
    }
  }

  /** One orchestration step for one migrating vbucket. Returns true while
   * the migration still needs future ticks.
   *
   * backfilling: run a full export/import pass (paged, per table), then
   * enter cutover — step 1's formal ordering: set migration_status='cutover'
   * and synchronously fence the source.
   *
   * cutover (steps 2-5): re-assert the fence (idempotent, heals a crash
   * between the status write and the fence write), wait for the source's
   * mirror queue to drain to zero for this vbucket, verify per-table content
   * checksums, then flip vbucket_map, unfence, and delete the source copy.
   * A checksum mismatch aborts back to 'backfilling' (fence lifted, target
   * wiped) per the spec's step-3 rule. */
  private async advanceMigration(m: MigrationRow): Promise<boolean> {
    if (!m.target_shard_id) {
      // Unreachable by construction (startMigration always sets both), but
      // fail safe: clear the inconsistent state rather than looping forever.
      this.sql.exec("UPDATE vbucket_map SET migration_status = 'none' WHERE vbucket = ?", m.vbucket);
      return false;
    }
    const source = m.shard_id;
    const target = m.target_shard_id;
    const tables = this.migratableTables();

    if (m.migration_status === "backfilling") {
      // Only tables that actually own rows of this vbucket on the source
      // need exporting — every other registered table has nothing to page.
      // Ordered by table_name (migratableTables orders) so the persisted
      // cursor's table position is stable across ticks.
      const vbTablesRes = await this.callShard(source, "/vbucket-tables", { vbucket: m.vbucket });
      if (!vbTablesRes.ok) throw new Error(`vbucket-tables failed on ${source}: ${vbTablesRes.status}`);
      const vbTables = new Set(((await vbTablesRes.json()) as { tables: string[] }).tables);
      const exportTables = tables.filter((t) => vbTables.has(t.table));

      // Review Tier 2 #8: resume from the persisted cursor and copy at most
      // MIGRATION_BACKFILL_PAGES_PER_TICK pages this tick, so a large vbucket
      // doesn't exceed the DO's per-invocation subrequest cap (which would
      // throw and restart from page zero every tick forever).
      const cursor = this.one<{ backfill_table: string | null; backfill_after_pk: string | null }>(
        "SELECT backfill_table, backfill_after_pk FROM vbucket_map WHERE vbucket = ?",
        m.vbucket,
      );
      let idx = cursor?.backfill_table ? exportTables.findIndex((t) => t.table === cursor.backfill_table) : 0;
      let afterPk = "";
      if (idx >= 0 && cursor?.backfill_table) {
        afterPk = cursor.backfill_after_pk ?? "";
      } else {
        idx = 0; // cursor's table no longer present (e.g. dropped) — restart
      }

      // P1 correctness (Codex review): when the target is a freshly created
      // split shard (provision_pending set by startMigration), provision
      // schema_sql for EVERY registered table with a captured schema — NOT just
      // the ones that have rows in this vbucket. A registered table with zero
      // rows here is absent from exportTables, so coupling provisioning to the
      // export loop would never create it on the fresh target, and the first
      // later write to it on the moved vbucket would fail `no such table` (a
      // mirror job for it would also stay queued forever). Gated on
      // provision_pending (not merely the initial tick) so a drain to an
      // EXISTING shard — which already has every table — issues zero provision
      // calls, keeping this O(1) rather than O(tables x vbuckets) subrequests.
      // The stable requestId makes it idempotent; only clear the flag once all
      // succeed so a thrown tick retries.
      const provisionPending =
        (this.one<{ provision_pending: number }>("SELECT provision_pending FROM vbucket_map WHERE vbucket = ?", m.vbucket)?.provision_pending ?? 0) === 1;
      if (provisionPending) {
        for (const t of tables) {
          if (!t.schemaSql) continue;
          const schemaRes = await this.callShard(target, "/execute", {
            // Idempotent DDL so re-execution against a table that already
            // physically exists is a no-op, not a 400. Its requestId is its OWN
            // namespace (migrate-provision-, NOT create-table-): reusing
            // /admin/create-table's requestId would collide in applied_requests
            // — that row is hashed over the UNMODIFIED schema, so this
            // IF-NOT-EXISTS-modified SQL would 409 "different sql" while the
            // (common, within-7-day-TTL) row is still present. Stable per
            // (table, target) so a resumed migration's retries still dedup.
            sql: ensureCreateTableIfNotExists(t.schemaSql),
            requestId: `migrate-provision-${t.table}-${target}`,
            isMutation: true,
          });
          if (!schemaRes.ok) throw new Error(`schema provisioning failed on ${target} for ${t.table}: ${schemaRes.status}`);
        }
        this.sql.exec(
          "UPDATE vbucket_map SET provision_pending = 0 WHERE vbucket = ? AND migration_status = 'backfilling' AND target_shard_id = ?",
          m.vbucket,
          target,
        );
      }

      let copied = 0;
      let pages = 0;
      while (idx < exportTables.length && pages < MIGRATION_BACKFILL_PAGES_PER_TICK) {
        const t = exportTables[idx];
        // Provision THIS table's schema on the target before its first page —
        // required for the import to succeed if the target lacks it (a fresh
        // split shard, or an existing shard missing a table). Bounded to
        // exportTables (tables that actually have rows in this vbucket), so a
        // drain to an existing shard stays O(tables-with-rows), not O(all
        // registered tables). Stable requestId dedupes across resumed ticks.
        // (Zero-row tables, absent from exportTables, are covered separately by
        // the provision_pending pass above — but only on a fresh target.)
        if (afterPk === "" && t.schemaSql) {
          const schemaRes = await this.callShard(target, "/execute", {
            // Idempotent DDL + its OWN requestId namespace — see the
            // provision_pending pass above. IF NOT EXISTS covers a table that
            // physically exists under a fresh (or TTL-pruned) dedup row; the
            // migrate-provision- prefix avoids colliding with
            // /admin/create-table's applied_requests row (hashed over the
            // UNMODIFIED schema), which would otherwise 409 within its TTL.
            sql: ensureCreateTableIfNotExists(t.schemaSql),
            requestId: `migrate-provision-${t.table}-${target}`,
            isMutation: true,
          });
          if (!schemaRes.ok) throw new Error(`schema provisioning failed on ${target} for ${t.table}: ${schemaRes.status}`);
        }
        const exportRes = await this.callShard(source, "/migrate-export", {
          vbucket: m.vbucket,
          table: t.table,
          partitionKeyColumn: t.partitionKeyColumn,
          afterPartitionKey: afterPk,
          limit: MIGRATE_PAGE_SIZE,
        });
        if (!exportRes.ok) throw new Error(`migrate-export failed on ${source} for ${t.table}: ${exportRes.status}`);
        const rows = ((await exportRes.json()) as {
          rows: Array<{ partitionKey: string; tenantId: string; row: Record<string, unknown> }>;
        }).rows;
        pages += 1;
        if (rows.length > 0) {
          const importRes = await this.callShard(target, "/migrate-import", { vbucket: m.vbucket, table: t.table, rows });
          if (!importRes.ok) throw new Error(`migrate-import failed on ${target} for ${t.table}: ${importRes.status}`);
          copied += rows.length;
          afterPk = rows[rows.length - 1].partitionKey;
        }
        if (rows.length < MIGRATE_PAGE_SIZE) {
          idx += 1; // this table is exhausted — advance to the next
          afterPk = "";
        }
      }

      // Persist progress. Conditional on the row still being THIS migration —
      // an abort landing during the awaits above must not have its cursor
      // resurrected.
      if (idx < exportTables.length) {
        // More to copy — save the cursor and come back next tick.
        this.sql.exec(
          "UPDATE vbucket_map SET migration_rows_copied = migration_rows_copied + ?, backfill_table = ?, backfill_after_pk = ?, updated_at = ? WHERE vbucket = ? AND migration_status = 'backfilling' AND target_shard_id = ?",
          copied,
          exportTables[idx].table,
          afterPk,
          new Date().toISOString(),
          m.vbucket,
          target,
        );
        return true;
      }
      // Fully backfilled — clear the cursor and proceed to cutover.
      this.sql.exec(
        "UPDATE vbucket_map SET migration_rows_copied = migration_rows_copied + ?, backfill_table = NULL, backfill_after_pk = NULL WHERE vbucket = ? AND migration_status = 'backfilling' AND target_shard_id = ?",
        copied,
        m.vbucket,
        target,
      );

      // Cutover step 1: status first (spec's stated order), then the fence,
      // synchronously in the same tick. A crash between the two writes is
      // healed by the cutover branch re-asserting the fence every tick.
      // Conditional on the row still being THIS migration — an
      // /admin/migrate-vbucket-abort that landed during the (awaited)
      // backfill pass above must not be resurrected into cutover.
      this.sql.exec(
        "UPDATE vbucket_map SET migration_status = 'cutover', cutover_started_at = ?, cutover_stall_reason = NULL, updated_at = ? WHERE vbucket = ? AND migration_status = 'backfilling' AND target_shard_id = ?",
        new Date().toISOString(),
        new Date().toISOString(),
        m.vbucket,
        target,
      );
      const advanced = this.one<{ n: number }>("SELECT changes() AS n");
      if ((advanced?.n ?? 0) === 0) {
        log("catalog.migration_advance_skipped_stale", { vbucket: m.vbucket, expected: "backfilling" });
        return false;
      }
      const fenceRes = await this.callShard(source, "/fence-vbucket", { vbucket: m.vbucket });
      if (!fenceRes.ok) throw new Error(`fence-vbucket failed on ${source}: ${fenceRes.status}`);
      // Attempt the cutover immediately in the same tick — for a quiet
      // vbucket (empty mirror queue, checksums already equal) the whole
      // migration completes in one pass; a busy one just returns true from
      // the cutover branch and polls again next tick. Chunk 5's sequential
      // shard drain leans on this so N vbuckets don't take 2N ticks.
      return this.advanceMigration({ ...m, migration_status: "cutover" });
    }

    if (m.migration_status === "cutover") {
      // Re-assert the fence (idempotent INSERT OR REPLACE) — guarantees it
      // exists even if the previous tick crashed after the status write.
      const fenceRes = await this.callShard(source, "/fence-vbucket", { vbucket: m.vbucket });
      if (!fenceRes.ok) throw new Error(`fence-vbucket failed on ${source}: ${fenceRes.status}`);

      // Step 2: the source's mirror queue for this vbucket must reach zero.
      // ACTIVELY drive the drain (review Tier 1 #2) rather than passively
      // wait on the source shard's alarm cadence — /drain-mirror-jobs
      // attempts every queued mirror now and reports how many remain
      // (unreachable targets stay, retried next tick). Now that mirrors are
      // enqueued atomically with the write, this count includes every
      // outstanding mirror, so once it's zero no slow mirror can still land
      // on the target after the flip.
      const mirrorRes = await this.callShard(source, "/drain-mirror-jobs", { vbucket: m.vbucket });
      if (!mirrorRes.ok) throw new Error(`drain-mirror-jobs failed on ${source}: ${mirrorRes.status}`);
      const mirrorDepth = ((await mirrorRes.json()) as { remaining: number }).remaining;
      if (mirrorDepth > 0) {
        return true; // poll again next tick
      }

      // Review Tier 1 #7: don't flip while the source has a prepared 2PC
      // intent touching this vbucket. A tx that prepared BEFORE the migration
      // started carries no mirror target, so a commit landing after the flip
      // would strand its write on the old source. The fence (set above)
      // blocks NEW prepares, so this count only decreases; once it's zero all
      // such txs have committed (applied to the source with provenance, so
      // the checksum below catches any source/target divergence and re-copies
      // it) or aborted. Wait for that rather than racing the flip.
      const preparedRes = await this.callShard(source, "/prepared-intent-count-for-vbucket", { vbucket: m.vbucket });
      if (!preparedRes.ok) throw new Error(`prepared-intent-count failed on ${source}: ${preparedRes.status}`);
      const preparedBody = (await preparedRes.json()) as { count: number; txIds?: string[] };
      const preparedCount = preparedBody.count;
      if (preparedCount > 0) {
        // Re-review: bound this wait. Once it exceeds CUTOVER_PREPARED_WAIT_MAX_MS,
        // mark the migration so /migrate-vbucket-status surfaces a distinct
        // 'cutover-blocked-on-prepared-intents' status (naming the txId), giving
        // the operator an escape (/admin/tx-force-abort) instead of a silent
        // livelock. Still poll rather than abort — a slow-but-live tx recovers.
        const startedRow = this.one<{ cutover_started_at: string | null }>(
          "SELECT cutover_started_at FROM vbucket_map WHERE vbucket = ?",
          m.vbucket,
        );
        // Adversarial re-review: cutover_started_at is a nullable column added
        // after cutover existed, so a migration ALREADY in 'cutover' at deploy
        // time (or one that reached cutover before this field was populated)
        // has NULL here. A NULL must NOT mean "never times out" — that would
        // reintroduce the exact livelock this bound closes. Stamp the clock
        // NOW (start it from this tick) so the bound engages on a later tick.
        let startedAt: number;
        if (startedRow?.cutover_started_at) {
          startedAt = new Date(startedRow.cutover_started_at).getTime();
        } else {
          startedAt = Date.now();
          this.sql.exec(
            "UPDATE vbucket_map SET cutover_started_at = ?, updated_at = ? WHERE vbucket = ? AND migration_status = 'cutover' AND cutover_started_at IS NULL",
            new Date(startedAt).toISOString(),
            new Date().toISOString(),
            m.vbucket,
          );
        }
        if (Date.now() - startedAt > CUTOVER_PREPARED_WAIT_MAX_MS) {
          log("catalog.migration_cutover_blocked_on_prepared_intents", {
            vbucket: m.vbucket,
            preparedCount,
            txIds: preparedBody.txIds,
            waitedMs: Date.now() - startedAt,
          });
          this.sql.exec(
            "UPDATE vbucket_map SET cutover_stall_reason = 'prepared-intents', updated_at = ? WHERE vbucket = ? AND migration_status = 'cutover'",
            new Date().toISOString(),
            m.vbucket,
          );
        }
        return true; // poll again next tick
      }
      // Prepared intents drained — clear any stall marker set above before
      // proceeding to the checksum/flip.
      this.sql.exec(
        "UPDATE vbucket_map SET cutover_stall_reason = NULL WHERE vbucket = ? AND cutover_stall_reason IS NOT NULL",
        m.vbucket,
      );

      // Step 3: per-table content checksums must match for EVERY registered
      // table (the spec's verify rule) — computed in one batched round trip
      // per shard rather than one per table.
      const [srcRes, tgtRes] = await Promise.all([
        this.callShard(source, "/migrate-checksums", { vbucket: m.vbucket, tables }),
        this.callShard(target, "/migrate-checksums", { vbucket: m.vbucket, tables }),
      ]);
      if (!srcRes.ok || !tgtRes.ok) throw new Error("migrate-checksums failed");
      const srcSums = ((await srcRes.json()) as { checksums: Record<string, { checksum: string }> }).checksums;
      const tgtSums = ((await tgtRes.json()) as { checksums: Record<string, { checksum: string }> }).checksums;
      // The checksum round-trips above are await points — re-read the row
      // and bail if the migration was aborted (or otherwise changed) while
      // they were in flight, BEFORE acting on the comparison. Acting on a
      // stale view here is the dangerous case: a wipe against a migration
      // that no longer exists, or a flip for one that was aborted.
      const fresh = this.one<{ migration_status: string; shard_id: string; target_shard_id: string | null }>(
        "SELECT migration_status, shard_id, target_shard_id FROM vbucket_map WHERE vbucket = ?",
        m.vbucket,
      );
      if (!fresh || fresh.migration_status !== "cutover" || fresh.shard_id !== source || fresh.target_shard_id !== target) {
        log("catalog.migration_cutover_skipped_stale", { vbucket: m.vbucket });
        return fresh !== null && fresh.migration_status !== "none";
      }

      const mismatched = tables.find((t) => srcSums[t.table]?.checksum !== tgtSums[t.table]?.checksum);
      if (mismatched) {
        log("catalog.migration_checksum_mismatch", { vbucket: m.vbucket, table: mismatched.table, source, target });
        // Abort this cutover attempt: fence lifted, target wiped, status
        // back to backfilling (a later tick re-copies and retries). Purge
        // the source's queued mirrors for this vbucket too — their content
        // is re-derived by the next backfill pass, and left queued they'd
        // fire against the just-wiped target out of order.
        //
        // Codex full-PR review P2: check ALL THREE cleanup calls and do NOT
        // rewind to 'backfilling' unless every one succeeded — return true so
        // this stays in 'cutover' and the next tick retries the (idempotent)
        // cleanup. A transient /unfence-vbucket failure that still rewound
        // would leave the source FENCED while status said 'backfilling', so the
        // next backfill/cutover ran against a fenced source (writes 409); a
        // failed purge/wipe would leave stale mirror jobs / target rows to
        // corrupt the retry.
        const unfenceRes = await this.callShard(source, "/unfence-vbucket", { vbucket: m.vbucket });
        if (!unfenceRes.ok) {
          log("catalog.mismatch_cleanup_unfence_failed", { vbucket: m.vbucket, source, status: unfenceRes.status });
          return true; // stay 'cutover', retry cleanup next tick — no rewind while fenced
        }
        const purgeRes = await this.callShard(source, "/purge-mirror-jobs", { vbucket: m.vbucket });
        if (!purgeRes.ok) {
          log("catalog.mismatch_cleanup_purge_failed", { vbucket: m.vbucket, source, status: purgeRes.status });
          return true;
        }
        const wipeRes = await this.callShard(target, "/delete-vbucket-rows", { vbucket: m.vbucket, tables });
        if (!wipeRes.ok) {
          log("catalog.mismatch_cleanup_wipe_failed", { vbucket: m.vbucket, target, status: wipeRes.status });
          return true;
        }
        this.sql.exec(
          // Re-review item D: reset migration_rows_copied to 0 alongside the
          // rewind. The target's copy was just wiped, so the next backfill
          // pass re-copies from scratch; without the reset /migrate-vbucket-status
          // rowsCopied inflated by a full vbucket's worth on every retry.
          "UPDATE vbucket_map SET migration_status = 'backfilling', migration_rows_copied = 0, cutover_started_at = NULL, cutover_stall_reason = NULL, updated_at = ? WHERE vbucket = ? AND migration_status = 'cutover' AND target_shard_id = ?",
          new Date().toISOString(),
          m.vbucket,
          target,
        );
        return true;
      }

      // Codex final-review P1 #2: re-verify the lock is STILL held by this
      // operation immediately before the destructive map-flip below — the
      // alarm heartbeats the lease once per tick, BEFORE advanceMigration is
      // even called, but everything from the fence check through the
      // checksum verify above is further awaited round trips where the
      // lease could have expired or been force-released. Acting on a stale
      // confirmation here is exactly the dangerous case: another operation
      // (e.g. a drain) could have acquired the freed lock and already be
      // acting on `target`, so flipping onto it now could produce a
      // permanently unroutable vbucket. Do NOT flip this tick — park (no
      // mutation) and let a later tick, either recovering the SAME lock or
      // running under a freshly re-acquired one, retry from 'cutover'.
      const stillHoldsLock = await this.holdsTopologyLockNow(m.topology_lock_operation_id);
      if (!stillHoldsLock) {
        log("catalog.migration_flip_aborted_lock_lost", { vbucket: m.vbucket });
        return true; // stay 'cutover', retry next tick — no mutation this tick
      }

      // Step 4: flip the map. From this write on, /route sends everything to
      // the target; the fence still blocks any straggler write that resolved
      // its route pre-flip and arrives at the source. Conditional, and step
      // 5's destructive source delete only runs if THIS tick actually
      // performed the flip.
      // Step 4: flip the map AND arm the retryable post-flip cleanup atomically.
      // Codex full-PR review P2: the map flip and cleanup_pending flag are set
      // in ONE UPDATE so there's no window where the flip committed but nothing
      // records that the source still needs cleaning — if step 5 below then
      // fails, the alarm's cleanup loop (keyed on cleanup_pending) retries it
      // independently of migration_status (which is now 'none').
      const version = this.bumpMetadataVersion();
      this.sql.exec(
        `
        UPDATE vbucket_map
        SET shard_id = ?, migration_status = 'none', target_shard_id = NULL, map_version = ?,
            cutover_started_at = NULL, cutover_stall_reason = NULL,
            cleanup_pending = 1, cleanup_source_shard_id = ?, updated_at = ?
        WHERE vbucket = ? AND migration_status = 'cutover' AND shard_id = ? AND target_shard_id = ?
        `,
        target,
        version,
        source,
        new Date().toISOString(),
        m.vbucket,
        source,
        target,
      );
      const flipped = this.one<{ n: number }>("SELECT changes() AS n");
      if ((flipped?.n ?? 0) === 0) {
        log("catalog.migration_flip_skipped_stale", { vbucket: m.vbucket });
        return false;
      }

      this.audit("/migrate-vbucket-complete", { vbucket: m.vbucket, fromShard: source, toShard: target, metadataVersion: version });
      // Step 5: attempt the (retryable) source cleanup now; if a call fails,
      // cleanup_pending stays 1 and the alarm retries it next tick.
      return this.runPostFlipCleanup(m.vbucket, source, tables, m.topology_lock_operation_id);
    }

    return false;
  }

  /** Post-flip source cleanup, made RETRYABLE via cleanup_pending (set
   * atomically in the flip UPDATE). The map is already flipped — reads/writes
   * route to the target — so this is pure cleanup that can retry indefinitely
   * without affecting routing. Deletes the migrated vbucket's rows + provenance
   * from the old source and unfences it; BOTH are idempotent (re-runnable).
   * Ordering (Codex P1 silent-loss fix): delete WHILE the source is still
   * fenced, then unfence LAST, so a straggler that resolved the old route can
   * only 409 during the window rather than being accepted-then-dropped. Clears
   * cleanup_pending only once BOTH succeed; on any failure leaves it set and
   * returns true so the caller/alarm keeps ticking. */
  private async runPostFlipCleanup(
    vbucket: number,
    sourceShardId: string,
    tables: Array<{ table: string; partitionKeyColumn: string; schemaSql: string | null }>,
    topologyLockOperationId?: string | null,
  ): Promise<boolean> {
    const deleteRes = await this.callShard(sourceShardId, "/delete-vbucket-rows", { vbucket, tables });
    if (!deleteRes.ok) {
      log("catalog.post_flip_cleanup_delete_failed", { vbucket, sourceShardId, status: deleteRes.status });
      return true; // retry next tick — cleanup_pending stays set
    }
    const unfenceRes = await this.callShard(sourceShardId, "/unfence-vbucket", { vbucket });
    if (!unfenceRes.ok) {
      log("catalog.post_flip_cleanup_unfence_failed", { vbucket, sourceShardId, status: unfenceRes.status });
      return true; // retry — source stays fenced (delete already done) until unfence succeeds
    }
    // Approved design (Stage 3): cleanup completing is the migration's TRUE
    // end — release the topology lock it held since /admin/migrate-vbucket (or
    // /admin/split-vbucket) started it. If this migration is a drain's
    // sub-migration (inherited the drain's own operationId), the enclosing
    // drain still owns the lock's lifecycle — releaseMigrationTopologyLock
    // detects that and skips the release.
    await this.releaseMigrationTopologyLock(topologyLockOperationId);
    this.sql.exec(
      "UPDATE vbucket_map SET cleanup_pending = 0, cleanup_source_shard_id = NULL, topology_lock_operation_id = NULL, updated_at = ? WHERE vbucket = ? AND cleanup_pending = 1",
      new Date().toISOString(),
      vbucket,
    );
    return false;
  }

  /** Milestone 3, Chunk 4 (POST /migrate-vbucket-status {vbucket}). */
  private async handleMigrateVbucketStatus(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number };
    if (body.vbucket === undefined) {
      return json({ error: "Missing vbucket" }, 400);
    }
    const row = this.one<MigrationRow & { migration_started_at: string | null; cutover_stall_reason: string | null }>(
      "SELECT vbucket, shard_id, target_shard_id, migration_status, migration_rows_copied, migration_started_at, cutover_stall_reason FROM vbucket_map WHERE vbucket = ?",
      body.vbucket,
    );
    if (!row) {
      return json({ error: `vbucket ${body.vbucket} has no mapping` }, 404);
    }
    let mirrorQueueDepth = 0;
    if (row.migration_status !== "none") {
      const mirrorRes = await this.callShard(row.shard_id, "/mirror-pending-count", { vbucket: body.vbucket });
      if (mirrorRes.ok) {
        mirrorQueueDepth = ((await mirrorRes.json()) as { count: number }).count;
      }
    }

    // Re-review: surface a bounded cutover stalled on prepared 2PC intents as a
    // distinct status naming the offending txId(s), so an operator can
    // /admin/tx-force-abort the wedged transaction instead of watching the
    // migration livelock in 'cutover' forever.
    let status = row.migration_status;
    let blockedTxIds: string[] | undefined;
    if (row.cutover_stall_reason === "prepared-intents" && row.migration_status === "cutover") {
      status = "cutover-blocked-on-prepared-intents";
      const preparedRes = await this.callShard(row.shard_id, "/prepared-intent-count-for-vbucket", { vbucket: body.vbucket });
      if (preparedRes.ok) {
        blockedTxIds = ((await preparedRes.json()) as { txIds?: string[] }).txIds;
      }
    }

    return json({
      vbucket: row.vbucket,
      status,
      fromShard: row.shard_id,
      toShard: row.target_shard_id,
      rowsCopied: row.migration_rows_copied,
      mirrorQueueDepth,
      startedAt: row.migration_started_at,
      ...(blockedTxIds ? { blockedTxIds } : {}),
    });
  }

  /** Milestone 3, Chunk 4 (POST /migrate-vbucket-abort {vbucket}). Safe at
   * any point before the map flip — the source never stopped being
   * authoritative, so aborting is purely: wipe the target's copy of this
   * vbucket (rows + provenance), lift the fence, clear the migration state.
   * After the flip there is nothing left to abort (the source copy is
   * deleted); rolling back is a fresh migration in the other direction.
   *
   * Review Tier 1 #4: cleanup transitions the row to an intermediate
   * 'aborting' status (target_shard_id retained) BEFORE unfence/purge/wipe,
   * and only clears to 'none' after all three succeed. A crash or failure
   * mid-cleanup leaves the row 'aborting', not 'none' — so a retried abort
   * RESUMES the (idempotent) cleanup and lifts the fence, instead of
   * returning 409 MIGRATION_ALREADY_COMMITTED and stranding the source
   * fenced forever. */
  private async handleMigrateVbucketAbort(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number };
    if (body.vbucket === undefined) {
      return json({ error: "Missing vbucket" }, 400);
    }

    // Take the same latch the orchestration ticks use: an in-flight tick
    // interleaves with this handler at await points, and a tick's cutover
    // branch re-asserts the fence — racing that with the unfence below
    // could leave a permanent fence on an aborted migration. Waiting the
    // tick out (they're short) makes abort-vs-tick strictly sequential.
    while (this.migrationTickInFlight) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    this.migrationTickInFlight = true;
    try {
      const row = this.one<MigrationRow>(
        "SELECT vbucket, shard_id, target_shard_id, migration_status, migration_rows_copied, topology_lock_operation_id FROM vbucket_map WHERE vbucket = ?",
        body.vbucket,
      );
      if (!row) {
        return json({ error: `vbucket ${body.vbucket} has no mapping` }, 404);
      }
      // 'none' means the migration already committed (map flipped) or never
      // started; there's nothing to abort. An 'aborting' row is a previously
      // interrupted abort — resume its cleanup below rather than reject.
      if (row.migration_status === "none" || !row.target_shard_id) {
        return json(
          {
            error: {
              code: "MIGRATION_ALREADY_COMMITTED",
              message: `vbucket ${body.vbucket} has no active migration — it either already committed (map flipped) or was never started.`,
              fix: "A committed migration is reversed by migrating the vbucket back with /admin/migrate-vbucket.",
            },
          },
          409,
        );
      }

      this.audit("/migrate-vbucket-abort", { vbucket: body.vbucket, fromShard: row.shard_id, toShard: row.target_shard_id });

      const tables = this.migratableTables();
      // Move to the intermediate 'aborting' state (keeping target_shard_id) —
      // survives a crash so a retry knows the target to finish wiping. The
      // alarm's migration loop ignores 'aborting' the same as 'none'.
      if (row.migration_status !== "aborting") {
        this.sql.exec(
          "UPDATE vbucket_map SET migration_status = 'aborting', updated_at = ? WHERE vbucket = ?",
          new Date().toISOString(),
          body.vbucket,
        );
      }
      // Cleanup, all idempotent so a resumed abort re-runs it safely:
      // unfence the source, purge its queued-but-unsent mirrors (a stale
      // mirror firing after the wipe would recreate unattributed junk on the
      // target), then wipe the target's copy. Every step's result is checked —
      // Codex review P2: a swallowed /unfence-vbucket failure that then cleared
      // to 'none' would strand the source permanently VBUCKET_FENCED with no
      // 'aborting' state left to resume from. On ANY failure, leave the row
      // 'aborting' and return 502 so a retried abort re-runs the (idempotent)
      // remaining cleanup.
      const unfenceRes = await this.callShard(row.shard_id, "/unfence-vbucket", { vbucket: body.vbucket });
      if (!unfenceRes.ok) {
        return json({ error: `Failed to unfence source shard ${row.shard_id} — abort not completed, retry.` }, 502);
      }
      const purgeRes = await this.callShard(row.shard_id, "/purge-mirror-jobs", { vbucket: body.vbucket });
      if (!purgeRes.ok) {
        return json({ error: `Failed to purge mirror jobs on source shard ${row.shard_id} — abort not completed, retry.` }, 502);
      }
      const wipeRes = await this.callShard(row.target_shard_id, "/delete-vbucket-rows", { vbucket: body.vbucket, tables });
      if (!wipeRes.ok) {
        // Leave the row 'aborting' — a retried abort resumes and completes the wipe.
        return json({ error: `Failed to wipe target shard ${row.target_shard_id} — abort not completed, retry.` }, 502);
      }

      // Cleanup fully succeeded — only now clear to 'none'. Approved design
      // (Stage 3): an abort is also a migration's TRUE end — release the
      // topology lock it held since start (unless an enclosing drain still
      // owns it, e.g. an operator aborting one of a drain's in-flight
      // sub-migrations directly).
      await this.releaseMigrationTopologyLock(row.topology_lock_operation_id);
      this.sql.exec(
        "UPDATE vbucket_map SET migration_status = 'none', target_shard_id = NULL, migration_rows_copied = 0, migration_started_at = NULL, backfill_table = NULL, backfill_after_pk = NULL, cutover_started_at = NULL, cutover_stall_reason = NULL, topology_lock_operation_id = NULL, updated_at = ? WHERE vbucket = ?",
        new Date().toISOString(),
        body.vbucket,
      );

      return json({ ok: true, vbucket: body.vbucket, status: "aborted" });
    } finally {
      this.migrationTickInFlight = false;
    }
  }
}
