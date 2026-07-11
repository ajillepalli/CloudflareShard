import { DurableObject } from "cloudflare:workers";
import { json } from "./http";
import { hashKey, pickRingSubstitute } from "./hash";
import { checkAdminAuth, sha256Hex, timingSafeEqual } from "./auth";
import { log } from "./log";
import { MIGRATE_PAGE_SIZE } from "./shard";
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
  "/mark-index-ready",
  "/list-tenants",
  "/vbucket-map",
  "/migrate-vbucket",
  "/migrate-vbucket-status",
  "/migrate-vbucket-abort",
  "/drain-shard-status",
]);

// Milestone 3, Chunk 4: cadence of the alarm-driven migration orchestration
// loop — each tick advances every in-flight migration one step (a bounded
// backfill slice, or one cutover attempt that may be waiting on the mirror
// queue to drain).
const MIGRATION_TICK_MS = 250;
// Review Tier 2 #8: cap the backfill work per tick and back off the alarm
// re-arm when a tick throws, so a large migration resumes from its cursor
// rather than restarting-and-throwing at 4Hz forever.
const MIGRATION_BACKFILL_PAGES_PER_TICK = 8;
const MIGRATION_TICK_MAX_MS = 30000;

type MigrationRow = {
  vbucket: number;
  shard_id: string;
  target_shard_id: string | null;
  migration_status: string;
  migration_rows_copied: number;
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
      "/drop-index": this.handleDropIndex.bind(this),
      "/mark-index-ready": this.handleMarkIndexReady.bind(this),
      "/list-tenants": this.handleListTenants.bind(this),
      "/vbucket-map": this.handleVbucketMap.bind(this),
      "/migrate-vbucket": this.handleMigrateVbucket.bind(this),
      "/migrate-vbucket-status": this.handleMigrateVbucketStatus.bind(this),
      "/migrate-vbucket-abort": this.handleMigrateVbucketAbort.bind(this),
      "/drain-shard-status": this.handleDrainShardStatus.bind(this),
      "/update-index-ring": this.handleUpdateIndexRing.bind(this),
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
    this.ensureColumn("table_rules", "schema_sql", "TEXT");

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
      schemaSql?: string;
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

    this.sql.exec(
      `
      INSERT OR REPLACE INTO table_rules (table_name, partitioning, partition_key_column, created_at, schema_sql)
      VALUES (?, ?, ?, ?, ?)
      `,
      body.table,
      body.partitioning ?? "hash",
      body.partitionKeyColumn,
      new Date().toISOString(),
      body.schemaSql ?? null,
    );

    const version = this.bumpMetadataVersion();
    return json({ ok: true, table: body.table, metadataVersion: version });
  }

  private async handleSetPartitionKeyColumn(request: Request): Promise<Response> {
    const body = (await request.json()) as { table?: string; partitionKeyColumn?: string };
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

    const existing = this.one<{ table_name: string }>(
      "SELECT table_name FROM table_rules WHERE table_name = ?",
      body.table,
    );
    if (!existing) {
      return json(
        { error: { code: "TABLE_NOT_REGISTERED", message: `Table ${body.table} is not registered.`, fix: "Call /register-table first." } },
        404,
      );
    }

    this.audit("/set-partition-key-column", { table: body.table, partitionKeyColumn: body.partitionKeyColumn });
    this.sql.exec(
      "UPDATE table_rules SET partition_key_column = ? WHERE table_name = ?",
      body.partitionKeyColumn,
      body.table,
    );

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

  /** Eng-review fix: flips an index from 'building' to 'ready' once the
   * Worker's backfill loop has fully completed. Called as the last step of
   * /admin/create-index, after every shard has been scanned and every row's
   * __cf_indexes entry written — before this, /lookup-index (and therefore
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

    const index = this.one<{ columns_json: string; partition_key_column: string; status: string; placement_ring_json: string }>(
      `
      SELECT ir.columns_json AS columns_json, tr.partition_key_column AS partition_key_column, ir.status AS status, ir.placement_ring_json AS placement_ring_json
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
    });
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

    const row = this.one<{ token_hash: string; revoked_at: string | null }>(
      "SELECT token_hash, revoked_at FROM tenant_auth WHERE tenant_id = ?",
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
    if (!timingSafeEqual(tokenHash, row.token_hash)) {
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

    const existing = this.one<{ tenant_id: string }>(
      "SELECT tenant_id FROM tenant_auth WHERE tenant_id = ?",
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

    // Log only {tenantId, rotate} — never the token or its hash, unlike other
    // audit() call sites in this file that log their full parsed body by
    // convention. This one must not, since audit_log is durably persisted
    // and readable via /admin/audit-log.
    this.audit("/register-tenant", { tenantId: body.tenantId, rotate: body.rotate === true });

    this.sql.exec(
      `
      INSERT OR REPLACE INTO tenant_auth (tenant_id, token_hash, created_at, revoked_at)
      VALUES (?, ?, ?, NULL)
      `,
      body.tenantId,
      tokenHash,
      new Date().toISOString(),
    );

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

  private async handleListShards(): Promise<Response> {
    const shards = this.many<{ shard_id: string }>(
      "SELECT shard_id FROM shards WHERE status = 'active' ORDER BY shard_id ASC",
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
    const rows = this.many<{ vbucket: number; shard_id: string }>("SELECT vbucket, shard_id FROM vbucket_map ORDER BY vbucket ASC");
    return json({ totalVBuckets: config.total_vbuckets, map: rows.map((r) => ({ vbucket: r.vbucket, shardId: r.shard_id })) });
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
    const body = (await request.json()) as { shardId: string };
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
    const ringsRemaining = this.many<{ placement_ring_json: string }>("SELECT placement_ring_json FROM index_rules").filter((r) =>
      (JSON.parse(r.placement_ring_json) as string[]).includes(body.shardId!),
    ).length;
    const status =
      shard.status !== "draining"
        ? shard.status
        : // Review Tier 2 #10: a drain parked on the provenance gate reports a
          // distinct status so the operator knows to run
          // /admin/backfill-provenance and re-invoke /admin/drain-shard.
          shard.drain_stall_reason === "provenance"
          ? "stalled-provenance"
          : vbucketsRemaining > 0
            ? "migrating-vbuckets"
            : ringsRemaining > 0
              ? "evacuating-rings"
              : "complete";
    return json({ shardId: body.shardId, vbucketsRemaining, ringsRemaining, status, stallReason: shard.drain_stall_reason });
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

  /** Milestone 3, Chunk 5 (internal, catalog-to-catalog): repoints one
   * index's pinned placement ring — the draining catalog fans a completed
   * ring substitution to every sibling catalog shard, since index_rules is
   * replicated identically to all of them. DO-binding-only route, no admin
   * gate: it's never exposed through the Worker, the same trust model every
   * ShardDO internal route already uses. */
  private async handleUpdateIndexRing(request: Request): Promise<Response> {
    const body = (await request.json()) as { indexName?: string; ring?: string[] };
    if (!body.indexName || !body.ring) {
      return json({ error: "Missing indexName or ring" }, 400);
    }
    this.sql.exec("UPDATE index_rules SET placement_ring_json = ? WHERE index_name = ?", JSON.stringify(body.ring), body.indexName);
    return json({ ok: true, indexName: body.indexName });
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
    const stall = this.one<{ drain_stall_reason: string | null }>("SELECT drain_stall_reason FROM shards WHERE shard_id = ?", shardId);
    if (stall?.drain_stall_reason) {
      return false;
    }

    const vbuckets = this.many<{ vbucket: number; migration_status: string }>(
      "SELECT vbucket, migration_status FROM vbucket_map WHERE shard_id = ? ORDER BY vbucket ASC",
      shardId,
    );
    if (vbuckets.length > 0) {
      if (vbuckets.some((v) => v.migration_status !== "none")) {
        return true; // sequential: the in-flight migration advances via the migration loop
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
      const started = await this.startMigration(next, target, "/drain-shard-migrate");
      if (started instanceof Response) {
        // Provenance-incomplete (or any start rejection): park the drain so
        // the alarm stops re-running the full-table provenance scan every
        // tick (review Tier 2 #10). The operator runs /admin/backfill-provenance
        // then re-calls /admin/drain-shard to clear the stall and resume.
        this.sql.exec("UPDATE shards SET drain_stall_reason = 'provenance' WHERE shard_id = ?", shardId);
        log("catalog.drain_stalled_provenance", { shardId, vbucket: next });
        return false;
      }
      // Advance it immediately — a quiet vbucket completes this same tick.
      const row = this.one<MigrationRow>(
        "SELECT vbucket, shard_id, target_shard_id, migration_status, migration_rows_copied FROM vbucket_map WHERE vbucket = ?",
        next,
      );
      if (row && row.migration_status !== "none") {
        await this.advanceMigration(row);
      }
      return true;
    }

    // Phase 2: ring evacuation.
    const indexRules = this.many<{ index_name: string; placement_ring_json: string }>(
      "SELECT index_name, placement_ring_json FROM index_rules ORDER BY index_name ASC",
    );
    let remaining = false;
    const ringsToEvacuate = indexRules.filter((r) => (JSON.parse(r.placement_ring_json) as string[]).includes(shardId));
    const clusterActive = ringsToEvacuate.length > 0 ? await this.clusterActiveShards(shardId) : [];
    for (const rule of ringsToEvacuate) {
      const ring = JSON.parse(rule.placement_ring_json) as string[];
      const pos = ring.indexOf(shardId);
      if (pos === -1) continue;

      const substitute = pickRingSubstitute(rule.index_name, clusterActive.filter((s) => !ring.includes(s)));
      if (substitute === null) {
        // The pre-check in handleDrainShard normally prevents this; the
        // active set may have shrunk since. Leave the ring untouched (reads
        // keep working against the draining shard) and report via status —
        // returning false, not true, so the alarm doesn't spin on a
        // condition only operator action (adding capacity) can change.
        log("catalog.ring_evacuation_no_candidate", { shardId, indexName: rule.index_name });
        continue;
      }

      // Review Tier 1 #6: an index write racing this evacuation must not be
      // silently lost. Ordering: (1) copy the draining shard's current
      // entries to the substitute (so a reader keeps seeing every entry —
      // the draining shard still has them, no read gap yet); (2) repoint the
      // ring, so from here every NEW /route resolves this index to the
      // substitute and no new write targets the draining shard; (3)
      // RECONCILE — re-copy any entry that landed on the draining shard
      // during the awaits above (a write whose /route resolved with the old
      // ring), looping by ascending rowid until a pass finds nothing new;
      // (4) only then delete the source copies. Because racing writes stop
      // arriving after the repoint, the reconcile converges.
      let afterRowid = await this.copyIndexEntries(shardId, substitute, rule.index_name, 0);

      // Substitute at the SAME ring position — every other entry's
      // placement is untouched.
      const newRing = [...ring];
      newRing[pos] = substitute;

      // Repoint the ring on this catalog and every sibling (index_rules is
      // replicated identically to all catalog shards). Self is updated
      // locally — never via a DO self-fetch.
      this.sql.exec("UPDATE index_rules SET placement_ring_json = ? WHERE index_name = ?", JSON.stringify(newRing), rule.index_name);
      const config = this.one<{ catalog_shard_count: number | null; catalog_shard_id: string | null }>(
        "SELECT catalog_shard_count, catalog_shard_id FROM cluster_config WHERE singleton = 1",
      );
      const siblingCount = config?.catalog_shard_count ?? 0;
      for (let i = 0; i < siblingCount; i += 1) {
        const siblingId = `catalog-${i}`;
        if (config?.catalog_shard_id === siblingId) continue; // already updated locally
        const stub = this.catalogEnv.CATALOG.get(this.catalogEnv.CATALOG.idFromName(siblingId));
        const res = await stub.fetch("https://catalog.internal/update-index-ring", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ indexName: rule.index_name, ring: newRing }),
        });
        if (!res.ok) throw new Error(`update-index-ring failed on ${siblingId}: ${res.status}`);
      }

      // Reconcile pass: catch any entry that raced onto the draining shard
      // between the initial copy's cursor and the repoint. Converges because
      // no new write targets the draining shard after the repoint.
      for (let pass = 0; pass < RING_EVAC_RECONCILE_MAX_PASSES; pass += 1) {
        const before = afterRowid;
        afterRowid = await this.copyIndexEntries(shardId, substitute, rule.index_name, afterRowid);
        if (afterRowid === before) break; // stable — nothing new copied
        if (pass === RING_EVAC_RECONCILE_MAX_PASSES - 1) {
          // Pathological churn — leave the source rows in place (unreachable
          // but not lost) rather than deleting entries the substitute may not
          // have yet. Documented residual (TODOS.md).
          log("catalog.ring_evacuation_reconcile_unstable", { shardId, indexName: rule.index_name });
          this.audit("/drain-shard-ring-evacuated", { shardId, indexName: rule.index_name, substitute, position: pos, reconcileUnstable: true });
          continue;
        }
      }

      // Now delete the source copies — everything present has been copied to
      // the substitute, and no new write can arrive (ring repointed).
      const deleteRes = await this.callShard(shardId, "/execute", {
        sql: "DELETE FROM __cf_indexes WHERE index_name = ?",
        params: [rule.index_name],
        requestId: `ring-evacuate-${rule.index_name}-${shardId}-${crypto.randomUUID()}`,
        isMutation: true,
      });
      if (!deleteRes.ok) {
        // Ring already repointed — stale rows on the draining shard are
        // unreachable garbage, not a correctness problem. Log and move on.
        log("catalog.ring_evacuation_source_cleanup_failed", { shardId, indexName: rule.index_name });
      }

      this.audit("/drain-shard-ring-evacuated", { shardId, indexName: rule.index_name, substitute, position: pos });
    }
    return remaining;
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
    };

    if (!Number.isInteger(body.vbucket) || body.vbucket < 0) {
      return json({ error: "vbucket must be a non-negative integer" }, 400);
    }

    // Milestone 3, Chunk 2: splitting no longer blocks on index presence
    // (the previous 409 SPLIT_BLOCKED_BY_INDEXES is removed) — index
    // placement hashes over each index's own pinned placement_ring_json,
    // so adding a new active shard never changes existing index placement.
    const started = await this.startMigration(body.vbucket, body.newShardId, "/split-vbucket");
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
    const body = (await request.json()) as { vbucket?: number; targetShardId?: string };
    if (body.vbucket === undefined || !Number.isInteger(body.vbucket) || body.vbucket < 0) {
      return json({ error: "vbucket must be a non-negative integer" }, 400);
    }
    const started = await this.startMigration(body.vbucket, body.targetShardId, "/migrate-vbucket");
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
  ): Promise<Response | { fromShard: string; toShard: string; metadataVersion: number }> {
    const existingMap = this.one<{ shard_id: string; migration_status: string }>(
      "SELECT shard_id, migration_status FROM vbucket_map WHERE vbucket = ?",
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

    this.audit(endpoint, { vbucket, fromShard: existingMap.shard_id, toShard: targetShard });

    this.sql.exec(
      "INSERT OR IGNORE INTO shards (shard_id, status, created_at) VALUES (?, 'active', ?)",
      targetShard,
      new Date().toISOString(),
    );
    const version = this.bumpMetadataVersion();
    this.sql.exec(
      `
      UPDATE vbucket_map
      SET migration_status = 'backfilling', target_shard_id = ?, migration_rows_copied = 0, migration_started_at = ?,
          backfill_table = NULL, backfill_after_pk = NULL, updated_at = ?
      WHERE vbucket = ?
      `,
      targetShard,
      new Date().toISOString(),
      new Date().toISOString(),
      vbucket,
    );

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
        "SELECT vbucket, shard_id, target_shard_id, migration_status, migration_rows_copied FROM vbucket_map WHERE migration_status IN ('backfilling', 'cutover') ORDER BY vbucket ASC",
      );
      for (const m of migrating) {
        try {
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

      let copied = 0;
      let pages = 0;
      while (idx < exportTables.length && pages < MIGRATION_BACKFILL_PAGES_PER_TICK) {
        const t = exportTables[idx];
        // Provision the table on a shard created mid-life (e.g. a split
        // target) before the first page of this table. Stable requestId
        // (create-table-<table>-<shard>) so re-applying it on a resumed tick
        // dedupes rather than double-executes.
        if (afterPk === "" && t.schemaSql) {
          const schemaRes = await this.callShard(target, "/execute", {
            sql: t.schemaSql,
            requestId: `create-table-${t.table}-${target}`,
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
        "UPDATE vbucket_map SET migration_status = 'cutover', updated_at = ? WHERE vbucket = ? AND migration_status = 'backfilling' AND target_shard_id = ?",
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
      const preparedCount = ((await preparedRes.json()) as { count: number }).count;
      if (preparedCount > 0) {
        return true; // poll again next tick
      }

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
        await this.callShard(source, "/unfence-vbucket", { vbucket: m.vbucket });
        await this.callShard(source, "/purge-mirror-jobs", { vbucket: m.vbucket });
        await this.callShard(target, "/delete-vbucket-rows", { vbucket: m.vbucket, tables });
        this.sql.exec(
          "UPDATE vbucket_map SET migration_status = 'backfilling', updated_at = ? WHERE vbucket = ? AND migration_status = 'cutover' AND target_shard_id = ?",
          new Date().toISOString(),
          m.vbucket,
          target,
        );
        return true;
      }

      // Step 4: flip the map. From this write on, /route sends everything to
      // the target; the fence still blocks any straggler write that resolved
      // its route pre-flip and arrives at the source. Conditional, and step
      // 5's destructive source delete only runs if THIS tick actually
      // performed the flip.
      const version = this.bumpMetadataVersion();
      this.sql.exec(
        `
        UPDATE vbucket_map
        SET shard_id = ?, migration_status = 'none', target_shard_id = NULL, map_version = ?, updated_at = ?
        WHERE vbucket = ? AND migration_status = 'cutover' AND shard_id = ? AND target_shard_id = ?
        `,
        target,
        version,
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

      // Step 5: unfence source, then delete the vbucket's rows + provenance
      // from it.
      await this.callShard(source, "/unfence-vbucket", { vbucket: m.vbucket });
      await this.callShard(source, "/delete-vbucket-rows", { vbucket: m.vbucket, tables });

      this.audit("/migrate-vbucket-complete", { vbucket: m.vbucket, fromShard: source, toShard: target, metadataVersion: version });
      return false;
    }

    return false;
  }

  /** Milestone 3, Chunk 4 (POST /migrate-vbucket-status {vbucket}). */
  private async handleMigrateVbucketStatus(request: Request): Promise<Response> {
    const body = (await request.json()) as { vbucket?: number };
    if (body.vbucket === undefined) {
      return json({ error: "Missing vbucket" }, 400);
    }
    const row = this.one<MigrationRow & { migration_started_at: string | null }>(
      "SELECT vbucket, shard_id, target_shard_id, migration_status, migration_rows_copied, migration_started_at FROM vbucket_map WHERE vbucket = ?",
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
    return json({
      vbucket: row.vbucket,
      status: row.migration_status,
      fromShard: row.shard_id,
      toShard: row.target_shard_id,
      rowsCopied: row.migration_rows_copied,
      mirrorQueueDepth,
      startedAt: row.migration_started_at,
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
        "SELECT vbucket, shard_id, target_shard_id, migration_status, migration_rows_copied FROM vbucket_map WHERE vbucket = ?",
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
      // target), then wipe the target's copy.
      await this.callShard(row.shard_id, "/unfence-vbucket", { vbucket: body.vbucket });
      await this.callShard(row.shard_id, "/purge-mirror-jobs", { vbucket: body.vbucket });
      const wipeRes = await this.callShard(row.target_shard_id, "/delete-vbucket-rows", { vbucket: body.vbucket, tables });
      if (!wipeRes.ok) {
        // Leave the row 'aborting' (fence already lifted above) — a retried
        // abort resumes and completes the wipe.
        return json({ error: `Failed to wipe target shard ${row.target_shard_id} — abort not completed, retry.` }, 502);
      }

      // Cleanup fully succeeded — only now clear to 'none'.
      this.sql.exec(
        "UPDATE vbucket_map SET migration_status = 'none', target_shard_id = NULL, migration_rows_copied = 0, migration_started_at = NULL, backfill_table = NULL, backfill_after_pk = NULL, updated_at = ? WHERE vbucket = ?",
        new Date().toISOString(),
        body.vbucket,
      );

      return json({ ok: true, vbucket: body.vbucket, status: "aborted" });
    } finally {
      this.migrationTickInFlight = false;
    }
  }
}
