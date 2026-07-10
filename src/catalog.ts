import { DurableObject } from "cloudflare:workers";
import { json } from "./http";
import { hashKey } from "./hash";
import { checkAdminAuth, sha256Hex, timingSafeEqual } from "./auth";
import { log } from "./log";
import { IDENTIFIER_RE, UNSET_PARTITION_KEY_COLUMN } from "./structured-op";

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
]);

export class CatalogDO extends DurableObject {
  private readonly sql: SqlStorage;
  private readonly adminToken?: string;
  private readonly routes: Record<string, (request: Request) => Promise<Response>>;

  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
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
    };
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
      INSERT OR REPLACE INTO table_rules (table_name, partitioning, partition_key_column, created_at)
      VALUES (?, ?, ?, ?)
      `,
      body.table,
      body.partitioning ?? "hash",
      body.partitionKeyColumn,
      new Date().toISOString(),
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

    // Milestone 3, Chunk 2: index placement now hashes over each index's own
    // PINNED placement_ring_json (captured once at /admin/create-index),
    // never the live active shard set — so draining a shard no longer
    // silently orphans __cf_indexes entries for indexes that don't happen to
    // include the draining shard in their ring. The previous blanket 409
    // here (SHARD_DRAIN_BLOCKED_BY_INDEXES) is removed. Draining a shard
    // that a ring DOES contain still needs care — that's Chunk 5's ring
    // evacuation (drain migrates every vbucket off first, then substitutes
    // this shard out of any ring containing it before finishing).
    this.audit("/drain-shard", { shardId: body.shardId });

    this.sql.exec("UPDATE shards SET status = 'draining' WHERE shard_id = ?", body.shardId);

    const version = this.bumpMetadataVersion();
    return json({ ok: true, shardId: body.shardId, metadataVersion: version });
  }

  private async handleSplitVbucket(request: Request): Promise<Response> {
    const body = (await request.json()) as {
      vbucket: number;
      newShardId?: string;
    };

    if (!Number.isInteger(body.vbucket) || body.vbucket < 0) {
      return json({ error: "vbucket must be a non-negative integer" }, 400);
    }

    const existingMap = this.one<{ shard_id: string }>(
      "SELECT shard_id FROM vbucket_map WHERE vbucket = ?",
      body.vbucket,
    );
    if (!existingMap) {
      return json({ error: `vbucket ${body.vbucket} has no mapping` }, 404);
    }

    // Milestone 3, Chunk 2: splitting no longer needs to block on index
    // presence (the previous 409 SPLIT_BLOCKED_BY_INDEXES is removed) —
    // index placement now hashes over each index's own pinned
    // placement_ring_json, captured once at /admin/create-index, so adding a
    // new active shard here never changes any existing index's placement
    // modulo.
    this.audit("/split-vbucket", { vbucket: body.vbucket, newShardId: body.newShardId, fromShard: existingMap.shard_id });

    const config = this.one<{ catalog_shard_id: string | null }>(
      "SELECT catalog_shard_id FROM cluster_config WHERE singleton = 1",
    );
    const shardPrefix = config?.catalog_shard_id ? `${config.catalog_shard_id}-` : "";
    const targetShard = body.newShardId ?? `${shardPrefix}shard-split-${Date.now()}`;
    this.sql.exec(
      `
      INSERT OR IGNORE INTO shards (shard_id, status, created_at)
      VALUES (?, 'active', ?)
      `,
      targetShard,
      new Date().toISOString(),
    );

    const version = this.bumpMetadataVersion();
    this.sql.exec(
      `
      UPDATE vbucket_map
      SET shard_id = ?, map_version = ?, updated_at = ?
      WHERE vbucket = ?
      `,
      targetShard,
      version,
      new Date().toISOString(),
      body.vbucket,
    );

    return json({
      ok: true,
      vbucket: body.vbucket,
      fromShard: existingMap.shard_id,
      toShard: targetShard,
      metadataVersion: version,
    });
  }
}
