import { DurableObject } from "cloudflare:workers";
import { json } from "./http";
import { hashKey } from "./hash";
import { checkAdminAuth } from "./auth";
import { log } from "./log";

const ADMIN_GATED_ROUTES = new Set([
  "/status",
  "/list-tables",
  "/drain-shard",
  "/init",
  "/register-table",
  "/split-vbucket",
  "/audit-log",
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

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS table_rules (
        table_name TEXT PRIMARY KEY,
        partitioning TEXT NOT NULL,
        created_at TEXT NOT NULL
      )
    `);

    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        endpoint TEXT NOT NULL,
        request_summary TEXT NOT NULL,
        created_at TEXT NOT NULL
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

    const numShards = Math.max(1, body.numShards ?? 8);
    const totalVBuckets = Math.max(64, body.totalVBuckets ?? 1024);
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
    };

    if (!body.table) {
      return json({ error: "Missing table" }, 400);
    }

    this.audit("/register-table", { table: body.table, partitioning: body.partitioning });

    this.sql.exec(
      `
      INSERT OR REPLACE INTO table_rules (table_name, partitioning, created_at)
      VALUES (?, ?, ?)
      `,
      body.table,
      body.partitioning ?? "hash",
      new Date().toISOString(),
    );

    const version = this.bumpMetadataVersion();
    return json({ ok: true, table: body.table, metadataVersion: version });
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

    const mapped = this.one<{ table_registered: string | null; shard_id: string; status: string }>(
      `
      SELECT
        (SELECT table_name FROM table_rules WHERE table_name = ?) AS table_registered,
        vm.shard_id AS shard_id,
        s.status AS status
      FROM vbucket_map vm
      JOIN shards s ON s.shard_id = vm.shard_id
      WHERE vm.vbucket = ?
      `,
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

    return json({
      shardId: mapped.shard_id,
      vbucket,
      metadataVersion: config.metadata_version,
      catalogShardCount: config.catalog_shard_count,
    });
  }

  private async handleListShards(): Promise<Response> {
    const shards = this.many<{ shard_id: string }>(
      "SELECT shard_id FROM shards WHERE status = 'active' ORDER BY shard_id ASC",
    );
    return json({ shardIds: shards.map((s) => s.shard_id) });
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
    const tables = this.many<{ table_name: string; partitioning: string; created_at: string }>(
      "SELECT table_name, partitioning, created_at FROM table_rules ORDER BY table_name ASC",
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
