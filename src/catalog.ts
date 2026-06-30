export class CatalogDO extends DurableObject {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
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

  private hashKey(input: string): number {
    let h = 0x811c9dc5;
    for (let i = 0; i < input.length; i += 1) {
      h ^= input.charCodeAt(i);
      h = Math.imul(h, 0x01000193);
    }
    return h >>> 0;
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
    this.ensureSchema();

    const url = new URL(request.url);
    const method = request.method.toUpperCase();

    const json = (data: unknown, status = 200): Response =>
      new Response(JSON.stringify(data, null, 2), {
        status,
        headers: { "content-type": "application/json; charset=utf-8" },
      });

    if (method !== "POST") {
      return json({ error: "Only POST allowed for catalog endpoints." }, 405);
    }

    if (url.pathname === "/init") {
      const body = (await request.json()) as {
        numShards?: number;
        totalVBuckets?: number;
        force?: boolean;
      };

      const numShards = Math.max(1, body.numShards ?? 8);
      const totalVBuckets = Math.max(64, body.totalVBuckets ?? 1024);
      const force = body.force === true;

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
        INSERT OR REPLACE INTO cluster_config (singleton, total_vbuckets, metadata_version, initialized_at)
        VALUES (1, ?, 1, ?)
        `,
        totalVBuckets,
        new Date().toISOString(),
      );

      for (let i = 0; i < numShards; i += 1) {
        const shardId = `shard-${i}`;
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
        const shardId = `shard-${vb % numShards}`;
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

      return json({ ok: true, numShards, totalVBuckets });
    }

    if (url.pathname === "/register-table") {
      const body = (await request.json()) as {
        table: string;
        partitioning?: string;
      };

      if (!body.table) {
        return json({ error: "Missing table" }, 400);
      }

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

    if (url.pathname === "/route") {
      const body = (await request.json()) as {
        table: string;
        tenantId: string;
        partitionKey: string;
      };

      if (!body.table || !body.tenantId || !body.partitionKey) {
        return json({ error: "Missing table, tenantId, or partitionKey" }, 400);
      }

      const config = this.one<{ total_vbuckets: number; metadata_version: number }>(
        "SELECT total_vbuckets, metadata_version FROM cluster_config WHERE singleton = 1",
      );
      if (!config) {
        return json({ error: "Cluster not initialized. Call /admin/init first." }, 400);
      }

      const rule = this.one<{ table_name: string }>(
        "SELECT table_name FROM table_rules WHERE table_name = ?",
        body.table,
      );
      if (!rule) {
        return json(
          {
            error: `Table ${body.table} is not registered. Call /admin/register-table first.`,
          },
          400,
        );
      }

      const composite = `${body.tenantId}:${body.table}:${body.partitionKey}`;
      const vbucket = this.hashKey(composite) % config.total_vbuckets;

      const mapped = this.one<{ shard_id: string }>(
        "SELECT shard_id FROM vbucket_map WHERE vbucket = ?",
        vbucket,
      );
      if (!mapped) {
        return json({ error: `No shard mapping for vbucket ${vbucket}` }, 500);
      }

      return json({
        shardId: mapped.shard_id,
        vbucket,
        metadataVersion: config.metadata_version,
      });
    }

    if (url.pathname === "/list-shards") {
      const shards = this.many<{ shard_id: string }>(
        "SELECT shard_id FROM shards WHERE status = 'active' ORDER BY shard_id ASC",
      );
      return json({ shardIds: shards.map((s) => s.shard_id) });
    }

    if (url.pathname === "/split-vbucket") {
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

      const targetShard = body.newShardId ?? `shard-split-${Date.now()}`;
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

    return json({ error: `Unknown catalog route: ${url.pathname}` }, 404);
  }
}
