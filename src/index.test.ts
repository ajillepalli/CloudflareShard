import { SELF, env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { hashKey } from "./hash";
import { sha256Hex } from "./auth";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";

function tenantForCatalogShard(catalogIndex: number, catalogShardCount: number): string {
  for (let i = 0; ; i += 1) {
    const tenantId = `tenant-${i}`;
    if (hashKey(tenantId) % catalogShardCount === catalogIndex) {
      return tenantId;
    }
  }
}

function post(path: string, body: unknown, authorization?: string) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (authorization) headers.authorization = authorization;
  return SELF.fetch(`https://worker.internal${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
}

const AUTH = () => `Bearer ${env.ADMIN_TOKEN}`;

async function initCluster(numShards = 2, totalVBuckets = 16) {
  const res = await post("/admin/init", { numShards, totalVBuckets, force: true }, AUTH());
  expect(res.status).toBe(200);
  const createRes = await post(
    "/admin/create-table",
    {
      table: "events",
      schema: "CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, v TEXT)",
      partitionKeyColumn: "id",
    },
    AUTH(),
  );
  expect(createRes.status).toBe(200);
}

// rotate: true makes this idempotent across tests that share a catalog shard
// (tenant_auth isn't wiped by /admin/init's force:true, unlike vbucket/shard
// state) — a tenantId reused across test cases would otherwise 409 on the
// second registration.
async function registerTenant(tenantId: string): Promise<string> {
  const res = await post("/admin/register-tenant", { tenantId, rotate: true }, AUTH());
  expect(res.status).toBe(200);
  const body = (await res.json()) as { token: string };
  return `Bearer ${body.token}`;
}

describe("Worker multi-catalog-shard fan-out", () => {
  it("/admin/init initializes every catalog shard, not just one", async () => {
    const res = await post("/admin/init", { numShards: 2, totalVBuckets: 16, force: true }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { catalogShardCount: number; catalogs: Array<{ catalogShardId: string }> };
    expect(body.catalogShardCount).toBeGreaterThan(1);
    expect(body.catalogs.map((c) => c.catalogShardId)).toEqual(
      expect.arrayContaining(["catalog-0", "catalog-1"]),
    );
  });

  it("/admin/status aggregates shard counts across all catalog shards", async () => {
    await initCluster(2, 16);
    const res = await post("/admin/status", {}, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { initialized: boolean; shards: { total: number } };
    expect(body.initialized).toBe(true);
    // 2 shards per catalog x >=2 catalog shards
    expect(body.shards.total).toBeGreaterThanOrEqual(4);
  });

  it("/admin/split-vbucket requires catalogShardId", async () => {
    await initCluster();
    const res = await post("/admin/split-vbucket", { vbucket: 0 }, AUTH());
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toContain("catalogShardId");
  });

  it("/admin/drain-shard requires catalogShardId", async () => {
    await initCluster();
    const res = await post("/admin/drain-shard", { shardId: "catalog-0-shard-0" }, AUTH());
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toContain("catalogShardId");
  });

  it("routes different tenants to their respective catalog shards without shard-ID collisions", async () => {
    await initCluster(2, 16);

    // Find two tenant IDs that land on different catalog shards.
    const tenants = Array.from({ length: 50 }, (_, i) => `tenant-${i}`);
    const routed = new Map<string, string>();
    for (const tenantId of tenants) {
      const token = await registerTenant(tenantId);
      const res = await post(
        "/v1/sql",
        {
          sql: "INSERT INTO events (id, v) VALUES (?, ?)",
          params: [`row-${tenantId}`, "x"],
          table: "events",
          tenantId,
          partitionKey: "p1",
        },
        token,
      );
      expect(res.status).toBe(200);
      const body = (await res.json()) as { route: { catalogShardId: string; shardId: string } };
      routed.set(tenantId, body.route.catalogShardId);
    }
    const distinctCatalogs = new Set(routed.values());
    expect(distinctCatalogs.size).toBeGreaterThan(1);
  });

  it("/v1/scatter merges shard lists across all catalog shards", async () => {
    await initCluster(2, 16);
    const res = await post("/v1/scatter", { sql: "SELECT 1" }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { observability: { shardCount: number } };
    // 2 shards per catalog x >=2 catalog shards
    expect(body.observability.shardCount).toBeGreaterThanOrEqual(4);
  });

  it("/admin/create-table rejects a non-CREATE-TABLE schema", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post(
      "/admin/create-table",
      { table: "events", schema: "DROP TABLE events", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res.status).toBe(400);
  });

  it("/admin/create-table requires partitionKeyColumn", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post(
      "/admin/create-table",
      { table: "events", schema: "CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY)" },
      AUTH(),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("MISSING_PARTITION_KEY_COLUMN");
  });

  it("/admin/create-table requires an admin token", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post("/admin/create-table", {
      table: "events",
      schema: "CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY)",
    });
    expect(res.status).toBe(401);
  });

  it("/admin/create-table rejects a schema smuggling a semicolon-chained statement", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post(
      "/admin/create-table",
      { table: "events", schema: "CREATE TABLE events (id TEXT PRIMARY KEY); DROP TABLE events", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res.status).toBe(403);
  });

  it("/admin/create-table rejects a schema containing a banned keyword like ATTACH", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post(
      "/admin/create-table",
      { table: "events", schema: "CREATE TABLE events (id TEXT PRIMARY KEY) attach database 'x' as y", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res.status).toBe(403);
  });

  it("/admin/init requires an admin token at the Worker level (structural /admin/* gate)", async () => {
    const res = await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true });
    expect(res.status).toBe(401);
  });

  it("/admin/create-table rejects a schema whose CREATE TABLE name doesn't match body.table (regression: could register 'events' while creating a table named 'orders')", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post(
      "/admin/create-table",
      { table: "mismatch_regression_evt", schema: "CREATE TABLE mismatch_regression_orders (id TEXT PRIMARY KEY)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string; schemaTableName: string };
    expect(body.error).toContain("does not match");
    expect(body.schemaTableName).toBe("mismatch_regression_orders");
  });

  it("/admin/create-table accepts a quoted table name matching body.table", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post(
      "/admin/create-table",
      { table: "quoted_regression_evt", schema: 'CREATE TABLE IF NOT EXISTS "quoted_regression_evt" (id TEXT PRIMARY KEY)', partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res.status).toBe(200);
  });

  it("/admin/create-table rejects a partitionKeyColumn that doesn't exist in the created schema, rolling back the table", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post(
      "/admin/create-table",
      { table: "column_mismatch_evt", schema: "CREATE TABLE column_mismatch_evt (id TEXT PRIMARY KEY, v TEXT)", partitionKeyColumn: "nonexistent_col" },
      AUTH(),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("COLUMN_NOT_IN_SCHEMA");

    // Rolled back — /register-table for the same name should succeed cleanly,
    // proving no orphaned physical table or catalog registration was left behind.
    const registerRes = await post(
      "/admin/register-table",
      { table: "column_mismatch_evt", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);
  });

  it("regression (Codex-found): retrying /admin/create-table with the same table+schema after a partitionKeyColumn rollback actually recreates the table, instead of replaying a stale cached success", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "retry_after_rollback_evt";
    const schema = `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`;

    const failed = await post("/admin/create-table", { table, schema, partitionKeyColumn: "nonexistent_col" }, AUTH());
    expect(failed.status).toBe(400);

    // Same table, same schema, corrected partitionKeyColumn — this reuses the
    // exact create-table requestId from the failed attempt. Without
    // invalidating that requestId's idempotency-cache entry on rollback, this
    // would replay the cached "success" from the first attempt without
    // actually recreating the (already-dropped) table.
    const retried = await post("/admin/create-table", { table, schema, partitionKeyColumn: "id" }, AUTH());
    expect(retried.status).toBe(200);

    const introspect = await post("/admin/shard-stats", { shardId: "catalog-0-shard-0" }, AUTH());
    const introspectBody = (await introspect.json()) as { tables: Array<{ table: string; rowCount: number }> };
    expect(introspectBody.tables.map((t) => t.table)).toContain(table);
  });

  it("regression (Codex-found): repeating the same failed /admin/create-table call twice actually drops the table on the second rollback, instead of replaying the first rollback's cached success", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "double_rollback_evt";
    const schema = `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`;

    const firstFailed = await post("/admin/create-table", { table, schema, partitionKeyColumn: "nonexistent_col" }, AUTH());
    expect(firstFailed.status).toBe(400);

    // Same bad call again — the create requestId invalidation from the first
    // rollback lets this genuinely recreate the table, but the rollback
    // DROP's own requestId was previously stable/deterministic too, so this
    // second rollback would replay the first rollback's cached "success"
    // instead of actually dropping the table it just recreated.
    const secondFailed = await post("/admin/create-table", { table, schema, partitionKeyColumn: "nonexistent_col" }, AUTH());
    expect(secondFailed.status).toBe(400);

    const introspect = await post("/admin/shard-stats", { shardId: "catalog-0-shard-0" }, AUTH());
    const introspectBody = (await introspect.json()) as { tables: Array<{ table: string; rowCount: number }> };
    expect(introspectBody.tables.map((t) => t.table)).not.toContain(table);
  });

  it("/admin/create-table reports a shard-level failure when the schema fails to apply", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    // Missing PRIMARY KEY column type makes this a syntactically invalid CREATE TABLE.
    const res = await post(
      "/admin/create-table",
      { table: "broken", schema: "CREATE TABLE broken (id PRIMARY", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toContain("Failed to create table");
  });

  it("/admin/shard-stats requires an admin token (regression: this endpoint had no auth check at all)", async () => {
    await initCluster(1, 4);
    const res = await post("/admin/shard-stats", { shardId: "catalog-0-shard-0" });
    expect(res.status).toBe(401);
  });

  it("/admin/shard-stats returns stats with a valid admin token", async () => {
    await initCluster(1, 4);
    const res = await post("/admin/shard-stats", { shardId: "catalog-0-shard-0" }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean };
    expect(body.ok).toBe(true);
  });

  it("/admin/audit-log requires an admin token", async () => {
    await initCluster(2, 16);
    const res = await post("/admin/audit-log", {});
    expect(res.status).toBe(401);
  });

  it("/admin/audit-log merges entries across all catalog shards (regression: this route was never wired into the Worker, only reachable directly on the CatalogDO)", async () => {
    await initCluster(2, 16);
    const res = await post("/admin/audit-log", {}, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { entries: Array<{ catalogShardId: string; endpoint: string }> };
    const catalogShardIds = new Set(body.entries.map((e) => e.catalogShardId));
    expect(catalogShardIds.size).toBeGreaterThan(1);
    expect(body.entries.map((e) => e.endpoint)).toEqual(expect.arrayContaining(["/init", "/register-table"]));
  });

  it("/v1/sql rejects requests when the cluster's stored catalog shard count differs from the Worker's configured count", async () => {
    await initCluster();

    // Directly seed a mismatched catalog_shard_count into catalog-0's storage,
    // simulating a live cluster whose CATALOG_SHARD_COUNT was changed after init.
    const id = env.CATALOG.idFromName("catalog-0");
    const stub = env.CATALOG.get(id);
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE cluster_config SET catalog_shard_count = 999 WHERE singleton = 1");
    });

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post(
      "/v1/sql",
      {
        sql: "SELECT 1",
        table: "events",
        tenantId,
        partitionKey: "p1",
      },
      token,
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: string };
    expect(body.error).toContain("mismatch");
  });

  it("/v1/sql still rejects CREATE/DROP/ALTER as dangerous", async () => {
    await initCluster();
    const res = await post("/v1/sql", {
      sql: "CREATE TABLE IF NOT EXISTS other (id TEXT PRIMARY KEY)",
      table: "events",
      tenantId: "t1",
      partitionKey: "p1",
    });
    expect(res.status).toBe(403);
  });
});

describe("Worker tenant authorization", () => {
  it("/admin/register-tenant requires an admin token", async () => {
    const res = await post("/admin/register-tenant", { tenantId: "t1" });
    expect(res.status).toBe(401);
  });

  it("/admin/register-tenant issues a token that /v1/sql accepts", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    const res = await post(
      "/v1/sql",
      { sql: "INSERT INTO events (id, v) VALUES (?, ?)", params: ["1", "a"], table: "events", tenantId: "t1", partitionKey: "p1" },
      token,
    );
    expect(res.status).toBe(200);
  });

  it("/v1/sql requires a tenant token (regression: data-plane had no auth at all)", async () => {
    await initCluster();
    const res = await post("/v1/sql", {
      sql: "SELECT * FROM events",
      table: "events",
      tenantId: "t1",
      partitionKey: "p1",
    });
    expect(res.status).toBe(401);
  });

  it("/admin/revoke-tenant invalidates a tenant's access via /v1/sql", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    await post("/admin/revoke-tenant", { tenantId: "t1" }, AUTH());
    const res = await post(
      "/v1/sql",
      { sql: "SELECT * FROM events", table: "events", tenantId: "t1", partitionKey: "p1" },
      token,
    );
    expect(res.status).toBe(401);
  });
});

describe("Worker /v1/mutate", () => {
  it("requires a tenant token", async () => {
    await initCluster();
    const res = await post("/v1/mutate", { op: "insert", table: "events", tenantId: "t1", partitionKey: "p1", values: { v: "a" } });
    expect(res.status).toBe(401);
  });

  it("inserts a row, force-setting the partition-key column, and reports rowsAffected", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    const res = await post(
      "/v1/mutate",
      { op: "insert", table: "events", tenantId: "t1", partitionKey: "row-1", values: { v: "hello" } },
      token,
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; rowsAffected: number };
    expect(body.ok).toBe(true);
    expect(body.rowsAffected).toBe(1);

    const checkRes = await post(
      "/v1/sql",
      { sql: "SELECT id FROM events WHERE id = ?", params: ["row-1"], table: "events", tenantId: "t1", partitionKey: "row-1" },
      token,
    );
    const checkBody = (await checkRes.json()) as { result: { rows: Array<{ id: string }> } };
    expect(checkBody.result.rows).toHaveLength(1);
  });

  it("delete with no where still only scopes to the one partitioned row, never the whole table", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    await post("/v1/mutate", { op: "insert", table: "events", tenantId: "t1", partitionKey: "row-a", values: { v: "a" } }, token);
    const tokenB = await registerTenant("t2");
    await post("/v1/mutate", { op: "insert", table: "events", tenantId: "t2", partitionKey: "row-b", values: { v: "b" } }, tokenB);

    const delRes = await post("/v1/mutate", { op: "delete", table: "events", tenantId: "t1", partitionKey: "row-a" }, token);
    expect(delRes.status).toBe(200);
    const delBody = (await delRes.json()) as { rowsAffected: number };
    expect(delBody.rowsAffected).toBe(1);

    // row-b (a different partition key, inserted under a different tenant/shard)
    // must survive — the delete must not have touched the whole table.
    const checkRes = await post(
      "/v1/sql",
      { sql: "SELECT id FROM events WHERE id = ?", params: ["row-b"], table: "events", tenantId: "t2", partitionKey: "row-b" },
      tokenB,
    );
    const checkBody = (await checkRes.json()) as { result: { rows: Array<{ id: string }> } };
    expect(checkBody.result.rows).toHaveLength(1);
  });

  it("rejects a caller-supplied partition-key value that conflicts with the declared partitionKey", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    const res = await post(
      "/v1/mutate",
      { op: "insert", table: "events", tenantId: "t1", partitionKey: "row-1", values: { id: "different-value", v: "a" } },
      token,
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("PARTITION_KEY_CONFLICT");
  });

  it("rejects mutations against a table that hasn't been upgraded with a partition-key column", async () => {
    // Directly seed a table_rules row still carrying the '__unset__' sentinel,
    // simulating a table registered before this migration (e.g. live from v1.0.0.0).
    // Must land on the SAME catalog shard the test's tenant routes to — the
    // seed is per-catalog-shard, not cluster-wide.
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const id = env.CATALOG.idFromName("catalog-0");
    const stub = env.CATALOG.get(id);
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO table_rules (table_name, partitioning, partition_key_column, created_at) VALUES (?, ?, ?, ?)",
        "legacy_table",
        "hash",
        "__unset__",
        new Date().toISOString(),
      );
    });

    const token = await registerTenant(tenantId);
    const res = await post(
      "/v1/mutate",
      { op: "insert", table: "legacy_table", tenantId, partitionKey: "p1", values: { v: "a" } },
      token,
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("PARTITION_KEY_COLUMN_UNSET");
  });
});

describe("Worker /admin/set-partition-key-column", () => {
  it("requires an admin token", async () => {
    await initCluster();
    const res = await post("/admin/set-partition-key-column", { table: "events", partitionKeyColumn: "id" });
    expect(res.status).toBe(401);
  });

  it("rejects a column that doesn't exist on the table", async () => {
    await initCluster();
    const res = await post(
      "/admin/set-partition-key-column",
      { table: "events", partitionKeyColumn: "nonexistent_col" },
      AUTH(),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("COLUMN_NOT_IN_SCHEMA");
  });

  it("upgrades a sentinel-tagged table, unblocking /v1/mutate against it", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      { table: "upgrade_me", schema: "CREATE TABLE IF NOT EXISTS upgrade_me (id TEXT PRIMARY KEY, v TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    // Force catalog-0 back to the sentinel to simulate a pre-Chunk-1 table —
    // the test tenant must route to catalog-0 specifically, since this seed
    // is per-catalog-shard, not cluster-wide.
    const tenantId = tenantForCatalogShard(0, 4);
    const id = env.CATALOG.idFromName("catalog-0");
    const stub = env.CATALOG.get(id);
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "UPDATE table_rules SET partition_key_column = '__unset__' WHERE table_name = 'upgrade_me'",
      );
    });

    const token = await registerTenant(tenantId);
    const blockedRes = await post(
      "/v1/mutate",
      { op: "insert", table: "upgrade_me", tenantId, partitionKey: "p1", values: { v: "a" } },
      token,
    );
    expect(blockedRes.status).toBe(409);

    // /admin/set-partition-key-column fans out to every catalog shard, so
    // this un-sticks catalog-0 regardless of which shard the tenant is on.
    const upgradeRes = await post(
      "/admin/set-partition-key-column",
      { table: "upgrade_me", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(upgradeRes.status).toBe(200);

    const unblockedRes = await post(
      "/v1/mutate",
      { op: "insert", table: "upgrade_me", tenantId, partitionKey: "p1", values: { v: "a" } },
      token,
    );
    expect(unblockedRes.status).toBe(200);
  });
});

/** ShardDO/CatalogDO storage isn't wiped by /admin/init's force:true (only
 * catalog/shard/vbucket assignment resets — see the established pattern
 * from the drain-shard and tenant-registration tests above). Each
 * create-index test below uses its own dedicated table + index name rather
 * than the shared "events" table, so backfill scans and index_rules
 * registration never leak across tests. */
async function createIndexTestTable(table: string): Promise<void> {
  const res = await post(
    "/admin/create-table",
    { table, schema: `CREATE TABLE IF NOT EXISTS ${table} (id TEXT PRIMARY KEY, v TEXT)`, partitionKeyColumn: "id" },
    AUTH(),
  );
  expect(res.status).toBe(200);
}

describe("Worker /admin/create-index (Milestone 2 Chunk 1)", () => {
  it("requires an admin token", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_auth_evt");
    const res = await post("/admin/create-index", { indexName: "idx_auth_by_v", table: "idx_auth_evt", columns: ["v"] });
    expect(res.status).toBe(401);
  });

  it("rejects missing fields", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post("/admin/create-index", { indexName: "idx_missing_fields" }, AUTH());
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("MISSING_FIELDS");
  });

  it("rejects unsafe identifiers", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_unsafe_evt");
    const res = await post("/admin/create-index", { indexName: "bad; drop table x", table: "idx_unsafe_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("UNSAFE_IDENTIFIER");
  });

  it("rejects a table that isn't registered", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post("/admin/create-index", { indexName: "idx_ghost", table: "idx_nonexistent_table", columns: ["v"] }, AUTH());
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TABLE_NOT_REGISTERED");
  });

  it("rejects a column that doesn't exist on the table's schema", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_badcol_evt");
    const res = await post("/admin/create-index", { indexName: "idx_badcol_by_ghost", table: "idx_badcol_evt", columns: ["nonexistent_col"] }, AUTH());
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("COLUMN_NOT_IN_SCHEMA");
  });

  it("creates an index, backfills pre-existing rows, and registers it", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_backfill_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // Insert rows BEFORE the index exists — proves backfill actually indexes
    // pre-existing data, not just future writes.
    await post("/v1/mutate", { op: "insert", table: "idx_backfill_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    await post("/v1/mutate", { op: "insert", table: "idx_backfill_evt", tenantId, partitionKey: "row-2", values: { v: "beta" } }, token);

    const res = await post("/admin/create-index", { indexName: "idx_backfill_by_v", table: "idx_backfill_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; indexName: string; table: string; columns: string[] };
    expect(body.ok).toBe(true);
    expect(body.indexName).toBe("idx_backfill_by_v");

    // numShards:1 still means 4 total physical shards (one per default
    // catalog shard) — indexShardIdForKey can hash a given entry onto any of
    // them, so search all four rather than assuming catalog-0-shard-0.
    const foundRows: Array<{ table_name: string; index_name: string; index_key_json: string; partition_key: string; source_shard_id: string }> = [];
    for (const candidateShardId of ["catalog-0-shard-0", "catalog-1-shard-0", "catalog-2-shard-0", "catalog-3-shard-0"]) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(candidateShardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        foundRows.push(
          ...(Array.from(
            state.storage.sql.exec(
              "SELECT table_name, index_name, index_key_json, partition_key, source_shard_id FROM __cf_indexes WHERE index_name = ?",
              "idx_backfill_by_v",
            ),
          ) as Array<{ table_name: string; index_name: string; index_key_json: string; partition_key: string; source_shard_id: string }>),
        );
      });
    }
    foundRows.sort((a, b) => (a.partition_key < b.partition_key ? -1 : 1));
    expect(foundRows).toHaveLength(2);
    expect(foundRows[0].partition_key).toBe("row-1");
    expect(JSON.parse(foundRows[0].index_key_json)).toEqual(["alpha"]);
    expect(foundRows[0].source_shard_id).toBe("catalog-0-shard-0");
    expect(foundRows[1].partition_key).toBe("row-2");
    expect(JSON.parse(foundRows[1].index_key_json)).toEqual(["beta"]);

    const listRes = await post("/admin/list-indexes", {}, AUTH());
    expect(listRes.status).toBe(200);
    const listBody = (await listRes.json()) as { indexes: Array<{ indexName: string; table: string; columns: string[] }> };
    expect(listBody.indexes.map((i) => i.indexName)).toContain("idx_backfill_by_v");
  });

  it("rejects a duplicate index name", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_dup_evt");
    const first = await post("/admin/create-index", { indexName: "idx_dup_by_v", table: "idx_dup_evt", columns: ["v"] }, AUTH());
    expect(first.status).toBe(200);
    const second = await post("/admin/create-index", { indexName: "idx_dup_by_v", table: "idx_dup_evt", columns: ["v"] }, AUTH());
    expect(second.status).toBe(409);
    // firstCatalogFanOutFailure wraps the per-shard error under `details`,
    // same shape as every other fanned-out admin route's failure response.
    const body = (await second.json()) as { details: { error: { code: string } } };
    expect(body.details.error.code).toBe("INDEX_ALREADY_REGISTERED");
  });

  it("rejects an index on a table whose partitionKeyColumn hasn't been upgraded", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      { table: "legacy_idx_evt", schema: "CREATE TABLE IF NOT EXISTS legacy_idx_evt (id TEXT PRIMARY KEY, v TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    const id = env.CATALOG.idFromName("catalog-0");
    const stub = env.CATALOG.get(id);
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE table_rules SET partition_key_column = '__unset__' WHERE table_name = 'legacy_idx_evt'");
    });

    const res = await post("/admin/create-index", { indexName: "legacy_idx", table: "legacy_idx_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("PARTITION_KEY_COLUMN_UNSET");
  });
});

describe("Worker /admin/drop-index (Milestone 2 Chunk 6)", () => {
  it("requires an admin token", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post("/admin/drop-index", { indexName: "idx_c6_auth_by_v" });
    expect(res.status).toBe(401);
  });

  it("rejects dropping a nonexistent index", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post("/admin/drop-index", { indexName: "idx_c6_ghost" }, AUTH());
    expect(res.status).toBe(404);
    const body = (await res.json()) as { details: { error: { code: string } } };
    expect(body.details.error.code).toBe("INDEX_NOT_REGISTERED");
  });

  it("drops an index: unregisters it, cleans up physical rows, and blocks future queries", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c6_drop_evt");
    await post("/admin/create-index", { indexName: "idx_c6_drop_by_v", table: "idx_c6_drop_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/mutate", { op: "insert", table: "idx_c6_drop_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    await pollIndexRows("idx_c6_drop_by_v", (r) => r.length === 1);

    const dropRes = await post("/admin/drop-index", { indexName: "idx_c6_drop_by_v" }, AUTH());
    expect(dropRes.status).toBe(200);
    const dropBody = (await dropRes.json()) as { ok: boolean; indexName: string; warning?: string };
    expect(dropBody.ok).toBe(true);
    expect(dropBody.warning).toBeUndefined();

    // Unregistered: /admin/list-indexes no longer includes it.
    const listRes = await post("/admin/list-indexes", {}, AUTH());
    const listBody = (await listRes.json()) as { indexes: Array<{ indexName: string }> };
    expect(listBody.indexes.map((i) => i.indexName)).not.toContain("idx_c6_drop_by_v");

    // Unregistered: a new /v1/index-query is rejected, not silently empty.
    const queryRes = await post("/v1/index-query", { table: "idx_c6_drop_evt", indexName: "idx_c6_drop_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(queryRes.status).toBe(404);

    // Physical rows cleaned up on every shard.
    for (const candidateShardId of ALL_TEST_SHARD_IDS) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(candidateShardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        const rows = Array.from(state.storage.sql.exec("SELECT * FROM __cf_indexes WHERE index_name = ?", "idx_c6_drop_by_v"));
        expect(rows).toHaveLength(0);
      });
    }

    // Raw /v1/sql mutations against the table are unblocked again — no
    // index left to desync.
    const rawRes = await post(
      "/v1/sql",
      { sql: "INSERT INTO idx_c6_drop_evt (id, v) VALUES (?, ?)", params: ["row-2", "beta"], table: "idx_c6_drop_evt", tenantId, partitionKey: "row-2" },
      token,
    );
    expect(rawRes.status).toBe(200);
  });
});

describe("Worker /v1/sql raw mutation against an indexed table (Milestone 2 Chunk 1)", () => {
  it("rejects a raw /v1/sql mutation against a table with a registered index", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_reject_evt");
    const createIndexRes = await post("/admin/create-index", { indexName: "idx_reject_by_v", table: "idx_reject_evt", columns: ["v"] }, AUTH());
    expect(createIndexRes.status).toBe(200);

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post(
      "/v1/sql",
      { sql: "INSERT INTO idx_reject_evt (id, v) VALUES (?, ?)", params: ["row-x", "x"], table: "idx_reject_evt", tenantId, partitionKey: "row-x" },
      token,
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TABLE_HAS_INDEX");
  });

  it("still allows a raw /v1/sql SELECT against a table with a registered index", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_select_evt");
    await post("/admin/create-index", { indexName: "idx_select_by_v", table: "idx_select_evt", columns: ["v"] }, AUTH());

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post(
      "/v1/sql",
      { sql: "SELECT * FROM idx_select_evt WHERE id = ?", params: ["row-x"], table: "idx_select_evt", tenantId, partitionKey: "row-x" },
      token,
    );
    expect(res.status).toBe(200);
  });

  it("allows a raw /v1/sql mutation against a table with no registered index", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_noindex_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post(
      "/v1/sql",
      { sql: "INSERT INTO idx_noindex_evt (id, v) VALUES (?, ?)", params: ["row-y", "y"], table: "idx_noindex_evt", tenantId, partitionKey: "row-y" },
      token,
    );
    expect(res.status).toBe(200);
  });
});

// With numShards:1, each of the 4 default catalog shards still gets its own
// physical shard (catalog-0-shard-0 .. catalog-3-shard-0) — 4 total, not 1.
// indexShardIdForKey hashes into that full pool, so a given index entry can
// land on any of them; tests must search all four, not assume shard 0.
const ALL_TEST_SHARD_IDS = ["catalog-0-shard-0", "catalog-1-shard-0", "catalog-2-shard-0", "catalog-3-shard-0"];

/** Polls __cf_indexes across every shard in the pool until the predicate
 * matches the combined row set, or the attempt budget runs out —
 * ctx.waitUntil()'s index-maintenance work runs after the response is
 * already sent, so a test asserting on its effect can't just check
 * synchronously after the /v1/mutate call resolves. */
async function pollIndexRows(
  indexName: string,
  predicate: (rows: Array<{ partition_key: string; index_key_json: string }>) => boolean,
): Promise<Array<{ partition_key: string; index_key_json: string }>> {
  for (let attempt = 0; attempt < 50; attempt++) {
    const rows: Array<{ partition_key: string; index_key_json: string }> = [];
    for (const shardId of ALL_TEST_SHARD_IDS) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        rows.push(
          ...(Array.from(
            state.storage.sql.exec(
              "SELECT partition_key, index_key_json FROM __cf_indexes WHERE index_name = ? ORDER BY partition_key ASC",
              indexName,
            ),
          ) as Array<{ partition_key: string; index_key_json: string }>),
        );
      });
    }
    rows.sort((a, b) => (a.partition_key < b.partition_key ? -1 : a.partition_key > b.partition_key ? 1 : 0));
    if (predicate(rows)) return rows;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`pollIndexRows timed out waiting for predicate on index ${indexName}`);
}

describe("Worker /v1/mutate async index maintenance (Milestone 2 Chunk 2)", () => {

  it("insert on an indexed table creates an index entry", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c2_insert_evt");
    await post("/admin/create-index", { indexName: "idx_c2_insert_by_v", table: "idx_c2_insert_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post("/v1/mutate", { op: "insert", table: "idx_c2_insert_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    expect(res.status).toBe(200);

    const rows = await pollIndexRows("idx_c2_insert_by_v", (r) => r.length === 1);
    expect(rows[0].partition_key).toBe("row-1");
    expect(JSON.parse(rows[0].index_key_json)).toEqual(["alpha"]);
  });

  it("update on an indexed table removes the old entry and creates the new one", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c2_update_evt");
    await post("/admin/create-index", { indexName: "idx_c2_update_by_v", table: "idx_c2_update_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/mutate", { op: "insert", table: "idx_c2_update_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    await pollIndexRows("idx_c2_update_by_v", (r) => r.length === 1);

    const res = await post("/v1/mutate", { op: "update", table: "idx_c2_update_evt", tenantId, partitionKey: "row-1", values: { v: "beta" } }, token);
    expect(res.status).toBe(200);

    const rows = await pollIndexRows("idx_c2_update_by_v", (r) => r.length === 1 && JSON.parse(r[0].index_key_json)[0] === "beta");
    expect(rows[0].partition_key).toBe("row-1");
    expect(JSON.parse(rows[0].index_key_json)).toEqual(["beta"]);
  });

  it("delete on an indexed table removes the index entry", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c2_delete_evt");
    await post("/admin/create-index", { indexName: "idx_c2_delete_by_v", table: "idx_c2_delete_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/mutate", { op: "insert", table: "idx_c2_delete_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    await pollIndexRows("idx_c2_delete_by_v", (r) => r.length === 1);

    const res = await post("/v1/mutate", { op: "delete", table: "idx_c2_delete_evt", tenantId, partitionKey: "row-1" }, token);
    expect(res.status).toBe(200);

    await pollIndexRows("idx_c2_delete_by_v", (r) => r.length === 0);
  });

  it("update that doesn't touch an indexed column leaves the index entry unchanged", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res0 = await post(
      "/admin/create-table",
      { table: "idx_c2_untouched_evt", schema: "CREATE TABLE IF NOT EXISTS idx_c2_untouched_evt (id TEXT PRIMARY KEY, v TEXT, other TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res0.status).toBe(200);
    await post("/admin/create-index", { indexName: "idx_c2_untouched_by_v", table: "idx_c2_untouched_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/mutate", { op: "insert", table: "idx_c2_untouched_evt", tenantId, partitionKey: "row-1", values: { v: "alpha", other: "x" } }, token);
    await pollIndexRows("idx_c2_untouched_by_v", (r) => r.length === 1);

    // Update only the non-indexed "other" column — "v" (indexed) is unchanged.
    const res = await post("/v1/mutate", { op: "update", table: "idx_c2_untouched_evt", tenantId, partitionKey: "row-1", values: { other: "y" } }, token);
    expect(res.status).toBe(200);

    // Give any (incorrect) async churn a moment, then assert the entry is
    // stable at exactly one row with the original indexed value.
    await new Promise((resolve) => setTimeout(resolve, 50));
    const rows = await pollIndexRows("idx_c2_untouched_by_v", (r) => r.length === 1);
    expect(JSON.parse(rows[0].index_key_json)).toEqual(["alpha"]);
  });

  it("a write to a table with no registered index creates no __cf_indexes rows", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c2_noindex_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post("/v1/mutate", { op: "insert", table: "idx_c2_noindex_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    expect(res.status).toBe(200);

    await new Promise((resolve) => setTimeout(resolve, 50));
    for (const candidateShardId of ALL_TEST_SHARD_IDS) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(candidateShardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        const rows = Array.from(state.storage.sql.exec("SELECT * FROM __cf_indexes WHERE table_name = ?", "idx_c2_noindex_evt"));
        expect(rows).toHaveLength(0);
      });
    }
  });
});

describe("ShardDO index_pending_jobs retry queue (Milestone 2 Chunk 2)", () => {
  it("a job enqueued via /enqueue-index-job is retried and cleared by alarm()", async () => {
    const targetShardId = `idx-retry-target-${crypto.randomUUID()}`;
    const targetStub = env.SHARD.get(env.SHARD.idFromName(targetShardId));
    await targetStub.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sql: "SELECT 1", requestId: "warmup", isMutation: false }),
      }),
    );

    const baseShardId = `idx-retry-base-${crypto.randomUUID()}`;
    const baseStub = env.SHARD.get(env.SHARD.idFromName(baseShardId));
    const enqueueRes = await baseStub.fetch(
      new Request("https://shard.internal/enqueue-index-job", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          targetShardId,
          sql: "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
          params: ["t", "idx_retry", JSON.stringify(["v"]), "pk-1", baseShardId, new Date().toISOString()],
          requestId: "retry-req-1",
        }),
      }),
    );
    expect(enqueueRes.status).toBe(200);

    await runInDurableObject(baseStub, async (instance: ShardDO) => {
      await instance.alarm();
    });

    await runInDurableObject(targetStub, async (_instance: unknown, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT * FROM __cf_indexes WHERE index_name = ?", "idx_retry"));
      expect(rows).toHaveLength(1);
    });
    await runInDurableObject(baseStub, async (_instance: unknown, state: DurableObjectState) => {
      const jobs = Array.from(state.storage.sql.exec("SELECT * FROM index_pending_jobs"));
      expect(jobs).toHaveLength(0);
    });
  });

  it("requires targetShardId, sql, and requestId", async () => {
    const stub = env.SHARD.get(env.SHARD.idFromName(`idx-retry-missing-${crypto.randomUUID()}`));
    const res = await stub.fetch(
      new Request("https://shard.internal/enqueue-index-job", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({}),
      }),
    );
    expect(res.status).toBe(400);
  });
});

/** Finds two partitionKey values that route to different shardIds under the
 * given tenant/table, by probing /v1/sql's route echo (established pattern:
 * see "routes different tenants" above). Needed to build genuine multi-shard
 * /v1/tx test fixtures. */
async function findPartitionKeyPairOnDifferentShards(token: string, tenantId: string, table: string): Promise<[string, string]> {
  const seen = new Map<string, string>();
  for (let i = 0; i < 200; i++) {
    const partitionKey = `pk-${i}`;
    const res = await post(
      "/v1/sql",
      { sql: "SELECT 1", table, tenantId, partitionKey },
      token,
    );
    const body = (await res.json()) as { route: { shardId: string } };
    seen.set(partitionKey, body.route.shardId);
    const distinct = new Set(seen.values());
    if (distinct.size > 1) {
      const entries = Array.from(seen.entries());
      const first = entries[0];
      const second = entries.find(([, shardId]) => shardId !== first[1])!;
      return [first[0], second[0]];
    }
  }
  throw new Error("Could not find two partition keys on different shards.");
}

describe("Worker /v1/tx (cross-shard atomic transactions)", () => {
  it("requires a tenant token", async () => {
    await initCluster();
    const res = await post("/v1/tx", { mutations: [{ op: "insert", table: "events", tenantId: "t1", partitionKey: "p1", values: { v: "a" } }], requestId: "req-1" });
    expect(res.status).toBe(401);
  });

  it("rejects an empty mutations array", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    const res = await post("/v1/tx", { mutations: [], requestId: "req-1" }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("MISSING_MUTATIONS");
  });

  it("rejects a missing requestId", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    const res = await post("/v1/tx", { mutations: [{ op: "insert", table: "events", tenantId: "t1", partitionKey: "p1", values: { v: "a" } }] }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("MISSING_REQUEST_ID");
  });

  it("rejects a transaction touching more than 8 distinct rows, before any DO call", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    const mutations = Array.from({ length: 9 }, (_, i) => ({
      op: "insert" as const,
      table: "events",
      tenantId: "t1",
      partitionKey: `too-many-${i}`,
      values: { v: "x" },
    }));
    const res = await post("/v1/tx", { mutations, requestId: "req-many" }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TOO_MANY_PARTICIPANTS");
  });

  it("rejects a cross-tenant mutation in the same batch with 401 (via its own /route call, not a separate check)", async () => {
    await initCluster();
    const tokenA = await registerTenant("tx-cross-a");
    await registerTenant("tx-cross-b");
    const res = await post(
      "/v1/tx",
      {
        mutations: [
          { op: "insert", table: "events", tenantId: "tx-cross-a", partitionKey: "p1", values: { v: "a" } },
          { op: "insert", table: "events", tenantId: "tx-cross-b", partitionKey: "p2", values: { v: "b" } },
        ],
        requestId: "req-cross",
      },
      tokenA,
    );
    expect(res.status).toBe(401);
  });

  it("commits atomically across two shards and both rows are visible afterward", async () => {
    await initCluster(2, 16);
    const token = await registerTenant("tx-happy");
    const [pkA, pkB] = await findPartitionKeyPairOnDifferentShards(token, "tx-happy", "events");

    const res = await post(
      "/v1/tx",
      {
        mutations: [
          { op: "insert", table: "events", tenantId: "tx-happy", partitionKey: pkA, values: { v: "a" } },
          { op: "insert", table: "events", tenantId: "tx-happy", partitionKey: pkB, values: { v: "b" } },
        ],
        requestId: "req-happy",
      },
      token,
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; status: string };
    expect(body.ok).toBe(true);
    expect(body.status).toBe("committed");

    const checkA = await post("/v1/sql", { sql: "SELECT id FROM events WHERE id = ?", params: [pkA], table: "events", tenantId: "tx-happy", partitionKey: pkA }, token);
    expect(((await checkA.json()) as { result: { rows: unknown[] } }).result.rows).toHaveLength(1);
    const checkB = await post("/v1/sql", { sql: "SELECT id FROM events WHERE id = ?", params: [pkB], table: "events", tenantId: "tx-happy", partitionKey: pkB }, token);
    expect(((await checkB.json()) as { result: { rows: unknown[] } }).result.rows).toHaveLength(1);
  });

  it("prepare failure on one shard rolls back all participants, leaving no trace", async () => {
    await initCluster(2, 16);
    const token = await registerTenant("tx-fail");
    const [pkA, pkB] = await findPartitionKeyPairOnDifferentShards(token, "tx-fail", "events");

    const res = await post(
      "/v1/tx",
      {
        mutations: [
          { op: "insert", table: "events", tenantId: "tx-fail", partitionKey: pkA, values: { v: "a" } },
          // "id" is the partitionKeyColumn, always force-set — but referencing
          // a column that doesn't exist on "events" makes this shard's
          // prepare fail with a genuine SQL error.
          { op: "insert", table: "events", tenantId: "tx-fail", partitionKey: pkB, values: { nonexistent_col: "boom" } },
        ],
        requestId: "req-fail",
      },
      token,
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_ABORTED");

    const checkA = await post("/v1/sql", { sql: "SELECT id FROM events WHERE id = ?", params: [pkA], table: "events", tenantId: "tx-fail", partitionKey: pkA }, token);
    expect(((await checkA.json()) as { result: { rows: unknown[] } }).result.rows).toHaveLength(0);
  });

  it("idempotent retry: re-POSTing the same requestId after commit returns the same committed result without double-applying", async () => {
    await initCluster();
    const token = await registerTenant("tx-idem");
    const payload = {
      mutations: [{ op: "insert", table: "events", tenantId: "tx-idem", partitionKey: "p-idem", values: { v: "a" } }],
      requestId: "req-idem",
    };
    const first = await post("/v1/tx", payload, token);
    expect(first.status).toBe(200);
    const second = await post("/v1/tx", payload, token);
    expect(second.status).toBe(200);
    const secondBody = (await second.json()) as { status: string };
    expect(secondBody.status).toBe("committed");

    const countRes = await post("/v1/sql", { sql: "SELECT COUNT(*) as n FROM events WHERE id = ?", params: ["p-idem"], table: "events", tenantId: "tx-idem", partitionKey: "p-idem" }, token);
    const countBody = (await countRes.json()) as { result: { rows: Array<{ n: number }> } };
    expect(countBody.result.rows[0].n).toBe(1);
  });
});

describe("Worker /v1/tx index-participant piggyback (Milestone 2 Chunk 3)", () => {
  it("insert via /v1/tx creates an index entry atomically with the base row", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c3_insert_evt");
    await post("/admin/create-index", { indexName: "idx_c3_insert_by_v", table: "idx_c3_insert_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post(
      "/v1/tx",
      { mutations: [{ op: "insert", table: "idx_c3_insert_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }], requestId: "req-c3-insert" },
      token,
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; status: string };
    expect(body.ok).toBe(true);
    expect(body.status).toBe("committed");

    const rows = await pollIndexRows("idx_c3_insert_by_v", (r) => r.length === 1);
    expect(rows[0].partition_key).toBe("row-1");
    expect(JSON.parse(rows[0].index_key_json)).toEqual(["alpha"]);
  });

  it("update via /v1/tx removes the old index entry and creates the new one atomically", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c3_update_evt");
    await post("/admin/create-index", { indexName: "idx_c3_update_by_v", table: "idx_c3_update_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post(
      "/v1/tx",
      { mutations: [{ op: "insert", table: "idx_c3_update_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }], requestId: "req-c3-update-1" },
      token,
    );
    await pollIndexRows("idx_c3_update_by_v", (r) => r.length === 1);

    const res = await post(
      "/v1/tx",
      { mutations: [{ op: "update", table: "idx_c3_update_evt", tenantId, partitionKey: "row-1", values: { v: "beta" } }], requestId: "req-c3-update-2" },
      token,
    );
    expect(res.status).toBe(200);

    const rows = await pollIndexRows("idx_c3_update_by_v", (r) => r.length === 1 && JSON.parse(r[0].index_key_json)[0] === "beta");
    expect(rows[0].partition_key).toBe("row-1");
  });

  it("prepare failure on one shard rolls back both the base row and the index entry (no torn state)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c3_fail_evt");
    await post("/admin/create-index", { indexName: "idx_c3_fail_by_v", table: "idx_c3_fail_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // A second mutation in the same batch, against a nonexistent column,
    // fails prepare on its own shard — the whole transaction (including the
    // first mutation's base row AND its index-participant intent) must
    // roll back, per Milestone 1's existing 2PC guarantee.
    const res = await post(
      "/v1/tx",
      {
        mutations: [
          { op: "insert", table: "idx_c3_fail_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } },
          { op: "insert", table: "idx_c3_fail_evt", tenantId, partitionKey: "row-2", values: { nonexistent_col: "boom" } },
        ],
        requestId: "req-c3-fail",
      },
      token,
    );
    expect(res.status).toBe(409);

    const checkRes = await post("/v1/sql", { sql: "SELECT * FROM idx_c3_fail_evt WHERE id = ?", params: ["row-1"], table: "idx_c3_fail_evt", tenantId, partitionKey: "row-1" }, token);
    const checkBody = (await checkRes.json()) as { result: { rows: unknown[] } };
    expect(checkBody.result.rows).toHaveLength(0);

    for (const candidateShardId of ALL_TEST_SHARD_IDS) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(candidateShardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        const rows = Array.from(state.storage.sql.exec("SELECT * FROM __cf_indexes WHERE index_name = ?", "idx_c3_fail_by_v"));
        expect(rows).toHaveLength(0);
      });
    }
  });

  it("CRITICAL regression: a /v1/tx transaction that worked before an index existed still works once one does, not TOO_MANY_PARTICIPANTS", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 16, force: true }, AUTH());
    const res0 = await post(
      "/admin/create-table",
      { table: "idx_c3_regress_evt", schema: "CREATE TABLE IF NOT EXISTS idx_c3_regress_evt (id TEXT PRIMARY KEY, v TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res0.status).toBe(200);
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // 8 distinct base-row mutations — exactly at MAX_TX_PARTICIPANT_KEYS.
    // This already worked pre-index; registering an index must not push it
    // over the cap just because index-participant intents ride along.
    const mutations = Array.from({ length: 8 }, (_, i) => ({
      op: "insert" as const,
      table: "idx_c3_regress_evt",
      tenantId,
      partitionKey: `row-${i}`,
      values: { v: `val-${i}` },
    }));

    const preIndexRes = await post("/v1/tx", { mutations, requestId: "req-c3-regress-pre" }, token);
    expect(preIndexRes.status).toBe(200);

    await post("/admin/create-index", { indexName: "idx_c3_regress_by_v", table: "idx_c3_regress_evt", columns: ["v"] }, AUTH());

    const postIndexMutations = Array.from({ length: 8 }, (_, i) => ({
      op: "insert" as const,
      table: "idx_c3_regress_evt",
      tenantId,
      partitionKey: `row2-${i}`,
      values: { v: `val2-${i}` },
    }));
    const postIndexRes = await post("/v1/tx", { mutations: postIndexMutations, requestId: "req-c3-regress-post" }, token);
    expect(postIndexRes.status).toBe(200);
    const postIndexBody = (await postIndexRes.json()) as { ok: boolean; status: string };
    expect(postIndexBody.ok).toBe(true);
    expect(postIndexBody.status).toBe("committed");
  });
});

describe("Worker /v1/index-query (Milestone 2 Chunk 4)", () => {
  it("finds a row inserted via /v1/mutate (async index path)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_mutate_evt");
    await post("/admin/create-index", { indexName: "idx_c4_mutate_by_v", table: "idx_c4_mutate_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/mutate", { op: "insert", table: "idx_c4_mutate_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    await pollIndexRows("idx_c4_mutate_by_v", (r) => r.length === 1);

    const res = await post("/v1/index-query", { table: "idx_c4_mutate_evt", indexName: "idx_c4_mutate_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: Array<{ id: string; v: string }> };
    expect(body.rows).toHaveLength(1);
    expect(body.rows[0].id).toBe("row-1");
    expect(body.rows[0].v).toBe("alpha");
  });

  it("finds a row inserted via /v1/tx (2PC piggyback path)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_tx_evt");
    await post("/admin/create-index", { indexName: "idx_c4_tx_by_v", table: "idx_c4_tx_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/tx", { mutations: [{ op: "insert", table: "idx_c4_tx_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }], requestId: "req-c4-tx" }, token);

    const res = await post("/v1/index-query", { table: "idx_c4_tx_evt", indexName: "idx_c4_tx_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: Array<{ id: string }> };
    expect(body.rows).toHaveLength(1);
    expect(body.rows[0].id).toBe("row-1");
  });

  it("returns an empty result for no matching rows", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_empty_evt");
    await post("/admin/create-index", { indexName: "idx_c4_empty_by_v", table: "idx_c4_empty_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post("/v1/index-query", { table: "idx_c4_empty_evt", indexName: "idx_c4_empty_by_v", tenantId, values: { v: "ghost" } }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: unknown[] };
    expect(body.rows).toHaveLength(0);
  });

  it("requires a tenant token", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_auth_evt");
    await post("/admin/create-index", { indexName: "idx_c4_auth_by_v", table: "idx_c4_auth_evt", columns: ["v"] }, AUTH());
    const res = await post("/v1/index-query", { table: "idx_c4_auth_evt", indexName: "idx_c4_auth_by_v", tenantId: "t1", values: { v: "alpha" } });
    expect(res.status).toBe(401);
  });

  it("rejects a query against an unregistered index name", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_ghost_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post("/v1/index-query", { table: "idx_c4_ghost_evt", indexName: "idx_c4_ghost_index", tenantId, values: { v: "alpha" } }, token);
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("INDEX_NOT_REGISTERED");
  });

  it("rejects a query missing a value for one of the index's columns", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res0 = await post(
      "/admin/create-table",
      { table: "idx_c4_partial_evt", schema: "CREATE TABLE IF NOT EXISTS idx_c4_partial_evt (id TEXT PRIMARY KEY, a TEXT, b TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res0.status).toBe(200);
    await post("/admin/create-index", { indexName: "idx_c4_partial_by_ab", table: "idx_c4_partial_evt", columns: ["a", "b"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post("/v1/index-query", { table: "idx_c4_partial_evt", indexName: "idx_c4_partial_by_ab", tenantId, values: { a: "x" } }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("INCOMPLETE_INDEX_KEY");
  });

  it("excludes a stale index entry whose base row no longer matches (async staleness re-check)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_stale_evt");
    await post("/admin/create-index", { indexName: "idx_c4_stale_by_v", table: "idx_c4_stale_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/mutate", { op: "insert", table: "idx_c4_stale_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    await pollIndexRows("idx_c4_stale_by_v", (r) => r.length === 1);

    // Simulate a race: the base row changed, but the OLD index entry
    // (pointing at "alpha") hasn't been cleaned up yet — directly seed a
    // stale entry alongside whatever the real async write already produced,
    // by updating the base row without going through /v1/mutate's index
    // maintenance (mirrors a lagging async write mid-flight).
    for (const candidateShardId of ALL_TEST_SHARD_IDS) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(candidateShardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        const rows = Array.from(state.storage.sql.exec("SELECT * FROM idx_c4_stale_evt WHERE id = 'row-1'"));
        if (rows.length > 0) {
          state.storage.sql.exec("UPDATE idx_c4_stale_evt SET v = 'beta' WHERE id = 'row-1'");
        }
      });
    }

    // The stale __cf_indexes entry (still keyed on "alpha") must not surface
    // a row whose actual current value no longer matches.
    const res = await post("/v1/index-query", { table: "idx_c4_stale_evt", indexName: "idx_c4_stale_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: unknown[] };
    expect(body.rows).toHaveLength(0);
  });

  it("caps fan-out at the requested limit", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_limit_evt");
    await post("/admin/create-index", { indexName: "idx_c4_limit_by_v", table: "idx_c4_limit_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    for (let i = 0; i < 5; i++) {
      await post("/v1/mutate", { op: "insert", table: "idx_c4_limit_evt", tenantId, partitionKey: `row-${i}`, values: { v: "shared" } }, token);
    }
    await pollIndexRows("idx_c4_limit_by_v", (r) => r.length === 5);

    const res = await post("/v1/index-query", { table: "idx_c4_limit_evt", indexName: "idx_c4_limit_by_v", tenantId, values: { v: "shared" }, limit: 2 }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: unknown[] };
    expect(body.rows).toHaveLength(2);
  });
});

describe("Worker /admin/tx-status and /admin/tx-force-abort", () => {
  it("/admin/tx-status requires an admin token", async () => {
    const res = await post("/admin/tx-status", { txId: "whatever" });
    expect(res.status).toBe(401);
  });

  it("/admin/tx-force-abort requires an admin token", async () => {
    const res = await post("/admin/tx-force-abort", { txId: "whatever" });
    expect(res.status).toBe(401);
  });

  it("/admin/tx-status reports a committed transaction created via /v1/tx", async () => {
    await initCluster();
    const token = await registerTenant("tx-status-check");
    const payload = {
      mutations: [{ op: "insert", table: "events", tenantId: "tx-status-check", partitionKey: "p-status", values: { v: "a" } }],
      requestId: "req-status",
    };
    const txRes = await post("/v1/tx", payload, token);
    expect(txRes.status).toBe(200);

    const txId = await sha256Hex(JSON.stringify(["tx-status-check", "req-status"]));
    const statusRes = await post("/admin/tx-status", { txId }, AUTH());
    expect(statusRes.status).toBe(200);
    const statusBody = (await statusRes.json()) as { found: boolean; status: string };
    expect(statusBody.found).toBe(true);
    expect(statusBody.status).toBe("committed");
  });
});

describe("Worker /admin/drain-shard: draining interaction with in-flight transactions (Milestone 1 Chunk 4)", () => {
  function shardPost(path: string, body: unknown) {
    return new Request(`https://shard.internal${path}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  it("drains a shard with zero pending intents unchanged", async () => {
    await initCluster(1, 4);
    const res = await post("/admin/drain-shard", { shardId: "catalog-0-shard-0", catalogShardId: "catalog-0" }, AUTH());
    expect(res.status).toBe(200);
  });

  it("rejects draining a shard with an in-flight prepared transaction, 409", async () => {
    await initCluster(1, 4);
    const shardId = "catalog-0-shard-0";
    const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));
    await shardStub.fetch(
      shardPost("/prepare", {
        coordinatorTxId: "drain-test-tx",
        intents: [{ sql: "INSERT INTO events (id, v) VALUES (?, ?)", params: ["drain-row", "x"], tenantId: "t1", table: "events", partitionKey: "drain-row" }],
      }),
    );

    const res = await post("/admin/drain-shard", { shardId, catalogShardId: "catalog-0" }, AUTH());
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("SHARD_HAS_IN_FLIGHT_TRANSACTIONS");

    // Clean up: this shard's DO storage (unlike CatalogDO's) isn't reset by
    // /admin/init between tests in this suite, since every test in this
    // describe block deliberately reuses the same deterministic shard name.
    await shardStub.fetch(shardPost("/abort", { coordinatorTxId: "drain-test-tx" }));
  });

  it("a retried drain succeeds once the in-flight transaction resolves", async () => {
    await initCluster(1, 4);
    const shardId = "catalog-0-shard-0";
    const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));
    await shardStub.fetch(
      shardPost("/prepare", {
        coordinatorTxId: "drain-test-tx-2",
        intents: [{ sql: "INSERT INTO events (id, v) VALUES (?, ?)", params: ["drain-row-2", "x"], tenantId: "t1", table: "events", partitionKey: "drain-row-2" }],
      }),
    );

    const blocked = await post("/admin/drain-shard", { shardId, catalogShardId: "catalog-0" }, AUTH());
    expect(blocked.status).toBe(409);

    await shardStub.fetch(shardPost("/commit", { coordinatorTxId: "drain-test-tx-2" }));

    const retried = await post("/admin/drain-shard", { shardId, catalogShardId: "catalog-0" }, AUTH());
    expect(retried.status).toBe(200);
  });

  it("confirms a new mutation targeting an already-draining shard is rejected 503 (existing CatalogDO behavior)", async () => {
    await initCluster(1, 4);
    const drainRes = await post("/admin/drain-shard", { shardId: "catalog-0-shard-0", catalogShardId: "catalog-0" }, AUTH());
    expect(drainRes.status).toBe(200);

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post(
      "/v1/sql",
      { sql: "INSERT INTO events (id, v) VALUES (?, ?)", params: ["p1", "a"], table: "events", tenantId, partitionKey: "p1" },
      token,
    );
    expect(res.status).toBe(503);
  });

  it("rejects draining a shard with an unresolved index-write retry job, 409 (Milestone 2 Chunk 5)", async () => {
    await initCluster(1, 4);
    const shardId = "catalog-0-shard-0";
    const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));
    await shardStub.fetch(
      shardPost("/enqueue-index-job", {
        targetShardId: "some-unreachable-index-shard",
        sql: "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        params: ["t", "idx", JSON.stringify(["v"]), "pk-1", shardId, new Date().toISOString()],
        requestId: "drain-index-job-1",
      }),
    );

    const res = await post("/admin/drain-shard", { shardId, catalogShardId: "catalog-0" }, AUTH());
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("SHARD_HAS_PENDING_INDEX_JOBS");

    // Clean up for subsequent tests reusing this same shard name.
    await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM index_pending_jobs");
    });
  });

  it("a retried drain succeeds once the pending index job resolves", async () => {
    await initCluster(1, 4);
    const shardId = "catalog-0-shard-0";
    const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));

    // Target a real, reachable shard this time so the alarm-driven retry
    // actually succeeds and clears the job.
    const targetStub = env.SHARD.get(env.SHARD.idFromName("catalog-1-shard-0"));
    await targetStub.fetch(shardPost("/execute", { sql: "SELECT 1", requestId: "warmup", isMutation: false }));

    await shardStub.fetch(
      shardPost("/enqueue-index-job", {
        targetShardId: "catalog-1-shard-0",
        sql: "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        params: ["t", "idx", JSON.stringify(["v"]), "pk-1", shardId, new Date().toISOString()],
        requestId: "drain-index-job-2",
      }),
    );

    const blocked = await post("/admin/drain-shard", { shardId, catalogShardId: "catalog-0" }, AUTH());
    expect(blocked.status).toBe(409);

    await runInDurableObject(shardStub, async (instance: ShardDO) => {
      await instance.alarm();
    });

    const retried = await post("/admin/drain-shard", { shardId, catalogShardId: "catalog-0" }, AUTH());
    expect(retried.status).toBe(200);
  });

  it("/admin/shard-stats reports indexPendingJobCount and indexEntryCount", async () => {
    await initCluster(1, 4);
    const shardId = "catalog-0-shard-0";
    const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));
    await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        "t",
        "idx",
        JSON.stringify(["v"]),
        "pk-1",
        shardId,
        new Date().toISOString(),
      );
    });

    const res = await post("/admin/shard-stats", { shardId }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { indexPendingJobCount: number; indexEntryCount: number };
    expect(body.indexEntryCount).toBeGreaterThanOrEqual(1);
    expect(body.indexPendingJobCount).toBe(0);
  });
});

describe("Worker top-level routes", () => {
  it("GET /health returns ok", async () => {
    const res = await SELF.fetch("https://worker.internal/health");
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; service: string };
    expect(body.ok).toBe(true);
  });

  it("returns 405 for non-POST on a POST-only route", async () => {
    const res = await SELF.fetch("https://worker.internal/v1/sql", { method: "GET" });
    expect(res.status).toBe(405);
  });

  it("returns 404 for an unknown route", async () => {
    const res = await post("/not-a-real-route", {});
    expect(res.status).toBe(404);
  });

  it("returns a clean 500 instead of a crash on malformed JSON", async () => {
    const res = await SELF.fetch("https://worker.internal/v1/sql", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{not valid json",
    });
    expect(res.status).toBe(500);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("Internal error.");
  });
});

describe("Worker /v1/sql input validation", () => {
  it("returns 400 for missing sql/table/tenantId", async () => {
    const res = await post("/v1/sql", { table: "events" });
    expect(res.status).toBe(400);
  });

  it("returns 400 when params is not an array", async () => {
    await initCluster();
    const res = await post("/v1/sql", {
      sql: "SELECT * FROM events",
      table: "events",
      tenantId: "t1",
      partitionKey: "p1",
      params: "not-an-array",
    });
    expect(res.status).toBe(400);
  });

  it("returns 400 for a mutating statement without partitionKey", async () => {
    await initCluster();
    const res = await post("/v1/sql", {
      sql: "INSERT INTO events (id, v) VALUES ('1','a')",
      table: "events",
      tenantId: "t1",
    });
    expect(res.status).toBe(400);
  });

  it("returns 400 for a SELECT without partitionKey (must use /v1/scatter)", async () => {
    await initCluster();
    const res = await post("/v1/sql", {
      sql: "SELECT * FROM events",
      table: "events",
      tenantId: "t1",
    });
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toContain("/v1/scatter");
  });
});

describe("Worker /v1/scatter input validation and partial failure", () => {
  it("requires an admin token (regression: scatter reads across every tenant indiscriminately)", async () => {
    const res = await post("/v1/scatter", { sql: "SELECT 1" });
    expect(res.status).toBe(401);
  });

  it("returns 400 for missing sql", async () => {
    const res = await post("/v1/scatter", {}, AUTH());
    expect(res.status).toBe(400);
  });

  it("returns 400 for a mutating statement", async () => {
    const res = await post("/v1/scatter", { sql: "INSERT INTO events (id) VALUES ('1')" }, AUTH());
    expect(res.status).toBe(400);
  });

  it("regression: rejects a comment-prefixed mutation instead of executing it as a read", async () => {
    await initCluster();
    const res = await post("/v1/scatter", { sql: "-- harmless\nDELETE FROM events" }, AUTH());
    expect(res.status).toBe(400);

    const res2 = await post("/v1/scatter", { sql: "/*x*/ UPDATE events SET v = 'pwned'" }, AUTH());
    expect(res2.status).toBe(400);
  });

  it("returns 403 for a dangerous non-mutation statement", async () => {
    // PRAGMA isn't classified as a mutation prefix, so it reaches the
    // isDangerous() deny-list check rather than the mutation-rejection check.
    const res = await post("/v1/scatter", { sql: "PRAGMA table_info(events)" }, AUTH());
    expect(res.status).toBe(403);
  });

  it("returns 400 when params is not an array", async () => {
    const res = await post("/v1/scatter", { sql: "SELECT 1", params: "nope" }, AUTH());
    expect(res.status).toBe(400);
  });

  it("caps results at the requested limit", async () => {
    await initCluster(1, 64);
    for (let i = 0; i < 5; i += 1) {
      const tenantId = `tenant-${i}`;
      const token = await registerTenant(tenantId);
      await post(
        "/v1/sql",
        {
          sql: "INSERT INTO events (id, v) VALUES (?, ?)",
          params: [`id-${i}`, "x"],
          table: "events",
          tenantId,
          partitionKey: "p1",
        },
        token,
      );
    }
    const res = await post("/v1/scatter", { sql: "SELECT id FROM events", limit: 2 }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: unknown[] };
    expect(body.rows.length).toBeLessThanOrEqual(2);
  });
});
