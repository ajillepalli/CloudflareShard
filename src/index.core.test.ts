import { SELF, env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { hashKey, indexShardIdForKey } from "./hash";
import { sha256Hex } from "./auth";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";
import { AUTH, initCluster, post, registerTenant, tenantForCatalogShard } from "./index.test-helpers";

// This file is one of several index.*.test.ts files split out of a single
// index.test.ts (see index.test-helpers.ts's header comment for why). DO
// storage persists across `it` blocks within a file, so afterEach(reset())
// gives every test clean storage — the same isolation the pre-split file used.
afterEach(async () => {
  await reset();
});

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
        AUTH(),
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
      AUTH(),
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: string };
    expect(body.error).toContain("mismatch");
  });

  it("/v1/sql still rejects CREATE/DROP/ALTER as dangerous", async () => {
    await initCluster();
    const res = await post(
      "/v1/sql",
      {
        sql: "CREATE TABLE IF NOT EXISTS other (id TEXT PRIMARY KEY)",
        table: "events",
        tenantId: "t1",
        partitionKey: "p1",
      },
      AUTH(),
    );
    expect(res.status).toBe(403);
  });
});


describe("Worker tenant authorization", () => {
  it("/admin/register-tenant requires an admin token", async () => {
    const res = await post("/admin/register-tenant", { tenantId: "t1" });
    expect(res.status).toBe(401);
  });

  // Architecture change: /v1/sql is ADMIN-ONLY. A tenant token (the value
  // /register-tenant issues) is NO LONGER accepted — tenants use /v1/mutate,
  // /v1/tx, /v1/index-query. The admin token works.
  it("/v1/sql rejects a tenant token 401 and accepts ADMIN_TOKEN", async () => {
    await initCluster();
    const tenantToken = await registerTenant("t1");
    const rejected = await post(
      "/v1/sql",
      { sql: "INSERT INTO events (id, v) VALUES (?, ?)", params: ["1", "a"], table: "events", tenantId: "t1", partitionKey: "p1" },
      tenantToken,
    );
    expect(rejected.status).toBe(401);

    const accepted = await post(
      "/v1/sql",
      { sql: "INSERT INTO events (id, v) VALUES (?, ?)", params: ["1", "a"], table: "events", tenantId: "t1", partitionKey: "p1" },
      AUTH(),
    );
    expect(accepted.status).toBe(200);
  });

  it("/v1/sql requires the admin token (no token → 401)", async () => {
    await initCluster();
    const res = await post("/v1/sql", {
      sql: "SELECT * FROM events",
      table: "events",
      tenantId: "t1",
      partitionKey: "p1",
    });
    expect(res.status).toBe(401);
  });

  it("/admin/revoke-tenant invalidates a tenant's access via /v1/mutate (the tenant data plane)", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    await post("/admin/revoke-tenant", { tenantId: "t1" }, AUTH());
    const res = await post(
      "/v1/mutate",
      { op: "insert", table: "events", tenantId: "t1", partitionKey: "p1", values: { v: "a" } },
      token,
    );
    expect(res.status).toBe(401);
  });

  // Architecture change: /v1/sql is admin-only. The remaining guardrail blocks
  // a MUTATION whose write TARGET is an internal bookkeeping table (so a
  // fat-fingered operator can't corrupt fence/provenance/mirror state), while
  // ALLOWING internal-table READS (operator debugging) and cross-table access
  // (admin is trusted). The exhaustive target-spelling matrix (case, inline
  // comments, schema qualifiers, quoting) lives in sql-safety.test.ts
  // (mutationTargetIsInternal); here we verify the worker wiring.
  it("admin guardrail: blocks WRITES to internal targets, but ALLOWS internal-table reads and cross-table writes", async () => {
    await initCluster();
    await post(
      "/admin/create-table",
      { table: "other_tbl", schema: "CREATE TABLE IF NOT EXISTS other_tbl (id TEXT PRIMARY KEY, v TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );

    // WRITES whose target is an internal table → 403, however spelled.
    for (const sql of [
      "DELETE FROM __cf_fenced_vbuckets",
      'DELETE FROM main . "__cf_row_owners"',
      "DELETE/**/FROM applied_requests",
      "UPDATE `row_locks` SET x = 1",
    ]) {
      const res = await post("/v1/sql", { sql, table: "events", tenantId: "t1", partitionKey: "p1" }, AUTH());
      expect(res.status, `${sql} should be 403`).toBe(403);
      expect(((await res.json()) as { error: { code?: string } }).error.code, `${sql} code`).toBe("INTERNAL_TABLE_WRITE_FORBIDDEN");
    }

    // A plain READ of an internal table is ALLOWED (operator debugging).
    const read = await post(
      "/v1/sql",
      { sql: "SELECT COUNT(*) AS n FROM __cf_row_owners", table: "events", tenantId: "t1", partitionKey: "p1" },
      AUTH(),
    );
    expect(read.status, "internal-table read allowed for admin").toBe(200);

    // A cross-table write (target differs from body.table) is ALLOWED — there
    // is no per-tenant table ownership for the operator.
    const cross = await post(
      "/v1/sql",
      { sql: "INSERT INTO other_tbl (id, v) VALUES (?, ?)", params: [`x-${crypto.randomUUID()}`, "v"], table: "events", tenantId: "t1", partitionKey: "p1" },
      AUTH(),
    );
    expect(cross.status, "cross-table write allowed for admin").toBe(200);
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
      AUTH(),
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
      AUTH(),
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

// Codex-found P1 (re-review of the P1 fix): /admin/register-table used to
// forward body.partitionKeyUnique straight into table_rules.partition_key_unique
// with zero verification, letting a caller bypass checkPartitionKeyUnique
// entirely by just asserting the flag. The Worker's handleAdminRegisterTable
// must never read that field off the raw request — it computes it itself,
// the same way /admin/create-table and /admin/set-partition-key-column do.
describe("Worker /admin/register-table partitionKeyUnique trust bypass (Codex P1 fix)", () => {
  /** Reads table_rules.partition_key_unique straight off catalog-0's own
   * storage — /admin/register-table's response body doesn't echo the computed
   * flag, so this is the only way to assert what actually landed. */
  async function readPartitionKeyUnique(table: string): Promise<number | undefined> {
    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    return runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT partition_key_unique FROM table_rules WHERE table_name = ?", table),
      ) as Array<{ partition_key_unique: number }>;
      return rows[0]?.partition_key_unique;
    });
  }

  it("ignores a smuggled partitionKeyUnique: true when the column has no real unique constraint — computes 0 and still 409s /v1/table-scan", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "register_bypass_notuniq_evt";
    const createRes = await post(
      "/admin/create-table",
      {
        table,
        schema: `CREATE TABLE IF NOT EXISTS ${table} (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, v TEXT)`,
        partitionKeyColumn: "id",
      },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    // Re-register the same physical table, this time declaring partitionKeyColumn
    // as the NON-unique "user_id" column, and smuggling partitionKeyUnique: true
    // in the request body — this must be silently ignored, not trusted.
    const registerRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "user_id", partitionKeyUnique: true },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);

    expect(await readPartitionKeyUnique(table)).toBe(0);

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const scanRes = await post("/v1/table-scan", { tenantId, table }, token);
    expect(scanRes.status).toBe(409);
    const scanBody = (await scanRes.json()) as { error: { code: string } };
    expect(scanBody.error.code).toBe("PARTITION_KEY_NOT_UNIQUE");
  });

  it("computes partitionKeyUnique: 1 for a genuinely unique column with no client-supplied field at all", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "register_bypass_uniq_evt";
    const createRes = await post(
      "/admin/create-table",
      {
        table,
        schema: `CREATE TABLE IF NOT EXISTS ${table} (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT UNIQUE, v TEXT)`,
        partitionKeyColumn: "id",
      },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    // Re-register declaring partitionKeyColumn as the genuinely-UNIQUE
    // "user_id" column — no partitionKeyUnique field supplied at all.
    const registerRes = await post("/admin/register-table", { table, partitionKeyColumn: "user_id" }, AUTH());
    expect(registerRes.status).toBe(200);

    expect(await readPartitionKeyUnique(table)).toBe(1);

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const scanRes = await post("/v1/table-scan", { tenantId, table }, token);
    expect(scanRes.status).toBe(200);
  });
});

// Coverage gap: catalog.ts's handleRegisterTable computes
// `existing?.provenance_complete === 1 || body.provenanceComplete === true`
// specifically so that re-registering an already-complete table (e.g. a
// manual /admin/register-table call, which never sends provenanceComplete
// itself) doesn't silently reset provenance_complete back to 0 via the
// INSERT OR REPLACE below it. That monotonic-preserve behavior had zero test
// coverage anywhere in the suite.
describe("Worker /admin/register-table preserves provenance_complete across re-registration (catalog.ts monotonic fix)", () => {
  async function readProvenanceComplete(table: string): Promise<number | undefined> {
    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    return runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT provenance_complete FROM table_rules WHERE table_name = ?", table),
      ) as Array<{ provenance_complete: number }>;
      return rows[0]?.provenance_complete;
    });
  }

  it("keeps provenance_complete=1 after /admin/register-table is called again for an already-complete table, instead of resetting it to 0", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "reregister_provdone_evt";
    // /admin/create-table sets provenanceComplete: true fresh at creation.
    const createRes = await post(
      "/admin/create-table",
      { table, schema: `CREATE TABLE IF NOT EXISTS ${table} (id TEXT PRIMARY KEY, v TEXT)`, partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    expect(await readProvenanceComplete(table)).toBe(1);

    // A manual re-registration via /admin/register-table never sends
    // provenanceComplete at all -- only the monotonic "preserve if already
    // complete" logic in catalog.ts can be responsible for it staying 1.
    const registerRes = await post("/admin/register-table", { table, partitionKeyColumn: "id" }, AUTH());
    expect(registerRes.status).toBe(200);
    expect(await readProvenanceComplete(table)).toBe(1);
  });

  it("leaves provenance_complete=0 after re-registering a table that was never marked complete (no false positive from the preserve logic)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "reregister_provpending_evt";
    const createRes = await post(
      "/admin/create-table",
      { table, schema: `CREATE TABLE IF NOT EXISTS ${table} (id TEXT PRIMARY KEY, v TEXT)`, partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    // Simulate a legacy table that predates completeness tracking.
    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE table_rules SET provenance_complete = 0 WHERE table_name = ?", table);
    });
    expect(await readProvenanceComplete(table)).toBe(0);

    const registerRes = await post("/admin/register-table", { table, partitionKeyColumn: "id" }, AUTH());
    expect(registerRes.status).toBe(200);
    expect(await readProvenanceComplete(table)).toBe(0);
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
    // /v1/sql is admin-only now, so pass the admin token to get PAST auth and
    // reach the body parse — the point of this test is that a malformed body is
    // a clean 500, not an unhandled crash.
    const res = await SELF.fetch("https://worker.internal/v1/sql", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${env.ADMIN_TOKEN}` },
      body: "{not valid json",
    });
    expect(res.status).toBe(500);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("Internal error.");
  });
});

describe("Worker /v1/sql input validation", () => {
  it("returns 400 for missing sql/table/tenantId", async () => {
    const res = await post("/v1/sql", { table: "events" }, AUTH());
    expect(res.status).toBe(400);
  });

  it("returns 400 when params is not an array", async () => {
    await initCluster();
    const res = await post(
      "/v1/sql",
      { sql: "SELECT * FROM events", table: "events", tenantId: "t1", partitionKey: "p1", params: "not-an-array" },
      AUTH(),
    );
    expect(res.status).toBe(400);
  });

  it("returns 400 for a mutating statement without partitionKey", async () => {
    await initCluster();
    const res = await post(
      "/v1/sql",
      { sql: "INSERT INTO events (id, v) VALUES ('1','a')", table: "events", tenantId: "t1" },
      AUTH(),
    );
    expect(res.status).toBe(400);
  });

  it("returns 400 for a SELECT without partitionKey (must use /v1/scatter)", async () => {
    await initCluster();
    const res = await post(
      "/v1/sql",
      { sql: "SELECT * FROM events", table: "events", tenantId: "t1" },
      AUTH(),
    );
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
        AUTH(),
      );
    }
    const res = await post("/v1/scatter", { sql: "SELECT id FROM events", limit: 2 }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: unknown[] };
    expect(body.rows.length).toBeLessThanOrEqual(2);
  });
});
