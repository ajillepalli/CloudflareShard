import { SELF, env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { hashKey, indexShardIdForKey } from "./hash";
import { sha256Hex } from "./auth";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";
import { ALL_TEST_SHARD_IDS, AUTH, initCluster, post, registerTenant, shardExecute, tenantForCatalogShard } from "./index.test-helpers";

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

    // Force EVERY catalog shard back to the sentinel to simulate a
    // pre-Chunk-1 table. A real pre-migration table would carry the sentinel
    // on all catalog shards alike (table_rules is fanned out identically to
    // each of them at /admin/create-table time) — resetting only catalog-0
    // would leave catalog-1/2/3 still holding the real "id" value from
    // create-table above, and since PR review round 6 made
    // /admin/set-partition-key-column a one-time unset->set upgrade, the
    // fan-out below would then get rejected 409 PARTITION_KEY_ALREADY_SET on
    // those three shards.
    const tenantId = tenantForCatalogShard(0, 4);
    for (let i = 0; i < 4; i += 1) {
      const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
      await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec(
          "UPDATE table_rules SET partition_key_column = '__unset__' WHERE table_name = 'upgrade_me'",
        );
      });
    }

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

  // PR review round 6: re-invoking this endpoint on a table that already has
  // a real (non-sentinel) partition_key_column used to silently repoint
  // table_rules, leaving __cf_row_owners' existing entries keyed under the
  // OLD column's values — a stale-provenance cross-tenant leak surfaced via
  // /tenant-scan-page. The endpoint is now a strictly one-time unset->set
  // upgrade: a second call must be rejected 409 PARTITION_KEY_ALREADY_SET and
  // must leave table_rules untouched.
  it("rejects a second call against a table whose partition key column was already set via /admin/create-table, leaving table_rules unchanged", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      {
        table: "already_set_via_create",
        schema: "CREATE TABLE IF NOT EXISTS already_set_via_create (id TEXT PRIMARY KEY, email TEXT, v TEXT)",
        partitionKeyColumn: "id",
      },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    const res = await post(
      "/admin/set-partition-key-column",
      { table: "already_set_via_create", partitionKeyColumn: "email" },
      AUTH(),
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { details: { error: { code: string } } };
    expect(body.details.error.code).toBe("PARTITION_KEY_ALREADY_SET");

    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const partitionKeyColumn = await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT partition_key_column FROM table_rules WHERE table_name = ?", "already_set_via_create"),
      ) as Array<{ partition_key_column: string }>;
      return rows[0]?.partition_key_column;
    });
    expect(partitionKeyColumn).toBe("id");
  });

  it("rejects a second call against a table whose partition key column was already set via a prior /admin/set-partition-key-column upgrade, leaving table_rules unchanged", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      {
        table: "already_set_via_upgrade",
        schema: "CREATE TABLE IF NOT EXISTS already_set_via_upgrade (id TEXT PRIMARY KEY, email TEXT, v TEXT)",
        partitionKeyColumn: "id",
      },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    // Simulate a legacy sentinel-tagged table (on every catalog shard — a
    // real one would carry the sentinel on all of them alike) and legitimately
    // upgrade it once via /admin/set-partition-key-column.
    for (let i = 0; i < 4; i += 1) {
      const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
      await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec(
          "UPDATE table_rules SET partition_key_column = '__unset__' WHERE table_name = 'already_set_via_upgrade'",
        );
      });
    }
    const upgradeRes = await post(
      "/admin/set-partition-key-column",
      { table: "already_set_via_upgrade", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(upgradeRes.status).toBe(200);

    // A SECOND call — even to a different column — must now be rejected.
    const res = await post(
      "/admin/set-partition-key-column",
      { table: "already_set_via_upgrade", partitionKeyColumn: "email" },
      AUTH(),
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { details: { error: { code: string } } };
    expect(body.details.error.code).toBe("PARTITION_KEY_ALREADY_SET");

    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const partitionKeyColumn = await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT partition_key_column FROM table_rules WHERE table_name = ?", "already_set_via_upgrade"),
      ) as Array<{ partition_key_column: string }>;
      return rows[0]?.partition_key_column;
    });
    expect(partitionKeyColumn).toBe("id");
  });

  // PR review round 11 (P1+P2 fundamental fix), test #5: this route's own
  // live-shard probe is the OTHER place (besides /admin/register-table's) a
  // table's partition_key_unique can flip to 1 independently of whatever
  // schema_sql happens to be on file — it must null schema_sql in the SAME
  // operation whenever it verifies uniqueness, since it can't guarantee the
  // stored text corresponds to what it just checked live.
  it("nulls out a previously-stored schema_sql when this call's own probe verifies partition_key_unique=1", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "setpkcol_nulls_schemasql_evt";
    const originalSchema = `CREATE TABLE IF NOT EXISTS ${table} (id TEXT PRIMARY KEY, v TEXT)`;
    const createRes = await post(
      "/admin/create-table",
      { table, schema: originalSchema, partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(1);
    expect(await readTableRulesSchemaSql(table)).toBe(originalSchema);

    // Overwrite schema_sql via /admin/register-table (hasSchemaSql=true —
    // demotes partition_key_unique to 0, per round 10's unaffected behavior)
    // so this test can prove the FOLLOW-UP set-partition-key-column call
    // nulls it out rather than merely leaving it at its original value.
    const overwrittenSchema = `CREATE TABLE ${table} (id TEXT, v TEXT, extra TEXT)`;
    const overwriteRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: overwrittenSchema },
      AUTH(),
    );
    expect(overwriteRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(0);
    expect(await readTableRulesSchemaSql(table)).toBe(overwrittenSchema);

    // /admin/set-partition-key-column is a one-time '__unset__'-sentinel
    // upgrade path only — reset to the sentinel so this call isn't rejected
    // 409 PARTITION_KEY_ALREADY_SET before reaching the verification below.
    await resetPartitionKeyColumnToSentinel(table, 4);

    // The live shard's REAL schema (from /admin/create-table above) still has
    // a genuine unique constraint on "id" — the probe verifies true.
    const setRes = await post("/admin/set-partition-key-column", { table, partitionKeyColumn: "id" }, AUTH());
    expect(setRes.status).toBe(200);

    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(1);
    // The fix: schema_sql is NULL now, not the stale overwrittenSchema this
    // call's own probe has no way to vouch for.
    expect(await readTableRulesSchemaSql(table)).toBeNull();
  });
});

/** Reads a single table_rules boolean-flag column straight off catalog-0's own
 * storage, keyed by table_name — used by tests below where the route's own
 * response body doesn't echo the computed flag, so this is the only way to
 * assert what actually landed. Column is restricted to the known flag columns
 * (never client/test-supplied free text) since column names can't be
 * parameterized in SQL. */
async function readTableRulesColumn(
  table: string,
  column: "partition_key_unique" | "provenance_complete",
): Promise<number | undefined> {
  const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
  return runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
    const rows = Array.from(
      state.storage.sql.exec(`SELECT ${column} FROM table_rules WHERE table_name = ?`, table),
    ) as Array<Record<string, number>>;
    return rows[0]?.[column];
  });
}

/** Reads table_rules.partition_key_column straight off catalog-0's own
 * storage, keyed by table_name — used to verify a rejected repoint left
 * table_rules genuinely unchanged, not just that the HTTP response was a
 * 409. */
async function readTableRulesPartitionKeyColumn(table: string): Promise<string | undefined> {
  const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
  return runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
    const rows = Array.from(
      state.storage.sql.exec("SELECT partition_key_column FROM table_rules WHERE table_name = ?", table),
    ) as Array<{ partition_key_column: string }>;
    return rows[0]?.partition_key_column;
  });
}

/** Resets `table_rules.partition_key_column` back to the '__unset__' sentinel
 * across every catalog shard for `table` — used by tests that intentionally
 * re-register a table under a DIFFERENT partitionKeyColumn to exercise some
 * OTHER behavior entirely (e.g. the partitionKeyUnique trust-bypass fix
 * below), where PR review round 7's /admin/register-table repoint guard
 * would otherwise reject the second call with 409 PARTITION_KEY_ALREADY_SET
 * before the test ever reaches what it's actually checking. Mirrors the
 * inline reset already used by the /admin/set-partition-key-column upgrade
 * test above. */
async function resetPartitionKeyColumnToSentinel(table: string, numCatalogShards: number): Promise<void> {
  for (let i = 0; i < numCatalogShards; i += 1) {
    const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE table_rules SET partition_key_column = '__unset__' WHERE table_name = ?", table);
    });
  }
}

// Codex-found P1 (re-review of the P1 fix): /admin/register-table used to
// forward body.partitionKeyUnique straight into table_rules.partition_key_unique
// with zero verification, letting a caller bypass checkPartitionKeyUnique
// entirely by just asserting the flag. The Worker's handleAdminRegisterTable
// must never read that field off the raw request — it computes it itself,
// the same way /admin/create-table and /admin/set-partition-key-column do.
describe("Worker /admin/register-table partitionKeyUnique trust bypass (Codex P1 fix)", () => {
  async function readPartitionKeyUnique(table: string): Promise<number | undefined> {
    return readTableRulesColumn(table, "partition_key_unique");
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

    // This test's actual target is partitionKeyUnique trust, not the repoint
    // guard — reset to the sentinel first so re-registering under a
    // DIFFERENT partitionKeyColumn ("user_id" instead of "id") isn't itself
    // rejected 409 PARTITION_KEY_ALREADY_SET by PR review round 7's guard.
    await resetPartitionKeyColumnToSentinel(table, 4);

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

    // This test's actual target is partitionKeyUnique computation, not the
    // repoint guard — reset to the sentinel first so re-registering under a
    // DIFFERENT partitionKeyColumn ("user_id" instead of "id") isn't itself
    // rejected 409 PARTITION_KEY_ALREADY_SET by PR review round 7's guard.
    await resetPartitionKeyColumnToSentinel(table, 4);

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

  // Fix (P2, provenance trust gap found by Codex structured review):
  // /admin/register-table already strips a client-supplied
  // partitionKeyUnique (tested above) but used to forward provenanceComplete
  // as-is -- a caller could claim {"provenanceComplete": true} for a table
  // that never actually had a backfill run, making /v1/table-scan's
  // provenance.complete field falsely report no legacy unattributed rows are
  // hidden.
  it("ignores a smuggled provenanceComplete: true for a table that was never actually backfilled — computes 0, not 1", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "register_bypass_provenance_evt";

    const registerRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", provenanceComplete: true },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);

    expect(await readTableRulesColumn(table, "provenance_complete")).toBe(0);
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
    return readTableRulesColumn(table, "provenance_complete");
  }

  it("keeps provenance_complete=1 after /admin/register-table is called again for an already-complete table, instead of resetting it to 0", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "reregister_provdone_evt";
    const createRes = await post(
      "/admin/create-table",
      { table, schema: `CREATE TABLE IF NOT EXISTS ${table} (id TEXT PRIMARY KEY, v TEXT)`, partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    // PR review round 11 (P2 fix): /admin/create-table no longer
    // auto-certifies provenance_complete=1 at creation (see this file's
    // "Worker /admin/create-table no longer auto-certifies provenance_complete"
    // describe block below) — a brand-new table now requires the normal
    // /admin/backfill-provenance certification step like any other table.
    // This test's actual target is the MONOTONIC PRESERVE behavior on
    // re-registration, not create-table's own certification, so simulate an
    // already-completed table the same way a real /admin/backfill-provenance
    // run would leave it, directly via DO storage.
    expect(await readProvenanceComplete(table)).toBe(0);
    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE table_rules SET provenance_complete = 1 WHERE table_name = ?", table);
    });
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

// PR review round 11 (P2 fix): /admin/create-table used to set
// provenanceComplete: true unconditionally when registering the table it
// just created via CREATE TABLE IF NOT EXISTS — wrong when the table name
// already physically existed (legacy rows predating row-provenance
// tracking): the DDL silently no-ops, but the table got certified
// provenance-complete anyway, hiding those legacy rows' absence from
// __cf_row_owners behind a false `provenance.complete: true` on
// /v1/table-scan. Fixed: create-table no longer auto-sets it — a newly
// created table starts at 0 like any other table and earns certification
// through the normal /admin/backfill-provenance mechanism (trivially, for a
// genuinely brand-new/empty table, since there's nothing to find).
describe("Worker /admin/create-table no longer auto-certifies provenance_complete (PR review round 11 fix)", () => {
  it("a genuinely brand-new table's provenance_complete is 0 immediately after /admin/create-table, not auto-certified 1", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "create_table_no_autocert_evt";
    const createRes = await post(
      "/admin/create-table",
      { table, schema: `CREATE TABLE IF NOT EXISTS ${table} (id TEXT PRIMARY KEY, v TEXT)`, partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "provenance_complete")).toBe(0);
  });

  it("a brand-new (empty) table's first full-cluster /admin/backfill-provenance run certifies it complete — same end state as the old auto-certify, one extra (cheap, no-op-ish) admin call", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "create_table_backfill_certifies_evt";
    const createRes = await post(
      "/admin/create-table",
      { table, schema: `CREATE TABLE IF NOT EXISTS ${table} (id TEXT PRIMARY KEY, v TEXT)`, partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "provenance_complete")).toBe(0);

    // Full-cluster run (catalogShardId omitted): the table is empty, so
    // there's nothing orphaned/ambiguous to find, and it's certified complete
    // through the normal mechanism, same as any other table.
    const backfillRes = await post("/admin/backfill-provenance", {}, AUTH());
    expect(backfillRes.status).toBe(200);
    const backfillBody = (await backfillRes.json()) as { orphaned: unknown[]; ambiguous: unknown[] };
    expect(backfillBody.orphaned).toHaveLength(0);
    expect(backfillBody.ambiguous).toHaveLength(0);

    expect(await readTableRulesColumn(table, "provenance_complete")).toBe(1);
  });
});

// PR review round 7: /admin/register-table's INSERT OR REPLACE used to take
// body.partitionKeyColumn unconditionally, silently repointing an
// already-configured table's partition_key_column to a different value —
// the SAME __cf_row_owners stale-provenance / cross-tenant leak round 6
// closed for /admin/set-partition-key-column (see that handler's comment),
// but reachable through this OTHER route since round 6's guard only lived in
// handleSetPartitionKeyColumn. handleRegisterTable now rejects a repoint the
// same way, while still allowing brand-new registrations, the sentinel
// upgrade path, and idempotent re-registration with the SAME value.
describe("Worker /admin/register-table rejects repointing an already-configured partition key column (PR review round 7 fix)", () => {
  it("allows re-registering a table with the SAME partitionKeyColumn it already has (idempotent re-registration)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "reregister_same_pkcol_evt";
    const createRes = await post(
      "/admin/create-table",
      { table, schema: `CREATE TABLE IF NOT EXISTS ${table} (id TEXT PRIMARY KEY, v TEXT)`, partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    const registerRes = await post("/admin/register-table", { table, partitionKeyColumn: "id" }, AUTH());
    expect(registerRes.status).toBe(200);

    expect(await readTableRulesPartitionKeyColumn(table)).toBe("id");
  });

  it("rejects re-registering a table with a DIFFERENT partitionKeyColumn than what's already set, leaving table_rules unchanged", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "reregister_diff_pkcol_evt";
    const createRes = await post(
      "/admin/create-table",
      {
        table,
        schema: `CREATE TABLE IF NOT EXISTS ${table} (id TEXT PRIMARY KEY, email TEXT, v TEXT)`,
        partitionKeyColumn: "id",
      },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    const registerRes = await post("/admin/register-table", { table, partitionKeyColumn: "email" }, AUTH());
    expect(registerRes.status).toBe(409);
    const body = (await registerRes.json()) as { details: { error: { code: string } } };
    expect(body.details.error.code).toBe("PARTITION_KEY_ALREADY_SET");

    const partitionKeyColumn = await readTableRulesPartitionKeyColumn(table);
    expect(partitionKeyColumn).toBe("id");
  });

  it("allows registering a brand-new table with no prior table_rules row", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "register_brand_new_evt";
    const registerRes = await post("/admin/register-table", { table, partitionKeyColumn: "id" }, AUTH());
    expect(registerRes.status).toBe(200);

    const partitionKeyColumn = await readTableRulesPartitionKeyColumn(table);
    expect(partitionKeyColumn).toBe("id");
  });

  it("allows upgrading a table still carrying the '__unset__' sentinel to a real partitionKeyColumn", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "register_sentinel_upgrade_evt";
    const createRes = await post(
      "/admin/create-table",
      { table, schema: `CREATE TABLE IF NOT EXISTS ${table} (id TEXT PRIMARY KEY, v TEXT)`, partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);

    await resetPartitionKeyColumnToSentinel(table, 4);
    expect(await readTableRulesPartitionKeyColumn(table)).toBe("__unset__");

    const registerRes = await post("/admin/register-table", { table, partitionKeyColumn: "id" }, AUTH());
    expect(registerRes.status).toBe(200);
    expect(await readTableRulesPartitionKeyColumn(table)).toBe("id");
  });
});

/** Reads table_rules.schema_sql straight off catalog-0's own storage, keyed by
 * table_name — used to verify what actually landed (including NULL), not
 * just that the HTTP response was a 200/409. */
async function readTableRulesSchemaSql(table: string): Promise<string | null | undefined> {
  const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
  return runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
    const rows = Array.from(
      state.storage.sql.exec("SELECT schema_sql FROM table_rules WHERE table_name = ?", table),
    ) as Array<{ schema_sql: string | null }>;
    return rows[0]?.schema_sql;
  });
}

// PR review round 11 (fundamental fix, replacing rounds 8/9's reject/preserve
// guard pair): table_rules.schema_sql is exactly what a future split/
// migration backfill executes verbatim to provision a table on a
// freshly-created target shard. It can only ever be trustworthy alongside a
// probe-verified partition_key_unique=1 when the two were established
// TOGETHER, atomically, by /admin/create-table's own push-then-verify flow —
// so /admin/register-table (whose own probe, when it runs at all, verifies
// against whatever ALREADY physically exists, completely disconnected from
// whatever schema_sql text this call ALSO submits) no longer tries to
// protect a stale pairing by rejecting a differing schema_sql once
// previously verified. Rounds 8/9's SCHEMA_SQL_ALREADY_VERIFIED rejection
// and omission-preserve fallback are gone: submitting a real schemaSql in
// this call ALWAYS demotes partition_key_unique to 0 (round 10's existing,
// unaffected behavior) and stores the submitted text as-is — there is no
// longer a stale-pairing risk to guard against, because the demotion itself
// closes it on every such call.
describe("Worker /admin/register-table: a real schemaSql always demotes partition_key_unique to 0 and stores as submitted, even overwriting an already-verified table (PR review round 11 fix)", () => {
  it("succeeds (200, not 409) overwriting a create-table-verified table's schema_sql with a DIFFERENT one, and demotes partition_key_unique to 0", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "reregister_diff_schemasql_evt";
    const originalSchema = `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`;
    const createRes = await post(
      "/admin/create-table",
      { table, schema: originalSchema, partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(1);
    expect(await readTableRulesSchemaSql(table)).toBe(originalSchema);

    // A schema that drops the PRIMARY KEY/UNIQUE constraint on "id" entirely.
    // Under rounds 8/9 this used to be rejected 409 SCHEMA_SQL_ALREADY_VERIFIED
    // to protect the (then-preserved) create-table-verified pairing. Under the
    // round-11 design there's nothing to protect: this call's own probe never
    // runs (schemaSql is present), so partition_key_unique demotes to 0 in the
    // SAME write that stores the new, differing text — never leaving a "1"
    // paired with unverified schema text.
    const driftedSchema = `CREATE TABLE ${table} (id TEXT, v TEXT)`;
    const registerRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: driftedSchema },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);

    expect(await readTableRulesSchemaSql(table)).toBe(driftedSchema);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(0);
  });

  it("re-registering a verified-unique table with the IDENTICAL schema_sql succeeds but demotes partition_key_unique to 0 (PR review round 10, unaffected)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "reregister_same_schemasql_evt";
    const originalSchema = `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`;
    const createRes = await post(
      "/admin/create-table",
      { table, schema: originalSchema, partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(1);

    const registerRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: originalSchema },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);
    expect(await readTableRulesSchemaSql(table)).toBe(originalSchema);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(0);
  });

  it("allows changing schema_sql for a table whose partition_key_unique is still 0 (unverified — no over-rejection)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "reregister_unverified_schemasql_evt";

    const firstSchema = `CREATE TABLE ${table} (id TEXT, v TEXT)`;
    const registerRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: firstSchema },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(0);
    expect(await readTableRulesSchemaSql(table)).toBe(firstSchema);

    const secondSchema = `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT, extra TEXT)`;
    const secondRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: secondSchema },
      AUTH(),
    );
    expect(secondRes.status).toBe(200);
    expect(await readTableRulesSchemaSql(table)).toBe(secondSchema);
  });

  it("allows registering a brand-new table with schema_sql set (first-ever registration, no prior row to conflict with)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "register_brandnew_schemasql_evt";
    const schema = `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`;

    const registerRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: schema },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);
    expect(await readTableRulesSchemaSql(table)).toBe(schema);
  });
});

// PR review round 11 finding 1 (closed): rounds 8/9's guards prevented an
// already-verified table's schema_sql from drifting away from the
// live-verified schema — but ONLY by comparing against a prior table_rules
// row's partition_key_unique. A LATER /admin/register-table call that OMITS
// schemaSql could still independently compute partitionKeyUnique=true from
// its OWN live probe, while schema_sql sat there untouched (round 9's
// preserve-on-omission fallback), holding whatever an EARLIER call had
// stored — recreating the exact partition_key_unique=1 +
// possibly-untrustworthy-schema_sql pairing round 10 closed for a SINGLE
// call, just via two calls instead of one. Fixed: whenever THIS call's own
// probe verifies partitionKeyUnique=true, schema_sql is explicitly nulled
// regardless of what's currently stored — this route's live-state check can
// never vouch for arbitrary stored text, so it must not be left in place.
describe("Worker /admin/register-table nulls a previously-stored schema_sql when a LATER call's own live probe verifies uniqueness (closes PR review round 11 finding 1)", () => {
  it("a first call stores a weak schema_sql (unverified); a later call OMITTING schemaSql verifies true via the live probe and nulls schema_sql instead of preserving the weak value", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "regreg_finding1_evt";

    // Physically create the table with a GENUINE unique constraint on "id"
    // (bypassing /admin/create-table and /admin/register-table entirely) —
    // simulating a table that already exists live with a real constraint.
    for (const shardId of ALL_TEST_SHARD_IDS) {
      await shardExecute(shardId, `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`);
    }

    // First call: register with a WEAK schemaSql present (doesn't preserve
    // the real constraint) — hasSchemaSql=true skips the probe entirely, so
    // this stores schema_sql=weakerSchema with partition_key_unique=0,
    // exactly round 10's existing (unaffected) behavior.
    const weakerSchema = `CREATE TABLE ${table} (id TEXT, v TEXT)`;
    const firstRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: weakerSchema },
      AUTH(),
    );
    expect(firstRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(0);
    expect(await readTableRulesSchemaSql(table)).toBe(weakerSchema);

    // Second call: OMITS schemaSql entirely (e.g. a metadata-only
    // re-registration). This route's own live probe now runs against the
    // REAL, genuinely-unique live schema and verifies true.
    const secondRes = await post("/admin/register-table", { table, partitionKeyColumn: "id" }, AUTH());
    expect(secondRes.status).toBe(200);

    // The fix: partition_key_unique becomes 1 (the probe is trustworthy and
    // allowed to run) BUT schema_sql is now NULL — not the previously-stored
    // weak value, which this call's own probe has no way to vouch for.
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(1);
    expect(await readTableRulesSchemaSql(table)).toBeNull();

    // And /v1/table-scan is now genuinely eligible.
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const scanRes = await post("/v1/table-scan", { tenantId, table }, token);
    expect(scanRes.status).toBe(200);
  });
});

// PR review round 11 finding 2 (closed): hasSchemaSql already treated an
// empty string identically to omitted for deciding whether the live probe
// runs (the `.length > 0` check) — but the empty string itself used to still
// ride along in the fanned-out payload and land in schema_sql as a garbage,
// non-null value (not a valid CREATE TABLE statement, but not null either).
// Fixed by scrubbing payload.schemaSql (delete or explicit null, matching
// finding 1's branches) whenever hasSchemaSql is false, whether that's
// because the field was omitted OR because it was an empty string.
describe('Worker /admin/register-table treats schemaSql: "" identically to omitted (closes PR review round 11 finding 2)', () => {
  it('registering with schemaSql: "" runs the live probe (not skipped) and, once it verifies true, ends up with schema_sql = NULL, not the empty string', async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "regreg_finding2_evt";

    for (const shardId of ALL_TEST_SHARD_IDS) {
      await shardExecute(shardId, `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`);
    }

    const registerRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: "" },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);

    // The probe ran (schemaSql: "" is treated as absent) and verified true
    // against the genuinely-unique live schema.
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(1);
    // The fix: schema_sql is NULL, never the empty string itself.
    expect(await readTableRulesSchemaSql(table)).toBeNull();

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const scanRes = await post("/v1/table-scan", { tenantId, table }, token);
    expect(scanRes.status).toBe(200);
  });

  it('registering with schemaSql: "" on a table whose live probe verifies FALSE simply omits schema_sql from the write (stays whatever was stored, no garbage "" value)', async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "regreg_finding2_notunique_evt";

    // No unique constraint on "id" this time — the probe will verify false.
    for (const shardId of ALL_TEST_SHARD_IDS) {
      await shardExecute(shardId, `CREATE TABLE ${table} (id TEXT, v TEXT)`);
    }

    const registerRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: "" },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);

    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(0);
    // No prior row existed, so "preserve whatever's stored" is still null —
    // critically, NOT the empty string that was submitted.
    expect(await readTableRulesSchemaSql(table)).toBeNull();
  });
});

// PR review round 10 (P1): rounds 8-9's guards above prevent an ALREADY-
// verified table's schema_sql from drifting away from the live-verified
// schema, but only fire when there's a prior table_rules row with
// partition_key_unique = 1 to compare against. They don't cover the FIRST
// registration event: handleAdminRegisterTable computes partitionKeyUnique
// by probing whatever ALREADY physically exists on a representative live
// shard right now — completely independent of whatever schemaSql the SAME
// request ALSO submits for storage (used later to provision split/migration
// targets). If a table physically exists somewhere with a genuinely unique
// constraint, but has never been registered before, an admin could register
// it for the first time with a schemaSql that's weaker than (and doesn't
// preserve) that live constraint: partition_key_unique would get computed
// as 1 (correctly reflecting the real live shard), while the STORED
// schema_sql doesn't actually carry the constraint a future split target
// needs. Fixed by never letting /admin/register-table produce a verified
// result when schemaSql is ALSO present in that same call — the live probe
// is skipped entirely and partition_key_unique always stores its false
// default in that case.
describe("Worker /admin/register-table with schemaSql never verifies partition_key_unique on that same call (PR review round 10 fix)", () => {
  it("stores partition_key_unique = 0 on first registration with schemaSql, even though the table physically exists elsewhere with a real unique constraint", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "register_schemasql_liveuniq_gap_evt";

    // Physically create the table directly on every shard (bypassing both
    // /admin/create-table and /admin/register-table entirely) with a
    // GENUINE unique constraint on "id" — simulating a table that already
    // exists live, but has never been registered in table_rules before.
    for (const shardId of ALL_TEST_SHARD_IDS) {
      await shardExecute(shardId, `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`);
    }

    // Register it for the FIRST time, submitting a schemaSql that's WEAKER
    // than the live schema — it omits the PRIMARY KEY/UNIQUE constraint on
    // "id" entirely. Before this fix, the live-shard probe would still
    // compute partitionKeyUnique = 1 (correctly reflecting the real,
    // already-unique live schema), while this weaker text got stored as
    // schema_sql for a future split target to provision from.
    const weakerSchema = `CREATE TABLE ${table} (id TEXT, v TEXT)`;
    const registerRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: weakerSchema },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);

    // The fix: partition_key_unique must be 0, not 1 — schemaSql being
    // present forces the route to skip the live probe entirely.
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(0);
    expect(await readTableRulesSchemaSql(table)).toBe(weakerSchema);

    // /v1/table-scan must therefore still refuse this table.
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const scanRes = await post("/v1/table-scan", { tenantId, table }, token);
    expect(scanRes.status).toBe(409);
    const scanBody = (await scanRes.json()) as { error: { code: string } };
    expect(scanBody.error.code).toBe("PARTITION_KEY_NOT_UNIQUE");
  });

  it("still computes partitionKeyUnique via the live-shard probe when schemaSql is OMITTED (no regression to the metadata-only path)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "register_noschemasql_liveuniq_evt";

    for (const shardId of ALL_TEST_SHARD_IDS) {
      await shardExecute(shardId, `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`);
    }

    // No schemaSql field at all — this is the pre-existing metadata-only
    // registration path, which must be unaffected by this fix.
    const registerRes = await post("/admin/register-table", { table, partitionKeyColumn: "id" }, AUTH());
    expect(registerRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(1);
  });

  it("allows a later /admin/set-partition-key-column call to verify a table registered with schemaSql (unverified=0), if the live schema is genuinely unique", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const table = "register_schemasql_followup_verify_evt";

    for (const shardId of ALL_TEST_SHARD_IDS) {
      await shardExecute(shardId, `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`);
    }

    const weakerSchema = `CREATE TABLE ${table} (id TEXT, v TEXT)`;
    const registerRes = await post(
      "/admin/register-table",
      { table, partitionKeyColumn: "id", schemaSql: weakerSchema },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(0);

    // /admin/set-partition-key-column is a one-time '__unset__'-sentinel
    // upgrade path only (PR review round 6) — reset back to the sentinel
    // first so this follow-up call isn't itself rejected 409
    // PARTITION_KEY_ALREADY_SET before ever reaching the re-verification
    // this test is actually about (same established pattern as the other
    // register-table tests above that exercise a second call for a
    // DIFFERENT reason than the repoint guard).
    await resetPartitionKeyColumnToSentinel(table, 4);

    // Follow-up call has no schemaSql field at all — it re-verifies fresh
    // against the live shard, independent of whatever schema_sql text is
    // stored, and is unaffected by this fix (it never had a probe-skip path
    // to begin with).
    const setRes = await post("/admin/set-partition-key-column", { table, partitionKeyColumn: "id" }, AUTH());
    expect(setRes.status).toBe(200);
    expect(await readTableRulesColumn(table, "partition_key_unique")).toBe(1);

    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const scanRes = await post("/v1/table-scan", { tenantId, table }, token);
    expect(scanRes.status).toBe(200);
  });
});

// Pre-landing review fix: /admin/register-table never validated payload.table
// or payload.partitionKeyColumn against IDENTIFIER_RE, unlike every sibling
// route (e.g. /admin/set-partition-key-column). An unvalidated table string
// reaches checkPartitionKeyUnique, which interpolates it directly into raw SQL
// text sent to a shard's /execute route — a real identifier-injection gap.
describe("Worker /admin/register-table validates identifiers (pre-landing review fix)", () => {
  it("rejects a table name that fails IDENTIFIER_RE with 400 UNSAFE_IDENTIFIER", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post("/admin/register-table", { table: `evil"; DROP TABLE x; --` }, AUTH());
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("UNSAFE_IDENTIFIER");
  });

  it("rejects a partitionKeyColumn that fails IDENTIFIER_RE with 400 UNSAFE_IDENTIFIER", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post(
      "/admin/register-table",
      { table: "register_bad_pkcol_evt", partitionKeyColumn: `id"); DROP TABLE x; --` },
      AUTH(),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("UNSAFE_IDENTIFIER");
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
