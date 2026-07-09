import { SELF, env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { hashKey } from "./hash";
import type { CatalogDO } from "./catalog";

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
    { table: "events", schema: "CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, v TEXT)" },
    AUTH(),
  );
  expect(createRes.status).toBe(200);
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
      const res = await post("/v1/sql", {
        sql: "INSERT INTO events (id, v) VALUES (?, ?)",
        params: [`row-${tenantId}`, "x"],
        table: "events",
        tenantId,
        partitionKey: "p1",
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as { route: { catalogShardId: string; shardId: string } };
      routed.set(tenantId, body.route.catalogShardId);
    }
    const distinctCatalogs = new Set(routed.values());
    expect(distinctCatalogs.size).toBeGreaterThan(1);
  });

  it("/v1/scatter merges shard lists across all catalog shards", async () => {
    await initCluster(2, 16);
    const res = await post("/v1/scatter", { sql: "SELECT 1" });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { observability: { shardCount: number } };
    // 2 shards per catalog x >=2 catalog shards
    expect(body.observability.shardCount).toBeGreaterThanOrEqual(4);
  });

  it("/admin/create-table rejects a non-CREATE-TABLE schema", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post(
      "/admin/create-table",
      { table: "events", schema: "DROP TABLE events" },
      AUTH(),
    );
    expect(res.status).toBe(400);
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
      { table: "events", schema: "CREATE TABLE events (id TEXT PRIMARY KEY); DROP TABLE events" },
      AUTH(),
    );
    expect(res.status).toBe(403);
  });

  it("/admin/create-table rejects a schema containing a banned keyword like ATTACH", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res = await post(
      "/admin/create-table",
      { table: "events", schema: "CREATE TABLE events (id TEXT PRIMARY KEY) attach database 'x' as y" },
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
      { table: "mismatch_regression_evt", schema: "CREATE TABLE mismatch_regression_orders (id TEXT PRIMARY KEY)" },
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
      { table: "quoted_regression_evt", schema: 'CREATE TABLE IF NOT EXISTS "quoted_regression_evt" (id TEXT PRIMARY KEY)' },
      AUTH(),
    );
    expect(res.status).toBe(200);
  });

  it("/admin/create-table reports a shard-level failure when the schema fails to apply", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    // Missing PRIMARY KEY column type makes this a syntactically invalid CREATE TABLE.
    const res = await post(
      "/admin/create-table",
      { table: "broken", schema: "CREATE TABLE broken (id PRIMARY" },
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
    const res = await post("/v1/sql", {
      sql: "SELECT 1",
      table: "events",
      tenantId,
      partitionKey: "p1",
    });
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
  it("returns 400 for missing sql", async () => {
    const res = await post("/v1/scatter", {});
    expect(res.status).toBe(400);
  });

  it("returns 400 for a mutating statement", async () => {
    const res = await post("/v1/scatter", { sql: "INSERT INTO events (id) VALUES ('1')" });
    expect(res.status).toBe(400);
  });

  it("regression: rejects a comment-prefixed mutation instead of executing it as a read", async () => {
    await initCluster();
    const res = await post("/v1/scatter", { sql: "-- harmless\nDELETE FROM events" });
    expect(res.status).toBe(400);

    const res2 = await post("/v1/scatter", { sql: "/*x*/ UPDATE events SET v = 'pwned'" });
    expect(res2.status).toBe(400);
  });

  it("returns 403 for a dangerous non-mutation statement", async () => {
    // PRAGMA isn't classified as a mutation prefix, so it reaches the
    // isDangerous() deny-list check rather than the mutation-rejection check.
    const res = await post("/v1/scatter", { sql: "PRAGMA table_info(events)" });
    expect(res.status).toBe(403);
  });

  it("returns 400 when params is not an array", async () => {
    const res = await post("/v1/scatter", { sql: "SELECT 1", params: "nope" });
    expect(res.status).toBe(400);
  });

  it("caps results at the requested limit", async () => {
    await initCluster(1, 64);
    for (let i = 0; i < 5; i += 1) {
      await post("/v1/sql", {
        sql: "INSERT INTO events (id, v) VALUES (?, ?)",
        params: [`id-${i}`, "x"],
        table: "events",
        tenantId: `tenant-${i}`,
        partitionKey: "p1",
      });
    }
    const res = await post("/v1/scatter", { sql: "SELECT id FROM events", limit: 2 });
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: unknown[] };
    expect(body.rows.length).toBeLessThanOrEqual(2);
  });
});
