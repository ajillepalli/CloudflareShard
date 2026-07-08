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
