import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { CatalogDO } from "./catalog";

describe("CatalogDO audit log", () => {
  it("records init, register-table, split-vbucket, and drain-shard as audit entries", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events" }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));

    const res = await stub.fetch(post("/audit-log", {}, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { entries: Array<{ endpoint: string }> };
    const endpoints = body.entries.map((e) => e.endpoint);
    expect(endpoints).toEqual(expect.arrayContaining(["/init", "/register-table", "/drain-shard"]));
  });

  it("requires an admin token to read the audit log", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/audit-log", {}));
    expect(res.status).toBe(401);
  });
});

describe("CatalogDO error boundary", () => {
  it("returns a clean 500 instead of an unhandled crash on malformed JSON", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(
      new Request("https://catalog.internal/init", {
        method: "POST",
        headers: { "content-type": "application/json", authorization: `Bearer ${env.ADMIN_TOKEN}` },
        body: "{not valid json",
      }),
    );
    expect(res.status).toBe(500);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("Internal error.");
  });
});

describe("CatalogDO schema migration", () => {
  it("adds catalog_shard_id/catalog_shard_count columns to a pre-existing cluster_config table missing them", async () => {
    const stub = await freshCatalog();

    // Simulate a DO provisioned before these columns existed: create the
    // old-shape table directly, bypassing ensureSchema().
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      sql.exec(`
        CREATE TABLE cluster_config (
          singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
          total_vbuckets INTEGER NOT NULL,
          metadata_version INTEGER NOT NULL DEFAULT 1,
          initialized_at TEXT NOT NULL
        )
      `);
    });

    // Any request runs ensureSchema(), which should now backfill the missing columns.
    const res = await stub.fetch(post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }));
    expect(res.status).toBe(400); // cluster not initialized — but no crash from the migration

    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const columns = Array.from(state.storage.sql.exec("PRAGMA table_info(cluster_config)")) as Array<{
        name: string;
      }>;
      const names = columns.map((c) => c.name);
      expect(names).toContain("catalog_shard_id");
      expect(names).toContain("catalog_shard_count");
    });
  });
});

async function freshCatalog() {
  const id = env.CATALOG.idFromName(`catalog-${crypto.randomUUID()}`);
  const stub = env.CATALOG.get(id);
  return stub;
}

function post(path: string, body: unknown, authorization?: string) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (authorization) headers.authorization = authorization;
  return new Request(`https://catalog.internal${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
}

describe("CatalogDO auth gate", () => {
  it("rejects /init without an admin token when ADMIN_TOKEN is configured", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 8 }));
    expect(res.status).toBe(401);
  });

  it("rejects /split-vbucket without an admin token", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/split-vbucket", { vbucket: 0 }));
    expect(res.status).toBe(401);
  });

  it("rejects /register-table without an admin token", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/register-table", { table: "events" }));
    expect(res.status).toBe(401);
  });

  it("allows /init with the correct admin token", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(
      post("/init", { numShards: 2, totalVBuckets: 8 }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean };
    expect(body.ok).toBe(true);
  });
});

describe("CatalogDO split-vbucket", () => {
  it("reassigns a vbucket to a new shard and the new mapping is used for routing", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 8 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events" }, `Bearer ${env.ADMIN_TOKEN}`));

    const routeBefore = (await (
      await stub.fetch(post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }))
    ).json()) as { vbucket: number; shardId: string };

    const splitRes = await stub.fetch(
      post(
        "/split-vbucket",
        { vbucket: routeBefore.vbucket, newShardId: "shard-new" },
        `Bearer ${env.ADMIN_TOKEN}`,
      ),
    );
    expect(splitRes.status).toBe(200);
    const splitBody = (await splitRes.json()) as { ok: boolean; fromShard: string; toShard: string };
    expect(splitBody.ok).toBe(true);
    expect(splitBody.toShard).toBe("shard-new");
    expect(splitBody.fromShard).toBe(routeBefore.shardId);

    const routeAfter = (await (
      await stub.fetch(post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }))
    ).json()) as { shardId: string };
    expect(routeAfter.shardId).toBe("shard-new");
  });

  it("returns 400 for a negative or non-integer vbucket", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    const res = await stub.fetch(post("/split-vbucket", { vbucket: -1 }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(400);
  });

  it("returns 404 when the vbucket has no existing mapping", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    const res = await stub.fetch(post("/split-vbucket", { vbucket: 9999 }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(404);
  });
});

describe("CatalogDO input validation and lifecycle", () => {
  it("returns 400 for /register-table with a missing table name", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/register-table", {}, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(400);
  });

  it("returns 400 for /route with missing fields", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/route", { table: "events" }));
    expect(res.status).toBe(400);
  });

  it("returns 400 for /drain-shard with a missing shardId", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/drain-shard", {}, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(400);
  });

  it("returns 404 for /drain-shard on an unknown shardId", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    const res = await stub.fetch(post("/drain-shard", { shardId: "does-not-exist" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(404);
  });

  it("/init is a no-op when already initialized and force is not set", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 64 }, `Bearer ${env.ADMIN_TOKEN}`));
    const res = await stub.fetch(post("/init", { numShards: 5, totalVBuckets: 999 }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { alreadyInitialized: boolean; totalVBuckets: number };
    expect(body.alreadyInitialized).toBe(true);
    expect(body.totalVBuckets).toBe(64);
  });

  it("/init clamps numShards/totalVBuckets to a safe range instead of allowing an unbounded cluster", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(
      post("/init", { numShards: 100000, totalVBuckets: 10000000, force: true }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { numShards: number; totalVBuckets: number };
    expect(body.numShards).toBe(256);
    expect(body.totalVBuckets).toBe(65536);
  });

  it("/init rejects a non-numeric numShards instead of silently corrupting the cluster with shard-NaN (regression: Math.max/min propagate NaN)", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(
      post("/init", { numShards: "not-a-number", totalVBuckets: 64, force: true }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toContain("numShards");
  });

  it("/init rejects a non-numeric totalVBuckets", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(
      post("/init", { numShards: 2, totalVBuckets: "lots", force: true }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toContain("totalVBuckets");
  });

  it("/init with force:true wipes existing shard/vbucket state and re-initializes", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 64 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events" }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));

    const res = await stub.fetch(
      post("/init", { numShards: 3, totalVBuckets: 128, force: true }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    expect(res.status).toBe(200);

    const status = (await (await stub.fetch(post("/status", {}, `Bearer ${env.ADMIN_TOKEN}`))).json()) as {
      totalVBuckets: number;
      shards: { total: number; active: number; draining: number };
    };
    expect(status.totalVBuckets).toBe(128);
    expect(status.shards.total).toBe(3);
    expect(status.shards.draining).toBe(0); // the drained shard-0 was wiped, not carried over
  });

  it("/status reports uninitialized before /init is called", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/status", {}, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { initialized: boolean };
    expect(body.initialized).toBe(false);
  });

  it("/list-tables returns registered tables", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/register-table", { table: "orders", partitioning: "hash" }, `Bearer ${env.ADMIN_TOKEN}`));
    const res = await stub.fetch(post("/list-tables", {}, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { tables: Array<{ table_name: string }> };
    expect(body.tables.map((t) => t.table_name)).toContain("orders");
  });

  it("rejects non-POST methods with 405", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(new Request("https://catalog.internal/status", { method: "GET" }));
    expect(res.status).toBe(405);
  });

  it("returns 404 for an unknown catalog route", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/not-a-real-route", {}));
    expect(res.status).toBe(404);
  });
});

describe("CatalogDO routing", () => {
  it("returns 400 when routing before /init", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(
      post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }),
    );
    expect(res.status).toBe(400);
  });

  it("routes deterministically to the same shard for the same key", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 4, totalVBuckets: 64 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(
      post("/register-table", { table: "events" }, `Bearer ${env.ADMIN_TOKEN}`),
    );

    const body = { table: "events", tenantId: "t1", partitionKey: "user-1" };
    const first = await (await stub.fetch(post("/route", body))).json();
    const second = await (await stub.fetch(post("/route", body))).json();
    expect(first).toEqual(second);
  });

  it("returns 400 when the table is not registered", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 8 }, `Bearer ${env.ADMIN_TOKEN}`));

    const res = await stub.fetch(
      post("/route", { table: "unregistered", tenantId: "t1", partitionKey: "p1" }),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toContain("not registered");
  });

  it("returns 503 when the mapped shard is draining", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(
      post("/register-table", { table: "events" }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    await stub.fetch(
      post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`),
    );

    const res = await stub.fetch(
      post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }),
    );
    expect(res.status).toBe(503);
  });
});
