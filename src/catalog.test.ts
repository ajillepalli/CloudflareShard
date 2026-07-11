import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { CatalogDO } from "./catalog";
import { hashKey } from "./hash";

describe("CatalogDO audit log", () => {
  it("records init, register-table, split-vbucket, and drain-shard as audit entries", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));
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

describe("CatalogDO Milestone 2 index routes — validation branches (eng-review coverage gaps)", () => {
  it("/mark-index-ready rejects a missing indexName, 400", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/mark-index-ready", {}, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("MISSING_FIELDS");
  });

  it("/mark-index-ready rejects an unregistered indexName, 404", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/mark-index-ready", { indexName: "idx_ghost" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("INDEX_NOT_REGISTERED");
  });

  it("/drop-index rejects a missing indexName, 400", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/drop-index", {}, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("MISSING_FIELDS");
  });

  it("/lookup-index rejects a request missing table, indexName, or tenantId, 400", async () => {
    const stub = await freshCatalog();
    const token = await registerTenant(stub, "t1");
    const res = await stub.fetch(post("/lookup-index", { table: "events" }, token));
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("MISSING_FIELDS");
  });

  it("/create-index rejects an unsafe column identifier, 400", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));
    const res = await stub.fetch(
      post("/create-index", { indexName: "idx_bad_col", table: "events", columns: ["v; DROP TABLE events"] }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("UNSAFE_IDENTIFIER");
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
    const token = await registerTenant(stub, "t1");
    const res = await stub.fetch(post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, token));
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

async function registerTenant(stub: Awaited<ReturnType<typeof freshCatalog>>, tenantId: string): Promise<string> {
  const res = await stub.fetch(post("/register-tenant", { tenantId }, `Bearer ${env.ADMIN_TOKEN}`));
  const body = (await res.json()) as { token: string };
  return `Bearer ${body.token}`;
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
  // Milestone 3, Chunk 4 update: /split-vbucket no longer repoints
  // vbucket_map immediately (the pre-M3 behavior this test used to assert —
  // which stranded every row already on the source). It now creates the
  // target shard and starts a real migration; routing flips only when the
  // fenced cutover completes. Same request shape; response gains
  // migrationStarted: true.
  it("starts a migration to the new shard, and routing flips there once the migration's cutover completes", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 8 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));
    const token = await registerTenant(stub, "t1");

    const routeBefore = (await (
      await stub.fetch(post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, token))
    ).json()) as { vbucket: number; shardId: string };

    const splitRes = await stub.fetch(
      post(
        "/split-vbucket",
        { vbucket: routeBefore.vbucket, newShardId: "shard-new" },
        `Bearer ${env.ADMIN_TOKEN}`,
      ),
    );
    expect(splitRes.status).toBe(200);
    const splitBody = (await splitRes.json()) as { ok: boolean; fromShard: string; toShard: string; migrationStarted: boolean };
    expect(splitBody.ok).toBe(true);
    expect(splitBody.toShard).toBe("shard-new");
    expect(splitBody.fromShard).toBe(routeBefore.shardId);
    expect(splitBody.migrationStarted).toBe(true);

    // The map is NOT flipped yet — the source stays authoritative while the
    // migration runs.
    const routeDuring = (await (
      await stub.fetch(post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, token))
    ).json()) as { shardId: string; migrationStatus?: string; targetShardId?: string };
    expect(routeDuring.shardId).toBe(routeBefore.shardId);
    expect(routeDuring.migrationStatus).toBe("backfilling");
    expect(routeDuring.targetShardId).toBe("shard-new");

    // Drive the alarm-based orchestration to completion (backfill pass ->
    // cutover: fence, mirror drain, checksum, flip, unfence, source delete).
    for (let tick = 0; tick < 10; tick += 1) {
      await runInDurableObject(stub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const statusRes = await stub.fetch(post("/migrate-vbucket-status", { vbucket: routeBefore.vbucket }, `Bearer ${env.ADMIN_TOKEN}`));
      const statusBody = (await statusRes.json()) as { status: string };
      if (statusBody.status === "none") break;
    }

    const routeAfter = (await (
      await stub.fetch(post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, token))
    ).json()) as { shardId: string; migrationStatus?: string };
    expect(routeAfter.shardId).toBe("shard-new");
    expect(routeAfter.migrationStatus).toBeUndefined();
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

  it("Milestone 3, Chunk 2: splitting a vbucket while an index is registered now succeeds (SPLIT_BLOCKED_BY_INDEXES is removed — index placement hashes over each index's own pinned ring, unaffected by a new active shard)", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));
    const createIndexRes = await stub.fetch(
      post(
        "/create-index",
        { indexName: "idx_split_no_block_by_v", table: "events", columns: ["v"], placementRing: ["shard-0"] },
        `Bearer ${env.ADMIN_TOKEN}`,
      ),
    );
    expect(createIndexRes.status).toBe(200);

    const res = await stub.fetch(post("/split-vbucket", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean };
    expect(body.ok).toBe(true);

    // The index's pinned ring is untouched by the split.
    const listRes = await stub.fetch(post("/list-indexes", {}, `Bearer ${env.ADMIN_TOKEN}`));
    const listBody = (await listRes.json()) as { indexes: Array<{ indexName: string; placementRing: string[] }> };
    const idx = listBody.indexes.find((i) => i.indexName === "idx_split_no_block_by_v");
    expect(idx?.placementRing).toEqual(["shard-0"]);
  });
});

describe("CatalogDO input validation and lifecycle", () => {
  it("returns 400 for /register-table with a missing table name", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/register-table", {}, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(400);
  });

  it("returns 400 for /register-table with a missing partitionKeyColumn", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/register-table", { table: "events" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("MISSING_PARTITION_KEY_COLUMN");
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
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));
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
    await stub.fetch(
      post("/register-table", { table: "orders", partitioning: "hash", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`),
    );
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
    const token = await registerTenant(stub, "t1");
    const res = await stub.fetch(
      post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, token),
    );
    expect(res.status).toBe(400);
  });

  it("routes deterministically to the same shard for the same key", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 4, totalVBuckets: 64 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(
      post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    const token = await registerTenant(stub, "t1");

    const body = { table: "events", tenantId: "t1", partitionKey: "user-1" };
    const first = await (await stub.fetch(post("/route", body, token))).json();
    const second = await (await stub.fetch(post("/route", body, token))).json();
    expect(first).toEqual(second);
  });

  it("returns 400 when the table is not registered", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 8 }, `Bearer ${env.ADMIN_TOKEN}`));
    const token = await registerTenant(stub, "t1");

    const res = await stub.fetch(
      post("/route", { table: "unregistered", tenantId: "t1", partitionKey: "p1" }, token),
    );
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toContain("not registered");
  });

  it("returns 503 when the mapped shard is draining", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(
      post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    await stub.fetch(
      post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    const token = await registerTenant(stub, "t1");

    const res = await stub.fetch(
      post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, token),
    );
    expect(res.status).toBe(503);
  });
});

describe("CatalogDO tenant authorization", () => {
  it("rejects /register-tenant without an admin token", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/register-tenant", { tenantId: "t1" }));
    expect(res.status).toBe(401);
  });

  it("registers a tenant and returns a token exactly once", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/register-tenant", { tenantId: "t1" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; tenantId: string; token: string };
    expect(body.ok).toBe(true);
    expect(body.tenantId).toBe("t1");
    expect(typeof body.token).toBe("string");
    expect(body.token.length).toBeGreaterThan(0);
  });

  it("rejects re-registering an already-registered tenant without rotate:true", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/register-tenant", { tenantId: "t1" }, `Bearer ${env.ADMIN_TOKEN}`));
    const res = await stub.fetch(post("/register-tenant", { tenantId: "t1" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(409);
  });

  it("rotate:true issues a new token that invalidates the old one", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));

    const firstRes = await stub.fetch(post("/register-tenant", { tenantId: "t1" }, `Bearer ${env.ADMIN_TOKEN}`));
    const { token: oldToken } = (await firstRes.json()) as { token: string };

    const rotateRes = await stub.fetch(
      post("/register-tenant", { tenantId: "t1", rotate: true }, `Bearer ${env.ADMIN_TOKEN}`),
    );
    expect(rotateRes.status).toBe(200);
    const { token: newToken } = (await rotateRes.json()) as { token: string };
    expect(newToken).not.toBe(oldToken);

    const oldTokenRes = await stub.fetch(
      post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, `Bearer ${oldToken}`),
    );
    expect(oldTokenRes.status).toBe(401);

    const newTokenRes = await stub.fetch(
      post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, `Bearer ${newToken}`),
    );
    expect(newTokenRes.status).toBe(200);
  });

  it("revoke-tenant invalidates the tenant's token", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));
    const token = await registerTenant(stub, "t1");

    const revokeRes = await stub.fetch(post("/revoke-tenant", { tenantId: "t1" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(revokeRes.status).toBe(200);

    const routeRes = await stub.fetch(
      post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, token),
    );
    expect(routeRes.status).toBe(401);
    const body = (await routeRes.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TENANT_TOKEN_REVOKED");
  });

  it("returns 404 revoking a tenant that was never registered", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/revoke-tenant", { tenantId: "ghost" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(404);
  });

  it("/route 401s with a distinct error code for missing, wrong, and unregistered tokens", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));
    await registerTenant(stub, "t1");

    const routeBody = { table: "events", tenantId: "t1", partitionKey: "p1" };

    const missing = await stub.fetch(post("/route", routeBody));
    expect(missing.status).toBe(401);
    expect(((await missing.json()) as { error: { code: string } }).error.code).toBe("TENANT_TOKEN_MISSING");

    const wrong = await stub.fetch(post("/route", routeBody, "Bearer not-the-real-token"));
    expect(wrong.status).toBe(401);
    expect(((await wrong.json()) as { error: { code: string } }).error.code).toBe("TENANT_TOKEN_INVALID");

    const unregistered = await stub.fetch(
      post("/route", { table: "events", tenantId: "never-registered", partitionKey: "p1" }, "Bearer whatever"),
    );
    expect(unregistered.status).toBe(401);
    expect(((await unregistered.json()) as { error: { code: string } }).error.code).toBe("TENANT_NOT_REGISTERED");
  });
});

describe("CatalogDO migration state on vbucket_map (Milestone 3, Chunk 3)", () => {
  it("/route returns {targetShardId, migrationStatus} while a vbucket is backfilling, and the authoritative shardId stays the source", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 64 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));
    const token = await registerTenant(stub, "t1");

    const before = (await (
      await stub.fetch(post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, token))
    ).json()) as { shardId: string; vbucket: number; migrationStatus?: string; targetShardId?: string };
    expect(before.migrationStatus).toBeUndefined();
    expect(before.targetShardId).toBeUndefined();

    // Put this vbucket into 'backfilling' directly — the state Chunk 4's
    // /admin/migrate-vbucket will set through orchestration.
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "UPDATE vbucket_map SET migration_status = 'backfilling', target_shard_id = 'shard-target-x' WHERE vbucket = ?",
        before.vbucket,
      );
    });

    const during = (await (
      await stub.fetch(post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, token))
    ).json()) as { shardId: string; migrationStatus?: string; targetShardId?: string };
    expect(during.shardId).toBe(before.shardId); // source stays authoritative
    expect(during.migrationStatus).toBe("backfilling");
    expect(during.targetShardId).toBe("shard-target-x");

    // Back to 'none' — the extra fields disappear again (pre-M3 shape).
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "UPDATE vbucket_map SET migration_status = 'none', target_shard_id = NULL WHERE vbucket = ?",
        before.vbucket,
      );
    });
    const after = (await (
      await stub.fetch(post("/route", { table: "events", tenantId: "t1", partitionKey: "p1" }, token))
    ).json()) as { migrationStatus?: string; targetShardId?: string };
    expect(after.migrationStatus).toBeUndefined();
    expect(after.targetShardId).toBeUndefined();
  });

  it("migration columns are added to a pre-existing vbucket_map via ensureColumn (additive migration)", async () => {
    const stub = await freshCatalog();
    // Simulate a DO provisioned before the migration columns existed.
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(`
        CREATE TABLE vbucket_map (
          vbucket INTEGER PRIMARY KEY,
          shard_id TEXT NOT NULL,
          map_version INTEGER NOT NULL,
          updated_at TEXT NOT NULL
        )
      `);
      state.storage.sql.exec(
        "INSERT INTO vbucket_map (vbucket, shard_id, map_version, updated_at) VALUES (0, 'shard-0', 1, ?)",
        new Date().toISOString(),
      );
    });

    // Any fetch triggers ensureSchema(), which must upgrade the old table
    // in place without touching the existing row.
    await stub.fetch(post("/list-shards", {}));

    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT vbucket, shard_id, migration_status, target_shard_id FROM vbucket_map WHERE vbucket = 0"),
      ) as Array<{ vbucket: number; shard_id: string; migration_status: string; target_shard_id: string | null }>;
      expect(rows).toHaveLength(1);
      expect(rows[0].shard_id).toBe("shard-0");
      expect(rows[0].migration_status).toBe("none");
      expect(rows[0].target_shard_id).toBeNull();
    });
  });
});

describe("CatalogDO migration state transitions (Milestone 3, Chunk 4)", () => {
  it("migrate-vbucket: none -> backfilling; a second migrate is 409 MIGRATION_IN_PROGRESS; abort returns to none and a fresh migrate is accepted", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 8 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));

    const start = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "shard-mig-a" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(start.status).toBe(200);
    const startBody = (await start.json()) as { ok: boolean; status: string; fromShard: string; toShard: string };
    expect(startBody.status).toBe("backfilling");
    expect(startBody.toShard).toBe("shard-mig-a");

    const dup = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "shard-mig-b" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(dup.status).toBe(409);
    expect(((await dup.json()) as { error: { code: string } }).error.code).toBe("MIGRATION_IN_PROGRESS");

    const statusRes = await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`));
    const statusBody = (await statusRes.json()) as { status: string; fromShard: string; toShard: string; rowsCopied: number; mirrorQueueDepth: number; startedAt: string };
    expect(statusBody.status).toBe("backfilling");
    expect(statusBody.toShard).toBe("shard-mig-a");
    expect(typeof statusBody.rowsCopied).toBe("number");
    expect(typeof statusBody.mirrorQueueDepth).toBe("number");
    expect(statusBody.startedAt).toBeTruthy();

    const abort = await stub.fetch(post("/migrate-vbucket-abort", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(abort.status).toBe(200);
    expect(((await abort.json()) as { status: string }).status).toBe("aborted");

    const statusAfter = await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(((await statusAfter.json()) as { status: string }).status).toBe("none");

    // Aborting again: nothing active -> 409 MIGRATION_ALREADY_COMMITTED.
    const abortAgain = await stub.fetch(post("/migrate-vbucket-abort", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(abortAgain.status).toBe(409);
    expect(((await abortAgain.json()) as { error: { code: string } }).error.code).toBe("MIGRATION_ALREADY_COMMITTED");

    // A fresh migration is accepted after the abort.
    const restart = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "shard-mig-c" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(restart.status).toBe(200);
  });

  it("migrate-vbucket rejects an unmapped vbucket 404 and a target equal to the source 400", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));

    const unmapped = await stub.fetch(post("/migrate-vbucket", { vbucket: 9999 }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(unmapped.status).toBe(404);

    const sameShard = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(sameShard.status).toBe(400);
  });
});

describe("CatalogDO drain v2 ring evacuation (Milestone 3, Chunk 5)", () => {
  it("substitutes a drained shard out of an index's pinned ring deterministically (smallest hashKey(indexName + ':' + shardId) among out-of-ring active shards), copying entries before repointing and deleting source copies after", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 3, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));

    // Pin an index ring containing ONLY shard-0, directly (create-index
    // would pin all three shards, leaving no candidate).
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'ready', ?, ?)",
        "idx_evac_by_v",
        "events",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify(["shard-0"]),
      );
      // Reassign shard-0's vbuckets elsewhere so the drain goes straight to
      // ring evacuation (phase 2) without vbucket migrations.
      state.storage.sql.exec("UPDATE vbucket_map SET shard_id = 'shard-1' WHERE shard_id = 'shard-0'");
    });

    // Seed an index entry on the shard being drained.
    const shard0 = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await shard0.fetch(
      new Request("https://shard.internal/index-entries-import", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          rows: [
            {
              table_name: "events",
              index_name: "idx_evac_by_v",
              index_key_json: JSON.stringify(["alpha"]),
              partition_key: "row-1",
              source_shard_id: "shard-1",
              tenant_id: "t1",
              updated_at: new Date().toISOString(),
            },
          ],
        }),
      }),
    );

    const drainRes = await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(drainRes.status).toBe(200);

    // Drive the evacuation.
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });

    // The spec's deterministic rule, computed independently here: candidates
    // are the active shards not already in the ring.
    const candidates = ["shard-1", "shard-2"];
    const expected = candidates.reduce((best, s) =>
      hashKey(`idx_evac_by_v:${s}`) < hashKey(`idx_evac_by_v:${best}`) ? s : best,
    );

    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT placement_ring_json FROM index_rules WHERE index_name = ?", "idx_evac_by_v"),
      ) as Array<{ placement_ring_json: string }>;
      expect(JSON.parse(rows[0].placement_ring_json)).toEqual([expected]);
    });

    // Entry copied to the substitute; source copy deleted.
    const substituteStub = env.SHARD.get(env.SHARD.idFromName(expected));
    await runInDurableObject(substituteStub, async (_instance2: unknown, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ?", "idx_evac_by_v"));
      expect(rows).toHaveLength(1);
    });
    await runInDurableObject(shard0, async (_instance2: unknown, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ?", "idx_evac_by_v"));
      expect(rows).toHaveLength(0);
    });

    const statusRes = await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));
    const statusBody = (await statusRes.json()) as { vbucketsRemaining: number; ringsRemaining: number; status: string };
    expect(statusBody.vbucketsRemaining).toBe(0);
    expect(statusBody.ringsRemaining).toBe(0);
    expect(statusBody.status).toBe("complete");
  });

  // Review Tier 1 #6: ring evacuation must not lose an index entry that
  // lands on the draining shard around the copy window. Entries with a range
  // of rowids (modelling a late/racing write appended after earlier ones)
  // are all captured — the reconcile loop re-scans by ascending rowid until
  // stable, so nothing beyond the initial copy cursor is dropped.
  it("ring evacuation copies EVERY index entry on the draining shard (including higher-rowid late arrivals) to the substitute, and the base row stays queryable position-for-position", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 3, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'ready', ?, ?)",
        "idx_evac_race",
        "events",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify(["shard-0"]),
      );
      state.storage.sql.exec("UPDATE vbucket_map SET shard_id = 'shard-1' WHERE shard_id = 'shard-0'");
    });

    const shard0 = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    const importEntry = (pk: string) =>
      shard0.fetch(
        new Request("https://shard.internal/index-entries-import", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            rows: [
              {
                table_name: "events",
                index_name: "idx_evac_race",
                index_key_json: JSON.stringify(["alpha"]),
                partition_key: pk,
                source_shard_id: "shard-1",
                tenant_id: "t1",
                updated_at: new Date().toISOString(),
              },
            ],
          }),
        }),
      );
    // First batch, then a "later" batch with higher rowids (a racing write
    // that appended after the earlier ones).
    await importEntry("row-a");
    await importEntry("row-b");
    await importEntry("row-c-late");

    await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });

    const candidates = ["shard-1", "shard-2"];
    const substitute = candidates.reduce((best, s) =>
      hashKey(`idx_evac_race:${s}`) < hashKey(`idx_evac_race:${best}`) ? s : best,
    );
    const substituteStub = env.SHARD.get(env.SHARD.idFromName(substitute));
    const copied = await runInDurableObject(substituteStub, async (_i: unknown, state: DurableObjectState) => {
      return (
        Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ? ORDER BY partition_key", "idx_evac_race")) as Array<{
          partition_key: string;
        }>
      ).map((r) => r.partition_key);
    });
    expect(copied).toEqual(["row-a", "row-b", "row-c-late"]);

    // Source drained.
    await runInDurableObject(shard0, async (_i: unknown, state: DurableObjectState) => {
      expect(Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ?", "idx_evac_race"))).toHaveLength(0);
    });
  });

  it("rejects the drain 409 RING_EVACUATION_NO_CANDIDATE when every active shard is already in the index's ring", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'ready', ?, ?)",
        "idx_nocand_by_v",
        "events",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify(["shard-0", "shard-1"]),
      );
    });

    const drainRes = await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(drainRes.status).toBe(409);
    const body = (await drainRes.json()) as { error: { code: string } };
    expect(body.error.code).toBe("RING_EVACUATION_NO_CANDIDATE");

    // The shard was never marked draining — the rejection happened before
    // any durable state change.
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT status FROM shards WHERE shard_id = 'shard-0'")) as Array<{ status: string }>;
      expect(rows[0].status).toBe("active");
    });
  });
});
