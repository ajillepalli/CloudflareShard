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

/** POSTs directly to a ShardDO stub, bypassing the catalog — for tests that
 * need to seed or inspect a shard's physical rows/provenance directly. */
function shardExecute(shardStub: { fetch: (req: Request) => Promise<Response> }, body: unknown) {
  return shardStub.fetch(
    new Request("https://shard.internal/execute", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    }),
  );
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

  // Review Tier 3: /list-tenants, /vbucket-map, /migrate-vbucket,
  // /migrate-vbucket-status, /migrate-vbucket-abort, and /drain-shard-status
  // were added to ADMIN_GATED_ROUTES without their own unauthorized-case
  // coverage (unlike /init, /split-vbucket, /register-table above).
  it("rejects /list-tenants without an admin token", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/list-tenants", {}));
    expect(res.status).toBe(401);
  });

  it("rejects /vbucket-map without an admin token", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/vbucket-map", {}));
    expect(res.status).toBe(401);
  });

  it("rejects /migrate-vbucket without an admin token", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "shard-x" }));
    expect(res.status).toBe(401);
  });

  it("rejects /migrate-vbucket-status without an admin token", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }));
    expect(res.status).toBe(401);
  });

  it("rejects /migrate-vbucket-abort without an admin token", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/migrate-vbucket-abort", { vbucket: 0 }));
    expect(res.status).toBe(401);
  });

  it("rejects /drain-shard-status without an admin token", async () => {
    const stub = await freshCatalog();
    const res = await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }));
    expect(res.status).toBe(401);
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

  // Review Tier 3 test-coverage gap: advanceMigration's cutover step-3 abort
  // path (a checksum mismatch) was only exercised implicitly (never actually
  // triggered) by the happy-path split-vbucket test above. Puts the vbucket
  // directly into 'cutover' with a target whose copy of the row deliberately
  // diverges from the source, so the very next tick's checksum comparison is
  // guaranteed to mismatch — then confirms recovery on a later tick.
  it("a cutover checksum mismatch wipes the target and rewinds status to 'backfilling'; a later tick then completes the migration", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(
      post(
        "/register-table",
        { table: "t", partitionKeyColumn: "id", schemaSql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)" },
        `Bearer ${env.ADMIN_TOKEN}`,
      ),
    );

    // shard-0 owns every vbucket (numShards: 1) — plant the source's real row.
    const source = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await shardExecute(source, { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `schema-src-${crypto.randomUUID()}`, isMutation: true });
    await shardExecute(source, {
      sql: "INSERT INTO t (id, v) VALUES ('row-1', 'correct')",
      requestId: `ins-src-${crypto.randomUUID()}`,
      isMutation: true,
      tenantId: "t1",
      table: "t",
      partitionKey: "row-1",
      vbucket: 0,
    });

    // Plant a target whose copy of the SAME row diverges — as if a prior
    // backfill pass had copied it, then the row changed only on one side.
    const targetShardId = "shard-mismatch-target";
    const target = env.SHARD.get(env.SHARD.idFromName(targetShardId));
    await shardExecute(target, { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `schema-tgt-${crypto.randomUUID()}`, isMutation: true });
    await shardExecute(target, {
      sql: "INSERT INTO t (id, v) VALUES ('row-1', 'WRONG')",
      requestId: `ins-tgt-${crypto.randomUUID()}`,
      isMutation: true,
      tenantId: "t1",
      table: "t",
      partitionKey: "row-1",
      vbucket: 0,
    });

    // Drop vbucket 0 directly into 'cutover' against that target — skips
    // driving a real backfill pass (irrelevant to the step-3 check under
    // test) and deterministically reproduces "backfill finished, but the
    // copies disagree" without racing the single-tick backfill-then-cutover
    // continuation that a normal migration would otherwise do in one call.
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT OR IGNORE INTO shards (shard_id, status, created_at) VALUES (?, 'active', ?)",
        targetShardId,
        new Date().toISOString(),
      );
      state.storage.sql.exec(
        `
        UPDATE vbucket_map
        SET migration_status = 'cutover', target_shard_id = ?, migration_rows_copied = 1,
            migration_started_at = ?, backfill_table = NULL, backfill_after_pk = NULL, updated_at = ?
        WHERE vbucket = 0
        `,
        targetShardId,
        new Date().toISOString(),
        new Date().toISOString(),
      );
    });

    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });

    // Rewound to 'backfilling' — not left fenced/half-flipped.
    const afterMismatch = (await (
      await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))
    ).json()) as { status: string; rowsCopied: number };
    expect(afterMismatch.status).toBe("backfilling");
    // Re-review item D: the rewind resets rowsCopied to 0 (the target was
    // wiped and will be re-copied from scratch) — it was seeded at 1 above, so
    // without the reset it would inflate on every retry.
    expect(afterMismatch.rowsCopied).toBe(0);

    // Target wiped — the mismatched copy is gone, not left behind.
    const targetCount = (await (
      await shardExecute(target, { sql: "SELECT COUNT(*) AS n FROM t WHERE id = 'row-1'", requestId: `cnt-tgt-${crypto.randomUUID()}`, isMutation: false })
    ).json()) as { rows: Array<{ n: number }> };
    expect(targetCount.rows[0].n).toBe(0);

    // Source untouched — the abort path never touches the source's data.
    const sourceRow = (await (
      await shardExecute(source, { sql: "SELECT v FROM t WHERE id = 'row-1'", requestId: `sel-src-${crypto.randomUUID()}`, isMutation: false })
    ).json()) as { rows: Array<{ v: string }> };
    expect(sourceRow.rows[0].v).toBe("correct");

    // A later tick (or several) re-backfills from the now-clean state and
    // completes: fresh copy matches, checksums agree, and the map flips.
    for (let tick = 0; tick < 10; tick += 1) {
      await runInDurableObject(stub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const statusBody = (await (
        await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))
      ).json()) as { status: string };
      if (statusBody.status === "none") break;
    }

    const finalStatus = (await (
      await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))
    ).json()) as { status: string };
    expect(finalStatus.status).toBe("none");

    const finalTargetRow = (await (
      await shardExecute(target, { sql: "SELECT v FROM t WHERE id = 'row-1'", requestId: `sel-tgt-final-${crypto.randomUUID()}`, isMutation: false })
    ).json()) as { rows: Array<{ v: string }> };
    expect(finalTargetRow.rows[0].v).toBe("correct");
  });

  // Re-review: the cutover wait for prepared 2PC intents was unbounded — a tx
  // prepared-but-never-resolved on the source would block cutover forever with
  // no operator signal. After the bound elapses the migration must surface a
  // distinguishable status naming the wedged txId, not loop mutely.
  it("a prepared 2PC intent that never resolves on the source surfaces status 'cutover-blocked-on-prepared-intents' with the txId (bounded wait)", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 8 }, `Bearer ${env.ADMIN_TOKEN}`));

    // Put vbucket 0 directly into cutover against a target, with the
    // cutover clock set well in the past so the very next tick exceeds the
    // bound. shard-0 owns vbucket 0 (round-robin from /init).
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT OR IGNORE INTO shards (shard_id, status, created_at) VALUES ('shard-cutover-target', 'active', ?)",
        new Date().toISOString(),
      );
      state.storage.sql.exec(
        `UPDATE vbucket_map
         SET migration_status = 'cutover', target_shard_id = 'shard-cutover-target',
             cutover_started_at = ?, cutover_stall_reason = NULL, migration_started_at = ?, updated_at = ?
         WHERE vbucket = 0`,
        new Date(Date.now() - 5 * 60_000).toISOString(), // 5 min ago — past the bound
        new Date().toISOString(),
        new Date().toISOString(),
      );
    });

    // Seed a never-resolving prepared 2PC intent on the SOURCE for vbucket 0.
    const source = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await source.fetch(new Request("https://shard.internal/stats", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }));
    await runInDurableObject(source, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec(
        `INSERT INTO pending_intents (coordinator_tx_id, intent_seq, sql, params_json, status, lock_keys_json, prepared_at, vbucket, op)
         VALUES ('tx-wedged', 0, 'INSERT INTO t (id) VALUES (''x'')', '[]', 'prepared', '[]', ?, 0, 'insert')`,
        new Date().toISOString(),
      );
    });

    // One orchestration tick: mirror queue is empty, so the cutover branch
    // reaches the prepared-intent gate, sees it non-empty AND past the bound,
    // and marks the stall (returning true — it keeps polling, doesn't abort).
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });

    const statusBody = (await (
      await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))
    ).json()) as { status: string; blockedTxIds?: string[] };
    expect(statusBody.status).toBe("cutover-blocked-on-prepared-intents");
    expect(statusBody.blockedTxIds).toEqual(["tx-wedged"]);

    // The migration is NOT aborted — it's still in cutover, awaiting operator
    // /admin/tx-force-abort of the wedged tx.
    const raw = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      return Array.from(
        state.storage.sql.exec("SELECT migration_status FROM vbucket_map WHERE vbucket = 0"),
      ) as Array<{ migration_status: string }>;
    });
    expect(raw[0].migration_status).toBe("cutover");
  });

  // Adversarial re-review: cutover_started_at is nullable, so a migration
  // already in 'cutover' at deploy time has NULL there. A NULL must not read
  // as "never times out" (that reintroduces the livelock) — the first tick
  // that observes a prepared intent stamps the clock, and once the stamp is
  // past the bound the stall surfaces.
  it("a 'cutover' row with a NULL cutover_started_at gets its clock stamped, then engages the prepared-intents bound", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 8 }, `Bearer ${env.ADMIN_TOKEN}`));

    // Put vbucket 0 into cutover with a NULL clock (models a pre-existing
    // cutover at the deploy that added the column). shard-0 owns vbucket 0.
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT OR IGNORE INTO shards (shard_id, status, created_at) VALUES ('shard-nullclock-target', 'active', ?)",
        new Date().toISOString(),
      );
      state.storage.sql.exec(
        `UPDATE vbucket_map
         SET migration_status = 'cutover', target_shard_id = 'shard-nullclock-target',
             cutover_started_at = NULL, cutover_stall_reason = NULL, migration_started_at = ?, updated_at = ?
         WHERE vbucket = 0`,
        new Date().toISOString(),
        new Date().toISOString(),
      );
    });

    // Never-resolving prepared 2PC intent on the source for vbucket 0. shard-0
    // is a fixed-name DO shared across this file's tests, so first clear any
    // prepared intents an earlier test left on vbucket 0 (keeps blockedTxIds
    // deterministic).
    const source = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await source.fetch(new Request("https://shard.internal/stats", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }));
    await runInDurableObject(source, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM pending_intents WHERE vbucket = 0");
      state.storage.sql.exec(
        `INSERT INTO pending_intents (coordinator_tx_id, intent_seq, sql, params_json, status, lock_keys_json, prepared_at, vbucket, op)
         VALUES ('tx-nullclock', 0, 'INSERT INTO t (id) VALUES (''x'')', '[]', 'prepared', '[]', ?, 0, 'insert')`,
        new Date().toISOString(),
      );
    });

    // First tick: the prepared-intent gate observes the NULL clock and STAMPS
    // it (starting the bound from now) — but not yet past the bound, so no
    // stall marker. Without the fix the NULL would yield elapsed=null and the
    // stall would never engage.
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const afterFirst = (await (
      await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))
    ).json()) as { status: string };
    expect(afterFirst.status).toBe("cutover"); // clock started, not yet stalled
    const stamped = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      return Array.from(
        state.storage.sql.exec("SELECT cutover_started_at FROM vbucket_map WHERE vbucket = 0"),
      ) as Array<{ cutover_started_at: string | null }>;
    });
    expect(stamped[0].cutover_started_at).not.toBeNull(); // the NULL was stamped

    // Simulate the bound elapsing by pushing the stamp into the past, then
    // tick again — now the stall engages.
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "UPDATE vbucket_map SET cutover_started_at = ? WHERE vbucket = 0",
        new Date(Date.now() - 5 * 60_000).toISOString(),
      );
    });
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const afterSecond = (await (
      await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))
    ).json()) as { status: string; blockedTxIds?: string[] };
    expect(afterSecond.status).toBe("cutover-blocked-on-prepared-intents");
    expect(afterSecond.blockedTxIds).toEqual(["tx-nullclock"]);
  });

  // Codex review P1 (correctness): schema provisioning on a freshly created
  // split target was coupled to the row-export loop, which is filtered to only
  // tables that have rows in the migrating vbucket. So a registered table with
  // ZERO rows in that vbucket never got its schema created on the new shard —
  // the migration cut over fine (empty checksums as empty), but a later write
  // to it on the moved vbucket would fail `no such table`.
  it("provisions schema_sql for a registered table with zero rows in the migrating vbucket onto a fresh split target", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));

    const hasSchema = "CREATE TABLE IF NOT EXISTS p1prov_hasrows (id TEXT PRIMARY KEY, v TEXT)";
    const noSchema = "CREATE TABLE IF NOT EXISTS p1prov_norows (id TEXT PRIMARY KEY, v TEXT)";
    await stub.fetch(post("/register-table", { table: "p1prov_hasrows", partitionKeyColumn: "id", schemaSql: hasSchema }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "p1prov_norows", partitionKeyColumn: "id", schemaSql: noSchema }, `Bearer ${env.ADMIN_TOKEN}`));

    // shard-0 owns vbucket 0 (numShards 1). Create ONLY p1prov_hasrows there
    // and give it one attributed row in vbucket 0. p1prov_norows has zero rows
    // (it isn't even created on the source).
    const source = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await shardExecute(source, { sql: hasSchema, requestId: `sc-${crypto.randomUUID()}`, isMutation: true });
    // shard-0 is a fixed-name DO shared across this file's tests; clear any
    // prepared intents / mirror jobs an earlier test left on vbucket 0, or the
    // cutover gates would block this migration from completing.
    await runInDurableObject(source, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM pending_intents WHERE vbucket = 0");
      state.storage.sql.exec("DELETE FROM __cf_mirror_pending WHERE vbucket = 0");
    });
    await shardExecute(source, {
      sql: "INSERT INTO p1prov_hasrows (id, v) VALUES ('r1', 'x')",
      requestId: `ins-${crypto.randomUUID()}`,
      isMutation: true,
      tenantId: "t1",
      table: "p1prov_hasrows",
      partitionKey: "r1",
      vbucket: 0,
    });

    // Migrate vbucket 0 to a BRAND-NEW target shard that never received the
    // create-table fan-out — it only gets whatever the migration provisions.
    const targetShardId = `shard-freshsplit-${crypto.randomUUID()}`;
    const mig = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(mig.status).toBe(200);

    for (let tick = 0; tick < 12; tick += 1) {
      await runInDurableObject(stub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const s = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
      if (s.status === "none") break;
    }
    const finalStatus = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
    expect(finalStatus.status).toBe("none"); // migration completed

    // The fresh target must physically have BOTH tables — including the one
    // with no rows in this vbucket.
    const targetStub = env.SHARD.get(env.SHARD.idFromName(targetShardId));
    const tableNames = await runInDurableObject(targetStub, async (_i: unknown, state: DurableObjectState) => {
      return (
        Array.from(
          state.storage.sql.exec("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('p1prov_hasrows','p1prov_norows')"),
        ) as Array<{ name: string }>
      ).map((r) => r.name);
    });
    expect(tableNames).toContain("p1prov_norows");
    expect(tableNames).toContain("p1prov_hasrows");

    // And a write to the zero-rows table on the target now succeeds.
    const write = await shardExecute(targetStub, {
      sql: "INSERT INTO p1prov_norows (id, v) VALUES ('n1', 'ok')",
      requestId: `w-${crypto.randomUUID()}`,
      isMutation: true,
    });
    expect(write.status).toBe(200);
  });

  // Codex review P2: /migrate-vbucket-abort swallowed the /unfence-vbucket
  // result. If unfence failed but the later wipe succeeded, the row was
  // cleared to 'none' with the source still fenced — a permanent VBUCKET_FENCED
  // with no 'aborting' state to resume from. The abort must leave the row
  // 'aborting' (source still fenced) on an unfence failure, and only a retry
  // (unfence succeeding) clears to 'none' and lifts the fence.
  it("abort leaves the row 'aborting' + source fenced when /unfence-vbucket fails; a retry completes and lifts the fence", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(
      post("/register-table", { table: "ab_evt", partitionKeyColumn: "id", schemaSql: "CREATE TABLE IF NOT EXISTS ab_evt (id TEXT PRIMARY KEY, v TEXT)" }, `Bearer ${env.ADMIN_TOKEN}`),
    );

    // Repoint vbucket 0 to a FRESH source shard (and a fresh target) so this
    // test doesn't inherit fence/migration state other tests left on the
    // shared shard-0. Put it into cutover, and REALLY fence the source.
    const sourceShardId = `shard-abort-src-${crypto.randomUUID()}`;
    const targetShardId = `shard-abort-target-${crypto.randomUUID()}`;
    const source = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("INSERT OR IGNORE INTO shards (shard_id, status, created_at) VALUES (?, 'active', ?)", sourceShardId, new Date().toISOString());
      state.storage.sql.exec("INSERT OR IGNORE INTO shards (shard_id, status, created_at) VALUES (?, 'active', ?)", targetShardId, new Date().toISOString());
      state.storage.sql.exec("UPDATE vbucket_map SET shard_id = ?, migration_status = 'cutover', target_shard_id = ?, updated_at = ? WHERE vbucket = 0", sourceShardId, targetShardId, new Date().toISOString());
    });
    await source.fetch(
      new Request("https://shard.internal/fence-vbucket", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ vbucket: 0 }) }),
    );

    const isFenced = async () =>
      runInDurableObject(source, async (_i: unknown, state: DurableObjectState) =>
        Array.from(state.storage.sql.exec("SELECT vbucket FROM __cf_fenced_vbuckets WHERE vbucket = 0")).length > 0,
      );
    const migStatus = async () =>
      runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
        (Array.from(state.storage.sql.exec("SELECT migration_status FROM vbucket_map WHERE vbucket = 0")) as Array<{ migration_status: string }>)[0].migration_status,
      );
    expect(await isFenced()).toBe(true);

    // First abort: intercept /unfence-vbucket to fail. Expect 502, and the row
    // stays 'aborting' with the source STILL fenced.
    const firstStatus = await runInDurableObject(stub, async (instance: CatalogDO) => {
      const inst = instance as unknown as {
        callShard: (s: string, p: string, b: unknown) => Promise<Response>;
        __realCallShard?: (s: string, p: string, b: unknown) => Promise<Response>;
        handleMigrateVbucketAbort: (r: Request) => Promise<Response>;
      };
      inst.__realCallShard = inst.callShard.bind(instance);
      inst.callShard = async (shardId, path, payload) =>
        path === "/unfence-vbucket"
          ? new Response(JSON.stringify({ error: "boom" }), { status: 500, headers: { "content-type": "application/json" } })
          : inst.__realCallShard!(shardId, path, payload);
      const res = await inst.handleMigrateVbucketAbort(
        new Request("https://catalog.internal/migrate-vbucket-abort", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ vbucket: 0 }) }),
      );
      return res.status;
    });
    expect(firstStatus).toBe(502);
    expect(await migStatus()).toBe("aborting");
    expect(await isFenced()).toBe(true); // source NOT left cleared-yet-fenced

    // Restore the real callShard, then retry — the abort now completes.
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      const inst = instance as unknown as { callShard: unknown; __realCallShard: unknown };
      inst.callShard = inst.__realCallShard;
    });
    const retry = await stub.fetch(post("/migrate-vbucket-abort", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(retry.status).toBe(200);
    expect(((await retry.json()) as { status: string }).status).toBe("aborted");
    expect(await migStatus()).toBe("none");
    expect(await isFenced()).toBe(false); // fence lifted on the successful retry
  });


  // Codex review P2 (TOCTOU): startMigration checked migration_status !== 'none'
  // then awaited /unattributed-count then UPDATEd unconditionally. Two
  // concurrent calls for the same vbucket both pass the check and both UPDATE —
  // the loser overwrites the winner's target_shard_id and orphans its own new
  // shard. The transition must be a conditional claim (only from 'none') so the
  // loser 409s without leaving state. Simulated deterministically: a concurrent
  // winner claims the vbucket DURING this call's provenance await.
  it("startMigration loses the race with a conditional claim: 409, no overwrite of the winner's target, no orphaned shard", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    // A registered table so startMigration performs the awaited provenance
    // check (the interleaving point) rather than skipping it.
    await stub.fetch(
      post("/register-table", { table: "toctou_evt", partitionKeyColumn: "id", schemaSql: "CREATE TABLE IF NOT EXISTS toctou_evt (id TEXT PRIMARY KEY, v TEXT)" }, `Bearer ${env.ADMIN_TOKEN}`),
    );

    // Override the provenance round trip so that, mid-await, a concurrent
    // migration claims vbucket 0 (status -> backfilling, target -> winner-shard).
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      const inst = instance as unknown as {
        callShard: (s: string, p: string, b: unknown) => Promise<Response>;
        __realCallShard?: (s: string, p: string, b: unknown) => Promise<Response>;
        sql: { exec: (q: string, ...p: unknown[]) => unknown };
      };
      inst.__realCallShard = inst.callShard.bind(instance);
      inst.callShard = async (shardId, path, payload) => {
        if (path === "/unattributed-count") {
          inst.sql.exec(
            "UPDATE vbucket_map SET migration_status = 'backfilling', target_shard_id = 'winner-shard', updated_at = ? WHERE vbucket = 0",
            new Date().toISOString(),
          );
        }
        return inst.__realCallShard!(shardId, path, payload);
      };
    });

    const res = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "loser-shard" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(409);
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe("MIGRATION_IN_PROGRESS");

    // The winner's target survived (not overwritten to loser-shard).
    const row = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      return (
        Array.from(state.storage.sql.exec("SELECT migration_status, target_shard_id FROM vbucket_map WHERE vbucket = 0")) as Array<{
          migration_status: string;
          target_shard_id: string | null;
        }>
      )[0];
    });
    expect(row.migration_status).toBe("backfilling");
    expect(row.target_shard_id).toBe("winner-shard");

    // The losing call created NO orphaned target shard.
    const loserShardExists = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      return Array.from(state.storage.sql.exec("SELECT shard_id FROM shards WHERE shard_id = 'loser-shard'")).length > 0;
    });
    expect(loserShardExists).toBe(false);
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

  // Re-review: the reconcile safety valve was dead code — the `continue` after
  // the max passes was on the inner pass-loop's LAST iteration, so execution
  // fell straight through to the unconditional source DELETE, wiping entries
  // the substitute might not have copied. Force non-convergence (stub
  // copyIndexEntries to keep returning a growing cursor) and assert the source
  // __cf_indexes rows are NOT deleted.
  it("ring evacuation does NOT delete the source entries when the reconcile never converges (safety valve)", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 3, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'ready', ?, ?)",
        "idx_evac_unstable",
        "events",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify(["shard-0"]),
      );
      // Move every vbucket off shard-0 so the drain skips phase-1 vbucket
      // migration and goes straight to phase-2 ring evacuation.
      state.storage.sql.exec("UPDATE vbucket_map SET shard_id = 'shard-1' WHERE shard_id = 'shard-0'");
    });

    const shard0 = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    for (const pk of ["row-a", "row-b"]) {
      await shard0.fetch(
        new Request("https://shard.internal/index-entries-import", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            rows: [
              {
                table_name: "events",
                index_name: "idx_evac_unstable",
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
    }

    await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));

    // Override copyIndexEntries on the live instance so every call reports a
    // strictly higher cursor — the reconcile loop can never see afterRowid ===
    // before, so it exhausts all passes without converging (reconcileUnstable).
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      let cursor = 0;
      (instance as unknown as { copyIndexEntries: () => Promise<number> }).copyIndexEntries = async () => {
        cursor += 1;
        return cursor;
      };
      await instance.alarm();
    });

    // The ring WAS repointed (that happens before reconcile) — but the source
    // entries must survive, because the substitute wasn't proven to hold them.
    await runInDurableObject(shard0, async (_i: unknown, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ? ORDER BY partition_key", "idx_evac_unstable"),
      ) as Array<{ partition_key: string }>;
      expect(rows.map((r) => r.partition_key)).toEqual(["row-a", "row-b"]);
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

  // Review Tier 2 #10: a drain whose vbucket migration is blocked by the
  // provenance gate must PARK (stalled-provenance), not re-scan every table on
  // the source at the 250ms tick cadence forever. Re-invoking /drain-shard
  // after backfilling provenance clears the stall and resumes.
  it("a drain blocked by the provenance gate parks as 'stalled-provenance' (no 4Hz rescan) and resumes when re-invoked after provenance is backfilled", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(post("/register-table", { table: "events", partitionKeyColumn: "id" }, `Bearer ${env.ADMIN_TOKEN}`));

    // Put an unattributed row on shard-0 (a registered table row with no
    // __cf_row_owners entry) so its vbucket migration fails the provenance
    // gate. shard-0 owns vbucket 0 (round-robin from /init).
    const shard0 = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await shard0.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, v TEXT)",
          requestId: `sc-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      }),
    );
    await shard0.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sql: "INSERT INTO events (id, v) VALUES ('legacy', 'x')", requestId: `li-${crypto.randomUUID()}`, isMutation: true }),
      }),
    );

    const drainRes = await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(drainRes.status).toBe(200);

    // Drive a tick: the migration start hits the provenance gate → parks.
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const parked = (await (await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as {
      status: string;
      stallReason: string | null;
    };
    expect(parked.status).toBe("stalled-provenance");
    expect(parked.stallReason).toBe("provenance");

    // Parked: a further alarm tick does NOT start a migration (no rescan-spin)
    // — the vbucket is still on shard-0, un-migrated.
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const stillParked = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      return Array.from(state.storage.sql.exec("SELECT migration_status FROM vbucket_map WHERE vbucket = 0")) as Array<{ migration_status: string }>;
    });
    expect(stillParked[0].migration_status).toBe("none"); // never started while parked

    // Attribute the legacy row directly (equivalent to running
    // /admin/backfill-provenance), then re-invoke /drain-shard.
    await shard0.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "INSERT INTO __cf_row_owners (table_name, partition_key, tenant_id, vbucket, updated_at) VALUES ('events','legacy','t1',0,'2026-01-01T00:00:00.000Z')",
          requestId: `prov-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      }),
    );

    const reinvoke = await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(reinvoke.status).toBe(200);
    const cleared = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      return Array.from(state.storage.sql.exec("SELECT drain_stall_reason FROM shards WHERE shard_id = 'shard-0'")) as Array<{ drain_stall_reason: string | null }>;
    });
    expect(cleared[0].drain_stall_reason).toBeNull();

    // Now a tick starts the vbucket migration (no longer gated).
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const resumed = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      return Array.from(state.storage.sql.exec("SELECT shard_id, migration_status FROM vbucket_map WHERE vbucket = 0")) as Array<{
        shard_id: string;
        migration_status: string;
      }>;
    });
    // Either mid-migration or already flipped off shard-0 — the key point is
    // it's no longer parked with the vbucket sitting untouched on shard-0.
    expect(resumed[0].shard_id === "shard-0" && resumed[0].migration_status === "none").toBe(false);
  });

  // Re-review item E: advanceDrain used to label ANY startMigration rejection
  // 'provenance'. A non-provenance rejection (e.g. MIGRATION_IN_PROGRESS for a
  // vbucket wedged in 'aborting') must record a distinct reason so the operator
  // isn't sent to run /admin/backfill-provenance for the wrong cause.
  it("a non-provenance startMigration rejection parks the drain as 'stalled' with a distinct reason, not mislabeled 'provenance'", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));

    await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));

    // Override startMigration to return a non-provenance rejection (as if the
    // target vbucket were mid-abort), then drive a tick.
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      (instance as unknown as { startMigration: () => Promise<Response> }).startMigration = async () =>
        new Response(JSON.stringify({ error: { code: "MIGRATION_IN_PROGRESS" } }), {
          status: 409,
          headers: { "content-type": "application/json" },
        });
      await instance.alarm();
    });

    const parked = (await (await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as {
      status: string;
      stallReason: string | null;
    };
    expect(parked.stallReason).toBe("migration-blocked");
    expect(parked.stallReason).not.toBe("provenance");
    expect(parked.status).toBe("stalled");
  });
});
