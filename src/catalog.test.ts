import { env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import type { CatalogDO } from "./catalog";
import { hashKey } from "./hash";

// Migration/drain tests below deliberately reuse FIXED Durable Object names
// across `it` blocks (e.g. "shard-0" — the name CatalogDO's own /init always
// assigns a single-shard cluster's first shard, per src/catalog.ts's
// `shard-${i}` naming; not something a test can randomize without changing
// production shard-naming). DO storage is NOT isolated per test by this pool
// (confirmed empirically — a value written to a fixed-name DO in one `it`
// block is still there in the next), and several of those tests leave a real
// DO alarm armed (CatalogDO.alarm() re-arms itself every MIGRATION_TICK_MS
// while a migration is active, and a test that intentionally stops driving
// ticks before a migration/drain fully resolves leaves that alarm live). Left
// alone, that alarm can fire in the background — on real wall-clock time —
// during a LATER test and mutate the shared shard's fencing/mirror/intent
// state out from under it, producing exactly the run-to-run flakiness this
// guards against (different assertion fails on different runs, depending on
// real timing). `reset()` is @cloudflare/vitest-pool-workers' documented
// fix for this: it deletes all Durable Object data (and, since the objects
// themselves are gone, cancels any alarm they had armed) between tests.
afterEach(async () => {
  await reset();
});

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

  // Codex full-PR review P2: the checksum-mismatch rewind must check its three
  // cleanup calls and NOT rewind to 'backfilling' unless all succeed. A
  // transient /unfence-vbucket failure that still rewound would leave the
  // source FENCED while status said 'backfilling' → the next backfill/cutover
  // runs against a fenced source (writes 409); a failed purge/wipe leaves stale
  // mirror jobs / target rows to corrupt the retry.
  it("checksum-mismatch rewind does NOT complete while cleanup fails: stays 'cutover' with the source fenced until unfence/purge/wipe all succeed", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(
      post("/register-table", { table: "t", partitionKeyColumn: "id", schemaSql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)" }, `Bearer ${env.ADMIN_TOKEN}`),
    );

    const sourceShardId = "shard-0";
    const source = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
    await shardExecute(source, { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `sc-${crypto.randomUUID()}`, isMutation: true });
    await runInDurableObject(source, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM pending_intents WHERE vbucket = 0");
      state.storage.sql.exec("DELETE FROM __cf_mirror_pending WHERE vbucket = 0");
      state.storage.sql.exec("DELETE FROM __cf_fenced_vbuckets WHERE vbucket = 0");
      state.storage.sql.exec("DELETE FROM __cf_row_owners WHERE table_name = 't'");
      state.storage.sql.exec("DELETE FROM t");
    });
    await shardExecute(source, {
      sql: "INSERT INTO t (id, v) VALUES ('row-1', 'correct')",
      requestId: `ins-${crypto.randomUUID()}`,
      isMutation: true,
      tenantId: "t1",
      table: "t",
      partitionKey: "row-1",
      vbucket: 0,
    });

    // Target with a DIVERGENT copy → guaranteed checksum mismatch.
    const targetShardId = `shard-mismatch-cleanup-${crypto.randomUUID()}`;
    const target = env.SHARD.get(env.SHARD.idFromName(targetShardId));
    await shardExecute(target, { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `sct-${crypto.randomUUID()}`, isMutation: true });
    await shardExecute(target, {
      sql: "INSERT INTO t (id, v) VALUES ('row-1', 'WRONG')",
      requestId: `inst-${crypto.randomUUID()}`,
      isMutation: true,
      tenantId: "t1",
      table: "t",
      partitionKey: "row-1",
      vbucket: 0,
    });
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("INSERT OR IGNORE INTO shards (shard_id, status, created_at) VALUES (?, 'active', ?)", targetShardId, new Date().toISOString());
      state.storage.sql.exec(
        "UPDATE vbucket_map SET migration_status = 'cutover', target_shard_id = ?, migration_rows_copied = 1, migration_started_at = ?, updated_at = ? WHERE vbucket = 0",
        targetShardId,
        new Date().toISOString(),
        new Date().toISOString(),
      );
    });

    const migStatus = async () =>
      ((await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string }).status;
    const sourceFenced = async () =>
      runInDurableObject(source, async (_i: unknown, state: DurableObjectState) => Array.from(state.storage.sql.exec("SELECT vbucket FROM __cf_fenced_vbuckets WHERE vbucket = 0")).length > 0);

    // Phase 1: /unfence-vbucket on the source fails → cleanup can't complete →
    // must NOT rewind; stays 'cutover' with the source (re-)fenced.
    const unfence = { fail: true };
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      const inst = instance as unknown as {
        callShard: (s: string, p: string, b: unknown) => Promise<Response>;
        __real?: (s: string, p: string, b: unknown) => Promise<Response>;
      };
      inst.__real = inst.callShard.bind(instance);
      inst.callShard = async (shardId, path, payload) => {
        if (shardId === sourceShardId && path === "/unfence-vbucket" && unfence.fail) {
          return new Response(JSON.stringify({ error: "boom" }), { status: 500, headers: { "content-type": "application/json" } });
        }
        return inst.__real!(shardId, path, payload);
      };
      for (let tick = 0; tick < 3; tick += 1) {
        await instance.alarm();
      }
    });
    expect(await migStatus()).toBe("cutover"); // did NOT rewind while cleanup failed
    expect(await sourceFenced()).toBe(true); // source still fenced (not left fenced-while-backfilling)
    // The target's divergent copy is still there — wipe wasn't reached.
    const tgtDuring = (await (
      await shardExecute(target, { sql: "SELECT COUNT(*) AS n FROM t WHERE id = 'row-1'", requestId: `q1-${crypto.randomUUID()}`, isMutation: false })
    ).json()) as { rows: Array<{ n: number }> };
    expect(tgtDuring.rows[0].n).toBe(1);

    // Phase 2: let unfence succeed; the next tick completes the rewind.
    unfence.fail = false;
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    expect(await migStatus()).toBe("backfilling"); // rewind completed
    expect(await sourceFenced()).toBe(false); // source unfenced
    const tgtAfter = (await (
      await shardExecute(target, { sql: "SELECT COUNT(*) AS n FROM t WHERE id = 'row-1'", requestId: `q2-${crypto.randomUUID()}`, isMutation: false })
    ).json()) as { rows: Array<{ n: number }> };
    expect(tgtAfter.rows[0].n).toBe(0); // target wiped
  });

  // Codex full-PR review P1 (silent data loss at cutover step 5): the source
  // must stay FENCED through cleanup (delete rows + purge mirror) and unfence
  // LAST. The old order unfenced FIRST, so a straggler write that resolved the
  // old route before the flip and arrived in the unfence→delete window was
  // ACCEPTED by the now-unfenced source (enqueuing a mirror) and then had its
  // row deleted AND its mirror purged — acked but reaching neither shard. Lost.
  // We assert the fix's invariant directly: on the source, /delete-vbucket-rows
  // is issued BEFORE /unfence-vbucket — so the fence set at cutover step 1 is
  // held through the entire delete+purge, and any straggler in that window can
  // only 409 VBUCKET_FENCED (retryable → re-routes to the flipped target).
  it("keeps the source fenced through cutover cleanup: /delete-vbucket-rows runs before /unfence-vbucket (straggler can only 409, not be dropped)", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(
      post("/register-table", { table: "t", partitionKeyColumn: "id", schemaSql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)" }, `Bearer ${env.ADMIN_TOKEN}`),
    );

    const sourceShardId = "shard-0";
    const source = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
    await shardExecute(source, { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `sc-${crypto.randomUUID()}`, isMutation: true });
    await runInDurableObject(source, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM pending_intents WHERE vbucket = 0");
      state.storage.sql.exec("DELETE FROM __cf_mirror_pending WHERE vbucket = 0");
    });
    await shardExecute(source, {
      sql: "INSERT INTO t (id, v) VALUES ('row-1', 'x')",
      requestId: `ins-${crypto.randomUUID()}`,
      isMutation: true,
      tenantId: "t1",
      table: "t",
      partitionKey: "row-1",
      vbucket: 0,
    });

    const targetShardId = `shard-flip-target-${crypto.randomUUID()}`;
    const mig = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(mig.status).toBe(200);

    // Record the ORDER of the catalog's shard calls (the cross-DO I/O rules
    // forbid actually fetching the source from inside this DO context, so we
    // assert the ordering invariant the straggler protection rests on).
    const calls: string[] = [];
    await runInDurableObject(stub, async (instance: CatalogDO, state: DurableObjectState) => {
      const inst = instance as unknown as {
        callShard: (s: string, p: string, b: unknown) => Promise<Response>;
        __real?: (s: string, p: string, b: unknown) => Promise<Response>;
      };
      inst.__real = inst.callShard.bind(instance);
      inst.callShard = async (shardId, path, payload) => {
        calls.push(`${shardId}|${path}`);
        return inst.__real!(shardId, path, payload);
      };
      for (let tick = 0; tick < 12; tick += 1) {
        await instance.alarm();
        const st = (
          Array.from(state.storage.sql.exec("SELECT migration_status FROM vbucket_map WHERE vbucket = 0")) as Array<{ migration_status: string }>
        )[0].migration_status;
        if (st === "none") break;
      }
    });
    const final = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
    expect(final.status).toBe("none"); // migration completed

    // Invariant: on the SOURCE, delete-vbucket-rows precedes unfence-vbucket —
    // the fence is held through row+mirror cleanup.
    const deleteIdx = calls.indexOf(`${sourceShardId}|/delete-vbucket-rows`);
    const unfenceIdx = calls.indexOf(`${sourceShardId}|/unfence-vbucket`);
    expect(deleteIdx, `calls=${JSON.stringify(calls)}`).toBeGreaterThanOrEqual(0);
    expect(unfenceIdx).toBeGreaterThanOrEqual(0);
    expect(unfenceIdx, "source must stay fenced through cleanup: delete before unfence").toBeGreaterThan(deleteIdx);

    // And the source ends with no rows for the migrated vbucket.
    const sourceRows = (await (
      await shardExecute(source, { sql: "SELECT id FROM t", requestId: `q-${crypto.randomUUID()}`, isMutation: false })
    ).json()) as { rows: Array<{ id: string }> };
    expect(sourceRows.rows.map((r) => r.id)).not.toContain("row-1");
  });

  // Codex full-PR review P2: post-flip cleanup (delete rows + unfence source)
  // must be retryable. The flip sets status='none' AND cleanup_pending=1
  // atomically; if step-5 cleanup fails there's no active migration row for the
  // migrating loop, so the alarm's cleanup loop (keyed on cleanup_pending) must
  // retry. A failed /unfence-vbucket must not leave the source stale-fenced
  // forever while status reports the migration complete.
  it("post-flip cleanup is retryable: a failed /unfence-vbucket keeps cleanup_pending set (status stays flipped, source fenced) until a later tick lifts the fence", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await stub.fetch(
      post("/register-table", { table: "t", partitionKeyColumn: "id", schemaSql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)" }, `Bearer ${env.ADMIN_TOKEN}`),
    );

    const sourceShardId = "shard-0";
    const source = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
    await shardExecute(source, { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `sc-${crypto.randomUUID()}`, isMutation: true });
    await runInDurableObject(source, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM pending_intents WHERE vbucket = 0");
      state.storage.sql.exec("DELETE FROM __cf_mirror_pending WHERE vbucket = 0");
      state.storage.sql.exec("DELETE FROM __cf_fenced_vbuckets WHERE vbucket = 0");
      state.storage.sql.exec("DELETE FROM __cf_row_owners WHERE table_name = 't'");
      state.storage.sql.exec("DELETE FROM t");
    });
    await shardExecute(source, {
      sql: "INSERT INTO t (id, v) VALUES ('row-1', 'x')",
      requestId: `ins-${crypto.randomUUID()}`,
      isMutation: true,
      tenantId: "t1",
      table: "t",
      partitionKey: "row-1",
      vbucket: 0,
    });

    const targetShardId = `shard-flip-cleanup-${crypto.randomUUID()}`;
    await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId }, `Bearer ${env.ADMIN_TOKEN}`));

    // Fail /unfence-vbucket on the source (the flip still commits, cleanup does
    // not). Drive several ticks: the flip lands, then the cleanup loop retries
    // and keeps failing while unfence is forced to fail.
    const unfence = { fail: true };
    await runInDurableObject(stub, async (instance: CatalogDO, state: DurableObjectState) => {
      const inst = instance as unknown as {
        callShard: (s: string, p: string, b: unknown) => Promise<Response>;
        __real?: (s: string, p: string, b: unknown) => Promise<Response>;
      };
      inst.__real = inst.callShard.bind(instance);
      inst.callShard = async (shardId, path, payload) => {
        if (shardId === sourceShardId && path === "/unfence-vbucket" && unfence.fail) {
          return new Response(JSON.stringify({ error: "boom" }), { status: 500, headers: { "content-type": "application/json" } });
        }
        return inst.__real!(shardId, path, payload);
      };
      for (let tick = 0; tick < 8; tick += 1) {
        await instance.alarm();
      }
      // Now let unfence succeed and drive the cleanup retry to completion.
      unfence.fail = false;
      for (let tick = 0; tick < 8; tick += 1) {
        await instance.alarm();
        const done = (
          Array.from(state.storage.sql.exec("SELECT cleanup_pending FROM vbucket_map WHERE vbucket = 0")) as Array<{ cleanup_pending: number }>
        )[0].cleanup_pending;
        if (done === 0) break;
      }
    });

    // The map flip stands: status 'none', vbucket now owned by the target.
    const status = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
    expect(status.status).toBe("none");
    const mapped = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      (Array.from(state.storage.sql.exec("SELECT shard_id, cleanup_pending FROM vbucket_map WHERE vbucket = 0")) as Array<{ shard_id: string; cleanup_pending: number }>)[0],
    );
    expect(mapped.shard_id).toBe(targetShardId); // flip stands

    // Cleanup completed on the retry: cleanup_pending cleared and the source is
    // no longer fenced.
    expect(mapped.cleanup_pending).toBe(0);
    const stillFenced = await runInDurableObject(source, async (_i: unknown, state: DurableObjectState) =>
      Array.from(state.storage.sql.exec("SELECT vbucket FROM __cf_fenced_vbuckets WHERE vbucket = 0")).length > 0,
    );
    expect(stillFenced).toBe(false);
  });

  // Codex full-PR review P1 A: startMigration checked migration_status but not
  // cleanup_pending. A prior cutover can leave migration_status='none' while
  // post-flip cleanup (source delete + unfence) is still retrying
  // (cleanup_pending=1). Starting a new migration then overwrites
  // cleanup_source_shard_id and can strand the old source's fence forever.
  // startMigration must reject MIGRATION_CLEANUP_PENDING until cleanup clears.
  it("startMigration rejects 409 MIGRATION_CLEANUP_PENDING while a prior flip's post-flip cleanup is still pending, and allows it once cleanup clears", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));

    // Model the post-flip window: vbucket 0 flipped to 'none' but cleanup is
    // still pending against the OLD source.
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "UPDATE vbucket_map SET migration_status = 'none', cleanup_pending = 1, cleanup_source_shard_id = 'shard-old-src', updated_at = ? WHERE vbucket = 0",
        new Date().toISOString(),
      );
    });

    const blocked = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "shard-new-target" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(blocked.status).toBe(409);
    expect(((await blocked.json()) as { error: { code: string } }).error.code).toBe("MIGRATION_CLEANUP_PENDING");

    // The rejected attempt must NOT have flipped the row into 'backfilling' or
    // overwritten cleanup_source_shard_id.
    const afterBlock = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      (Array.from(
        state.storage.sql.exec("SELECT migration_status, target_shard_id, cleanup_source_shard_id FROM vbucket_map WHERE vbucket = 0"),
      ) as Array<{ migration_status: string; target_shard_id: string | null; cleanup_source_shard_id: string | null }>)[0],
    );
    expect(afterBlock.migration_status).toBe("none");
    expect(afterBlock.cleanup_source_shard_id).toBe("shard-old-src");

    // Cleanup finishes (clears the flag) — a fresh migration is now accepted.
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE vbucket_map SET cleanup_pending = 0, cleanup_source_shard_id = NULL WHERE vbucket = 0");
    });
    const allowed = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "shard-new-target" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(allowed.status).toBe(200);
    expect(((await allowed.json()) as { status: string }).status).toBe("backfilling");
  });

  // Codex full-PR review P1 B: a migration must never target a non-active
  // shard. INSERT OR IGNORE left a pre-existing 'draining' target in place and
  // let the migration proceed; once cutover flipped vbucket_map to it, /route
  // (which rejects a non-active mapping) would 503 that vbucket forever.
  it("startMigration rejects 409 TARGET_SHARD_NOT_ACTIVE for an explicit target that already exists and is draining, and never flips the map to it", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));

    // A pre-existing DRAINING shard — a valid shard id, but not a legal target.
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO shards (shard_id, status, created_at) VALUES ('shard-draining-tgt', 'draining', ?)",
        new Date().toISOString(),
      );
    });

    const res = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "shard-draining-tgt" }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(res.status).toBe(409);
    expect(((await res.json()) as { error: { code: string } }).error.code).toBe("TARGET_SHARD_NOT_ACTIVE");

    // The vbucket must NOT have been claimed for migration toward the draining shard.
    const row = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      (Array.from(
        state.storage.sql.exec("SELECT migration_status, target_shard_id FROM vbucket_map WHERE vbucket = 0"),
      ) as Array<{ migration_status: string; target_shard_id: string | null }>)[0],
    );
    expect(row.migration_status).toBe("none");
    expect(row.target_shard_id).not.toBe("shard-draining-tgt");
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

  // Codex review P2 (residual): migration-time schema provisioning re-executes
  // the captured schema_sql against the target. Its stable requestId normally
  // dedupes, but applied_requests has a 7-day TTL — once pruned, a
  // migration/drain to an ALREADY-EXISTING shard re-runs the CREATE TABLE. If
  // the captured schema lacks IF NOT EXISTS, that's a 400 "table already
  // exists" → advanceMigration throws → the migration retries forever. The fix
  // injects IF NOT EXISTS at provision time so re-execution is a no-op.
  it("migration-time provisioning is idempotent: a re-run CREATE TABLE (no IF NOT EXISTS, dedup absent) on an existing target no-ops instead of stalling; a fresh target still gets the table", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    // Registered schema DELIBERATELY without IF NOT EXISTS.
    const schema = "CREATE TABLE provreplay_evt (id TEXT PRIMARY KEY, v TEXT)";
    await stub.fetch(post("/register-table", { table: "provreplay_evt", partitionKeyColumn: "id", schemaSql: schema }, `Bearer ${env.ADMIN_TOKEN}`));

    const source = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await shardExecute(source, { sql: "CREATE TABLE IF NOT EXISTS provreplay_evt (id TEXT PRIMARY KEY, v TEXT)", requestId: `sc-${crypto.randomUUID()}`, isMutation: true });
    await runInDurableObject(source, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM pending_intents WHERE vbucket IN (0, 1)");
      state.storage.sql.exec("DELETE FROM __cf_mirror_pending WHERE vbucket IN (0, 1)");
    });
    await shardExecute(source, {
      sql: "INSERT INTO provreplay_evt (id, v) VALUES ('r0', 'x')",
      requestId: `ins-${crypto.randomUUID()}`,
      isMutation: true,
      tenantId: "t1",
      table: "provreplay_evt",
      partitionKey: "r0",
      vbucket: 0,
    });

    // Part A — EXISTING target that ALREADY has the table. Its copy was created
    // under a DIFFERENT requestId, so the migration's provisioning requestId
    // (create-table-provreplay_evt-<target>) is NOT cached there: it genuinely
    // re-executes the captured CREATE TABLE, exactly as it would after the
    // dedup row's TTL prune.
    const existingTarget = `shard-existing-${crypto.randomUUID()}`;
    const existingTargetStub = env.SHARD.get(env.SHARD.idFromName(existingTarget));
    await shardExecute(existingTargetStub, { sql: "CREATE TABLE provreplay_evt (id TEXT PRIMARY KEY, v TEXT)", requestId: `pre-${crypto.randomUUID()}`, isMutation: true });
    // Pre-register it as an active shard so startMigration treats it as EXISTING
    // (provision_pending=0 → exercises the per-table in-loop provisioning path).
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("INSERT OR IGNORE INTO shards (shard_id, status, created_at) VALUES (?, 'active', ?)", existingTarget, new Date().toISOString());
    });

    const migA = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: existingTarget }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(migA.status).toBe(200);
    for (let tick = 0; tick < 12; tick += 1) {
      await runInDurableObject(stub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const s = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
      if (s.status === "none") break;
    }
    const finalA = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
    expect(finalA.status).toBe("none"); // did NOT stall on the re-run CREATE TABLE
    const rowA = (await (
      await shardExecute(existingTargetStub, { sql: "SELECT v FROM provreplay_evt WHERE id = 'r0'", requestId: `q-${crypto.randomUUID()}`, isMutation: false })
    ).json()) as { rows: Array<{ v: string }> };
    expect(rowA.rows).toHaveLength(1); // the row imported

    // Part B — a FRESH target still gets the table created from the same
    // no-IF-NOT-EXISTS schema (provision_pending path also normalizes the DDL).
    const freshTarget = `shard-fresh-${crypto.randomUUID()}`;
    const migB = await stub.fetch(post("/migrate-vbucket", { vbucket: 1, targetShardId: freshTarget }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(migB.status).toBe(200);
    for (let tick = 0; tick < 12; tick += 1) {
      await runInDurableObject(stub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const s = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 1 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
      if (s.status === "none") break;
    }
    const finalB = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 1 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
    expect(finalB.status).toBe("none");
    const freshTables = await runInDurableObject(env.SHARD.get(env.SHARD.idFromName(freshTarget)), async (_i: unknown, state: DurableObjectState) => {
      return (Array.from(state.storage.sql.exec("SELECT name FROM sqlite_master WHERE type='table' AND name = 'provreplay_evt'")) as Array<{ name: string }>).map((r) => r.name);
    });
    expect(freshTables).toContain("provreplay_evt");
  });

  // Codex review P1 (regression on the idempotency fix): the provisioning sites
  // send IF-NOT-EXISTS-MODIFIED SQL. If they reuse /admin/create-table's
  // requestId (create-table-<table>-<target>), and that applied_requests row is
  // still PRESENT (the common case, within its 7-day TTL, hashed over the
  // UNMODIFIED schema), ShardDO returns 409 "different sql" → provisioning
  // throws → the migration to that existing shard stalls. Provisioning must use
  // its OWN requestId namespace (migrate-provision-) so it can't collide.
  it("migration provisioning uses a distinct requestId namespace: it does NOT 409 against a present create-table dedup row (within-TTL) and the migration completes", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    const schema = "CREATE TABLE provcollide_evt (id TEXT PRIMARY KEY, v TEXT)"; // no IF NOT EXISTS
    await stub.fetch(post("/register-table", { table: "provcollide_evt", partitionKeyColumn: "id", schemaSql: schema }, `Bearer ${env.ADMIN_TOKEN}`));

    const source = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await shardExecute(source, { sql: "CREATE TABLE IF NOT EXISTS provcollide_evt (id TEXT PRIMARY KEY, v TEXT)", requestId: `sc-${crypto.randomUUID()}`, isMutation: true });
    await runInDurableObject(source, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM pending_intents WHERE vbucket = 0");
      state.storage.sql.exec("DELETE FROM __cf_mirror_pending WHERE vbucket = 0");
    });
    await shardExecute(source, {
      sql: "INSERT INTO provcollide_evt (id, v) VALUES ('r0', 'x')",
      requestId: `ins-${crypto.randomUUID()}`,
      isMutation: true,
      tenantId: "t1",
      table: "provcollide_evt",
      partitionKey: "r0",
      vbucket: 0,
    });

    const target = `shard-collide-${crypto.randomUUID()}`;
    const targetStub = env.SHARD.get(env.SHARD.idFromName(target));
    // Create the table on the target under the EXACT requestId /admin/create-table
    // uses, with the UNMODIFIED (no IF NOT EXISTS) schema — leaving the
    // applied_requests row (hashed over that SQL) that the OLD provisioning
    // would collide with. Register it as an existing active shard so
    // startMigration uses the per-table in-loop provisioning path.
    await shardExecute(targetStub, { sql: schema, requestId: `create-table-provcollide_evt-${target}`, isMutation: true });
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("INSERT OR IGNORE INTO shards (shard_id, status, created_at) VALUES (?, 'active', ?)", target, new Date().toISOString());
    });

    const mig = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: target }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(mig.status).toBe(200);
    for (let tick = 0; tick < 12; tick += 1) {
      await runInDurableObject(stub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const s = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
      if (s.status === "none") break;
    }
    const final = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
    expect(final.status).toBe("none"); // did NOT 409-stall on the create-table requestId collision

    // Distinct namespaces coexist: the create-table row is untouched, and a
    // separate migrate-provision row now records the (idempotent) provisioning.
    const appliedIds = await runInDurableObject(targetStub, async (_i: unknown, state: DurableObjectState) => {
      return (
        Array.from(
          state.storage.sql.exec(
            "SELECT request_id FROM applied_requests WHERE request_id IN (?, ?)",
            `create-table-provcollide_evt-${target}`,
            `migrate-provision-provcollide_evt-${target}`,
          ),
        ) as Array<{ request_id: string }>
      ).map((r) => r.request_id);
    });
    expect(appliedIds).toContain(`create-table-provcollide_evt-${target}`);
    expect(appliedIds).toContain(`migrate-provision-provcollide_evt-${target}`);
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

  // Codex full-PR review P2 (drain reports complete despite stranded entries):
  // when ring-evacuation reconcile never converges, the safety valve correctly
  // SKIPS the destructive source delete — but the ring is already repointed, so
  // vbuckets=0 + rings-remaining=0 made /drain-shard-status report 'complete'.
  // An operator could then decommission a shard that still physically holds
  // index entries the substitute was never proven to have (silent index miss).
  // The fix parks the drain with drain_stall_reason='ring-reconcile-unstable' so
  // status reports a distinguishable NON-'complete' stall.
  it("a reconcile that never converges parks the drain as a 'ring-reconcile-unstable' stall (NOT 'complete'), and the source index entries remain queryable", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 3, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'ready', ?, ?)",
        "idx_evac_stall",
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
                index_name: "idx_evac_stall",
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

    // Force the reconcile to never converge (strictly growing cursor), then run
    // a tick.
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      let cursor = 0;
      (instance as unknown as { copyIndexEntries: () => Promise<number> }).copyIndexEntries = async () => {
        cursor += 1;
        return cursor;
      };
      await instance.alarm();
    });

    // /drain-shard-status must NOT report 'complete' — it reports the stall.
    const status = (await (
      await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`))
    ).json()) as { status: string; stallReason: string | null };
    expect(status.status).not.toBe("complete");
    expect(status.stallReason).toBe("ring-reconcile-unstable");

    // The source index entries remain in place (not deleted) — still there to
    // be queried / recovered, not silently lost.
    await runInDurableObject(shard0, async (_i: unknown, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ? ORDER BY partition_key", "idx_evac_stall"),
      ) as Array<{ partition_key: string }>;
      expect(rows.map((r) => r.partition_key)).toEqual(["row-a", "row-b"]);
    });
  });

  // Codex full-PR review P1 D (unstable evacuation must stay retryable): the old
  // design repointed the ring BEFORE the reconcile converged, so an unstable
  // pass left the ring already repointed — the next tick no longer saw this
  // index and never retried, permanently stranding any source-only entries. The
  // fix converges FIRST (copies every straggler) and only THEN repoints+deletes;
  // while unstable it leaves the ring on the draining shard (reads resolve) and
  // retries on later ticks. Here: force non-convergence, prove the ring is NOT
  // repointed and the drain is not 'complete'; then let the churn stop and prove
  // a later tick converges, repoints, and deletes with no missed entries.
  it("fences + repoints EARLY but does not delete the source while the copy is unstable; a marker keeps it revisited until a quiet tick converges and deletes", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 3, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'ready', ?, ?)",
        "idx_evac_d",
        "events",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify(["shard-0"]),
      );
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
                index_name: "idx_evac_d",
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

    // UNSTABLE: force copyIndexEntries to never converge (strictly growing
    // cursor). Capture the real method so a later tick can restore it.
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      const inst = instance as unknown as { copyIndexEntries: (...a: unknown[]) => Promise<number>; __realCopy?: (...a: unknown[]) => Promise<number> };
      inst.__realCopy = inst.copyIndexEntries.bind(instance);
      let cursor = 0;
      inst.copyIndexEntries = async () => {
        cursor += 1;
        return cursor;
      };
      await instance.alarm();
    });

    // Round-13 invariant: the ring IS repointed early (fence protects the
    // draining shard), but the source is NOT deleted while the copy is unstable,
    // and a marker keeps the index revisited.
    const ringDuring = (await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      JSON.parse(
        (Array.from(state.storage.sql.exec("SELECT placement_ring_json FROM index_rules WHERE index_name = 'idx_evac_d'")) as Array<{ placement_ring_json: string }>)[0]
          .placement_ring_json,
      ),
    )) as string[];
    expect(ringDuring).not.toContain("shard-0"); // repointed early
    expect(ringDuring).toHaveLength(1);
    // The index is fenced on the draining shard (so an in-flight write 409s).
    const fencedDuring = await runInDurableObject(shard0, async (_i: unknown, state: DurableObjectState) =>
      Array.from(state.storage.sql.exec("SELECT index_name FROM __cf_fenced_index_rings WHERE index_name = 'idx_evac_d'")).length > 0,
    );
    expect(fencedDuring).toBe(true);
    // A marker keeps evacuation in flight (revisit not driven by ring membership).
    const markerDuring = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      Array.from(state.storage.sql.exec("SELECT index_name FROM drain_ring_evac WHERE shard_id = 'shard-0' AND index_name = 'idx_evac_d'")).length,
    );
    expect(markerDuring).toBe(1);
    const statusDuring = (await (await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as {
      status: string;
      stallReason: string | null;
    };
    expect(statusDuring.status).not.toBe("complete");
    expect(statusDuring.stallReason).toBe("ring-reconcile-unstable");
    // Source entries NOT deleted while unstable (still there to be copied).
    await runInDurableObject(shard0, async (_i: unknown, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ? ORDER BY partition_key", "idx_evac_d")) as Array<{ partition_key: string }>;
      expect(rows.map((r) => r.partition_key)).toEqual(["row-a", "row-b"]);
    });

    // CHURN STOPS: restore the real copy; drive ticks until the drain completes.
    for (let tick = 0; tick < 5; tick += 1) {
      const done = await runInDurableObject(stub, async (instance: CatalogDO, state: DurableObjectState) => {
        const inst = instance as unknown as { copyIndexEntries: (...a: unknown[]) => Promise<number>; __realCopy?: (...a: unknown[]) => Promise<number> };
        if (inst.__realCopy) inst.copyIndexEntries = inst.__realCopy;
        await instance.alarm();
        const reason = (Array.from(state.storage.sql.exec("SELECT drain_stall_reason FROM shards WHERE shard_id = 'shard-0'")) as Array<{ drain_stall_reason: string | null }>)[0]
          .drain_stall_reason;
        return reason === null;
      });
      if (done) break;
    }

    // Converged: ring repointed away from shard-0, source deleted, complete.
    const ringAfter = (await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      JSON.parse(
        (Array.from(state.storage.sql.exec("SELECT placement_ring_json FROM index_rules WHERE index_name = 'idx_evac_d'")) as Array<{ placement_ring_json: string }>)[0]
          .placement_ring_json,
      ),
    )) as string[];
    expect(ringAfter).not.toContain("shard-0");
    const substitute = ringAfter[0];
    const substituteStub = env.SHARD.get(env.SHARD.idFromName(substitute));
    await runInDurableObject(substituteStub, async (_i: unknown, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ? ORDER BY partition_key", "idx_evac_d")) as Array<{ partition_key: string }>;
      expect(rows.map((r) => r.partition_key)).toEqual(["row-a", "row-b"]); // no missed entries
    });
    await runInDurableObject(shard0, async (_i: unknown, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ?", "idx_evac_d"));
      expect(rows).toHaveLength(0); // source deleted after convergence
    });
    const statusAfter = (await (await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
    expect(statusAfter.status).toBe("complete");
  });

  // Codex full-PR review P1 E: a queued index_pending_job on a DIFFERENT (base)
  // shard whose targetShardId is the draining shard would, if it fired after the
  // source delete, write an index entry onto the drained shard (outside the
  // ring) — a silent /v1/index-query miss. The drain's precheck only inspected
  // jobs stored ON the draining shard, so this cross-shard job was missed. Ring
  // evacuation must flush such jobs (cluster-wide) before completing, so their
  // entry lands on the draining shard while it is still the ring target and is
  // copied to the substitute.
  it("flushes a queued index job on another base shard that targets the draining shard, so its entry reaches the substitute (not stranded on the drained shard)", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 3, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'ready', ?, ?)",
        "idx_evac_e",
        "events",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify(["shard-0"]),
      );
      state.storage.sql.exec("UPDATE vbucket_map SET shard_id = 'shard-1' WHERE shard_id = 'shard-0'");
    });

    // A base shard (shard-1) has a QUEUED index-write retry whose target is the
    // draining shard (shard-0) — it inserts an entry for idx_evac_e/'row-late'.
    // Seed it directly on shard-1's index_pending_jobs (warm ensureSchema first
    // with a harmless fetch). No alarm is armed by this direct insert, so the
    // catalog's evacuation flush is what must deliver it.
    const shard1 = env.SHARD.get(env.SHARD.idFromName("shard-1"));
    await shard1.fetch(new Request("https://shard.internal/stats", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }));
    const entrySql =
      "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)";
    const entryParams = ["events", "idx_evac_e", JSON.stringify(["alpha"]), "row-late", "shard-1", "t1", new Date().toISOString()];
    await runInDurableObject(shard1, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_pending_jobs (target_shard_id, sql, params_json, request_id, next_attempt_at, attempt_count, created_at) VALUES (?, ?, ?, ?, ?, 0, ?)",
        "shard-0",
        entrySql,
        JSON.stringify(entryParams),
        `late-${crypto.randomUUID()}`,
        new Date().toISOString(),
        new Date().toISOString(),
      );
    });

    await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));

    // Drive ticks to completion.
    for (let tick = 0; tick < 6; tick += 1) {
      const done = await runInDurableObject(stub, async (instance: CatalogDO, state: DurableObjectState) => {
        await instance.alarm();
        const ring = JSON.parse(
          (Array.from(state.storage.sql.exec("SELECT placement_ring_json FROM index_rules WHERE index_name = 'idx_evac_e'")) as Array<{ placement_ring_json: string }>)[0]
            .placement_ring_json,
        ) as string[];
        return !ring.includes("shard-0");
      });
      if (done) break;
    }

    const ringAfter = (await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      JSON.parse(
        (Array.from(state.storage.sql.exec("SELECT placement_ring_json FROM index_rules WHERE index_name = 'idx_evac_e'")) as Array<{ placement_ring_json: string }>)[0]
          .placement_ring_json,
      ),
    )) as string[];
    expect(ringAfter).not.toContain("shard-0");

    // The late job's entry reached the SUBSTITUTE — not stranded on shard-0.
    const substitute = ringAfter[0];
    const substituteStub = env.SHARD.get(env.SHARD.idFromName(substitute));
    await runInDurableObject(substituteStub, async (_i: unknown, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ? AND partition_key = ?", "idx_evac_e", "row-late"));
      expect(rows).toHaveLength(1);
    });
    // No job left queued on the base shard targeting the drained shard.
    await runInDurableObject(shard1, async (_i: unknown, state: DurableObjectState) => {
      const n = (Array.from(state.storage.sql.exec("SELECT COUNT(*) AS n FROM index_pending_jobs WHERE target_shard_id = 'shard-0'")) as Array<{ n: number }>)[0].n;
      expect(n).toBe(0);
    });
    // And nothing stranded on the drained shard.
    const shard0 = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await runInDurableObject(shard0, async (_i: unknown, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ?", "idx_evac_e"));
      expect(rows).toHaveLength(0);
    });
  });

  // Codex round-13 fix (commit 2 — re-resolve on fence): a queued index-job
  // retry whose stored target is being evacuated (INDEX_RING_FENCED) must NOT
  // keep hammering the draining shard — it re-resolves the index's CURRENT ring
  // (via catalog /index-ring) and delivers to the substitute instead.
  it("an index_pending_jobs retry that hits INDEX_RING_FENCED re-resolves the current ring and delivers to the substitute", async () => {
    const shardStale = `idxjob-stale-${crypto.randomUUID()}`;
    const shardSub = `idxjob-sub-${crypto.randomUUID()}`;
    const base = `idxjob-base-${crypto.randomUUID()}`;

    // catalog-0 holds the index_rules row; the CURRENT ring already points at
    // the substitute (as if the evacuation repoint already happened).
    const catalog0 = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    await catalog0.fetch(new Request("https://catalog.internal/list-shards", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }));
    await runInDurableObject(catalog0, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM index_rules WHERE index_name = 'idxjob_reresolve'");
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'ready', ?, ?)",
        "idxjob_reresolve",
        "events",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify([shardSub]),
      );
    });

    // Fence the index on the stale target so a write there 409s.
    const staleStub = env.SHARD.get(env.SHARD.idFromName(shardStale));
    await staleStub.fetch(new Request("https://shard.internal/fence-index-ring", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ indexName: "idxjob_reresolve" }) }));

    // Enqueue a retry job on the base shard whose stored target is the stale
    // (fenced) shard, carrying the structured identity needed to re-resolve.
    const baseStub = env.SHARD.get(env.SHARD.idFromName(base));
    const entrySql =
      "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)";
    await baseStub.fetch(
      new Request("https://shard.internal/enqueue-index-job", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          targetShardId: shardStale,
          sql: entrySql,
          params: ["events", "idxjob_reresolve", JSON.stringify(["alpha"]), "row-rr", shardStale, "t1", new Date().toISOString()],
          requestId: `rr-${crypto.randomUUID()}`,
          indexName: "idxjob_reresolve",
          indexTable: "events",
          indexKeyJson: JSON.stringify(["alpha"]),
        }),
      }),
    );

    // Flush jobs targeting the stale shard → delivery 409s → re-resolves to the
    // substitute (the only shard in the current ring) and lands there.
    const flush = await baseStub.fetch(
      new Request("https://shard.internal/flush-index-jobs-for-target", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ targetShardId: shardStale }) }),
    );
    expect(((await flush.json()) as { remaining: number }).remaining).toBe(0); // job cleared

    // The entry landed on the SUBSTITUTE, not the stale/fenced shard.
    const subStub = env.SHARD.get(env.SHARD.idFromName(shardSub));
    await runInDurableObject(subStub, async (_i: unknown, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ? AND partition_key = ?", "idxjob_reresolve", "row-rr"));
      expect(rows).toHaveLength(1);
    });
    await runInDurableObject(staleStub, async (_i: unknown, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ?", "idxjob_reresolve"));
      expect(rows).toHaveLength(0);
    });
    await runInDurableObject(baseStub, async (_i: unknown, state: DurableObjectState) => {
      const n = (Array.from(state.storage.sql.exec("SELECT COUNT(*) AS n FROM index_pending_jobs")) as Array<{ n: number }>)[0].n;
      expect(n).toBe(0);
    });
  });

  // Codex full-PR review P2: ring evacuation repointed the LOCAL ring first,
  // then fanned /update-index-ring out to siblings. If a sibling call failed,
  // the next tick read the already-repointed local ring (no longer containing
  // the draining shard), skipped this ring, and left the failed sibling routing
  // to the drained shard forever. The fix updates local LAST, so a sibling
  // failure leaves local still showing the draining shard and the next tick
  // retries the whole fan-out until every catalog is consistent.
  it("retries the ring fan-out when a sibling /update-index-ring fails once, so no catalog is left pointing the index at the drained shard", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 3, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES ('idx_ring_partial', 'events', ?, 'ready', ?, ?)",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify(["shard-0"]),
      );
      state.storage.sql.exec("UPDATE vbucket_map SET shard_id = 'shard-1' WHERE shard_id = 'shard-0'");
      // Pretend this catalog is 'catalog-0' in a 2-shard catalog cluster, so
      // ring evacuation fans /update-index-ring out to the sibling 'catalog-1'.
      state.storage.sql.exec("UPDATE cluster_config SET catalog_shard_id = 'catalog-0', catalog_shard_count = 2 WHERE singleton = 1");
    });

    // Seed the identical index_rules row on the real sibling catalog-1.
    const catalog1 = env.CATALOG.get(env.CATALOG.idFromName("catalog-1"));
    await catalog1.fetch(new Request("https://catalog.internal/list-shards", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" })); // trigger ensureSchema
    await runInDurableObject(catalog1, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM index_rules WHERE index_name = 'idx_ring_partial'");
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES ('idx_ring_partial', 'events', ?, 'ready', ?, ?)",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify(["shard-0"]),
      );
    });

    const shard0 = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await shard0.fetch(
      new Request("https://shard.internal/index-entries-import", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          rows: [
            { table_name: "events", index_name: "idx_ring_partial", index_key_json: JSON.stringify(["alpha"]), partition_key: "row-1", source_shard_id: "shard-1", tenant_id: "t1", updated_at: new Date().toISOString() },
          ],
        }),
      }),
    );

    await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));

    // Fail the sibling /update-index-ring the FIRST time, then delegate to the
    // real catalog-1 so it genuinely repoints on retry.
    let ringUpdateAttempts = 0;
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      const inst = instance as unknown as { catalogEnv: { CATALOG: { get: (id: unknown) => { fetch: (i: unknown, init?: unknown) => Promise<Response> } } } };
      // catalogEnv.CATALOG is the shared namespace binding — save and RESTORE
      // its .get so this override doesn't leak into other tests.
      const catalogBinding = inst.catalogEnv.CATALOG;
      const originalGet = catalogBinding.get;
      const realGet = originalGet.bind(catalogBinding);
      catalogBinding.get = (id: unknown) => {
        const realStub = realGet(id);
        return {
          fetch: async (input: unknown, init?: unknown) => {
            const url = typeof input === "string" ? input : (input as Request).url;
            if (url.includes("/update-index-ring")) {
              ringUpdateAttempts += 1;
              if (ringUpdateAttempts === 1) {
                return new Response(JSON.stringify({ error: "boom" }), { status: 500, headers: { "content-type": "application/json" } });
              }
            }
            return realStub.fetch(input as Request, init as RequestInit);
          },
        };
      };
      try {
        for (let tick = 0; tick < 15; tick += 1) {
          await instance.alarm();
        }
      } finally {
        catalogBinding.get = originalGet;
      }
    });

    // The fan-out failed once and was retried.
    expect(ringUpdateAttempts).toBeGreaterThanOrEqual(2);

    const ringOf = async (cat: typeof stub) =>
      (await runInDurableObject(cat, async (_i: CatalogDO, state: DurableObjectState) => {
        const rows = Array.from(state.storage.sql.exec("SELECT placement_ring_json FROM index_rules WHERE index_name = 'idx_ring_partial'")) as Array<{ placement_ring_json: string }>;
        return rows.length ? (JSON.parse(rows[0].placement_ring_json) as string[]) : [];
      }));

    // BOTH catalogs are repointed away from the drained shard — the local one
    // AND the sibling that initially failed (without the fix the sibling would
    // stay on [shard-0] forever after the first-tick failure repointed local).
    expect(await ringOf(stub), "local (catalog-0) ring must be repointed").not.toContain("shard-0");
    expect(await ringOf(catalog1), "sibling (catalog-1) ring must be repointed").not.toContain("shard-0");
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

  // Codex full-PR review P1 C: a vbucket wedged in 'aborting' on the draining
  // shard livelocked the drain — advanceDrain returned true forever (the alarm
  // only advances 'backfilling'/'cutover'), so the 250ms alarm spun and
  // /drain-shard-status showed 'migrating-vbuckets', never a stall. The drain
  // must PARK on a non-advancing in-flight status with a distinct reason.
  it("a vbucket stuck 'aborting' on the draining shard parks the drain ('aborting-migration' stall), it does not livelock as 'migrating-vbuckets'", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));

    // shard-0 draining, with one of its vbuckets wedged in 'aborting' (an abort
    // whose cleanup failed). 'aborting' is never advanced by the migration loop.
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE shards SET status = 'draining', drain_stall_reason = NULL WHERE shard_id = 'shard-0'");
      state.storage.sql.exec(
        "UPDATE vbucket_map SET migration_status = 'aborting', target_shard_id = 'shard-1', updated_at = ? WHERE vbucket = 0 AND shard_id = 'shard-0'",
        new Date().toISOString(),
      );
    });

    // A single tick must park the drain, not spin.
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });

    const parked = (await (await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as {
      status: string;
      stallReason: string | null;
    };
    expect(parked.stallReason).toBe("aborting-migration");
    expect(parked.status).toBe("stalled"); // NOT the livelocking 'migrating-vbuckets'
  });
});

// Codex round-13 KILLER: an index write that resolved the OLD ring before the
// repoint (here a delayed/queued LABELLED write to the draining shard) must,
// once that shard is fenced during a REAL ring evacuation, be turned away
// (409 INDEX_RING_FENCED) and RE-RESOLVED to the substitute — never stranded on
// the shard about to be decommissioned. This runs the whole subsystem end to
// end: fence-first evacuation (commit 3) + the writer/retry re-resolve on fence
// (commit 2), tied together by a real /drain-shard. It uses catalog-0 directly
// so the shard's re-resolve (ShardDO→CatalogDO catalog-0 /index-ring) resolves
// against this same catalog's live, already-repointed ring.
describe("CatalogDO index-ring write fence — round-13 end-to-end", () => {
  it("a delayed index write that resolved the old ring lands on the SUBSTITUTE (fence→re-resolve), not stranded on the drained shard; the drain then completes", async () => {
    // catalog-0 (not a random freshCatalog) so ShardDO.resolveIndexRing, which
    // canonically asks catalog-0, hits this instance. Single-catalog cluster
    // (no catalogShardId → siblingCount 0), so ring repoint is local-only.
    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    await stub.fetch(post("/init", { numShards: 3, totalVBuckets: 4, force: true }, `Bearer ${env.ADMIN_TOKEN}`));
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM index_rules WHERE index_name = 'idx_kill'");
      state.storage.sql.exec("DELETE FROM drain_ring_evac");
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'ready', ?, ?)",
        "idx_kill",
        "events",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify(["shard-0"]),
      );
      // Move vbuckets off shard-0 so the drain goes straight to ring evacuation.
      state.storage.sql.exec("UPDATE vbucket_map SET shard_id = 'shard-1' WHERE shard_id = 'shard-0'");
      // Reset any leftover fence/entries on shard-0's index (fixed DO name).
    });

    // A real, already-present entry on shard-0 (will be copied to the substitute).
    const shard0 = env.SHARD.get(env.SHARD.idFromName("shard-0"));
    await shard0.fetch(new Request("https://shard.internal/stats", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }));
    await runInDurableObject(shard0, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM __cf_indexes WHERE index_name = 'idx_kill'");
      state.storage.sql.exec("DELETE FROM __cf_fenced_index_rings WHERE index_name = 'idx_kill'");
    });
    await shard0.fetch(
      new Request("https://shard.internal/index-entries-import", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          rows: [{ table_name: "events", index_name: "idx_kill", index_key_json: JSON.stringify(["alpha"]), partition_key: "row-present", source_shard_id: "shard-1", tenant_id: "t1", updated_at: new Date().toISOString() }],
        }),
      }),
    );

    // Drain shard-0 and drive the ring evacuation to completion. The index is
    // now fenced on shard-0, the ring is repointed to the substitute, and
    // shard-0's entries are deleted.
    await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));
    for (let tick = 0; tick < 8; tick += 1) {
      const done = await runInDurableObject(stub, async (instance: CatalogDO, state: DurableObjectState) => {
        await instance.alarm();
        const marker = Array.from(state.storage.sql.exec("SELECT index_name FROM drain_ring_evac WHERE shard_id = 'shard-0'")).length;
        const ring = JSON.parse(
          (Array.from(state.storage.sql.exec("SELECT placement_ring_json FROM index_rules WHERE index_name = 'idx_kill'")) as Array<{ placement_ring_json: string }>)[0].placement_ring_json,
        ) as string[];
        return marker === 0 && !ring.includes("shard-0");
      });
      if (done) break;
    }

    const ringAfter = (await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      JSON.parse((Array.from(state.storage.sql.exec("SELECT placement_ring_json FROM index_rules WHERE index_name = 'idx_kill'")) as Array<{ placement_ring_json: string }>)[0].placement_ring_json),
    )) as string[];
    expect(ringAfter).not.toContain("shard-0");
    const substitute = ringAfter[0];

    // THE KILLER: NOW — after the flip — an index write that had resolved the
    // OLD ring [shard-0] before the repoint finally arrives (its retry was
    // queued on base shard-1, still targeting shard-0). Without the fence it
    // would deliver to the drained shard-0 and be stranded (the ring points at
    // the substitute, so /v1/index-query would miss it). WITH the fence (left
    // set on shard-0 through decommission), the delivery 409s INDEX_RING_FENCED
    // and re-resolves to the substitute.
    const shard1 = env.SHARD.get(env.SHARD.idFromName("shard-1"));
    await shard1.fetch(new Request("https://shard.internal/stats", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }));
    await runInDurableObject(shard1, async (_i: unknown, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM index_pending_jobs");
      state.storage.sql.exec(
        "INSERT INTO index_pending_jobs (target_shard_id, sql, params_json, request_id, next_attempt_at, attempt_count, created_at, index_name, index_table, index_key_json) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
        "shard-0",
        "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        JSON.stringify(["events", "idx_kill", JSON.stringify(["alpha"]), "row-straggler", "shard-1", "t1", new Date().toISOString()]),
        `strag-${crypto.randomUUID()}`,
        new Date().toISOString(),
        new Date().toISOString(),
        "idx_kill",
        "events",
        JSON.stringify(["alpha"]),
      );
    });
    // Deliver the straggler (its retry fires).
    await shard1.fetch(new Request("https://shard.internal/flush-index-jobs-for-target", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ targetShardId: "shard-0" }) }));

    // The straggler landed on the SUBSTITUTE (fence→re-resolve), NOT shard-0.
    const substitute2 = ringAfter[0];
    const subStub = env.SHARD.get(env.SHARD.idFromName(substitute2));
    const subPks = await runInDurableObject(subStub, async (_i: unknown, state: DurableObjectState) =>
      (Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = 'idx_kill' ORDER BY partition_key")) as Array<{ partition_key: string }>).map((r) => r.partition_key),
    );
    expect(subPks).toEqual(["row-present", "row-straggler"]); // both reachable via the current ring

    // Nothing stranded on the drained shard, and the retry job is cleared.
    await runInDurableObject(shard0, async (_i: unknown, state: DurableObjectState) => {
      expect(Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = 'idx_kill'"))).toHaveLength(0);
    });
    await runInDurableObject(shard1, async (_i: unknown, state: DurableObjectState) => {
      const n = (Array.from(state.storage.sql.exec("SELECT COUNT(*) AS n FROM index_pending_jobs WHERE target_shard_id = 'shard-0'")) as Array<{ n: number }>)[0].n;
      expect(n).toBe(0);
    });
    const status = (await (await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
    expect(status.status).toBe("complete");
  });
});

// Approved design: a durable cluster-wide topology-operation lock serializes
// every topology mutation. Stage 1 — the lock mechanism on CatalogDO.
describe("CatalogDO topology-operation lock (Stage 1 mechanism)", () => {
  it("CAS acquire: a live lock blocks a second acquire (409); heartbeat/release/holds/status behave; an expired lock is takeable", async () => {
    const stub = await freshCatalog();

    // Acquire.
    const acq = await stub.fetch(post("/acquire-topology-lock", { operationType: "drain-shard" }));
    expect(acq.status).toBe(200);
    const acqBody = (await acq.json()) as { ok: boolean; operationId: string };
    expect(acqBody.ok).toBe(true);
    const opId = acqBody.operationId;

    // A second acquire while held → 409 with the holder's info.
    const dup = await stub.fetch(post("/acquire-topology-lock", { operationType: "create-index" }));
    expect(dup.status).toBe(409);
    const dupBody = (await dup.json()) as { error: { code: string; operationType: string; operationId: string } };
    expect(dupBody.error.code).toBe("TOPOLOGY_OPERATION_IN_PROGRESS");
    expect(dupBody.error.operationType).toBe("drain-shard");
    expect(dupBody.error.operationId).toBe(opId);

    // holds: true for the holder, false for a stranger.
    expect(((await (await stub.fetch(post("/holds-topology-lock", { operationId: opId }))).json()) as { holds: boolean }).holds).toBe(true);
    expect(((await (await stub.fetch(post("/holds-topology-lock", { operationId: "someone-else" }))).json()) as { holds: boolean }).holds).toBe(false);

    // status reflects the holder.
    const st = (await (await stub.fetch(post("/topology-lock-status", {}))).json()) as { held: boolean; operationId: string; expired: boolean };
    expect(st.held).toBe(true);
    expect(st.operationId).toBe(opId);
    expect(st.expired).toBe(false);

    // heartbeat: correct id refreshes; wrong id → LOCK_LOST.
    expect((await stub.fetch(post("/heartbeat-topology-lock", { operationId: opId }))).status).toBe(200);
    const hbBad = await stub.fetch(post("/heartbeat-topology-lock", { operationId: "not-me" }));
    expect(hbBad.status).toBe(409);
    expect(((await hbBad.json()) as { error: { code: string } }).error.code).toBe("LOCK_LOST");

    // release: wrong id is a no-op (lock still held); correct id frees it.
    expect(((await (await stub.fetch(post("/release-topology-lock", { operationId: "not-me" }))).json()) as { released: boolean }).released).toBe(false);
    expect(((await (await stub.fetch(post("/holds-topology-lock", { operationId: opId }))).json()) as { holds: boolean }).holds).toBe(true);
    expect(((await (await stub.fetch(post("/release-topology-lock", { operationId: opId }))).json()) as { released: boolean }).released).toBe(true);
    expect(((await (await stub.fetch(post("/topology-lock-status", {}))).json()) as { held: boolean }).held).toBe(false);

    // After release a fresh acquire succeeds.
    const acq2 = await stub.fetch(post("/acquire-topology-lock", { operationType: "migrate-vbucket" }));
    expect(acq2.status).toBe(200);
    const opId2 = ((await acq2.json()) as { operationId: string }).operationId;

    // An EXPIRED lock is takeable: force expiry, then a new acquire succeeds and
    // the old holder no longer holds it.
    await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE topology_lock SET expires_at = ? WHERE singleton = 1", new Date(Date.now() - 1000).toISOString());
    });
    expect(((await (await stub.fetch(post("/holds-topology-lock", { operationId: opId2 }))).json()) as { holds: boolean }).holds).toBe(false); // expired
    const acq3 = await stub.fetch(post("/acquire-topology-lock", { operationType: "split-vbucket" }));
    expect(acq3.status).toBe(200);
    const opId3 = ((await acq3.json()) as { operationId: string }).operationId;
    expect(opId3).not.toBe(opId2);
    // The stale holder's heartbeat now fails (someone else holds it).
    expect((await stub.fetch(post("/heartbeat-topology-lock", { operationId: opId2 }))).status).toBe(409);
  });
});

// Approved design: Stage 3 wires the topology lock into the LONG-running
// migration/drain state machines — held for the whole multi-tick operation,
// heartbeated every tick, released only on full completion (or park if lost).
describe("CatalogDO topology-operation lock — Stage 3 (long-running operations)", () => {
  // The lock always lives on the PHYSICAL "catalog-0" DO (CatalogDO detects
  // "am I catalog-0" by DO identity, not by cluster_config — see
  // isCatalogZero) regardless of which catalog instance is running the
  // migration/drain. freshCatalog() mints a random-named catalog for test
  // isolation, so the tests below acquire/inspect/release the lock against
  // the REAL catalog-0 stub — exactly what the Worker's acquireTopologyLock
  // does in production — while driving the migration/drain on the
  // freshCatalog() instance itself.
  function catalogZero() {
    return env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
  }

  it("a Worker-mediated migration heartbeats the topology lock every tick and releases it only once cleanup (the migration's TRUE end) completes", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    const lockStub = catalogZero();

    const acq = (await (await lockStub.fetch(post("/acquire-topology-lock", { operationType: "migrate-vbucket" }))).json()) as { operationId: string };
    const opId = acq.operationId;

    const mig = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "shard-lockheld-target", operationId: opId }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(mig.status).toBe(200);

    // The lock is recorded on the vbucket_map row and still held mid-flight.
    const midRow = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      (Array.from(state.storage.sql.exec("SELECT topology_lock_operation_id FROM vbucket_map WHERE vbucket = 0")) as Array<{ topology_lock_operation_id: string | null }>)[0],
    );
    expect(midRow.topology_lock_operation_id).toBe(opId);
    expect(((await (await lockStub.fetch(post("/holds-topology-lock", { operationId: opId }))).json()) as { holds: boolean }).holds).toBe(true);

    // Drive the migration to completion (backfill -> cutover -> flip -> cleanup).
    for (let tick = 0; tick < 15; tick += 1) {
      await runInDurableObject(stub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const s = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
      if (s.status === "none") break;
    }
    const final = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string };
    expect(final.status).toBe("none"); // completed

    // The lock was released — held by no one now — and the row's reference cleared.
    expect(((await (await lockStub.fetch(post("/holds-topology-lock", { operationId: opId }))).json()) as { holds: boolean }).holds).toBe(false);
    expect(((await (await lockStub.fetch(post("/topology-lock-status", {}))).json()) as { held: boolean }).held).toBe(false);
    const afterRow = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      (Array.from(state.storage.sql.exec("SELECT topology_lock_operation_id FROM vbucket_map WHERE vbucket = 0")) as Array<{ topology_lock_operation_id: string | null }>)[0],
    );
    expect(afterRow.topology_lock_operation_id).toBeNull();
  });

  it("if the topology lock is released out from under an in-flight migration (force-release), the next tick PARKS (no mutation) instead of continuing; the lock is then free for a new operation", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 1, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    const lockStub = catalogZero();

    const acq = (await (await lockStub.fetch(post("/acquire-topology-lock", { operationType: "migrate-vbucket" }))).json()) as { operationId: string };
    const opId = acq.operationId;
    const mig = await stub.fetch(post("/migrate-vbucket", { vbucket: 0, targetShardId: "shard-lostlock-target", operationId: opId }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(mig.status).toBe(200);
    const before = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string; rowsCopied: number };
    expect(before.status).toBe("backfilling"); // freshly started, no tick driven yet

    // Simulate an operator force-releasing the lock (Stage 4's
    // /admin/force-release-topology-lock does exactly this at the route
    // level; released here directly since that's a Worker-level admin route)
    // BEFORE this migration ever gets a tick — the fresh case of "lost from
    // the very start of its lifecycle."
    const rel = await lockStub.fetch(post("/release-topology-lock", { operationId: opId }));
    expect(((await rel.json()) as { released: boolean }).released).toBe(true);

    // The next tick must NOT advance this migration at all — it has no lock.
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const after = (await (await stub.fetch(post("/migrate-vbucket-status", { vbucket: 0 }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { status: string; rowsCopied: number };
    expect(after.status).toBe(before.status); // no state transition
    expect(after.rowsCopied).toBe(before.rowsCopied); // no further copying

    // The lock itself is genuinely free again — a brand-new operation can
    // acquire it (this migration's own stale operationId can never heartbeat
    // successfully again, by design — an operator must intervene).
    const acq2 = await lockStub.fetch(post("/acquire-topology-lock", { operationType: "create-index" }));
    expect(acq2.status).toBe(200);
  });

  it("a drain HOLDS its topology lock across multiple sub-migrations (one vbucket's own migration completing does NOT prematurely release it) and releases only on the drain's TRUE completion", async () => {
    const stub = await freshCatalog();
    // 2 vbuckets on shard-0, one active target — the drain migrates them
    // SEQUENTIALLY, so vbucket 0's migration can fully complete while vbucket
    // 1 (and the drain overall) is still in flight.
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 2 }, `Bearer ${env.ADMIN_TOKEN}`));
    const lockStub = catalogZero();

    const acq = (await (await lockStub.fetch(post("/acquire-topology-lock", { operationType: "drain-shard" }))).json()) as { operationId: string };
    const opId = acq.operationId;
    const drainRes = await stub.fetch(post("/drain-shard", { shardId: "shard-0", operationId: opId }, `Bearer ${env.ADMIN_TOKEN}`));
    expect(drainRes.status).toBe(200);

    const shardRow = () =>
      runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
        (Array.from(state.storage.sql.exec("SELECT topology_lock_operation_id FROM shards WHERE shard_id = 'shard-0'")) as Array<{ topology_lock_operation_id: string | null }>)[0],
      );
    expect((await shardRow()).topology_lock_operation_id).toBe(opId);

    // Drive ticks; stop the instant vbucket 0's OWN migration is done (status
    // 'none') but the drain overall is NOT complete yet (shard.status still
    // 'draining' — 2 vbuckets, only 1 fully migrated).
    let vbucket0Done = false;
    for (let tick = 0; tick < 30 && !vbucket0Done; tick += 1) {
      await runInDurableObject(stub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const row = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
        (Array.from(state.storage.sql.exec("SELECT migration_status FROM vbucket_map WHERE vbucket = 0")) as Array<{ migration_status: string }>)[0],
      );
      if (row.migration_status === "none") vbucket0Done = true;
    }
    expect(vbucket0Done).toBe(true);

    // THE REGRESSION THIS GUARDS: vbucket 0's sub-migration completing must
    // NOT have released the drain's shared lock — the drain (vbucket 1, then
    // ring evacuation if any) still needs it.
    expect(((await (await lockStub.fetch(post("/holds-topology-lock", { operationId: opId }))).json()) as { holds: boolean }).holds).toBe(true);
    expect((await shardRow()).topology_lock_operation_id).toBe(opId);

    // Drive to the drain's actual completion. /init clamps totalVBuckets to a
    // floor of 64 regardless of what's requested, so shard-0 (half of a
    // 2-shard round robin) starts this loop owning ~30 vbuckets — one
    // migrates per tick sequentially, so the budget must comfortably exceed
    // that count. /drain-shard-status's "complete" is a pure read (derived
    // from vbuckets/rings remaining hitting zero) that can turn true ONE tick
    // before advanceDrain itself re-runs with vbuckets.length===0 and
    // performs the actual release — so wait for the LOCK to clear too, not
    // just the status string.
    let complete = false;
    for (let tick = 0; tick < 85 && !complete; tick += 1) {
      await runInDurableObject(stub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const statusRes = await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));
      const statusBody = (await statusRes.json()) as { status: string };
      const holds = ((await (await lockStub.fetch(post("/holds-topology-lock", { operationId: opId }))).json()) as { holds: boolean }).holds;
      if (statusBody.status === "complete" && !holds) complete = true;
    }
    expect(complete).toBe(true);

    // NOW it's released.
    expect(((await (await lockStub.fetch(post("/holds-topology-lock", { operationId: opId }))).json()) as { holds: boolean }).holds).toBe(false);
    expect((await shardRow()).topology_lock_operation_id).toBeNull();
  });

  it("a drain that loses its topology lock parks with a distinguishable 'topology-lock-lost' stall (not 'complete'/'migrating-vbuckets') and does not mutate; recovers if the SAME operationId is restored", async () => {
    const stub = await freshCatalog();
    await stub.fetch(post("/init", { numShards: 2, totalVBuckets: 2 }, `Bearer ${env.ADMIN_TOKEN}`));
    const lockStub = catalogZero();

    const acq = (await (await lockStub.fetch(post("/acquire-topology-lock", { operationType: "drain-shard" }))).json()) as { operationId: string };
    const opId = acq.operationId;
    await stub.fetch(post("/drain-shard", { shardId: "shard-0", operationId: opId }, `Bearer ${env.ADMIN_TOKEN}`));

    // Force-release out from under the drain.
    await lockStub.fetch(post("/release-topology-lock", { operationId: opId }));

    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const parked = (await (await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as {
      status: string;
      stallReason: string | null;
    };
    expect(parked.stallReason).toBe("topology-lock-lost");
    expect(parked.status).not.toBe("complete");
    expect(parked.status).not.toBe("migrating-vbuckets"); // distinguishable, not misleadingly "just working"

    // Restoring a lock recorded under this exact operationId (a direct
    // simulation of "the lock situation resolved") lets the very next tick's
    // heartbeat succeed again, clearing the soft stall.
    await runInDurableObject(lockStub, async (_i: CatalogDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        "INSERT OR REPLACE INTO topology_lock (singleton, operation_id, operation_type, acquired_at, expires_at, heartbeat_at) VALUES (1, ?, 'drain-shard', ?, ?, ?)",
        opId,
        now,
        new Date(Date.now() + 30_000).toISOString(),
        now,
      );
    });
    await runInDurableObject(stub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const recovered = (await (await stub.fetch(post("/drain-shard-status", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`))).json()) as { stallReason: string | null };
    expect(recovered.stallReason).not.toBe("topology-lock-lost");
  });
});

// Approved design: round-16 defense-in-depth hardening (correct regardless of
// whether the topology lock did its job) for the two direct concurrency
// findings.
describe("CatalogDO ring evacuation — round-16 defense-in-depth: re-read the ring at write time", () => {
  it("repointing shard-0's OWN ring position does NOT revert a DIFFERENT position's substitution applied concurrently (e.g. a sibling catalog's own drain) during the awaited fence call", async () => {
    const stub = await freshCatalog();
    // 5 shards: the ring pins shard-0/1/2, leaving shard-3/4 as active
    // out-of-ring candidates so a substitute for shard-0's drain can actually
    // be found (RING_EVACUATION_NO_CANDIDATE otherwise pre-rejects the drain).
    await stub.fetch(post("/init", { numShards: 5, totalVBuckets: 4 }, `Bearer ${env.ADMIN_TOKEN}`));
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'ready', ?, ?)",
        "idx_r16a",
        "events",
        JSON.stringify(["v"]),
        new Date().toISOString(),
        JSON.stringify(["shard-0", "shard-1", "shard-2"]),
      );
      // Move every vbucket off shard-0 so its drain goes straight to ring
      // evacuation (phase 2) — no vbucket migration in the way.
      state.storage.sql.exec("UPDATE vbucket_map SET shard_id = 'shard-1' WHERE shard_id = 'shard-0'");
    });
    await stub.fetch(post("/drain-shard", { shardId: "shard-0" }, `Bearer ${env.ADMIN_TOKEN}`));

    // Monkey-patch callShard so shard-0's OWN /fence-index-ring call (step b,
    // BEFORE the repoint at step c reads the ring) ALSO — as a side effect —
    // applies a DIFFERENT position's substitution directly, simulating a
    // sibling catalog's own concurrent drain of shard-2 completing its own
    // /update-index-ring fan-out during this call's await. THE FIX BEING
    // TESTED: the repoint step must RE-READ the ring at write time so it only
    // touches shard-0's own position (0), never reverting shard-2's (position
    // 2) concurrent substitution back with a stale full-array snapshot.
    await runInDurableObject(stub, async (instance: CatalogDO, state: DurableObjectState) => {
      const inst = instance as unknown as {
        callShard: (s: string, p: string, b: unknown) => Promise<Response>;
        __real?: (s: string, p: string, b: unknown) => Promise<Response>;
      };
      inst.__real = inst.callShard.bind(instance);
      inst.callShard = async (shardId, path, payload) => {
        const res = await inst.__real!(shardId, path, payload);
        if (shardId === "shard-0" && path === "/fence-index-ring") {
          state.storage.sql.exec(
            "UPDATE index_rules SET placement_ring_json = ? WHERE index_name = 'idx_r16a'",
            JSON.stringify(["shard-0", "shard-1", "shard-2-concurrent-substitute"]),
          );
        }
        return res;
      };
      await instance.alarm();
    });

    const ringAfter = await runInDurableObject(stub, async (_i: CatalogDO, state: DurableObjectState) =>
      JSON.parse(
        (Array.from(state.storage.sql.exec("SELECT placement_ring_json FROM index_rules WHERE index_name = 'idx_r16a'")) as Array<{ placement_ring_json: string }>)[0]
          .placement_ring_json,
      ) as string[],
    );
    // Position 0 (shard-0) was substituted by THIS drain.
    expect(ringAfter[0]).not.toBe("shard-0");
    // Position 2's CONCURRENT substitution must survive — not reverted back
    // to "shard-2" by this drain's stale-snapshot-based write.
    expect(ringAfter[2]).toBe("shard-2-concurrent-substitute");
    // Position 1 (untouched by either drain) is unaffected.
    expect(ringAfter[1]).toBe("shard-1");
  });
});
