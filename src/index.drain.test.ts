import { SELF, env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { hashKey, indexShardIdForKey } from "./hash";
import { sha256Hex } from "./auth";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";
import { AUTH, createIndexTestTable, driveIndexBackfillToCompletion, driveMigrationToCompletion, initCluster, post, registerTenant, tenantForCatalogShard } from "./index.test-helpers";

// This file is one of several index.*.test.ts files split out of a single
// index.test.ts (see index.test-helpers.ts's header comment for why). DO
// storage persists across `it` blocks within a file, so afterEach(reset())
// gives every test clean storage — the same isolation the pre-split file used.
afterEach(async () => {
  await reset();
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

  // Milestone 2 eng-review fix: draining is now blocked while any index is
  // registered, cluster-wide (index-shard placement hashes over the active
  // shard set, so draining orphans __cf_indexes entries). index_rules isn't
  // cleared by /admin/init's force:true (only vbucket_map/shards/
  // cluster_config are), so an index registered by an earlier describe block
  // in this file persists on catalog-0's storage and would otherwise leak
  // into these unrelated pre-existing drain tests. Clear it directly so this
  // block tests drain-vs-2PC/index-job interaction only, not index presence.
  beforeEach(async () => {
    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    // ensureSchema() only runs on fetch() — warm it first so the direct
    // storage access below can't hit "no such table" when this block runs
    // before any other catalog-0 traffic (e.g. under a -t filter).
    await stub.fetch(
      new Request("https://catalog.internal/list-shards", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }),
    );
    await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM index_rules");
    });
  });

  it("drains a shard with zero pending intents unchanged", async () => {
    await initCluster(1, 4);
    const res = await post("/admin/drain-shard", { shardId: "catalog-0-shard-0", catalogShardId: "catalog-0" }, AUTH());
    expect(res.status).toBe(200);
  });

  it("Milestone 3, Chunks 2+5 E2E (criteria 4 & 5): init -> table -> index -> writes -> split -> drain the split source — completes end-to-end with every row readable and every index entry resolving, via vbucket evacuation + deterministic ring substitution (both former 409 codes unreachable)", async () => {
    // 8 shards per catalog: 64/8 = 8 vbuckets per catalog-0 shard, keeping
    // the sequential drain's migration count manageable.
    await post("/admin/init", { numShards: 8, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m5_e2e_evt");
    const createIndexRes = await post("/admin/create-index", { indexName: "idx_m5_e2e_by_v", table: "m5_e2e_evt", columns: ["v"] }, AUTH());
    expect(createIndexRes.status).toBe(200);
    // numShards:8 per catalog x 4 catalogs = 32 data shards to scan -- even
    // with only 12 rows total (mostly-empty shards each cost one tick to
    // detect-and-advance), that's above driveIndexBackfillToCompletion's
    // default tick budget.
    await driveIndexBackfillToCompletion("idx_m5_e2e_by_v", 40);
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // Writes: 12 rows, v alternating alpha/beta, spread across vbuckets.
    const ids = Array.from({ length: 12 }, (_, i) => `e2e-${i}`);
    for (const [i, id] of ids.entries()) {
      const res = await post(
        "/v1/mutate",
        { op: "insert", table: "m5_e2e_evt", tenantId, partitionKey: id, values: { v: i % 2 === 0 ? "alpha" : "beta" } },
        token,
      );
      expect(res.status).toBe(200);
    }

    // Async index maintenance settles: both value classes fully queryable.
    const queryCount = async (v: string): Promise<number> => {
      const res = await post("/v1/index-query", { table: "m5_e2e_evt", indexName: "idx_m5_e2e_by_v", tenantId, values: { v }, limit: 50 }, token);
      expect(res.status).toBe(200);
      return ((await res.json()) as { rows: unknown[] }).rows.length;
    };
    for (let attempt = 0; attempt < 100; attempt += 1) {
      if ((await queryCount("alpha")) === 6 && (await queryCount("beta")) === 6) break;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    expect(await queryCount("alpha")).toBe(6);
    expect(await queryCount("beta")).toBe(6);

    // SPLIT: move e2e-0's vbucket onto a brand-new shard (created by the
    // split itself — its table schema arrives via the migration's
    // schema-provisioning step, since create-table's fan-out predates it).
    const splitVbucket = hashKey(`${tenantId}:m5_e2e_evt:e2e-0`) % 64;
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const splitSource = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", splitVbucket)) as Array<{ shard_id: string }>;
      return rows[0].shard_id;
    });
    const splitTarget = "catalog-0-shard-split-e2e";
    const splitRes = await post("/admin/split-vbucket", { catalogShardId: "catalog-0", vbucket: splitVbucket, newShardId: splitTarget }, AUTH());
    expect(splitRes.status).toBe(200);
    expect(((await splitRes.json()) as { migrationStarted: boolean }).migrationStarted).toBe(true);
    await driveMigrationToCompletion(splitVbucket);

    // The moved row reads back through the data plane from the new shard.
    const movedRead = await post(
      "/v1/sql",
      { sql: "SELECT v FROM m5_e2e_evt WHERE id = ?", params: ["e2e-0"], table: "m5_e2e_evt", tenantId, partitionKey: "e2e-0" },
      AUTH(),
    );
    const movedBody = (await movedRead.json()) as { route: { shardId: string }; result: { rows: Array<{ v: string }> } };
    expect(movedBody.route.shardId).toBe(splitTarget);
    expect(movedBody.result.rows[0].v).toBe("alpha");

    // DRAIN the split source. Its pinned-ring membership makes this exercise
    // ring evacuation too; the only active shard outside the ring is the
    // split-created one, so the deterministic substitute must be exactly it.
    const drainRes = await post("/admin/drain-shard", { shardId: splitSource, catalogShardId: "catalog-0" }, AUTH());
    expect(drainRes.status).toBe(200);

    let drainStatus: { vbucketsRemaining: number; ringsRemaining: number; status: string } | null = null;
    for (let tick = 0; tick < 40; tick += 1) {
      await runInDurableObject(catalogStub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const statusRes = await post("/admin/drain-shard-status", { catalogShardId: "catalog-0", shardId: splitSource }, AUTH());
      expect(statusRes.status).toBe(200);
      drainStatus = (await statusRes.json()) as { vbucketsRemaining: number; ringsRemaining: number; status: string };
      if (drainStatus.status === "complete") break;
    }
    expect(drainStatus?.status).toBe("complete");
    expect(drainStatus?.vbucketsRemaining).toBe(0);
    expect(drainStatus?.ringsRemaining).toBe(0);

    // Every row is still readable through the data plane, none on the
    // drained shard.
    for (const [i, id] of ids.entries()) {
      const readRes = await post(
        "/v1/sql",
        { sql: "SELECT v FROM m5_e2e_evt WHERE id = ?", params: [id], table: "m5_e2e_evt", tenantId, partitionKey: id },
        AUTH(),
      );
      expect(readRes.status).toBe(200);
      const readBody = (await readRes.json()) as { route: { shardId: string }; result: { rows: Array<{ v: string }> } };
      expect(readBody.route.shardId).not.toBe(splitSource);
      expect(readBody.result.rows).toHaveLength(1);
      expect(readBody.result.rows[0].v).toBe(i % 2 === 0 ? "alpha" : "beta");
    }

    // The index survived registered, queryable, and complete — no dropped
    // matches (criteria 4 & 5), with the ring's drained slot substituted by
    // the split shard (the only out-of-ring candidate) and the drained
    // shard's entry copies gone.
    expect(await queryCount("alpha")).toBe(6);
    expect(await queryCount("beta")).toBe(6);
    const listRes = await post("/admin/list-indexes", {}, AUTH());
    const listBody = (await listRes.json()) as { indexes: Array<{ indexName: string; status: string; placementRing: string[] }> };
    const idx = listBody.indexes.find((i) => i.indexName === "idx_m5_e2e_by_v");
    expect(idx?.status).toBe("ready");
    expect(idx?.placementRing).not.toContain(splitSource);
    expect(idx?.placementRing).toContain(splitTarget);

    const drainedStub = env.SHARD.get(env.SHARD.idFromName(splitSource));
    await runInDurableObject(drainedStub, async (_instance: ShardDO, state: DurableObjectState) => {
      const entries = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ?", "idx_m5_e2e_by_v"));
      expect(entries).toHaveLength(0);
      const baseRows = Array.from(state.storage.sql.exec("SELECT id FROM m5_e2e_evt"));
      expect(baseRows).toHaveLength(0);
    });
  }, 120000);

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
      AUTH(),
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
