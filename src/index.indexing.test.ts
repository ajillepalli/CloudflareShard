import { SELF, env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { hashKey, indexShardIdForKey } from "./hash";
import { sha256Hex } from "./auth";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";
import { ALL_TEST_SHARD_IDS, AUTH, createIndexTestTable, initCluster, pollIndexRows, post, registerTenant, tenantForCatalogShard } from "./index.test-helpers";

// This file is one of several index.*.test.ts files split out of a single
// index.test.ts (see index.test-helpers.ts's header comment for why). DO
// storage persists across `it` blocks within a file, so afterEach(reset())
// gives every test clean storage — the same isolation the pre-split file used.
afterEach(async () => {
  await reset();
});

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

  // Codex full-PR review P1 (silent index miss): an index created AFTER a
  // shard is marked draining but BEFORE its vbuckets migrate must still index
  // that shard's existing rows — the backfill scan is drain-aware (active +
  // draining). The placement RING stays active-only, so the draining shard is
  // never pinned; its rows' entries land on the active ring and carry
  // tenant_id, so /v1/index-query hydrates them via the live vbucket_map.
  it("backfills a DRAINING shard's rows so /v1/index-query doesn't silently miss them, while the placement ring stays active-only", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_draining_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // Insert via the normal path so __cf_row_owners provenance is recorded
    // (create-index's backfill requires it). numShards:1 → the tenant's
    // catalog (catalog-0) has a single shard, so row-1 lands on
    // catalog-0-shard-0.
    await post("/v1/mutate", { op: "insert", table: "idx_draining_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);

    // Mark that shard 'draining' — it still physically holds row-1 (its
    // vbuckets haven't migrated). Every other catalog's shard stays active.
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    await runInDurableObject(catalogStub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE shards SET status = 'draining' WHERE shard_id = ?", "catalog-0-shard-0");
    });

    // Create the index AFTER the shard is draining. The drain-aware backfill
    // must still index row-1.
    const res = await post("/admin/create-index", { indexName: "idx_draining_by_v", table: "idx_draining_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(200);

    // The row is queryable — not silently missed.
    const queryRes = await post("/v1/index-query", { table: "idx_draining_evt", indexName: "idx_draining_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(queryRes.status).toBe(200);
    const queryBody = (await queryRes.json()) as { rows: Array<{ id: string; v: string }> };
    expect(queryBody.rows).toHaveLength(1);
    expect(queryBody.rows[0].id).toBe("row-1");

    // The placement ring is active-only — it must not contain the draining shard.
    const listRes = await post("/admin/list-indexes", {}, AUTH());
    const listBody = (await listRes.json()) as { indexes: Array<{ indexName: string; placementRing?: string[] }> };
    const ring = listBody.indexes.find((i) => i.indexName === "idx_draining_by_v")?.placementRing ?? [];
    expect(ring.length).toBeGreaterThan(0);
    expect(ring).not.toContain("catalog-0-shard-0");
  });

  it("is idempotent: retrying the same indexName+table+columns succeeds instead of 409 (eng-review fix — needed so a caller can retry after a partial backfill failure)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_dup_evt");
    const first = await post("/admin/create-index", { indexName: "idx_dup_by_v", table: "idx_dup_evt", columns: ["v"] }, AUTH());
    expect(first.status).toBe(200);
    const second = await post("/admin/create-index", { indexName: "idx_dup_by_v", table: "idx_dup_evt", columns: ["v"] }, AUTH());
    expect(second.status).toBe(200);
  });

  it("rejects reusing an indexName with different table/columns as a genuine conflict", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_conflict_evt_a");
    await createIndexTestTable("idx_conflict_evt_b");
    const first = await post("/admin/create-index", { indexName: "idx_conflict_by_v", table: "idx_conflict_evt_a", columns: ["v"] }, AUTH());
    expect(first.status).toBe(200);
    const second = await post("/admin/create-index", { indexName: "idx_conflict_by_v", table: "idx_conflict_evt_b", columns: ["v"] }, AUTH());
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

  // Codex round-14 P2: backfill must place entries over the catalog's PERSISTED
  // ring, not a locally-recomputed active set. On a retry/resume the live active
  // set can differ from the pinned ring; placing over the active set would write
  // entries to shards /v1/index-query (which reads the pinned ring) never looks
  // at — a silently unqueryable index.
  it("backfill places entries over the index's PERSISTED ring (not the live active set), so the backfilled row is queryable", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("persistring_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // A base row (with provenance) BEFORE any index exists — only backfill will
    // write its index entry. Pick a value V whose placement over the full active
    // set differs from the pinned single-shard ring, so the two rings disagree.
    const pinnedRing = ["catalog-0-shard-0"];
    const activeSet = ["catalog-0-shard-0", "catalog-1-shard-0", "catalog-2-shard-0", "catalog-3-shard-0"];
    let V = "";
    for (let i = 0; ; i += 1) {
      const cand = `pr-${i}`;
      const overActive = indexShardIdForKey("persistring_evt", "persistring_by_v", JSON.stringify([cand]), activeSet);
      if (overActive !== "catalog-0-shard-0") { V = cand; break; }
    }
    await post("/v1/mutate", { op: "insert", table: "persistring_evt", tenantId, partitionKey: "row-1", values: { v: V } }, token);

    // Seed the index as 'building' with a PINNED ring of just catalog-0-shard-0
    // on every catalog (models an index created when the pool was smaller).
    for (let c = 0; c < 4; c += 1) {
      const catStub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${c}`));
      await catStub.fetch(new Request("https://catalog.internal/list-shards", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }));
      await runInDurableObject(catStub, async (_i: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec("DELETE FROM index_rules WHERE index_name = 'persistring_by_v'");
        state.storage.sql.exec(
          "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at, placement_ring_json) VALUES (?, ?, ?, 'building', ?, ?)",
          "persistring_by_v",
          "persistring_evt",
          JSON.stringify(["v"]),
          new Date().toISOString(),
          JSON.stringify(pinnedRing),
        );
      });
    }

    // /admin/create-index resumes: registration is idempotent (ring unchanged),
    // and backfill must place row-1's entry over the PERSISTED ring
    // (catalog-0-shard-0), not the recomputed active set.
    const res = await post("/admin/create-index", { indexName: "persistring_by_v", table: "persistring_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(200);

    // Queryable — the entry landed where the pinned ring reads it.
    const q = await post("/v1/index-query", { table: "persistring_evt", indexName: "persistring_by_v", tenantId, values: { v: V } }, token);
    expect(q.status).toBe(200);
    expect(((await q.json()) as { rows: Array<{ id: string }> }).rows.map((r) => r.id)).toEqual(["row-1"]);

    // Physically: the entry is on catalog-0-shard-0 (pinned ring), NOT on the
    // shard the active-set placement would have chosen.
    const onPinned = await runInDurableObject(env.SHARD.get(env.SHARD.idFromName("catalog-0-shard-0")), async (_i: unknown, state: DurableObjectState) =>
      (Array.from(state.storage.sql.exec("SELECT COUNT(*) AS n FROM __cf_indexes WHERE index_name = 'persistring_by_v'")) as Array<{ n: number }>)[0].n,
    );
    expect(onPinned).toBe(1);
  });
});

// Approved design: a durable cluster-wide topology lock (held on catalog-0)
// serializes all topology mutations. Stage 2 — the short admin ops acquire it.
describe("Worker topology-operation lock — short admin ops (Stage 2)", () => {
  function catalog0Lock(path: string, body: unknown) {
    return env.CATALOG.get(env.CATALOG.idFromName("catalog-0")).fetch(
      new Request(`https://catalog.internal${path}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }),
    );
  }

  it("/admin/create-index, /admin/drop-index, /admin/split-vbucket, /admin/migrate-vbucket all 409 TOPOLOGY_OPERATION_IN_PROGRESS while the lock is held, and create-index succeeds after release", async () => {
    await initCluster(1, 64);
    await createIndexTestTable("locktest_evt");

    // Hold the lock on catalog-0 directly (as a concurrent operation would).
    const acq = await catalog0Lock("/acquire-topology-lock", { operationType: "drain-shard" });
    expect(acq.status).toBe(200);
    const opId = ((await acq.json()) as { operationId: string }).operationId;

    const create = await post("/admin/create-index", { indexName: "locktest_by_v", table: "locktest_evt", columns: ["v"] }, AUTH());
    expect(create.status).toBe(409);
    expect(((await create.json()) as { error: { code: string } }).error.code).toBe("TOPOLOGY_OPERATION_IN_PROGRESS");

    const drop = await post("/admin/drop-index", { indexName: "locktest_by_v" }, AUTH());
    expect(drop.status).toBe(409);
    expect(((await drop.json()) as { error: { code: string } }).error.code).toBe("TOPOLOGY_OPERATION_IN_PROGRESS");

    const split = await post("/admin/split-vbucket", { catalogShardId: "catalog-0", vbucket: 0 }, AUTH());
    expect(split.status).toBe(409);
    expect(((await split.json()) as { error: { code: string } }).error.code).toBe("TOPOLOGY_OPERATION_IN_PROGRESS");

    const migrate = await post("/admin/migrate-vbucket", { catalogShardId: "catalog-0", vbucket: 0, targetShardId: "catalog-0-shard-x" }, AUTH());
    expect(migrate.status).toBe(409);
    expect(((await migrate.json()) as { error: { code: string } }).error.code).toBe("TOPOLOGY_OPERATION_IN_PROGRESS");

    // Release → a topology op is accepted again.
    const rel = await catalog0Lock("/release-topology-lock", { operationId: opId });
    expect(rel.status).toBe(200);
    const ok = await post("/admin/create-index", { indexName: "locktest_by_v", table: "locktest_evt", columns: ["v"] }, AUTH());
    expect(ok.status).toBe(200);
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
      AUTH(),
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
      AUTH(),
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
      AUTH(),
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
      AUTH(),
    );
    expect(res.status).toBe(200);
  });
});

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

  it("eng-review fix: a delete whose extra where clause doesn't match affects 0 rows and must not delete the row's live index entry", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res0 = await post(
      "/admin/create-table",
      { table: "idx_c2_zerorow_evt", schema: "CREATE TABLE IF NOT EXISTS idx_c2_zerorow_evt (id TEXT PRIMARY KEY, v TEXT, status TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res0.status).toBe(200);
    await post("/admin/create-index", { indexName: "idx_c2_zerorow_by_v", table: "idx_c2_zerorow_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post(
      "/v1/mutate",
      { op: "insert", table: "idx_c2_zerorow_evt", tenantId, partitionKey: "row-1", values: { v: "alpha", status: "open" } },
      token,
    );
    await pollIndexRows("idx_c2_zerorow_by_v", (r) => r.length === 1);

    // where.status doesn't match the row's actual status ("open") — the
    // shard-level DELETE affects 0 rows. Before the fix, this still deleted
    // the "alpha" index entry (computed from a beforeRow pre-read that
    // ignored `where`), silently hiding the still-live row from index-query.
    const res = await post(
      "/v1/mutate",
      { op: "delete", table: "idx_c2_zerorow_evt", tenantId, partitionKey: "row-1", where: { status: "closed" } },
      token,
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rowsAffected: number };
    expect(body.rowsAffected).toBe(0);

    await new Promise((resolve) => setTimeout(resolve, 50));
    const rows = await pollIndexRows("idx_c2_zerorow_by_v", (r) => r.length === 1);
    expect(rows[0].partition_key).toBe("row-1");
    expect(JSON.parse(rows[0].index_key_json)).toEqual(["alpha"]);
  });

  it("eng-review fix (Codex-found): rejects an insert on an indexed table that omits an indexed column's value, 400 INDEXED_COLUMN_REQUIRES_VALUE", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res0 = await post(
      "/admin/create-table",
      { table: "idx_c2_defaulted_evt", schema: "CREATE TABLE IF NOT EXISTS idx_c2_defaulted_evt (id TEXT PRIMARY KEY, v TEXT DEFAULT 'fallback', other TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res0.status).toBe(200);
    await post("/admin/create-index", { indexName: "idx_c2_defaulted_by_v", table: "idx_c2_defaulted_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // Supplies "other" but omits "v" (the indexed column, which has a SQL
    // DEFAULT) — before the fix, this would have silently indexed the row
    // as v=null instead of the actual DEFAULT SQLite assigns, making the
    // row unfindable for its real stored value.
    const res = await post("/v1/mutate", { op: "insert", table: "idx_c2_defaulted_evt", tenantId, partitionKey: "row-1", values: { other: "x" } }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("INDEXED_COLUMN_REQUIRES_VALUE");
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

  it("a job whose target shard returns non-2xx stays queued with attempt_count incremented and next_attempt_at following exponential backoff", async () => {
    const baseShardId = `idx-retry-fail-base-${crypto.randomUUID()}`;
    const baseStub = env.SHARD.get(env.SHARD.idFromName(baseShardId));

    // "nonexistent_table_xyz" was never created on the target shard, so
    // /execute fails with a non-2xx response — exercising processIndexPendingJobs'
    // `throw new Error(...)` / catch branch, not the happy-path retry the
    // sibling test above covers.
    const enqueueRes = await baseStub.fetch(
      new Request("https://shard.internal/enqueue-index-job", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          targetShardId: `idx-retry-fail-target-${crypto.randomUUID()}`,
          sql: "INSERT INTO nonexistent_table_xyz (a) VALUES (?)",
          params: ["x"],
          requestId: "retry-fail-req-1",
        }),
      }),
    );
    expect(enqueueRes.status).toBe(200);

    const alarmStart = Date.now();
    await runInDurableObject(baseStub, async (instance: ShardDO) => {
      await instance.alarm();
    });

    // First failure: attempt_count 0 -> 1, backoff = base delay (5000ms,
    // shard.ts's INDEX_JOB_BASE_DELAY_MS) from the time of this attempt.
    await runInDurableObject(baseStub, async (_instance: unknown, state: DurableObjectState) => {
      const jobs = Array.from(
        state.storage.sql.exec("SELECT attempt_count, next_attempt_at FROM index_pending_jobs"),
      ) as Array<{ attempt_count: number; next_attempt_at: string }>;
      expect(jobs).toHaveLength(1);
      expect(jobs[0].attempt_count).toBe(1);
      const nextAttemptMs = new Date(jobs[0].next_attempt_at).getTime();
      expect(nextAttemptMs).toBeGreaterThanOrEqual(alarmStart + 4000);
      expect(nextAttemptMs).toBeLessThanOrEqual(alarmStart + 7000);

      // Force it due now, and simulate a job that's already failed many
      // times, to assert the delay caps at INDEX_JOB_MAX_DELAY_MS (60000ms)
      // rather than growing unbounded.
      state.storage.sql.exec(
        "UPDATE index_pending_jobs SET next_attempt_at = ?, attempt_count = 10 WHERE request_id = ?",
        new Date().toISOString(),
        "retry-fail-req-1",
      );
    });

    const secondAlarmStart = Date.now();
    await runInDurableObject(baseStub, async (instance: ShardDO) => {
      await instance.alarm();
    });

    await runInDurableObject(baseStub, async (_instance: unknown, state: DurableObjectState) => {
      const jobs = Array.from(
        state.storage.sql.exec("SELECT attempt_count, next_attempt_at FROM index_pending_jobs"),
      ) as Array<{ attempt_count: number; next_attempt_at: string }>;
      expect(jobs).toHaveLength(1);
      expect(jobs[0].attempt_count).toBe(11);
      const nextAttemptMs = new Date(jobs[0].next_attempt_at).getTime();
      // 5000 * 2**10 would be ~5.1M ms uncapped; must be clamped to 60000ms.
      expect(nextAttemptMs).toBeGreaterThanOrEqual(secondAlarmStart + 59000);
      expect(nextAttemptMs).toBeLessThanOrEqual(secondAlarmStart + 61000);
    });
  });
});
