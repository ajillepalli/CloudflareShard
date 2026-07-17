import { SELF, env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { hashKey, indexShardIdForKey } from "./hash";
import { sha256Hex } from "./auth";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";
import { ALL_TEST_SHARD_IDS, AUTH, createIndexTestTable, driveIndexBackfillToCompletion, initCluster, pollIndexRows, post, registerTenant, tenantForCatalogShard } from "./index.test-helpers";

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
    await driveIndexBackfillToCompletion("idx_backfill_by_v");

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

  // Codex review P1 fix: a fresh shard's FIRST backfill page used to run
  // `WHERE pk > ''`. For a TEXT-affinity partition key that's a correct
  // "everything" sentinel (any non-empty TEXT sorts after ''), but for an
  // INTEGER-affinity partition key column, SQLite's type-ordering ranks
  // every INTEGER below every TEXT value, so `id > ''` is FALSE for every
  // existing integer -- the first page would silently scan zero rows, the
  // shard-exhausted branch would advance past it as if it had no data, and
  // the index would reach 'ready' having silently skipped every pre-existing
  // row on a table using a non-TEXT-affinity partition key.
  it("backfills pre-existing rows on a table whose partition key column is INTEGER-affinity, not just TEXT (Codex review P1 fix)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const createRes = await post(
      "/admin/create-table",
      { table: "idx_intpk_evt", schema: "CREATE TABLE idx_intpk_evt (id INTEGER PRIMARY KEY, v TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(createRes.status).toBe(200);
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // Insert rows BEFORE the index exists, same as the TEXT-partition-key
    // test above -- proves backfill actually indexes pre-existing data on
    // an INTEGER-affinity column, not just a TEXT one.
    await post("/v1/mutate", { op: "insert", table: "idx_intpk_evt", tenantId, partitionKey: "1", values: { v: "alpha" } }, token);
    await post("/v1/mutate", { op: "insert", table: "idx_intpk_evt", tenantId, partitionKey: "2", values: { v: "beta" } }, token);

    const res = await post("/admin/create-index", { indexName: "idx_intpk_by_v", table: "idx_intpk_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(200);
    await driveIndexBackfillToCompletion("idx_intpk_by_v");

    const foundRows: Array<{ partition_key: string; index_key_json: string }> = [];
    for (const candidateShardId of ["catalog-0-shard-0", "catalog-1-shard-0", "catalog-2-shard-0", "catalog-3-shard-0"]) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(candidateShardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        foundRows.push(
          ...(Array.from(
            state.storage.sql.exec("SELECT partition_key, index_key_json FROM __cf_indexes WHERE index_name = ?", "idx_intpk_by_v"),
          ) as Array<{ partition_key: string; index_key_json: string }>),
        );
      });
    }
    // Both pre-existing rows must be indexed -- neither silently skipped.
    foundRows.sort((a, b) => (a.partition_key < b.partition_key ? -1 : 1));
    expect(foundRows).toHaveLength(2);
    expect(JSON.parse(foundRows[0].index_key_json)).toEqual(["alpha"]);
    expect(JSON.parse(foundRows[1].index_key_json)).toEqual(["beta"]);

    // Also confirmed via the actual query path, not just physical inspection.
    const q = await post("/v1/index-query", { table: "idx_intpk_evt", indexName: "idx_intpk_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(q.status).toBe(200);
    expect(((await q.json()) as { rows: unknown[] }).rows).toHaveLength(1);
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
    await driveIndexBackfillToCompletion("idx_draining_by_v");

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
    // Codex live-deployment finding: backfill now runs on catalog-0's alarm,
    // which holds the topology lock this whole call acquired for "create-
    // index" until backfill genuinely finishes (or fails) — a retry
    // attempted WHILE it's still in flight would 409
    // TOPOLOGY_OPERATION_IN_PROGRESS, not the registration-idempotency 200
    // this test is actually about. Drive the first attempt's backfill to
    // completion (releasing the lock) before retrying, matching how a real
    // caller would behave: retry once the first attempt is done, not while
    // it's still running.
    await driveIndexBackfillToCompletion("idx_dup_by_v");
    const second = await post("/admin/create-index", { indexName: "idx_dup_by_v", table: "idx_dup_evt", columns: ["v"] }, AUTH());
    expect(second.status).toBe(200);

    // Codex live-deployment finding (round 2, caught only in production, NOT
    // by this test suite -- closing that gap here): this SECOND, idempotent
    // call against an ALREADY-'ready' index acquires its OWN fresh topology
    // lock, but /start-index-backfill correctly reports nothing to hand off
    // to (backfill already finished) -- the Worker must release that lock
    // itself rather than treating a bare 200 as "handed off" and leaking it
    // forever. Confirmed by checking the lock is actually free afterward: a
    // genuinely NEW topology operation must succeed immediately, not 409.
    const lockCheck = await post("/admin/drop-index", { indexName: "idx_dup_by_v" }, AUTH());
    expect(lockCheck.status).toBe(200); // would be 409 TOPOLOGY_OPERATION_IN_PROGRESS if leaked
  });

  it("rejects reusing an indexName with different table/columns as a genuine conflict", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_conflict_evt_a");
    await createIndexTestTable("idx_conflict_evt_b");
    const first = await post("/admin/create-index", { indexName: "idx_conflict_by_v", table: "idx_conflict_evt_a", columns: ["v"] }, AUTH());
    expect(first.status).toBe(200);
    // The first call's topology lock is held by catalog-0 until ITS backfill
    // finishes (see the idempotent-retry test's comment above) — drain it so
    // the conflicting second call reaches the actual INDEX_ALREADY_REGISTERED
    // check instead of 409 TOPOLOGY_OPERATION_IN_PROGRESS.
    await driveIndexBackfillToCompletion("idx_conflict_by_v");
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
      { table: "legacy_idx_evt", schema: "CREATE TABLE legacy_idx_evt (id TEXT PRIMARY KEY, v TEXT)", partitionKeyColumn: "id" },
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
    await driveIndexBackfillToCompletion("persistring_by_v");

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

  // Codex round-3 review (PR #20): retrying a 'failed' backfill preserves its
  // OLD numeric backfill_shard_idx/backfill_after_pk cursor, but the shard
  // list it indexes into (backfillShardIds) is recomputed FRESH by the
  // Worker on every /admin/create-index call. If the active+draining shard
  // set changes between the original failed attempt and the retry (a shard
  // fully drains and drops out of the list, here simulated directly), the
  // stale cursor can point at the WRONG shard in the new list, silently
  // skipping a shard that still has an unindexed row. handleStartIndexBackfill
  // must detect the shard-list change and reset the cursor rather than
  // resuming it blindly.
  it("resets the backfill cursor (not resuming a stale shard index) when the shard list changed since a failed attempt, so no shard is silently skipped", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_topo_evt");

    // Shard A (catalog-0-shard-0) gets a normal, fully-provenanced row —
    // its backfill page completes cleanly, advancing the cursor to shard
    // index 1 before the run below ever fails.
    const tenantA = tenantForCatalogShard(0, 4);
    const tokenA = await registerTenant(tenantA);
    await post("/v1/mutate", { op: "insert", table: "idx_topo_evt", tenantId: tenantA, partitionKey: "row-a", values: { v: "alpha" } }, tokenA);

    // Shard B (catalog-1-shard-0) — dataShardIds' position 1 — gets a row
    // written directly, bypassing provenance (mirrors the
    // PROVENANCE_MISSING_FOR_INDEX test): the tick that reaches shard index
    // 1 throws PermanentIndexBackfillError immediately, before advancing
    // past it, so backfill_shard_idx is left at 1 and backfill_after_pk at ''.
    const shardB = env.SHARD.get(env.SHARD.idFromName("catalog-1-shard-0"));
    await shardB.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "INSERT INTO idx_topo_evt (id, v) VALUES (?, ?)",
          params: ["row-b-no-prov", "bravo"],
          requestId: `topo-insert-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      }),
    );

    const res = await post("/admin/create-index", { indexName: "idx_topo_by_v", table: "idx_topo_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(200);
    const catalogZero = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    for (let tick = 0; tick < 10; tick += 1) {
      await runInDurableObject(catalogZero, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const statusRes = await post("/admin/create-index-status", { indexName: "idx_topo_by_v" }, AUTH());
      if (((await statusRes.json()) as { status: string }).status === "failed") break;
    }
    const failedStatusRes = await post("/admin/create-index-status", { indexName: "idx_topo_by_v" }, AUTH());
    expect(((await failedStatusRes.json()) as { status: string }).status).toBe("failed");

    // Simulate the topology changing since the failed attempt: shard A
    // fully drains and drops out of the active+draining set entirely. The
    // NEXT /admin/create-index call recomputes dataShardIds as
    // [catalog-1-shard-0, catalog-2-shard-0, catalog-3-shard-0] — shard B
    // is now at position 0, not the position-1 the failed attempt's cursor
    // still points at.
    await runInDurableObject(env.CATALOG.get(env.CATALOG.idFromName("catalog-0")), async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("DELETE FROM shards WHERE shard_id = ?", "catalog-0-shard-0");
    });

    // The operator fixes shard B's provenance gap before retrying.
    const tenantB = tenantForCatalogShard(1, 4);
    const setOwnerRes = await post(
      "/admin/set-row-owner",
      { catalogShardId: "catalog-1", shardId: "catalog-1-shard-0", table: "idx_topo_evt", partitionKey: "row-b-no-prov", tenantId: tenantB },
      AUTH(),
    );
    expect(setOwnerRes.status).toBe(200);

    const retry = await post("/admin/create-index", { indexName: "idx_topo_by_v", table: "idx_topo_evt", columns: ["v"] }, AUTH());
    expect(retry.status).toBe(200);
    await driveIndexBackfillToCompletion("idx_topo_by_v");

    // Shard B's row must be queryable — a stale cursor resuming at the old
    // position 1 (now catalog-2-shard-0, empty) would have skipped it and
    // still reached 'ready'.
    const tokenB = await registerTenant(tenantB);
    const queryRes = await post("/v1/index-query", { table: "idx_topo_evt", indexName: "idx_topo_by_v", tenantId: tenantB, values: { v: "bravo" } }, tokenB);
    expect(queryRes.status).toBe(200);
    const queryBody = (await queryRes.json()) as { rows: Array<{ id: string; v: string }> };
    expect(queryBody.rows.map((r) => r.id)).toEqual(["row-b-no-prov"]);
  });

  // Codex round-4 review (PR #20): a scan request that comes back !ok (shard.
  // ts's /execute has exactly one non-2xx path for a read-only scan -- its
  // catch-all "SQL execution failed", e.g. the table is missing on this
  // shard, which /admin/register-table can leave true since (unlike
  // /admin/create-table) it never pushes DDL to any shard) used to be thrown
  // as a plain Error, treated as transient by the alarm loop's shared catch,
  // and retried forever -- heartbeating (never releasing) this index's
  // topology lock and wedging every other topology operation, exactly the
  // failure mode PermanentIndexBackfillError exists to avoid for
  // PROVENANCE_MISSING_FOR_INDEX above.
  it("gives up (does not retry forever) when a data shard can't scan the table at all, e.g. registered via /admin/register-table but never physically created there", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());

    // /admin/register-table only writes catalog metadata (table_rules) --
    // unlike /admin/create-table, it never pushes CREATE TABLE to any shard.
    const registerRes = await post(
      "/admin/register-table",
      { table: "idx_noshard_evt", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(registerRes.status).toBe(200);

    // Physically create the table on 3 of the 4 data shards, deliberately
    // skipping catalog-1-shard-0 -- whichever tick reaches it will find no
    // such table.
    for (const shardId of ["catalog-0-shard-0", "catalog-2-shard-0", "catalog-3-shard-0"]) {
      const stub = env.SHARD.get(env.SHARD.idFromName(shardId));
      const createRes = await stub.fetch(
        new Request("https://shard.internal/execute", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            sql: "CREATE TABLE idx_noshard_evt (id TEXT PRIMARY KEY, v TEXT)",
            params: [],
            requestId: `noshard-ddl-${shardId}-${crypto.randomUUID()}`,
            isMutation: false,
          }),
        }),
      );
      expect(createRes.ok).toBe(true);
    }

    const res = await post("/admin/create-index", { indexName: "idx_noshard_by_v", table: "idx_noshard_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(200);
    const catalogZero = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    for (let tick = 0; tick < 10; tick += 1) {
      await runInDurableObject(catalogZero, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const statusRes = await post("/admin/create-index-status", { indexName: "idx_noshard_by_v" }, AUTH());
      if (((await statusRes.json()) as { status: string }).status === "failed") break;
    }
    const failedStatusRes = await post("/admin/create-index-status", { indexName: "idx_noshard_by_v" }, AUTH());
    expect(((await failedStatusRes.json()) as { status: string }).status).toBe("failed");

    // The lock was actually released -- an unrelated topology op succeeds
    // immediately instead of 409 TOPOLOGY_OPERATION_IN_PROGRESS.
    await createIndexTestTable("idx_noshard_unrelated_evt");
    const unrelated = await post(
      "/admin/create-index",
      { indexName: "idx_noshard_unrelated_by_v", table: "idx_noshard_unrelated_evt", columns: ["v"] },
      AUTH(),
    );
    expect(unrelated.status).toBe(200);
    await driveIndexBackfillToCompletion("idx_noshard_unrelated_by_v");

    // Once the operator actually creates the missing table, retrying
    // /admin/create-index resumes and completes normally.
    const fixRes = await env.SHARD.get(env.SHARD.idFromName("catalog-1-shard-0")).fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "CREATE TABLE idx_noshard_evt (id TEXT PRIMARY KEY, v TEXT)",
          params: [],
          requestId: `noshard-ddl-fix-${crypto.randomUUID()}`,
          isMutation: false,
        }),
      }),
    );
    expect(fixRes.ok).toBe(true);
    const retry = await post("/admin/create-index", { indexName: "idx_noshard_by_v", table: "idx_noshard_evt", columns: ["v"] }, AUTH());
    expect(retry.status).toBe(200);
    await driveIndexBackfillToCompletion("idx_noshard_by_v");
    const finalStatusRes = await post("/admin/create-index-status", { indexName: "idx_noshard_by_v" }, AUTH());
    expect(((await finalStatusRes.json()) as { status: string }).status).toBe("ready");
  });

  // Codex round-5 review (PR #20): round 3's fix only reset the backfill
  // cursor when the SET of shards changed between a failed attempt and its
  // retry -- but a vbucket migration doesn't change which shards exist, it
  // moves rows BETWEEN existing ones. If a row migrates from a not-yet-
  // scanned shard onto a shard the failed attempt had ALREADY finished
  // scanning, a resumed cursor (or round 3's unchanged-list check, which
  // sees no list difference here) would never revisit that already-done
  // shard and silently miss the row -- exactly the class of bug
  // PermanentIndexBackfillError's whole cursor-preservation design was
  // trying to avoid, just one level deeper. The fix: always restart a
  // retried-from-'failed' backfill from shard 0, never resume any cursor.
  it("re-discovers a row that migrated onto an already-scanned shard during the window between a failed attempt and its retry", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_vbmig_evt");

    // Shard A (catalog-0-shard-0, dataShardIds position 0) gets a normal,
    // fully-provenanced row -- its page completes cleanly, advancing the
    // backfill past position 0 before the run below ever fails.
    const tenantA = tenantForCatalogShard(0, 4);
    const tokenA = await registerTenant(tenantA);
    await post("/v1/mutate", { op: "insert", table: "idx_vbmig_evt", tenantId: tenantA, partitionKey: "row-a", values: { v: "alpha" } }, tokenA);

    // Shard B (catalog-1-shard-0, position 1) gets a row written directly,
    // bypassing provenance -- the tick that reaches position 1 throws
    // PermanentIndexBackfillError immediately, before scanning position 2.
    const shardB = env.SHARD.get(env.SHARD.idFromName("catalog-1-shard-0"));
    await shardB.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "INSERT INTO idx_vbmig_evt (id, v) VALUES (?, ?)",
          params: ["row-b-no-prov", "bravo"],
          requestId: `vbmig-insert-b-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      }),
    );

    // Shard C (catalog-2-shard-0, position 2) gets a normal, fully-
    // provenanced row too -- never reached by the run below, since it fails
    // at position 1 first.
    const shardC = env.SHARD.get(env.SHARD.idFromName("catalog-2-shard-0"));
    await shardC.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "INSERT INTO idx_vbmig_evt (id, v) VALUES (?, ?)",
          params: ["row-c", "charlie"],
          requestId: `vbmig-insert-c-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      }),
    );
    const tenantC = tenantForCatalogShard(2, 4);
    const setOwnerCRes = await post(
      "/admin/set-row-owner",
      { catalogShardId: "catalog-2", shardId: "catalog-2-shard-0", table: "idx_vbmig_evt", partitionKey: "row-c", tenantId: tenantC },
      AUTH(),
    );
    expect(setOwnerCRes.status).toBe(200);

    const res = await post("/admin/create-index", { indexName: "idx_vbmig_by_v", table: "idx_vbmig_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(200);
    const catalogZero = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    for (let tick = 0; tick < 10; tick += 1) {
      await runInDurableObject(catalogZero, async (instance: CatalogDO) => {
        await instance.alarm();
      });
      const statusRes = await post("/admin/create-index-status", { indexName: "idx_vbmig_by_v" }, AUTH());
      if (((await statusRes.json()) as { status: string }).status === "failed") break;
    }
    const failedStatusRes = await post("/admin/create-index-status", { indexName: "idx_vbmig_by_v" }, AUTH());
    expect(((await failedStatusRes.json()) as { status: string }).status).toBe("failed");

    // Simulate a vbucket migration in the window between the failure (whose
    // lock release just happened) and the retry below: row-c moves from
    // shard C (never scanned yet) onto shard A (ALREADY fully scanned by
    // the failed attempt) -- the shard SET stays exactly [A,B,C,D], only
    // where row-c physically lives changes.
    await shardC.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "DELETE FROM idx_vbmig_evt WHERE id = ?",
          params: ["row-c"],
          requestId: `vbmig-delete-c-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      }),
    );
    const shardA = env.SHARD.get(env.SHARD.idFromName("catalog-0-shard-0"));
    await shardA.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "INSERT INTO idx_vbmig_evt (id, v) VALUES (?, ?)",
          params: ["row-c", "charlie"],
          requestId: `vbmig-insert-c-on-a-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      }),
    );
    const setOwnerCOnARes = await post(
      "/admin/set-row-owner",
      { catalogShardId: "catalog-0", shardId: "catalog-0-shard-0", table: "idx_vbmig_evt", partitionKey: "row-c", tenantId: tenantA },
      AUTH(),
    );
    expect(setOwnerCOnARes.status).toBe(200);

    // The operator fixes shard B's provenance gap before retrying.
    const tenantB = tenantForCatalogShard(1, 4);
    const setOwnerBRes = await post(
      "/admin/set-row-owner",
      { catalogShardId: "catalog-1", shardId: "catalog-1-shard-0", table: "idx_vbmig_evt", partitionKey: "row-b-no-prov", tenantId: tenantB },
      AUTH(),
    );
    expect(setOwnerBRes.status).toBe(200);

    const retry = await post("/admin/create-index", { indexName: "idx_vbmig_by_v", table: "idx_vbmig_evt", columns: ["v"] }, AUTH());
    expect(retry.status).toBe(200);
    await driveIndexBackfillToCompletion("idx_vbmig_by_v");

    // row-c must be queryable under tenantA -- a resumed cursor starting
    // past position 0 (shard A) would never revisit it there and miss the
    // migrated row entirely, even though the index reaches 'ready'.
    const queryRes = await post("/v1/index-query", { table: "idx_vbmig_evt", indexName: "idx_vbmig_by_v", tenantId: tenantA, values: { v: "charlie" } }, tokenA);
    expect(queryRes.status).toBe(200);
    const queryBody = (await queryRes.json()) as { rows: Array<{ id: string; v: string }> };
    expect(queryBody.rows.map((r) => r.id)).toEqual(["row-c"]);
  });
});

// Codex final-review P1 #1 (original synchronous version) / Codex
// live-deployment finding (this alarm-driven rewrite): backfill must not
// keep writing index entries once its topology lock is lost — a concurrent
// drain (now free to acquire the lock) could already be moving rows this
// backfill hasn't scanned yet off their source shard. The synchronous
// version heartbeated once per data shard and hard-aborted the HTTP call the
// moment a heartbeat failed; now that backfill is alarm-driven (see
// advanceIndexBackfill), the SAME protection is provided by alarm()'s own
// heartbeatTopologyLockOrPark check, called once before each tick — a failed
// heartbeat simply skips that tick's mutation (no entries written, no
// progress made) and keeps re-arming so a later tick notices if the lock
// situation resolves, rather than ever returning an error to any caller
// (there IS no caller left waiting by the time a tick runs).
describe("Worker /admin/create-index backfill heartbeats its topology lock (Codex final-review P1 #1)", () => {
  it("stops writing further index entries the moment a tick's heartbeat reports the lock lost, and resumes once the lock is confirmed again", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("hblost_evt");

    // One row per catalog shard's tenant — numShards:1 means dataShardIds is
    // exactly the 4 catalog-*-shard-0 physical shards (ALL_TEST_SHARD_IDS).
    // Each shard has exactly 1 row, well under INDEX_BACKFILL_PAGE_SIZE, so
    // advanceIndexBackfill fully backfills one shard per tick — meaning one
    // heartbeat check (in alarm()'s dispatcher, before calling
    // advanceIndexBackfill) gates one shard's worth of progress.
    const catalogIndexOf: Record<string, number> = { "catalog-0-shard-0": 0, "catalog-1-shard-0": 1, "catalog-2-shard-0": 2, "catalog-3-shard-0": 3 };
    for (const shardId of Object.keys(catalogIndexOf)) {
      const tenantId = tenantForCatalogShard(catalogIndexOf[shardId], 4);
      const token = await registerTenant(tenantId);
      await post("/v1/mutate", { op: "insert", table: "hblost_evt", tenantId, partitionKey: `row-${shardId}`, values: { v: "x" } }, token);
    }

    // Monkey-patch catalog-0's handleHeartbeatTopologyLock DIRECTLY (not
    // routes["/heartbeat-topology-lock"] — heartbeatTopologyLockOrPark calls
    // this method in-process when running on catalog-0 itself, bypassing the
    // routes map entirely; see catalog.ts's isCatalogZero doc comment). Let
    // the FIRST call through (so the backfill genuinely gets underway and
    // indexes one shard), then report LOCK_LOST for every call after — as a
    // real force-release/expiry would.
    const catalogZero = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    await runInDurableObject(catalogZero, async (instance: CatalogDO) => {
      const inst = instance as unknown as {
        handleHeartbeatTopologyLock: (request: Request) => Promise<Response>;
        __realHeartbeat?: (request: Request) => Promise<Response>;
      };
      inst.__realHeartbeat = inst.handleHeartbeatTopologyLock.bind(inst);
      let calls = 0;
      inst.handleHeartbeatTopologyLock = async (request: Request) => {
        calls += 1;
        if (calls === 1) return inst.__realHeartbeat!(request);
        return new Response(
          JSON.stringify({ error: { code: "LOCK_LOST", message: "simulated lock loss for the regression test" } }),
          { status: 409, headers: { "content-type": "application/json" } },
        );
      };
    });

    const res = await post("/admin/create-index", { indexName: "hblost_by_v", table: "hblost_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(200); // registration + backfill START always succeeds now

    // Drive several ticks -- backfill must NOT make progress past the first
    // (heartbeat succeeds) tick once every later heartbeat starts failing.
    for (let i = 0; i < 5; i += 1) {
      await runInDurableObject(catalogZero, async (instance: CatalogDO) => {
        await instance.alarm();
      });
    }

    // Exactly the one shard whose heartbeat was let through — never all 4
    // (that would mean the lock loss was ignored) and never 0 (that would
    // mean nothing progressed even before the simulated loss).
    let indexedShards = 0;
    for (const shardId of Object.keys(catalogIndexOf)) {
      const rows = await runInDurableObject(env.SHARD.get(env.SHARD.idFromName(shardId)), async (_i: unknown, state: DurableObjectState) =>
        Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ?", "hblost_by_v")) as Array<{ partition_key: string }>,
      );
      indexedShards += rows.length;
    }
    expect(indexedShards).toBe(1);

    // The index must never have been marked ready — backfill never finished.
    const listRes = await post("/admin/list-indexes", {}, AUTH());
    const listBody = (await listRes.json()) as { indexes: Array<{ indexName: string; status?: string }> };
    const entry = listBody.indexes.find((i) => i.indexName === "hblost_by_v");
    expect(entry).toBeDefined();
    expect(entry?.status).not.toBe("ready");

    // Restore the real heartbeat handler (the same "undo the monkey-patch
    // before continuing" convention catalog.test.ts uses for callShard) —
    // resuming ticks now must exercise the FIX, not the simulated failure
    // again. The underlying lock itself was never actually touched by the
    // monkey-patch (only its heartbeat CHECK responses were faked), so it's
    // still genuinely valid and ticking resumes the SAME backfill from its
    // persisted cursor rather than needing a fresh /admin/create-index call.
    await runInDurableObject(catalogZero, async (instance: CatalogDO) => {
      const inst = instance as unknown as { handleHeartbeatTopologyLock: (request: Request) => Promise<Response>; __realHeartbeat: (request: Request) => Promise<Response> };
      inst.handleHeartbeatTopologyLock = inst.__realHeartbeat;
    });
    await driveIndexBackfillToCompletion("hblost_by_v");

    let indexedAfterResume = 0;
    for (const shardId of Object.keys(catalogIndexOf)) {
      const rows = await runInDurableObject(env.SHARD.get(env.SHARD.idFromName(shardId)), async (_i: unknown, state: DurableObjectState) =>
        Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ?", "hblost_by_v")) as Array<{ partition_key: string }>,
      );
      indexedAfterResume += rows.length;
    }
    expect(indexedAfterResume).toBe(4);
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
    await driveIndexBackfillToCompletion("idx_c6_drop_by_v");
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
      { table: "idx_c2_untouched_evt", schema: "CREATE TABLE idx_c2_untouched_evt (id TEXT PRIMARY KEY, v TEXT, other TEXT)", partitionKeyColumn: "id" },
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
      { table: "idx_c2_zerorow_evt", schema: "CREATE TABLE idx_c2_zerorow_evt (id TEXT PRIMARY KEY, v TEXT, status TEXT)", partitionKeyColumn: "id" },
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
      { table: "idx_c2_defaulted_evt", schema: "CREATE TABLE idx_c2_defaulted_evt (id TEXT PRIMARY KEY, v TEXT DEFAULT 'fallback', other TEXT)", partitionKeyColumn: "id" },
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

// Stage 4 of the approved topology-lock design: admin recovery routes.
describe("Worker topology-operation lock — admin recovery (Stage 4)", () => {
  function catalog0Lock(path: string, body: unknown) {
    return env.CATALOG.get(env.CATALOG.idFromName("catalog-0")).fetch(
      new Request(`https://catalog.internal${path}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }),
    );
  }

  it("/admin/topology-lock-status requires an admin token and reports the current holder (or none)", async () => {
    await initCluster(1, 64);

    const noAuth = await post("/admin/topology-lock-status", {});
    expect(noAuth.status).toBe(401);

    const none = await post("/admin/topology-lock-status", {}, AUTH());
    expect(none.status).toBe(200);
    expect(((await none.json()) as { held: boolean }).held).toBe(false);

    const acq = await catalog0Lock("/acquire-topology-lock", { operationType: "drain-shard" });
    const opId = ((await acq.json()) as { operationId: string }).operationId;

    const held = await post("/admin/topology-lock-status", {}, AUTH());
    expect(held.status).toBe(200);
    const heldBody = (await held.json()) as { held: boolean; operationId: string; operationType: string };
    expect(heldBody.held).toBe(true);
    expect(heldBody.operationId).toBe(opId);
    expect(heldBody.operationType).toBe("drain-shard");
  });

  it("/admin/force-release-topology-lock requires an admin token, clears a stuck lock (freeing a blocked topology op), and is a safe idempotent no-op for a non-matching or already-cleared operationId", async () => {
    await initCluster(1, 64);
    await createIndexTestTable("forcerelease_evt");

    const acq = await catalog0Lock("/acquire-topology-lock", { operationType: "drain-shard" });
    const opId = ((await acq.json()) as { operationId: string }).operationId;

    // Blocked while the (simulated-stuck) lock is held.
    const blocked = await post("/admin/create-index", { indexName: "forcerelease_by_v", table: "forcerelease_evt", columns: ["v"] }, AUTH());
    expect(blocked.status).toBe(409);

    const noAuth = await post("/admin/force-release-topology-lock", { operationId: opId });
    expect(noAuth.status).toBe(401);

    // Wrong/non-matching operationId: a safe no-op (does NOT clear the real lock).
    const wrongId = await post("/admin/force-release-topology-lock", { operationId: "not-the-real-one" }, AUTH());
    expect(wrongId.status).toBe(200);
    expect(((await wrongId.json()) as { released: boolean }).released).toBe(false);
    const stillBlocked = await post("/admin/create-index", { indexName: "forcerelease_by_v", table: "forcerelease_evt", columns: ["v"] }, AUTH());
    expect(stillBlocked.status).toBe(409);

    // The operator's actual force-release — clears the stuck lock.
    const force = await post("/admin/force-release-topology-lock", { operationId: opId }, AUTH());
    expect(force.status).toBe(200);
    expect(((await force.json()) as { released: boolean }).released).toBe(true);

    // The previously-blocked topology op now succeeds.
    const now = await post("/admin/create-index", { indexName: "forcerelease_by_v", table: "forcerelease_evt", columns: ["v"] }, AUTH());
    expect(now.status).toBe(200);

    // Re-releasing the same (now-cleared) operationId is a safe idempotent no-op.
    const again = await post("/admin/force-release-topology-lock", { operationId: opId }, AUTH());
    expect(again.status).toBe(200);
    expect(((await again.json()) as { released: boolean }).released).toBe(false);
  });
});
