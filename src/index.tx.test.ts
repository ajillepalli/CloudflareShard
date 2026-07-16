import { SELF, env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { hashKey, indexShardIdForKey } from "./hash";
import { sha256Hex } from "./auth";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";
import { ALL_TEST_SHARD_IDS, AUTH, createIndexTestTable, driveIndexBackfillToCompletion, findPartitionKeyPairOnDifferentShards, initCluster, pollIndexRows, post, registerTenant, tenantForCatalogShard } from "./index.test-helpers";

// This file is one of several index.*.test.ts files split out of a single
// index.test.ts (see index.test-helpers.ts's header comment for why). DO
// storage persists across `it` blocks within a file, so afterEach(reset())
// gives every test clean storage — the same isolation the pre-split file used.
afterEach(async () => {
  await reset();
});

describe("Worker /v1/tx (cross-shard atomic transactions)", () => {
  it("requires a tenant token", async () => {
    await initCluster();
    const res = await post("/v1/tx", { mutations: [{ op: "insert", table: "events", tenantId: "t1", partitionKey: "p1", values: { v: "a" } }], requestId: "req-1" });
    expect(res.status).toBe(401);
  });

  it("rejects an empty mutations array", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    const res = await post("/v1/tx", { mutations: [], requestId: "req-1" }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("MISSING_MUTATIONS");
  });

  it("rejects a missing requestId", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    const res = await post("/v1/tx", { mutations: [{ op: "insert", table: "events", tenantId: "t1", partitionKey: "p1", values: { v: "a" } }] }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("MISSING_REQUEST_ID");
  });

  it("rejects a transaction touching more than 8 distinct rows, before any DO call", async () => {
    await initCluster();
    const token = await registerTenant("t1");
    const mutations = Array.from({ length: 9 }, (_, i) => ({
      op: "insert" as const,
      table: "events",
      tenantId: "t1",
      partitionKey: `too-many-${i}`,
      values: { v: "x" },
    }));
    const res = await post("/v1/tx", { mutations, requestId: "req-many" }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TOO_MANY_PARTICIPANTS");
  });

  it("rejects a cross-tenant mutation in the same batch with 401 (via its own /route call, not a separate check)", async () => {
    await initCluster();
    const tokenA = await registerTenant("tx-cross-a");
    await registerTenant("tx-cross-b");
    const res = await post(
      "/v1/tx",
      {
        mutations: [
          { op: "insert", table: "events", tenantId: "tx-cross-a", partitionKey: "p1", values: { v: "a" } },
          { op: "insert", table: "events", tenantId: "tx-cross-b", partitionKey: "p2", values: { v: "b" } },
        ],
        requestId: "req-cross",
      },
      tokenA,
    );
    expect(res.status).toBe(401);
  });

  it("commits atomically across two shards and both rows are visible afterward", async () => {
    await initCluster(2, 16);
    const token = await registerTenant("tx-happy");
    const [pkA, pkB] = await findPartitionKeyPairOnDifferentShards(token, "tx-happy", "events");

    const res = await post(
      "/v1/tx",
      {
        mutations: [
          { op: "insert", table: "events", tenantId: "tx-happy", partitionKey: pkA, values: { v: "a" } },
          { op: "insert", table: "events", tenantId: "tx-happy", partitionKey: pkB, values: { v: "b" } },
        ],
        requestId: "req-happy",
      },
      token,
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; status: string };
    expect(body.ok).toBe(true);
    expect(body.status).toBe("committed");

    const checkA = await post("/v1/sql", { sql: "SELECT id FROM events WHERE id = ?", params: [pkA], table: "events", tenantId: "tx-happy", partitionKey: pkA }, AUTH());
    expect(((await checkA.json()) as { result: { rows: unknown[] } }).result.rows).toHaveLength(1);
    const checkB = await post("/v1/sql", { sql: "SELECT id FROM events WHERE id = ?", params: [pkB], table: "events", tenantId: "tx-happy", partitionKey: pkB }, AUTH());
    expect(((await checkB.json()) as { result: { rows: unknown[] } }).result.rows).toHaveLength(1);
  });

  it("prepare failure on one shard rolls back all participants, leaving no trace", async () => {
    await initCluster(2, 16);
    const token = await registerTenant("tx-fail");
    const [pkA, pkB] = await findPartitionKeyPairOnDifferentShards(token, "tx-fail", "events");

    const res = await post(
      "/v1/tx",
      {
        mutations: [
          { op: "insert", table: "events", tenantId: "tx-fail", partitionKey: pkA, values: { v: "a" } },
          // "id" is the partitionKeyColumn, always force-set — but referencing
          // a column that doesn't exist on "events" makes this shard's
          // prepare fail with a genuine SQL error.
          { op: "insert", table: "events", tenantId: "tx-fail", partitionKey: pkB, values: { nonexistent_col: "boom" } },
        ],
        requestId: "req-fail",
      },
      token,
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_ABORTED");

    const checkA = await post("/v1/sql", { sql: "SELECT id FROM events WHERE id = ?", params: [pkA], table: "events", tenantId: "tx-fail", partitionKey: pkA }, AUTH());
    expect(((await checkA.json()) as { result: { rows: unknown[] } }).result.rows).toHaveLength(0);
  });

  it("idempotent retry: re-POSTing the same requestId after commit returns the same committed result without double-applying", async () => {
    await initCluster();
    const token = await registerTenant("tx-idem");
    const payload = {
      mutations: [{ op: "insert", table: "events", tenantId: "tx-idem", partitionKey: "p-idem", values: { v: "a" } }],
      requestId: "req-idem",
    };
    const first = await post("/v1/tx", payload, token);
    expect(first.status).toBe(200);
    const second = await post("/v1/tx", payload, token);
    expect(second.status).toBe(200);
    const secondBody = (await second.json()) as { status: string };
    expect(secondBody.status).toBe("committed");

    const countRes = await post("/v1/sql", { sql: "SELECT COUNT(*) as n FROM events WHERE id = ?", params: ["p-idem"], table: "events", tenantId: "tx-idem", partitionKey: "p-idem" }, AUTH());
    const countBody = (await countRes.json()) as { result: { rows: Array<{ n: number }> } };
    expect(countBody.result.rows[0].n).toBe(1);
  });
});

describe("Worker /v1/tx index-participant piggyback (Milestone 2 Chunk 3)", () => {
  it("insert via /v1/tx creates an index entry atomically with the base row", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c3_insert_evt");
    await post("/admin/create-index", { indexName: "idx_c3_insert_by_v", table: "idx_c3_insert_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post(
      "/v1/tx",
      { mutations: [{ op: "insert", table: "idx_c3_insert_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }], requestId: "req-c3-insert" },
      token,
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; status: string };
    expect(body.ok).toBe(true);
    expect(body.status).toBe("committed");

    const rows = await pollIndexRows("idx_c3_insert_by_v", (r) => r.length === 1);
    expect(rows[0].partition_key).toBe("row-1");
    expect(JSON.parse(rows[0].index_key_json)).toEqual(["alpha"]);
  });

  it("update via /v1/tx removes the old index entry and creates the new one atomically", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c3_update_evt");
    await post("/admin/create-index", { indexName: "idx_c3_update_by_v", table: "idx_c3_update_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post(
      "/v1/tx",
      { mutations: [{ op: "insert", table: "idx_c3_update_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }], requestId: "req-c3-update-1" },
      token,
    );
    await pollIndexRows("idx_c3_update_by_v", (r) => r.length === 1);

    const res = await post(
      "/v1/tx",
      { mutations: [{ op: "update", table: "idx_c3_update_evt", tenantId, partitionKey: "row-1", values: { v: "beta" } }], requestId: "req-c3-update-2" },
      token,
    );
    expect(res.status).toBe(200);

    const rows = await pollIndexRows("idx_c3_update_by_v", (r) => r.length === 1 && JSON.parse(r[0].index_key_json)[0] === "beta");
    expect(rows[0].partition_key).toBe("row-1");
  });

  it("prepare failure on one shard rolls back both the base row and the index entry (no torn state)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c3_fail_evt");
    await post("/admin/create-index", { indexName: "idx_c3_fail_by_v", table: "idx_c3_fail_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // A second mutation in the same batch, against a nonexistent column,
    // fails prepare on its own shard — the whole transaction (including the
    // first mutation's base row AND its index-participant intent) must
    // roll back, per Milestone 1's existing 2PC guarantee.
    const res = await post(
      "/v1/tx",
      {
        mutations: [
          { op: "insert", table: "idx_c3_fail_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } },
          { op: "insert", table: "idx_c3_fail_evt", tenantId, partitionKey: "row-2", values: { v: "boom", nonexistent_col: "boom" } },
        ],
        requestId: "req-c3-fail",
      },
      token,
    );
    expect(res.status).toBe(409);

    const checkRes = await post("/v1/sql", { sql: "SELECT * FROM idx_c3_fail_evt WHERE id = ?", params: ["row-1"], table: "idx_c3_fail_evt", tenantId, partitionKey: "row-1" }, AUTH());
    const checkBody = (await checkRes.json()) as { result: { rows: unknown[] } };
    expect(checkBody.result.rows).toHaveLength(0);

    for (const candidateShardId of ALL_TEST_SHARD_IDS) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(candidateShardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        const rows = Array.from(state.storage.sql.exec("SELECT * FROM __cf_indexes WHERE index_name = ?", "idx_c3_fail_by_v"));
        expect(rows).toHaveLength(0);
      });
    }
  });

  it("CRITICAL regression: a /v1/tx transaction that worked before an index existed still works once one does, not TOO_MANY_PARTICIPANTS", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 16, force: true }, AUTH());
    const res0 = await post(
      "/admin/create-table",
      { table: "idx_c3_regress_evt", schema: "CREATE TABLE idx_c3_regress_evt (id TEXT PRIMARY KEY, v TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res0.status).toBe(200);
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // 8 distinct base-row mutations — exactly at MAX_TX_PARTICIPANT_KEYS.
    // This already worked pre-index; registering an index must not push it
    // over the cap just because index-participant intents ride along.
    const mutations = Array.from({ length: 8 }, (_, i) => ({
      op: "insert" as const,
      table: "idx_c3_regress_evt",
      tenantId,
      partitionKey: `row-${i}`,
      values: { v: `val-${i}` },
    }));

    const preIndexRes = await post("/v1/tx", { mutations, requestId: "req-c3-regress-pre" }, token);
    expect(preIndexRes.status).toBe(200);

    await post("/admin/create-index", { indexName: "idx_c3_regress_by_v", table: "idx_c3_regress_evt", columns: ["v"] }, AUTH());

    const postIndexMutations = Array.from({ length: 8 }, (_, i) => ({
      op: "insert" as const,
      table: "idx_c3_regress_evt",
      tenantId,
      partitionKey: `row2-${i}`,
      values: { v: `val2-${i}` },
    }));
    const postIndexRes = await post("/v1/tx", { mutations: postIndexMutations, requestId: "req-c3-regress-post" }, token);
    expect(postIndexRes.status).toBe(200);
    const postIndexBody = (await postIndexRes.json()) as { ok: boolean; status: string };
    expect(postIndexBody.ok).toBe(true);
    expect(postIndexBody.status).toBe("committed");
  });

  it("eng-review fix: an update via /v1/tx whose where clause doesn't match leaves the index entry unchanged", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res0 = await post(
      "/admin/create-table",
      { table: "idx_c3_zerorow_evt", schema: "CREATE TABLE idx_c3_zerorow_evt (id TEXT PRIMARY KEY, v TEXT, status TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res0.status).toBe(200);
    await post("/admin/create-index", { indexName: "idx_c3_zerorow_by_v", table: "idx_c3_zerorow_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post(
      "/v1/tx",
      { mutations: [{ op: "insert", table: "idx_c3_zerorow_evt", tenantId, partitionKey: "row-1", values: { v: "alpha", status: "open" } }], requestId: "req-c3-zerorow-1" },
      token,
    );
    await pollIndexRows("idx_c3_zerorow_by_v", (r) => r.length === 1);

    // where.status doesn't match ("open" !== "closed") — the compiled UPDATE
    // affects 0 rows on the base shard. Before the fix, the tx-piggyback
    // pre-read ignored `where` entirely, so it would still see beforeRow
    // (v: "alpha") and piggyback a delta that rewrote the index entry to
    // "beta" even though the base row was never actually touched by this tx.
    const res = await post(
      "/v1/tx",
      {
        mutations: [{ op: "update", table: "idx_c3_zerorow_evt", tenantId, partitionKey: "row-1", where: { status: "closed" }, values: { v: "beta" } }],
        requestId: "req-c3-zerorow-2",
      },
      token,
    );
    expect(res.status).toBe(200);

    const checkRes = await post(
      "/v1/sql",
      { sql: "SELECT v FROM idx_c3_zerorow_evt WHERE id = ?", params: ["row-1"], table: "idx_c3_zerorow_evt", tenantId, partitionKey: "row-1" },
      AUTH(),
    );
    const checkBody = (await checkRes.json()) as { result: { rows: Array<{ v: string }> } };
    expect(checkBody.result.rows[0].v).toBe("alpha");

    const rows = await pollIndexRows("idx_c3_zerorow_by_v", (r) => r.length === 1);
    expect(rows[0].partition_key).toBe("row-1");
    expect(JSON.parse(rows[0].index_key_json)).toEqual(["alpha"]);
  });

  it("eng-review fix (Codex-found): two mutations on the same row in one /v1/tx batch (insert then update) index the row's final value, not the first mutation's stale one", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c3_samerow_evt");
    await post("/admin/create-index", { indexName: "idx_c3_samerow_by_v", table: "idx_c3_samerow_evt", columns: ["v"] }, AUTH());
    await driveIndexBackfillToCompletion("idx_c3_samerow_by_v");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // Insert v='a' then update v='b' on the SAME row, in one /v1/tx batch.
    // Before the fix, the update's beforeRow pre-read hit the real DB
    // (which doesn't have row-1 yet — /begin hasn't run), so its delta was
    // computed against a null/stale prior state instead of the insert's
    // pending "a" — the committed row's real final value ("b") would never
    // get an index entry at all, only the transient "a" from mutation 1.
    const res = await post(
      "/v1/tx",
      {
        mutations: [
          { op: "insert", table: "idx_c3_samerow_evt", tenantId, partitionKey: "row-1", values: { v: "a" } },
          { op: "update", table: "idx_c3_samerow_evt", tenantId, partitionKey: "row-1", values: { v: "b" } },
        ],
        requestId: "req-c3-samerow-1",
      },
      token,
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; status: string };
    expect(body.ok).toBe(true);
    expect(body.status).toBe("committed");

    const bRows = await pollIndexRows("idx_c3_samerow_by_v", (r) => r.some((row) => JSON.parse(row.index_key_json)[0] === "b"));
    const bEntry = bRows.find((row) => JSON.parse(row.index_key_json)[0] === "b");
    expect(bEntry?.partition_key).toBe("row-1");

    // No leftover entry for the transient "a" value — mutation 2's delta
    // correctly issued a DELETE for it alongside the INSERT for "b".
    const aQuery = await post("/v1/index-query", { table: "idx_c3_samerow_evt", indexName: "idx_c3_samerow_by_v", tenantId, values: { v: "a" } }, token);
    const aBody = (await aQuery.json()) as { rows: unknown[] };
    expect(aBody.rows).toHaveLength(0);

    const bQuery = await post("/v1/index-query", { table: "idx_c3_samerow_evt", indexName: "idx_c3_samerow_by_v", tenantId, values: { v: "b" } }, token);
    const queryBody = (await bQuery.json()) as { rows: Array<{ id: string; v: string }> };
    expect(queryBody.rows).toHaveLength(1);
    expect(queryBody.rows[0].id).toBe("row-1");
  });

  it("eng-review fix (Codex-found): a where clause on a later mutation in the same batch matches against the earlier mutation's pending state", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res0 = await post(
      "/admin/create-table",
      { table: "idx_c3_simwhere_evt", schema: "CREATE TABLE idx_c3_simwhere_evt (id TEXT PRIMARY KEY, v TEXT, status TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res0.status).toBe(200);
    await post("/admin/create-index", { indexName: "idx_c3_simwhere_by_v", table: "idx_c3_simwhere_evt", columns: ["v"] }, AUTH());
    await driveIndexBackfillToCompletion("idx_c3_simwhere_by_v");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // Insert status='open', then update where status='open' (matches the
    // FIRST mutation's pending value, never committed to the DB yet) to
    // v='b'. This only succeeds if the where-check is evaluated against the
    // simulated state, not a real (nonexistent-until-commit) DB row.
    const res = await post(
      "/v1/tx",
      {
        mutations: [
          { op: "insert", table: "idx_c3_simwhere_evt", tenantId, partitionKey: "row-1", values: { v: "a", status: "open" } },
          { op: "update", table: "idx_c3_simwhere_evt", tenantId, partitionKey: "row-1", where: { status: "open" }, values: { v: "b" } },
        ],
        requestId: "req-c3-simwhere-1",
      },
      token,
    );
    expect(res.status).toBe(200);

    const checkRes = await post(
      "/v1/sql",
      { sql: "SELECT v FROM idx_c3_simwhere_evt WHERE id = ?", params: ["row-1"], table: "idx_c3_simwhere_evt", tenantId, partitionKey: "row-1" },
      AUTH(),
    );
    const checkBody = (await checkRes.json()) as { result: { rows: Array<{ v: string }> } };
    expect(checkBody.result.rows[0].v).toBe("b");

    const bQuery = await post("/v1/index-query", { table: "idx_c3_simwhere_evt", indexName: "idx_c3_simwhere_by_v", tenantId, values: { v: "b" } }, token);
    const bBody = (await bQuery.json()) as { rows: Array<{ id: string }> };
    expect(bBody.rows).toHaveLength(1);
    expect(bBody.rows[0].id).toBe("row-1");
  });
});

// Codex round-14 P1: a /v1/tx index write that resolved the OLD ring before a
// drain repoint is applied via /commit (2PC piggyback), NOT /execute — so it
// bypassed the /execute index-ring fence and could strand its entry on the
// drained shard. ShardDO /prepare now votes ABORT when a synthetic __cf_indexes
// intent's ring is fenced; the client's retry recomputes the (repointed) ring
// and targets the substitute.
describe("Worker /v1/tx index-ring write fence (Codex round-14 P1)", () => {
  it("a /v1/tx whose index delta targets a fenced ring position aborts (retryable, INDEX_RING_FENCED); a retry after repoint lands the entry on the substitute and it is queryable, never on the drained shard", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 8, force: true }, AUTH());
    await createIndexTestTable("txfence_evt");
    await post("/admin/create-index", { indexName: "txfence_by_v", table: "txfence_evt", columns: ["v"] }, AUTH());
    await driveIndexBackfillToCompletion("txfence_by_v");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // The index's pinned ring (all active shards at create-index).
    const ring = ((await (await post("/admin/list-indexes", {}, AUTH())).json()) as { indexes: Array<{ indexName: string; placementRing: string[] }> })
      .indexes.find((i) => i.indexName === "txfence_by_v")!.placementRing;

    // Pick a value V whose index entry is placed on P = catalog-0-shard-0 (the
    // shard we fence). numShards:1 → the tenant's base row is also on P, but the
    // index-ring fence only rejects the SYNTHETIC index intent, not the base row.
    const P = "catalog-0-shard-0";
    let V = "";
    for (let i = 0; ; i += 1) {
      const cand = `alpha-${i}`;
      if (indexShardIdForKey("txfence_evt", "txfence_by_v", JSON.stringify([cand]), ring) === P) {
        V = cand;
        break;
      }
    }

    // Fence the index ring on P (as a drain evacuation's step-b would).
    const pStub = env.SHARD.get(env.SHARD.idFromName(P));
    await pStub.fetch(new Request("https://shard.internal/fence-index-ring", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ indexName: "txfence_by_v" }) }));

    // The tx's synthetic index intent targets P → /prepare votes abort → the
    // WHOLE tx aborts (retryable), and nothing is written.
    const abortRes = await post("/v1/tx", { mutations: [{ op: "insert", table: "txfence_evt", tenantId, partitionKey: "row-1", values: { v: V } }], requestId: "txfence-1" }, token);
    expect(abortRes.status).toBe(409);
    const abortBody = (await abortRes.json()) as { error: { code: string; details?: { error?: { code?: string } } } };
    expect(abortBody.error.code).toBe("TX_ABORTED");
    expect(abortBody.error.details?.error?.code).toBe("INDEX_RING_FENCED"); // distinguishable cause

    // Repoint the ring P→substitute on every catalog shard (what drain
    // evacuation does), keeping P fenced. Then a retry resolves to the substitute.
    const substitute = "catalog-0-shard-txsub";
    const newRing = ring.map((s) => (s === P ? substitute : s));
    for (let c = 0; c < 4; c += 1) {
      const catStub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${c}`));
      await catStub.fetch(new Request("https://catalog.internal/update-index-ring", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ indexName: "txfence_by_v", ring: newRing }) }));
    }

    // Retry with a fresh requestId (the aborted tx id is terminal). The index
    // delta now resolves to the substitute (not fenced) and commits.
    const okRes = await post("/v1/tx", { mutations: [{ op: "insert", table: "txfence_evt", tenantId, partitionKey: "row-1", values: { v: V } }], requestId: "txfence-2" }, token);
    expect(okRes.status).toBe(200);

    // Found by /v1/index-query (hydrated from its base shard).
    const q = await post("/v1/index-query", { table: "txfence_evt", indexName: "txfence_by_v", tenantId, values: { v: V } }, token);
    expect(q.status).toBe(200);
    expect(((await q.json()) as { rows: Array<{ id: string }> }).rows.map((r) => r.id)).toEqual(["row-1"]);

    // The index entry is on the SUBSTITUTE, never on the fenced shard P.
    const subN = await runInDurableObject(env.SHARD.get(env.SHARD.idFromName(substitute)), async (_i: unknown, state: DurableObjectState) =>
      (Array.from(state.storage.sql.exec("SELECT COUNT(*) AS n FROM __cf_indexes WHERE index_name = 'txfence_by_v'")) as Array<{ n: number }>)[0].n,
    );
    expect(subN).toBe(1);
    const pN = await runInDurableObject(pStub, async (_i: unknown, state: DurableObjectState) =>
      (Array.from(state.storage.sql.exec("SELECT COUNT(*) AS n FROM __cf_indexes WHERE index_name = 'txfence_by_v'")) as Array<{ n: number }>)[0].n,
    );
    expect(pN).toBe(0);
  });
});
