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

describe("Worker /v1/index-query (Milestone 2 Chunk 4)", () => {
  it("finds a row inserted via /v1/mutate (async index path)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_mutate_evt");
    await post("/admin/create-index", { indexName: "idx_c4_mutate_by_v", table: "idx_c4_mutate_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/mutate", { op: "insert", table: "idx_c4_mutate_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    await pollIndexRows("idx_c4_mutate_by_v", (r) => r.length === 1);

    const res = await post("/v1/index-query", { table: "idx_c4_mutate_evt", indexName: "idx_c4_mutate_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: Array<{ id: string; v: string }> };
    expect(body.rows).toHaveLength(1);
    expect(body.rows[0].id).toBe("row-1");
    expect(body.rows[0].v).toBe("alpha");
  });

  it("finds a row inserted via /v1/tx (2PC piggyback path)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_tx_evt");
    await post("/admin/create-index", { indexName: "idx_c4_tx_by_v", table: "idx_c4_tx_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/tx", { mutations: [{ op: "insert", table: "idx_c4_tx_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }], requestId: "req-c4-tx" }, token);

    const res = await post("/v1/index-query", { table: "idx_c4_tx_evt", indexName: "idx_c4_tx_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: Array<{ id: string }> };
    expect(body.rows).toHaveLength(1);
    expect(body.rows[0].id).toBe("row-1");
  });

  it("returns an empty result for no matching rows", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_empty_evt");
    await post("/admin/create-index", { indexName: "idx_c4_empty_by_v", table: "idx_c4_empty_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post("/v1/index-query", { table: "idx_c4_empty_evt", indexName: "idx_c4_empty_by_v", tenantId, values: { v: "ghost" } }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: unknown[] };
    expect(body.rows).toHaveLength(0);
  });

  it("requires a tenant token", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_auth_evt");
    await post("/admin/create-index", { indexName: "idx_c4_auth_by_v", table: "idx_c4_auth_evt", columns: ["v"] }, AUTH());
    const res = await post("/v1/index-query", { table: "idx_c4_auth_evt", indexName: "idx_c4_auth_by_v", tenantId: "t1", values: { v: "alpha" } });
    expect(res.status).toBe(401);
  });

  it("rejects a query against an unregistered index name", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_ghost_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);
    const res = await post("/v1/index-query", { table: "idx_c4_ghost_evt", indexName: "idx_c4_ghost_index", tenantId, values: { v: "alpha" } }, token);
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("INDEX_NOT_REGISTERED");
  });

  it("rejects a query missing a value for one of the index's columns", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    const res0 = await post(
      "/admin/create-table",
      { table: "idx_c4_partial_evt", schema: "CREATE TABLE idx_c4_partial_evt (id TEXT PRIMARY KEY, a TEXT, b TEXT)", partitionKeyColumn: "id" },
      AUTH(),
    );
    expect(res0.status).toBe(200);
    await post("/admin/create-index", { indexName: "idx_c4_partial_by_ab", table: "idx_c4_partial_evt", columns: ["a", "b"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post("/v1/index-query", { table: "idx_c4_partial_evt", indexName: "idx_c4_partial_by_ab", tenantId, values: { a: "x" } }, token);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("INCOMPLETE_INDEX_KEY");
  });

  it("excludes a stale index entry whose base row no longer matches (async staleness re-check)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_stale_evt");
    await post("/admin/create-index", { indexName: "idx_c4_stale_by_v", table: "idx_c4_stale_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/mutate", { op: "insert", table: "idx_c4_stale_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    await pollIndexRows("idx_c4_stale_by_v", (r) => r.length === 1);

    // Simulate a race: the base row changed, but the OLD index entry
    // (pointing at "alpha") hasn't been cleaned up yet — directly seed a
    // stale entry alongside whatever the real async write already produced,
    // by updating the base row without going through /v1/mutate's index
    // maintenance (mirrors a lagging async write mid-flight).
    for (const candidateShardId of ALL_TEST_SHARD_IDS) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(candidateShardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        const rows = Array.from(state.storage.sql.exec("SELECT * FROM idx_c4_stale_evt WHERE id = 'row-1'"));
        if (rows.length > 0) {
          state.storage.sql.exec("UPDATE idx_c4_stale_evt SET v = 'beta' WHERE id = 'row-1'");
        }
      });
    }

    // The stale __cf_indexes entry (still keyed on "alpha") must not surface
    // a row whose actual current value no longer matches.
    const res = await post("/v1/index-query", { table: "idx_c4_stale_evt", indexName: "idx_c4_stale_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: unknown[] };
    expect(body.rows).toHaveLength(0);
  });

  it("caps fan-out at the requested limit", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_limit_evt");
    await post("/admin/create-index", { indexName: "idx_c4_limit_by_v", table: "idx_c4_limit_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    for (let i = 0; i < 5; i++) {
      await post("/v1/mutate", { op: "insert", table: "idx_c4_limit_evt", tenantId, partitionKey: `row-${i}`, values: { v: "shared" } }, token);
    }
    await pollIndexRows("idx_c4_limit_by_v", (r) => r.length === 5);

    const res = await post("/v1/index-query", { table: "idx_c4_limit_evt", indexName: "idx_c4_limit_by_v", tenantId, values: { v: "shared" }, limit: 2 }, token);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: unknown[] };
    expect(body.rows).toHaveLength(2);
  });

  it("eng-review fix: does not under-fill results when stale entries sort before live matches under a limit", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_pagestale_evt");
    await post("/admin/create-index", { indexName: "idx_c4_pagestale_by_v", table: "idx_c4_pagestale_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    await post("/v1/mutate", { op: "insert", table: "idx_c4_pagestale_evt", tenantId, partitionKey: "z-live-1", values: { v: "shared" } }, token);
    await post("/v1/mutate", { op: "insert", table: "idx_c4_pagestale_evt", tenantId, partitionKey: "z-live-2", values: { v: "shared" } }, token);
    const liveRows = await pollIndexRows("idx_c4_pagestale_by_v", (r) => r.length === 2);
    const indexKeyJson = liveRows[0].index_key_json;

    // Directly seed 6 stale entries on the SAME index shard, same
    // index_key_json ("shared"), whose partition keys sort alphabetically
    // BEFORE the two live ones ("z-live-1"/"z-live-2") — they point at
    // partition keys that don't exist in the base table, so the staleness
    // re-check filters every one of them. Under the old single-LIMIT-then-
    // filter behavior, a limit of 2 would only ever see these 6 stale
    // entries and return an empty result even though 2 live matches exist.
    let seededOnShardId: string | null = null;
    for (const shardId of ALL_TEST_SHARD_IDS) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));
      const found = await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        const rows = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_indexes WHERE index_name = ?", "idx_c4_pagestale_by_v"));
        return rows.length > 0;
      });
      if (found) {
        seededOnShardId = shardId;
        break;
      }
    }
    expect(seededOnShardId).not.toBeNull();
    const targetStub = env.SHARD.get(env.SHARD.idFromName(seededOnShardId as string));
    await runInDurableObject(targetStub, async (_instance: unknown, state: DurableObjectState) => {
      for (let i = 0; i < 6; i++) {
        state.storage.sql.exec(
          "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
          "idx_c4_pagestale_evt",
          "idx_c4_pagestale_by_v",
          indexKeyJson,
          `a-stale-${i}`,
          seededOnShardId,
          new Date().toISOString(),
        );
      }
    });

    const res = await post(
      "/v1/index-query",
      { table: "idx_c4_pagestale_evt", indexName: "idx_c4_pagestale_by_v", tenantId, values: { v: "shared" }, limit: 2 },
      token,
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: Array<{ id: string }> };
    expect(body.rows).toHaveLength(2);
    expect(body.rows.map((r) => r.id).sort()).toEqual(["z-live-1", "z-live-2"]);
  });

  // Codex round-15 P1 #2: the __cf_indexes scan must filter by the caller's
  // tenant_id IN THE SQL PREDICATE (before ORDER BY/LIMIT), not only during
  // hydration. A shared indexed value can have entries for many tenants; if
  // another tenant owns the first rawScanCap (=limit*5) partition keys, a
  // post-scan filter leaves this tenant's entries past the cap, unscanned →
  // empty/under-filled despite live rows.
  it("filters by tenant in the index scan before the cap, so a tenant whose entries sort after another tenant's cap-filling entries is still returned", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_tenantcap_evt");
    await post("/admin/create-index", { indexName: "idx_tenantcap_by_v", table: "idx_tenantcap_evt", columns: ["v"] }, AUTH());
    const tenantA = tenantForCatalogShard(0, 4);
    const tokenA = await registerTenant(tenantA);

    // Tenant A: two real rows (v="shared") whose partition keys sort AFTER B's.
    await post("/v1/mutate", { op: "insert", table: "idx_tenantcap_evt", tenantId: tenantA, partitionKey: "z-a-1", values: { v: "shared" } }, tokenA);
    await post("/v1/mutate", { op: "insert", table: "idx_tenantcap_evt", tenantId: tenantA, partitionKey: "z-a-2", values: { v: "shared" } }, tokenA);
    const aRows = await pollIndexRows("idx_tenantcap_by_v", (r) => r.length === 2);
    const indexKeyJson = aRows[0].index_key_json;

    // Find the index shard P that holds A's entries.
    let P: string | null = null;
    for (const shardId of ALL_TEST_SHARD_IDS) {
      const found = await runInDurableObject(env.SHARD.get(env.SHARD.idFromName(shardId)), async (_i: unknown, state: DurableObjectState) =>
        (Array.from(state.storage.sql.exec("SELECT COUNT(*) AS n FROM __cf_indexes WHERE index_name = 'idx_tenantcap_by_v'")) as Array<{ n: number }>)[0].n > 0,
      );
      if (found) { P = shardId; break; }
    }
    expect(P).not.toBeNull();

    // Tenant B: seed limit*5 (=10, for limit 2) entries on the SAME shard, SAME
    // key, whose partition keys sort BEFORE A's "z-..." keys — so under an
    // active-set-only LIMIT they fill the whole scan window ahead of A's.
    const tenantB = "tenant-cap-b";
    await runInDurableObject(env.SHARD.get(env.SHARD.idFromName(P as string)), async (_i: unknown, state: DurableObjectState) => {
      for (let i = 0; i < 10; i++) {
        state.storage.sql.exec(
          "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
          "idx_tenantcap_evt",
          "idx_tenantcap_by_v",
          indexKeyJson,
          `a-b-${i}`,
          P,
          tenantB,
          new Date().toISOString(),
        );
      }
    });

    // A's query (limit 2 → rawScanCap 10): without the tenant predicate the 10
    // B entries fill the scan window and A gets an empty result; with it, A's
    // own entries are scanned and returned — and no B row leaks.
    const res = await post("/v1/index-query", { table: "idx_tenantcap_evt", indexName: "idx_tenantcap_by_v", tenantId: tenantA, values: { v: "shared" }, limit: 2 }, tokenA);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rows: Array<{ id: string }> };
    expect(body.rows.map((r) => r.id).sort()).toEqual(["z-a-1", "z-a-2"]);
  });

  it("eng-review fix: rejects a query against an index still backfilling, 425 INDEX_BUILDING", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c4_building_evt");
    // Register directly against CatalogDO (bypassing the Worker's
    // /admin/create-index, which always backfills-then-marks-ready
    // synchronously before returning) so the index is left in its
    // just-registered 'building' state for this test to observe.
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO index_rules (index_name, table_name, columns_json, status, created_at) VALUES (?, ?, ?, 'building', ?)",
        "idx_c4_building_by_v",
        "idx_c4_building_evt",
        JSON.stringify(["v"]),
        new Date().toISOString(),
      );
    });
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const res = await post(
      "/v1/index-query",
      { table: "idx_c4_building_evt", indexName: "idx_c4_building_by_v", tenantId, values: { v: "alpha" } },
      token,
    );
    expect(res.status).toBe(425);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("INDEX_BUILDING");
  });

  // Review Tier 2 #12: hydration resolves all matched entries' shards in one
  // tenant-authenticated /route-batch call (not one /route per entry).
  // Multiple matches all hydrate; an entry belonging to another tenant that
  // shares the indexed value is NOT returned (isolation, previously enforced
  // by the per-entry /route auth check).
  it("returns all of one tenant's matching rows in a single query and never another tenant's row sharing the indexed value", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("idx_iso_evt");
    await post("/admin/create-index", { indexName: "idx_iso_by_v", table: "idx_iso_evt", columns: ["v"] }, AUTH());

    const tenantA = tenantForCatalogShard(0, 4);
    const tokenA = await registerTenant(tenantA);
    // A different tenant on the SAME catalog shard (so its rows share the
    // vbucket space), also writing the shared indexed value.
    let tenantB = "";
    for (let i = 0; ; i += 1) {
      const cand = `iso-b-${i}`;
      if (hashKey(cand) % 4 === 0) {
        tenantB = cand;
        break;
      }
    }
    const tokenB = await registerTenant(tenantB);

    // Tenant A: three rows with v='shared'. Tenant B: one row with v='shared'.
    for (const pk of ["a1", "a2", "a3"]) {
      const r = await post("/v1/mutate", { op: "insert", table: "idx_iso_evt", tenantId: tenantA, partitionKey: pk, values: { v: "shared" } }, tokenA);
      expect(r.status).toBe(200);
    }
    const rb = await post("/v1/mutate", { op: "insert", table: "idx_iso_evt", tenantId: tenantB, partitionKey: "b1", values: { v: "shared" } }, tokenB);
    expect(rb.status).toBe(200);

    // Let async index maintenance settle, then query as A.
    let bodyA: { rows: Array<{ id: string }> } = { rows: [] };
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const res = await post("/v1/index-query", { table: "idx_iso_evt", indexName: "idx_iso_by_v", tenantId: tenantA, values: { v: "shared" }, limit: 50 }, tokenA);
      expect(res.status).toBe(200);
      bodyA = (await res.json()) as { rows: Array<{ id: string }> };
      if (bodyA.rows.length === 3) break;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    expect(bodyA.rows.map((r) => r.id).sort()).toEqual(["a1", "a2", "a3"]); // all of A's, none of B's

    // And B sees only its own.
    const resB = await post("/v1/index-query", { table: "idx_iso_evt", indexName: "idx_iso_by_v", tenantId: tenantB, values: { v: "shared" }, limit: 50 }, tokenB);
    const bodyB = (await resB.json()) as { rows: Array<{ id: string }> };
    expect(bodyB.rows.map((r) => r.id)).toEqual(["b1"]);
  });
});

// Codex round-14 P2 (read-visibility during evacuation): ring evacuation
// repoints the query-visible ring to the substitute BEFORE copying the existing
// entries. /v1/index-query must dual-look-up the draining shard (via the
// replicated evacFromShards read-shadow) so a pre-existing key stays visible in
// that window, then reads only the substitute once evacuation completes.
describe("Worker /v1/index-query dual-lookup during ring evacuation (Codex round-14 P2)", () => {
  it("returns a pre-existing row during an in-progress evacuation (ring repointed, entry not yet copied), and still after completion (from the substitute)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("p2dual_evt");
    await post("/admin/create-index", { indexName: "p2dual_by_v", table: "p2dual_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // A pre-existing indexed row; wait for its async index entry to land on P.
    await post("/v1/mutate", { op: "insert", table: "p2dual_evt", tenantId, partitionKey: "row-1", values: { v: "alpha" } }, token);
    await pollIndexRows("p2dual_by_v", (r) => r.length === 1);

    const ring = ((await (await post("/admin/list-indexes", {}, AUTH())).json()) as { indexes: Array<{ indexName: string; placementRing: string[] }> })
      .indexes.find((i) => i.indexName === "p2dual_by_v")!.placementRing;
    const indexKeyJson = JSON.stringify(["alpha"]);
    const P = indexShardIdForKey("p2dual_evt", "p2dual_by_v", indexKeyJson, ring);
    const substitute = "catalog-0-shard-p2sub";
    const newRing = ring.map((s) => (s === P ? substitute : s));

    // Force the in-progress evacuation window: repoint the ring P→substitute AND
    // set the read-shadow evacFromShards=[P] on EVERY catalog, WITHOUT copying
    // the entry (substitute is still empty). This is exactly the state drain
    // evacuation is in between its repoint and its copy.
    for (let c = 0; c < 4; c += 1) {
      const catStub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${c}`));
      await catStub.fetch(new Request("https://catalog.internal/update-index-ring", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ indexName: "p2dual_by_v", ring: newRing, evacFromShards: [P] }) }));
    }

    // Query resolves to the (empty) substitute via the repointed ring — but the
    // dual-lookup still finds the pre-existing entry on the draining shard P.
    const during = await post("/v1/index-query", { table: "p2dual_evt", indexName: "p2dual_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(during.status).toBe(200);
    expect(((await during.json()) as { rows: Array<{ id: string }> }).rows.map((r) => r.id)).toEqual(["row-1"]);

    // Complete the evacuation: copy the entry to the substitute, then clear the
    // read-shadow (evacFromShards=[]) on every catalog. Reads now hit only the
    // substitute.
    const entry = await runInDurableObject(env.SHARD.get(env.SHARD.idFromName(P)), async (_i: unknown, state: DurableObjectState) =>
      (Array.from(state.storage.sql.exec("SELECT table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at FROM __cf_indexes WHERE index_name = 'p2dual_by_v'")) as Array<Record<string, unknown>>)[0],
    );
    await env.SHARD.get(env.SHARD.idFromName(substitute)).fetch(new Request("https://shard.internal/index-entries-import", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ rows: [entry] }) }));
    for (let c = 0; c < 4; c += 1) {
      const catStub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${c}`));
      await catStub.fetch(new Request("https://catalog.internal/update-index-ring", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ indexName: "p2dual_by_v", ring: newRing, evacFromShards: [] }) }));
    }

    const after = await post("/v1/index-query", { table: "p2dual_evt", indexName: "p2dual_by_v", tenantId, values: { v: "alpha" } }, token);
    expect(after.status).toBe(200);
    expect(((await after.json()) as { rows: Array<{ id: string }> }).rows.map((r) => r.id)).toEqual(["row-1"]);
  });

  // Codex round-15 P1 #1: the read-shadow must be the UNION of ALL concurrent
  // same-index evacuations' draining shards, not a singleton. A second drain's
  // repoint that overwrote the shadow with only its own shard would drop the
  // first drain's shard → /v1/index-query silently stops dual-reading it.
  it("unions the read-shadow across two concurrent evacuations of the same index; completing one leaves the other still shadowed and queryable", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("union_evt");
    await post("/admin/create-index", { indexName: "union_by_v", table: "union_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const ring = ((await (await post("/admin/list-indexes", {}, AUTH())).json()) as { indexes: Array<{ indexName: string; placementRing: string[] }> })
      .indexes.find((i) => i.indexName === "union_by_v")!.placementRing;

    // Two indexed values whose entries land on two DIFFERENT ring shards S1, S2.
    let V1 = "", V2 = "", S1 = "", S2 = "";
    for (let i = 0; V1 === "" || V2 === ""; i += 1) {
      const cand = `u-${i}`;
      const s = indexShardIdForKey("union_evt", "union_by_v", JSON.stringify([cand]), ring);
      if (V1 === "") { V1 = cand; S1 = s; }
      else if (s !== S1) { V2 = cand; S2 = s; }
    }
    expect(S1).not.toBe(S2);

    await post("/v1/mutate", { op: "insert", table: "union_evt", tenantId, partitionKey: "row-1", values: { v: V1 } }, token);
    await post("/v1/mutate", { op: "insert", table: "union_evt", tenantId, partitionKey: "row-2", values: { v: V2 } }, token);
    await pollIndexRows("union_by_v", (r) => r.length === 2);

    // Simulate two CONCURRENT evacuations (different catalogs) of the same index:
    // drain of S1 repoints S1→sub1 and MERGE-adds S1; drain of S2 repoints
    // S2→sub2 and MERGE-adds S2. Neither copies its entry yet. The shadow must
    // end up = {S1, S2}.
    const sub1 = "catalog-0-shard-usub1";
    const sub2 = "catalog-0-shard-usub2";
    const ring1 = ring.map((s) => (s === S1 ? sub1 : s));
    const ring2 = ring1.map((s) => (s === S2 ? sub2 : s));
    for (let c = 0; c < 4; c += 1) {
      const catStub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${c}`));
      await catStub.fetch(new Request("https://catalog.internal/update-index-ring", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ indexName: "union_by_v", ring: ring1, evacAdd: S1 }) }));
      await catStub.fetch(new Request("https://catalog.internal/update-index-ring", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ indexName: "union_by_v", ring: ring2, evacAdd: S2 }) }));
    }

    // Both draining shards are dual-read: V1 (entry on S1) and V2 (entry on S2)
    // are both found even though the ring now points at the empty substitutes.
    const q1 = await post("/v1/index-query", { table: "union_evt", indexName: "union_by_v", tenantId, values: { v: V1 } }, token);
    expect(((await q1.json()) as { rows: Array<{ id: string }> }).rows.map((r) => r.id)).toEqual(["row-1"]);
    const q2 = await post("/v1/index-query", { table: "union_evt", indexName: "union_by_v", tenantId, values: { v: V2 } }, token);
    expect(((await q2.json()) as { rows: Array<{ id: string }> }).rows.map((r) => r.id)).toEqual(["row-2"]);

    // Complete drain of S1: copy its entry to sub1, then MERGE-remove S1 (leaving
    // S2 shadowed). Both keys stay queryable.
    const e1 = await runInDurableObject(env.SHARD.get(env.SHARD.idFromName(S1)), async (_i: unknown, state: DurableObjectState) =>
      (Array.from(state.storage.sql.exec("SELECT table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at FROM __cf_indexes WHERE index_name = 'union_by_v'")) as Array<Record<string, unknown>>)[0],
    );
    await env.SHARD.get(env.SHARD.idFromName(sub1)).fetch(new Request("https://shard.internal/index-entries-import", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ rows: [e1] }) }));
    for (let c = 0; c < 4; c += 1) {
      const catStub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${c}`));
      await catStub.fetch(new Request("https://catalog.internal/update-index-ring", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ indexName: "union_by_v", ring: ring2, evacRemove: S1 }) }));
    }

    const q1b = await post("/v1/index-query", { table: "union_evt", indexName: "union_by_v", tenantId, values: { v: V1 } }, token);
    expect(((await q1b.json()) as { rows: Array<{ id: string }> }).rows.map((r) => r.id)).toEqual(["row-1"]); // from sub1
    const q2b = await post("/v1/index-query", { table: "union_evt", indexName: "union_by_v", tenantId, values: { v: V2 } }, token);
    expect(((await q2b.json()) as { rows: Array<{ id: string }> }).rows.map((r) => r.id)).toEqual(["row-2"]); // S2 still shadowed
  });
});

describe("Worker /v1/index-query read-time re-routing (Milestone 3, Chunk 2)", () => {
  it("still finds a matching row after its base row physically moves to a different shard and vbucket_map is flipped to point there (simulating what Chunk 4's migration does), proving hydration re-routes via the entry's recorded tenant_id rather than a stale source_shard_id snapshot", async () => {
    // /admin/init clamps totalVBuckets to a floor of 64 regardless of what's
    // requested here — matching that floor so the vbucket computed below
    // agrees with what CatalogDO actually stored.
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("idx_c2_reroute_evt");
    await post("/admin/create-index", { indexName: "idx_c2_reroute_by_v", table: "idx_c2_reroute_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const insertRes = await post(
      "/v1/mutate",
      { op: "insert", table: "idx_c2_reroute_evt", tenantId, partitionKey: "row-move", values: { v: "alpha" } },
      token,
    );
    expect(insertRes.status).toBe(200);
    await pollIndexRows("idx_c2_reroute_by_v", (r) => r.length === 1);

    // Confirm it resolves correctly before the move (sanity check).
    const before = await post(
      "/v1/index-query",
      { table: "idx_c2_reroute_evt", indexName: "idx_c2_reroute_by_v", tenantId, values: { v: "alpha" } },
      token,
    );
    expect((await before.json() as { rows: Array<{ id: string }> }).rows).toHaveLength(1);

    // Figure out which vbucket/shard this row currently lives on.
    const vbucket = hashKey(`${tenantId}:idx_c2_reroute_evt:row-move`) % 64;
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const fromShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const row = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return row[0].shard_id;
    });
    const toShardId = fromShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    // Simulate a completed migration: copy the row onto the other shard,
    // delete it from the original, then flip vbucket_map — exactly the end
    // state Chunk 4's cutover (step 4/5) will produce, just performed by
    // hand here instead of by that not-yet-built machinery.
    const toStub = env.SHARD.get(env.SHARD.idFromName(toShardId));
    await toStub.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "INSERT INTO idx_c2_reroute_evt (id, v) VALUES (?, ?)",
          params: ["row-move", "alpha"],
          requestId: `reroute-copy-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      }),
    );
    const fromStub = env.SHARD.get(env.SHARD.idFromName(fromShardId));
    await fromStub.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "DELETE FROM idx_c2_reroute_evt WHERE id = ?",
          params: ["row-move"],
          requestId: `reroute-delete-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      }),
    );
    await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "UPDATE vbucket_map SET shard_id = ?, updated_at = ? WHERE vbucket = ?",
        toShardId,
        new Date().toISOString(),
        vbucket,
      );
    });

    // The index entry itself was never touched (still whatever tenant_id
    // was recorded at write time) — hydration must recompute the row's
    // current shard via /route rather than trust any physical shard
    // snapshot, so this must still find it on its NEW shard.
    const after = await post(
      "/v1/index-query",
      { table: "idx_c2_reroute_evt", indexName: "idx_c2_reroute_by_v", tenantId, values: { v: "alpha" } },
      token,
    );
    expect(after.status).toBe(200);
    const afterBody = (await after.json()) as { rows: Array<{ id: string; v: string }> };
    expect(afterBody.rows).toHaveLength(1);
    expect(afterBody.rows[0].id).toBe("row-move");
  });

  it("returns PROVENANCE_MISSING_FOR_INDEX when a row has no __cf_row_owners entry at /admin/create-index backfill time", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c2_noprov_evt");

    // Write the row directly to the shard, bypassing every write path that
    // would normally record __cf_row_owners (Chunk 0) — simulates a row
    // written before Milestone 3 Chunk 0 shipped.
    const shardStub = env.SHARD.get(env.SHARD.idFromName("catalog-0-shard-0"));
    await shardStub.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "INSERT INTO idx_c2_noprov_evt (id, v) VALUES (?, ?)",
          params: ["row-no-prov", "alpha"],
          requestId: `noprov-insert-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      }),
    );

    const res = await post("/admin/create-index", { indexName: "idx_c2_noprov_by_v", table: "idx_c2_noprov_evt", columns: ["v"] }, AUTH());
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("PROVENANCE_MISSING_FOR_INDEX");
  });
});
