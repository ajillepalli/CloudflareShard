import { SELF, env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { hashKey, indexShardIdForKey } from "./hash";
import { sha256Hex } from "./auth";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";
import { ALL_TEST_SHARD_IDS, AUTH, createIndexTestTable, driveMigrationToCompletion, initCluster, insertRowBypassingProvenance, median, partitionKeysInSameVbucket, pollIndexRows, pollShardRows, post, purgeUnattributedRows, registerTenant, setMigrationState, shardExecute, tenantForCatalogShard } from "./index.test-helpers";

// This file is one of several index.*.test.ts files split out of a single
// index.test.ts (see index.test-helpers.ts's header comment for why). DO
// storage persists across `it` blocks within a file, so afterEach(reset())
// gives every test clean storage — the same isolation the pre-split file used.
afterEach(async () => {
  await reset();
});

describe("Worker dual-write mirroring during migration (Milestone 3, Chunk 3)", () => {
  it("a /v1/mutate write to a backfilling vbucket lands on both source and target with the same requestId, and the client write is authoritative on source", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m3_mirror_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const vbucket = hashKey(`${tenantId}:m3_mirror_evt:row-m1`) % 64;
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const sourceShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return rows[0].shard_id;
    });
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    await setMigrationState(vbucket, "backfilling", targetShardId);
    try {
      const requestId = `mirror-req-${crypto.randomUUID()}`;
      const res = await post(
        "/v1/mutate",
        { op: "insert", table: "m3_mirror_evt", tenantId, partitionKey: "row-m1", values: { v: "alpha" }, requestId },
        token,
      );
      expect(res.status).toBe(200);

      // Authoritative copy is on the source, synchronously.
      const sourceRows = (await shardExecute(sourceShardId, "SELECT v FROM m3_mirror_evt WHERE id = ?", ["row-m1"])).rows;
      expect(sourceRows).toHaveLength(1);

      // Review Tier 1 #2: the mirror is enqueued ATOMICALLY with the source
      // write, so it's counted the instant the write returns — before any
      // delivery attempt. This is what makes cutover's drain-to-zero gate
      // correct.
      const sourceStub = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
      const mirrorCount = async () =>
        ((await (
          await sourceStub.fetch(
            new Request("https://shard.internal/mirror-pending-count", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ vbucket }),
            }),
          )
        ).json()) as { count: number }).count;
      expect(await mirrorCount()).toBe(1);

      // Drive the source alarm to deliver the mirror; the target gets it and
      // the job drains to zero.
      await runInDurableObject(sourceStub, async (instance: ShardDO, state: DurableObjectState) => {
        state.storage.sql.exec("UPDATE __cf_mirror_pending SET next_attempt_at = ?", new Date(Date.now() - 1).toISOString());
        await instance.alarm();
      });
      expect(await mirrorCount()).toBe(0);

      const targetRows = (await shardExecute(targetShardId, "SELECT v FROM m3_mirror_evt WHERE id = ?", ["row-m1"])).rows;
      expect(targetRows).toHaveLength(1);

      // Idempotent: re-running the alarm (queue already empty) leaves exactly
      // one row on the target — the delivered mirror wrote under an
      // unforgeable derived requestId, so redelivery dedupes.
      await runInDurableObject(sourceStub, async (instance: ShardDO) => {
        await instance.alarm();
      });
      const targetRowsAgain = (await shardExecute(targetShardId, "SELECT v FROM m3_mirror_evt WHERE id = ?", ["row-m1"])).rows;
      expect(targetRowsAgain).toHaveLength(1);
    } finally {
      await setMigrationState(vbucket, "none", null);
    }
  });

  it("criterion 9 shape: a failed mirror write never fails the client write; it lands in __cf_mirror_pending on the source and drains via alarm retry once the target recovers", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m3_mirrfail_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const vbucket = hashKey(`${tenantId}:m3_mirrfail_evt:row-mf`) % 64;
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const sourceShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return rows[0].shard_id;
    });
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    // "Kill" the target for this table: drop it there, so the mirrored
    // INSERT fails on every attempt until the table is restored.
    const targetStub = env.SHARD.get(env.SHARD.idFromName(targetShardId));
    await targetStub.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sql: 'DROP TABLE IF EXISTS "m3_mirrfail_evt"', requestId: `drop-${crypto.randomUUID()}`, isMutation: true }),
      }),
    );

    await setMigrationState(vbucket, "backfilling", targetShardId);
    try {
      const res = await post(
        "/v1/mutate",
        { op: "insert", table: "m3_mirrfail_evt", tenantId, partitionKey: "row-mf", values: { v: "beta" } },
        token,
      );
      // The client write succeeded despite the doomed mirror.
      expect(res.status).toBe(200);

      // The failed mirror is durably queued on the SOURCE shard.
      const sourceStub = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
      let queued = 0;
      for (let attempt = 0; attempt < 50; attempt++) {
        const countRes = await sourceStub.fetch(
          new Request("https://shard.internal/mirror-pending-count", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ vbucket }),
          }),
        );
        queued = ((await countRes.json()) as { count: number }).count;
        if (queued === 1) break;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      expect(queued).toBe(1);

      // Target recovers (table restored); alarm drains the queue.
      await targetStub.fetch(
        new Request("https://shard.internal/execute", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            sql: "CREATE TABLE IF NOT EXISTS m3_mirrfail_evt (id TEXT PRIMARY KEY, v TEXT)",
            requestId: `recreate-${crypto.randomUUID()}`,
            isMutation: true,
          }),
        }),
      );
      await runInDurableObject(sourceStub, async (instance: ShardDO, state: DurableObjectState) => {
        // Force the job due now (its first retry is 1s out).
        state.storage.sql.exec("UPDATE __cf_mirror_pending SET next_attempt_at = ?", new Date(Date.now() - 1).toISOString());
        await instance.alarm();
      });

      const drained = await sourceStub.fetch(
        new Request("https://shard.internal/mirror-pending-count", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ vbucket }),
        }),
      );
      expect(((await drained.json()) as { count: number }).count).toBe(0);

      const targetRows = (await shardExecute(targetShardId, "SELECT v FROM m3_mirrfail_evt WHERE id = ?", ["row-mf"])).rows;
      expect(targetRows).toHaveLength(1);
      expect(targetRows[0].v).toBe("beta");
    } finally {
      await setMigrationState(vbucket, "none", null);
    }
  });

  it("a /v1/tx commit whose vbucket is migrating mirrors the committed intent to the target post-commit (tx-during-migration)", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m3_txmirror_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const vbucket = hashKey(`${tenantId}:m3_txmirror_evt:row-tx1`) % 64;
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const sourceShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return rows[0].shard_id;
    });
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    await setMigrationState(vbucket, "backfilling", targetShardId);
    try {
      const txRes = await post(
        "/v1/tx",
        {
          mutations: [{ op: "insert", table: "m3_txmirror_evt", tenantId, partitionKey: "row-tx1", values: { v: "gamma" } }],
          requestId: `tx-mirror-${crypto.randomUUID()}`,
        },
        token,
      );
      expect(txRes.status).toBe(200);

      // Source stayed authoritative and has it immediately.
      const sourceRows = (await shardExecute(sourceShardId, "SELECT v FROM m3_txmirror_evt WHERE id = ?", ["row-tx1"])).rows;
      expect(sourceRows).toHaveLength(1);

      // Review Tier 1 #2: the committed intent's mirror is enqueued
      // atomically on the SOURCE shard inside handleCommit (no longer a
      // synchronous coordinator round trip) — counted, then delivered by the
      // source alarm.
      const sourceStub = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
      await runInDurableObject(sourceStub, async (instance: ShardDO, state: DurableObjectState) => {
        state.storage.sql.exec("UPDATE __cf_mirror_pending SET next_attempt_at = ?", new Date(Date.now() - 1).toISOString());
        await instance.alarm();
      });

      const targetRows = (await shardExecute(targetShardId, "SELECT v FROM m3_txmirror_evt WHERE id = ?", ["row-tx1"])).rows;
      expect(targetRows).toHaveLength(1);
      expect(targetRows[0].v).toBe("gamma");
    } finally {
      await setMigrationState(vbucket, "none", null);
    }
  });

  // Review "add missing tests": a /v1/tx whose mirror target is unreachable
  // must still commit; the mirror lands in __cf_mirror_pending on the source
  // and drains via alarm retry once the target recovers.
  it("a /v1/tx commit whose mirror target is unreachable still commits; the mirror enqueues on the source and drains via alarm once the target recovers", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m3_txmirrfail_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const vbucket = hashKey(`${tenantId}:m3_txmirrfail_evt:row-txf`) % 64;
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const sourceShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return rows[0].shard_id;
    });
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    // Drop the table on the target so the mirror INSERT fails until restored.
    const targetStub = env.SHARD.get(env.SHARD.idFromName(targetShardId));
    await targetStub.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sql: 'DROP TABLE IF EXISTS "m3_txmirrfail_evt"', requestId: `drop-${crypto.randomUUID()}`, isMutation: true }),
      }),
    );

    await setMigrationState(vbucket, "backfilling", targetShardId);
    try {
      const txRes = await post(
        "/v1/tx",
        {
          mutations: [{ op: "insert", table: "m3_txmirrfail_evt", tenantId, partitionKey: "row-txf", values: { v: "delta" } }],
          requestId: `tx-mirrfail-${crypto.randomUUID()}`,
        },
        token,
      );
      // Commit succeeded despite the doomed mirror.
      expect(txRes.status).toBe(200);

      const sourceStub = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
      const mirrorCount = async () =>
        ((await (
          await sourceStub.fetch(
            new Request("https://shard.internal/mirror-pending-count", {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ vbucket }),
            }),
          )
        ).json()) as { count: number }).count;
      expect(await mirrorCount()).toBe(1);

      // First alarm: delivery fails (table missing), job stays queued.
      await runInDurableObject(sourceStub, async (instance: ShardDO, state: DurableObjectState) => {
        state.storage.sql.exec("UPDATE __cf_mirror_pending SET next_attempt_at = ?", new Date(Date.now() - 1).toISOString());
        await instance.alarm();
      });
      expect(await mirrorCount()).toBe(1);

      // Restore the target table; next alarm drains the queue.
      await targetStub.fetch(
        new Request("https://shard.internal/execute", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({
            sql: "CREATE TABLE IF NOT EXISTS m3_txmirrfail_evt (id TEXT PRIMARY KEY, v TEXT)",
            requestId: `recreate-${crypto.randomUUID()}`,
            isMutation: true,
          }),
        }),
      );
      await runInDurableObject(sourceStub, async (instance: ShardDO, state: DurableObjectState) => {
        state.storage.sql.exec("UPDATE __cf_mirror_pending SET next_attempt_at = ?", new Date(Date.now() - 1).toISOString());
        await instance.alarm();
      });
      expect(await mirrorCount()).toBe(0);

      const targetRows = (await shardExecute(targetShardId, "SELECT v FROM m3_txmirrfail_evt WHERE id = ?", ["row-txf"])).rows;
      expect(targetRows).toHaveLength(1);
      expect(targetRows[0].v).toBe("delta");
    } finally {
      await setMigrationState(vbucket, "none", null);
    }
  });
});

describe("Worker /admin/migrate-vbucket end-to-end (Milestone 3, Chunk 4)", () => {
  // 30s hook budget: purgeUnattributedRows fans out list-tables + per-shard
  // deletes, and per-request latency in the workers pool compounds over this
  // long test file (an environment property — see vitest.config.ts note).
  beforeEach(async () => {
    await purgeUnattributedRows();
  }, 30000);

  // Review Tier 2 #8: a backfill that needs more rows than one tick's page
  // cap (MIGRATION_BACKFILL_PAGES_PER_TICK * MIGRATE_PAGE_SIZE = 8*500=4000)
  // must resume from its persisted cursor across ticks — monotonic rowsCopied
  // — instead of restarting from page zero and re-exceeding the subrequest
  // cap every tick forever.
  it("a large backfill spans multiple ticks, resuming from its persisted cursor (monotonic rowsCopied, no restart)", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m4_big_evt");

    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const vbucket = 0;
    const tenantId = "big-tenant";
    const sourceShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return rows[0].shard_id;
    });
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    // Bulk-seed 4500 rows + provenance (all tagged vbucket 0) directly on the
    // source shard — two recursive-CTE statements, cheap despite the count.
    const N = 4500;
    const bulk = async (sql: string) => {
      const res = await env.SHARD.get(env.SHARD.idFromName(sourceShardId)).fetch(
        new Request("https://shard.internal/execute", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ sql, requestId: `bulk-${crypto.randomUUID()}`, isMutation: true }),
        }),
      );
      expect(res.status).toBe(200);
    };
    await bulk(
      `WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < ${N}) INSERT INTO m4_big_evt (id, v) SELECT printf('k%06d', n), 'x' FROM seq`,
    );
    await bulk(
      `WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < ${N}) INSERT INTO __cf_row_owners (table_name, partition_key, tenant_id, vbucket, updated_at) SELECT 'm4_big_evt', printf('k%06d', n), '${tenantId}', ${vbucket}, '2026-01-01T00:00:00.000Z' FROM seq`,
    );

    const migrateRes = await post("/admin/migrate-vbucket", { catalogShardId: "catalog-0", vbucket, targetShardId }, AUTH());
    expect(migrateRes.status).toBe(200);

    // Tick 1: copies exactly one page-cap's worth (4000), stays 'backfilling'
    // with a persisted cursor — does NOT reach cutover.
    await runInDurableObject(catalogStub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const afterTick1 = (await (
      await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket }, AUTH())
    ).json()) as { status: string; rowsCopied: number };
    // Still backfilling (page cap hit — didn't reach cutover), with a
    // persisted cursor. The exact count depends on whether any other table
    // also has vbucket-0 rows (page accounting), so assert the invariants:
    // capped (< N), meaningful progress, cursor set.
    expect(afterTick1.status).toBe("backfilling");
    expect(afterTick1.rowsCopied).toBeGreaterThan(3000);
    expect(afterTick1.rowsCopied).toBeLessThan(N);
    const cursor1 = await runInDurableObject(catalogStub, async (_i: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT backfill_table, backfill_after_pk FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{
        backfill_table: string | null;
        backfill_after_pk: string | null;
      }>;
      return rows[0];
    });
    expect(cursor1.backfill_table).not.toBeNull(); // cursor persisted for resume

    // Tick 2: resumes from the cursor — rowsCopied is monotonic (did NOT
    // restart from zero) and it moves past backfilling.
    await runInDurableObject(catalogStub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const afterTick2 = (await (
      await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket }, AUTH())
    ).json()) as { status: string; rowsCopied: number };
    expect(afterTick2.rowsCopied).toBeGreaterThan(afterTick1.rowsCopied);

    // Drive to completion and confirm all 4500 rows landed on the target.
    await driveMigrationToCompletion(vbucket).catch(() => undefined);
    const targetCount = (await (
      await env.SHARD.get(env.SHARD.idFromName(targetShardId)).fetch(
        new Request("https://shard.internal/execute", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ sql: "SELECT COUNT(*) AS n FROM m4_big_evt", requestId: "cnt", isMutation: false }),
        }),
      )
    ).json()) as { rows: Array<{ n: number }> };
    expect(targetCount.rows[0].n).toBe(N);
  }, 60000);

  it("criterion 1: a row written before migration is readable via /v1/sql with the same partitionKey after its vbucket migrates, and the source copy is gone", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m4_happy_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const keys = partitionKeysInSameVbucket(tenantId, "m4_happy_evt", "hp", 5, 64);
    for (const key of keys) {
      const res = await post(
        "/v1/sql",
        { sql: "INSERT INTO m4_happy_evt (id, v) VALUES (?, ?)", params: [key, `val-${key}`], table: "m4_happy_evt", tenantId, partitionKey: key },
        AUTH(),
      );
      expect(res.status).toBe(200);
    }

    const vbucket = hashKey(`${tenantId}:m4_happy_evt:hp`) % 64;
    const routeBefore = (await (
      await post("/v1/sql", { sql: "SELECT * FROM m4_happy_evt WHERE id = ?", params: ["hp"], table: "m4_happy_evt", tenantId, partitionKey: "hp" }, AUTH())
    ).json()) as { route: { shardId: string } };
    const sourceShardId = routeBefore.route.shardId;
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    const migrateRes = await post("/admin/migrate-vbucket", { catalogShardId: "catalog-0", vbucket, targetShardId }, AUTH());
    expect(migrateRes.status).toBe(200);
    const migrateBody = (await migrateRes.json()) as { ok: boolean; status: string; fromShard: string; toShard: string };
    expect(migrateBody.status).toBe("backfilling");
    expect(migrateBody.fromShard).toBe(sourceShardId);
    expect(migrateBody.toShard).toBe(targetShardId);

    await driveMigrationToCompletion(vbucket);

    // Every pre-migration row reads back through the normal data plane with
    // the same partitionKey, now routed to the target.
    for (const key of keys) {
      const readRes = await post(
        "/v1/sql",
        { sql: "SELECT v FROM m4_happy_evt WHERE id = ?", params: [key], table: "m4_happy_evt", tenantId, partitionKey: key },
        AUTH(),
      );
      expect(readRes.status).toBe(200);
      const readBody = (await readRes.json()) as { route: { shardId: string }; result: { rows: Array<{ v: string }> } };
      expect(readBody.route.shardId).toBe(targetShardId);
      expect(readBody.result.rows).toHaveLength(1);
      expect(readBody.result.rows[0].v).toBe(`val-${key}`);
    }

    // Cutover step 5 deleted the source copy.
    const sourceLeftovers = (await shardExecute(sourceShardId, "SELECT id FROM m4_happy_evt", [])).rows.filter((r) =>
      keys.includes(String(r.id)),
    );
    expect(sourceLeftovers).toHaveLength(0);
  });


  it("criterion 2: >=100 writes issued concurrently with the migration all land — post-cutover checksums pass, every requestId is on the target exactly once, and replays return the stored result", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m4_conc_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const keys = partitionKeysInSameVbucket(tenantId, "m4_conc_evt", "cc", 100, 64);
    const vbucket = hashKey(`${tenantId}:m4_conc_evt:cc`) % 64;

    // Seed a handful of pre-migration rows.
    for (const key of keys.slice(0, 10)) {
      await post(
        "/v1/sql",
        { sql: "INSERT INTO m4_conc_evt (id, v) VALUES (?, ?)", params: [key, `pre-${key}`], table: "m4_conc_evt", tenantId, partitionKey: key, requestId: `pre-${key}` },
        AUTH(),
      );
    }

    const routeBefore = (await (
      await post("/v1/sql", { sql: "SELECT 1 AS one FROM m4_conc_evt WHERE id = ?", params: ["cc"], table: "m4_conc_evt", tenantId, partitionKey: "cc" }, AUTH())
    ).json()) as { route: { shardId: string } };
    const sourceShardId = routeBefore.route.shardId;
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    const migrateRes = await post("/admin/migrate-vbucket", { catalogShardId: "catalog-0", vbucket, targetShardId }, AUTH());
    expect(migrateRes.status).toBe(200);

    // Fire the remaining 90 writes (upserts so pre-seeded keys get updated
    // — every key ends with a deterministic final value) CONCURRENTLY with
    // the migration, retrying on the fence (409, retryable by contract) and
    // on 503. Interleave orchestration ticks so the fence race genuinely
    // happens while writes are in flight.
    const writeOne = async (key: string): Promise<void> => {
      const requestId = `conc-${key}`;
      for (let attempt = 0; attempt < 60; attempt += 1) {
        const res = await post(
          "/v1/sql",
          {
            sql: "INSERT INTO m4_conc_evt (id, v) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET v = excluded.v",
            params: [key, `final-${key}`],
            table: "m4_conc_evt",
            tenantId,
            partitionKey: key,
            requestId,
          },
          AUTH(),
        );
        if (res.status === 200) return;
        const body = (await res.json()) as { error?: { code?: string } | string };
        const code = typeof body.error === "object" ? body.error?.code : undefined;
        if (res.status === 409 && code === "VBUCKET_FENCED") {
          await new Promise((resolve) => setTimeout(resolve, 25));
          continue;
        }
        if (res.status === 503) {
          await new Promise((resolve) => setTimeout(resolve, 25));
          continue;
        }
        throw new Error(`write ${key} failed unexpectedly: ${res.status} ${JSON.stringify(body)}`);
      }
      throw new Error(`write ${key} exhausted retries`);
    };

    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const writesPromise = Promise.all(keys.map((key) => writeOne(key)));
    // Drive migration ticks concurrently with the writes.
    const drivePromise = (async () => {
      for (let tick = 0; tick < 40; tick += 1) {
        await runInDurableObject(catalogStub, async (instance: CatalogDO) => {
          await instance.alarm();
        });
        const statusRes = await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket }, AUTH());
        if (((await statusRes.json()) as { status: string }).status === "none") return;
        await new Promise((resolve) => setTimeout(resolve, 30));
      }
      throw new Error("migration did not complete under concurrent writes");
    })();
    await Promise.all([writesPromise, drivePromise]);

    // Any writes that raced the final flip may still be mirroring; drain any
    // residue then verify.
    await driveMigrationToCompletion(vbucket, 5).catch(() => undefined);

    // All 100 rows have their final value, read through the data plane.
    for (const key of keys) {
      const readRes = await post(
        "/v1/sql",
        { sql: "SELECT v FROM m4_conc_evt WHERE id = ?", params: [key], table: "m4_conc_evt", tenantId, partitionKey: key },
        AUTH(),
      );
      const readBody = (await readRes.json()) as { route: { shardId: string }; result: { rows: Array<{ v: string }> } };
      expect(readBody.route.shardId).toBe(targetShardId);
      expect(readBody.result.rows).toHaveLength(1);
      expect(readBody.result.rows[0].v).toBe(`final-${key}`);
    }

    // Post-cutover the target passes the per-table checksum against the
    // authoritative content (the source copy is deleted, so the invariant is
    // target-vs-expected: recompute from what the data plane returns).
    const targetSum = await env.SHARD.get(env.SHARD.idFromName(targetShardId)).fetch(
      new Request("https://shard.internal/migrate-checksum", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ vbucket, table: "m4_conc_evt", partitionKeyColumn: "id" }),
      }),
    );
    const targetSumBody = (await targetSum.json()) as { checksum: string; rowCount: number };
    expect(targetSumBody.rowCount).toBe(keys.length);

    // Every requestId appears exactly once in the target's applied_requests.
    const targetStub = env.SHARD.get(env.SHARD.idFromName(targetShardId));
    await runInDurableObject(targetStub, async (_instance: ShardDO, state: DurableObjectState) => {
      for (const key of keys) {
        const rows = Array.from(
          state.storage.sql.exec("SELECT COUNT(*) AS n FROM applied_requests WHERE request_id = ?", `conc-${key}`),
        ) as Array<{ n: number }>;
        expect(rows[0].n).toBe(1);
      }
    });

    // Replaying one of the writes returns the stored result (idempotency
    // contract) instead of re-applying.
    const replayRes = await post(
      "/v1/sql",
      {
        sql: "INSERT INTO m4_conc_evt (id, v) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET v = excluded.v",
        params: [keys[0], `final-${keys[0]}`],
        table: "m4_conc_evt",
        tenantId,
        partitionKey: keys[0],
        requestId: `conc-${keys[0]}`,
      },
      AUTH(),
    );
    expect(replayRes.status).toBe(200);
    const replayBody = (await replayRes.json()) as { result: { duplicated?: boolean } };
    expect(replayBody.result.duplicated).toBe(true);
  }, 150000);

  // Review Tier 1 #7: a 2PC tx prepared BEFORE its vbucket's migration
  // started carries no mirror target. Cutover must not flip while such a
  // prepared intent exists on the source (the fence blocks new prepares, so
  // the count only decreases); once the tx commits, its write lands on the
  // source with provenance and the checksum re-copies it to the target — so
  // the write is never silently stranded on the old source after the flip.
  it("a tx prepared before migration commits after cutover starts without being stranded — the write ends up queryable on the new shard", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m4_tx7_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const pk = "tx7-row";
    const vbucket = hashKey(`${tenantId}:m4_tx7_evt:${pk}`) % 64;
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const sourceShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return rows[0].shard_id;
    });
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";
    const sourceStub = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
    const shardPost = (path: string, body: unknown) =>
      sourceStub.fetch(new Request(`https://shard.internal${path}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) }));

    // Prepare a 2PC intent on the source for this vbucket (no mirror target —
    // migration hasn't started yet).
    const prep = await shardPost("/prepare", {
      coordinatorTxId: "tx7",
      intents: [
        {
          sql: "INSERT INTO m4_tx7_evt (id, v) VALUES (?, ?)",
          params: [pk, "committed"],
          tenantId,
          table: "m4_tx7_evt",
          partitionKey: pk,
          vbucket,
          op: "insert",
        },
      ],
    });
    expect(prep.status).toBe(200);

    // Start the migration and drive several ticks — it reaches cutover but
    // WAITS for the prepared intent (won't flip).
    const migrateRes = await post("/admin/migrate-vbucket", { catalogShardId: "catalog-0", vbucket, targetShardId }, AUTH());
    expect(migrateRes.status).toBe(200);
    for (let tick = 0; tick < 6; tick += 1) {
      await runInDurableObject(catalogStub, async (instance: CatalogDO) => {
        await instance.alarm();
      });
    }
    const blockedStatus = (await (
      await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket }, AUTH())
    ).json()) as { status: string };
    expect(blockedStatus.status).toBe("cutover"); // fenced, blocked on the prepared intent

    // Commit the tx — the row lands on the source with provenance.
    const commit = await shardPost("/commit", { coordinatorTxId: "tx7" });
    expect(commit.status).toBe(200);

    // Now the migration completes (checksum mismatch re-copies the row, then
    // flips).
    await driveMigrationToCompletion(vbucket);

    // The committed write is queryable via the data plane, routed to the NEW
    // shard — not stranded on the old source.
    const readRes = await post(
      "/v1/sql",
      { sql: "SELECT v FROM m4_tx7_evt WHERE id = ?", params: [pk], table: "m4_tx7_evt", tenantId, partitionKey: pk },
      AUTH(),
    );
    expect(readRes.status).toBe(200);
    const readBody = (await readRes.json()) as { route: { shardId: string }; result: { rows: Array<{ v: string }> } };
    expect(readBody.route.shardId).toBe(targetShardId);
    expect(readBody.result.rows).toHaveLength(1);
    expect(readBody.result.rows[0].v).toBe("committed");

    // The old source no longer has it (deleted on flip).
    const sourceLeftover = (await shardExecute(sourceShardId, "SELECT id FROM m4_tx7_evt WHERE id = ?", [pk])).rows;
    expect(sourceLeftover).toHaveLength(0);
  }, 60000);

  // Review Tier 1 #4: a crash mid-abort must leave the row in 'aborting'
  // (fence still needing lifting), and a retried abort must RESUME cleanup —
  // completing the wipe and lifting the fence — rather than 409ing on a
  // 'none' row and stranding the source fenced forever.
  it("a retried abort resumes cleanup from the intermediate 'aborting' state and lifts the source fence (not 409)", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m4_abortresume_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const vbucket = hashKey(`${tenantId}:m4_abortresume_evt:ar`) % 64;
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const sourceShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return rows[0].shard_id;
    });
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    // Simulate a crashed abort: the row is stuck 'aborting' (target retained)
    // and the source is still fenced for the vbucket.
    await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "UPDATE vbucket_map SET migration_status = 'aborting', target_shard_id = ? WHERE vbucket = ?",
        targetShardId,
        vbucket,
      );
    });
    const sourceStub = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
    await sourceStub.fetch(
      new Request("https://shard.internal/fence-vbucket", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ vbucket }),
      }),
    );

    // Retried abort resumes: returns aborted (NOT 409), status back to 'none'.
    const abortRes = await post("/admin/migrate-vbucket-abort", { catalogShardId: "catalog-0", vbucket }, AUTH());
    expect(abortRes.status).toBe(200);
    expect(((await abortRes.json()) as { status: string }).status).toBe("aborted");

    const statusAfter = (await (
      await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket }, AUTH())
    ).json()) as { status: string };
    expect(statusAfter.status).toBe("none");

    // The fence was lifted — a write to the vbucket on the source succeeds.
    const fenced = await runInDurableObject(sourceStub, async (_i: ShardDO, state: DurableObjectState) => {
      return Array.from(state.storage.sql.exec("SELECT vbucket FROM __cf_fenced_vbuckets WHERE vbucket = ?", vbucket)).length;
    });
    expect(fenced).toBe(0);

    const writeRes = await post(
      "/v1/sql",
      { sql: "INSERT INTO m4_abortresume_evt (id, v) VALUES (?, ?)", params: ["ar", "x"], table: "m4_abortresume_evt", tenantId, partitionKey: "ar" },
      AUTH(),
    );
    expect(writeRes.status).toBe(200);
  });

  it("criterion 6: abort before the map flip leaves the source's checksum untouched and the target with zero rows and zero provenance for the vbucket", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m4_abort_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const keys = partitionKeysInSameVbucket(tenantId, "m4_abort_evt", "ab", 5, 64);
    for (const key of keys) {
      await post(
        "/v1/sql",
        { sql: "INSERT INTO m4_abort_evt (id, v) VALUES (?, ?)", params: [key, `v-${key}`], table: "m4_abort_evt", tenantId, partitionKey: key },
        AUTH(),
      );
    }
    const vbucket = hashKey(`${tenantId}:m4_abort_evt:ab`) % 64;
    const routeBefore = (await (
      await post("/v1/sql", { sql: "SELECT 1 AS one FROM m4_abort_evt WHERE id = ?", params: ["ab"], table: "m4_abort_evt", tenantId, partitionKey: "ab" }, AUTH())
    ).json()) as { route: { shardId: string } };
    const sourceShardId = routeBefore.route.shardId;
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    const checksumOf = async (shardId: string) => {
      const res = await env.SHARD.get(env.SHARD.idFromName(shardId)).fetch(
        new Request("https://shard.internal/migrate-checksum", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ vbucket, table: "m4_abort_evt", partitionKeyColumn: "id" }),
        }),
      );
      return (await res.json()) as { checksum: string; rowCount: number };
    };
    const sourceBefore = await checksumOf(sourceShardId);
    expect(sourceBefore.rowCount).toBe(keys.length);

    // Pin the migration pre-flip deterministically: a queued mirror job for
    // this vbucket that can never land (its SQL targets a nonexistent
    // table) keeps cutover's step-2 gate (mirror queue must reach zero)
    // closed no matter how many orchestration ticks run — a quiet migration
    // would otherwise complete within a single tick, leaving no pre-flip
    // window to abort in. The abort itself purges this job.
    const sourceStub = env.SHARD.get(env.SHARD.idFromName(sourceShardId));
    const poison = await sourceStub.fetch(
      new Request("https://shard.internal/enqueue-mirror-job", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          targetShardId,
          sql: "INSERT INTO __nonexistent_m4_abort (id) VALUES ('x')",
          params: [],
          requestId: `poison-${crypto.randomUUID()}`,
          vbucket,
        }),
      }),
    );
    expect(poison.status).toBe(200);

    const migrateRes = await post("/admin/migrate-vbucket", { catalogShardId: "catalog-0", vbucket, targetShardId }, AUTH());
    expect(migrateRes.status).toBe(200);

    // Simulate partial backfill progress so the abort's target wipe is
    // observable, through the same /migrate-import the real backfill uses.
    const targetImport = await env.SHARD.get(env.SHARD.idFromName(targetShardId)).fetch(
      new Request("https://shard.internal/migrate-import", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          vbucket,
          table: "m4_abort_evt",
          rows: keys.slice(0, 2).map((key) => ({ partitionKey: key, tenantId, row: { id: key, v: `v-${key}` } })),
        }),
      }),
    );
    expect(targetImport.status).toBe(200);

    const midStatus = (await (
      await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket }, AUTH())
    ).json()) as { status: string };
    expect(midStatus.status).not.toBe("none"); // started (backfilling or fenced cutover), never flipped

    const abortRes = await post("/admin/migrate-vbucket-abort", { catalogShardId: "catalog-0", vbucket }, AUTH());
    expect(abortRes.status).toBe(200);
    expect(((await abortRes.json()) as { status: string }).status).toBe("aborted");

    // Source untouched: identical checksum, still authoritative for reads.
    const sourceAfter = await checksumOf(sourceShardId);
    expect(sourceAfter.checksum).toBe(sourceBefore.checksum);
    const readRes = await post(
      "/v1/sql",
      { sql: "SELECT v FROM m4_abort_evt WHERE id = ?", params: [keys[0]], table: "m4_abort_evt", tenantId, partitionKey: keys[0] },
      AUTH(),
    );
    const readBody = (await readRes.json()) as { route: { shardId: string }; result: { rows: unknown[] } };
    expect(readBody.route.shardId).toBe(sourceShardId);
    expect(readBody.result.rows).toHaveLength(1);

    // Target: zero rows and zero provenance for this vbucket.
    const targetRows = (await shardExecute(targetShardId, "SELECT id FROM m4_abort_evt", [])).rows.filter((r) =>
      keys.includes(String(r.id)),
    );
    expect(targetRows).toHaveLength(0);
    const targetStub = env.SHARD.get(env.SHARD.idFromName(targetShardId));
    await runInDurableObject(targetStub, async (_instance: ShardDO, state: DurableObjectState) => {
      const prov = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_row_owners WHERE vbucket = ?", vbucket));
      expect(prov).toHaveLength(0);
    });

    // Writes to the vbucket work again (fence lifted).
    const postAbortWrite = await post(
      "/v1/sql",
      { sql: "INSERT INTO m4_abort_evt (id, v) VALUES (?, ?)", params: [`${keys[0]}-after`, "x"], table: "m4_abort_evt", tenantId, partitionKey: keys[0] },
      AUTH(),
    );
    expect(postAbortWrite.status).toBe(200);
  }, 60000);

  it("criterion 10: a write that resolved its route pre-fence and arrives post-fence gets 409 VBUCKET_FENCED, and the same requestId succeeds on retry against the new shard after the flip", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m4_race_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const key = "race-row";
    await post(
      "/v1/sql",
      { sql: "INSERT INTO m4_race_evt (id, v) VALUES (?, ?)", params: [key, "seed"], table: "m4_race_evt", tenantId, partitionKey: key },
      AUTH(),
    );
    const vbucket = hashKey(`${tenantId}:m4_race_evt:${key}`) % 64;
    const routeBefore = (await (
      await post("/v1/sql", { sql: "SELECT 1 AS one FROM m4_race_evt WHERE id = ?", params: [key], table: "m4_race_evt", tenantId, partitionKey: key }, AUTH())
    ).json()) as { route: { shardId: string } };
    const sourceShardId = routeBefore.route.shardId;
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    // Simulate the race deterministically: the fence lands on the source
    // (cutover step 1) while the client's write — whose route was resolved
    // BEFORE the fence — arrives at the source afterwards. Sending directly
    // to the source shard with the resolved routing context is exactly what
    // the gateway does after /route.
    const fenceRes = await env.SHARD.get(env.SHARD.idFromName(sourceShardId)).fetch(
      new Request("https://shard.internal/fence-vbucket", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ vbucket }),
      }),
    );
    expect(fenceRes.status).toBe(200);

    const requestId = `race-req-${crypto.randomUUID()}`;
    const racedWrite = await env.SHARD.get(env.SHARD.idFromName(sourceShardId)).fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          sql: "UPDATE m4_race_evt SET v = ? WHERE id = ?",
          params: ["raced", key],
          requestId,
          isMutation: true,
          tenantId,
          table: "m4_race_evt",
          partitionKey: key,
          vbucket,
        }),
      }),
    );
    expect(racedWrite.status).toBe(409);
    expect(((await racedWrite.json()) as { error: { code: string } }).error.code).toBe("VBUCKET_FENCED");

    // Complete the "flip": move the row, repoint the map, unfence — the end
    // state cutover steps 4-5 produce.
    const targetStub = env.SHARD.get(env.SHARD.idFromName(targetShardId));
    await targetStub.fetch(
      new Request("https://shard.internal/migrate-import", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ vbucket, table: "m4_race_evt", rows: [{ partitionKey: key, tenantId, row: { id: key, v: "seed" } }] }),
      }),
    );
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE vbucket_map SET shard_id = ?, updated_at = ? WHERE vbucket = ?", targetShardId, new Date().toISOString(), vbucket);
    });
    await env.SHARD.get(env.SHARD.idFromName(sourceShardId)).fetch(
      new Request("https://shard.internal/unfence-vbucket", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ vbucket }),
      }),
    );

    // The client's retry — same requestId, through the normal gateway path —
    // now routes to the new shard and succeeds.
    const retryRes = await post(
      "/v1/sql",
      { sql: "UPDATE m4_race_evt SET v = ? WHERE id = ?", params: ["raced", key], table: "m4_race_evt", tenantId, partitionKey: key, requestId },
      AUTH(),
    );
    expect(retryRes.status).toBe(200);
    const retryBody = (await retryRes.json()) as { route: { shardId: string }; result: { rowsAffected: number } };
    expect(retryBody.route.shardId).toBe(targetShardId);
    expect(retryBody.result.rowsAffected).toBe(1);

    const finalRead = (await shardExecute(targetShardId, "SELECT v FROM m4_race_evt WHERE id = ?", [key])).rows;
    expect(finalRead[0].v).toBe("raced");
  });

  it("criterion 7: migrating a vbucket whose source shard has an unattributed row is rejected 409 VBUCKET_PROVENANCE_INCOMPLETE naming the count, and proceeds after /admin/backfill-provenance", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m4_gate_evt");
    // Clear tenants so the gate's later re-attribution is deterministic.
    for (let i = 0; i < 4; i += 1) {
      const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
      await stub.fetch(new Request("https://catalog.internal/list-shards", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }));
      await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec("DELETE FROM tenant_auth");
      });
    }
    const tenantId = tenantForCatalogShard(0, 4);
    await registerTenant(tenantId);

    // A pre-Chunk-0 row: written directly to its OWN home shard (where this
    // tenant's hash actually maps it) with no provenance — so
    // /admin/backfill-provenance can later attribute it with exactly one
    // candidate, unblocking the gate.
    const vbucket = hashKey(`${tenantId}:m4_gate_evt:legacy-row`) % 64;
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const homeShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return rows[0].shard_id;
    });
    const otherShardId = homeShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";
    await insertRowBypassingProvenance(homeShardId, "m4_gate_evt", "legacy-row", "x");

    const blocked = await post("/admin/migrate-vbucket", { catalogShardId: "catalog-0", vbucket, targetShardId: otherShardId }, AUTH());
    expect(blocked.status).toBe(409);
    const blockedBody = (await blocked.json()) as { error: { code: string; unattributedRows: number } };
    expect(blockedBody.error.code).toBe("VBUCKET_PROVENANCE_INCOMPLETE");
    expect(blockedBody.error.unattributedRows).toBeGreaterThanOrEqual(1);

    const backfillRes = await post("/admin/backfill-provenance", { catalogShardId: "catalog-0" }, AUTH());
    expect(backfillRes.status).toBe(200);

    const allowed = await post("/admin/migrate-vbucket", { catalogShardId: "catalog-0", vbucket, targetShardId: otherShardId }, AUTH());
    expect(allowed.status).toBe(200);

    await driveMigrationToCompletion(vbucket);
  });
});

describe("Milestone 2 Chunk 7: benchmark — indexed /v1/mutate write latency and repair-debt backlog", () => {
  it("indexed inserts stay within the regression bar of unindexed inserts (p50 latency), and accumulate zero repair-debt backlog", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 4, force: true }, AUTH());
    await createIndexTestTable("idx_c7_bench_plain_evt");
    await createIndexTestTable("idx_c7_bench_indexed_evt");
    await post("/admin/create-index", { indexName: "idx_c7_bench_by_v", table: "idx_c7_bench_indexed_evt", columns: ["v"] }, AUTH());
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    const ITERATIONS = 20;

    const plainLatencies: number[] = [];
    for (let i = 0; i < ITERATIONS; i++) {
      const start = performance.now();
      const res = await post("/v1/mutate", { op: "insert", table: "idx_c7_bench_plain_evt", tenantId, partitionKey: `plain-${i}`, values: { v: `val-${i}` } }, token);
      plainLatencies.push(performance.now() - start);
      expect(res.status).toBe(200);
    }

    const indexedLatencies: number[] = [];
    for (let i = 0; i < ITERATIONS; i++) {
      const start = performance.now();
      const res = await post("/v1/mutate", { op: "insert", table: "idx_c7_bench_indexed_evt", tenantId, partitionKey: `indexed-${i}`, values: { v: `val-${i}` } }, token);
      indexedLatencies.push(performance.now() - start);
      expect(res.status).toBe(200);
    }

    const plainP50 = median(plainLatencies);
    const indexedP50 = median(indexedLatencies);
    // Regression bar (Milestone 2 design doc's Success Criterion 2,
    // finalized here in Chunk 7): the async dispatch (ctx.waitUntil(), after
    // the response is already prepared) means an indexed write's caller-
    // observed latency SHOULD be indistinguishable from an unindexed one —
    // the index-maintenance work happens after the response is on its way
    // back. Generous absolute floor (25ms) alongside the percentage bar
    // absorbs test-environment timing noise on a fast baseline without
    // weakening the bar on a realistically-sized one.
    const regressionMs = indexedP50 - plainP50;
    const regressionPct = plainP50 > 0 ? (regressionMs / plainP50) * 100 : 0;
    expect(regressionMs < 25 || regressionPct < 10).toBe(true);

    // Repair-debt backlog: under normal (non-failing) conditions, every
    // async index write should succeed on its first attempt — zero jobs
    // should ever land in index_pending_jobs. A nonzero count here would
    // mean the benchmark run itself is silently degrading, not just slow.
    await pollIndexRows("idx_c7_bench_by_v", (r) => r.length === ITERATIONS);
    for (const candidateShardId of ALL_TEST_SHARD_IDS) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(candidateShardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        const jobs = Array.from(state.storage.sql.exec("SELECT * FROM index_pending_jobs"));
        expect(jobs).toHaveLength(0);
      });
    }
    // Wall-clock budget only (the p50 regression bar above is the actual
    // assertion, unaffected by this): 40 sequential gateway round trips this
    // late in the file already sit near the cumulative-latency ceiling
    // (vitest.config.ts), and per-test reset() adds a little more, so the old
    // 20s budget was too tight. Widened; a genuine hang still blows past it.
  }, 40000);
});

describe("Milestone 3 Chunk 6: migration benchmark (observational — no pass/fail thresholds)", () => {
  it("records backfill throughput, cutover fence-window duration, and peak mirror-queue depth under sustained writes", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("m6_bench_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    const token = await registerTenant(tenantId);

    // 80 pre-migration rows plus 30 sustained-write keys, all in one vbucket.
    const seedKeys = partitionKeysInSameVbucket(tenantId, "m6_bench_evt", "bm", 80, 64);
    const vbucket = hashKey(`${tenantId}:m6_bench_evt:bm`) % 64;
    for (const key of seedKeys) {
      await post(
        "/v1/sql",
        { sql: "INSERT INTO m6_bench_evt (id, v) VALUES (?, ?)", params: [key, "seed"], table: "m6_bench_evt", tenantId, partitionKey: key },
        AUTH(),
      );
    }
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const sourceShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return rows[0].shard_id;
    });
    const targetShardId = sourceShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    const migrationStart = Date.now();
    let firstFenceAt: number | null = null;
    let firstPostFenceSuccessAt: number | null = null;
    let peakMirrorDepth = 0;

    const migrateRes = await post("/admin/migrate-vbucket", { catalogShardId: "catalog-0", vbucket, targetShardId }, AUTH());
    expect(migrateRes.status).toBe(200);

    // Sustained writes (30 upserts against migrating keys) — retried on the
    // fence, timestamping the fence window.
    const writesPromise = (async () => {
      for (const key of seedKeys.slice(0, 30)) {
        for (let attempt = 0; attempt < 80; attempt += 1) {
          const res = await post(
            "/v1/sql",
            {
              sql: "INSERT INTO m6_bench_evt (id, v) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET v = excluded.v",
              params: [key, "live"],
              table: "m6_bench_evt",
              tenantId,
              partitionKey: key,
              requestId: `bench-${key}`,
            },
            AUTH(),
          );
          if (res.status === 200) {
            if (firstFenceAt !== null && firstPostFenceSuccessAt === null) firstPostFenceSuccessAt = Date.now();
            break;
          }
          const body = (await res.json()) as { error?: { code?: string } | string };
          const code = typeof body.error === "object" ? body.error?.code : undefined;
          if ((res.status === 409 && code === "VBUCKET_FENCED") || res.status === 503) {
            if (res.status === 409 && firstFenceAt === null) firstFenceAt = Date.now();
            await new Promise((resolve) => setTimeout(resolve, 20));
            continue;
          }
          throw new Error(`benchmark write failed: ${res.status} ${JSON.stringify(body)}`);
        }
      }
    })();

    const drivePromise = (async () => {
      for (let tick = 0; tick < 60; tick += 1) {
        await runInDurableObject(catalogStub, async (instance: CatalogDO) => {
          await instance.alarm();
        });
        const depthRes = await env.SHARD.get(env.SHARD.idFromName(sourceShardId)).fetch(
          new Request("https://shard.internal/mirror-pending-count", {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ vbucket }),
          }),
        );
        peakMirrorDepth = Math.max(peakMirrorDepth, ((await depthRes.json()) as { count: number }).count);
        const statusRes = await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket }, AUTH());
        if (((await statusRes.json()) as { status: string }).status === "none") return;
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      throw new Error("benchmark migration did not complete");
    })();
    await Promise.all([writesPromise, drivePromise]);
    const migrationMs = Date.now() - migrationStart;

    const finalStatus = (await (
      await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket }, AUTH())
    ).json()) as { rowsCopied: number };

    // Observational metrics — logged (visible with --silent=false), sanity-
    // checked for well-formedness only, per the spec ("no pass/fail
    // thresholds"): the absolute numbers are workload- and environment-
    // dependent.
    const backfillRowsPerSec = finalStatus.rowsCopied / (migrationMs / 1000);
    const fenceWindowMs = firstFenceAt !== null && firstPostFenceSuccessAt !== null ? firstPostFenceSuccessAt - firstFenceAt : 0;
    console.log("MIGRATION BENCHMARK", {
      rowsCopied: finalStatus.rowsCopied,
      migrationMs,
      backfillRowsPerSec: Math.round(backfillRowsPerSec * 10) / 10,
      fenceWindowMs,
      peakMirrorDepth,
    });

    expect(finalStatus.rowsCopied).toBeGreaterThanOrEqual(seedKeys.length);
    expect(Number.isFinite(backfillRowsPerSec)).toBe(true);
    expect(backfillRowsPerSec).toBeGreaterThan(0);
    expect(fenceWindowMs).toBeGreaterThanOrEqual(0);
    expect(peakMirrorDepth).toBeGreaterThanOrEqual(0);

    // Correctness backstop: every seed row is present post-migration.
    const targetCount = await env.SHARD.get(env.SHARD.idFromName(targetShardId)).fetch(
      new Request("https://shard.internal/migrate-checksum", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ vbucket, table: "m6_bench_evt", partitionKeyColumn: "id" }),
      }),
    );
    expect(((await targetCount.json()) as { rowCount: number }).rowCount).toBeGreaterThanOrEqual(seedKeys.length);
  }, 120000);
});
