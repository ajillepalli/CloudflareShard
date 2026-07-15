import { SELF, env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { hashKey, indexShardIdForKey } from "./hash";
import { sha256Hex } from "./auth";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";
import { AUTH, createIndexTestTable, initCluster, insertRowBypassingProvenance, post, registerTenant, rowOwnerEntries, tenantForCatalogShard } from "./index.test-helpers";

// This file is one of several index.*.test.ts files split out of a single
// index.test.ts (see index.test-helpers.ts's header comment for why). DO
// storage persists across `it` blocks within a file, so afterEach(reset())
// gives every test clean storage — the same isolation the pre-split file used.
afterEach(async () => {
  await reset();
});

describe("Worker /admin/backfill-provenance and /admin/set-row-owner (Milestone 3, Chunk 1)", () => {
  // tenant_auth isn't cleared by /admin/init's force:true (same reason
  // index_rules isn't — see the drain-shard describe block's beforeEach
  // above), so every tenantId ever registered on catalog-0 earlier in this
  // file's run persists into these tests. Left uncleared, "exactly one
  // candidate" tests become nondeterministic: with numShards: 1 every
  // vbucket (and therefore every registered tenant, regardless of its own
  // hash) maps to the single shard, so any tenant registered by an earlier,
  // unrelated test would also count as a "candidate" here. Clearing
  // tenant_auth on every catalog shard before each test isolates this
  // block's re-attribution scenarios from that history.
  beforeEach(async () => {
    for (let i = 0; i < 4; i += 1) {
      const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
      // ensureSchema() only runs on fetch(), not on raw runInDurableObject
      // storage access (same reason shard.test.ts's seedCoordinatorDecision
      // warms CoordinatorDO first) — trigger it with a harmless request
      // before touching tenant_auth directly, in case this catalog shard
      // hasn't been initialized yet in this test run.
      await stub.fetch(
        new Request("https://catalog.internal/list-shards", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }),
      );
      await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec("DELETE FROM tenant_auth");
      });
    }
  });

  it("attributes a row with exactly one candidate tenant", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("bp_unique_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    await registerTenant(tenantId);
    await insertRowBypassingProvenance("catalog-0-shard-0", "bp_unique_evt", "row-a", "x");

    const res = await post("/admin/backfill-provenance", { catalogShardId: "catalog-0" }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { attributed: number; ambiguous: unknown[]; orphaned: unknown[] };
    expect(body.attributed).toBeGreaterThanOrEqual(1);
    expect(body.ambiguous).toHaveLength(0);
    expect(body.orphaned).toHaveLength(0);

    const owners = await rowOwnerEntries("catalog-0-shard-0", "bp_unique_evt", "row-a");
    expect(owners).toHaveLength(1);
    expect(owners[0].tenant_id).toBe(tenantId);
  });

  it("reports an unattributed row orphaned when no registered tenant's hash lands on this shard", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("bp_orphan_evt");
    // Deliberately no tenants registered — the candidate list is empty by
    // construction, so this row can't have a unique (or ambiguous) match.
    await insertRowBypassingProvenance("catalog-0-shard-0", "bp_orphan_evt", "row-b", "x");

    const res = await post("/admin/backfill-provenance", { catalogShardId: "catalog-0" }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { orphaned: Array<{ table: string; partitionKey: string }> };
    expect(body.orphaned.some((o) => o.table === "bp_orphan_evt" && o.partitionKey === "row-b")).toBe(true);

    expect(await rowOwnerEntries("catalog-0-shard-0", "bp_orphan_evt", "row-b")).toHaveLength(0);
  });

  it("reports an unattributed row ambiguous with every candidate tenant listed, when multiple registered tenants all hash to this shard; /admin/set-row-owner resolves it", async () => {
    // numShards: 1 means every vbucket (and therefore every tenant's hash)
    // maps to the single shard — so any two registered tenants are both
    // trivially candidates for the same unattributed row.
    await post("/admin/init", { numShards: 1, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("bp_ambig_evt");
    // Both candidate tenants must be registered on catalog-0 specifically —
    // /admin/register-tenant routes each tenantId to whichever catalog shard
    // its own hash lands on, which is unrelated to this test's vbucket_map
    // scenario, so pick two tenantIds that both happen to land there.
    const tenantA = tenantForCatalogShard(0, 4);
    let tenantB = tenantA;
    for (let i = 0; ; i += 1) {
      const candidate = `bp-ambig-b-${i}`;
      if (hashKey(candidate) % 4 === 0) {
        tenantB = candidate;
        break;
      }
    }
    await registerTenant(tenantA);
    await registerTenant(tenantB);
    await insertRowBypassingProvenance("catalog-0-shard-0", "bp_ambig_evt", "row-c", "x");

    const res = await post("/admin/backfill-provenance", { catalogShardId: "catalog-0" }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      ambiguous: Array<{ shardId: string; table: string; partitionKey: string; candidateTenants: string[] }>;
    };
    const match = body.ambiguous.find((a) => a.table === "bp_ambig_evt" && a.partitionKey === "row-c");
    expect(match).toBeDefined();
    expect(match?.candidateTenants.sort()).toEqual([tenantA, tenantB].sort());
    expect(await rowOwnerEntries("catalog-0-shard-0", "bp_ambig_evt", "row-c")).toHaveLength(0);

    // Resolve it manually.
    const setRes = await post(
      "/admin/set-row-owner",
      { catalogShardId: "catalog-0", shardId: match!.shardId, table: "bp_ambig_evt", partitionKey: "row-c", tenantId: tenantA },
      AUTH(),
    );
    expect(setRes.status).toBe(200);
    const setBody = (await setRes.json()) as { ok: boolean };
    expect(setBody.ok).toBe(true);

    const owners = await rowOwnerEntries("catalog-0-shard-0", "bp_ambig_evt", "row-c");
    expect(owners).toHaveLength(1);
    expect(owners[0].tenant_id).toBe(tenantA);

    // Re-running backfill-provenance is idempotent: the row is no longer
    // reported at all (it's now attributed).
    const rerun = await post("/admin/backfill-provenance", { catalogShardId: "catalog-0" }, AUTH());
    const rerunBody = (await rerun.json()) as { ambiguous: Array<{ table: string; partitionKey: string }> };
    expect(rerunBody.ambiguous.some((a) => a.table === "bp_ambig_evt" && a.partitionKey === "row-c")).toBe(false);
  });

  it("/admin/set-row-owner rejects 409 ROW_OWNER_SHARD_MISMATCH when the claimed tenant's hash doesn't land on the claimed shard", async () => {
    await post("/admin/init", { numShards: 2, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("bp_mismatch_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    await registerTenant(tenantId);

    const vbucket = hashKey(`${tenantId}:bp_mismatch_evt:row-d`) % 64;
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const correctShardId = await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const row = Array.from(state.storage.sql.exec("SELECT shard_id FROM vbucket_map WHERE vbucket = ?", vbucket)) as Array<{ shard_id: string }>;
      return row[0].shard_id;
    });
    const wrongShardId = correctShardId === "catalog-0-shard-0" ? "catalog-0-shard-1" : "catalog-0-shard-0";

    const res = await post(
      "/admin/set-row-owner",
      { catalogShardId: "catalog-0", shardId: wrongShardId, table: "bp_mismatch_evt", partitionKey: "row-d", tenantId },
      AUTH(),
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("ROW_OWNER_SHARD_MISMATCH");
  });

  it("/admin/backfill-provenance is idempotent and re-runnable — a second run against already-attributed rows finds nothing left to do", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("bp_idempotent_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    await registerTenant(tenantId);
    await insertRowBypassingProvenance("catalog-0-shard-0", "bp_idempotent_evt", "row-e", "x");

    const first = await post("/admin/backfill-provenance", { catalogShardId: "catalog-0" }, AUTH());
    const firstBody = (await first.json()) as { attributed: number };
    expect(firstBody.attributed).toBeGreaterThanOrEqual(1);

    const second = await post("/admin/backfill-provenance", { catalogShardId: "catalog-0" }, AUTH());
    const secondBody = (await second.json()) as { attributed: number; ambiguous: unknown[]; orphaned: unknown[] };
    // Nothing new left unattributed for this table/row on this shard.
    expect(secondBody.ambiguous.some((a) => (a as { partitionKey: string }).partitionKey === "row-e")).toBe(false);
    expect(secondBody.orphaned.some((o) => (o as { partitionKey: string }).partitionKey === "row-e")).toBe(false);
  });

  it("/admin/backfill-provenance without catalogShardId runs against every catalog shard", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("bp_allshards_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    await registerTenant(tenantId);
    await insertRowBypassingProvenance("catalog-0-shard-0", "bp_allshards_evt", "row-f", "x");

    const res = await post("/admin/backfill-provenance", {}, AUTH());
    expect(res.status).toBe(200);
    const owners = await rowOwnerEntries("catalog-0-shard-0", "bp_allshards_evt", "row-f");
    expect(owners).toHaveLength(1);
  });

  // Codex full-PR review P1 (drain deadlock): /admin/drain-shard marks a shard
  // 'draining' BEFORE its vbuckets migrate, then — if that shard holds
  // unattributed rows — stalls VBUCKET_PROVENANCE_INCOMPLETE and tells the
  // operator to run /admin/backfill-provenance. But backfill-provenance
  // enumerated shards via /list-shards (active-only), which excludes the very
  // draining shard whose rows are blocking the drain: the rows were never
  // scanned, provenance never written, and the drain could never resume — a
  // deadlock. The fix enumerates active + draining, so the draining source is
  // scanned and its rows attributed.
  it("scans a DRAINING shard's unattributed rows (drain-aware) so the drain no longer deadlocks on VBUCKET_PROVENANCE_INCOMPLETE", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("bp_draining_evt");
    const tenantId = tenantForCatalogShard(0, 4);
    await registerTenant(tenantId);
    // An unattributed row on the shard we're about to drain.
    await insertRowBypassingProvenance("catalog-0-shard-0", "bp_draining_evt", "row-drn", "x");

    // Mark the shard 'draining' directly — exactly the state /admin/drain-shard
    // leaves it in before it stalls on the provenance gate.
    const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    await runInDurableObject(catalogStub, async (_i: CatalogDO, state: DurableObjectState) => {
      state.storage.sql.exec("UPDATE shards SET status = 'draining' WHERE shard_id = ?", "catalog-0-shard-0");
    });

    const res = await post("/admin/backfill-provenance", { catalogShardId: "catalog-0" }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { attributed: number };
    expect(body.attributed).toBeGreaterThanOrEqual(1); // the DRAINING shard was scanned

    // The blocking row now has provenance — the drain's gate can pass.
    const owners = await rowOwnerEntries("catalog-0-shard-0", "bp_draining_evt", "row-drn");
    expect(owners).toHaveLength(1);
    expect(owners[0].tenant_id).toBe(tenantId);
  });

  // PR review round 8 (P2): the scan loop used to infer "table not
  // physically present on this shard" from the scan query ITSELF failing
  // (!pageRes.ok), which silently treated ANY other genuine SQL error (a
  // malformed query, a missing/renamed partition-key column, etc.)
  // identically to that one legitimate case — the table stayed eligible for
  // provenance_complete certification even though this shard's rows for it
  // were never actually checked. The fix probes existence via PRAGMA
  // table_info first (mirroring f585f9b's /tenant-scan-page fix in
  // shard.ts), so a genuine failure against a table that DOES physically
  // exist now fails the whole /admin/backfill-provenance request instead.
  it("fails the whole /admin/backfill-provenance request on a genuine shard-side SQL error, instead of silently certifying the table provenance-complete", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 64, force: true }, AUTH());
    await createIndexTestTable("bp_genuineerr_evt");

    // Simulate a legacy table that predates completeness tracking (same
    // premise as index.table-scan.test.ts's criterion-4 test), then corrupt
    // table_rules' partition_key_column to point at a column that doesn't
    // exist on the PHYSICALLY-PRESENT table (e.g. renamed/dropped out from
    // under it) — replicated to every catalog shard, same as table_rules
    // itself is normally fanned out.
    for (let i = 0; i < 4; i += 1) {
      const stub = env.CATALOG.get(env.CATALOG.idFromName(`catalog-${i}`));
      await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
        state.storage.sql.exec(
          "UPDATE table_rules SET provenance_complete = 0, partition_key_column = ? WHERE table_name = ?",
          "ghost_col",
          "bp_genuineerr_evt",
        );
      });
    }

    const res = await post("/admin/backfill-provenance", {}, AUTH());
    // A genuine SQL error (no such column "ghost_col" on a table that
    // definitely exists) must fail the whole request — never silently
    // skipped as "table absent" the way the legitimate case below is.
    expect(res.status).not.toBe(200);

    // Critically: the table must NOT have been silently certified
    // provenance-complete off the back of an unscanned/errored shard.
    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const provenanceComplete = await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT provenance_complete FROM table_rules WHERE table_name = ?", "bp_genuineerr_evt"),
      ) as Array<{ provenance_complete: number }>;
      return rows[0]?.provenance_complete;
    });
    expect(provenanceComplete).toBe(0);
  });

  // Preserves the ORIGINAL legitimate case the old (too-broad) `!pageRes.ok`
  // check existed for: a table_rules entry with no physical table ever
  // created on this shard (e.g. /admin/register-table registers metadata
  // only — mirrors index.core.test.ts's column_mismatch_evt regression
  // scenario). This must still be silently skipped, not treated as a
  // failure, and — since nothing else is wrong with it — the table still
  // gets certified provenance_complete exactly as before this fix.
  it("still silently skips a registered-but-never-physically-created table (existing legitimate case, no regression)", async () => {
    await post("/admin/init", { numShards: 1, totalVBuckets: 64, force: true }, AUTH());
    const table = "bp_neverphysical_evt";
    const registerRes = await post("/admin/register-table", { table, partitionKeyColumn: "id" }, AUTH());
    expect(registerRes.status).toBe(200);

    const res = await post("/admin/backfill-provenance", {}, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { orphaned: Array<{ table: string }>; ambiguous: Array<{ table: string }> };
    expect(body.orphaned.some((o) => o.table === table)).toBe(false);
    expect(body.ambiguous.some((a) => a.table === table)).toBe(false);

    // A genuinely full-cluster run with nothing wrong recorded for this
    // table still certifies it complete, exactly as before this fix.
    const stub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
    const provenanceComplete = await runInDurableObject(stub, async (_instance: CatalogDO, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT provenance_complete FROM table_rules WHERE table_name = ?", table),
      ) as Array<{ provenance_complete: number }>;
      return rows[0]?.provenance_complete;
    });
    expect(provenanceComplete).toBe(1);
  });
});
