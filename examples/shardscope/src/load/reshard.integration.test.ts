/// <reference types="@cloudflare/vitest-pool-workers/types" />
/// <reference path="../../../../src/env.d.ts" />
/** reshard.integration.test.ts — the in-process END-TO-END proof that
 * CloudflareShard reshards under load with zero lost writes, verified by
 * Shardscope's own correctness tracker (./correctness.ts).
 *
 * WHY this file exists: every other test in this package (correctness.test.ts,
 * skew.test.ts if it existed, load-driver.test.ts, ...) exercises the
 * demo's LOGIC against fabricated data — a fake TxExecutor, a hand-built
 * vbucket map. None of them ever drive a real CatalogDO/ShardDO cluster.
 * This file does: it stands up a real (small) cluster through
 * CloudflareShard's own in-process gateway (@cloudflare/vitest-pool-workers'
 * Miniflare runtime, via `cloudflare:test`'s SELF.fetch — the exact harness
 * src/index.migration.test.ts uses for the core product's own migration
 * suite), heats a real vbucket using Shardscope's actual hot-shard skew
 * driver (./skew.ts), drives a REAL /admin/migrate-vbucket to completion
 * with writes spanning the cutover, and then asks Shardscope's own
 * CorrectnessTracker (./correctness.ts) to read every tracked write back
 * through the real gateway. If the demo's central claim ("reshard under
 * load, lost: 0") is false, THIS is the test that would catch it — not a
 * unit test feeding the tracker a fake ReadBackFn.
 *
 * HARNESS: reused, not invented. vitest.config.ts wires
 * @cloudflare/vitest-pool-workers against the repo's real ./wrangler.toml
 * for the WHOLE project (there is only one vitest config, no
 * poolMatchGlobs), so a test under examples/shardscope/src/load/ gets the
 * identical Miniflare-backed `cloudflare:test` environment src/*.test.ts
 * gets — confirmed by this file actually running (see the report). The only
 * addition needed to make TypeScript resolve `cloudflare:test`'s ambient
 * types from outside src/ (examples/shardscope has its own tsconfig.json,
 * which doesn't list "@cloudflare/vitest-pool-workers/types") is the
 * triple-slash reference at the top of this file — a one-line, file-local
 * fix, no tsconfig or vitest.config.ts edit required.
 *
 * Every gateway call below goes through SELF.fetch via
 * ../../../../src/index.test-helpers's `post()` helper — the exact helper
 * src/index.migration.test.ts itself uses — never a mock, never
 * runInDurableObject to poke internal state (except where the established
 * migration-test pattern itself does, e.g. driving CatalogDO's alarm via
 * driveMigrationToCompletion).
 */
import { env, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import { hashKey } from "../../../../src/hash";
import type { CatalogDO } from "../../../../src/catalog";
import {
  AUTH,
  createIndexTestTable,
  driveIndexBackfillToCompletion,
  driveMigrationToCompletion,
  post,
  registerTenant,
  shardExecute,
  tenantForCatalogShard,
} from "../../../../src/index.test-helpers";
import { generateSkewedKeys, type SkewedKey, type VBucketOwnership } from "./skew";
import { CorrectnessTracker, TrackingTxExecutor, gatewayReadBack, type ReadBackFn, type ReadBackResult, type SqlPointReader } from "./correctness";
import { stockKey, tenantIdForWarehouse, type MutateCall, type MutateResult, type QueryResult, type TxExecutor } from "./transactions";

// DO storage persists across `it` blocks within one vitest test FILE (the
// same reason every src/index.*.test.ts file resets) — several tests below
// register a table named "tpcc_stock", so without this every test after the
// first would either 409 re-registering it or (worse) silently reuse a
// physical table still holding the previous test's rows.
afterEach(async () => {
  await reset();
});

// ----------------------------------------------------------------------------
// tpcc_stock schema — byte-identical to examples/tpc-c-benchmark/src/schema.mjs's
// TABLES entry for "tpcc_stock" and INDEXES entry for "idx_stock_by_item". Not
// imported directly: schema.mjs is a plain .mjs module and this repo has no
// `allowJs`/`checkJs` anywhere (verified before writing this file), so a
// direct import would need a tsconfig change this test doesn't otherwise
// need. Kept inline instead, with this comment as the tripwire — if
// schema.mjs's tpcc_stock definition ever changes, this constant must change
// with it, or ./correctness.ts's trackableWriteFromStockUpdate (which parses
// the exact "s-WWWW-IIIIII" partition-key format ./transactions.ts's
// stockKey() produces) silently stops matching what's actually on the wire.
// ----------------------------------------------------------------------------
const TPCC_STOCK_SCHEMA =
  "CREATE TABLE tpcc_stock (s_key TEXT PRIMARY KEY, w_id INTEGER, i_id INTEGER, s_quantity INTEGER, s_ytd INTEGER, s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data TEXT)";
const STOCK_INDEX_NAME = "idx_stock_by_item";

/** Finds the smallest warehouse id whose ./transactions.ts tenantIdForWarehouse()
 * hashes onto the given catalog index — the tpcc-tenant-naming analog of
 * ../../../../src/index.test-helpers's own tenantForCatalogShard(). Needed
 * because that file's driveMigrationToCompletion() hardcodes "catalog-0" (both
 * the CatalogDO it ticks and the catalogShardId it polls
 * /admin/migrate-vbucket-status with) — every existing caller in
 * src/index.migration.test.ts deliberately uses tenantForCatalogShard(0, 4)
 * for the same reason. Picking an arbitrary warehouse id here would silently
 * drive the WRONG (idle) catalog's alarm while the real migration never
 * advances — exactly the bug this helper exists to avoid. */
function warehouseForCatalogShard(catalogIndex: number, catalogShardCount: number): number {
  for (let w = 1; ; w += 1) {
    if (hashKey(tenantIdForWarehouse(w)) % catalogShardCount === catalogIndex) return w;
  }
}

/** Which catalog shard governs a given tenant — the SAME formula
 * ./load-driver.ts's own (deliberately duplicated, per that file's doc
 * comment) catalogShardIdForTenant uses, which in turn mirrors src/index.ts's
 * private routing formula: `catalog-${hashKey(tenantId) % catalogShardCount}`.
 * Duplicated here (a third copy) for the same reason load-driver.ts's is a
 * second copy rather than an import: this is a separate deployable's test
 * file, not a shared library. Used below to build the REAL 3rd-arg resolver
 * TrackingTxExecutor now requires — see correctness.ts's header comment on
 * why a resolver that always returns null makes verify() check nothing and
 * report a vacuous lost:0. `catalogShardCount` is always taken from a LIVE
 * /admin/vbucket-map response (never guessed), so this can't drift from
 * whatever the test cluster was actually initialized with. */
function catalogShardIdForTenant(tenantId: string, catalogShardCount: number): string {
  return `catalog-${hashKey(tenantId) % catalogShardCount}`;
}

async function setUpStockCluster(numShards: number, totalVBuckets: number): Promise<void> {
  const initRes = await post("/admin/init", { numShards, totalVBuckets, force: true }, AUTH());
  expect(initRes.status).toBe(200);
  const createRes = await post(
    "/admin/create-table",
    { table: "tpcc_stock", schema: TPCC_STOCK_SCHEMA, partitionKeyColumn: "s_key" },
    AUTH(),
  );
  expect(createRes.status).toBe(200);
  const indexRes = await post(
    "/admin/create-index",
    { indexName: STOCK_INDEX_NAME, table: "tpcc_stock", columns: ["i_id"] },
    AUTH(),
  );
  expect(indexRes.status).toBe(200);
  // create-index's backfill is alarm-driven and holds the cluster-wide
  // topology lock (held on catalog-0) until it finishes — see
  // ../../../../src/index.test-helpers's driveIndexBackfillToCompletion and
  // main's PR #20 (synchronous backfill → alarm-driven background job). Drain
  // it to completion here so the /admin/migrate-vbucket reshard below can
  // acquire that same lock instead of racing the backfill and getting a 409
  // TOPOLOGY_OPERATION_IN_PROGRESS.
  await driveIndexBackfillToCompletion(STOCK_INDEX_NAME);
  // driveIndexBackfillToCompletion ticks CatalogDO.alarm() by hand but does
  // not consume the alarm /start-index-backfill already SCHEDULED — cancel it
  // so a stale wall-clock firing can't race the migration below into a
  // premature cutover. See clearCatalogAlarm's doc comment.
  await clearCatalogAlarm();
}

/** In-process analog of ./gateway-client.ts's HttpTxExecutor: the SAME
 * TxExecutor contract (./transactions.ts), the SAME four wire routes
 * (/v1/mutate, /v1/tx, /v1/index-query, /v1/table-scan), the SAME per-call
 * body shape — just swapping global `fetch(baseUrl + path)` for
 * ../../../../src/index.test-helpers's `post()` (SELF.fetch against the
 * in-process Worker), because there is no real HTTP base URL in this
 * Miniflare test harness and no TokenProvider/wrangler-dev round trip is
 * needed or wanted (see this file's header comment: no wrangler dev). This
 * is deliberately NOT a mock: every call is a real /v1/* request handled by
 * the real gateway (src/index.ts) against real CatalogDO/ShardDO instances. */
class InProcessTxExecutor implements TxExecutor {
  constructor(
    private readonly tenantId: string,
    /** Full "Bearer <token>" string, as returned by registerTenant(). */
    private readonly authorization: string,
  ) {}

  async mutate(_warehouseId: number, call: MutateCall): Promise<MutateResult> {
    return this.send("/v1/mutate", { ...call, tenantId: this.tenantId, requestId: call.requestId ?? crypto.randomUUID() }) as Promise<MutateResult>;
  }

  async tx(_warehouseId: number, mutations: MutateCall[], requestId?: string): Promise<{ committed?: boolean; [k: string]: unknown }> {
    const stamped = mutations.map((m) => ({ ...m, tenantId: this.tenantId }));
    return this.send("/v1/tx", { mutations: stamped, requestId: requestId ?? crypto.randomUUID() }) as Promise<{
      committed?: boolean;
      [k: string]: unknown;
    }>;
  }

  async indexQuery(_warehouseId: number, table: string, indexName: string, values: Record<string, unknown>, limit?: number): Promise<QueryResult> {
    return this.send("/v1/index-query", { table, indexName, tenantId: this.tenantId, values, limit }) as Promise<QueryResult>;
  }

  async tableScan(_warehouseId: number, table: string, limit: number, cursor?: unknown): Promise<QueryResult> {
    return this.send("/v1/table-scan", { tenantId: this.tenantId, table, limit, cursor }) as Promise<QueryResult>;
  }

  private async send(path: string, body: Record<string, unknown>): Promise<unknown> {
    const res = await post(path, body, this.authorization);
    const text = await res.text();
    const json = text ? JSON.parse(text) : {};
    if (!res.ok) {
      throw new Error(`POST ${path} -> ${res.status}: ${JSON.stringify(json)}`);
    }
    return json;
  }
}

/** In-process analog of ./gateway-client.ts's HttpSqlPointReader (design
 * round 3, point 2): the SAME admin-scoped /v1/sql primitive gatewayReadBack
 * now reads through for a PRECISE, primary-key point read-back — just
 * swapping global `fetch` for `post()` (SELF.fetch), same reasoning as
 * InProcessTxExecutor above. Uses AUTH() (the admin token), never a tenant
 * bearer token, matching /v1/sql's real auth requirement (see sqlCore's own
 * header comment on why the per-tenant SQL path was removed entirely). */
class InProcessSqlPointReader implements SqlPointReader {
  async sqlSelect(args: { table: string; tenantId: string; partitionKey: string; sql: string; params: unknown[] }): Promise<{ rows: Record<string, unknown>[] }> {
    const res = await post(
      "/v1/sql",
      { sql: args.sql, params: args.params, table: args.table, tenantId: args.tenantId, partitionKey: args.partitionKey },
      AUTH(),
    );
    const text = await res.text();
    const json = text ? JSON.parse(text) : {};
    if (!res.ok) {
      throw new Error(`POST /v1/sql -> ${res.status}: ${JSON.stringify(json)}`);
    }
    const rows = (json as { result?: { rows?: Record<string, unknown>[] } })?.result?.rows;
    return { rows: rows ?? [] };
  }
}

/** Wraps a ReadBackFn with the SAME bounded-retry idiom
 * ../../../../src/index.test-helpers's pollIndexRows/pollShardRows already
 * use elsewhere in this codebase: index-maintenance writes run in
 * ctx.waitUntil() AFTER the mutating response is sent (see
 * src/index.ts's mutateCore), so a read-back racing that background work
 * could otherwise report a false "lost" — exactly the false-RED failure mode
 * ./correctness.ts's own header comment warns against being as bad as a
 * false-GREEN. Retries until the row matches every field in `write.values`
 * (the same subset-match ./correctness.ts's internal matchesExpected
 * applies) or the budget is exhausted, at which point it returns whatever it
 * last saw and lets CorrectnessTracker.verify's own comparison make the
 * final, authoritative call. */
function pollingReadBack(inner: ReadBackFn): ReadBackFn {
  return async (write) => {
    let result: ReadBackResult = { found: false };
    for (let attempt = 0; attempt < 50; attempt += 1) {
      result = await inner(write);
      const row = result.row;
      const matches = !!row && Object.entries(write.values).every(([k, v]) => row[k] === v);
      if (matches) return result;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    return result;
  };
}

/** Cancels catalog-0's pending storage alarm so the alarm-driven migration
 * AND index-backfill orchestration (main's PR #20) advances ONLY under this
 * test's explicit manual ticks (driveIndexBackfillToCompletion /
 * driveMigrationToCompletion), never on Miniflare's wall clock.
 *
 * WHY this is required for a HONEST proof, not just a green test: both
 * /admin/create-index (its backfill) and /admin/migrate-vbucket SCHEDULE a
 * real alarm on catalog-0 — `state.storage.setAlarm(Date.now() + 250ms)`
 * (MIGRATION_TICK_MS in src/catalog.ts). The driveXToCompletion helpers tick
 * CatalogDO.alarm() by HAND but never CONSUME that already-scheduled alarm,
 * so once its fire time passes Miniflare fires it on its own, between the
 * test's own calls. Two distinct failures both trace to this:
 *   (1) create-index's leftover backfill alarm, or the migration's own tick
 *       alarm, fires mid-"during"-batch and carries the migration from
 *       'backfilling' into cutover — fencing the source — so a write 409s
 *       VBUCKET_FENCED (the original flake); and, more insidiously,
 *   (2) that same premature advance can push the migration THROUGH cutover
 *       before any "during" write lands, so the batch routes post-cutover and
 *       the proof passes while covering NOTHING of the mirrored-backfilling
 *       window it exists to exercise (Codex P2 — a hollow, not a wrong,
 *       proof).
 * Clearing the pending alarm right after each op that schedules one closes
 * both: the migration can then ONLY move when the test ticks it, so the
 * "during" batch is deterministically inside the true mirrored window every
 * run (locked in by the explicit `migrate-vbucket-status === backfilling`
 * assertion just before each "during" batch below). */
async function clearCatalogAlarm(): Promise<void> {
  const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
  await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
    await state.storage.deleteAlarm();
  });
}

// ----------------------------------------------------------------------------
// THE PROOF
// ----------------------------------------------------------------------------

describe("Shardscope live-cluster proof: reshard under load, lost 0 (real in-process gateway)", () => {
  it(
    "a real /admin/migrate-vbucket moves a hot vbucket's tpcc_stock rows to a new shard while writes span the cutover, and CorrectnessTracker's own gateway read-back reports zero loss",
    async () => {
      const totalVBuckets = 64;
      await setUpStockCluster(2, totalVBuckets);

      // Deliberately routed to catalog-0 (see warehouseForCatalogShard's doc
      // comment) so driveMigrationToCompletion below — which hardcodes
      // catalog-0, matching every existing caller in
      // src/index.migration.test.ts — actually drives THIS migration.
      const warehouseId = warehouseForCatalogShard(0, 4);
      const tenantId = tenantIdForWarehouse(warehouseId);
      const authorization = await registerTenant(tenantId);
      const exec = new InProcessTxExecutor(tenantId, authorization);

      // Resolve which catalog + physical shard this warehouse's tpcc_stock
      // rows currently route through (the real gateway's own /route
      // decision, echoed by /v1/sql — the same mechanism
      // src/index.migration.test.ts's own tests use to find sourceShardId).
      const routeProbe = await post(
        "/v1/sql",
        { sql: "SELECT 1 AS one", table: "tpcc_stock", tenantId, partitionKey: stockKey(warehouseId, 1) },
        AUTH(),
      );
      expect(routeProbe.status).toBe(200);
      const routeProbeBody = (await routeProbe.json()) as { route: { shardId: string } };
      const sourceShardId = routeProbeBody.route.shardId;
      const catalogShardId = sourceShardId.split("-shard-")[0];
      const targetShardId = sourceShardId.endsWith("-shard-0") ? `${catalogShardId}-shard-1` : `${catalogShardId}-shard-0`;

      // Pull this catalog's LIVE vbucket map and feed it straight into
      // Shardscope's real hot-shard skew driver (./skew.ts) — the exact
      // mechanism the demo itself uses to heat a shard — to find tpcc_stock
      // item ids whose partition key currently routes to the source shard.
      const vbMapRes = await post("/admin/vbucket-map", {}, AUTH());
      expect(vbMapRes.status).toBe(200);
      const vbMapBody = (await vbMapRes.json()) as {
        catalogShardCount: number;
        catalogs: Array<{ catalogShardId: string; map: Array<{ vbucket: number; shardId: string }> }>;
      };
      const catalogMap = vbMapBody.catalogs.find((c) => c.catalogShardId === catalogShardId)!.map;
      const ownership: VBucketOwnership[] = catalogMap.map((m) => ({ vbucket: m.vbucket, shardId: m.shardId }));

      // Sanity check: catalogShardIdForTenant's formula (fed the LIVE
      // catalogShardCount, never guessed) must agree with catalogShardId as
      // actually resolved above from the gateway's own /v1/sql route decision
      // (line ~sourceShardId.split). If these ever diverged, the resolver
      // below would silently resolve every candidate to the WRONG catalog id,
      // which pickTrackedCandidates would then (correctly) drop as
      // cross-catalog — this assertion is what would catch that class of bug
      // here instead of it silently degrading to a vacuous 0-tracked run.
      expect(catalogShardIdForTenant(tenantId, vbMapBody.catalogShardCount)).toBe(catalogShardId);

      const skewed: SkewedKey<number>[] = generateSkewedKeys({
        targetShardId: sourceShardId,
        vbucketMap: ownership,
        totalVBuckets,
        tenantId,
        table: "tpcc_stock",
        candidateToKey: (i) => ({ value: i + 1, partitionKey: stockKey(warehouseId, i + 1) }),
        count: 1500,
      });
      expect(skewed.length).toBeGreaterThan(0);

      // generateSkewedKeys matches ANY vbucket the source shard owns; group
      // by vbucket and take the single richest one so ONE /admin/migrate-vbucket
      // call moves every tracked write (this is deterministic — hashKey has
      // no randomness — so whichever vbucket wins here wins every run).
      const byVbucket = new Map<number, SkewedKey<number>[]>();
      for (const k of skewed) {
        const arr = byVbucket.get(k.vbucket) ?? [];
        arr.push(k);
        byVbucket.set(k.vbucket, arr);
      }
      let targetVbucket = -1;
      let hotKeys: SkewedKey<number>[] = [];
      for (const [vb, keys] of byVbucket) {
        if (keys.length > hotKeys.length) {
          targetVbucket = vb;
          hotKeys = keys;
        }
      }
      const WRITE_COUNT = 12;
      expect(hotKeys.length).toBeGreaterThanOrEqual(WRITE_COUNT);
      const items = hotKeys.slice(0, WRITE_COUNT);
      for (const item of items) {
        expect(item.vbucket).toBe(targetVbucket);
      }

      // --- Seed baseline stock rows. Not individually tracked: like the
      // real demo, ./correctness.ts's trackableWriteFromStockUpdate only
      // tracks UPDATEs (a stock row is inserted once by New-Order, then
      // updated repeatedly) — see that file's header comment. ---
      for (const item of items) {
        const seedRes = await post(
          "/v1/mutate",
          {
            op: "insert",
            table: "tpcc_stock",
            tenantId,
            partitionKey: item.partitionKey,
            values: { w_id: warehouseId, i_id: item.value, s_quantity: 100, s_ytd: 0, s_order_cnt: 0, s_remote_cnt: 0, s_data: "seed" },
            requestId: `seed-${item.partitionKey}-${crypto.randomUUID()}`,
          },
          authorization,
        );
        expect(seedRes.status).toBe(200);
      }

      const tracker = new CorrectnessTracker();
      // Real resolver, not a stub: resolves via the SAME formula/live
      // catalogShardCount the sanity check above just verified agrees with
      // this tenant's actual routing — never null in this flow (the vbucket
      // map is already fetched by this point), so every tracked candidate
      // resolves to a real catalogShardId. See correctness.ts's header
      // comment: a resolver that returns null would make trackedWrites()
      // stay empty and verify() check nothing — the vacuous-pass trap the
      // assertion right after writeBatch("pre", ...) below exists to catch.
      const resolveCatalogShardId = (tid: string): string | null => catalogShardIdForTenant(tid, vbMapBody.catalogShardCount);
      const tracked = new TrackingTxExecutor(exec, tracker, resolveCatalogShardId);

      async function writeBatch(phase: string, quantity: number): Promise<void> {
        for (const item of items) {
          const result = await tracked.mutate(warehouseId, {
            op: "update",
            table: "tpcc_stock",
            partitionKey: item.partitionKey,
            values: { s_quantity: quantity, s_ytd: quantity, s_order_cnt: quantity, s_remote_cnt: 0 },
            requestId: `${phase}-${item.partitionKey}-${crypto.randomUUID()}`,
          });
          expect(result.rowsAffected).toBe(1);
        }
      }

      // Phase A: writes BEFORE the reshard starts.
      await writeBatch("pre", 91);
      tracker.promoteToTracked(tracker.drainPendingCandidates());
      expect(tracker.trackedWrites()).toHaveLength(WRITE_COUNT);

      // THE RESHARD: a real /admin/migrate-vbucket. Immediately after this
      // call the vbucket is 'backfilling' with zero orchestration ticks run
      // yet — the same deterministic "during migration" window
      // src/index.migration.test.ts's dual-write-mirroring tests (Milestone
      // 3, Chunk 3) rely on, used here instead of racing real concurrency
      // (which the pool-workers single-threaded model can make flaky —
      // write-fencing around each cutover phase like this still genuinely
      // spans the migration lifecycle: writes land before, during
      // backfilling/mirroring, and after cutover).
      const migrateRes = await post("/admin/migrate-vbucket", { catalogShardId, vbucket: targetVbucket, targetShardId }, AUTH());
      expect(migrateRes.status).toBe(200);
      const migrateBody = (await migrateRes.json()) as { ok: boolean; status: string; fromShard: string; toShard: string };
      expect(migrateBody.status).toBe("backfilling");
      expect(migrateBody.fromShard).toBe(sourceShardId);
      expect(migrateBody.toShard).toBe(targetShardId);
      // Cancel the tick alarm /admin/migrate-vbucket just scheduled so the
      // migration advances ONLY when driveMigrationToCompletion ticks it
      // below — a stray wall-clock firing must not push it into cutover
      // before (or during) the "during" batch. See clearCatalogAlarm.
      await clearCatalogAlarm();

      // Phase B: writes DURING the backfill window — still authoritative on
      // the source, mirrored atomically to the target (Milestone 3, Chunk 3).
      // Lock in the coverage this batch claims: with the alarm cleared, the
      // migration is provably STILL pre-cutover here, so these writes really
      // do exercise the mirrored-backfilling window (not a post-cutover route
      // that would make the "during" label a lie — Codex P2).
      const duringStatus = await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket: targetVbucket }, AUTH());
      expect(((await duringStatus.json()) as { status: string }).status).toBe("backfilling");
      await writeBatch("during", 47);

      // Drive the REAL orchestration (CatalogDO alarm ticks) to completion —
      // the exact helper src/index.migration.test.ts's own suite uses,
      // polling /admin/migrate-vbucket-status rather than a fixed sleep, so
      // this is deterministic regardless of how many ticks backfill+cutover
      // actually take.
      await driveMigrationToCompletion(targetVbucket);

      // Phase C: writes AFTER cutover — now routed straight to the new shard.
      await writeBatch("post", 12);

      // --- THE PROOF: read every tracked key back through the REAL gateway
      // via a PRECISE primary-key point SELECT (/v1/sql, admin-scoped — see
      // InProcessSqlPointReader above and ./correctness.ts's SqlPointReader
      // doc comment), via CorrectnessTracker.verify(), Shardscope's own
      // loss-detection core. ---
      const readBack = pollingReadBack(gatewayReadBack(new InProcessSqlPointReader()));
      const verifyResult = await tracker.verify(readBack);
      const snapshot = tracker.snapshot();

      expect(verifyResult.checked).toBe(WRITE_COUNT);
      expect(verifyResult.lostThisPass).toBe(0);
      expect(snapshot.lost).toBe(0);
      expect(snapshot.meterState).toBe("green");
      // 3 batches (pre/during/post) x WRITE_COUNT keys, all fresh acks (no
      // replays in this flow).
      expect(snapshot.writesAcked).toBe(WRITE_COUNT * 3);
      expect(snapshot.writesRetriedIdempotent).toBe(0);

      // --- Independently confirm a REAL reshard happened: the vbucket's
      // shardId actually changed in /admin/vbucket-map, and its migration
      // state is fully cleared — this is what makes this test prove a real
      // reshard occurred, not just that writes happened to succeed. ---
      const finalMapRes = await post("/admin/vbucket-map", {}, AUTH());
      const finalMapBody = (await finalMapRes.json()) as {
        catalogs: Array<{
          catalogShardId: string;
          map: Array<{ vbucket: number; shardId: string; migrationStatus: string; targetShardId: string | null }>;
        }>;
      };
      const finalRow = finalMapBody.catalogs.find((c) => c.catalogShardId === catalogShardId)!.map.find((m) => m.vbucket === targetVbucket)!;
      expect(finalRow.shardId).toBe(targetShardId);
      expect(finalRow.migrationStatus).toBe("none");
      expect(finalRow.targetShardId).toBeNull();

      // And the OLD source shard genuinely no longer holds these rows
      // (cutover's step 5 deleted them) — queried directly against the
      // source shard's own storage, not through the (now target-routed)
      // gateway, so this can't be fooled by re-routing.
      const placeholders = items.map(() => "?").join(", ");
      const sourceLeftovers = (
        await shardExecute(sourceShardId, `SELECT s_key FROM tpcc_stock WHERE s_key IN (${placeholders})`, items.map((i) => i.partitionKey))
      ).rows;
      expect(sourceLeftovers).toHaveLength(0);
    },
    90000,
  );
});

// ----------------------------------------------------------------------------
// ROUND 5 ADDITION — Codex finding #4: notifyClusterChanged() must keep
// `verified` false for the WHOLE duration of a REAL reshard (not just a
// simulated epoch bump), returning true only once a fresh verify() pass runs
// over the post-cutover state. This drives the exact hook load-driver.ts's
// runTick wires up (see that file's hasActiveMigration/notifyClusterChanged
// call), just invoked directly here (no LoadDriver DO harness exists in this
// package) against a REAL /admin/migrate-vbucket cutover — the same
// distinction this file's header comment draws for the rest of its tests:
// this proves the OBSERVABLE claim ("not verified across a real reshard"),
// not just the pure epoch-counter mechanics correctness.test.ts already
// covers in isolation.
// ----------------------------------------------------------------------------

describe("Shardscope live-cluster proof: notifyClusterChanged() keeps `verified` false across a REAL reshard, true again only after a fresh post-cutover verify() pass", () => {
  it(
    "verified is true before the reshard, goes false the instant migration activity is observed, stays false while the migration is ongoing, and only returns to true after a clean verify() pass over the post-cutover state",
    async () => {
      const totalVBuckets = 64;
      await setUpStockCluster(2, totalVBuckets);

      const warehouseId = warehouseForCatalogShard(0, 4);
      const tenantId = tenantIdForWarehouse(warehouseId);
      const authorization = await registerTenant(tenantId);
      const exec = new InProcessTxExecutor(tenantId, authorization);

      const routeProbe = await post(
        "/v1/sql",
        { sql: "SELECT 1 AS one", table: "tpcc_stock", tenantId, partitionKey: stockKey(warehouseId, 1) },
        AUTH(),
      );
      expect(routeProbe.status).toBe(200);
      const routeProbeBody = (await routeProbe.json()) as { route: { shardId: string } };
      const sourceShardId = routeProbeBody.route.shardId;
      const catalogShardId = sourceShardId.split("-shard-")[0];
      const targetShardId = sourceShardId.endsWith("-shard-0") ? `${catalogShardId}-shard-1` : `${catalogShardId}-shard-0`;

      const vbMapRes = await post("/admin/vbucket-map", {}, AUTH());
      expect(vbMapRes.status).toBe(200);
      const vbMapBody = (await vbMapRes.json()) as {
        catalogShardCount: number;
        catalogs: Array<{ catalogShardId: string; map: Array<{ vbucket: number; shardId: string }> }>;
      };
      const catalogMap = vbMapBody.catalogs.find((c) => c.catalogShardId === catalogShardId)!.map;
      const ownership: VBucketOwnership[] = catalogMap.map((m) => ({ vbucket: m.vbucket, shardId: m.shardId }));
      expect(catalogShardIdForTenant(tenantId, vbMapBody.catalogShardCount)).toBe(catalogShardId);

      const skewed: SkewedKey<number>[] = generateSkewedKeys({
        targetShardId: sourceShardId,
        vbucketMap: ownership,
        totalVBuckets,
        tenantId,
        table: "tpcc_stock",
        candidateToKey: (i) => ({ value: i + 1, partitionKey: stockKey(warehouseId, i + 1) }),
        count: 1500,
      });
      expect(skewed.length).toBeGreaterThan(0);

      const byVbucket = new Map<number, SkewedKey<number>[]>();
      for (const k of skewed) {
        const arr = byVbucket.get(k.vbucket) ?? [];
        arr.push(k);
        byVbucket.set(k.vbucket, arr);
      }
      let targetVbucket = -1;
      let hotKeys: SkewedKey<number>[] = [];
      for (const [vb, keys] of byVbucket) {
        if (keys.length > hotKeys.length) {
          targetVbucket = vb;
          hotKeys = keys;
        }
      }
      const WRITE_COUNT = 5;
      expect(hotKeys.length).toBeGreaterThanOrEqual(WRITE_COUNT);
      const items = hotKeys.slice(0, WRITE_COUNT);

      for (const item of items) {
        const seedRes = await post(
          "/v1/mutate",
          {
            op: "insert",
            table: "tpcc_stock",
            tenantId,
            partitionKey: item.partitionKey,
            values: { w_id: warehouseId, i_id: item.value, s_quantity: 100, s_ytd: 0, s_order_cnt: 0, s_remote_cnt: 0, s_data: "seed" },
            requestId: `seed-verified-${item.partitionKey}-${crypto.randomUUID()}`,
          },
          authorization,
        );
        expect(seedRes.status).toBe(200);
      }

      const tracker = new CorrectnessTracker();
      const resolveCatalogShardId = (tid: string): string | null => catalogShardIdForTenant(tid, vbMapBody.catalogShardCount);
      const tracked = new TrackingTxExecutor(exec, tracker, resolveCatalogShardId);

      async function writeBatch(phase: string, quantity: number): Promise<void> {
        for (const item of items) {
          const result = await tracked.mutate(warehouseId, {
            op: "update",
            table: "tpcc_stock",
            partitionKey: item.partitionKey,
            values: { s_quantity: quantity, s_ytd: quantity, s_order_cnt: quantity, s_remote_cnt: 0 },
            requestId: `verified-${phase}-${item.partitionKey}-${crypto.randomUUID()}`,
          });
          expect(result.rowsAffected).toBe(1);
        }
      }

      await writeBatch("pre", 55);
      tracker.promoteToTracked(tracker.drainPendingCandidates());
      expect(tracker.trackedWrites()).toHaveLength(WRITE_COUNT);

      const readBack = pollingReadBack(gatewayReadBack(new InProcessSqlPointReader()));

      // BEFORE the reshard: a clean verify() pass genuinely earns
      // verified: true.
      await tracker.verify(readBack);
      expect(tracker.snapshot().verified).toBe(true);
      expect(tracker.snapshot().meterState).toBe("green");

      // THE RESHARD starts (a real /admin/migrate-vbucket, same as the main
      // proof above). Mirrors load-driver.ts's runTick wiring exactly: the
      // moment migration activity is observed in the live vbucket map, the
      // invalidation hook is called — verified must go false immediately,
      // even though NOTHING about the tracked set or its expected values
      // changed.
      const migrateRes = await post("/admin/migrate-vbucket", { catalogShardId, vbucket: targetVbucket, targetShardId }, AUTH());
      expect(migrateRes.status).toBe(200);
      const migrateBody = (await migrateRes.json()) as { status: string };
      expect(migrateBody.status).toBe("backfilling");
      // Cancel the tick alarm /admin/migrate-vbucket just scheduled so the
      // migration advances ONLY under driveMigrationToCompletion below (see
      // clearCatalogAlarm) — no stray wall-clock cutover during the "during"
      // batch.
      await clearCatalogAlarm();
      tracker.notifyClusterChanged();
      expect(tracker.snapshot().verified).toBe(false);
      expect(tracker.snapshot().lost).toBe(0); // nothing PROVEN lost — just no longer freshly verified

      // Writes DURING the backfill window, mirroring another load-driver
      // tick that still observes migration activity and re-invalidates
      // (matches runTick's per-tick call, not a one-shot). With the alarm
      // cleared, the migration is provably STILL pre-cutover here, so these
      // writes genuinely exercise the mirrored-backfilling window (Codex P2).
      const duringStatus = await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket: targetVbucket }, AUTH());
      expect(((await duringStatus.json()) as { status: string }).status).toBe("backfilling");
      await writeBatch("during", 66);
      tracker.promoteToTracked(tracker.drainPendingCandidates());
      tracker.notifyClusterChanged();
      expect(tracker.snapshot().verified).toBe(false);

      // Drive the REAL orchestration to completion.
      await driveMigrationToCompletion(targetVbucket);

      // Post-cutover, writes routed straight to the new shard.
      await writeBatch("post", 12);
      tracker.promoteToTracked(tracker.drainPendingCandidates());

      // No more migration activity is observed post-cutover — the FIRST
      // fresh verify() pass over the post-reshard state genuinely re-earns
      // verified: true (never notifyClusterChanged'd again after this).
      const verifyResult = await tracker.verify(readBack);
      expect(verifyResult.lostThisPass).toBe(0);
      expect(tracker.snapshot().lost).toBe(0);
      expect(tracker.snapshot().meterState).toBe("green");
      expect(tracker.snapshot().verified).toBe(true);
    },
    90000,
  );
});

// ----------------------------------------------------------------------------
// HIGH-VALUE ADDITION 1: real mismatched-replay against a live cluster.
// ----------------------------------------------------------------------------

describe("Shardscope live-cluster proof: mismatched-replay contract holds against the real gateway", () => {
  it(
    "replaying a requestId through /v1/mutate with a DIFFERENT body returns the real 409 contract, and the row shows only the first write",
    async () => {
      const initRes = await post("/admin/init", { numShards: 1, totalVBuckets: 16, force: true }, AUTH());
      expect(initRes.status).toBe(200);
      await createIndexTestTable("reshard_replay_evt");
      const tenantId = tenantForCatalogShard(0, 4);
      const token = await registerTenant(tenantId);

      const requestId = `replay-proof-${crypto.randomUUID()}`;
      const first = await post(
        "/v1/mutate",
        { op: "insert", table: "reshard_replay_evt", tenantId, partitionKey: "row-1", values: { v: "first" }, requestId },
        token,
      );
      expect(first.status).toBe(200);

      // Same requestId, a DIFFERENT body — chaos attack #2 against a real
      // cluster: refusing to silently replay a mismatched result.
      const mismatched = await post(
        "/v1/mutate",
        { op: "insert", table: "reshard_replay_evt", tenantId, partitionKey: "row-1", values: { v: "second-different" }, requestId },
        token,
      );
      expect(mismatched.status).toBe(409);
      const mismatchedBody = (await mismatched.json()) as { error: string };
      expect(mismatchedBody.error).toContain("different sql/params");

      const readRes = await post(
        "/v1/sql",
        { sql: "SELECT v FROM reshard_replay_evt WHERE id = ?", params: ["row-1"], table: "reshard_replay_evt", tenantId, partitionKey: "row-1" },
        AUTH(),
      );
      expect(readRes.status).toBe(200);
      const readBody = (await readRes.json()) as { result: { rows: Array<{ v: string }> } };
      expect(readBody.result.rows).toHaveLength(1);
      expect(readBody.result.rows[0].v).toBe("first");
    },
    30000,
  );
});

// ----------------------------------------------------------------------------
// HIGH-VALUE ADDITION 2: real fault injection on a NON-migrating shard,
// concurrent with a tracked write set to a DIFFERENT shard staying lost:0.
// ----------------------------------------------------------------------------

describe("Shardscope live-cluster proof: a real fault on one shard doesn't touch a concurrent write set on another", () => {
  afterEach(() => {
    env.FAULT_INJECTION_ENABLED = undefined;
  });

  it(
    "injecting a fault on one shard 503s only that shard; a concurrent tracked write set to the OTHER shard verifies lost:0; the fault auto-expires and the shard recovers",
    async () => {
      const totalVBuckets = 64;
      await setUpStockCluster(2, totalVBuckets);

      const warehouseId = 8801;
      const tenantId = tenantIdForWarehouse(warehouseId);
      const authorization = await registerTenant(tenantId);
      const exec = new InProcessTxExecutor(tenantId, authorization);

      const routeProbe = await post(
        "/v1/sql",
        { sql: "SELECT 1 AS one", table: "tpcc_stock", tenantId, partitionKey: stockKey(warehouseId, 1) },
        AUTH(),
      );
      const routeProbeBody = (await routeProbe.json()) as { route: { shardId: string } };
      const faultShardId = routeProbeBody.route.shardId;
      const catalogShardId = faultShardId.split("-shard-")[0];
      const otherShardId = faultShardId.endsWith("-shard-0") ? `${catalogShardId}-shard-1` : `${catalogShardId}-shard-0`;

      const vbMapRes = await post("/admin/vbucket-map", {}, AUTH());
      const vbMapBody = (await vbMapRes.json()) as {
        catalogShardCount: number;
        catalogs: Array<{ catalogShardId: string; map: Array<{ vbucket: number; shardId: string }> }>;
      };
      const catalogMap = vbMapBody.catalogs.find((c) => c.catalogShardId === catalogShardId)!.map;
      const ownership: VBucketOwnership[] = catalogMap.map((m) => ({ vbucket: m.vbucket, shardId: m.shardId }));

      // Sanity check (see the identical check in the reshard-under-load test
      // above for why this matters): the resolver below MUST resolve to the
      // same catalogShardId this test already derived from the gateway's own
      // routing, or the tracked write set would silently end up empty.
      expect(catalogShardIdForTenant(tenantId, vbMapBody.catalogShardCount)).toBe(catalogShardId);

      // One warehouse's stock rows naturally spread across both physical
      // shards by hash — no second tenant needed to get keys on "the other
      // shard".
      const skewedOther = generateSkewedKeys({
        targetShardId: otherShardId,
        vbucketMap: ownership,
        totalVBuckets,
        tenantId,
        table: "tpcc_stock",
        candidateToKey: (i) => ({ value: i + 1, partitionKey: stockKey(warehouseId, i + 1) }),
        count: 8,
      });
      expect(skewedOther.length).toBeGreaterThanOrEqual(5);
      const otherItems = skewedOther.slice(0, 5);

      const skewedFault = generateSkewedKeys({
        targetShardId: faultShardId,
        vbucketMap: ownership,
        totalVBuckets,
        tenantId,
        table: "tpcc_stock",
        candidateToKey: (i) => ({ value: i + 1, partitionKey: stockKey(warehouseId, i + 1) }),
        count: 1,
      });
      expect(skewedFault.length).toBe(1);
      const faultItem = skewedFault[0];

      for (const item of [...otherItems, faultItem]) {
        const seedRes = await post(
          "/v1/mutate",
          {
            op: "insert",
            table: "tpcc_stock",
            tenantId,
            partitionKey: item.partitionKey,
            values: { w_id: warehouseId, i_id: item.value, s_quantity: 100, s_ytd: 0, s_order_cnt: 0, s_remote_cnt: 0, s_data: "seed" },
            requestId: `seed-${item.partitionKey}-${crypto.randomUUID()}`,
          },
          authorization,
        );
        expect(seedRes.status).toBe(200);
      }

      const tracker = new CorrectnessTracker();
      // Real resolver — see the identical comment in the reshard-under-load
      // test above; never null here since vbMapBody is already in scope.
      const resolveCatalogShardId = (tid: string): string | null => catalogShardIdForTenant(tid, vbMapBody.catalogShardCount);
      const tracked = new TrackingTxExecutor(exec, tracker, resolveCatalogShardId);

      // FAULT_INJECTION_ENABLED is off by default cluster-wide (see
      // src/fault-injection.test.ts's header comment) — SELF's worker runs
      // in the same isolate as this test, so mutating env directly here (and
      // restoring it in afterEach) is the established way to turn it on for
      // just this test.
      env.FAULT_INJECTION_ENABLED = "true";
      const injectRes = await post("/admin/fault-inject", { shardId: faultShardId, durationMs: 150 }, AUTH());
      expect(injectRes.status).toBe(200);

      // The faulted shard genuinely rejects requests...
      const faultedWrite = await post(
        "/v1/mutate",
        { op: "update", table: "tpcc_stock", tenantId, partitionKey: faultItem.partitionKey, values: { s_quantity: 1 }, requestId: `fault-write-${crypto.randomUUID()}` },
        authorization,
      );
      expect(faultedWrite.status).toBe(503);

      // ...while a concurrent, TRACKED write set to the OTHER (healthy)
      // shard keeps landing normally.
      for (const item of otherItems) {
        const result = await tracked.mutate(warehouseId, {
          op: "update",
          table: "tpcc_stock",
          partitionKey: item.partitionKey,
          values: { s_quantity: 77, s_ytd: 77, s_order_cnt: 77, s_remote_cnt: 0 },
          requestId: `during-fault-${item.partitionKey}-${crypto.randomUUID()}`,
        });
        expect(result.rowsAffected).toBe(1);
      }
      tracker.promoteToTracked(tracker.drainPendingCandidates());
      expect(tracker.trackedWrites()).toHaveLength(otherItems.length);

      // Wait out the (short) fault window — no /admin/fault-clear call is
      // made, proving auto-expiry, not a manual clear.
      await new Promise((resolve) => setTimeout(resolve, 250));

      // The faulted shard recovers on its own.
      const recoveredWrite = await post(
        "/v1/mutate",
        { op: "update", table: "tpcc_stock", tenantId, partitionKey: faultItem.partitionKey, values: { s_quantity: 2 }, requestId: `recovered-write-${crypto.randomUUID()}` },
        authorization,
      );
      expect(recoveredWrite.status).toBe(200);

      // The concurrent write set to the OTHER shard verifies lost:0 through
      // the real correctness tracker's own gateway read-back.
      const readBack = pollingReadBack(gatewayReadBack(new InProcessSqlPointReader()));
      const verifyResult = await tracker.verify(readBack);
      expect(verifyResult.lostThisPass).toBe(0);
      expect(tracker.snapshot().lost).toBe(0);
      expect(tracker.snapshot().meterState).toBe("green");
    },
    30000,
  );
});
