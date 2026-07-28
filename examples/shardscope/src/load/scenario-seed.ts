/** scenario-seed.ts — minimal reference-data bootstrap for the self-service
 * "Start the scenario" flow (Shardscope's Topology room).
 *
 * WHY THIS EXISTS: real TPC-C transactions (./transactions.ts) only ever
 * UPDATE or READ existing warehouse/district/customer/item/stock rows — none
 * of the 5 transaction types create that reference data from nothing. The
 * TPC-C benchmark harness (examples/tpc-c-benchmark) seeds it via its own
 * Node script, but that script registers its OWN tenant tokens through
 * /admin/register-tenant directly — a DIFFERENT token-issuing path than
 * ./tenant-token-store.ts's TenantTokenStoreTokenProvider, which LoadDriver
 * uses. Those two paths can't share a tenant: TenantTokenStoreTokenProvider
 * deliberately REFUSES to rotate a tenant's token if it's already registered
 * by someone else (TenantAlreadyRegisteredError — see tenant-token-store.ts's
 * header comment), specifically so it never silently invalidates a token the
 * Node harness already cached. So a warehouse the Node harness seeded can
 * never be used by LoadDriver's own token, and vice versa — whichever caller
 * registers a tenant first is the only one who can ever get a working token
 * for it again.
 *
 * The practical fix: LoadDriver seeds its OWN tiny slice of reference data,
 * through its OWN token, so "Start the scenario" is fully self-contained —
 * no external setup step, no shared-tenant conflict. Deliberately much
 * smaller than the benchmark harness's real-TPC-C-scale defaults (10
 * districts * 100 customers, 200 items): this exists to give the 5
 * transaction types SOMETHING real to act on quickly when a visitor clicks a
 * button, not to reproduce a representative OLTP dataset. Every row is
 * upserted (safe to call repeatedly — re-running "Start the scenario" against
 * a warehouse that already has this data just overwrites it with equivalent
 * values, mirroring generate.mjs's own "upsert is safe to rerun" convention).
 *
 * SIZING NOTE (found via live testing): the ORIGINAL constants here (2
 * districts, 5 customers, 10 items) were too small to run under real
 * concurrent TPC-C traffic. CloudflareShard's /v1/tx uses fail-fast 2PC row
 * locking (see src/shard.ts's handlePrepare: "no queueing, reject before
 * touching anything if any row is locked by a DIFFERENT in-flight
 * transaction") — a deliberate, correct design, not a bug. With only 2
 * district rows shared across EVERY New-Order and Payment transaction for a
 * warehouse, and New-Order's own tx() calls having no retry-on-abort logic
 * (unlike Payment's explicit CAS retry loop), a live test against the
 * original sizing saw a genuinely high New-Order abort rate (TX_ABORTED /
 * "Prepare failed on shard ..." from real row-lock contention) — high enough
 * that the resulting churn (successful writes, aborts, compensating
 * reversals, all hammering the same handful of rows) exposed edge cases in
 * ./correctness.ts's tracker that don't show up at realistic TPC-C
 * contention levels. These constants are sized up specifically to bring
 * lock contention down to a level a real (if small) warehouse would see —
 * not to chase a specific "lost: 0" number, but because the original sizing
 * was creating artificial contention no real deployment would produce.
 */
import { runPool, type TxExecutor } from "./transactions";

export const SCENARIO_DISTRICTS_PER_WAREHOUSE = 6;
export const SCENARIO_CUSTOMERS_PER_DISTRICT = 10;
export const SCENARIO_ITEM_COUNT = 60;

function pad(n: number, width: number): string {
  return String(n).padStart(width, "0");
}

// Key formats mirror examples/tpc-c-benchmark/src/keys.mjs exactly (fixed-
// width zero-padded so table-scan's lexicographic partition-key order stays
// numerically correct) — but note this ISN'T a correctness requirement
// shared with ./transactions.ts: it never constructs these keys itself, only
// ever discovers rows via /v1/table-scan or /v1/index-query and reads back
// whatever partition key the row already has. Any reasonable, internally-
// consistent format would work; matching keys.mjs's is just a convenience so
// a partition key looks familiar next to rows the benchmark harness seeded
// in some OTHER, non-conflicting warehouse.
function warehouseKey(w: number): string {
  return `wh-${pad(w, 4)}`;
}
function districtKey(w: number, d: number): string {
  return `d-${pad(w, 4)}-${pad(d, 2)}`;
}
function customerKey(w: number, d: number, c: number): string {
  return `c-${pad(w, 4)}-${pad(d, 2)}-${pad(c, 5)}`;
}
function itemKey(w: number, i: number): string {
  return `i-${pad(w, 4)}-${pad(i, 6)}`;
}
function stockKeyFor(w: number, i: number): string {
  // Deliberately duplicated rather than importing transactions.ts's exported
  // stockKey: that one is documented as the exact format processOrderLine
  // writes to and skew.ts's candidateToKey scans over — this file has no
  // need to share that specific coupling, and a local copy keeps this
  // module's only dependency on transactions.ts down to the TxExecutor type.
  return `s-${pad(w, 4)}-${pad(i, 6)}`;
}

function randomPrice(min: number, max: number): number {
  return Math.round((min + Math.random() * (max - min)) * 100) / 100;
}

/** Seeds SCENARIO_DISTRICTS_PER_WAREHOUSE districts, each with
 * `customersPerDistrict` customers, plus `itemCount` items (and one stock
 * row per item) for `warehouseId` — enough for every one of the 5 TPC-C
 * transaction types to find real rows to act on. These MUST match
 * load-driver.ts's own config.districtsPerWarehouse/customersPerDistrict/
 * itemCount exactly (see handleStart's willSelfSeed branch, which is the
 * only caller and derives both from the same source) — the transaction mix
 * picks random district/customer/item ids up to those counts, so seeding a
 * smaller range than the mix believes exists means most picks miss
 * entirely. Every call is a plain upsert with fixed, deterministic values
 * (no read-before-write, no "preserve existing state" logic like
 * generate.mjs's own reseed path has — this is a quick demo bootstrap, not
 * a benchmark harness guarding against clobbering real accumulated
 * history). Throws on the first failed mutate; callers decide how to
 * surface that (see load-driver.ts's handleStart). */
export async function seedScenarioReferenceData(
  executor: TxExecutor,
  warehouseId: number,
  districtsPerWarehouse: number,
  customersPerDistrict: number,
  itemCount: number,
): Promise<void> {
  const w = warehouseId;

  await executor.mutate(w, {
    op: "upsert",
    table: "tpcc_warehouse",
    partitionKey: warehouseKey(w),
    values: { w_id: w, w_name: `WH${w}`, w_tax: randomPrice(0.0, 0.2), w_ytd: 300000.0 },
  });

  // Codex review P2 fix: re-running "Start the scenario" against a warehouse
  // that already has orders from a PREVIOUS run must never regress
  // d_next_o_id below its current value. The old code unconditionally reset
  // it to customersPerDistrict + 1 on every reseed, while leaving that prior
  // run's tpcc_orders/tpcc_order_line/tpcc_new_order rows in place — the next
  // New-Order then reused an already-existing o_id, its header tx's insert
  // collided with that pre-existing order row, and New-Order stayed wedged
  // for that district (every attempt failing) until the process restarted.
  // Reading the district's CURRENT d_next_o_id first and keeping the higher
  // of (existing, customersPerDistrict + 1) fixes this without needing to
  // clean up prior orders: a fresh district gets the same baseline as
  // before, an already-seeded one just keeps counting forward from where it
  // left off, exactly like a real warehouse would.
  const existingDistrictScan = await executor.tableScan(w, "tpcc_district", districtsPerWarehouse);
  const existingNextOId = new Map<number, number>();
  for (const row of (existingDistrictScan.rows ?? []) as unknown as Array<{ d_id?: number; d_next_o_id?: number }>) {
    if (typeof row.d_id === "number" && typeof row.d_next_o_id === "number") {
      existingNextOId.set(row.d_id, row.d_next_o_id);
    }
  }

  for (let d = 1; d <= districtsPerWarehouse; d++) {
    const baselineNextOId = customersPerDistrict + 1;
    const currentNextOId = existingNextOId.get(d);
    await executor.mutate(w, {
      op: "upsert",
      table: "tpcc_district",
      partitionKey: districtKey(w, d),
      values: {
        w_id: w,
        d_id: d,
        d_name: `D${w}-${d}`,
        d_tax: randomPrice(0.0, 0.2),
        d_ytd: 30000.0,
        // Past customersPerDistrict so New-Order's first real order in this
        // district starts numbering right after the seeded customers,
        // matching generate.mjs's own baseline convention (see that file's
        // d_next_o_id comment) — but never REGRESSING a district that
        // already has real orders from a previous run (see this function's
        // own comment above).
        d_next_o_id: currentNextOId !== undefined ? Math.max(currentNextOId, baselineNextOId) : baselineNextOId,
      },
    });

    for (let c = 1; c <= customersPerDistrict; c++) {
      await executor.mutate(w, {
        op: "upsert",
        table: "tpcc_customer",
        partitionKey: customerKey(w, d, c),
        values: {
          w_id: w,
          d_id: d,
          c_id: c,
          c_first: `Scenario${c}`,
          c_last: `Customer${d}`,
          c_credit: "GC",
          c_discount: randomPrice(0.0, 0.5),
          c_balance: -10.0,
          c_ytd_payment: 10.0,
          c_payment_cnt: 1,
          c_delivery_cnt: 0,
        },
      });
    }
  }

  for (let i = 1; i <= itemCount; i++) {
    await executor.mutate(w, {
      op: "upsert",
      table: "tpcc_item",
      partitionKey: itemKey(w, i),
      values: { i_id: i, i_name: `Item ${i}`, i_price: randomPrice(1, 100), i_data: "scenario-seeded" },
    });
    await executor.mutate(w, {
      op: "upsert",
      table: "tpcc_stock",
      partitionKey: stockKeyFor(w, i),
      values: { w_id: w, i_id: i, s_quantity: 100, s_ytd: 0, s_order_cnt: 0, s_remote_cnt: 0, s_data: "scenario-seeded" },
    });
  }
}

const SEED_INDEX_VISIBILITY_RETRY_ATTEMPTS = 20;
const SEED_INDEX_VISIBILITY_RETRY_DELAY_MS = 250;

/** Codex review P2 fix: schema-bootstrap.ts's ensureScenarioIndexesReady
 * only proves the INDEX RULE itself is 'ready' — a rule can be 'ready'
 * because SOME earlier run already built it (e.g. warehouse 1's own prior
 * "Start the scenario"), while THIS call's freshly-upserted rows (a new
 * warehouseId, or an expanded itemCount/customersPerDistrict on a re-seed)
 * haven't reached it yet: /v1/mutate's index maintenance for those specific
 * rows is dispatched via the gateway's own ctx.waitUntil (see
 * src/index.ts's mutateCore) and isn't guaranteed done by the time this
 * function's own upserts return. Without this check, the very first ticks
 * could query idx_stock_by_item/idx_customer_by_id for a row that was just
 * seeded but isn't index-visible yet, reporting a spurious "no stock row"/
 * "no customer" failure even though seeding genuinely succeeded.
 *
 * ROUND 9 CORRECTION: an earlier version of this function canary-checked
 * only item 1 and (district 1, customer 1), on the (wrong) assumption that
 * "index maintenance for one write request's whole batch of rows completes
 * together" — that's not actually true here: seedScenarioReferenceData sends
 * one SEPARATE /v1/mutate call per row, not one batched request, so each
 * row's index maintenance is dispatched independently and can complete in
 * any order. Item 1 becoming visible says nothing about item 60. This now
 * verifies EVERY seeded item and EVERY seeded customer (not just canaries),
 * polling only the ones not yet confirmed each pass (same shape as
 * schema-bootstrap.ts's waitForIndexesReady) — throws (never silently
 * proceeds) if any of them never catches up.
 *
 * ROUND 10 CORRECTION: round 9's fix still only checked customers in
 * DISTRICT 1 — the same independent-async-maintenance reasoning above
 * applies across districts too, not just across customer ids within one
 * district, so a customer in district 2 could remain unindexed even after
 * every district-1 customer settled. Now checks every (district, customer)
 * pair across all `districtsPerWarehouse` districts, since the transaction
 * mix picks a random district for every Payment/Order-Status/New-Order
 * attempt, not just district 1.
 *
 * ROUND 11 CORRECTION: firing every pending check in ONE unbounded
 * Promise.all is fine at this feature's own default scale (60 items + 60
 * customers = 120 checks) but not in general — a caller supplying larger
 * custom counts (e.g. customersPerDistrict: 100, itemCount: 200 with the
 * default 6 districts is 800 checks) could exceed the Worker's
 * per-invocation subrequest budget in a single pass (each indexQuery is
 * itself 2 subrequests — a tenant-token resolution plus the actual HTTP
 * call — see gateway-client.ts's HttpTxExecutor.post), failing the whole
 * start instead of merely taking a few extra polling passes. Now runs
 * through runPool (the same bounded-concurrency helper transactions.ts
 * already uses for its own per-order line pools) instead of a raw
 * Promise.all, capping how many checks are ever in flight at once
 * regardless of how many are pending. */
const SEED_INDEX_VISIBILITY_CONCURRENCY = 20;

export async function verifySeededDataIndexed(
  executor: TxExecutor,
  warehouseId: number,
  districtsPerWarehouse: number,
  customersPerDistrict: number,
  itemCount: number,
): Promise<void> {
  const w = warehouseId;

  const pendingItems = new Set<number>();
  for (let i = 1; i <= itemCount; i++) pendingItems.add(i);
  // Keyed by "d_id:c_id" — every (district, customer) pair this warehouse
  // seeded, not just district 1's.
  const pendingCustomers = new Set<string>();
  for (let d = 1; d <= districtsPerWarehouse; d++) {
    for (let c = 1; c <= customersPerDistrict; c++) pendingCustomers.add(`${d}:${c}`);
  }

  for (let attempt = 1; ; attempt++) {
    const checks: Array<() => Promise<void>> = [
      ...[...pendingItems].map((i_id) => async () => {
        const res = await executor.indexQuery(w, "tpcc_stock", "idx_stock_by_item", { i_id });
        if ((res.rows ?? []).length > 0) pendingItems.delete(i_id);
      }),
      ...[...pendingCustomers].map((key) => async () => {
        const [d_id, c_id] = key.split(":").map(Number);
        const res = await executor.indexQuery(w, "tpcc_customer", "idx_customer_by_id", { d_id, c_id });
        if ((res.rows ?? []).length > 0) pendingCustomers.delete(key);
      }),
    ];
    await runPool(checks, SEED_INDEX_VISIBILITY_CONCURRENCY, (check) => check());
    if (pendingItems.size === 0 && pendingCustomers.size === 0) return;
    if (attempt >= SEED_INDEX_VISIBILITY_RETRY_ATTEMPTS) {
      throw new Error(
        `warehouse ${w}'s seeded data never became fully visible through idx_stock_by_item/idx_customer_by_id after ${SEED_INDEX_VISIBILITY_RETRY_ATTEMPTS} attempts (${pendingItems.size} item(s), ${pendingCustomers.size} customer(s) still not indexed)`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, SEED_INDEX_VISIBILITY_RETRY_DELAY_MS));
  }
}
