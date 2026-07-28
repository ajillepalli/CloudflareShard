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
import type { TxExecutor } from "./transactions";

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

  for (let d = 1; d <= districtsPerWarehouse; d++) {
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
        // d_next_o_id comment) even though this bootstrap seeds no
        // pre-existing orders itself.
        d_next_o_id: customersPerDistrict + 1,
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
