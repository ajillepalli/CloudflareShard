// The 5 TPC-C transaction types, implemented against CloudflareShard's
// existing /v1/mutate, /v1/tx, /v1/index-query, and /v1/table-scan
// primitives. See the design doc (issue #16) for why each call sequence is
// shaped the way it is -- the two load-bearing constraints throughout are:
//   1. /v1/tx caps a single call at MAX_TX_PARTICIPANT_KEYS = 8 distinct
//      (tenantId, table, partitionKey) rows (src/index.ts).
//   2. /v1/tx requires every mutation in one call to share the same
//      tenantId -- and this benchmark's tenancy model is one tenant per
//      warehouse, so a transaction that must touch two different
//      warehouses' data can never be a single atomic /v1/tx call.
//
// Documented deviations from the official TPC-C spec (see also
// generate.mjs and README non-goals):
//   - Order-Status's by-name lookup variant is dropped (ID-only) -- the
//     by-name variant needs a substring/text-search capability this project
//     deliberately doesn't have.
//   - Payment's by-name lookup variant is dropped (ID-only), same reason.
//   - Stock-Level's server-side aggregate is replaced with table-scan +
//     client-side counting -- no aggregation pushdown exists.
//   - New-Order's per-order-line atomicity is split from the order header
//     (district update + order header + new_order insert are one tx; each
//     line's order_line insert + stock update is a SEPARATE tx) -- a direct
//     consequence of the 8-participant cap: a New-Order with the spec's
//     default 5-15 lines could touch 2 + 2*15 = 32 distinct rows, far past
//     the cap, if attempted as one transaction.
//   - Remote (cross-warehouse) order lines in New-Order (~1% of lines) fall
//     back to two independent, non-atomic /v1/mutate calls instead of a
//     2-participant tx -- a consequence of /v1/tx's one-tenantId-per-call
//     rule combined with the one-tenant-per-warehouse model. Only
//     same-warehouse order lines (~99%) get full order_line+stock
//     atomicity.

import { runPool } from "./client.mjs";
import { orderKey, orderLineKey, newOrderKey, historyKey } from "./keys.mjs";

function randInt(min, max) {
  return min + Math.floor(Math.random() * (max - min + 1));
}

function randomAmount(min, max) {
  return Math.round((min + Math.random() * (max - min)) * 100) / 100;
}

function round2(n) {
  return Math.round(n * 100) / 100;
}

/** New-Order: weight 45% of the standard TPC-C mix. */
async function newOrder(world) {
  const home = world.randomWarehouse();
  const w = home.warehouseId;
  const d = world.randomDistrictId();
  const c_id = world.randomCustomerId();

  const lineCount = randInt(5, 15);
  const lines = [];
  for (let l = 1; l <= lineCount; l++) {
    const i_id = world.randomItemId();
    let supplyWarehouseId = w;
    // 1% cross-warehouse rate, matching the real spec's convention -- only
    // possible when more than one warehouse was seeded.
    if (world.warehouses.length > 1 && Math.random() < 0.01) {
      do {
        supplyWarehouseId = world.warehouses[randInt(0, world.warehouses.length - 1)].warehouseId;
      } while (supplyWarehouseId === w);
    }
    lines.push({ ol_number: l, i_id, supplyWarehouseId, qty: randInt(1, 10), remote: supplyWarehouseId !== w });
  }

  // d_next_o_id mutates on every New-Order, so it can never be served from
  // the client-side reference cache (unlike district tax rates, which are
  // immutable post-load) -- always read it fresh. districts-per-warehouse is
  // small (~10 by default), so a full table-scan of this tiny table and
  // filtering client-side to the target d_id is simpler than a 7th index.
  const districtScan = await home.client.tableScan("tpcc_district", world.config.districtsPerWarehouse);
  const districtRow = (districtScan.rows || []).find((r) => r.d_id === d);
  if (!districtRow) throw new Error(`district ${d} not found in warehouse ${w}'s district table-scan`);
  const o_id = districtRow.d_next_o_id;
  const entryDate = new Date().toISOString();

  await home.client.tx(
    [
      { op: "update", table: "tpcc_district", partitionKey: districtRow.d_key, values: { d_next_o_id: o_id + 1 } },
      {
        op: "insert",
        table: "tpcc_orders",
        partitionKey: orderKey(w, d, o_id),
        values: { w_id: w, d_id: d, o_id, c_id, o_entry_d: entryDate, o_carrier_id: null, o_ol_cnt: lineCount },
      },
      {
        op: "insert",
        table: "tpcc_new_order",
        partitionKey: newOrderKey(w, d, o_id),
        values: { w_id: w, d_id: d, o_id },
      },
    ],
    crypto.randomUUID(),
  );

  // Process lines with a little concurrency -- independent rows in the
  // common (same-warehouse) case, so there's no reason to serialize them.
  await runPool(lines, Math.min(4, lines.length), async (line) => {
    const item = world.itemByI_id.get(line.i_id);
    const supplyWarehouse = world.warehouseById.get(line.supplyWarehouseId);

    // This index-query always targets the SUPPLY warehouse's own tenant's
    // stock table (which only has stock for its own warehouse) -- for a
    // remote line that means looking up the OTHER warehouse's cached token,
    // the honest way to model "remote supply warehouse" under the
    // one-tenant-per-warehouse model.
    const stockRes = await supplyWarehouse.client.indexQuery("tpcc_stock", "idx_stock_by_item", { i_id: line.i_id });
    const stockRow = (stockRes.rows || [])[0];
    if (!stockRow) throw new Error(`no stock row for item ${line.i_id} in warehouse ${line.supplyWarehouseId}`);

    let newQty = stockRow.s_quantity - line.qty;
    if (newQty < 10) newQty += 91;
    const stockValues = {
      s_quantity: newQty,
      s_ytd: stockRow.s_ytd + line.qty,
      s_order_cnt: stockRow.s_order_cnt + 1,
      s_remote_cnt: stockRow.s_remote_cnt + (line.remote ? 1 : 0),
    };
    const ol_amount = round2(item.i_price * line.qty);
    const orderLineValues = {
      w_id: w,
      d_id: d,
      o_id,
      ol_number: line.ol_number,
      ol_i_id: line.i_id,
      ol_supply_w_id: line.supplyWarehouseId,
      ol_quantity: line.qty,
      ol_amount,
      ol_delivery_d: null,
    };
    const olKey = orderLineKey(w, d, o_id, line.ol_number);

    if (!line.remote) {
      // Same-warehouse line: order_line insert + stock update as one
      // 2-participant tx -- full atomicity, both mutations share this
      // warehouse's tenantId.
      await home.client.tx(
        [
          { op: "insert", table: "tpcc_order_line", partitionKey: olKey, values: orderLineValues },
          { op: "update", table: "tpcc_stock", partitionKey: stockRow.s_key, values: stockValues },
        ],
        crypto.randomUUID(),
      );
    } else {
      // Remote line (~1%): order_line belongs to the ORDERING warehouse's
      // tenant, the stock update belongs to the SUPPLY warehouse's tenant --
      // /v1/tx requires every mutation in one call to share one tenantId, so
      // this pair can't be a single atomic tx across two tenants. Falls back
      // to two independent /v1/mutate calls (see file header comment) --
      // trades away order_line+stock atomicity for the ~1% of lines this
      // happens to.
      await home.client.mutate({ op: "insert", table: "tpcc_order_line", partitionKey: olKey, values: orderLineValues, requestId: crypto.randomUUID() });
      await supplyWarehouse.client.mutate({ op: "update", table: "tpcc_stock", partitionKey: stockRow.s_key, values: stockValues, requestId: crypto.randomUUID() });
    }
  });
}

/** Payment: weight 43% of the standard TPC-C mix. */
async function payment(world) {
  const home = world.randomWarehouse();
  const w = home.warehouseId;
  const d = world.randomDistrictId();
  const c_id = world.randomCustomerId();
  const amount = randomAmount(1.0, 5000.0);

  // Each of these tables has exactly one relevant row per tenant call here
  // (warehouse: singleton; district: small table filtered client-side), so a
  // cheap table-scan is simplest -- see New-Order's d_next_o_id comment for
  // why this can't be served from the client-side reference cache (w_ytd and
  // d_ytd both mutate on every Payment).
  const whScan = await home.client.tableScan("tpcc_warehouse", 1);
  const whRow = (whScan.rows || [])[0];
  if (!whRow) throw new Error(`warehouse ${w} row missing`);

  const distScan = await home.client.tableScan("tpcc_district", world.config.districtsPerWarehouse);
  const distRow = (distScan.rows || []).find((r) => r.d_id === d);
  if (!distRow) throw new Error(`district ${d} not found in warehouse ${w}`);

  const custRes = await home.client.indexQuery("tpcc_customer", "idx_customer_by_id", { d_id: d, c_id });
  const custRow = (custRes.rows || [])[0];
  if (!custRow) throw new Error(`customer ${c_id} in district ${d} of warehouse ${w} not found`);

  await home.client.tx(
    [
      { op: "update", table: "tpcc_warehouse", partitionKey: whRow.wh_key, values: { w_ytd: whRow.w_ytd + amount } },
      { op: "update", table: "tpcc_district", partitionKey: distRow.d_key, values: { d_ytd: distRow.d_ytd + amount } },
      {
        op: "update",
        table: "tpcc_customer",
        partitionKey: custRow.c_key,
        values: {
          c_balance: custRow.c_balance - amount,
          c_ytd_payment: custRow.c_ytd_payment + amount,
          c_payment_cnt: custRow.c_payment_cnt + 1,
        },
      },
      {
        op: "insert",
        table: "tpcc_history",
        partitionKey: historyKey(),
        values: {
          w_id: w,
          d_id: d,
          c_id,
          h_amount: amount,
          h_date: new Date().toISOString(),
          h_data: `${whRow.w_name ?? ""} ${distRow.d_name ?? ""}`.trim(),
        },
      },
    ],
    crypto.randomUUID(),
  );
}

/** Order-Status: weight 4%, read-only. ID-only lookup -- the official
 * spec's by-name-substring variant is dropped, see file header. */
async function orderStatus(world) {
  const home = world.randomWarehouse();
  const d = world.randomDistrictId();
  const c_id = world.randomCustomerId();

  const custRes = await home.client.indexQuery("tpcc_customer", "idx_customer_by_id", { d_id: d, c_id });
  if ((custRes.rows || []).length === 0) throw new Error(`customer ${c_id} in district ${d} not found`);

  // limit:100 (not 20) -- 100 is /v1/index-query's own hard server-side cap
  // (MAX_INDEX_QUERY_LIMIT in src/index.ts), and there's no pagination for
  // this route, so a customer who accumulates more than 100 orders over one
  // benchmark run's lifetime would still have this exact truncation problem
  // (the code below picks the highest o_id it CAN see, not necessarily the
  // true highest) -- an accepted, documented residual limit at this
  // benchmark's realistic scale/duration, not something worth adding a
  // dedicated "orders by district, most-recent-N" index to work around.
  const ordersRes = await home.client.indexQuery("tpcc_orders", "idx_orders_by_customer", { d_id: d, c_id }, 100);
  const orderRows = ordersRes.rows || [];
  if (orderRows.length === 0) {
    // A customer with no orders yet is a legitimate (if, at steady state,
    // unlikely) outcome -- not an error.
    return;
  }
  // Ascending by partition key (o_key), which embeds a zero-padded o_id --
  // the LAST row is the highest o_id, i.e. the customer's most recent order
  // among those returned (see the limit comment above for the residual edge case).
  const latestOrder = orderRows[orderRows.length - 1];

  await home.client.indexQuery("tpcc_order_line", "idx_order_line_by_order", { d_id: d, o_id: latestOrder.o_id }, 15);
}

/** Delivery: weight 4%. Processes the oldest undelivered order in EVERY
 * district of the chosen warehouse -- one Delivery "transaction" in the mix
 * covers all districts-per-warehouse, matching real TPC-C's batch-style
 * Delivery. */
async function delivery(world) {
  const home = world.randomWarehouse();
  const w = home.warehouseId;

  for (let d = 1; d <= world.config.districtsPerWarehouse; d++) {
    const noRes = await home.client.indexQuery("tpcc_new_order", "idx_new_order_by_district", { d_id: d }, 100);
    const noRows = noRes.rows || [];
    if (noRows.length === 0) continue; // nothing outstanding in this district, skip it

    // Ascending by partition key (no_key, zero-padded o_id) -- the FIRST row
    // is the lowest o_id, i.e. the oldest undelivered order.
    const oldest = noRows[0];
    const o_id = oldest.o_id;

    const orderRes = await home.client.indexQuery("tpcc_orders", "idx_orders_by_id", { d_id: d, o_id });
    const orderRow = (orderRes.rows || [])[0];
    if (!orderRow) throw new Error(`order ${o_id} in district ${d} of warehouse ${w} not found despite a new_order row`);

    const custRes = await home.client.indexQuery("tpcc_customer", "idx_customer_by_id", { d_id: d, c_id: orderRow.c_id });
    const custRow = (custRes.rows || [])[0];
    if (!custRow) throw new Error(`customer ${orderRow.c_id} in district ${d} of warehouse ${w} not found`);

    const linesRes = await home.client.indexQuery("tpcc_order_line", "idx_order_line_by_order", { d_id: d, o_id }, 15);
    const lines = linesRes.rows || [];
    const sumAmount = round2(lines.reduce((acc, l) => acc + (l.ol_amount || 0), 0));

    await home.client.tx(
      [
        { op: "delete", table: "tpcc_new_order", partitionKey: oldest.no_key },
        { op: "update", table: "tpcc_orders", partitionKey: orderRow.o_key, values: { o_carrier_id: randInt(1, 10) } },
        {
          op: "update",
          table: "tpcc_customer",
          partitionKey: custRow.c_key,
          values: { c_balance: custRow.c_balance + sumAmount, c_delivery_cnt: custRow.c_delivery_cnt + 1 },
        },
      ],
      crypto.randomUUID(),
    );

    // Marking each line delivered is done non-transactionally, one
    // /v1/mutate per line, deliberately OUTSIDE the tx above: these rows are
    // already committed by the time we get here, there's no cross-row
    // invariant requiring them to land atomically with the tx (or with each
    // other), and folding them in would burn scarce participant-count
    // headroom against the 8-row /v1/tx cap for orders with many lines, for
    // no correctness benefit.
    if (lines.length > 0) {
      const deliveredAt = new Date().toISOString();
      await runPool(lines, Math.min(4, lines.length), (line) =>
        home.client.mutate({
          op: "update",
          table: "tpcc_order_line",
          partitionKey: line.ol_key,
          values: { ol_delivery_d: deliveredAt },
          requestId: crypto.randomUUID(),
        }),
      );
    }
  }
}

/** Stock-Level: weight 4%, read-only. Real TPC-C semantics: of the distinct
 * items appearing in this district's last 20 orders, how many have stock
 * below the threshold in this warehouse. The server-side aggregate is
 * replaced with index-query/table-scan + client-side counting (no
 * aggregation pushdown exists), but the actual QUESTION being measured
 * still matches the spec -- Codex review P2 fix: an earlier version
 * table-scanned the whole warehouse's stock table regardless of district or
 * recent-order scoping, which reports a materially different (and
 * inflated/irrelevant) number than real Stock-Level measures. */
async function stockLevel(world) {
  const home = world.randomWarehouse();
  const d = world.randomDistrictId();
  const threshold = randInt(10, 20);

  // idx_orders_by_district returns entries ascending by o_key (zero-padded
  // o_id), so the LAST up to 20 of up to 100 returned are this district's
  // most recent orders -- same "limit:100, take the tail" residual-limit
  // reasoning as Order-Status above (see that function's comment) applies
  // here too, at this benchmark's realistic scale.
  const ordersRes = await home.client.indexQuery("tpcc_orders", "idx_orders_by_district", { d_id: d }, 100);
  const orderRows = ordersRes.rows || [];
  const recentOrders = orderRows.slice(-20);
  if (recentOrders.length === 0) return 0; // no orders yet for this district

  const itemIds = new Set();
  for (const order of recentOrders) {
    const linesRes = await home.client.indexQuery("tpcc_order_line", "idx_order_line_by_order", { d_id: d, o_id: order.o_id }, 15);
    for (const line of linesRes.rows || []) itemIds.add(line.ol_i_id);
  }
  if (itemIds.size === 0) return 0;

  const lowStockResults = await runPool([...itemIds], Math.min(8, itemIds.size), async (i_id) => {
    const stockRes = await home.client.indexQuery("tpcc_stock", "idx_stock_by_item", { i_id });
    const stockRow = (stockRes.rows || [])[0];
    return stockRow ? stockRow.s_quantity < threshold : false;
  });
  return lowStockResults.filter(Boolean).length;
}

export const TRANSACTION_MIX = [
  { type: "new-order", weight: 45, run: newOrder },
  { type: "payment", weight: 43, run: payment },
  { type: "order-status", weight: 4, run: orderStatus },
  { type: "delivery", weight: 4, run: delivery },
  { type: "stock-level", weight: 4, run: stockLevel },
];

const TOTAL_WEIGHT = TRANSACTION_MIX.reduce((acc, t) => acc + t.weight, 0);

function pickTransaction() {
  let r = Math.random() * TOTAL_WEIGHT;
  for (const t of TRANSACTION_MIX) {
    if (r < t.weight) return t;
    r -= t.weight;
  }
  return TRANSACTION_MIX[TRANSACTION_MIX.length - 1];
}

/** Runs one randomly-chosen transaction (weighted per TRANSACTION_MIX) and
 * returns a {type, ok, latencyMs, error?} record for the benchmark report. */
export async function runOneTransaction(world) {
  const chosen = pickTransaction();
  const start = globalThis.performance.now();
  try {
    await chosen.run(world);
    return { type: chosen.type, ok: true, latencyMs: globalThis.performance.now() - start };
  } catch (err) {
    return {
      type: chosen.type,
      ok: false,
      latencyMs: globalThis.performance.now() - start,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}
