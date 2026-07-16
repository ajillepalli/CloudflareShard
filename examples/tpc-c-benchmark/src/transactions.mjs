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
//     (district update + order insert are one tx; each line's order_line
//     insert + stock update is a SEPARATE tx) -- a direct consequence of the
//     8-participant cap: a New-Order with the spec's default 5-15 lines
//     could touch 2 + 2*15 = 32 distinct rows, far past the cap, if
//     attempted as one transaction. The new_order marker itself (what makes
//     an order visible to Delivery at all) is deliberately inserted LAST,
//     as its own standalone write after every line has committed -- not
//     alongside district+orders in the header tx -- specifically so a
//     concurrent Delivery pass can never observe an order that's still
//     mid-flight (Codex review found this: inserting the marker up front
//     let Delivery race New-Order's own line-processing loop, summing an
//     incomplete order and orphaning whichever lines hadn't landed yet).
//     A second, narrower gap was found the same way (confirmed live:
//     seeding fresh warehouses and running under contention reliably
//     produced a small number of these): runPool's own `Promise.all`
//     rejects on the FIRST line to throw without waiting for other
//     still-in-flight lines, so if one line failed (a TX_ABORTED, a
//     detected stock conflict), the New-Order function threw and skipped
//     the marker insert entirely -- while OTHER lines already in flight in
//     parallel workers could still commit successfully in the background
//     afterward, leaving a real, partial, permanently-undelivered order
//     behind. Fixed by catching each line's own error INSIDE the pool
//     callback instead of letting it propagate through Promise.all: every
//     line is now always awaited to completion (success or caught failure)
//     before the function decides whether to insert the marker or report an
//     aggregate failure. A THIRD round of review (Codex, again) correctly
//     pointed out that this still left the header + any already-succeeded
//     lines committed with no rollback when one line failed -- a real,
//     permanent partial order. compensateFailedOrder (below, in newOrder)
//     now actually reverses those: deletes each already-committed line and
//     reverts its stock update (itself a compare-and-swap against exactly
//     the values that line applied, so it safely no-ops rather than
//     corrupts if some OTHER write touched the row again meanwhile), then
//     deletes the orphaned orders row. d_next_o_id is deliberately NOT
//     decremented back (a different, newer New-Order may have already
//     claimed the incremented value) -- the resulting gap in the o_id
//     sequence is expected and harmless, like a rolled-back insert leaving
//     a gap in a real database's auto-increment sequence. Genuinely
//     residual after this: a THIRD concurrent write landing in the narrow
//     window between a line's own update and its compensation reversal
//     would make the reversal itself safely no-op (never corrupt) rather
//     than fully undo -- doubly unlikely, and a dropped compensation is a
//     stranded stock decrement, not silent corruption.
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

  // Codex review P2 fix: the new_order marker (the thing that makes this
  // order visible to Delivery at all) is deliberately NOT inserted here,
  // alongside district+orders. If it were, a concurrent Delivery worker
  // could pick up this order in the window between this header tx
  // committing and the per-line transactions below finishing -- summing
  // zero or partial order_line rows, deleting the marker, and adjusting the
  // customer's balance by the wrong (incomplete) amount, with the still-
  // arriving lines left permanently undelivered (no marker left for a
  // future Delivery pass to find them by). Inserting new_order as the LAST
  // step, only once every line has actually committed, closes that window:
  // an order is never visible to Delivery until it's actually complete.
  await home.client.tx(
    [
      { op: "update", table: "tpcc_district", partitionKey: districtRow.d_key, values: { d_next_o_id: o_id + 1 } },
      {
        op: "insert",
        table: "tpcc_orders",
        partitionKey: orderKey(w, d, o_id),
        values: { w_id: w, d_id: d, o_id, c_id, o_entry_d: entryDate, o_carrier_id: null, o_ol_cnt: lineCount },
      },
    ],
    crypto.randomUUID(),
  );

  // Process lines with a little concurrency -- independent rows in the
  // common (same-warehouse) case, so there's no reason to serialize them.
  // Verification found a real gap in an earlier version of this: runPool's
  // Promise.all rejects on the FIRST line to throw without waiting for
  // other still-in-flight lines, so those other lines could still commit
  // successfully in the background AFTER this function had already thrown
  // past the marker-insert step below -- a partial, never-delivered order.
  // Catching each line's own error INSIDE the pool callback (instead of
  // letting it propagate through Promise.all) means every line is always
  // awaited to completion, one way or the other, before this function
  // decides whether to insert the marker or report a failure -- no line's
  // write can land after the caller has already given up on it.
  const lineErrors = [];
  const succeededLines = [];
  await runPool(lines, Math.min(4, lines.length), async (line) => {
    try {
      succeededLines.push(await processOrderLine(line));
    } catch (err) {
      lineErrors.push({ ol_number: line.ol_number, error: err instanceof Error ? err.message : String(err) });
    }
  });
  if (lineErrors.length > 0) {
    // Codex review P2 fix: compensate already-committed lines (and the
    // header) instead of just leaving them behind as a permanent partial
    // order. See compensateFailedOrder's own comment for the mechanics and
    // its own residual limits.
    await compensateFailedOrder(succeededLines);
    throw new Error(
      `New-Order failed on ${lineErrors.length}/${lines.length} line(s) (compensated ${succeededLines.length} already-committed line(s)): ${lineErrors.map((e) => `line ${e.ol_number}: ${e.error}`).join("; ")}`,
    );
  }

  // Only now -- after every line has actually committed -- does this order
  // become visible to Delivery. See the comment on the header tx above.
  await home.client.mutate({
    op: "insert",
    table: "tpcc_new_order",
    partitionKey: newOrderKey(w, d, o_id),
    values: { w_id: w, d_id: d, o_id },
    requestId: crypto.randomUUID(),
  });

  /** Reverses whatever already-committed lines a failed New-Order left
   * behind, plus the orphaned header row -- a real compensating-transaction
   * (saga) step, not just documentation, per Codex round 5 review.
   * Each line's stock reversal is itself a compare-and-swap (`where` matches
   * exactly the values THIS line's own update applied): if some OTHER write
   * touched the stock row again since, the reversal safely no-ops instead of
   * corrupting that newer state -- a genuine, if now much narrower and
   * doubly-unlikely, residual (a THIRD concurrent write landing in the
   * exact window between this line's update and its own compensation).
   * The orphaned `tpcc_orders` row is deleted outright; `d_next_o_id` is
   * deliberately NOT decremented back -- a different, newer New-Order may
   * have already claimed the incremented value, and reversing it could
   * collide with that legitimately-different order. The resulting gap in
   * the o_id sequence is expected and harmless, the same way a rolled-back
   * insert leaves a gap in a real database's auto-increment sequence. */
  async function compensateFailedOrder(succeeded) {
    for (const line of succeeded) {
      try {
        // order_line always belongs to the ORDERING warehouse's tenant;
        // stock belongs to the SUPPLY warehouse's tenant -- the same two
        // (possibly different, for a remote line) tenants processOrderLine
        // itself wrote to, so reversal must address each independently.
        await line.orderLineClient.mutate({ op: "delete", table: "tpcc_order_line", partitionKey: line.olKey, requestId: crypto.randomUUID() });
        await line.stockClient.mutate({
          op: "update",
          table: "tpcc_stock",
          partitionKey: line.stockKey,
          values: line.originalStock,
          where: line.appliedStock,
          requestId: crypto.randomUUID(),
        });
      } catch {
        // Best-effort: keep compensating the rest of the lines rather than
        // aborting the whole rollback over one failed reversal.
      }
    }
    await home.client.mutate({ op: "delete", table: "tpcc_orders", partitionKey: orderKey(w, d, o_id), requestId: crypto.randomUUID() });
  }

  async function processOrderLine(line) {
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
    // Codex review P2 fix: these are absolute values computed from a read
    // that happened before this write -- CloudflareShard's structured
    // mutations have no server-side arithmetic UPDATE (`values` always SETs
    // to a literal, never `col - ?`), so a second concurrent New-Order line
    // touching the SAME stock row (a popular item, or a small --items pool)
    // could otherwise silently overwrite this decrement with its own
    // equally-stale computation, losing one of the two entirely with no
    // error from either caller. `where` makes the UPDATE a compare-and-swap
    // against the exact row this was computed from: if another write beat
    // this one to the row, the predicate won't match and this UPDATE becomes
    // a no-op instead of corrupting the counter with a stale value. Note this
    // still isn't a full fix -- /v1/tx doesn't report per-mutation
    // rowsAffected, so there's no signal here to retry on (see the mutate
    // path below, which can detect and at least surface it). A dropped
    // update is silently safer than a corrupting one, not a correct one.
    const stockWhere = {
      s_quantity: stockRow.s_quantity,
      s_ytd: stockRow.s_ytd,
      s_order_cnt: stockRow.s_order_cnt,
      s_remote_cnt: stockRow.s_remote_cnt,
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

    // Compensation data for compensateFailedOrder, if a LATER sibling line
    // fails and this already-committed one needs to be reversed.
    const compensation = {
      orderLineClient: home.client,
      stockClient: supplyWarehouse.client,
      olKey,
      stockKey: stockRow.s_key,
      originalStock: stockWhere,
      appliedStock: stockValues,
    };

    if (!line.remote) {
      // Same-warehouse line: order_line insert + stock update as one
      // 2-participant tx -- full atomicity, both mutations share this
      // warehouse's tenantId.
      await home.client.tx(
        [
          { op: "insert", table: "tpcc_order_line", partitionKey: olKey, values: orderLineValues },
          { op: "update", table: "tpcc_stock", partitionKey: stockRow.s_key, values: stockValues, where: stockWhere },
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
      // happens to. /v1/mutate (unlike /v1/tx) DOES report rowsAffected, so
      // the same compare-and-swap `where` guard here can actually be
      // detected and surfaced as a real error rather than silently no-oping.
      await home.client.mutate({ op: "insert", table: "tpcc_order_line", partitionKey: olKey, values: orderLineValues, requestId: crypto.randomUUID() });
      const stockMutateResult = await supplyWarehouse.client.mutate({
        op: "update",
        table: "tpcc_stock",
        partitionKey: stockRow.s_key,
        values: stockValues,
        where: stockWhere,
        requestId: crypto.randomUUID(),
      });
      if (stockMutateResult.rowsAffected === 0) {
        throw new Error(`stock row ${stockRow.s_key} changed concurrently -- remote line's stock update did not apply`);
      }
    }
    return compensation;
  }
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

  // Codex review P2 fix: compare-and-swap guards on every counter update
  // below, same reasoning as New-Order's stock update above -- warehouse/
  // district/customer rows are hot (every Payment in the warehouse touches
  // the same warehouse row and one of a handful of district rows), and these
  // are absolute values computed from a read that happened before this
  // write. Without a guard, a second concurrent Payment reading the same
  // stale value would silently overwrite this one's increment; with it, the
  // conflicting write becomes a no-op instead (still not retried -- see the
  // New-Order comment for why /v1/tx can't signal that back to the caller).
  await home.client.tx(
    [
      {
        op: "update",
        table: "tpcc_warehouse",
        partitionKey: whRow.wh_key,
        values: { w_ytd: whRow.w_ytd + amount },
        where: { w_ytd: whRow.w_ytd },
      },
      {
        op: "update",
        table: "tpcc_district",
        partitionKey: distRow.d_key,
        values: { d_ytd: distRow.d_ytd + amount },
        where: { d_ytd: distRow.d_ytd },
      },
      {
        op: "update",
        table: "tpcc_customer",
        partitionKey: custRow.c_key,
        values: {
          c_balance: custRow.c_balance - amount,
          c_ytd_payment: custRow.c_ytd_payment + amount,
          c_payment_cnt: custRow.c_payment_cnt + 1,
        },
        where: { c_balance: custRow.c_balance, c_ytd_payment: custRow.c_ytd_payment, c_payment_cnt: custRow.c_payment_cnt },
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

    // Codex review P2 fix: CLAIM the marker first, via its own /v1/mutate
    // delete (checked for rowsAffected), before reading or touching anything
    // else. This is the actual concurrency-safe fence: a second Delivery
    // worker that read this SAME "oldest" row (before this delete committed)
    // will see rowsAffected: 0 here and correctly skip the order as already
    // claimed by someone else, instead of proceeding to re-credit the
    // customer for an order someone else already delivered. The previous
    // version deleted the marker as part of the closing /v1/tx below, whose
    // response doesn't report per-mutation rowsAffected -- a stale-read
    // second worker's delete would silently no-op there while its customer
    // credit still committed, double-crediting the customer for one order.
    const claimResult = await home.client.mutate({
      op: "delete",
      table: "tpcc_new_order",
      partitionKey: oldest.no_key,
      requestId: crypto.randomUUID(),
    });
    if (claimResult.rowsAffected === 0) continue; // already claimed by a concurrent Delivery worker

    const orderRes = await home.client.indexQuery("tpcc_orders", "idx_orders_by_id", { d_id: d, o_id });
    const orderRow = (orderRes.rows || [])[0];
    if (!orderRow) throw new Error(`order ${o_id} in district ${d} of warehouse ${w} not found despite a new_order row`);

    const custRes = await home.client.indexQuery("tpcc_customer", "idx_customer_by_id", { d_id: d, c_id: orderRow.c_id });
    const custRow = (custRes.rows || [])[0];
    if (!custRow) throw new Error(`customer ${orderRow.c_id} in district ${d} of warehouse ${w} not found`);

    const linesRes = await home.client.indexQuery("tpcc_order_line", "idx_order_line_by_order", { d_id: d, o_id }, 15);
    const lines = linesRes.rows || [];
    const sumAmount = round2(lines.reduce((acc, l) => acc + (l.ol_amount || 0), 0));

    // The marker is already claimed above -- this tx only needs to update
    // orders + customer now.
    await home.client.tx(
      [
        // o_carrier_id is a plain SET, not derived from a prior read, so no
        // compare-and-swap guard is needed here (unlike the customer update
        // below).
        { op: "update", table: "tpcc_orders", partitionKey: orderRow.o_key, values: { o_carrier_id: randInt(1, 10) } },
        {
          // Codex review P2 fix: same compare-and-swap guard as Payment's
          // customer update above -- c_balance/c_delivery_cnt are absolute
          // values computed from a read predating this write.
          op: "update",
          table: "tpcc_customer",
          partitionKey: custRow.c_key,
          values: { c_balance: custRow.c_balance + sumAmount, c_delivery_cnt: custRow.c_delivery_cnt + 1 },
          where: { c_balance: custRow.c_balance, c_delivery_cnt: custRow.c_delivery_cnt },
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
