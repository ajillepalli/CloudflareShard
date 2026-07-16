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
//   - New-Order's atomicity is split across several pieces, not one single
//     transaction covering the whole order -- a direct consequence of two
//     platform limits working together: the 8-participant /v1/tx cap (a
//     5-15 line order could touch 30+ rows if attempted as one transaction)
//     and /v1/tx not reporting per-mutation rowsAffected (a compare-and-swap
//     `where` guard bundled inside a /v1/tx is undetectable on failure --
//     the whole transaction still reports "committed" even when the guard
//     silently matched zero rows). The header (district update + order
//     insert) is one small tx (2 rows, always fits, no guard needed --
//     see below). Each line does its order_line insert and stock update as
//     TWO separate /v1/mutate calls -- same-warehouse and remote lines are
//     no longer distinguished for atomicity, both trade order_line+stock
//     atomicity for the ability to actually detect and retry a losing
//     stock CAS guard (which /v1/mutate's rowsAffected makes possible,
//     unlike /v1/tx). The new_order marker (what makes an order visible to
//     Delivery at all) is inserted LAST, only once every line has
//     genuinely committed -- not alongside the header -- so a concurrent
//     Delivery pass can never observe an order that's still mid-flight.
//     Five real bugs were found here across four rounds of review, each
//     verified live against fresh warehouses under contention, before this
//     shape converged: (1) inserting the marker alongside the header let
//     Delivery race New-Order's own line-processing loop, summing an
//     incomplete order. (2) `runPool`'s `Promise.all` used to reject on the
//     first line to fail without waiting for other still-in-flight lines,
//     so other lines could keep committing in the background after the
//     function had already thrown past the marker insert -- fixed by
//     catching each line's own error inside the pool instead of letting it
//     propagate. (3) even with that fix, a genuinely failed line still left
//     its already-committed siblings and the header uncompensated forever
//     -- fixed with `compensateFailedOrder`, a real compensating-transaction
//     step: reverses each already-committed line (deletes its order_line
//     row, reverts its stock update via its own compare-and-swap matching
//     exactly the values that line applied, so it safely no-ops rather than
//     corrupts if a different write touched the row again meanwhile), then
//     deletes the orphaned `orders` row (`d_next_o_id` deliberately not
//     decremented back, since a different, newer order may have already
//     claimed the incremented value -- an expected, harmless gap in the
//     o_id sequence, like a rolled-back insert leaving a gap in a real
//     database's auto-increment sequence). (4) a remote line whose
//     order_line insert succeeded but whose stock update then failed threw
//     before it could return its compensation record, so a partial insert
//     went uncompensated -- fixed by compensating it inline, immediately,
//     at the point of failure. (5) two DIFFERENT concurrent orders racing
//     the SAME stock item (this benchmark's reduced default --items pool
//     makes this real, not theoretical) could have the losing one's stock
//     CAS guard silently fail to match while the same-warehouse case's
//     /v1/tx bundle still reported "committed" -- undetectable and
//     unretriable from inside /v1/tx. Fixed by unifying same-warehouse and
//     remote lines onto the same /v1/mutate-plus-bounded-retry pattern (see
//     MUTATION_RETRY_ATTEMPTS), giving up order_line+stock atomicity
//     entirely in exchange for a stock CAS that's always detectable and
//     safely retryable. Genuinely residual after all five fixes: a THIRD
//     concurrent write landing in the narrow window between a line's
//     update and its own compensation reversal makes that reversal safely
//     no-op rather than fully undo -- doubly unlikely, and results in a
//     stranded stock decrement, not silent corruption. Round 6's
//     within-one-order (supplyWarehouseId, i_id) dedup remains in place
//     too -- it reduces how often the retry loop is even needed, though it
//     no longer needs to carry the whole correctness burden by itself.

import { runPool, ApiError } from "./client.mjs";
import { orderKey, orderLineKey, newOrderKey, historyKey } from "./keys.mjs";

function randInt(min, max) {
  return min + Math.floor(Math.random() * (max - min + 1));
}

function randomAmount(min, max) {
  return Math.round((min + Math.random() * (max - min)) * 100) / 100;
}

// Shared by every read-compute-write retry loop in this file (New-Order's
// stock update, Payment's warehouse/district/customer updates, Delivery's
// customer credit) -- see their own comments for why a bounded retry
// against a fresh read, backed by /v1/mutate's per-call rowsAffected, is
// the only safe way to make a compare-and-swap-guarded counter update both
// correct AND detectable.
const MUTATION_RETRY_ATTEMPTS = 5;

// Codex review round 9 P1 fix: CloudflareShard's secondary-index maintenance
// for /v1/mutate is dispatched via ctx.waitUntil() AFTER the base row write
// already succeeded and its response was returned (see mutateCore's own
// comment in src/index.ts) -- deliberately non-blocking for the caller, but
// it means a /v1/mutate response saying "ok" does NOT guarantee that row's
// secondary-index entry is queryable yet. Delivery's order_line lookup
// (idx_order_line_by_order) depends on exactly that index, and depending on
// an incomplete result there isn't just stale data (like Order-Status
// possibly missing a just-placed order for a moment) -- it's a genuine data
// loss: Delivery would sum only the visible lines, credit the customer too
// little, and delete the new_order marker, permanently losing any lines
// whose index entry hadn't caught up yet (no marker survives to let a later
// pass find them). ORDERS_LINES_VISIBILITY_RETRY_ATTEMPTS/_DELAY_MS bound
// how long Delivery waits for the index to catch up to the order's own
// known `o_ol_cnt` before giving up.
const ORDER_LINES_VISIBILITY_RETRY_ATTEMPTS = 10;
const ORDER_LINES_VISIBILITY_RETRY_DELAY_MS = 150;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
  // Codex review P2 fix: no two lines in ONE order may reference the same
  // (supplyWarehouseId, i_id) stock row. If they did, both lines' workers
  // could read the same stock row before either writes, then race their
  // compare-and-swap updates -- for a same-warehouse line pair, `/v1/tx`
  // still reports "committed" even when the second update's `where` guard
  // silently failed to match (no per-mutation rowsAffected from /v1/tx), so
  // the order would end up "successful" with both order_line rows inserted
  // but only ONE of the two stock decrements actually applied. A real
  // arithmetic UPDATE wouldn't have this problem at all; CloudflareShard's
  // structured mutations don't offer one, so the fix here is to prevent the
  // collision from ever occurring instead of trying to detect it after the
  // fact. Bounded retry: if a configured --items pool is smaller than this
  // order's line count (only reachable with an unusually small --items
  // value relative to the realistic 5-15 line count), falls back to
  // accepting a duplicate rather than looping forever.
  const usedSupplyItemKeys = new Set();
  for (let l = 1; l <= lineCount; l++) {
    let supplyWarehouseId = w;
    // 1% cross-warehouse rate, matching the real spec's convention -- only
    // possible when more than one warehouse was seeded.
    if (world.warehouses.length > 1 && Math.random() < 0.01) {
      do {
        supplyWarehouseId = world.warehouses[randInt(0, world.warehouses.length - 1)].warehouseId;
      } while (supplyWarehouseId === w);
    }
    let i_id;
    let key;
    let attempts = 0;
    do {
      i_id = world.randomItemId();
      key = `${supplyWarehouseId}-${i_id}`;
      attempts++;
    } while (usedSupplyItemKeys.has(key) && attempts < 20);
    usedSupplyItemKeys.add(key);
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
  //
  // Codex review round 11 P2 fix: the district update below now carries a
  // `where: { d_next_o_id: o_id }` guard against d_next_o_id regressing.
  // Without it, a request that read d_next_o_id=o_id, then stalled (e.g.
  // behind a slow line/compensation elsewhere), could still commit this
  // header tx AFTER a different, faster order had already advanced the
  // counter past o_id -- blindly setting d_next_o_id back DOWN to o_id + 1
  // and handing that same, now-stale o_id out again to a future New-Order
  // read, colliding with an order that already exists. The order INSERT
  // alongside it stays unconditional: per this file's established /v1/tx
  // semantics (see the file header and Payment's own comment above), a
  // where-mismatched participant simply has no effect while the rest of the
  // SAME tx still commits -- so a guard miss here just skips the counter
  // regression, while the insert independently succeeds (reusing a key a
  // prior compensation already freed -- harmless) or fails as a duplicate
  // (self-resolving: recorded as an ordinary failed New-Order attempt, the
  // same as any other TX_ABORTED). No retry loop is needed here for a
  // GUARD MISS (unlike Payment/Delivery's counters): there's no legitimate
  // reason to retry one -- a fresh New-Order attempt just reads whatever
  // d_next_o_id is current next time around.
  //
  // Codex review round 12 P2 fix: a bounded retry IS needed for a different
  // failure mode though -- this call itself can throw on an ambiguous
  // network error (the tx actually committed on the coordinator, but the
  // response never reached the client). Before this fix, that error exited
  // newOrder immediately, before line processing or compensation ever ran,
  // leaving a genuinely committed tpcc_orders row with no lines and no
  // new_order marker -- permanently invisible to Delivery AND never
  // compensated (compensateFailedOrder only runs once at least one line has
  // been attempted). Safe to retry blindly under the SAME stable
  // headerRequestId: txId is derived from (tenantId, requestId) (see
  // src/coordinator.ts's handleBegin), so a retry of an already-committed
  // header replays its cached "committed" outcome instead of re-executing
  // (double-incrementing d_next_o_id or double-inserting the order). This
  // must NOT retry on a clean, definitive error response, though -- a real
  // 409 TX_ABORTED (e.g. genuine prepare-phase lock contention) means the
  // coordinator has already recorded this txId as aborted for good;
  // retrying with the same requestId would just replay that same aborted
  // response every time (see handleBegin's `status === "aborted"` branch),
  // burning the whole retry budget for nothing instead of surfacing the
  // real, immediate failure this benchmark already handles correctly
  // elsewhere. Only a thrown error that ISN'T a clean ApiError (a
  // network-level failure with no definitive server response at all) is
  // genuinely ambiguous enough to warrant a retry here.
  const headerRequestId = crypto.randomUUID();
  for (let attempt = 1; ; attempt++) {
    try {
      await home.client.tx(
        [
          {
            op: "update",
            table: "tpcc_district",
            partitionKey: districtRow.d_key,
            values: { d_next_o_id: o_id + 1 },
            where: { d_next_o_id: o_id },
          },
          {
            op: "insert",
            table: "tpcc_orders",
            partitionKey: orderKey(w, d, o_id),
            values: { w_id: w, d_id: d, o_id, c_id, o_entry_d: entryDate, o_carrier_id: null, o_ol_cnt: lineCount },
          },
        ],
        headerRequestId,
      );
      break;
    } catch (err) {
      if (err instanceof ApiError || attempt >= MUTATION_RETRY_ATTEMPTS) throw err;
    }
  }

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
  //
  // Codex review round 10 P2 fix: if THIS call itself fails (a transient
  // network/topology error, not a rejection), every line and the header have
  // already committed -- there's nothing left to compensate, but without the
  // marker the order is still permanently invisible to Delivery (the same
  // failure mode compensateFailedOrder exists for, just one step later).
  // Retrying with a bounded loop closes that gap, and it's safe to retry
  // blindly (no fresh read/compare-and-swap needed, unlike the counter
  // updates elsewhere in this file) because every attempt reuses the SAME
  // requestId: /v1/mutate's underlying ShardDO records each requestId in an
  // applied_requests dedupe cache (see src/shard.ts), so a retry after the
  // first attempt actually applied but its response was merely lost replays
  // the cached "already applied" outcome instead of double-inserting.
  const markerRequestId = crypto.randomUUID();
  let markerInserted = false;
  let lastMarkerError;
  for (let attempt = 1; attempt <= MUTATION_RETRY_ATTEMPTS; attempt++) {
    try {
      await home.client.mutate({
        op: "insert",
        table: "tpcc_new_order",
        partitionKey: newOrderKey(w, d, o_id),
        values: { w_id: w, d_id: d, o_id },
        requestId: markerRequestId,
      });
      markerInserted = true;
      break;
    } catch (err) {
      lastMarkerError = err;
    }
  }
  if (!markerInserted) {
    // Deliberately does NOT fall back to compensateFailedOrder here: after
    // exhausting retries there's no way to tell "never applied on the
    // server" apart from "applied on some earlier attempt, response lost
    // every time since" (the dedupe cache only helps a RETRY converge, it
    // doesn't let the caller distinguish these cases in hindsight). Reverting
    // the lines/stock in the second case would corrupt a real, already-marked
    // order. Throwing loudly and leaving the order exactly as committed is
    // the same choice this file makes everywhere else under that ambiguity
    // (see Payment's retry loops) -- a rare, visible failed attempt instead
    // of a rarer but silent double-corruption.
    throw new Error(
      `New-Order's line(s) and header committed but the new_order marker insert failed after ${MUTATION_RETRY_ATTEMPTS} attempts -- order ${o_id} is committed but may be invisible to Delivery: ${lastMarkerError instanceof Error ? lastMarkerError.message : String(lastMarkerError)}`,
    );
  }

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
    const olKey = orderLineKey(w, d, o_id, line.ol_number);
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

    // Codex review round 8 P2 fix: same-warehouse and remote lines now share
    // the IDENTICAL pattern -- insert order_line via /v1/mutate, then
    // retry-loop the stock update via /v1/mutate (checked rowsAffected),
    // compensating the order_line insert if the stock update ultimately
    // can't apply. An earlier version bundled the same-warehouse case into
    // one atomic 2-participant /v1/tx instead, for full order_line+stock
    // atomicity -- but review found that atomicity comes at the cost of
    // detectability: two DIFFERENT concurrent New-Orders racing the SAME
    // stock item (this benchmark's deliberately reduced default --items
    // pool makes this a real, not theoretical, scenario -- round 6's
    // within-one-order dedup doesn't help here, since these are two
    // SEPARATE orders) could have the losing one's stock CAS guard silently
    // fail to match while /v1/tx still reported the whole line's tx
    // "committed" -- order_line inserted, stock decrement silently dropped,
    // and the New-Order counted as fully successful. Only /v1/mutate's
    // per-call rowsAffected makes a safe retry possible, the same reasoning
    // already applied to Payment/Delivery's counter updates.
    // Codex review round 12 P2 fix: the order_line insert now runs INSIDE
    // the same try block as the stock retry loop below (it used to run
    // before it). If this insert call itself throws on an ambiguous
    // network error (the response lost after the insert actually
    // committed server-side), the old code exited processOrderLine
    // immediately, before ever entering the try -- so the catch block's
    // cleanup (deleting the just-inserted order_line row) never ran, and
    // this line was reported as failed to the caller without ever
    // appearing in succeededLines. compensateFailedOrder only reverses
    // lines IT was told about, so a maybe-already-committed order_line row
    // from an ambiguous insert failure would have been left behind,
    // orphaned under a header that goes on to get deleted. Moving the
    // insert inside the try means this exact failure now hits the SAME
    // best-effort delete-then-rethrow cleanup the stock-update failure
    // path already relies on.
    try {
      await home.client.mutate({ op: "insert", table: "tpcc_order_line", partitionKey: olKey, values: orderLineValues, requestId: crypto.randomUUID() });
      for (let attempt = 1; ; attempt++) {
        // This index-query always targets the SUPPLY warehouse's own
        // tenant's stock table (which only has stock for its own
        // warehouse) -- for a remote line that means looking up the OTHER
        // warehouse's cached token, the honest way to model "remote supply
        // warehouse" under the one-tenant-per-warehouse model.
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
        const stockWhere = {
          s_quantity: stockRow.s_quantity,
          s_ytd: stockRow.s_ytd,
          s_order_cnt: stockRow.s_order_cnt,
          s_remote_cnt: stockRow.s_remote_cnt,
        };

        const result = await supplyWarehouse.client.mutate({
          op: "update",
          table: "tpcc_stock",
          partitionKey: stockRow.s_key,
          values: stockValues,
          where: stockWhere,
          requestId: crypto.randomUUID(),
        });
        if (result.rowsAffected > 0) {
          // Compensation data for compensateFailedOrder, if a LATER sibling
          // line fails and this already-committed one needs to be reversed.
          return {
            orderLineClient: home.client,
            stockClient: supplyWarehouse.client,
            olKey,
            stockKey: stockRow.s_key,
            originalStock: stockWhere,
            appliedStock: stockValues,
          };
        }
        if (attempt >= MUTATION_RETRY_ATTEMPTS) {
          throw new Error(`stock row ${stockRow.s_key} update did not apply after ${MUTATION_RETRY_ATTEMPTS} attempts -- persistent contention`);
        }
      }
    } catch (err) {
      // This line as a whole is about to fail -- delete the order_line row
      // right here, immediately, rather than depend on the outer
      // succeededLines/compensateFailedOrder mechanism knowing about a
      // partial success it was never told about (processOrderLine never
      // returns in this path, so it can't). Covers both the case the stock
      // retry loop threw (the insert definitely committed) and the case the
      // insert call itself threw ambiguously (round 12 fix -- it may or may
      // not have committed); a delete of a row that was never actually
      // inserted is a harmless no-op either way.
      await home.client
        .mutate({ op: "delete", table: "tpcc_order_line", partitionKey: olKey, requestId: crypto.randomUUID() })
        .catch(() => {
          // Best-effort -- surfacing the ORIGINAL error below matters more
          // than this cleanup attempt's own outcome.
        });
      throw err;
    }
  }
}

/** Payment: weight 43% of the standard TPC-C mix.
 *
 * Codex review round 7 P2 fix: warehouse/district/customer are updated via
 * separate /v1/mutate calls with a bounded read-compute-write retry loop
 * each, NOT bundled into one /v1/tx the way an earlier version did. This is
 * a real, deliberate change in this transaction's shape, not a small patch
 * -- worth explaining why. The earlier version's /v1/tx bundle used a
 * compare-and-swap `where` guard on each update (matching mutateCore's
 * pattern elsewhere in this file), which correctly turns a losing race into
 * a no-op instead of a corruption -- but /v1/tx reports the whole
 * transaction as "committed" with no per-mutation rowsAffected, so a losing
 * guard is invisible: the benchmark would count the Payment as fully
 * successful even when its warehouse/district/customer updates silently
 * didn't apply (only the always-unconditional history insert would have
 * landed). Every warehouse in the default mix gets hit by EVERY Payment in
 * that warehouse (43% of the whole transaction mix), so this isn't a rare
 * corner case -- it's the single hottest row in the whole benchmark.
 *
 * A verify-then-retry-the-whole-tx approach (re-read after commit, retry if
 * the guarded fields don't match what was intended) was considered and
 * rejected: it can't distinguish "my guarded update never applied" from "my
 * update applied AND a later Payment's update applied on top of it" -- both
 * look identical from a post-commit read, but retrying in the second case
 * would silently apply this Payment's amount a SECOND time, a real
 * double-charge bug. Only /v1/mutate's per-call rowsAffected can tell the
 * two apart reliably, which is why each row here gets its own retry loop
 * instead. The trade-off: warehouse/district/customer/history no longer
 * commit as one atomic unit the way real TPC-C Payment specifies -- each
 * row's own update is still individually safe (never corrupts, always
 * either applies cleanly or is retried against a fresh read), but a crash
 * between two of these calls could leave Payment partially applied across
 * rows in a way a single real transaction wouldn't. Accepted for a
 * benchmark whose point is exercising real primitives under load, not
 * modeling failure-injection between individual HTTP calls.
 */
async function payment(world) {
  const home = world.randomWarehouse();
  const w = home.warehouseId;
  const d = world.randomDistrictId();
  const c_id = world.randomCustomerId();
  const amount = randomAmount(1.0, 5000.0);

  // Codex review round 11 P2 fix: real TPC-C's Payment draws a REMOTE
  // customer (a different warehouse than the terminal/home processing the
  // payment) ~15% of the time whenever more than one warehouse exists --
  // an earlier version always resolved the customer against `home`,
  // silently dropping this path (and the cross-warehouse load it's meant to
  // exercise) entirely. warehouse/district YTD always update on the
  // TERMINAL's own warehouse (home) regardless -- only the customer
  // lookup/update below resolves against customerHome instead. This is
  // straightforward specifically because Payment already does each counter
  // as its own single-tenant /v1/mutate call (see this function's own
  // comment above) rather than one bundled /v1/tx: /v1/tx's same-tenant
  // requirement would have made combining a remote customer with the
  // other counters in one transaction impossible anyway.
  let customerHome = home;
  let customerDistrictId = d;
  const remote = world.warehouses.length > 1 && Math.random() < 0.15;
  if (remote) {
    do {
      customerHome = world.warehouses[randInt(0, world.warehouses.length - 1)];
    } while (customerHome.warehouseId === w);
    customerDistrictId = world.randomDistrictId();
  }

  let whRow, distRow;

  // Each of these tables has exactly one relevant row per tenant call here
  // (warehouse: singleton; district: small table filtered client-side), so a
  // cheap table-scan is simplest -- see New-Order's d_next_o_id comment for
  // why this can't be served from the client-side reference cache (w_ytd and
  // d_ytd both mutate on every Payment).
  for (let attempt = 1; ; attempt++) {
    const whScan = await home.client.tableScan("tpcc_warehouse", 1);
    whRow = (whScan.rows || [])[0];
    if (!whRow) throw new Error(`warehouse ${w} row missing`);
    const result = await home.client.mutate({
      op: "update",
      table: "tpcc_warehouse",
      partitionKey: whRow.wh_key,
      values: { w_ytd: round2(whRow.w_ytd + amount) },
      where: { w_ytd: whRow.w_ytd },
      requestId: crypto.randomUUID(),
    });
    if (result.rowsAffected > 0) break;
    if (attempt >= MUTATION_RETRY_ATTEMPTS) {
      throw new Error(`warehouse ${w} w_ytd update did not apply after ${MUTATION_RETRY_ATTEMPTS} attempts -- persistent contention`);
    }
  }

  for (let attempt = 1; ; attempt++) {
    const distScan = await home.client.tableScan("tpcc_district", world.config.districtsPerWarehouse);
    distRow = (distScan.rows || []).find((r) => r.d_id === d);
    if (!distRow) throw new Error(`district ${d} not found in warehouse ${w}`);
    const result = await home.client.mutate({
      op: "update",
      table: "tpcc_district",
      partitionKey: distRow.d_key,
      values: { d_ytd: round2(distRow.d_ytd + amount) },
      where: { d_ytd: distRow.d_ytd },
      requestId: crypto.randomUUID(),
    });
    if (result.rowsAffected > 0) break;
    if (attempt >= MUTATION_RETRY_ATTEMPTS) {
      throw new Error(`district ${d} d_ytd update did not apply after ${MUTATION_RETRY_ATTEMPTS} attempts -- persistent contention`);
    }
  }

  let custRow;
  for (let attempt = 1; ; attempt++) {
    const custRes = await customerHome.client.indexQuery("tpcc_customer", "idx_customer_by_id", { d_id: customerDistrictId, c_id });
    custRow = (custRes.rows || [])[0];
    if (!custRow) throw new Error(`customer ${c_id} in district ${customerDistrictId} of warehouse ${customerHome.warehouseId} not found`);
    const result = await customerHome.client.mutate({
      op: "update",
      table: "tpcc_customer",
      partitionKey: custRow.c_key,
      values: {
        c_balance: round2(custRow.c_balance - amount),
        c_ytd_payment: round2(custRow.c_ytd_payment + amount),
        c_payment_cnt: custRow.c_payment_cnt + 1,
      },
      where: { c_balance: custRow.c_balance, c_ytd_payment: custRow.c_ytd_payment, c_payment_cnt: custRow.c_payment_cnt },
      requestId: crypto.randomUUID(),
    });
    if (result.rowsAffected > 0) break;
    if (attempt >= MUTATION_RETRY_ATTEMPTS) {
      throw new Error(`customer ${c_id} balance update did not apply after ${MUTATION_RETRY_ATTEMPTS} attempts -- persistent contention`);
    }
  }

  // Unconditional side effect -- only recorded once the three updates above
  // are all confirmed applied, so a retried/abandoned Payment never leaves
  // a phantom history row behind for an update that didn't actually happen.
  // w_id/d_id here are always the TERMINAL's (home's) own warehouse/district
  // -- this reduced schema has no separate customer-warehouse/district
  // columns, so a remote payment's customer location is noted in h_data
  // instead, the same minimal-schema trade-off historyKey()/tpcc_history
  // already make elsewhere.
  await home.client.mutate({
    op: "insert",
    table: "tpcc_history",
    partitionKey: historyKey(),
    values: {
      w_id: w,
      d_id: d,
      c_id,
      h_amount: amount,
      h_date: new Date().toISOString(),
      h_data: `${whRow.w_name ?? ""} ${distRow.d_name ?? ""}`.trim() + (remote ? ` (remote customer: warehouse ${customerHome.warehouseId} district ${customerDistrictId})` : ""),
    },
    requestId: crypto.randomUUID(),
  });
}

/** Order-Status: weight 4%, read-only. ID-only lookup -- the official
 * spec's by-name-substring variant is dropped, see file header.
 *
 * This reads via index-query the same way Delivery does (see
 * ORDER_LINES_VISIBILITY_RETRY_ATTEMPTS's comment for the underlying
 * secondary-index-maintenance lag /v1/mutate can have), but deliberately
 * doesn't wait/retry for it here: the consequence of a moment's staleness
 * is fundamentally different. Delivery's staleness deletes the only marker
 * that makes a lagging line ever findable again -- real, permanent data
 * loss. Order-Status is read-only and reports live, current state on its
 * NEXT call regardless; missing a just-placed order for a few hundred
 * milliseconds is ordinary, self-healing eventual consistency, not a bug
 * worth adding retry machinery for. */
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
    // Codex review round 11 P2 fix: if this call itself THROWS (a transient
    // network/topology error, not a clean rowsAffected:0 response), we can't
    // tell whether the delete actually committed before the error -- unlike
    // the marker-restore try/catch below, this failure happens BEFORE that
    // block even starts, so it would otherwise skip straight past every
    // safety net and leave the order permanently unclaimable if the delete
    // in fact applied. Best-effort reinsert the marker on any such ambiguous
    // failure: if the delete never actually applied, the row is still there
    // and this insert simply fails as a duplicate (harmless, swallowed); if
    // it did apply, this restores it so a later Delivery pass can retry.
    // Either way, the original error is rethrown -- this attempt is still a
    // genuine failure, not a silent skip.
    let claimResult;
    try {
      claimResult = await home.client.mutate({
        op: "delete",
        table: "tpcc_new_order",
        partitionKey: oldest.no_key,
        requestId: crypto.randomUUID(),
      });
    } catch (err) {
      await home.client
        .mutate({
          op: "insert",
          table: "tpcc_new_order",
          partitionKey: oldest.no_key,
          values: { w_id: w, d_id: d, o_id },
          requestId: crypto.randomUUID(),
        })
        .catch(() => {});
      throw err;
    }
    if (claimResult.rowsAffected === 0) continue; // already claimed by a concurrent Delivery worker

    // Codex review P2 fix: the marker is gone the moment the claim above
    // commits -- if anything before the customer credit below fails (a
    // not-found lookup, contention on the customer row exhausting its
    // retry budget, a network error), the order would otherwise be
    // permanently invisible to every future Delivery pass (claimed, but
    // never actually delivered, with no marker left to find it by again).
    // Restore the marker on any SUCH failure so a later Delivery pass can
    // retry the whole thing from scratch. Codex review round 8 P2 fix:
    // this try/catch must end the MOMENT the customer credit is confirmed
    // applied, not wrap deliverLines(lines) too -- an earlier version
    // wrapped both, so a failure in deliverLines (marking individual lines'
    // delivery dates, well after the customer was already credited) would
    // restore the marker and let a future Delivery pass pick up and
    // re-credit the SAME order a second time. Once the credit is confirmed,
    // this order must never be reprocessed, regardless of what happens next.
    let lines;
    try {
      const orderRes = await home.client.indexQuery("tpcc_orders", "idx_orders_by_id", { d_id: d, o_id });
      const orderRow = (orderRes.rows || [])[0];
      if (!orderRow) throw new Error(`order ${o_id} in district ${d} of warehouse ${w} not found despite a new_order row`);

      // o_carrier_id is a plain SET, not derived from a prior read, so no
      // compare-and-swap guard (or retry) is needed here.
      await home.client.mutate({
        op: "update",
        table: "tpcc_orders",
        partitionKey: orderRow.o_key,
        values: { o_carrier_id: randInt(1, 10) },
        requestId: crypto.randomUUID(),
      });

      // Wait for idx_order_line_by_order to catch up to this order's own
      // known line count before trusting it -- see
      // ORDER_LINES_VISIBILITY_RETRY_ATTEMPTS's comment for why this
      // matters here specifically (unlike Order-Status's read-only,
      // non-data-losing exposure to the same underlying index-maintenance
      // lag). o_ol_cnt is a base-row field on `orders`, written
      // synchronously in New-Order's header tx -- always trustworthy as
      // the target count to wait for, independent of index lag.
      for (let attempt = 1; ; attempt++) {
        const linesRes = await home.client.indexQuery("tpcc_order_line", "idx_order_line_by_order", { d_id: d, o_id }, 15);
        lines = linesRes.rows || [];
        if (lines.length >= orderRow.o_ol_cnt) break;
        if (attempt >= ORDER_LINES_VISIBILITY_RETRY_ATTEMPTS) {
          throw new Error(
            `order ${o_id} in district ${d} of warehouse ${w}: only ${lines.length}/${orderRow.o_ol_cnt} order_line index entries visible after ${ORDER_LINES_VISIBILITY_RETRY_ATTEMPTS} attempts -- index maintenance lag did not clear`,
          );
        }
        await sleep(ORDER_LINES_VISIBILITY_RETRY_DELAY_MS);
      }
      const sumAmount = round2(lines.reduce((acc, l) => acc + (l.ol_amount || 0), 0));

      // Codex review round 7 P2 fix: same reasoning as Payment's retry loops
      // -- this used to be a compare-and-swap-guarded update bundled into a
      // /v1/tx alongside the orders update above, but /v1/tx doesn't report
      // per-mutation rowsAffected, so a losing guard here would have been
      // invisible: the customer's balance/delivery_cnt simply wouldn't
      // update, yet the whole Delivery attempt would still be recorded as
      // successful. A plain /v1/mutate call DOES report rowsAffected, making
      // a real bounded retry-against-a-fresh-read possible and safe here
      // (unlike a naive verify-then-retry-the-whole-tx approach, which can't
      // tell "my update never applied" apart from "my update applied and a
      // later Payment's also applied on top of it" -- see Payment's longer
      // comment above for why that distinction matters).
      for (let attempt = 1; ; attempt++) {
        const custRes = await home.client.indexQuery("tpcc_customer", "idx_customer_by_id", { d_id: d, c_id: orderRow.c_id });
        const custRow = (custRes.rows || [])[0];
        if (!custRow) throw new Error(`customer ${orderRow.c_id} in district ${d} of warehouse ${w} not found`);
        const result = await home.client.mutate({
          op: "update",
          table: "tpcc_customer",
          partitionKey: custRow.c_key,
          values: { c_balance: round2(custRow.c_balance + sumAmount), c_delivery_cnt: custRow.c_delivery_cnt + 1 },
          where: { c_balance: custRow.c_balance, c_delivery_cnt: custRow.c_delivery_cnt },
          requestId: crypto.randomUUID(),
        });
        if (result.rowsAffected > 0) break;
        if (attempt >= MUTATION_RETRY_ATTEMPTS) {
          throw new Error(`customer ${orderRow.c_id} delivery credit did not apply after ${MUTATION_RETRY_ATTEMPTS} attempts -- persistent contention`);
        }
      }
    } catch (err) {
      await home.client
        .mutate({ op: "insert", table: "tpcc_new_order", partitionKey: oldest.no_key, values: { w_id: w, d_id: d, o_id }, requestId: crypto.randomUUID() })
        .catch(() => {
          // Best-effort restoration -- surfacing the ORIGINAL error below
          // matters more than this recovery attempt's own outcome.
        });
      throw err;
    }

    // Deliberately OUTSIDE the try/catch above: the customer is already
    // credited at this point, so a failure here must NOT restore the
    // marker (that would let a future Delivery pass re-credit the same
    // order). A failure marking lines delivered is recorded as this
    // Delivery attempt's own failure, but the order itself is done --
    // narrower and more benign than losing the credit-vs-no-credit
    // distinction would be.
    await deliverLines(lines);

    // Marking each line delivered is done non-transactionally, one
    // /v1/mutate per line, deliberately OUTSIDE the customer-credit update
    // above: these rows are already committed by the time we get here,
    // there's no cross-row invariant requiring them to land atomically with
    // it (or with each other).
    async function deliverLines(lines) {
      if (lines.length === 0) return;
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
 * still matches the spec -- Codex review P2 fix (round 1): an earlier
 * version table-scanned the whole warehouse's stock table regardless of
 * district or recent-order scoping, which reports a materially different
 * (and inflated/irrelevant) number than real Stock-Level measures.
 *
 * Codex review P2 fix (round 10): the version that replaced round 1's fix
 * used idx_orders_by_district with limit:100 and took the tail
 * (orderRows.slice(-20)), reasoning it was the same "limit:100, take the
 * tail" residual as Order-Status. It isn't -- index-query has NO cursor and
 * hard-caps at 100 (MAX_INDEX_QUERY_LIMIT in src/index.ts), and it always
 * returns the FIRST 100 matches in ascending o_id order, never the last.
 * Once a district has more than 100 orders (the default --seed-orders
 * baseline of 100 customers already produces exactly 100 pre-existing
 * orders per district before the benchmark places a single new one), the
 * slice(-20) tail is stuck on orders 81-100 forever -- every subsequent
 * benchmark-created order (o_id 101+) is permanently excluded, not just
 * occasionally missed. Order-Status's limit:100 residual is genuinely rare
 * (a customer needs >100 lifetime orders) and self-correcting once it does
 * happen (index-query's ordering means the visible window still SLIDES
 * forward as older entries fall out); this one is neither.
 *
 * Fixed by using table-scan instead, which DOES support a cursor
 * (nextCursor / cursor), to walk tpcc_orders directly. Every order key is
 * `o-{w}-{d}-{o_id}`, all zero-padded (see keys.mjs) -- ascending
 * partition-key order therefore groups ALL of one district's orders into a
 * single contiguous block, ordered by o_id, strictly before the next
 * district's block. That means finding district d's last 20 orders only
 * ever requires scanning up through the END of district d's own block, not
 * the whole orders table: skip rows before it, collect a sliding window of
 * the last 20 rows within it, and stop as soon as a row from a LATER
 * district appears (or the scan itself ends). */
async function stockLevel(world) {
  const home = world.randomWarehouse();
  const d = world.randomDistrictId();
  const threshold = randInt(10, 20);

  const recentOrders = [];
  let cursor;
  scanLoop: for (;;) {
    const page = await home.client.tableScan("tpcc_orders", 200, cursor);
    for (const row of page.rows || []) {
      if (row.d_id < d) continue;
      if (row.d_id > d) break scanLoop; // past district d's contiguous block -- done
      recentOrders.push(row);
      if (recentOrders.length > 20) recentOrders.shift(); // keep only the most recent 20 seen so far
    }
    if (!page.nextCursor) break;
    cursor = page.nextCursor;
  }
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
