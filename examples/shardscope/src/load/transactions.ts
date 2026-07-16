/** transactions.ts — TypeScript port of the TPC-C-style transaction mix from
 * examples/tpc-c-benchmark/src/transactions.mjs (the Node reference harness
 * for CloudflareShard's own /v1/mutate, /v1/tx, /v1/index-query, and
 * /v1/table-scan primitives). This file ports the LOGIC (the weighted mix,
 * the call sequence + control flow for each of the 5 TPC-C transaction
 * types, and — deliberately preserved, not simplified — the documented
 * adaptations transactions.mjs makes for CloudflareShard's constraints:
 *   1. New-Order is split into a header tx (district + orders) followed by
 *      per-line fragments (order_line + stock, same-warehouse lines as one
 *      2-participant tx; the ~1% cross-warehouse lines as two independent
 *      mutate calls), with the tpcc_new_order MARKER inserted LAST — only
 *      once every line has actually committed — so a concurrent Delivery
 *      pass can never observe a still-mid-flight order. A failed line
 *      compensates (reverses) every already-committed sibling line plus the
 *      orphaned header row, exactly like transactions.mjs's
 *      compensateFailedOrder.
 *   2. Payment updates warehouse/district/customer via separate
 *      /v1/mutate calls with a bounded compare-and-swap retry loop each
 *      (not one bundled /v1/tx) — because /v1/tx doesn't report
 *      per-mutation rowsAffected, so a losing compare-and-swap guard inside
 *      a tx would be silently invisible on the single hottest row in the
 *      whole mix (every Payment in a warehouse touches that warehouse's row).
 *   3. Delivery claims the tpcc_new_order marker via its own /v1/mutate
 *      delete (checked for rowsAffected) BEFORE touching anything else, so a
 *      second concurrent Delivery worker racing the same order sees
 *      rowsAffected: 0 and skips it instead of double-crediting the
 *      customer. On any failure after the claim, the marker is restored so a
 *      later Delivery pass can retry from scratch.
 *   4. Order-Status and Payment's by-name lookup variants are dropped
 *      (ID-only) — no substring/text-search capability exists here.
 *   5. Stock-Level replaces the server-side aggregate with index-query +
 *      client-side counting, scoped to the district's last 20 orders' items.
 *
 * What is NOT ported: transactions.mjs's own HTTP client (client.mjs) and
 * its seeded-reference-data "world" cache (world.mjs) — those exist to talk
 * to a real cluster from Node and to reuse per-warehouse bearer tokens
 * fetched once at seed time. This Worker-native driver has neither a Node
 * filesystem nor that seeding step, so both are replaced with seams instead:
 *   - `TxExecutor` (below) stands in for client.mjs's TenantClient — the
 *     actual HTTP + auth mechanics live in ./gateway-client.ts (via
 *     ./token-provider.ts), NOT in this file. This file only describes WHAT
 *     to call and in WHAT order.
 *   - `KeyPicker` (below) stands in for world.mjs's random ID pickers
 *     (randomWarehouse/randomDistrictId/randomCustomerId/randomItemId).
 *     `UniformKeyPicker` is a direct port of those. `pickItemId` additionally
 *     takes the supplying warehouse id — the one ID-selection point in the
 *     whole mix that determines which `tpcc_stock` partition key (and
 *     therefore which vBucket/shard) a New-Order line's stock write lands
 *     on. ./load-driver.ts's skew mode substitutes a KeyPicker whose
 *     pickItemId is backed by ./skew.ts instead of uniform randomness — see
 *     that file for why this is the deliberate hot-shard lever.
 *   - Real seeded item prices aren't available without a client-side
 *     reference-data cache this Worker doesn't build; `TpccWorldConfig.itemPrice`
 *     is a pluggable stand-in (default: a cheap deterministic synthetic
 *     price per item id) that only affects the stored ol_amount figure, never
 *     routing or transactional correctness.
 */

// ----------------------------------------------------------------------------
// Partition-key helpers — ported from examples/tpc-c-benchmark/src/keys.mjs.
// Zero-padding matters: /v1/table-scan and /v1/index-query both return rows
// ordered by partition key as a lexicographic TEXT sort (SQLite's default
// BINARY collation), not a numeric sort, so every numeric ID embedded in a
// key here must stay zero-padded to the exact same fixed width keys.mjs uses,
// or Delivery's "lowest o_id" / Order-Status's "highest o_id" logic (both
// dependent on ascending partition-key order) would silently corrupt.
// ----------------------------------------------------------------------------

function pad(n: number, width: number): string {
  return String(n).padStart(width, "0");
}

/** Tenant ID for a warehouse — one tenant per warehouse (see design doc).
 * Exported: ./gateway-client.ts and ./load-driver.ts both need this to map a
 * warehouseId onto the tenantId CloudflareShard's routing actually hashes on
 * (`hashKey(tenantId) % catalogShardCount`, and
 * `hashKey(\`${tenantId}:${table}:${partitionKey}\`) % totalVBuckets`). */
export function tenantIdForWarehouse(w: number): string {
  return `tpcc-w${pad(w, 4)}`;
}

function orderKey(w: number, d: number, o: number): string {
  return `o-${pad(w, 4)}-${pad(d, 2)}-${pad(o, 9)}`;
}

function orderLineKey(w: number, d: number, o: number, l: number): string {
  return `ol-${pad(w, 4)}-${pad(d, 2)}-${pad(o, 9)}-${pad(l, 2)}`;
}

/** Stock partition key. Exported: this is the exact key format
 * ./load-driver.ts's skew wiring scans over (via ./skew.ts's
 * candidateToKey) to find item ids whose stock row hashes into a target
 * shard's vBuckets — must stay byte-identical to what processOrderLine below
 * actually writes to, or a "skewed" item id wouldn't really land where the
 * skew driver computed it would. */
export function stockKey(w: number, i: number): string {
  return `s-${pad(w, 4)}-${pad(i, 6)}`;
}

function newOrderKey(w: number, d: number, o: number): string {
  return `no-${pad(w, 4)}-${pad(d, 2)}-${pad(o, 9)}`;
}

/** History rows have no natural composite id and are never looked up by key
 * (only ever inserted) — a random UUID avoids inventing a fake ordering,
 * matching keys.mjs's historyKey(). */
function historyKey(): string {
  return crypto.randomUUID();
}

// ----------------------------------------------------------------------------
// Small numeric helpers — ported from transactions.mjs.
// ----------------------------------------------------------------------------

/** Pluggable randomness source so the weighted picker + ID pickers are
 * deterministically testable (see the vitest suite). Defaults to
 * Math.random everywhere it's used at the top level. */
export type Rng = () => number;

function randInt(rng: Rng, min: number, max: number): number {
  return min + Math.floor(rng() * (max - min + 1));
}

function randomAmount(rng: Rng, min: number, max: number): number {
  return Math.round((min + rng() * (max - min)) * 100) / 100;
}

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}

/** Runs `fn` over `items` with at most `limit` calls in flight at once,
 * preserving input order in the result array. Direct port of
 * transactions.mjs/client.mjs's runPool. */
async function runPool<T, R>(items: T[], limit: number, fn: (item: T, index: number) => Promise<R>): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  async function worker(): Promise<void> {
    for (;;) {
      const i = next++;
      if (i >= items.length) return;
      results[i] = await fn(items[i], i);
    }
  }
  const workers = Array.from({ length: Math.min(limit, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

// ----------------------------------------------------------------------------
// TxExecutor — the seam that replaces transactions.mjs's TenantClient.
// ----------------------------------------------------------------------------

export type MutateOp = "insert" | "update" | "upsert" | "delete";

export interface MutateCall {
  op: MutateOp;
  table: string;
  partitionKey: string;
  values?: Record<string, unknown>;
  where?: Record<string, unknown>;
  requestId?: string;
}

export interface MutateResult {
  rowsAffected?: number;
  [key: string]: unknown;
}

export interface QueryResult {
  rows?: Array<Record<string, unknown>>;
  cursor?: unknown;
  [key: string]: unknown;
}

/** Issues the four CloudflareShard tenant-data-plane primitives on behalf of
 * one warehouse's tenant. Implementations own auth (resolving a bearer token
 * via a TokenProvider, see ./token-provider.ts) and the actual network call
 * (see ./gateway-client.ts's HttpTxExecutor) — this file only decides WHAT
 * to call and in WHAT order, driven by the same read-then-write control flow
 * the real TPC-C spec (and transactions.mjs) requires. Every method is
 * expected to THROW on any non-success outcome (mirrors client.mjs's
 * ApiError-throwing `post()`), never to return a sentinel failure value —
 * the retry/compensation logic below depends on that. */
export interface TxExecutor {
  mutate(warehouseId: number, call: MutateCall): Promise<MutateResult>;
  tx(warehouseId: number, mutations: MutateCall[], requestId?: string): Promise<{ committed?: boolean; [k: string]: unknown }>;
  indexQuery(warehouseId: number, table: string, indexName: string, values: Record<string, unknown>, limit?: number): Promise<QueryResult>;
  tableScan(warehouseId: number, table: string, limit: number, cursor?: unknown): Promise<QueryResult>;
}

// ----------------------------------------------------------------------------
// World config + KeyPicker — replaces world.mjs's loaded reference data.
// ----------------------------------------------------------------------------

export interface TpccWorldConfig {
  warehouseIds: number[];
  districtsPerWarehouse: number;
  customersPerDistrict: number;
  itemCount: number;
  /** Stand-in for world.mjs's client-side item-price cache (built from real
   * seeded reference data this Worker never loads). Only affects the stored
   * ol_amount figure on an order_line row — never routing or any
   * transaction's correctness. Default: a cheap deterministic synthetic
   * price, stable per item id (not re-randomized per call). */
  itemPrice?: (i_id: number) => number;
}

function defaultItemPrice(i_id: number): number {
  // Deterministic, bounded to a plausible ~$1.00-$100.00 range — the exact
  // value doesn't matter (see TpccWorldConfig.itemPrice's doc comment above).
  return round2(1 + ((i_id * 37) % 9973) / 100);
}

/** Replaces world.mjs's randomWarehouse()/randomDistrictId()/
 * randomCustomerId()/randomItemId(). `pickItemId` additionally takes the
 * supplying warehouse id — see this file's header comment for why that's the
 * hook ./load-driver.ts's skew mode uses. */
export interface KeyPicker {
  pickWarehouseId(rng: Rng): number;
  pickDistrictId(rng: Rng): number;
  pickCustomerId(rng: Rng): number;
  pickItemId(rng: Rng, supplyWarehouseId: number): number;
}

/** Direct port of world.mjs's random pickers — every ID sampled uniformly at
 * random over its configured range, with replacement. This is "vanilla
 * TPC-C": exactly what produces the "hashes across vBuckets, may never
 * produce a clear hot shard" behavior this project's skew driver exists to
 * counteract (see ./skew.ts). */
export class UniformKeyPicker implements KeyPicker {
  constructor(private readonly cfg: TpccWorldConfig) {}

  pickWarehouseId(rng: Rng): number {
    return this.cfg.warehouseIds[Math.floor(rng() * this.cfg.warehouseIds.length)];
  }

  pickDistrictId(rng: Rng): number {
    return 1 + Math.floor(rng() * this.cfg.districtsPerWarehouse);
  }

  pickCustomerId(rng: Rng): number {
    return 1 + Math.floor(rng() * this.cfg.customersPerDistrict);
  }

  pickItemId(rng: Rng, _supplyWarehouseId: number): number {
    return 1 + Math.floor(rng() * this.cfg.itemCount);
  }
}

const MUTATION_RETRY_ATTEMPTS = 5;

// ----------------------------------------------------------------------------
// New-Order — weight 45%.
// ----------------------------------------------------------------------------

interface DistrictRow {
  d_key: string;
  d_id: number;
  d_next_o_id: number;
}

interface LineCompensation {
  orderLineWarehouseId: number;
  stockWarehouseId: number;
  olKey: string;
  stockKey: string;
  originalStock: Record<string, unknown>;
  appliedStock: Record<string, unknown>;
}

interface OrderLinePlan {
  ol_number: number;
  i_id: number;
  supplyWarehouseId: number;
  qty: number;
  remote: boolean;
}

async function newOrder(exec: TxExecutor, cfg: TpccWorldConfig, picker: KeyPicker, rng: Rng): Promise<void> {
  const itemPrice = cfg.itemPrice ?? defaultItemPrice;
  const w = picker.pickWarehouseId(rng);
  const d = picker.pickDistrictId(rng);
  const c_id = picker.pickCustomerId(rng);

  const lineCount = randInt(rng, 5, 15);
  const lines: OrderLinePlan[] = [];
  // Codex review P2 fix (ported): no two lines in ONE order may reference the
  // same (supplyWarehouseId, i_id) stock row — a same-warehouse compare-and-
  // swap collision inside one order is silently unrecoverable (/v1/tx
  // reports "committed" with no per-mutation rowsAffected). Bounded retry;
  // falls back to accepting a duplicate only if itemCount is unrealistically
  // small relative to lineCount.
  const usedSupplyItemKeys = new Set<string>();
  for (let l = 1; l <= lineCount; l++) {
    let supplyWarehouseId = w;
    if (cfg.warehouseIds.length > 1 && rng() < 0.01) {
      do {
        supplyWarehouseId = cfg.warehouseIds[randInt(rng, 0, cfg.warehouseIds.length - 1)];
      } while (supplyWarehouseId === w);
    }
    let i_id = 0;
    let key = "";
    let attempts = 0;
    do {
      i_id = picker.pickItemId(rng, supplyWarehouseId);
      key = `${supplyWarehouseId}-${i_id}`;
      attempts++;
    } while (usedSupplyItemKeys.has(key) && attempts < 20);
    usedSupplyItemKeys.add(key);
    lines.push({ ol_number: l, i_id, supplyWarehouseId, qty: randInt(rng, 1, 10), remote: supplyWarehouseId !== w });
  }

  // d_next_o_id mutates on every New-Order, so (like the .mjs reference) it's
  // always read fresh via a table-scan of the small per-warehouse district
  // table rather than any cache.
  const districtScan = await exec.tableScan(w, "tpcc_district", cfg.districtsPerWarehouse);
  const districtRow = ((districtScan.rows ?? []) as unknown as DistrictRow[]).find((r) => r.d_id === d);
  if (!districtRow) throw new Error(`district ${d} not found in warehouse ${w}'s district table-scan`);
  const o_id = districtRow.d_next_o_id;
  const entryDate = new Date().toISOString();

  // Header tx: district next-o-id bump + orders insert. The tpcc_new_order
  // MARKER is deliberately NOT included here — see this file's header
  // comment (adaptation #1) for why it's inserted last, only after every
  // line has committed.
  await exec.tx(
    w,
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

  // Every line is awaited to completion (success or caught failure) before
  // deciding whether to insert the marker — see header comment; this is what
  // stops a marker from being skipped while a sibling line is still
  // in-flight in the background.
  const lineErrors: Array<{ ol_number: number; error: string }> = [];
  const succeededLines: LineCompensation[] = [];
  await runPool(lines, Math.min(4, lines.length), async (line) => {
    try {
      succeededLines.push(await processOrderLine(exec, w, d, o_id, itemPrice, line));
    } catch (err) {
      lineErrors.push({ ol_number: line.ol_number, error: err instanceof Error ? err.message : String(err) });
    }
  });

  if (lineErrors.length > 0) {
    await compensateFailedOrder(exec, w, d, o_id, succeededLines);
    throw new Error(
      `New-Order failed on ${lineErrors.length}/${lines.length} line(s) (compensated ${succeededLines.length} already-committed line(s)): ${lineErrors
        .map((e) => `line ${e.ol_number}: ${e.error}`)
        .join("; ")}`,
    );
  }

  // Only now — after every line has actually committed — does this order
  // become visible to Delivery.
  await exec.mutate(w, {
    op: "insert",
    table: "tpcc_new_order",
    partitionKey: newOrderKey(w, d, o_id),
    values: { w_id: w, d_id: d, o_id },
    requestId: crypto.randomUUID(),
  });
}

/** Direct port of transactions.mjs's compensateFailedOrder — reverses
 * already-committed lines (each stock reversal is itself a compare-and-swap
 * against exactly the values that line's own update applied, so it safely
 * no-ops rather than corrupts if some OTHER write touched the row again
 * meanwhile) plus the orphaned orders header row. d_next_o_id is
 * deliberately NOT decremented — see transactions.mjs's comment on this same
 * function for why the resulting sequence gap is expected and harmless. */
async function compensateFailedOrder(exec: TxExecutor, w: number, d: number, o_id: number, succeeded: LineCompensation[]): Promise<void> {
  for (const line of succeeded) {
    try {
      await exec.mutate(line.orderLineWarehouseId, { op: "delete", table: "tpcc_order_line", partitionKey: line.olKey, requestId: crypto.randomUUID() });
      await exec.mutate(line.stockWarehouseId, {
        op: "update",
        table: "tpcc_stock",
        partitionKey: line.stockKey,
        values: line.originalStock,
        where: line.appliedStock,
        requestId: crypto.randomUUID(),
      });
    } catch {
      // Best-effort — keep compensating the rest rather than aborting the
      // whole rollback over one failed reversal.
    }
  }
  await exec.mutate(w, { op: "delete", table: "tpcc_orders", partitionKey: orderKey(w, d, o_id), requestId: crypto.randomUUID() }).catch(() => {
    // Best-effort, same reasoning.
  });
}

interface StockRow {
  s_key: string;
  s_quantity: number;
  s_ytd: number;
  s_order_cnt: number;
  s_remote_cnt: number;
}

async function processOrderLine(
  exec: TxExecutor,
  w: number,
  d: number,
  o_id: number,
  itemPrice: (i_id: number) => number,
  line: OrderLinePlan,
): Promise<LineCompensation> {
  const stockRes = await exec.indexQuery(line.supplyWarehouseId, "tpcc_stock", "idx_stock_by_item", { i_id: line.i_id });
  const stockRow = ((stockRes.rows ?? [])[0] as unknown as StockRow) ?? undefined;
  if (!stockRow) throw new Error(`no stock row for item ${line.i_id} in warehouse ${line.supplyWarehouseId}`);

  let newQty = stockRow.s_quantity - line.qty;
  if (newQty < 10) newQty += 91;
  const stockValues = {
    s_quantity: newQty,
    s_ytd: stockRow.s_ytd + line.qty,
    s_order_cnt: stockRow.s_order_cnt + 1,
    s_remote_cnt: stockRow.s_remote_cnt + (line.remote ? 1 : 0),
  };
  // Compare-and-swap guard against the exact row this was computed from —
  // see transactions.mjs's longer comment on this same pattern for why
  // (no server-side arithmetic UPDATE exists here; this is the only way to
  // avoid a lost-update race on a popular stock row).
  const stockWhere = {
    s_quantity: stockRow.s_quantity,
    s_ytd: stockRow.s_ytd,
    s_order_cnt: stockRow.s_order_cnt,
    s_remote_cnt: stockRow.s_remote_cnt,
  };
  const ol_amount = round2(itemPrice(line.i_id) * line.qty);
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

  const compensation: LineCompensation = {
    orderLineWarehouseId: w,
    stockWarehouseId: line.supplyWarehouseId,
    olKey,
    stockKey: stockRow.s_key,
    originalStock: stockWhere,
    appliedStock: stockValues,
  };

  if (!line.remote) {
    // Same-warehouse line: order_line insert + stock update as one
    // 2-participant tx — full atomicity, both mutations share this
    // warehouse's tenantId.
    await exec.tx(
      w,
      [
        { op: "insert", table: "tpcc_order_line", partitionKey: olKey, values: orderLineValues },
        { op: "update", table: "tpcc_stock", partitionKey: stockRow.s_key, values: stockValues, where: stockWhere },
      ],
      crypto.randomUUID(),
    );
  } else {
    // Remote line (~1%): order_line belongs to the ORDERING warehouse's
    // tenant, the stock update to the SUPPLY warehouse's — /v1/tx requires
    // one shared tenantId per call, so this pair can't be one atomic tx
    // across two tenants. Falls back to two independent /v1/mutate calls.
    await exec.mutate(w, { op: "insert", table: "tpcc_order_line", partitionKey: olKey, values: orderLineValues, requestId: crypto.randomUUID() });
    try {
      const stockMutateResult = await exec.mutate(line.supplyWarehouseId, {
        op: "update",
        table: "tpcc_stock",
        partitionKey: stockRow.s_key,
        values: stockValues,
        where: stockWhere,
        requestId: crypto.randomUUID(),
      });
      if (stockMutateResult.rowsAffected === 0) {
        throw new Error(`stock row ${stockRow.s_key} changed concurrently — remote line's stock update did not apply`);
      }
    } catch (err) {
      // The order_line insert above already committed; compensate it right
      // here (this line never reaches succeededLines since it's about to
      // throw), rather than relying on the outer mechanism to know about a
      // partial success it was never told about.
      await exec.mutate(w, { op: "delete", table: "tpcc_order_line", partitionKey: olKey, requestId: crypto.randomUUID() }).catch(() => {
        // Best-effort — surfacing the ORIGINAL error matters more.
      });
      throw err;
    }
  }
  return compensation;
}

// ----------------------------------------------------------------------------
// Payment — weight 43%.
// ----------------------------------------------------------------------------

interface WarehouseRow {
  wh_key: string;
  w_ytd: number;
  w_name?: string;
}

interface CustomerRow {
  c_key: string;
  c_balance: number;
  c_ytd_payment: number;
  c_payment_cnt: number;
}

async function payment(exec: TxExecutor, cfg: TpccWorldConfig, picker: KeyPicker, rng: Rng): Promise<void> {
  const w = picker.pickWarehouseId(rng);
  const d = picker.pickDistrictId(rng);
  const c_id = picker.pickCustomerId(rng);
  const amount = randomAmount(rng, 1.0, 5000.0);

  let whRow: WarehouseRow | undefined;
  let distRow: DistrictRow | undefined;

  for (let attempt = 1; ; attempt++) {
    const whScan = await exec.tableScan(w, "tpcc_warehouse", 1);
    whRow = (whScan.rows ?? [])[0] as unknown as WarehouseRow | undefined;
    if (!whRow) throw new Error(`warehouse ${w} row missing`);
    const result = await exec.mutate(w, {
      op: "update",
      table: "tpcc_warehouse",
      partitionKey: whRow.wh_key,
      values: { w_ytd: round2(whRow.w_ytd + amount) },
      where: { w_ytd: whRow.w_ytd },
      requestId: crypto.randomUUID(),
    });
    if ((result.rowsAffected ?? 0) > 0) break;
    if (attempt >= MUTATION_RETRY_ATTEMPTS) {
      throw new Error(`warehouse ${w} w_ytd update did not apply after ${MUTATION_RETRY_ATTEMPTS} attempts — persistent contention`);
    }
  }

  for (let attempt = 1; ; attempt++) {
    const distScan = await exec.tableScan(w, "tpcc_district", cfg.districtsPerWarehouse);
    distRow = ((distScan.rows ?? []) as unknown as DistrictRow[]).find((r) => r.d_id === d);
    if (!distRow) throw new Error(`district ${d} not found in warehouse ${w}`);
    const result = await exec.mutate(w, {
      op: "update",
      table: "tpcc_district",
      partitionKey: distRow.d_key,
      values: { d_ytd: round2(((distRow as unknown as { d_ytd?: number }).d_ytd ?? 0) + amount) },
      where: { d_ytd: (distRow as unknown as { d_ytd?: number }).d_ytd ?? 0 },
      requestId: crypto.randomUUID(),
    });
    if ((result.rowsAffected ?? 0) > 0) break;
    if (attempt >= MUTATION_RETRY_ATTEMPTS) {
      throw new Error(`district ${d} d_ytd update did not apply after ${MUTATION_RETRY_ATTEMPTS} attempts — persistent contention`);
    }
  }

  for (let attempt = 1; ; attempt++) {
    const custRes = await exec.indexQuery(w, "tpcc_customer", "idx_customer_by_id", { d_id: d, c_id });
    const custRow = (custRes.rows ?? [])[0] as unknown as CustomerRow | undefined;
    if (!custRow) throw new Error(`customer ${c_id} in district ${d} of warehouse ${w} not found`);
    const result = await exec.mutate(w, {
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
    if ((result.rowsAffected ?? 0) > 0) break;
    if (attempt >= MUTATION_RETRY_ATTEMPTS) {
      throw new Error(`customer ${c_id} balance update did not apply after ${MUTATION_RETRY_ATTEMPTS} attempts — persistent contention`);
    }
  }

  // Unconditional side effect, only recorded once all three updates above
  // are confirmed applied.
  await exec.mutate(w, {
    op: "insert",
    table: "tpcc_history",
    partitionKey: historyKey(),
    values: {
      w_id: w,
      d_id: d,
      c_id,
      h_amount: amount,
      h_date: new Date().toISOString(),
      h_data: `${whRow.w_name ?? ""}`.trim(),
    },
    requestId: crypto.randomUUID(),
  });
}

// ----------------------------------------------------------------------------
// Order-Status — weight 4%, read-only. ID-only lookup (see header comment).
// ----------------------------------------------------------------------------

interface OrderRow {
  o_id: number;
}

async function orderStatus(exec: TxExecutor, _cfg: TpccWorldConfig, picker: KeyPicker, rng: Rng): Promise<void> {
  const w = picker.pickWarehouseId(rng);
  const d = picker.pickDistrictId(rng);
  const c_id = picker.pickCustomerId(rng);

  const custRes = await exec.indexQuery(w, "tpcc_customer", "idx_customer_by_id", { d_id: d, c_id });
  if ((custRes.rows ?? []).length === 0) throw new Error(`customer ${c_id} in district ${d} not found`);

  // limit:100 is /v1/index-query's own hard server-side cap; a customer with
  // more than 100 accumulated orders would still hit this exact truncation
  // in the real .mjs harness too — an accepted, documented residual limit,
  // not something this port tries to fix.
  const ordersRes = await exec.indexQuery(w, "tpcc_orders", "idx_orders_by_customer", { d_id: d, c_id }, 100);
  const orderRows = (ordersRes.rows ?? []) as unknown as OrderRow[];
  if (orderRows.length === 0) return; // no orders yet — legitimate, not an error

  const latestOrder = orderRows[orderRows.length - 1];
  await exec.indexQuery(w, "tpcc_order_line", "idx_order_line_by_order", { d_id: d, o_id: latestOrder.o_id }, 15);
}

// ----------------------------------------------------------------------------
// Delivery — weight 4%. Processes the oldest undelivered order in EVERY
// district of the chosen warehouse, matching real TPC-C's batch-style
// Delivery.
// ----------------------------------------------------------------------------

interface NewOrderRow {
  no_key: string;
  o_id: number;
}

interface DeliveryOrderRow {
  o_key: string;
  c_id: number;
}

interface OrderLineRow {
  ol_key: string;
  ol_amount?: number;
}

async function delivery(exec: TxExecutor, cfg: TpccWorldConfig, picker: KeyPicker, rng: Rng): Promise<void> {
  const w = picker.pickWarehouseId(rng);

  for (let d = 1; d <= cfg.districtsPerWarehouse; d++) {
    const noRes = await exec.indexQuery(w, "tpcc_new_order", "idx_new_order_by_district", { d_id: d }, 100);
    const noRows = (noRes.rows ?? []) as unknown as NewOrderRow[];
    if (noRows.length === 0) continue; // nothing outstanding in this district

    const oldest = noRows[0];
    const o_id = oldest.o_id;

    // CLAIM the marker first — the actual concurrency-safe fence. A second
    // Delivery worker racing the same "oldest" row sees rowsAffected: 0 here
    // and correctly skips it instead of double-crediting the customer. See
    // header comment (adaptation #3).
    const claimResult = await exec.mutate(w, { op: "delete", table: "tpcc_new_order", partitionKey: oldest.no_key, requestId: crypto.randomUUID() });
    if ((claimResult.rowsAffected ?? 0) === 0) continue; // already claimed by a concurrent worker

    try {
      const orderRes = await exec.indexQuery(w, "tpcc_orders", "idx_orders_by_id", { d_id: d, o_id });
      const orderRow = (orderRes.rows ?? [])[0] as unknown as DeliveryOrderRow | undefined;
      if (!orderRow) throw new Error(`order ${o_id} in district ${d} of warehouse ${w} not found despite a new_order row`);

      await exec.mutate(w, {
        op: "update",
        table: "tpcc_orders",
        partitionKey: orderRow.o_key,
        values: { o_carrier_id: randInt(rng, 1, 10) },
        requestId: crypto.randomUUID(),
      });

      const linesRes = await exec.indexQuery(w, "tpcc_order_line", "idx_order_line_by_order", { d_id: d, o_id }, 15);
      const lines = (linesRes.rows ?? []) as unknown as OrderLineRow[];
      const sumAmount = round2(lines.reduce((acc, l) => acc + (l.ol_amount || 0), 0));

      for (let attempt = 1; ; attempt++) {
        const custRes = await exec.indexQuery(w, "tpcc_customer", "idx_customer_by_id", { d_id: d, c_id: orderRow.c_id });
        const custRow = (custRes.rows ?? [])[0] as unknown as (CustomerRow & { c_delivery_cnt: number }) | undefined;
        if (!custRow) throw new Error(`customer ${orderRow.c_id} in district ${d} of warehouse ${w} not found`);
        const result = await exec.mutate(w, {
          op: "update",
          table: "tpcc_customer",
          partitionKey: custRow.c_key,
          values: { c_balance: round2(custRow.c_balance + sumAmount), c_delivery_cnt: custRow.c_delivery_cnt + 1 },
          where: { c_balance: custRow.c_balance, c_delivery_cnt: custRow.c_delivery_cnt },
          requestId: crypto.randomUUID(),
        });
        if ((result.rowsAffected ?? 0) > 0) break;
        if (attempt >= MUTATION_RETRY_ATTEMPTS) {
          throw new Error(`customer ${orderRow.c_id} delivery credit did not apply after ${MUTATION_RETRY_ATTEMPTS} attempts — persistent contention`);
        }
      }

      if (lines.length > 0) {
        const deliveredAt = new Date().toISOString();
        await runPool(lines, Math.min(4, lines.length), (line) =>
          exec.mutate(w, { op: "update", table: "tpcc_order_line", partitionKey: line.ol_key, values: { ol_delivery_d: deliveredAt }, requestId: crypto.randomUUID() }),
        );
      }
    } catch (err) {
      // Restore the marker on any failure after the claim — otherwise the
      // order would be permanently invisible to every future Delivery pass.
      await exec.mutate(w, { op: "insert", table: "tpcc_new_order", partitionKey: oldest.no_key, values: { w_id: w, d_id: d, o_id }, requestId: crypto.randomUUID() }).catch(() => {
        // Best-effort — surfacing the ORIGINAL error matters more.
      });
      throw err;
    }
  }
}

// ----------------------------------------------------------------------------
// Stock-Level — weight 4%, read-only.
// ----------------------------------------------------------------------------

interface StockLevelOrderRow {
  o_id: number;
}

async function stockLevel(exec: TxExecutor, _cfg: TpccWorldConfig, picker: KeyPicker, rng: Rng): Promise<void> {
  const w = picker.pickWarehouseId(rng);
  const d = picker.pickDistrictId(rng);
  const threshold = randInt(rng, 10, 20);

  const ordersRes = await exec.indexQuery(w, "tpcc_orders", "idx_orders_by_district", { d_id: d }, 100);
  const orderRows = (ordersRes.rows ?? []) as unknown as StockLevelOrderRow[];
  const recentOrders = orderRows.slice(-20);
  if (recentOrders.length === 0) return;

  const itemIds = new Set<number>();
  for (const order of recentOrders) {
    const linesRes = await exec.indexQuery(w, "tpcc_order_line", "idx_order_line_by_order", { d_id: d, o_id: order.o_id }, 15);
    for (const line of (linesRes.rows ?? []) as unknown as Array<{ ol_i_id: number }>) itemIds.add(line.ol_i_id);
  }
  if (itemIds.size === 0) return;

  await runPool([...itemIds], Math.min(8, itemIds.size), async (i_id) => {
    const stockRes = await exec.indexQuery(w, "tpcc_stock", "idx_stock_by_item", { i_id });
    const stockRow = (stockRes.rows ?? [])[0] as unknown as { s_quantity: number } | undefined;
    return stockRow ? stockRow.s_quantity < threshold : false;
  });
}

// ----------------------------------------------------------------------------
// The weighted mix + picker — direct port of transactions.mjs's
// TRANSACTION_MIX / pickTransaction / runOneTransaction.
// ----------------------------------------------------------------------------

export type TransactionType = "new-order" | "payment" | "order-status" | "delivery" | "stock-level";

export interface TransactionDef {
  type: TransactionType;
  weight: number;
  run: (exec: TxExecutor, cfg: TpccWorldConfig, picker: KeyPicker, rng: Rng) => Promise<void>;
}

/** The standard TPC-C weighted mix: 45% New-Order, 43% Payment, 4% each
 * Order-Status/Delivery/Stock-Level — exported so callers (and tests) can
 * inspect the weights without re-deriving them. */
export const TRANSACTION_MIX: TransactionDef[] = [
  { type: "new-order", weight: 45, run: newOrder },
  { type: "payment", weight: 43, run: payment },
  { type: "order-status", weight: 4, run: orderStatus },
  { type: "delivery", weight: 4, run: delivery },
  { type: "stock-level", weight: 4, run: stockLevel },
];

const TOTAL_WEIGHT = TRANSACTION_MIX.reduce((acc, t) => acc + t.weight, 0);

/** Weighted random pick over TRANSACTION_MIX. Direct port of
 * transactions.mjs's pickTransaction. */
export function pickTransactionType(rng: Rng = Math.random): TransactionDef {
  let r = rng() * TOTAL_WEIGHT;
  for (const t of TRANSACTION_MIX) {
    if (r < t.weight) return t;
    r -= t.weight;
  }
  return TRANSACTION_MIX[TRANSACTION_MIX.length - 1];
}

export interface TransactionOutcome {
  type: TransactionType;
  ok: boolean;
  latencyMs: number;
  error?: string;
}

/** Runs one randomly-chosen transaction (weighted per TRANSACTION_MIX) and
 * returns a {type, ok, latencyMs, error?} record — direct port of
 * transactions.mjs's runOneTransaction. */
export async function runOneTransaction(
  exec: TxExecutor,
  cfg: TpccWorldConfig,
  picker: KeyPicker = new UniformKeyPicker(cfg),
  rng: Rng = Math.random,
): Promise<TransactionOutcome> {
  const chosen = pickTransactionType(rng);
  const start = performance.now();
  try {
    await chosen.run(exec, cfg, picker, rng);
    return { type: chosen.type, ok: true, latencyMs: performance.now() - start };
  } catch (err) {
    return { type: chosen.type, ok: false, latencyMs: performance.now() - start, error: err instanceof Error ? err.message : String(err) };
  }
}
