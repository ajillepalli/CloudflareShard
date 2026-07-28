/** schema-bootstrap.ts — ensures the TPC-C tables exist before "Start the
 * scenario" seeds any data into them, and the secondary indexes exist
 * (and are ready) afterward.
 *
 * WHY THIS EXISTS (Codex review finding on the initial version of this
 * feature): scenario-seed.ts's seedScenarioReferenceData assumed the 9
 * tpcc_* tables and 7 secondary indexes already existed on the cluster —
 * true for a cluster that has already run examples/tpc-c-benchmark's own
 * `generate.mjs seed`, but NOT true for a genuinely fresh, just-`/admin/init`
 * cluster (exactly what a real "Deploy to Cloudflare" visitor's core Worker
 * starts as). Against a fresh cluster, seedScenarioReferenceData's first
 * mutate() 502'd (no such table), so "Start the scenario" could not start at
 * all — directly contradicting this feature's own "fully self-contained, no
 * external setup step" claim.
 *
 * TABLES BEFORE SEED, INDEXES AFTER (Codex review P2 fix, round 2): tables
 * must exist before seeding (an upsert into a nonexistent table 502s), but
 * indexes must NOT be created until AFTER seeding — see
 * ensureScenarioIndexesReady's own doc comment for exactly why (an index
 * created before its rows exist only picks them up via /v1/mutate's
 * asynchronous, best-effort maintenance path, which the seeding call's HTTP
 * response doesn't wait for). load-driver.ts's handleStart therefore calls
 * ensureScenarioTables, then seedScenarioReferenceData, then
 * ensureScenarioIndexesReady, in that exact order — mirroring
 * examples/tpc-c-benchmark/src/generate.mjs's own proven-correct sequence
 * (seed every warehouse, THEN create every index).
 *
 * Table/index definitions here are a deliberate, byte-for-byte duplicate of
 * examples/tpc-c-benchmark/src/schema.mjs's TABLES/INDEXES (see that file's
 * own header comment for the schema rationale) — not imported, because these
 * are two independent, self-contained example apps and this feature has no
 * other reason to depend on the benchmark harness's own module.
 */

export interface TableSchema {
  table: string;
  partitionKeyColumn: string;
  schema: string;
}

export interface IndexSchema {
  table: string;
  indexName: string;
  columns: string[];
}

export const TPCC_TABLES: TableSchema[] = [
  {
    table: "tpcc_warehouse",
    partitionKeyColumn: "wh_key",
    schema: "CREATE TABLE tpcc_warehouse (wh_key TEXT PRIMARY KEY, w_id INTEGER, w_name TEXT, w_tax REAL, w_ytd REAL)",
  },
  {
    table: "tpcc_district",
    partitionKeyColumn: "d_key",
    schema:
      "CREATE TABLE tpcc_district (d_key TEXT PRIMARY KEY, w_id INTEGER, d_id INTEGER, d_name TEXT, d_tax REAL, d_ytd REAL, d_next_o_id INTEGER)",
  },
  {
    table: "tpcc_customer",
    partitionKeyColumn: "c_key",
    schema:
      "CREATE TABLE tpcc_customer (c_key TEXT PRIMARY KEY, w_id INTEGER, d_id INTEGER, c_id INTEGER, c_first TEXT, c_last TEXT, c_credit TEXT, c_discount REAL, c_balance REAL, c_ytd_payment REAL, c_payment_cnt INTEGER, c_delivery_cnt INTEGER)",
  },
  {
    table: "tpcc_history",
    partitionKeyColumn: "h_key",
    schema: "CREATE TABLE tpcc_history (h_key TEXT PRIMARY KEY, w_id INTEGER, d_id INTEGER, c_id INTEGER, h_amount REAL, h_date TEXT, h_data TEXT)",
  },
  {
    table: "tpcc_new_order",
    partitionKeyColumn: "no_key",
    schema: "CREATE TABLE tpcc_new_order (no_key TEXT PRIMARY KEY, w_id INTEGER, d_id INTEGER, o_id INTEGER)",
  },
  {
    table: "tpcc_orders",
    partitionKeyColumn: "o_key",
    schema:
      "CREATE TABLE tpcc_orders (o_key TEXT PRIMARY KEY, w_id INTEGER, d_id INTEGER, o_id INTEGER, c_id INTEGER, o_entry_d TEXT, o_carrier_id INTEGER, o_ol_cnt INTEGER)",
  },
  {
    table: "tpcc_order_line",
    partitionKeyColumn: "ol_key",
    schema:
      "CREATE TABLE tpcc_order_line (ol_key TEXT PRIMARY KEY, w_id INTEGER, d_id INTEGER, o_id INTEGER, ol_number INTEGER, ol_i_id INTEGER, ol_supply_w_id INTEGER, ol_quantity INTEGER, ol_amount REAL, ol_delivery_d TEXT)",
  },
  {
    table: "tpcc_item",
    partitionKeyColumn: "i_key",
    schema: "CREATE TABLE tpcc_item (i_key TEXT PRIMARY KEY, i_id INTEGER, i_name TEXT, i_price REAL, i_data TEXT)",
  },
  {
    table: "tpcc_stock",
    partitionKeyColumn: "s_key",
    schema:
      "CREATE TABLE tpcc_stock (s_key TEXT PRIMARY KEY, w_id INTEGER, i_id INTEGER, s_quantity INTEGER, s_ytd INTEGER, s_order_cnt INTEGER, s_remote_cnt INTEGER, s_data TEXT)",
  },
];

export const TPCC_INDEXES: IndexSchema[] = [
  { table: "tpcc_customer", indexName: "idx_customer_by_id", columns: ["d_id", "c_id"] },
  { table: "tpcc_orders", indexName: "idx_orders_by_customer", columns: ["d_id", "c_id"] },
  { table: "tpcc_orders", indexName: "idx_orders_by_id", columns: ["d_id", "o_id"] },
  { table: "tpcc_orders", indexName: "idx_orders_by_district", columns: ["d_id"] },
  { table: "tpcc_order_line", indexName: "idx_order_line_by_order", columns: ["d_id", "o_id"] },
  { table: "tpcc_new_order", indexName: "idx_new_order_by_district", columns: ["d_id"] },
  { table: "tpcc_stock", indexName: "idx_stock_by_item", columns: ["i_id"] },
];

/** Minimal admin-token-backed client for the 3 endpoints schema bootstrap
 * needs — deliberately separate from ./gateway-client.ts's HttpTxExecutor
 * (tenant-scoped) and HttpSqlPointReader (a single admin-scoped endpoint):
 * this is the only caller in this codebase that needs /admin/list-tables,
 * /admin/create-table, and /admin/create-index together. */
export interface SchemaAdminClient {
  listTables(): Promise<{ tables?: Array<{ table_name?: string }> }>;
  createTable(table: string, schema: string, partitionKeyColumn: string): Promise<unknown>;
  listIndexes(): Promise<{ indexes?: Array<{ indexName?: string; table?: string; status?: string }> }>;
  createIndex(indexName: string, table: string, columns: string[]): Promise<unknown>;
}

// Codex review P2 fix: a trailing-slash baseUrl (e.g. "https://worker.example/"
// — a common, entirely reasonable way to configure CORE_GATEWAY_BASE_URL)
// concatenated directly with a leading-slash path produces a DOUBLE slash
// ("https://worker.example//admin/list-tables"), which the Worker's routes
// (matched on exact pathname) don't recognize — every schema-bootstrap call
// 404s and "Start the scenario" can't start. Mirrors
// examples/tpc-c-benchmark/src/client.mjs's own joinUrl fix for the
// identical issue (see that file's "Codex review round 12 P3 fix" comment).
//
// Codex review P2 fix (round 13): accepts `string | null | undefined`, not
// just `string` — CORE_GATEWAY_BASE_URL is deliberately left unset in the
// committed wrangler.toml (see that file's own P2 fix comment), so
// HttpSchemaAdminClient can genuinely be constructed with `undefined` as its
// baseUrl. The FIRST fix for this (load-driver.ts's own `?? ""` guard on its
// baseUrl-comparison check) only covered that ONE call site — the
// HttpSchemaAdminClient CONSTRUCTION site a few lines later passed the same
// possibly-undefined value straight through, and this function's own
// `.replace()` threw the identical TypeError one call later. Guarding HERE,
// at the one place every caller's baseUrl ultimately flows through, closes
// the whole class of "missed another call site" bug rather than relying on
// every future caller to remember `?? ""` individually. An empty/missing
// baseUrl still fails — just at the network layer (an unparseable/relative
// fetch URL), a clear, obvious failure surfaced as a 502 bootstrap error,
// never an unhandled TypeError.
function joinUrl(baseUrl: string | null | undefined, path: string): string {
  return `${(baseUrl ?? "").replace(/\/+$/, "")}${path}`;
}

export class HttpSchemaAdminClient implements SchemaAdminClient {
  constructor(
    private readonly baseUrl: string,
    private readonly adminToken: string,
  ) {}

  private async post<T>(path: string, body: unknown): Promise<T> {
    const res = await fetch(joinUrl(this.baseUrl, path), {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${this.adminToken}` },
      body: JSON.stringify(body ?? {}),
    });
    const text = await res.text();
    let json: unknown;
    try {
      json = text ? JSON.parse(text) : {};
    } catch {
      json = { raw: text };
    }
    if (!res.ok) {
      const err = (json as { error?: { code?: string; message?: string } })?.error;
      throw new Error(`POST ${path} -> ${res.status}${err?.code ? ` ${err.code}` : ""}${err?.message ? `: ${err.message}` : ""}`);
    }
    return json as T;
  }

  listTables(): Promise<{ tables?: Array<{ table_name?: string }> }> {
    return this.post("/admin/list-tables", {});
  }

  createTable(table: string, schema: string, partitionKeyColumn: string): Promise<unknown> {
    return this.post("/admin/create-table", { table, schema, partitionKeyColumn });
  }

  listIndexes(): Promise<{ indexes?: Array<{ indexName?: string; table?: string; status?: string }> }> {
    return this.post("/admin/list-indexes", {});
  }

  createIndex(indexName: string, table: string, columns: string[]): Promise<unknown> {
    return this.post("/admin/create-index", { indexName, table, columns });
  }
}

const TOPOLOGY_LOCK_RETRY_ATTEMPTS = 20;
const TOPOLOGY_LOCK_RETRY_DELAY_MS = 500;

/** CORRECTION (found via live testing, after this feature's initial P2 fix
 * assumed create-index was a safe no-op on an already-built index, mirroring
 * generate.mjs's own unconditional per-run createIndex loop): calling
 * /admin/create-index against an index that already exists and is fully
 * `ready` does NOT just report its current status — src/index.ts's
 * adminCreateIndexLockedCore re-registers the index rule and then
 * unconditionally calls /start-index-backfill again, re-scanning every
 * shard's rows. generate.mjs gets away with this because it's a one-time
 * setup script run against a cluster with no existing traffic; THIS feature
 * calls ensureScenarioIndexesReady on every single "Start the scenario" click,
 * including on an already-bootstrapped cluster, where restarting a real
 * backfill (each one holding the topology lock, taking real wall-clock time)
 * is both needless and — during live testing — correlated with correctness-
 * meter noise on the traffic that starts moments later. waitForIndexesReady
 * below now checks /admin/list-indexes's STATUS first and only reaches this
 * function for an index that's missing or 'failed' — never one already
 * 'ready' or one still 'building' (see waitForIndexesReady's own comment on
 * why a 'building' index is left to finish on its own rather than retried).
 * A topology-lock collision with some OTHER concurrent operation (a real
 * reshard, another visitor's own scenario start registering a NEW index for
 * the first time) is the one real reason a create-index call reached from
 * here can still transiently fail. */
async function createIndexWithRetry(admin: SchemaAdminClient, idx: IndexSchema): Promise<void> {
  for (let attempt = 1; ; attempt++) {
    try {
      await admin.createIndex(idx.indexName, idx.table, idx.columns);
      return;
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (message.includes("TOPOLOGY_OPERATION_IN_PROGRESS") && attempt < TOPOLOGY_LOCK_RETRY_ATTEMPTS) {
        await new Promise((resolve) => setTimeout(resolve, TOPOLOGY_LOCK_RETRY_DELAY_MS));
        continue;
      }
      throw err;
    }
  }
}

const INDEX_READY_POLL_ATTEMPTS = 40;
const INDEX_READY_POLL_DELAY_MS = 500;

/** Codex review P2 fix: /admin/list-indexes reporting an index BY NAME does
 * not mean it's usable — src/catalog.ts's index_rules.status is 'building'
 * from the moment it's registered until backfill fully completes (or
 * 'failed' if backfill gave up), and /v1/index-query rejects reads against
 * anything that isn't 'ready'. The previous version of this function only
 * checked existence, so an index left 'building' (a prior run's own bootstrap
 * still in progress) or 'failed' (a prior backfill that gave up) was treated
 * as already done — the scenario would start immediately, and every
 * index-backed transaction (New-Order's stock lookup, Stock-Level, Delivery,
 * Order-Status) would fail against that specific index until it happened to
 * finish on its own. This now waits for every required index to actually
 * report 'ready' before returning, re-triggering create-index for anything
 * 'failed' (src/index.ts's adminCreateIndexLockedCore resets a 'failed'
 * index's backfill on retry — see its own SQL's `status IN ('building',
 * 'failed')` clause) — but deliberately NOT for an index still 'building',
 * since re-calling create-index on one already in progress would just reset
 * its backfill cursor back to the start (same clause) for no benefit; a
 * 'building' index just needs time, handled by polling below. */
async function waitForIndexesReady(admin: SchemaAdminClient, required: IndexSchema[]): Promise<void> {
  for (let attempt = 1; attempt <= INDEX_READY_POLL_ATTEMPTS; attempt++) {
    const { indexes } = await admin.listIndexes();
    const statusByName = new Map((indexes ?? []).filter((i) => typeof i.indexName === "string").map((i) => [i.indexName as string, i.status]));

    const notReady = required.filter((idx) => statusByName.get(idx.indexName) !== "ready");
    if (notReady.length === 0) return;

    for (const idx of notReady) {
      const status = statusByName.get(idx.indexName);
      if (status === undefined || status === "failed") {
        await createIndexWithRetry(admin, idx);
      }
      // status === "building": no action — just keep polling below.
    }

    if (attempt < INDEX_READY_POLL_ATTEMPTS) {
      await new Promise((resolve) => setTimeout(resolve, INDEX_READY_POLL_DELAY_MS));
    }
  }
  throw new Error(
    `Timed out waiting for scenario indexes to become ready after ${INDEX_READY_POLL_ATTEMPTS} attempts — check /admin/list-indexes.`,
  );
}

/** Ensures every TPC-C TABLE this scenario needs exists — creating only
 * what's missing (mirrors generate.mjs's createSchema — see that function's
 * own comment on why create-table has no idempotent "IF NOT EXISTS" escape
 * hatch). Deliberately does NOT touch indexes — see this file's header
 * comment (Codex review P2 fix) on why index creation must happen AFTER
 * seeding, not before: call ensureScenarioIndexesReady once seeding
 * completes, not here. Called from load-driver.ts's handleStart, BEFORE
 * seedScenarioReferenceData — seeding a table that doesn't exist yet 502s
 * instead of upserting. */
export async function ensureScenarioTables(admin: SchemaAdminClient): Promise<void> {
  const { tables } = await admin.listTables();
  const existingTableNames = new Set((tables ?? []).map((t) => t.table_name).filter((n): n is string => typeof n === "string"));
  for (const t of TPCC_TABLES) {
    if (existingTableNames.has(t.table)) continue;
    await admin.createTable(t.table, t.schema, t.partitionKeyColumn);
  }
}

/** Ensures every TPC-C secondary index this scenario needs exists AND is
 * ready — creating/retrying only what's missing or stuck (see
 * waitForIndexesReady's own comment for the full status-handling logic).
 *
 * MUST be called AFTER seedScenarioReferenceData, never before (Codex review
 * P2 fix — this file's original version called this before seeding, mirrored
 * from ensureScenarioTables's own "safe to call anytime" table logic, which
 * doesn't actually apply here): /admin/create-index's own backfill scans
 * every row that exists on the table AT THE TIME IT'S CALLED. Creating an
 * index BEFORE seeding means the freshly-seeded rows only ever get indexed
 * via /v1/mutate's own asynchronous, best-effort index-maintenance path
 * (dispatched via ctx.waitUntil — see src/index.ts's mutateCore), which the
 * seeding call's HTTP response does NOT wait for; the very first ticks
 * (scheduled immediately after handleStart returns) could then query an
 * index for a row that was seeded but isn't queryable through it yet.
 * Calling this AFTER seeding instead means a freshly-registered index's own
 * synchronous-relative-to-readiness backfill scan picks up every seeded row
 * as a matter of course — the same reason generate.mjs itself seeds every
 * warehouse BEFORE calling createIndex (see that file's own "Creating N
 * secondary indexes (after all warehouses seeded)..." comment). An
 * ALREADY-'ready' index (a warm re-click) is unaffected either way — this
 * function skips it entirely, same as ensureScenarioTables does for tables. */
export async function ensureScenarioIndexesReady(admin: SchemaAdminClient): Promise<void> {
  await waitForIndexesReady(admin, TPCC_INDEXES);
}
