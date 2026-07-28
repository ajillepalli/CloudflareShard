/** schema-bootstrap.ts — ensures the TPC-C tables + secondary indexes exist
 * before "Start the scenario" seeds any data into them.
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
  listIndexes(): Promise<{ indexes?: Array<{ indexName?: string; table?: string }> }>;
  createIndex(indexName: string, table: string, columns: string[]): Promise<unknown>;
}

export class HttpSchemaAdminClient implements SchemaAdminClient {
  constructor(
    private readonly baseUrl: string,
    private readonly adminToken: string,
  ) {}

  private async post<T>(path: string, body: unknown): Promise<T> {
    const res = await fetch(`${this.baseUrl}${path}`, {
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

  listIndexes(): Promise<{ indexes?: Array<{ indexName?: string; table?: string }> }> {
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
 * calls ensureScenarioSchema on every single "Start the scenario" click,
 * including on an already-bootstrapped cluster, where restarting a real
 * backfill (each one holding the topology lock, taking real wall-clock time)
 * is both needless and — during live testing — correlated with correctness-
 * meter noise on the traffic that starts moments later. ensureScenarioSchema
 * below now checks /admin/list-indexes first and skips create-index entirely
 * for anything already registered, exactly like the table-existence check;
 * this function's retry loop is now reached only for a genuinely NEW index
 * on this cluster (never on a warm re-click), where a topology-lock
 * collision with some OTHER concurrent operation (a real reshard, another
 * visitor's own scenario start) is the one real reason a create-index call
 * can still transiently fail here. */
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

/** Ensures every TPC-C table + secondary index this scenario needs exists on
 * the cluster, creating only what's missing. Safe (and fast) to call on
 * every "Start the scenario" click: both table creation and index creation
 * are skipped entirely for anything already registered — see
 * /admin/list-tables (mirrors generate.mjs's createSchema — see that
 * function's own comment on why create-table has no idempotent "IF NOT
 * EXISTS" escape hatch) and /admin/list-indexes (see createIndexWithRetry's
 * own comment on why create-index must NOT be called against an
 * already-built index). Called from load-driver.ts's handleStart, BEFORE
 * seedScenarioReferenceData — seeding a table that doesn't exist yet 502s
 * instead of upserting. */
export async function ensureScenarioSchema(admin: SchemaAdminClient): Promise<void> {
  const { tables } = await admin.listTables();
  const existingTableNames = new Set((tables ?? []).map((t) => t.table_name).filter((n): n is string => typeof n === "string"));
  for (const t of TPCC_TABLES) {
    if (existingTableNames.has(t.table)) continue;
    await admin.createTable(t.table, t.schema, t.partitionKeyColumn);
  }

  const { indexes } = await admin.listIndexes();
  const existingIndexNames = new Set((indexes ?? []).map((i) => i.indexName).filter((n): n is string => typeof n === "string"));
  for (const idx of TPCC_INDEXES) {
    if (existingIndexNames.has(idx.indexName)) continue;
    await createIndexWithRetry(admin, idx);
  }
}
