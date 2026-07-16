// TPC-C-derived schema, registered via /admin/create-table.
//
// Every table's partition key is its own synthetic TEXT column (always the
// sole PRIMARY KEY, per CloudflareShard's partition-key eligibility rules --
// see README "Create the table's schema"). The real TPC-C numeric ID columns
// (w_id, d_id, c_id, o_id, ...) are also stored as plain INTEGER columns
// alongside the key so the row's actual TPC-C fields remain readable and
// queryable via /admin/create-index.
//
// NOTE: `/admin/create-table`'s schema string must not use
// `CREATE TABLE IF NOT EXISTS` (rejected 400) and the CREATE TABLE name must
// match `table` exactly.

export const TABLES = [
  {
    table: "tpcc_warehouse",
    partitionKeyColumn: "wh_key",
    schema:
      "CREATE TABLE tpcc_warehouse (wh_key TEXT PRIMARY KEY, w_id INTEGER, w_name TEXT, w_tax REAL, w_ytd REAL)",
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
    schema:
      "CREATE TABLE tpcc_history (h_key TEXT PRIMARY KEY, w_id INTEGER, d_id INTEGER, c_id INTEGER, h_amount REAL, h_date TEXT, h_data TEXT)",
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
    // Real TPC-C shares one global item catalog across all warehouses. This
    // benchmark instead seeds an identical copy of the item catalog into
    // every warehouse's tenant (see generate.mjs) -- a documented
    // simplification driven by the one-tenant-per-warehouse model, since
    // there is no cross-tenant read path to a single shared catalog.
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

// Secondary indexes, registered via /admin/create-index AFTER seeding each
// table (create-index synchronously backfills every existing row and blocks
// until the index is `ready`, so creating it post-seed means it's
// immediately queryable and never has to backfill an empty table twice).
export const INDEXES = [
  { table: "tpcc_customer", indexName: "idx_customer_by_id", columns: ["d_id", "c_id"] },
  { table: "tpcc_orders", indexName: "idx_orders_by_customer", columns: ["d_id", "c_id"] },
  { table: "tpcc_orders", indexName: "idx_orders_by_id", columns: ["d_id", "o_id"] },
  { table: "tpcc_order_line", indexName: "idx_order_line_by_order", columns: ["d_id", "o_id"] },
  { table: "tpcc_new_order", indexName: "idx_new_order_by_district", columns: ["d_id"] },
  { table: "tpcc_stock", indexName: "idx_stock_by_item", columns: ["i_id"] },
];
