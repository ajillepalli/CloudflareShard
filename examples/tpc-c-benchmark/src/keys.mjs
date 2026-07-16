// Synthetic TEXT partition-key helpers.
//
// CloudflareShard requires every table's partition key to be a single TEXT
// (or BLOB) column that collates as BINARY (SQLite's default) -- see the
// repo README's "Create the table's schema" / "Tenant-scoped table scan"
// sections. `/v1/table-scan` and `/v1/index-query` both return rows ordered
// by partition key as a lexicographic TEXT sort, NOT a numeric sort, so
// every numeric ID component embedded in a key here is zero-padded to a
// fixed width. Without padding, "...-9" would sort AFTER "...-10"
// (lexicographically "1" < "9"), which would corrupt e.g. Delivery's
// "take the lowest o_id" and Order-Status's "take the highest o_id" logic
// that both depend on ascending partition-key order.
//
// Widths (fixed, chosen to comfortably exceed this benchmark's default and
// realistic scale-up ranges): warehouse 4 digits, district 2 digits,
// customer 5 digits, order 9 digits, order-line number 2 digits, item 6
// digits.

/** Zero-pad a non-negative integer to `width` digits. */
function pad(n, width) {
  return String(n).padStart(width, "0");
}

/** Tenant ID for a warehouse -- one tenant per warehouse (see design doc). */
export function tenantIdForWarehouse(w) {
  return `tpcc-w${pad(w, 4)}`;
}

export function warehouseKey(w) {
  return `wh-${pad(w, 4)}`;
}

export function districtKey(w, d) {
  return `d-${pad(w, 4)}-${pad(d, 2)}`;
}

export function customerKey(w, d, c) {
  return `c-${pad(w, 4)}-${pad(d, 2)}-${pad(c, 5)}`;
}

export function orderKey(w, d, o) {
  return `o-${pad(w, 4)}-${pad(d, 2)}-${pad(o, 9)}`;
}

export function orderLineKey(w, d, o, l) {
  return `ol-${pad(w, 4)}-${pad(d, 2)}-${pad(o, 9)}-${pad(l, 2)}`;
}

// Codex review round 18 P2 fix: this key used to omit the warehouse
// entirely (`i-{i}`), identical across every tenant. Item catalog rows are
// deliberately DUPLICATED per warehouse-tenant (see generate.mjs and README
// item 7 -- one global catalog, like real TPC-C has, isn't possible under
// this project's one-tenant-per-warehouse model with no cross-tenant read
// path), but sharing the exact same partition key across tenants doesn't
// give each tenant its OWN copy at all: CloudflareShard's row provenance
// (__cf_row_owners) attributes a physical (table, partitionKey) row to
// exactly ONE tenant at a time, "latest write wins" with no cross-tenant
// conflict check (src/shard.ts's upsertRowOwner). Seeding multiple
// warehouses in sequence meant each later warehouse's upsert silently
// stole ownership of the SAME physical rows away from every earlier
// warehouse -- only the last-seeded warehouse actually retained a
// provenance-valid tpcc_item catalog; every earlier one's rows were still
// physically present but no longer attributed to it. This never surfaced
// in this benchmark's own transaction logic (item name/price are read
// from generate.mjs's client-side cache -- see README item 6 -- never
// read back from the server at runtime), but it's a real violation of the
// "each warehouse gets its own copy" design this key was supposed to
// implement, exactly like stockKey below already does correctly. Fixed by
// including the warehouse, matching stockKey's own pattern.
export function itemKey(w, i) {
  return `i-${pad(w, 4)}-${pad(i, 6)}`;
}

export function stockKey(w, i) {
  return `s-${pad(w, 4)}-${pad(i, 6)}`;
}

export function newOrderKey(w, d, o) {
  return `no-${pad(w, 4)}-${pad(d, 2)}-${pad(o, 9)}`;
}

/** History rows have no natural composite id in the TPC-C spec and are never
 * looked up by key (only ever inserted, never read back by this benchmark) --
 * a random UUID is sufficient and avoids inventing a fake ordering. */
export function historyKey() {
  return crypto.randomUUID();
}

export { pad };
