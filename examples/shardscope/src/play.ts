/** play.ts — Shardscope's Playground room server-side layer (backend only;
 * no UI yet — see the room's rail item in README.md, currently disabled).
 *
 * ============================================================================
 * WHAT THIS ROOM IS: a gate-protected proxy that lets a browser drive
 * CloudflareShard's developer primitives (mutate/tx/index-query/table-scan,
 * plus the operator-only sql/scatter) safely — see docs/design-doc.md's
 * "Room 4 — Playground" for the original product spec this implements:
 * "Interactive console for every primitive ... /v1/mutate (with requestId to
 * demo idempotency — replay the same request, watch it reject a mismatched
 * body instead of returning stale), /v1/tx, /v1/index-query, /v1/scatter,
 * /v1/table-scan."
 *
 * ============================================================================
 * SECURITY MODEL (read this before adding a route or loosening a check)
 * ============================================================================
 *   1. The browser NEVER supplies ADMIN_TOKEN or a raw tenant token, and
 *      NEVER picks an arbitrary tenant identity. Every tenant-scoped route
 *      (playMutate/playTx/playIndexQuery/playTableScan) operates on a
 *      CONTROLLED demo tenant selected from PLAYGROUND_WAREHOUSE_IDS below —
 *      the exact `tenantIdForWarehouse()` naming ./load/transactions.ts (the
 *      load engine) already uses, so a Playground tenant is a real,
 *      recognizable demo warehouse, not a bespoke identity. The bearer token
 *      for it is minted/cached through ./load/tenant-token-store.ts's
 *      TenantTokenStoreTokenProvider — the exact durable get-or-create store
 *      the load engine itself uses — never accepted from the request body.
 *      `table`/`indexName` are validated against PLAYGROUND_TABLES /
 *      PLAYGROUND_INDEXES; anything outside those whitelists is a 400
 *      PlayValidationError (see requireWarehouseId/requireTable/
 *      requireIndexName below), never silently coerced or passed through.
 *   2. Operator routes (playSql/playScatter) run server-side with
 *      env.ADMIN_TOKEN (see reshard.ts's identical pattern) — the browser
 *      never sees it. playSql additionally enforces READ-ONLY: only
 *      SELECT/EXPLAIN-shaped statements are accepted (see
 *      requireReadOnlySql below, which reuses the main repo's own
 *      src/sql-safety.ts classifiers — isMutation/isDangerous — rather than
 *      re-deriving a denylist; ../../../src/gate.ts and ./load/skew.ts
 *      already establish this exact cross-boundary reuse pattern for
 *      SHARE-safe pure helpers). Writes/DDL (INSERT/UPDATE/DELETE/REPLACE/
 *      CREATE/DROP/ALTER/TRUNCATE/PRAGMA/VACUUM/REINDEX/ATTACH/DETACH, or any
 *      multi-statement payload) are rejected with a 400 explaining this is a
 *      read-only operator console — never forwarded to SHARD_API. playScatter
 *      additionally requires the query be a single-table, no-JOIN
 *      `SELECT ... FROM <demo-table>` targeting PLAYGROUND_TABLES (see
 *      extractScatterFromTable below) — core's own scatterCore already fans
 *      a raw SELECT out across EVERY tenant on EVERY shard indiscriminately
 *      (that's the whole point of scatter), so this scopes the demo's blast
 *      radius to the same table whitelist every other Playground route
 *      already respects, on top of core's own SELECT-only + isDangerous
 *      enforcement. See this file's bottom "DEVIATION NOTE" for why this is
 *      a conservative allowlist rather than a full SQL parser.
 *   3. Every route here is dispatched under /api/play/* in src/index.ts,
 *      which only ever runs after url.pathname.startsWith("/api/") has
 *      already passed isGateAuthorized(request, env) — see that file's
 *      header comment and its "GATING CONFIRMATION" comments on the
 *      /api/reshard/* and /api/chaos/* blocks for the identical structural
 *      argument, which applies unchanged here. On the underlying SHARD_API
 *      call rejecting, src/index.ts's shared runOperatorOp (reused here via
 *      a new runPlayOp, exactly like runReshardOp/runChaosOp) unpacks a
 *      structured "CloudflareShard RPC error <status>: <body>" into the
 *      original status + body (this is also how the idempotent-mismatch 409
 *      demo surfaces to the browser honestly — see playMutate's doc comment)
 *      and collapses anything else into a generic 502 rather than leaking
 *      internal detail.
 *   4. Every parse*() function below validates/coerces its input before any
 *      SHARD_API call is made — mirroring reshard.ts's parse*() style (throw
 *      PlayValidationError, never trust a browser-supplied shape). Arrays
 *      accepted from the browser (tx's `mutations`, sql's `params`) are
 *      length-bounded (MAX_TX_MUTATIONS, MAX_SQL_PARAMS) so a single request
 *      can't drive an unbounded fan-out or an unbounded loop.
 * ============================================================================
 *
 * WHY RPC, NOT RAW HTTP FETCH (unlike ./chaos.ts's rawMutate): CloudflareShardRpc
 * (the main repo's src/index.ts) exposes `mutate`/`tx`/`indexQuery`/`tableScan`
 * as RPC methods taking a TENANT bearer token directly (not just the
 * admin-gated `adminXxx` methods reshard.ts calls), and `sql`/`scatter` as
 * admin-gated RPC methods — see env.d.ts's ShardApiBinding for the exact
 * signatures, hand-mirrored from that class. Calling these through the
 * existing SHARD_API service binding (like every other file in this Worker
 * already does) avoids a second network hop through a gateway "baseUrl" the
 * way ./chaos.ts's double-submit/mismatched-replay attacks need to (those
 * two need the RAW HTTP status/body pair to verify an exact wire-format
 * contract; Playground doesn't — it only needs to forward an honest
 * status/body to the browser, which unwrapForRpc's "CloudflareShard RPC
 * error <status>: <body>" + src/index.ts's runOperatorOp unpacking already
 * does end to end).
 */
import type { Env } from "./env";
import { tenantIdForWarehouse } from "./load/transactions";
import { TenantTokenStoreTokenProvider } from "./load/tenant-token-store";
// Cross-boundary reuse of the main repo's pure SQL classifiers — same
// established pattern as ./gate.ts (`timingSafeEqual` from "../../../src/auth")
// and ./load/skew.ts (`hashKey` from "../../../../src/hash"): a dependency-free
// pure function, imported directly rather than re-derived, so this file's
// read-only enforcement can never drift from what core itself considers a
// mutation/dangerous statement.
import { isDangerous, isMutation } from "../../../src/sql-safety";
// Same cross-boundary-reuse pattern for the routing math itself (playRouteInspect,
// below): ./load/skew.ts already inverts + verifies this EXACT formula for the
// hot-shard skew driver, so its `vbucketForKey` (the piece that formula lives
// in) is imported here rather than a second copy being written — see that
// file's own header comment for the formula's source-of-truth pointer into
// core (src/hash.ts's hashKey + src/index.ts's mutate/tx routing path).
import { vbucketForKey } from "./load/skew";
import { hashKey } from "../../../src/hash";

/** Thrown by every parse*() function below on a malformed or
 * whitelist-violating request from the browser — distinct from whatever
 * env.SHARD_API's RPC calls themselves reject with, so src/index.ts's
 * runPlayOp can tell "bad/disallowed request from the browser" (400) apart
 * from "the cluster rejected this operation" (forwarded with its own
 * status). Mirrors reshard.ts's ReshardValidationError / chaos.ts's
 * ChaosPreconditionError. */
export class PlayValidationError extends Error {}

// ============================================================================
// The controlled demo scope — see this file's header comment, point 1.
// ============================================================================

/** The Playground's fixed demo tenant set. `1` is also ./load/load-driver.ts's
 * own DEFAULT_WAREHOUSE_IDS entry (so it plausibly already has real seeded
 * data from a load run); 2 and 3 are additional controlled tenants for
 * demonstrating cross-tenant/warehouse selection without ever letting the
 * browser name an arbitrary tenant. Every one of these resolves through the
 * exact same tenantIdForWarehouse() the load engine uses (tpcc-w0001, etc.),
 * and its bearer token is minted/cached via TenantTokenStore the same way. */
export const PLAYGROUND_WAREHOUSE_IDS = [1, 2, 3] as const;
export type PlaygroundWarehouseId = (typeof PLAYGROUND_WAREHOUSE_IDS)[number];

/** The Playground's fixed demo table set — the same 8 TPC-C tables
 * ./load/transactions.ts already writes/reads via the real load engine, so
 * every Playground call targets data the demo already treats as scratch
 * space, never an arbitrary or internal table. */
export const PLAYGROUND_TABLES = [
  "tpcc_warehouse",
  "tpcc_district",
  "tpcc_customer",
  "tpcc_history",
  "tpcc_new_order",
  "tpcc_order_line",
  "tpcc_orders",
  "tpcc_stock",
] as const;
export type PlaygroundTable = (typeof PLAYGROUND_TABLES)[number];

/** The exact secondary indexes ./load/transactions.ts's real TPC-C mix
 * queries against each demo table (see that file's own indexQuery() calls)
 * — reused verbatim rather than invented, so a Playground index-query is
 * guaranteed to be a real, already-exercised index, not a hypothetical one. */
export const PLAYGROUND_INDEXES: Record<PlaygroundTable, readonly string[]> = {
  tpcc_warehouse: [],
  tpcc_district: [],
  tpcc_customer: ["idx_customer_by_id"],
  tpcc_history: [],
  tpcc_new_order: ["idx_new_order_by_district"],
  tpcc_order_line: ["idx_order_line_by_order"],
  tpcc_orders: ["idx_orders_by_customer", "idx_orders_by_id", "idx_orders_by_district"],
  tpcc_stock: ["idx_stock_by_item"],
};

const MUTATE_OPS = ["insert", "update", "delete", "upsert"] as const;
export type PlayMutateOp = (typeof MUTATE_OPS)[number];

// ----------------------------------------------------------------------------
// Small validation helpers — mirrors reshard.ts's parse*() style (throw on
// anything unexpected, never coerce silently past a whitelist).
// ----------------------------------------------------------------------------

function asRecord(body: unknown): Record<string, unknown> {
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    throw new PlayValidationError("Request body must be a JSON object.");
  }
  return body as Record<string, unknown>;
}

function requireNonEmptyString(value: unknown, field: string, maxLen = 256): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new PlayValidationError(`Missing or invalid "${field}" (must be a non-empty string).`);
  }
  if (value.length > maxLen) {
    throw new PlayValidationError(`"${field}" exceeds the playground's ${maxLen}-character limit.`);
  }
  return value;
}

function optionalNonEmptyString(value: unknown, field: string, maxLen = 256): string | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  return requireNonEmptyString(value, field, maxLen);
}

function requireRecord(value: unknown, field: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new PlayValidationError(`Missing or invalid "${field}" (must be a JSON object).`);
  }
  return value as Record<string, unknown>;
}

function optionalRecord(value: unknown, field: string): Record<string, unknown> | undefined {
  if (value === undefined || value === null) return undefined;
  return requireRecord(value, field);
}

/** Whitelist check — the ONLY thing that decides whether a browser-supplied
 * warehouseId is honored. Never widen this to "any positive integer": that
 * would let the browser pick an arbitrary tenant identity, exactly what this
 * file's header comment (point 1) forbids. */
function requireWarehouseId(value: unknown): PlaygroundWarehouseId {
  const n = typeof value === "string" && value.trim() !== "" ? Number(value) : value;
  if (typeof n !== "number" || !Number.isInteger(n) || !(PLAYGROUND_WAREHOUSE_IDS as readonly number[]).includes(n)) {
    throw new PlayValidationError(`Missing or invalid "warehouseId" — must be one of the playground's demo tenants: ${PLAYGROUND_WAREHOUSE_IDS.join(", ")}.`);
  }
  return n as PlaygroundWarehouseId;
}

function requireTable(value: unknown, field = "table"): PlaygroundTable {
  if (typeof value !== "string" || !(PLAYGROUND_TABLES as readonly string[]).includes(value)) {
    throw new PlayValidationError(`Missing or invalid "${field}" — must be one of the playground's demo tables: ${PLAYGROUND_TABLES.join(", ")}.`);
  }
  return value as PlaygroundTable;
}

function requireIndexName(table: PlaygroundTable, value: unknown): string {
  const allowed = PLAYGROUND_INDEXES[table];
  if (typeof value !== "string" || !allowed.includes(value)) {
    throw new PlayValidationError(
      `Missing or invalid "indexName" for table "${table}" — must be one of: ${allowed.length > 0 ? allowed.join(", ") : "(no indexes registered for this demo table)"}.`,
    );
  }
  return value;
}

function requireOp(value: unknown, field = "op"): PlayMutateOp {
  if (typeof value !== "string" || !(MUTATE_OPS as readonly string[]).includes(value)) {
    throw new PlayValidationError(`Missing or invalid "${field}" — must be one of: ${MUTATE_OPS.join(", ")}.`);
  }
  return value as PlayMutateOp;
}

function optionalBoundedInt(value: unknown, field: string, max: number): number | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  const n = typeof value === "string" ? Number(value) : value;
  if (typeof n !== "number" || !Number.isInteger(n) || n <= 0) {
    throw new PlayValidationError(`"${field}", if provided, must be a positive integer.`);
  }
  return Math.min(n, max);
}

const MAX_SQL_PARAMS = 20;

function optionalParamsArray(value: unknown): unknown[] | undefined {
  if (value === undefined) return undefined;
  if (!Array.isArray(value)) throw new PlayValidationError('"params", if provided, must be an array.');
  if (value.length > MAX_SQL_PARAMS) {
    throw new PlayValidationError(`"params" may contain at most ${MAX_SQL_PARAMS} values.`);
  }
  for (const v of value) {
    if (v !== null && typeof v !== "string" && typeof v !== "number" && typeof v !== "boolean") {
      throw new PlayValidationError('"params" values must be string, number, boolean, or null.');
    }
  }
  return value;
}

/** Resolves the demo tenant's bearer token via TenantTokenStore — the exact
 * same durable get-or-create store (and TokenProvider) the load engine uses
 * (./load/tenant-token-store.ts), never a browser-supplied token. */
async function resolvePlaygroundTenantToken(env: Env, warehouseId: PlaygroundWarehouseId): Promise<string> {
  return new TenantTokenStoreTokenProvider(env).getTenantToken(warehouseId);
}

// ============================================================================
// /api/play/mutate — tenant-scoped single /v1/mutate. Accepts a
// client-supplied requestId (echoed back in the response either way) so the
// UI can demo BOTH idempotent-replay contracts:
//   - same requestId + SAME body, replayed -> ShardDO's applied_requests
//     cache serves the identical cached result (see ../chaos.ts's header
//     comment for the exact contract, verified against core's real
//     src/shard.ts) — no double effect, same requestId back.
//   - same requestId + DIFFERENT body, replayed -> ShardDO rejects with 409
//     "requestId was already used with different sql/params — refusing to
//     replay a mismatched result." — src/index.ts's runPlayOp (mirroring
//     runReshardOp/runChaosOp) unpacks that INTO this exact status + body for
//     the browser, so the demo shows the real contract, not a simulation.
// ============================================================================

export interface PlayMutateInput {
  warehouseId: PlaygroundWarehouseId;
  op: PlayMutateOp;
  table: PlaygroundTable;
  partitionKey: string;
  values?: Record<string, unknown>;
  where?: Record<string, unknown>;
  requestId?: string;
}

export function parsePlayMutateInput(body: unknown): PlayMutateInput {
  const b = asRecord(body);
  return {
    warehouseId: requireWarehouseId(b.warehouseId),
    op: requireOp(b.op),
    table: requireTable(b.table),
    partitionKey: requireNonEmptyString(b.partitionKey, "partitionKey"),
    values: optionalRecord(b.values, "values"),
    where: optionalRecord(b.where, "where"),
    requestId: optionalNonEmptyString(b.requestId, "requestId"),
  };
}

export async function playMutate(env: Env, input: PlayMutateInput): Promise<Record<string, unknown>> {
  const tenantId = tenantIdForWarehouse(input.warehouseId);
  const token = await resolvePlaygroundTenantToken(env, input.warehouseId);
  const requestId = input.requestId ?? crypto.randomUUID();
  const result = (await env.SHARD_API.mutate(token, {
    op: input.op,
    table: input.table,
    tenantId,
    partitionKey: input.partitionKey,
    values: input.values,
    where: input.where,
    requestId,
  })) as Record<string, unknown>;
  return { ...result, requestId };
}

// ============================================================================
// /api/play/tx — tenant-scoped /v1/tx (multi-mutation 2PC), same tenant for
// every mutation in the call (mirrors ./chaos.ts/./load/gateway-client.ts's
// own "stamp every mutation with the SAME tenantId" convention — /v1/tx has
// no cross-tenant mode).
// ============================================================================

export interface PlayTxMutationInput {
  op: PlayMutateOp;
  table: PlaygroundTable;
  partitionKey: string;
  values?: Record<string, unknown>;
  where?: Record<string, unknown>;
}

export interface PlayTxInput {
  warehouseId: PlaygroundWarehouseId;
  mutations: PlayTxMutationInput[];
  requestId?: string;
}

const MAX_TX_MUTATIONS = 10;

export function parsePlayTxInput(body: unknown): PlayTxInput {
  const b = asRecord(body);
  const warehouseId = requireWarehouseId(b.warehouseId);
  if (!Array.isArray(b.mutations) || b.mutations.length === 0) {
    throw new PlayValidationError('Missing or invalid "mutations" (must be a non-empty array).');
  }
  if (b.mutations.length > MAX_TX_MUTATIONS) {
    throw new PlayValidationError(`"mutations" may contain at most ${MAX_TX_MUTATIONS} entries.`);
  }
  const mutations = b.mutations.map((m, i) => {
    const mb = asRecord(m);
    return {
      op: requireOp(mb.op, `mutations[${i}].op`),
      table: requireTable(mb.table, `mutations[${i}].table`),
      partitionKey: requireNonEmptyString(mb.partitionKey, `mutations[${i}].partitionKey`),
      values: optionalRecord(mb.values, `mutations[${i}].values`),
      where: optionalRecord(mb.where, `mutations[${i}].where`),
    };
  });
  return { warehouseId, mutations, requestId: optionalNonEmptyString(b.requestId, "requestId") };
}

export async function playTx(env: Env, input: PlayTxInput): Promise<Record<string, unknown>> {
  const tenantId = tenantIdForWarehouse(input.warehouseId);
  const token = await resolvePlaygroundTenantToken(env, input.warehouseId);
  const requestId = input.requestId ?? crypto.randomUUID();
  const stamped = input.mutations.map((m) => ({ ...m, tenantId }));
  const result = (await env.SHARD_API.tx(token, { mutations: stamped, requestId })) as Record<string, unknown>;
  return { ...result, requestId };
}

// ============================================================================
// /api/play/index-query — tenant-scoped /v1/index-query.
// ============================================================================

export interface PlayIndexQueryInput {
  warehouseId: PlaygroundWarehouseId;
  table: PlaygroundTable;
  indexName: string;
  values: Record<string, unknown>;
  limit?: number;
}

const MAX_INDEX_QUERY_LIMIT = 100; // matches /v1/index-query's own hard server-side cap (see ./load/transactions.ts's orderStatus() comment)

export function parsePlayIndexQueryInput(body: unknown): PlayIndexQueryInput {
  const b = asRecord(body);
  const table = requireTable(b.table);
  return {
    warehouseId: requireWarehouseId(b.warehouseId),
    table,
    indexName: requireIndexName(table, b.indexName),
    values: requireRecord(b.values, "values"),
    limit: optionalBoundedInt(b.limit, "limit", MAX_INDEX_QUERY_LIMIT),
  };
}

export async function playIndexQuery(env: Env, input: PlayIndexQueryInput): Promise<unknown> {
  const tenantId = tenantIdForWarehouse(input.warehouseId);
  const token = await resolvePlaygroundTenantToken(env, input.warehouseId);
  return env.SHARD_API.indexQuery(token, { table: input.table, indexName: input.indexName, tenantId, values: input.values, limit: input.limit });
}

// ============================================================================
// /api/play/table-scan — tenant-scoped /v1/table-scan.
// ============================================================================

export interface PlayTableScanInput {
  warehouseId: PlaygroundWarehouseId;
  table: PlaygroundTable;
  limit: number;
  cursor?: string;
}

const DEFAULT_TABLE_SCAN_LIMIT = 20;
const MAX_TABLE_SCAN_LIMIT = 100;

export function parsePlayTableScanInput(body: unknown): PlayTableScanInput {
  const b = asRecord(body);
  return {
    warehouseId: requireWarehouseId(b.warehouseId),
    table: requireTable(b.table),
    limit: optionalBoundedInt(b.limit, "limit", MAX_TABLE_SCAN_LIMIT) ?? DEFAULT_TABLE_SCAN_LIMIT,
    cursor: optionalNonEmptyString(b.cursor, "cursor", 1024),
  };
}

export async function playTableScan(env: Env, input: PlayTableScanInput): Promise<unknown> {
  const tenantId = tenantIdForWarehouse(input.warehouseId);
  const token = await resolvePlaygroundTenantToken(env, input.warehouseId);
  return env.SHARD_API.tableScan(token, { tenantId, table: input.table, limit: input.limit, cursor: input.cursor });
}

// ============================================================================
// Operator-only: /api/play/sql, /api/play/scatter — run server-side with
// env.ADMIN_TOKEN, READ-ONLY (see this file's header comment, point 2).
// ============================================================================

const MAX_SQL_LENGTH = 4000;

const READ_ONLY_REJECTION_MESSAGE =
  "This is a read-only operator console — only SELECT/EXPLAIN-style statements are permitted here. " +
  "Writes and DDL (INSERT/UPDATE/DELETE/REPLACE/CREATE/DROP/ALTER/TRUNCATE/PRAGMA/VACUUM/REINDEX/ATTACH/DETACH), " +
  "and multi-statement payloads, are rejected.";

/** Reuses core's own isMutation/isDangerous (src/sql-safety.ts) rather than
 * re-deriving a keyword list — see this file's header comment, point 2, for
 * why this is a cross-boundary import rather than a local reimplementation.
 * Note this is STRICTER than a plain "no writes" rule: isDangerous also
 * blocks every PRAGMA (not just a write-pragma) — there is no reliable way
 * to tell a read-only PRAGMA from a mutating one without a real SQL parser,
 * so the conservative choice is to block all of them; see this file's
 * bottom "DEVIATION NOTE". */
function requireReadOnlySql(value: unknown): string {
  const sql = requireNonEmptyString(value, "sql", MAX_SQL_LENGTH);
  if (isMutation(sql) || isDangerous(sql)) {
    throw new PlayValidationError(READ_ONLY_REJECTION_MESSAGE);
  }
  return sql;
}

export interface PlaySqlInput {
  warehouseId: PlaygroundWarehouseId;
  table: PlaygroundTable;
  partitionKey: string;
  sql: string;
  params?: unknown[];
}

export function parsePlaySqlInput(body: unknown): PlaySqlInput {
  const b = asRecord(body);
  return {
    warehouseId: requireWarehouseId(b.warehouseId),
    table: requireTable(b.table),
    partitionKey: requireNonEmptyString(b.partitionKey, "partitionKey"),
    sql: requireReadOnlySql(b.sql),
    params: optionalParamsArray(b.params),
  };
}

export function playSql(env: Env, input: PlaySqlInput): Promise<unknown> {
  const tenantId = tenantIdForWarehouse(input.warehouseId);
  return env.SHARD_API.sql(env.ADMIN_TOKEN, {
    sql: input.sql,
    params: input.params,
    table: input.table,
    tenantId,
    partitionKey: input.partitionKey,
  });
}

/** Conservative allowlist check for scatter's free-form SELECT (see this
 * file's header comment, point 2, and the DEVIATION NOTE at the bottom of
 * this file): requires exactly one `FROM <identifier>` and no `JOIN`, so the
 * query can only ever read one demo table, never an arbitrary or joined
 * one. Returns null (caller rejects) on anything that doesn't confidently
 * match this shape — fails CLOSED, never guesses. */
export function extractScatterFromTable(sql: string): string | null {
  const lower = sql.toLowerCase();
  if (/\bjoin\b/.test(lower)) return null;
  const fromOccurrences = lower.match(/\bfrom\b/g) ?? [];
  if (fromOccurrences.length !== 1) return null;
  const m = /\bfrom\s+("([^"]+)"|`([^`]+)`|\[([^\]]+)\]|([A-Za-z_][A-Za-z0-9_]*))/i.exec(sql);
  if (!m) return null;
  const raw = m[2] ?? m[3] ?? m[4] ?? m[5];
  return raw ? raw.toLowerCase() : null;
}

export interface PlayScatterInput {
  sql: string;
  params?: unknown[];
  limit?: number;
}

const MAX_SCATTER_LIMIT = 200;

export function parsePlayScatterInput(body: unknown): PlayScatterInput {
  const b = asRecord(body);
  const sql = requireReadOnlySql(b.sql);
  const table = extractScatterFromTable(sql);
  if (!table || !(PLAYGROUND_TABLES as readonly string[]).includes(table)) {
    throw new PlayValidationError(
      `This playground's scatter console only allows a single-table "SELECT ... FROM <table>" query (no JOIN) targeting one of the demo tables: ${PLAYGROUND_TABLES.join(", ")}.`,
    );
  }
  return {
    sql,
    params: optionalParamsArray(b.params),
    limit: optionalBoundedInt(b.limit, "limit", MAX_SCATTER_LIMIT),
  };
}

export function playScatter(env: Env, input: PlayScatterInput): Promise<unknown> {
  return env.SHARD_API.scatter(env.ADMIN_TOKEN, { sql: input.sql, params: input.params, limit: input.limit });
}

// ============================================================================
// /api/play/route-inspect — the Playground's routing inspector (READ-ONLY,
// no cluster mutation). Given a whitelisted (warehouseId, table,
// partitionKey), resolves the SAME two-step hash routing core itself applies
// to a real /v1/mutate call:
//   1. tenantId -> catalogShardId: `catalog-${hashKey(tenantId) %
//      catalogShardCount}` (src/index.ts's catalogShardIdForTenant — mirrored
//      here as a local pure function exactly the way ./load/load-driver.ts
//      already mirrors it, with the identical "MUST stay in sync" comment;
//      that file's own header comment explains why this small formula is
//      duplicated rather than imported: src/index.ts is a separate
//      deployable Worker's *private*, non-exported function, not a shared
//      library).
//   2. tenantId:table:partitionKey -> vbucket: ./load/skew.ts's
//      vbucketForKey — imported, NOT re-derived (see this file's import
//      comment above) — the exact function the hot-shard skew driver itself
//      uses and verifies against the live map, so this resolver can never
//      silently drift from core's real routing.
// Both catalogShardCount and totalVBuckets/the vbucket->shard ownership map
// come from a LIVE env.SHARD_API.adminVbucketMap(env.ADMIN_TOKEN) call — the
// same admin RPC ./aggregator.ts's TopologyAggregator already polls for the
// Topology room (see that file's pollSnapshot) — never a hardcoded/guessed
// count, so a key's reported owner reflects the cluster's ACTUAL current
// state (including a live reshard happening in another room), not a stale or
// assumed one.
// ============================================================================

/** Loosely-typed slice of adminVbucketMap's response this resolver needs —
 * mirrors aggregator.ts's own local AdminVbucketMapResponse (env.d.ts's
 * ShardApiBinding.adminVbucketMap intentionally returns `unknown`; every
 * caller narrows it locally, same established convention as
 * aggregator.ts/chaos.ts's VbucketMapLike/./load/load-driver.ts's own copy). */
interface RouteInspectVbucketMapRow {
  vbucket: number;
  shardId: string;
  migrationStatus: string;
  targetShardId: string | null;
}

interface RouteInspectVbucketMapResponse {
  catalogShardCount: number;
  totalVBuckets: number;
  catalogs: Array<{ catalogShardId: string; totalVBuckets: number; map: RouteInspectVbucketMapRow[] }>;
}

/** Which catalog shard governs a given tenant — deliberately duplicated (not
 * imported) from src/index.ts's private, non-exported `catalogShardIdForTenant`,
 * the same mirrored-formula pattern ./load/load-driver.ts's own copy already
 * establishes (see that file's identical doc comment for the full reasoning).
 * MUST stay in sync with src/index.ts's version: `catalog-${hashKey(tenantId)
 * % catalogShardCount}`. `catalogShardCount` here always comes from the live
 * adminVbucketMap response (never a locally-guessed env var), so this can
 * never drift from whatever the cluster was actually initialized with. */
function catalogShardIdForTenant(tenantId: string, catalogShardCount: number): string {
  return `catalog-${hashKey(tenantId) % catalogShardCount}`;
}

export interface PlayRouteInspectInput {
  warehouseId: PlaygroundWarehouseId;
  table: PlaygroundTable;
  partitionKey: string;
}

export function parsePlayRouteInspectInput(body: unknown): PlayRouteInspectInput {
  const b = asRecord(body);
  return {
    warehouseId: requireWarehouseId(b.warehouseId),
    table: requireTable(b.table),
    partitionKey: requireNonEmptyString(b.partitionKey, "partitionKey"),
  };
}

export interface PlayRouteInspectMigration {
  status: string;
  fromShardId: string;
  toShardId: string;
}

export interface PlayRouteInspectResult {
  tenantId: string;
  catalogShardId: string;
  vbucket: number;
  totalVBuckets: number;
  catalogShardCount: number;
  ownerShardId: string;
  migration?: PlayRouteInspectMigration;
}

/** Resolves a (warehouseId, table, partitionKey) to its real current owning
 * shard against the LIVE vbucket map — see this section's header comment for
 * the two-step formula and why every count/mapping comes from a fresh
 * env.SHARD_API.adminVbucketMap call rather than a cached/guessed value.
 *
 * If the resolved catalog or vbucket isn't present in the live map (an
 * uninitialized cluster, or a catalogShardCount that has changed out from
 * under a stale assumption), this throws PlayValidationError rather than
 * fabricating an owner — same "fail honestly, never guess" contract as
 * ./chaos.ts's ChaosPreconditionError for an analogous "catalog not found in
 * the live vBucket map" case (see that file's pickHotVbucketTarget), reusing
 * this file's single PlayValidationError class since Playground doesn't
 * otherwise distinguish "malformed request" from "current cluster state
 * can't satisfy this request" — both are calm 400s to the browser via
 * runPlayOp, never a 500 or a fabricated result. */
export async function playRouteInspect(env: Env, input: PlayRouteInspectInput): Promise<PlayRouteInspectResult> {
  const tenantId = tenantIdForWarehouse(input.warehouseId);
  const vbucketMap = (await env.SHARD_API.adminVbucketMap(env.ADMIN_TOKEN)) as RouteInspectVbucketMapResponse;

  const catalogShardId = catalogShardIdForTenant(tenantId, vbucketMap.catalogShardCount);
  const catalog = vbucketMap.catalogs.find((c) => c.catalogShardId === catalogShardId);
  if (!catalog) {
    throw new PlayValidationError(
      `Catalog ${catalogShardId} not found in the live vBucket map — the cluster may not be initialized yet. Try again once it is.`,
    );
  }

  const vbucket = vbucketForKey(tenantId, input.table, input.partitionKey, catalog.totalVBuckets);
  const row = catalog.map.find((r) => r.vbucket === vbucket);
  if (!row) {
    throw new PlayValidationError(
      `vBucket ${vbucket} not found in catalog ${catalogShardId}'s live map — the cluster may not be initialized yet. Try again once it is.`,
    );
  }

  const result: PlayRouteInspectResult = {
    tenantId,
    catalogShardId,
    vbucket,
    totalVBuckets: catalog.totalVBuckets,
    catalogShardCount: vbucketMap.catalogShardCount,
    ownerShardId: row.shardId,
  };
  // A vbucket mid-migration still routes writes to its CURRENT shardId (the
  // cutover hasn't flipped ownership yet) — same "current shardId is the
  // real owner" rule ./load/skew.ts's ownedVBuckets doc comment states for
  // the identical reason. Reported honestly alongside the target, never
  // hidden or presented as already-moved.
  if (row.migrationStatus !== "none" && row.targetShardId) {
    result.migration = { status: row.migrationStatus, fromShardId: row.shardId, toShardId: row.targetShardId };
  }
  return result;
}

// ============================================================================
// DEVIATION NOTE (per this task's instructions: "If you believe a different
// constraint is safer/more honest, propose it in your report rather than
// silently widening scope"):
//
// extractScatterFromTable's single-table/no-JOIN regex is a conservative
// allowlist, not a real SQL parser — it can be fooled by a pathological
// query (e.g. a subquery containing its own "from other_table" — the outer
// FROM-count check above would then see 2 occurrences and correctly reject
// it, but a maliciously crafted comment or string literal containing the
// literal text "from " could in principle throw off the count in either
// direction). Given playScatter ALSO requires isMutation/isDangerous to pass
// first (core's own multi-statement + dangerous-keyword denylist) and is
// SELECT-only end to end, the worst case of a bypass here is an unexpected
// (but still read-only, still non-dangerous) SELECT reaching scatterCore —
// not a write, not DDL, not credential exposure. This is flagged in the
// report as the one place a real SQL parser would be strictly safer than a
// regex; a conservative allowlist was judged sufficient for a demo console
// given that bounded blast radius, per this task's own fallback guidance.
// ============================================================================
