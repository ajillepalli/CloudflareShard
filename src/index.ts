import { CatalogDO } from "./catalog";
import { ShardDO, TENANT_SCAN_PAGE_SIZE } from "./shard";
import { CoordinatorDO } from "./coordinator";
import { json } from "./http";
import { hashKey, indexShardIdForKey } from "./hash";
import { checkAdminAuth } from "./auth";
import { log } from "./log";
import { extractCreateTableName, isDangerous, isDangerousSchema, isInternalTableName, isMutation, mutationTargetIsInternal, normalizeTableName } from "./sql-safety";
import {
  compileMutation,
  IDENTIFIER_RE,
  participantKey,
  rowKey,
  SYNTHETIC_INDEX_TABLE_PREFIX,
  UNSET_PARTITION_KEY_COLUMN,
  validateMutation,
  type StructuredMutation,
  type StructuredOperation,
} from "./structured-op";
import { sha256Hex } from "./auth";

export { CatalogDO, ShardDO, CoordinatorDO };

export interface Env {
  CATALOG: DurableObjectNamespace<CatalogDO>;
  SHARD: DurableObjectNamespace<ShardDO>;
  COORDINATOR: DurableObjectNamespace<CoordinatorDO>;
  ADMIN_TOKEN?: string;
  CATALOG_SHARD_COUNT?: string;
}

const DEFAULT_CATALOG_SHARD_COUNT = 4;
const SHARD_FANOUT_CONCURRENCY = 10;
const MAX_TX_PARTICIPANT_KEYS = 8;
// Codex final-review P1 #1: create-index's backfill holds the topology lock
// for its entire (synchronous, potentially long) scan-and-write loop, but
// only ever refreshed the lease once, at acquisition. A large table's
// backfill can run past the lock's 30s TTL with no renewal, letting the
// lease expire mid-backfill — a concurrent drain could then acquire it and
// start moving rows off a shard this backfill hasn't scanned yet (migration
// import doesn't create index entries, so those rows would be permanently
// missing from the new index). Re-heartbeat at a cadence far tighter than
// the TTL: once per data shard (bounding the gap even when a shard has very
// few rows) AND every N rows within a shard's scan (bounding a single huge
// shard).
const BACKFILL_HEARTBEAT_ROW_INTERVAL = 200;

/** Maps over items in bounded-size batches so a large shard count can't fire
 * unbounded simultaneous Durable Object calls in one request. */
async function batchedMap<T, R>(items: T[], concurrency: number, fn: (item: T) => Promise<R>): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    results.push(...(await Promise.all(batch.map(fn))));
  }
  return results;
}

type SqlRequest = {
  sql: string;
  params?: unknown[];
  table: string;
  tenantId: string;
  partitionKey?: string;
  requestId?: string;
};


function assertParamsArray(params: unknown): params is unknown[] {
  return Array.isArray(params);
}

/** Gate for admin endpoints that call ShardDO directly and so bypass CatalogDO's own auth check. */
function requireAdminAuth(env: Env, request: Request): Response | null {
  const authError = checkAdminAuth(env.ADMIN_TOKEN, request);
  return authError ? json({ error: authError.error }, authError.status) : null;
}

function catalogShardCount(env: Env): number {
  const parsed = env.CATALOG_SHARD_COUNT ? Number.parseInt(env.CATALOG_SHARD_COUNT, 10) : NaN;
  return Number.isInteger(parsed) && parsed > 0 ? parsed : DEFAULT_CATALOG_SHARD_COUNT;
}

/** The fixed, well-known set of catalog shard IDs. Computed, never looked up — this
 * sidesteps the bootstrapping problem of sharding the metadata store itself. */
function allCatalogShardIds(env: Env): string[] {
  return Array.from({ length: catalogShardCount(env) }, (_, i) => `catalog-${i}`);
}

/** Which catalog shard governs a given tenant. Pure function of tenantId — no lookup. */
function catalogShardIdForTenant(env: Env, tenantId: string): string {
  const count = catalogShardCount(env);
  return `catalog-${hashKey(tenantId) % count}`;
}

async function routeToCatalog(
  env: Env,
  catalogShardId: string,
  path: string,
  payload: unknown,
  authorization?: string,
): Promise<Response> {
  const id = env.CATALOG.idFromName(catalogShardId);
  const stub = env.CATALOG.get(id);
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (authorization) {
    headers.authorization = authorization;
  }
  return stub.fetch(`https://catalog.internal${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
}

/** Finds the first failed result from a catalog fan-out and builds the
 * standard error response for it, or returns null if everything succeeded. */
function firstCatalogFanOutFailure(
  results: Array<{ catalogShardId: string; res: Response; body: unknown }>,
  errorMessage: string,
): Response | null {
  const failed = results.find((r) => !r.res.ok);
  if (!failed) return null;
  return json(
    { error: errorMessage, catalogShardId: failed.catalogShardId, details: failed.body },
    failed.res.status,
  );
}

async function fanOutToAllCatalogs(
  env: Env,
  path: string,
  payloadFor: (catalogShardId: string) => unknown,
  authorization?: string,
): Promise<Array<{ catalogShardId: string; res: Response; body: unknown }>> {
  const catalogShardIds = allCatalogShardIds(env);
  return batchedMap(catalogShardIds, SHARD_FANOUT_CONCURRENCY, async (catalogShardId) => {
    const res = await routeToCatalog(env, catalogShardId, path, payloadFor(catalogShardId), authorization);
    const body = await res.json();
    return { catalogShardId, res, body };
  });
}

/** The single cluster-wide topology lock is held on catalog-0. Acquire it for a
 * topology operation; returns the operationId, or a Response to propagate to the
 * caller (409 TOPOLOGY_OPERATION_IN_PROGRESS when another op holds it, 503 if
 * catalog-0 is unreachable). */
async function acquireTopologyLock(env: Env, operationType: string): Promise<{ operationId: string } | Response> {
  const res = await routeToCatalog(env, "catalog-0", "/acquire-topology-lock", { operationType });
  if (res.status === 409) {
    return new Response(res.body, { status: 409, headers: res.headers });
  }
  if (!res.ok) {
    return json({ error: { code: "TOPOLOGY_LOCK_UNAVAILABLE", message: "Could not reach the topology lock (catalog-0).", fix: "Retry shortly." } }, 503);
  }
  const body = (await res.json()) as { operationId: string };
  return { operationId: body.operationId };
}

/** Best-effort release — if it fails, the lease TTL reclaims the lock. */
async function releaseTopologyLock(env: Env, operationId: string): Promise<void> {
  try {
    await routeToCatalog(env, "catalog-0", "/release-topology-lock", { operationId });
  } catch {
    // ignore — the lease expires on its own
  }
}

/** Codex final-review P1 #1: refresh this operation's lease mid-flight (a
 * long-running Worker-side loop — e.g. create-index's backfill — that isn't
 * ticked by CatalogDO's own alarm still needs to renew the same lock it
 * acquired up front). Returns false — LOCK LOST — on a non-2xx response
 * (expired, force-released, or reacquired by another operation) OR if
 * catalog-0 can't be reached at all; fail-safe, matching
 * heartbeatTopologyLockOrPark's convention in catalog.ts: an unconfirmable
 * lock must be treated the same as a lost one, never as "still fine". */
async function heartbeatTopologyLock(env: Env, operationId: string): Promise<boolean> {
  try {
    const res = await routeToCatalog(env, "catalog-0", "/heartbeat-topology-lock", { operationId });
    return res.ok;
  } catch {
    return false;
  }
}

async function routeToShard(env: Env, shardId: string, path: string, payload: unknown): Promise<Response> {
  const id = env.SHARD.idFromName(shardId);
  const stub = env.SHARD.get(id);
  return stub.fetch(`https://shard.internal${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

async function handleAdminInit(request: Request, env: Env): Promise<Response> {
  const payload = (await request.json()) as Record<string, unknown>;
  const authorization = request.headers.get("authorization") ?? undefined;
  const results = await fanOutToAllCatalogs(
    env,
    "/init",
    (catalogShardId) => ({ ...payload, catalogShardId, catalogShardCount: catalogShardCount(env) }),
    authorization,
  );
  const failed = firstCatalogFanOutFailure(results, "One or more catalog shards failed to initialize.");
  if (failed) return failed;
  return json({
    ok: true,
    catalogShardCount: results.length,
    catalogs: results.map((r) => ({ catalogShardId: r.catalogShardId, ...(r.body as object) })),
  });
}

async function handleAdminRegisterTable(request: Request, env: Env): Promise<Response> {
  const payload = (await request.json()) as Record<string, unknown> & {
    table?: string;
    schemaSql?: string;
    partitionKeyColumn?: string;
  };
  // Pre-landing review fix: every sibling route (handleAdminSetPartitionKeyColumn,
  // handleAdminCreateIndex) validates its identifiers against IDENTIFIER_RE before
  // using them; this route was missing that check even though payload.table flows
  // unvalidated into checkPartitionKeyUnique below, which interpolates it directly
  // into raw SQL text (PRAGMA table_info("${table}") etc.) sent to a shard's
  // /execute route. partitionKeyColumn is optional on this route, so it's only
  // validated when actually provided, matching the existing guard used below.
  if (typeof payload.table !== "string" || !IDENTIFIER_RE.test(payload.table)) {
    return json(
      { error: { code: "UNSAFE_IDENTIFIER", message: "table or partitionKeyColumn is not a valid identifier.", fix: "Use only letters, digits, and underscores, starting with a letter or underscore." } },
      400,
    );
  }
  if (typeof payload.partitionKeyColumn === "string" && payload.partitionKeyColumn.length > 0 && !IDENTIFIER_RE.test(payload.partitionKeyColumn)) {
    return json(
      { error: { code: "UNSAFE_IDENTIFIER", message: "table or partitionKeyColumn is not a valid identifier.", fix: "Use only letters, digits, and underscores, starting with a letter or underscore." } },
      400,
    );
  }
  // Review Tier 3: /admin/register-table stores schemaSql (if present) for
  // later use — a split target's backfill executes it verbatim to provision
  // the table (see handleAdminCreateTable's comment above /register-table's
  // fan-out, and CatalogDO.migratableTables/advanceMigration's backfill pass).
  // Without this check an admin could seed DDL here that /admin/create-table
  // itself would reject, and have it silently executed later.
  if (typeof payload.schemaSql === "string" && payload.schemaSql.length > 0) {
    if (!/^\s*create\s+table\b/i.test(payload.schemaSql)) {
      return json({ error: "schemaSql must be a CREATE TABLE statement." }, 400);
    }
    if (isDangerousSchema(payload.schemaSql)) {
      return json({ error: "schemaSql statement not permitted." }, 403);
    }
    const schemaTableName = extractCreateTableName(payload.schemaSql);
    if (schemaTableName === null || schemaTableName !== payload.table) {
      return json(
        {
          error: "schemaSql's CREATE TABLE name does not match table.",
          table: payload.table,
          schemaTableName,
        },
        400,
      );
    }
  }

  // Codex P1 fix (register-table trust bypass): the raw request body's
  // partitionKeyUnique must NEVER be forwarded — a caller could otherwise set
  // {partitionKeyColumn, partitionKeyUnique: true} with no actual unique
  // constraint on that column and completely bypass checkPartitionKeyUnique,
  // reopening the exact cross-tenant /v1/table-scan leak that check exists to
  // close. Always compute it ourselves, the same way /admin/create-table and
  // /admin/set-partition-key-column do. Unlike those two, this route is
  // metadata-only and doesn't know which shard(s) the table lives on, so pick
  // any one active shard (schema is uniform across shards for a table by
  // construction — see checkPartitionKeyUnique's doc comment). If no shards
  // exist yet, or listing them fails, fail closed (unverified = false):
  // registration itself must not hard-fail here (a table can legitimately be
  // registered before /admin/init has ever run), but nothing can be verified
  // as unique either.
  delete (payload as Record<string, unknown>).partitionKeyUnique;
  let partitionKeyUnique = false;
  if (typeof payload.partitionKeyColumn === "string" && payload.partitionKeyColumn.length > 0 && typeof payload.table === "string") {
    const listResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({}));
    const listFailed = firstCatalogFanOutFailure(listResults, "Failed to list shards.");
    if (!listFailed) {
      const shardIds = listResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);
      if (shardIds.length > 0) {
        partitionKeyUnique = await checkPartitionKeyUnique(env, shardIds[0], payload.table, payload.partitionKeyColumn);
      }
    }
  }

  const authorization = request.headers.get("authorization") ?? undefined;
  const results = await fanOutToAllCatalogs(env, "/register-table", () => ({ ...payload, partitionKeyUnique }), authorization);
  const failed = firstCatalogFanOutFailure(results, "One or more catalog shards failed to register the table.");
  if (failed) return failed;
  return json({ ok: true, catalogShardCount: results.length });
}

/** Codex P1 fix (cross-tenant table-scan leak): verifies that `partitionKeyColumn`
 * on `table` (already created/present on `shardId`) is backed by a UNIQUE
 * constraint or is the table's SOLE primary-key column. /v1/table-scan's
 * per-shard JOIN against __cf_row_owners matches purely on partition-key
 * VALUE (see the /tenant-scan-page comment in shard.ts) — if the column isn't
 * guaranteed unique, two different tenants' rows can share a value and the
 * join would attribute both rows to whichever tenant currently owns that key
 * in __cf_row_owners, leaking one tenant's row to another. A composite
 * PRIMARY KEY where partitionKeyColumn is only one part is NOT sufficient —
 * the value alone must be guaranteed unique on its own. Checked against a
 * single representative shard: schema is uniform across shards for a table by
 * construction, so one check suffices. Fails closed (false) on any
 * introspection error — callers must treat "unable to verify" as "not safe to
 * scan", matching table_rules.partition_key_unique's fail-closed default. */
async function checkPartitionKeyUnique(env: Env, shardId: string, table: string, partitionKeyColumn: string): Promise<boolean> {
  const tableInfoRes = await routeToShard(env, shardId, "/execute", {
    sql: `PRAGMA table_info("${table}")`,
    requestId: `partition-key-unique-tableinfo-${table}-${crypto.randomUUID()}`,
    isMutation: false,
  });
  if (!tableInfoRes.ok) return false;
  const tableInfoBody = (await tableInfoRes.json()) as { rows?: Array<{ name: string; pk: number }> };
  const columns = tableInfoBody.rows ?? [];
  const pkColumns = columns.filter((c) => c.pk !== 0);
  if (pkColumns.length === 1 && pkColumns[0].name === partitionKeyColumn) {
    return true;
  }

  const indexListRes = await routeToShard(env, shardId, "/execute", {
    sql: `PRAGMA index_list("${table}")`,
    requestId: `partition-key-unique-indexlist-${table}-${crypto.randomUUID()}`,
    isMutation: false,
  });
  if (!indexListRes.ok) return false;
  const indexListBody = (await indexListRes.json()) as { rows?: Array<{ name: string; unique: number }> };
  const uniqueIndexes = (indexListBody.rows ?? []).filter((i) => i.unique === 1);
  for (const idx of uniqueIndexes) {
    const indexInfoRes = await routeToShard(env, shardId, "/execute", {
      sql: `PRAGMA index_info("${idx.name}")`,
      requestId: `partition-key-unique-indexinfo-${table}-${idx.name}-${crypto.randomUUID()}`,
      isMutation: false,
    });
    if (!indexInfoRes.ok) continue;
    const indexInfoBody = (await indexInfoRes.json()) as { rows?: Array<{ name: string }> };
    const idxColumns = indexInfoBody.rows ?? [];
    if (idxColumns.length !== 1 || idxColumns[0].name !== partitionKeyColumn) {
      continue;
    }

    // Codex P1 fix (partial-unique-index bypass): PRAGMA index_list reports
    // unique=1 for a PARTIAL unique index too (e.g. `CREATE UNIQUE INDEX ux ON
    // t(col) WHERE active = 1`), and PRAGMA index_info never exposes the
    // predicate — so without this check a partial unique index would be
    // accepted as full-table uniqueness when it isn't (duplicate values ARE
    // allowed for rows outside the predicate), reopening the exact
    // cross-tenant leak this function exists to close. SQLite doesn't expose
    // "is this index partial" via any PRAGMA boolean; the reliable signal is
    // the index's own stored CREATE INDEX text in sqlite_master (same
    // regex-text-parsing pattern as extractCreateTableName in sql-safety.ts).
    // A NULL sql (an auto-created index backing a UNIQUE column constraint,
    // or an implicit PRIMARY KEY index) is never partial, so that case is
    // safe to accept.
    const indexSqlRes = await routeToShard(env, shardId, "/execute", {
      sql: `SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?`,
      params: [idx.name],
      requestId: `partition-key-unique-indexsql-${table}-${idx.name}-${crypto.randomUUID()}`,
      isMutation: false,
    });
    if (!indexSqlRes.ok) continue;
    const indexSqlBody = (await indexSqlRes.json()) as { rows?: Array<{ sql: string | null }> };
    const indexSql = indexSqlBody.rows?.[0]?.sql ?? null;
    if (indexSql !== null && /\bwhere\b/i.test(indexSql)) {
      // Partial index — not sufficient on its own. Keep checking other
      // unique indexes rather than returning false immediately, in case a
      // later (non-partial) unique index on this same column also exists.
      continue;
    }
    return true;
  }
  return false;
}

async function handleAdminCreateTable(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as {
    table?: string;
    schema?: string;
    partitioning?: string;
    partitionKeyColumn?: string;
  };
  if (!body.table || !body.schema) {
    return json({ error: "Missing table or schema." }, 400);
  }
  if (!body.partitionKeyColumn) {
    return json(
      {
        error: {
          code: "MISSING_PARTITION_KEY_COLUMN",
          message: "Missing partitionKeyColumn.",
          fix: "Provide the column name that holds each row's partition key.",
        },
      },
      400,
    );
  }
  if (!IDENTIFIER_RE.test(body.partitionKeyColumn)) {
    return json(
      { error: { code: "UNSAFE_IDENTIFIER", message: "partitionKeyColumn is not a valid identifier.", fix: "Use only letters, digits, and underscores, starting with a letter or underscore." } },
      400,
    );
  }
  if (!/^\s*create\s+table\b/i.test(body.schema)) {
    return json({ error: "schema must be a CREATE TABLE statement." }, 400);
  }
  if (isDangerousSchema(body.schema)) {
    return json({ error: "schema statement not permitted." }, 403);
  }

  const schemaTableName = extractCreateTableName(body.schema);
  if (schemaTableName === null || schemaTableName !== body.table) {
    return json(
      {
        error: "schema's CREATE TABLE name does not match body.table.",
        table: body.table,
        schemaTableName,
      },
      400,
    );
  }

  const listResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({}));
  const listFailed = firstCatalogFanOutFailure(listResults, "Failed to list shards.");
  if (listFailed) return listFailed;
  const shardIds = listResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);
  if (shardIds.length === 0) {
    return json({ error: { code: "NO_SHARDS", message: "No shards exist yet.", fix: "Call /admin/init first." } }, 400);
  }

  // Create on every shard BEFORE registering in table_rules — if anything
  // below fails (shard execution, or the partitionKeyColumn/schema mismatch
  // check), the table was never registered, so rollback is just dropping the
  // physical tables, not also having to unregister catalog-level metadata.
  const shardResults = await batchedMap(shardIds, SHARD_FANOUT_CONCURRENCY, async (shardId) => {
    const res = await routeToShard(env, shardId, "/execute", {
      sql: body.schema,
      requestId: `create-table-${body.table}-${shardId}`,
      isMutation: true,
    });
    return { shardId, ok: res.ok, status: res.status, body: await res.json() };
  });
  const shardFailed = shardResults.find((r) => !r.ok);
  if (shardFailed) {
    return json(
      { error: "Failed to create table on one or more shards.", shardId: shardFailed.shardId, details: shardFailed.body },
      shardFailed.status,
    );
  }

  // Validate the declared partitionKeyColumn actually exists in the schema
  // that was just applied, via SQLite's own PRAGMA table_info introspection
  // on one representative shard (not a hand-rolled DDL parser — the same
  // schema was applied identically to every shard, so one check suffices).
  const introspectRes = await routeToShard(env, shardIds[0], "/execute", {
    sql: `PRAGMA table_info("${body.table}")`,
    requestId: `create-table-introspect-${body.table}-${Date.now()}`,
    isMutation: false,
  });
  if (introspectRes.ok) {
    const introspectBody = (await introspectRes.json()) as { rows?: Array<{ name: string }> };
    const columns = (introspectBody.rows ?? []).map((c) => c.name);
    if (!columns.includes(body.partitionKeyColumn)) {
      // Unique per attempt (not a stable per-table key like the create
      // requestId above): DROP TABLE IF EXISTS is already idempotent at the
      // SQL level, and a stable rollback requestId would itself become a
      // poisoned idempotency-cache entry on a second failed create-table
      // attempt for the same table — the second rollback would replay the
      // FIRST rollback's cached "success" instead of actually re-executing,
      // leaving the just-recreated table behind despite the 400 response.
      // crypto.randomUUID(), not Date.now(): two rollback attempts landing
      // in the same millisecond would otherwise collide on the same key.
      const rollbackAttemptId = crypto.randomUUID();
      await batchedMap(shardIds, SHARD_FANOUT_CONCURRENCY, async (shardId) => {
        await routeToShard(env, shardId, "/execute", {
          sql: `DROP TABLE IF EXISTS "${body.table}"`,
          requestId: `create-table-rollback-${body.table}-${shardId}-${rollbackAttemptId}`,
          isMutation: true,
        });
        // Undo the idempotency-cache side effect together with the DDL: the
        // "success" cached under the original create requestId is now a lie
        // (the table it recorded creating no longer exists), so a retry must
        // genuinely re-execute rather than replay that cached result.
        await routeToShard(env, shardId, "/invalidate-request", {
          requestId: `create-table-${body.table}-${shardId}`,
        });
      });
      return json(
        {
          error: {
            code: "COLUMN_NOT_IN_SCHEMA",
            message: `partitionKeyColumn ${body.partitionKeyColumn} does not exist on the created table ${body.table}.`,
            fix: `Choose one of the schema's actual columns: ${columns.join(", ")}.`,
          },
        },
        400,
      );
    }
  }

  // Codex P1 fix: verify partitionKeyColumn is UNIQUE/PRIMARY KEY on the
  // table just created, so /v1/table-scan can later be safely allowed for it
  // (see checkPartitionKeyUnique's doc comment). Checked against the same
  // representative shard as the column-exists check above.
  const partitionKeyUnique = await checkPartitionKeyUnique(env, shardIds[0], body.table, body.partitionKeyColumn);

  const registerResults = await fanOutToAllCatalogs(
    env,
    "/register-table",
    // schemaSql: Milestone 3, Chunk 5 — captured so migration backfill can
    // provision this table on a shard created after this fan-out ran (e.g.
    // a split target). provenanceComplete: true — a table just created here
    // has zero legacy rows (nothing predates its own existence), so it starts
    // fully backfilled rather than 0 (see table_rules.provenance_complete's
    // ensureSchema comment in catalog.ts).
    () => ({
      table: body.table,
      partitioning: body.partitioning,
      partitionKeyColumn: body.partitionKeyColumn,
      schemaSql: body.schema,
      provenanceComplete: true,
      partitionKeyUnique,
    }),
    request.headers.get("authorization") ?? undefined,
  );
  const registerFailed = firstCatalogFanOutFailure(registerResults, "Failed to register table.");
  if (registerFailed) return registerFailed;

  return json({ ok: true, table: body.table, shardsApplied: shardResults.length });
}

async function handleAdminSetPartitionKeyColumn(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as { table?: string; partitionKeyColumn?: string };
  if (!body.table || !body.partitionKeyColumn) {
    return json(
      { error: { code: "MISSING_FIELDS", message: "Missing table or partitionKeyColumn.", fix: "Provide both table and partitionKeyColumn." } },
      400,
    );
  }
  if (!IDENTIFIER_RE.test(body.table) || !IDENTIFIER_RE.test(body.partitionKeyColumn)) {
    return json(
      { error: { code: "UNSAFE_IDENTIFIER", message: "table or partitionKeyColumn is not a valid identifier.", fix: "Use only letters, digits, and underscores, starting with a letter or underscore." } },
      400,
    );
  }

  const listResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({}));
  const listFailed = firstCatalogFanOutFailure(listResults, "Failed to list shards.");
  if (listFailed) return listFailed;
  const shardIds = listResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);
  if (shardIds.length === 0) {
    return json({ error: { code: "NO_SHARDS", message: "No shards exist yet.", fix: "Call /admin/init first." } }, 400);
  }

  const introspectRes = await routeToShard(env, shardIds[0], "/execute", {
    sql: `PRAGMA table_info("${body.table}")`,
    requestId: `set-partition-key-column-introspect-${body.table}-${Date.now()}`,
    isMutation: false,
  });
  if (!introspectRes.ok) {
    return new Response(introspectRes.body, { status: introspectRes.status, headers: introspectRes.headers });
  }
  const introspectBody = (await introspectRes.json()) as { rows?: Array<{ name: string }> };
  const columns = (introspectBody.rows ?? []).map((c) => c.name);
  if (!columns.includes(body.partitionKeyColumn)) {
    return json(
      {
        error: {
          code: "COLUMN_NOT_IN_SCHEMA",
          message: `Column ${body.partitionKeyColumn} does not exist on table ${body.table}.`,
          fix: `Choose one of the table's actual columns: ${columns.join(", ")}.`,
        },
      },
      400,
    );
  }

  // Codex P1 fix: this is the OTHER place a table's partitionKeyColumn is
  // established (for tables carrying the __unset__ sentinel from before this
  // validation existed) — same verification as /admin/create-table, since a
  // stale "unique" flag for a PREVIOUS partitionKeyColumn must never carry
  // forward to a newly-set one.
  const partitionKeyUnique = await checkPartitionKeyUnique(env, shardIds[0], body.table, body.partitionKeyColumn);

  const results = await fanOutToAllCatalogs(
    env,
    "/set-partition-key-column",
    () => ({ ...body, partitionKeyUnique }),
    request.headers.get("authorization") ?? undefined,
  );
  const failed = firstCatalogFanOutFailure(results, "Failed to update partitionKeyColumn on one or more catalog shards.");
  if (failed) return failed;
  return json({ ok: true, table: body.table, partitionKeyColumn: body.partitionKeyColumn });
}

/** Milestone 2, Chunk 1. Registers a secondary index and backfills it against
 * existing rows. Index entries live on a shard chosen by hashing
 * (table, indexName, indexKeyJson) — independent of the base row's own
 * shard (see the Milestone 2 design doc's index-placement decision) — so
 * backfill writes each entry to its own computed shard, not the base row's
 * shard. Single-pass, not chunked: acceptable for this milestone's stated
 * pre-product scale (see design doc Premise 1); a very large table could hit
 * a Worker CPU-time limit, a known simplification, not a silent bug. */
async function handleAdminCreateIndex(request: Request, env: Env): Promise<Response> {
  // Topology lock (Stage 2): create-index mutates every catalog's index_rules
  // and races drain ring-evacuation; serialize it against all other topology
  // ops. Held for the whole (synchronous) registration + backfill, released in
  // finally on any exit.
  const lock = await acquireTopologyLock(env, "create-index");
  if (lock instanceof Response) return lock;
  try {
    return await handleAdminCreateIndexLocked(request, env, lock.operationId);
  } finally {
    await releaseTopologyLock(env, lock.operationId);
  }
}

async function handleAdminCreateIndexLocked(request: Request, env: Env, operationId: string): Promise<Response> {
  const body = (await request.json()) as { indexName?: string; table?: string; columns?: string[] };
  if (!body.indexName || !body.table || !body.columns || body.columns.length === 0) {
    return json(
      { error: { code: "MISSING_FIELDS", message: "Missing indexName, table, or columns.", fix: "Provide indexName, table, and a non-empty columns array." } },
      400,
    );
  }
  if (!IDENTIFIER_RE.test(body.indexName)) {
    return json({ error: { code: "UNSAFE_IDENTIFIER", message: "indexName is not a valid identifier." } }, 400);
  }
  for (const col of body.columns) {
    if (!IDENTIFIER_RE.test(col)) {
      return json({ error: { code: "UNSAFE_IDENTIFIER", message: `Unsafe identifier in columns: ${col}` } }, 400);
    }
  }

  // table_rules is fanned out identically to every catalog shard, so
  // catalog-0's view is representative (same pattern as handleAdminListTables).
  const tablesRes = await routeToCatalog(env, "catalog-0", "/list-tables", {}, request.headers.get("authorization") ?? undefined);
  if (!tablesRes.ok) {
    return new Response(tablesRes.body, { status: tablesRes.status, headers: tablesRes.headers });
  }
  const tablesBody = (await tablesRes.json()) as {
    tables: Array<{ table_name: string; partition_key_column: string }>;
  };
  const tableInfo = tablesBody.tables.find((t) => t.table_name === body.table);
  if (!tableInfo) {
    return json(
      { error: { code: "TABLE_NOT_REGISTERED", message: `Table ${body.table} is not registered.`, fix: "Call /admin/create-table first." } },
      404,
    );
  }
  if (tableInfo.partition_key_column === UNSET_PARTITION_KEY_COLUMN) {
    return json(
      { error: { code: "PARTITION_KEY_COLUMN_UNSET", message: `Table ${body.table} has not been upgraded with a partition key column.`, fix: "Call /admin/set-partition-key-column first." } },
      409,
    );
  }
  const pkCol = tableInfo.partition_key_column;

  // The index's PLACEMENT RING is active shards only: a draining shard is
  // about to be evacuated and must never be pinned into a new ring (it would
  // strand entries on a shard headed for decommission). shardIds is captured
  // here and persisted as index_rules.placement_ring_json for the index's
  // whole lifetime.
  const listResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({}));
  const listFailed = firstCatalogFanOutFailure(listResults, "Failed to list shards.");
  if (listFailed) return listFailed;
  const shardIds = listResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);
  if (shardIds.length === 0) {
    return json({ error: { code: "NO_SHARDS", message: "No shards exist yet.", fix: "Call /admin/init first." } }, 400);
  }

  // Codex full-PR review P1 (silent index miss): the BACKFILL SCAN, by
  // contrast, must cover active + DRAINING shards. A draining shard still
  // physically holds its base rows until its vbuckets finish migrating; an
  // index created after the shard is marked draining but before that migration
  // would, with an active-only scan, never index those existing rows —
  // /v1/index-query would then silently miss them. The entries it finds are
  // still PLACED on the active ring (indexShardIdForKey over shardIds) and
  // carry tenant_id, so hydration re-routes correctly after the base rows
  // later migrate off the draining shard.
  const dataListResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({ includeDraining: true }));
  const dataListFailed = firstCatalogFanOutFailure(dataListResults, "Failed to list data shards.");
  if (dataListFailed) return dataListFailed;
  const dataShardIds = dataListResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);

  // Validate the requested columns actually exist on the schema (mirrors
  // handleAdminCreateTable's PRAGMA table_info check).
  const introspectRes = await routeToShard(env, shardIds[0], "/execute", {
    sql: `PRAGMA table_info("${body.table}")`,
    requestId: `create-index-introspect-${body.indexName}-${crypto.randomUUID()}`,
    isMutation: false,
  });
  if (introspectRes.ok) {
    const introspectBody = (await introspectRes.json()) as { rows?: Array<{ name: string }> };
    const actualColumns = (introspectBody.rows ?? []).map((c) => c.name);
    const missing = body.columns.filter((c) => !actualColumns.includes(c));
    if (missing.length > 0) {
      return json(
        {
          error: {
            code: "COLUMN_NOT_IN_SCHEMA",
            message: `Column(s) not in schema: ${missing.join(", ")}.`,
            fix: `Choose from the table's actual columns: ${actualColumns.join(", ")}.`,
          },
        },
        400,
      );
    }
  }

  const columns = body.columns;
  const indexName = body.indexName;
  const table = body.table;

  // Register BEFORE backfilling, not after (eng-review fix: registering
  // last left a real gap, not just a staleness window — a row written
  // between backfill's scan of its shard and registration would be missed
  // by the scan AND never trigger Chunk 2's async maintenance, since
  // route.indexes only includes already-registered indexes. Registering
  // first means any concurrent write during backfill is already covered by
  // Chunk 2's async path; backfill's own scan may then redundantly re-write
  // the same row, which is harmless since __cf_indexes writes are already
  // idempotent INSERT OR REPLACE). CatalogDO's /create-index is idempotent
  // on the same table+columns specifically so a retry after a partial
  // backfill failure below can call this endpoint again.
  // Codex round-16 defense-in-depth: re-verify each shard in the captured
  // ring is STILL active RIGHT BEFORE it is persisted as the placement ring —
  // several awaits (introspect's PRAGMA round trip, the table-rules lookups
  // above) separate the initial /list-shards snapshot (shardIds) from here,
  // and a concurrent drain — normally excluded by the topology lock, but this
  // check holds regardless of whether the lock did its job — could have moved
  // one of them to 'draining' in that window. Pinning a draining shard into a
  // NEW ring is exactly what the lock exists to prevent, so exclude any shard
  // that's no longer active rather than trusting the stale snapshot; a
  // smaller-but-still-valid active-only ring is always safe.
  const reverifyResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({}));
  const reverifyFailed = firstCatalogFanOutFailure(reverifyResults, "Failed to re-verify active shards before registration.");
  if (reverifyFailed) return reverifyFailed;
  const stillActiveShardIds = new Set(reverifyResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds));
  const verifiedRingShardIds = shardIds.filter((s) => stillActiveShardIds.has(s));
  if (verifiedRingShardIds.length === 0) {
    return json(
      {
        error: {
          code: "NO_SHARDS",
          message: "No active shards remain to place this index's ring on — they all went draining since the initial check.",
          fix: "Retry once shard topology stabilizes.",
        },
      },
      400,
    );
  }

  // Milestone 3, Chunk 2: shardIds (now re-verified into verifiedRingShardIds)
  // IS this index's pinned placement ring — captured here, at creation time,
  // and sent to CatalogDO to persist as index_rules.placement_ring_json.
  // Every later placement computation for this index (write-path maintenance,
  // /v1/index-query, this backfill loop itself) must reuse this exact array
  // for the index's entire lifetime.
  const registerResults = await fanOutToAllCatalogs(
    env,
    "/create-index",
    () => ({ indexName, table, columns, placementRing: verifiedRingShardIds }),
    request.headers.get("authorization") ?? undefined,
  );
  const registerFailed = firstCatalogFanOutFailure(registerResults, "Failed to register index.");
  if (registerFailed) return registerFailed;

  // Codex round-14 P2: backfill placement must use the catalog's PERSISTED ring
  // for this index — never the locally-recomputed active set. On a RETRY of
  // /admin/create-index (idempotent registration), `shardIds` reflects the live
  // active set at retry time, which can differ from the ring pinned by the FIRST
  // call; placing over `shardIds` would then write entries to shards
  // /v1/index-query (which reads the pinned ring) never looks at. The persisted
  // ring equals `verifiedRingShardIds` on the first call and the original pinned
  // ring on a retry; fall back to `verifiedRingShardIds` (never the pre-verification
  // `shardIds`) only if it can't be fetched.
  const persistedRing = await fetchIndexRing(env, indexName);
  const placementRing = persistedRing.length > 0 ? persistedRing : verifiedRingShardIds;

  // Backfill: scan every data-holding shard's existing rows for this table
  // (active + draining — see dataShardIds above), compute each row's index
  // entry, and write it to its own computed index shard (placed on the active
  // ring via indexShardIdForKey(..., shardIds)).
  //
  // Codex final-review P1 #1: this loop holds the topology lock acquired once
  // in handleAdminCreateIndex, for its ENTIRE (synchronous) duration — a large
  // table can run well past the lock's 30s TTL with no renewal otherwise. Two
  // heartbeat points below re-confirm the lease: once per shard (so even a
  // backfill spread across many small shards keeps refreshing) and every
  // BACKFILL_HEARTBEAT_ROW_INTERVAL rows (so a single huge shard does too). A
  // failed heartbeat means the lease is gone — expired, force-released, or
  // reacquired by another topology op — so this ABORTS rather than risk
  // writing more index entries while a concurrent drain/migration may already
  // be moving rows this backfill hasn't scanned yet off their source shard
  // (migration import never creates index entries, so those rows would be
  // silently, permanently missing from the new index).
  let rowsSinceHeartbeat = 0;
  for (const shardId of dataShardIds) {
    if (!(await heartbeatTopologyLock(env, operationId))) {
      return json(
        {
          error: {
            code: "TOPOLOGY_LOCK_LOST",
            message: `The topology lock backing this backfill was lost before scanning shard ${shardId} (expired, force-released, or reacquired by another operation).`,
            fix: "Retry /admin/create-index once any concurrent topology operation completes (idempotent on registration and on already-written entries).",
          },
        },
        409,
      );
    }
    const safeTable = `"${table}"`;
    const selectCols = [pkCol, ...columns].map((c) => `"${c}"`).join(", ");
    const scanRes = await routeToShard(env, shardId, "/execute", {
      sql: `SELECT ${selectCols} FROM ${safeTable}`,
      requestId: `create-index-backfill-scan-${indexName}-${shardId}-${crypto.randomUUID()}`,
      isMutation: false,
    });
    if (!scanRes.ok) {
      return json(
        { error: { code: "BACKFILL_SCAN_FAILED", message: `Failed to scan shard ${shardId} for backfill.` } },
        500,
      );
    }
    const scanBody = (await scanRes.json()) as { rows: Array<Record<string, unknown>> };
    for (const row of scanBody.rows) {
      rowsSinceHeartbeat += 1;
      if (rowsSinceHeartbeat >= BACKFILL_HEARTBEAT_ROW_INTERVAL) {
        rowsSinceHeartbeat = 0;
        if (!(await heartbeatTopologyLock(env, operationId))) {
          return json(
            {
              error: {
                code: "TOPOLOGY_LOCK_LOST",
                message: `The topology lock backing this backfill was lost partway through shard ${shardId} (expired, force-released, or reacquired by another operation).`,
                fix: "Retry /admin/create-index once any concurrent topology operation completes (idempotent on registration and on already-written entries).",
              },
            },
            409,
          );
        }
      }
      const partitionKey = String(row[pkCol]);

      // Re-read this row's CURRENT values immediately before writing its
      // index entry, instead of trusting the value captured by the bulk
      // scan above (eng-review fix). Registration already happened, so a
      // concurrent /v1/mutate on this row runs its own async index write
      // concurrently with backfill; without this re-read, backfill could
      // clobber that fresher write with the stale value it scanned earlier
      // (a wide window spanning the whole scan+loop). Re-reading narrows the
      // hazard to a single read-then-write round trip — the same order of
      // race the rest of the async index-maintenance path already accepts
      // elsewhere, not a new or larger one. The row may have been deleted
      // since the scan; skip it if so rather than indexing stale data.
      //
      // Milestone 3, Chunk 2: also joins __cf_row_owners (Chunk 0) for this
      // row's tenant_id — index-topology v2 needs the OWNING tenant identity
      // to re-route hydration reads at query time (vbucket =
      // hashKey(tenant_id:table:partition_key) % total_vbuckets), since the
      // base row's own columns carry no tenant identity (docs/SPEC.md §14).
      const freshRes = await routeToShard(env, shardId, "/execute", {
        sql: `SELECT ${selectCols}, ro.tenant_id AS __cf_tenant_id FROM ${safeTable} b LEFT JOIN __cf_row_owners ro ON ro.table_name = ? AND ro.partition_key = b."${pkCol}" WHERE b."${pkCol}" = ?`,
        params: [table, row[pkCol]],
        requestId: `create-index-backfill-refresh-${indexName}-${crypto.randomUUID()}`,
        isMutation: false,
      });
      if (!freshRes.ok) {
        return json(
          { error: { code: "BACKFILL_SCAN_FAILED", message: `Failed to refresh row for partitionKey ${partitionKey} during backfill.` } },
          500,
        );
      }
      const freshBody = (await freshRes.json()) as { rows: Array<Record<string, unknown> & { __cf_tenant_id: string | null }> };
      const freshRow = freshBody.rows[0];
      if (!freshRow) continue;

      // No __cf_row_owners entry for this row — it predates Milestone 3
      // Chunk 0's provenance tracking (or Chunk 1's re-attribution hasn't
      // run yet) and its tenant identity can't be safely recovered here.
      // Rejecting rather than guessing/defaulting to '' keeps a bad tenant_id
      // from ever landing in __cf_indexes, where it would silently misroute
      // hydration forever instead of failing loudly once, up front.
      if (!freshRow.__cf_tenant_id) {
        return json(
          {
            error: {
              code: "PROVENANCE_MISSING_FOR_INDEX",
              message: `Row ${partitionKey} on shard ${shardId}, table ${table} has no row-provenance entry (__cf_row_owners) — its tenant identity can't be determined for index placement.`,
              fix: "Run /admin/backfill-provenance for this shard, then retry /admin/create-index (idempotent on registration and on already-written entries).",
            },
          },
          409,
        );
      }
      const tenantId = freshRow.__cf_tenant_id;

      const indexKeyJson = JSON.stringify(columns.map((c) => freshRow[c] ?? null));
      // Codex round-14 P2: place over the PERSISTED ring, and on INDEX_RING_FENCED
      // re-resolve to the substitute instead of hard-failing (a drain may have
      // fenced a shard after this /admin/create-index captured the ring).
      const wrote = await backfillWriteIndexEntry(env, table, indexName, indexKeyJson, partitionKey, shardId, tenantId, placementRing);
      if (!wrote) {
        return json(
          { error: { code: "BACKFILL_WRITE_FAILED", message: `Failed to write index entry for partitionKey ${partitionKey}.` } },
          500,
        );
      }
    }
  }

  // Backfill fully succeeded on every shard — flip the index from 'building'
  // to 'ready' so /v1/index-query stops rejecting reads against it. If this
  // fan-out fails, the index stays 'building'; a retry of this whole
  // /admin/create-index call (idempotent on registration, redundant-but-safe
  // on backfill) will attempt it again.
  const readyResults = await fanOutToAllCatalogs(
    env,
    "/mark-index-ready",
    () => ({ indexName }),
    request.headers.get("authorization") ?? undefined,
  );
  const readyFailed = firstCatalogFanOutFailure(readyResults, "Failed to mark index ready.");
  if (readyFailed) return readyFailed;

  return json({ ok: true, indexName, table, columns });
}

async function handleAdminSplitVbucket(request: Request, env: Env): Promise<Response> {
  const payload = (await request.json()) as { catalogShardId?: string };
  if (!payload.catalogShardId) {
    return json({ error: "Missing catalogShardId. vBucket numbering is local to a catalog shard." }, 400);
  }
  // Stage 2/3: acquire the topology lock for the split's START, then HAND IT
  // OFF to the vbucket-migration state machine on success — CatalogDO stores
  // the operationId on the vbucket_map row and its alarm heartbeats/releases
  // it for the migration's whole multi-tick duration (Stage 3). Only release
  // here if the catalog call did NOT actually start a migration (nothing to
  // hand off to, e.g. MIGRATION_IN_PROGRESS or a validation failure).
  const lock = await acquireTopologyLock(env, "split-vbucket");
  if (lock instanceof Response) return lock;
  let handedOff = false;
  try {
    const res = await routeToCatalog(
      env,
      payload.catalogShardId,
      "/split-vbucket",
      { ...payload, operationId: lock.operationId },
      request.headers.get("authorization") ?? undefined,
    );
    handedOff = res.ok;
    return new Response(res.body, { status: res.status, headers: res.headers });
  } finally {
    if (!handedOff) await releaseTopologyLock(env, lock.operationId);
  }
}

async function handleAdminMigrateVbucket(request: Request, env: Env): Promise<Response> {
  const payload = (await request.json()) as { catalogShardId?: string };
  if (!payload.catalogShardId) {
    return json({ error: "Missing catalogShardId. vBucket numbering is local to a catalog shard." }, 400);
  }
  // Same hand-off pattern as split-vbucket above.
  const lock = await acquireTopologyLock(env, "migrate-vbucket");
  if (lock instanceof Response) return lock;
  let handedOff = false;
  try {
    const res = await routeToCatalog(env, payload.catalogShardId, "/migrate-vbucket", { ...payload, operationId: lock.operationId }, request.headers.get("authorization") ?? undefined);
    handedOff = res.ok;
    return new Response(res.body, { status: res.status, headers: res.headers });
  } finally {
    if (!handedOff) await releaseTopologyLock(env, lock.operationId);
  }
}

/** Milestone 3, Chunk 4: thin forwarders to the owning catalog shard's
 * migration endpoints — the catalog owns the state machine and drives the
 * shard-level export/import/fence orchestration from its own alarm. Read-only
 * status / abort forwarders do NOT take the topology lock. */
function makeCatalogMigrationForwarder(path: string): (request: Request, env: Env) => Promise<Response> {
  return async (request, env) => {
    const payload = (await request.json()) as { catalogShardId?: string };
    if (!payload.catalogShardId) {
      return json({ error: "Missing catalogShardId. vBucket numbering is local to a catalog shard." }, 400);
    }
    const res = await routeToCatalog(env, payload.catalogShardId, path, payload, request.headers.get("authorization") ?? undefined);
    return new Response(res.body, { status: res.status, headers: res.headers });
  };
}

const handleAdminMigrateVbucketStatus = makeCatalogMigrationForwarder("/migrate-vbucket-status");
const handleAdminMigrateVbucketAbort = makeCatalogMigrationForwarder("/migrate-vbucket-abort");
// Milestone 3, Chunk 5: progress of a shard drain's two evacuation loops.
const handleAdminDrainShardStatus = makeCatalogMigrationForwarder("/drain-shard-status");

async function handleAdminStatus(request: Request, env: Env): Promise<Response> {
  const authorization = request.headers.get("authorization") ?? undefined;
  const results = await fanOutToAllCatalogs(env, "/status", () => ({}), authorization);
  const failed = firstCatalogFanOutFailure(results, "One or more catalog shards failed to report status.");
  if (failed) return failed;
  type CatalogStatus = {
    catalogShardId: string;
    initialized: boolean;
    shards?: { total: number; active: number; draining: number };
  };
  const catalogs: CatalogStatus[] = results.map((r) => ({
    catalogShardId: r.catalogShardId,
    ...(r.body as object),
  })) as CatalogStatus[];
  const initialized = catalogs.every((c) => c.initialized);
  const totals = catalogs.reduce(
    (acc, c) => {
      const shards = c.shards;
      if (shards) {
        acc.total += shards.total;
        acc.active += shards.active;
        acc.draining += shards.draining;
      }
      return acc;
    },
    { total: 0, active: 0, draining: 0 },
  );
  return json({ initialized, catalogShardCount: results.length, shards: totals, catalogs });
}

async function handleAdminListTables(request: Request, env: Env): Promise<Response> {
  // table_rules are fanned out identically to every catalog shard by
  // /admin/register-table, so catalog-0 is representative of all of them.
  const res = await routeToCatalog(
    env,
    "catalog-0",
    "/list-tables",
    {},
    request.headers.get("authorization") ?? undefined,
  );
  return new Response(res.body, { status: res.status, headers: res.headers });
}

async function handleAdminListIndexes(request: Request, env: Env): Promise<Response> {
  // index_rules are fanned out identically to every catalog shard by
  // /admin/create-index, so catalog-0 is representative of all of them.
  const res = await routeToCatalog(
    env,
    "catalog-0",
    "/list-indexes",
    {},
    request.headers.get("authorization") ?? undefined,
  );
  return new Response(res.body, { status: res.status, headers: res.headers });
}

/** Milestone 2, Chunk 6. Unregisters the index in CatalogDO first (fanned to
 * every catalog shard) so /v1/index-query and /lookup-index start rejecting
 * it immediately, then best-effort deletes its physical __cf_indexes rows
 * across every shard — the index could be on any shard given hash-based
 * placement, so this fans out rather than targeting one. */
async function handleAdminDropIndex(request: Request, env: Env): Promise<Response> {
  // Topology lock (Stage 2): drop-index mutates every catalog's index_rules and
  // deletes physical entries; serialize it against all other topology ops.
  const lock = await acquireTopologyLock(env, "drop-index");
  if (lock instanceof Response) return lock;
  try {
    return await handleAdminDropIndexLocked(request, env);
  } finally {
    await releaseTopologyLock(env, lock.operationId);
  }
}

async function handleAdminDropIndexLocked(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as { indexName?: string };
  if (!body.indexName) {
    return json({ error: "Missing indexName" }, 400);
  }

  // Milestone 3, Chunk 2: fetch this index's pinned placement ring BEFORE
  // unregistering (the ring lives on index_rules, which /drop-index deletes)
  // — the ring may contain a shard no longer in the live active set (e.g.
  // one later evacuated by Chunk 5's drain), so cleanup below targets the
  // UNION of the pinned ring and the current live set rather than only
  // whichever set happens to include every shard the index ever touched.
  const existingListRes = await routeToCatalog(env, "catalog-0", "/list-indexes", {}, request.headers.get("authorization") ?? undefined);
  const existingList = existingListRes.ok
    ? ((await existingListRes.json()) as { indexes: Array<{ indexName: string; placementRing?: string[] }> })
    : { indexes: [] };
  const pinnedRing = existingList.indexes.find((i) => i.indexName === body.indexName)?.placementRing ?? [];

  const unregisterResults = await fanOutToAllCatalogs(
    env,
    "/drop-index",
    () => ({ indexName: body.indexName }),
    request.headers.get("authorization") ?? undefined,
  );
  const unregisterFailed = firstCatalogFanOutFailure(unregisterResults, "Failed to unregister index.");
  if (unregisterFailed) return unregisterFailed;

  const listResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({}));
  const listFailed = firstCatalogFanOutFailure(listResults, "Failed to list shards.");
  if (listFailed) return listFailed;
  const liveShardIds = listResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);
  const shardIds = Array.from(new Set([...liveShardIds, ...pinnedRing]));

  const cleanupResults = await batchedMap(shardIds, SHARD_FANOUT_CONCURRENCY, async (shardId) => {
    const res = await routeToShard(env, shardId, "/execute", {
      sql: "DELETE FROM __cf_indexes WHERE index_name = ?",
      params: [body.indexName],
      requestId: `drop-index-cleanup-${body.indexName}-${shardId}-${crypto.randomUUID()}`,
      isMutation: true,
    });
    return { shardId, ok: res.ok };
  });
  const cleanupFailures = cleanupResults.filter((r) => !r.ok).map((r) => r.shardId);

  return json({
    ok: true,
    indexName: body.indexName,
    ...(cleanupFailures.length > 0
      ? { warning: `Physical cleanup failed on shard(s): ${cleanupFailures.join(", ")}. The index is unregistered and no longer queryable, but stale __cf_indexes rows may remain on those shards.` }
      : {}),
  });
}

const PROVENANCE_BACKFILL_PAGE_SIZE = 500;

/** Milestone 3, Chunk 1: writes __cf_row_owners for one (table, partitionKey)
 * pair directly — a plain /execute call against the shard, not a
 * StructuredMutation, since this targets the internal provenance table
 * itself rather than a base table row. Shared by /admin/backfill-provenance
 * (single-candidate case) and /admin/set-row-owner. */
async function writeProvenanceDirect(
  env: Env,
  shardId: string,
  table: string,
  partitionKey: string,
  tenantId: string,
  vbucket: number,
): Promise<boolean> {
  const res = await routeToShard(env, shardId, "/execute", {
    sql: `
      INSERT INTO __cf_row_owners (table_name, partition_key, tenant_id, vbucket, updated_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (table_name, partition_key) DO UPDATE SET tenant_id = excluded.tenant_id, vbucket = excluded.vbucket, updated_at = excluded.updated_at
    `,
    params: [table, partitionKey, tenantId, vbucket, new Date().toISOString()],
    requestId: `backfill-provenance-write-${crypto.randomUUID()}`,
    isMutation: true,
  });
  return res.ok;
}

type AmbiguousRow = { catalogShardId: string; shardId: string; table: string; partitionKey: string; candidateTenants: string[] };
type OrphanedRow = { catalogShardId: string; shardId: string; table: string; partitionKey: string };

/** Milestone 3, Chunk 1 (POST /admin/backfill-provenance {catalogShardId?}).
 * Re-attributes rows written before Chunk 0's __cf_row_owners existed.
 * Discovery is fully mechanical, per the design doc: table names + their
 * partition_key_column come from table_rules; candidate tenants come from
 * that catalog shard's tenant_auth; for each shard, for each registered
 * table, page through partition keys lacking a __cf_row_owners row and test
 * every candidate tenant's hash against this catalog shard's vbucket_map.
 * Exactly one match writes provenance; zero is reported orphaned; more than
 * one is reported ambiguous for manual resolution via /admin/set-row-owner.
 * Single-pass per shard/table (paginated 500 rows at a time, not chunked
 * across separate requests) — the same "acceptable at this milestone's
 * pre-product scale, a known simplification, not a silent bug" tradeoff
 * /admin/create-index's backfill already makes. */
async function handleAdminBackfillProvenance(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as { catalogShardId?: string };
  const authorization = request.headers.get("authorization") ?? undefined;
  const catalogShardIds = body.catalogShardId ? [body.catalogShardId] : allCatalogShardIds(env);

  let attributed = 0;
  const ambiguous: AmbiguousRow[] = [];
  const orphaned: OrphanedRow[] = [];
  // Tenant-scoped table scan: every table_name this run actually scanned
  // (across every catalog shard processed), used below to flip
  // table_rules.provenance_complete for the ones that came out clean. Only
  // meaningful when catalogShardId was omitted (a genuinely full-cluster
  // run) — a scoped single-catalog-shard run doesn't see every shard pool a
  // table's rows could live in, so it can't certify completeness.
  const scannedTableNames = new Set<string>();

  for (const catalogShardId of catalogShardIds) {
    const [tablesRes, tenantsRes, vbMapRes, shardsRes] = await Promise.all([
      routeToCatalog(env, catalogShardId, "/list-tables", {}, authorization),
      routeToCatalog(env, catalogShardId, "/list-tenants", {}, authorization),
      routeToCatalog(env, catalogShardId, "/vbucket-map", {}, authorization),
      // P1 (Codex): a shard is marked 'draining' BEFORE its vbuckets migrate,
      // and /admin/drain-shard stalls with VBUCKET_PROVENANCE_INCOMPLETE
      // pointing the operator here. An active-only enumeration would then skip
      // the very draining shard whose unattributed rows are blocking the drain
      // — a deadlock. Enumerate active + draining so re-attribution scans the
      // draining source and the drain can resume.
      routeToCatalog(env, catalogShardId, "/list-shards", { includeDraining: true }, authorization),
    ]);
    if (!tablesRes.ok) return new Response(tablesRes.body, { status: tablesRes.status, headers: tablesRes.headers });
    if (!tenantsRes.ok) return new Response(tenantsRes.body, { status: tenantsRes.status, headers: tenantsRes.headers });
    if (!vbMapRes.ok) return new Response(vbMapRes.body, { status: vbMapRes.status, headers: vbMapRes.headers });
    if (!shardsRes.ok) return new Response(shardsRes.body, { status: shardsRes.status, headers: shardsRes.headers });

    const tables = ((await tablesRes.json()) as { tables: Array<{ table_name: string; partition_key_column: string }> }).tables.filter(
      (t) => t.partition_key_column !== UNSET_PARTITION_KEY_COLUMN,
    );
    for (const t of tables) scannedTableNames.add(t.table_name);
    const tenantIds = ((await tenantsRes.json()) as { tenantIds: string[] }).tenantIds;
    const vbMapBody = (await vbMapRes.json()) as { totalVBuckets: number; map: Array<{ vbucket: number; shardId: string }> };
    const totalVBuckets = vbMapBody.totalVBuckets;
    const vbucketToShard = new Map(vbMapBody.map.map((m) => [m.vbucket, m.shardId]));
    const shardIds = ((await shardsRes.json()) as { shardIds: string[] }).shardIds;

    for (const shardId of shardIds) {
      for (const table of tables) {
        const pkCol = table.partition_key_column;
        const safeTable = `"${table.table_name}"`;
        const safePk = `"${pkCol}"`;
        let afterPk = "";
        for (;;) {
          const pageRes = await routeToShard(env, shardId, "/execute", {
            sql: `
              SELECT b.${safePk} AS pk FROM ${safeTable} b
              LEFT JOIN __cf_row_owners ro ON ro.table_name = ? AND ro.partition_key = b.${safePk}
              WHERE ro.partition_key IS NULL AND b.${safePk} > ?
              ORDER BY b.${safePk} ASC
              LIMIT ?
            `,
            params: [table.table_name, afterPk, PROVENANCE_BACKFILL_PAGE_SIZE],
            requestId: `backfill-provenance-scan-${catalogShardId}-${shardId}-${table.table_name}-${crypto.randomUUID()}`,
            isMutation: false,
          });
          if (!pageRes.ok) {
            // A table_rules entry doesn't guarantee a physical table exists
            // on every shard (e.g. /admin/register-table registers metadata
            // only, with no physical DDL — see index.test.ts's
            // column_mismatch_evt regression test) — skip rather than fail
            // the whole multi-shard/multi-table run over one such entry.
            log("worker.provenance_scan_skipped", { catalogShardId, shardId, table: table.table_name });
            break;
          }
          const pageBody = (await pageRes.json()) as { rows?: Array<{ pk: unknown }> };
          const pks = pageBody.rows ?? [];
          if (pks.length === 0) break;

          for (const { pk } of pks) {
            const partitionKey = String(pk);
            const candidates = tenantIds.filter((tenantId) => {
              const vbucket = hashKey(`${tenantId}:${table.table_name}:${partitionKey}`) % totalVBuckets;
              return vbucketToShard.get(vbucket) === shardId;
            });
            if (candidates.length === 1) {
              const vbucket = hashKey(`${candidates[0]}:${table.table_name}:${partitionKey}`) % totalVBuckets;
              const ok = await writeProvenanceDirect(env, shardId, table.table_name, partitionKey, candidates[0], vbucket);
              if (ok) attributed += 1;
              else orphaned.push({ catalogShardId, shardId, table: table.table_name, partitionKey });
            } else if (candidates.length === 0) {
              orphaned.push({ catalogShardId, shardId, table: table.table_name, partitionKey });
            } else {
              ambiguous.push({ catalogShardId, shardId, table: table.table_name, partitionKey, candidateTenants: candidates });
            }
          }

          afterPk = String(pks[pks.length - 1].pk);
          if (pks.length < PROVENANCE_BACKFILL_PAGE_SIZE) break;
        }
      }
    }
  }

  // Tenant-scoped table scan: only a genuinely full-cluster run (catalogShardId
  // omitted) can certify a table's provenance complete — a scoped run only
  // ever sees one catalog shard's own shard pool, which may leave other
  // catalog shards' pools (and therefore other rows of the same table) unseen.
  if (!body.catalogShardId) {
    const orphanedTables = new Set(orphaned.map((o) => o.table));
    const ambiguousTables = new Set(ambiguous.map((a) => a.table));
    const completeTables = Array.from(scannedTableNames).filter(
      (t) => !orphanedTables.has(t) && !ambiguousTables.has(t),
    );
    const markOutcomes = await Promise.all(
      completeTables.map(async (tableName) => {
        const markResults = await fanOutToAllCatalogs(
          env,
          "/mark-table-provenance-complete",
          () => ({ table: tableName }),
          authorization,
        );
        return firstCatalogFanOutFailure(markResults, `Failed to mark ${tableName} provenance complete.`);
      }),
    );
    const firstMarkFailed = markOutcomes.find((failed) => failed !== null);
    if (firstMarkFailed) return firstMarkFailed;
  }

  return json({ attributed, ambiguous, orphaned });
}

/** Milestone 3, Chunk 1 (POST /admin/set-row-owner). Manual resolution for a
 * row /admin/backfill-provenance reported ambiguous (or any row an operator
 * otherwise knows the true owner of). Rejects 409 if the claimed tenant's
 * hash doesn't actually land on the claimed shard — refuses to durably
 * record an owner assignment that /admin/migrate-vbucket's provenance-gate
 * (Chunk 4) would itself never have produced, rather than trusting the
 * caller unconditionally. */
async function handleAdminSetRowOwner(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as {
    catalogShardId?: string;
    shardId?: string;
    table?: string;
    partitionKey?: string;
    tenantId?: string;
  };
  if (!body.catalogShardId || !body.shardId || !body.table || !body.partitionKey || !body.tenantId) {
    return json(
      { error: { code: "MISSING_FIELDS", message: "Missing catalogShardId, shardId, table, partitionKey, or tenantId." } },
      400,
    );
  }
  const authorization = request.headers.get("authorization") ?? undefined;

  const vbMapRes = await routeToCatalog(env, body.catalogShardId, "/vbucket-map", {}, authorization);
  if (!vbMapRes.ok) return new Response(vbMapRes.body, { status: vbMapRes.status, headers: vbMapRes.headers });
  const vbMapBody = (await vbMapRes.json()) as { totalVBuckets: number; map: Array<{ vbucket: number; shardId: string }> };
  const vbucketToShard = new Map(vbMapBody.map.map((m) => [m.vbucket, m.shardId]));

  const vbucket = hashKey(`${body.tenantId}:${body.table}:${body.partitionKey}`) % vbMapBody.totalVBuckets;
  const mappedShardId = vbucketToShard.get(vbucket);
  if (mappedShardId !== body.shardId) {
    return json(
      {
        error: {
          code: "ROW_OWNER_SHARD_MISMATCH",
          message: `Tenant ${body.tenantId}'s hash for this (table, partitionKey) maps to vbucket ${vbucket}, which is on shard ${mappedShardId ?? "(unmapped)"}, not the claimed shard ${body.shardId}.`,
          fix: "Verify the claimed tenantId, or check /admin/status for the current vbucket_map.",
        },
      },
      409,
    );
  }

  const ok = await writeProvenanceDirect(env, body.shardId, body.table, body.partitionKey, body.tenantId, vbucket);
  if (!ok) {
    return json({ error: { code: "ROW_OWNER_WRITE_FAILED", message: `Failed to write provenance on shard ${body.shardId}.` } }, 500);
  }
  return json({ ok: true });
}

async function handleAdminDrainShard(request: Request, env: Env): Promise<Response> {
  const payload = (await request.json()) as { shardId: string; catalogShardId?: string };
  if (!payload.catalogShardId) {
    return json({ error: "Missing catalogShardId. Shard ownership is scoped to a catalog shard." }, 400);
  }
  if (!payload.shardId) {
    return json({ error: "Missing shardId." }, 400);
  }

  // Check for in-flight prepared 2PC intents before draining — this is a
  // Worker-level check (not pushed into CatalogDO) to preserve the existing
  // invariant that CatalogDO and ShardDO never call each other directly.
  // Chunk 3's recovery loop bounds how long a prepared intent can linger, and
  // /admin/tx-force-abort is the manual escape hatch if it doesn't resolve.
  const pendingRes = await routeToShard(env, payload.shardId, "/pending-intent-count", {});
  if (!pendingRes.ok) {
    return new Response(pendingRes.body, { status: pendingRes.status, headers: pendingRes.headers });
  }
  const pendingBody = (await pendingRes.json()) as { count: number; indexPendingJobCount?: number };
  if (pendingBody.count > 0) {
    return json(
      {
        error: {
          code: "SHARD_HAS_IN_FLIGHT_TRANSACTIONS",
          message: `Shard ${payload.shardId} has ${pendingBody.count} in-flight transaction(s).`,
          fix: "Retry after they resolve, or use /admin/tx-force-abort to unblock a stuck one.",
        },
      },
      409,
    );
  }

  // Milestone 2, Chunk 5: also block on unresolved async index-write retries
  // queued on this shard (Chunk 2's index_pending_jobs) — draining doesn't
  // stop the underlying DO or its alarm() from continuing to retry them, but
  // an operator draining a shard ahead of decommissioning it should see and
  // resolve outstanding index-repair work first, not have it silently
  // continue running on a shard the catalog no longer routes traffic to.
  const indexPendingJobCount = pendingBody.indexPendingJobCount ?? 0;
  if (indexPendingJobCount > 0) {
    return json(
      {
        error: {
          code: "SHARD_HAS_PENDING_INDEX_JOBS",
          message: `Shard ${payload.shardId} has ${indexPendingJobCount} unresolved index-write retry job(s).`,
          fix: "Retry after they resolve (check /admin/shard-stats for indexPendingJobCount), or investigate why the target index shard is unreachable.",
        },
      },
      409,
    );
  }

  // Stage 3: a drain is a LONG operation (many alarm ticks) — the lock is
  // acquired for a FRESH start and HANDED OFF to advanceDrain's whole
  // multi-tick duration (CatalogDO stores it on the shards row; heartbeated
  // each tick, released only on full completion). A RE-INVOKE of an
  // ALREADY-draining shard (the "I've fixed the stall, resume" signal) must
  // NOT acquire a NEW lock — that drain already holds its own lock, and a
  // fresh acquire attempt would 409 against it. Check current status first to
  // tell the two apart.
  const authorization = request.headers.get("authorization") ?? undefined;
  const statusRes = await routeToCatalog(env, payload.catalogShardId, "/drain-shard-status", { shardId: payload.shardId }, authorization);
  let alreadyDraining = false;
  if (statusRes.ok) {
    const statusBody = (await statusRes.clone().json()) as { status?: string };
    alreadyDraining = statusBody.status !== "active";
  }

  let lockOperationId: string | undefined;
  if (!alreadyDraining) {
    const lock = await acquireTopologyLock(env, "drain-shard");
    if (lock instanceof Response) return lock;
    lockOperationId = lock.operationId;
  }
  let handedOff = false;
  try {
    const res = await routeToCatalog(
      env,
      payload.catalogShardId,
      "/drain-shard",
      lockOperationId ? { ...payload, operationId: lockOperationId } : payload,
      authorization,
    );
    handedOff = res.ok;
    return new Response(res.body, { status: res.status, headers: res.headers });
  } finally {
    if (lockOperationId && !handedOff) await releaseTopologyLock(env, lockOperationId);
  }
}

async function handleAdminAuditLog(request: Request, env: Env): Promise<Response> {
  const authorization = request.headers.get("authorization") ?? undefined;
  const results = await fanOutToAllCatalogs(env, "/audit-log", () => ({}), authorization);
  const failed = firstCatalogFanOutFailure(results, "One or more catalog shards failed to report the audit log.");
  if (failed) return failed;
  type AuditEntry = { endpoint: string; request: unknown; createdAt: string };
  const entries = results
    .flatMap((r) =>
      ((r.body as { entries: AuditEntry[] }).entries ?? []).map((e) => ({
        catalogShardId: r.catalogShardId,
        ...e,
      })),
    )
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : a.createdAt > b.createdAt ? -1 : 0));
  return json({ entries });
}

async function handleAdminRegisterTenant(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as { tenantId?: string; rotate?: boolean };
  if (!body.tenantId) {
    return json(
      { error: { code: "MISSING_TENANT_ID", message: "Missing tenantId.", fix: "Provide a tenantId in the request body." } },
      400,
    );
  }
  const catalogShardId = catalogShardIdForTenant(env, body.tenantId);
  const res = await routeToCatalog(env, catalogShardId, "/register-tenant", body, request.headers.get("authorization") ?? undefined);
  return new Response(res.body, { status: res.status, headers: res.headers });
}

async function handleAdminRevokeTenant(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as { tenantId?: string };
  if (!body.tenantId) {
    return json(
      { error: { code: "MISSING_TENANT_ID", message: "Missing tenantId.", fix: "Provide a tenantId in the request body." } },
      400,
    );
  }
  const catalogShardId = catalogShardIdForTenant(env, body.tenantId);
  const res = await routeToCatalog(env, catalogShardId, "/revoke-tenant", body, request.headers.get("authorization") ?? undefined);
  return new Response(res.body, { status: res.status, headers: res.headers });
}

async function handleAdminShardStats(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as { shardId: string };
  if (!body.shardId) {
    return json({ error: "Missing shardId" }, 400);
  }
  const res = await routeToShard(env, body.shardId, "/stats", {});
  return new Response(res.body, { status: res.status, headers: res.headers });
}

async function handleV1Sql(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  // ARCHITECTURE CHANGE: /v1/sql is now ADMIN-ONLY (operator/debugging), like
  // /v1/scatter. The per-tenant SQL guard was structurally unwinnable — the
  // denylist/allowlist leaked six times — and a raw partition-scoped SELECT
  // leaked another tenant's data (base rows carry no physical tenant_id, so the
  // shard can't filter by tenant; see docs/SPEC.md §14). Rather than keep
  // patching an unwinnable guard, the whole trust-based tenant path is removed:
  // tenants write via /v1/mutate + /v1/tx and read via /v1/index-query.
  // body.tenantId is still required (routing/hashing) but is NOT authenticated
  // against the caller — the caller is the operator (ADMIN_TOKEN).
  const adminAuthError = requireAdminAuth(env, request);
  if (adminAuthError) return adminAuthError;

  const body = (await request.json()) as SqlRequest;
  if (!body.sql || !body.table || !body.tenantId) {
    return json({ error: "Missing required fields: sql, table, tenantId." }, 400);
  }

  const mutating = isMutation(body.sql);

  if (isDangerous(body.sql)) {
    return json({ error: "SQL statement not permitted." }, 403);
  }

  // Light guardrail: block a MUTATION whose write target is an internal
  // bookkeeping table, so a fat-fingered operator query can't corrupt
  // fence/provenance/mirror state from the data plane. Internal-table READS are
  // ALLOWED (an operator may need to inspect them for debugging) and cross-table
  // access is allowed — admin is trusted. The internal DO routes that
  // legitimately write these tables call /execute directly, never through here.
  if (mutating && mutationTargetIsInternal(body.sql)) {
    return json(
      {
        error: {
          code: "INTERNAL_TABLE_WRITE_FORBIDDEN",
          message: "Writes to internal system tables are not permitted, even for the operator.",
          fix: "Manipulate migration/fence/provenance state only through the /admin/* orchestration routes.",
        },
      },
      403,
    );
  }

  if (body.params !== undefined && !assertParamsArray(body.params)) {
    return json({ error: "params must be an array." }, 400);
  }

  if (!body.partitionKey) {
    if (mutating) {
      return json(
        { error: "Mutating SQL requires partitionKey for deterministic single-shard routing." },
        400,
      );
    }
    return json(
      { error: "SELECT without partitionKey is not allowed on /v1/sql. Use /v1/scatter for fan-out reads." },
      400,
    );
  }

  const catalogShardId = catalogShardIdForTenant(env, body.tenantId);
  const routeStart = Date.now();
  const routeRes = await routeToCatalog(
    env,
    catalogShardId,
    "/route",
    {
      table: body.table,
      tenantId: body.tenantId,
      partitionKey: body.partitionKey,
    },
    request.headers.get("authorization") ?? undefined,
  );
  const routeLookupMs = Date.now() - routeStart;

  if (!routeRes.ok) {
    return new Response(routeRes.body, { status: routeRes.status, headers: routeRes.headers });
  }

  const route = (await routeRes.json()) as {
    shardId: string;
    vbucket: number;
    metadataVersion: number;
    catalogShardCount: number | null;
    indexNames?: string[];
    migrationStatus?: string;
    targetShardId?: string;
  };

  const currentCount = catalogShardCount(env);
  if (route.catalogShardCount !== null && route.catalogShardCount !== currentCount) {
    log("worker.catalog_shard_count_mismatch", {
      initializedCount: route.catalogShardCount,
      configuredCount: currentCount,
      tenantId: body.tenantId,
    });
    return json(
      {
        error: `Catalog shard count mismatch: cluster was initialized with ${route.catalogShardCount} catalog shards, but this Worker is configured for ${currentCount} (CATALOG_SHARD_COUNT). Changing this on a live cluster silently re-routes tenants to different catalog shards and orphans their data. Re-run /admin/init consistently or fix the CATALOG_SHARD_COUNT var.`,
      },
      409,
    );
  }

  // Milestone 2: raw SQL bypasses every index-maintenance mechanism
  // (neither /v1/mutate's async path nor /v1/tx's 2PC piggyback ever fires
  // for a raw /execute call), so a mutation against a table carrying a
  // registered index would silently desync it. Reject here, at the Worker
  // layer — ShardDO has no CatalogDO access to check this itself.
  if (mutating && route.indexNames && route.indexNames.length > 0) {
    return json(
      {
        error: {
          code: "TABLE_HAS_INDEX",
          message: `Table ${body.table} has registered index(es) (${route.indexNames.join(", ")}) — raw /v1/sql mutations are not permitted against it.`,
          fix: "Use /v1/mutate or /v1/tx instead, which maintain indexes correctly.",
        },
      },
      409,
    );
  }

  const reservedError = rejectReservedRequestId(body.requestId);
  if (reservedError) return reservedError;
  const requestId = body.requestId ?? crypto.randomUUID();
  // Review Tier 1 #2: mid-migration, the SOURCE shard enqueues the mirror
  // atomically with the write (passing mirrorTargetShardId here), so it's
  // always counted by /mirror-pending-count — no best-effort ctx.waitUntil.
  const mirrorTargetShardId =
    mutating && route.targetShardId && (route.migrationStatus === "backfilling" || route.migrationStatus === "cutover")
      ? route.targetShardId
      : undefined;
  const shardStart = Date.now();
  const shardRes = await routeToShard(env, route.shardId, "/execute", {
    sql: body.sql,
    params: body.params ?? [],
    requestId,
    isMutation: mutating,
    tenantId: body.tenantId,
    table: body.table,
    partitionKey: body.partitionKey,
    vbucket: route.vbucket,
    mirrorTargetShardId,
  });
  const shardExecuteMs = Date.now() - shardStart;

  if (!shardRes.ok) {
    return new Response(shardRes.body, { status: shardRes.status, headers: shardRes.headers });
  }

  const shardPayload = await shardRes.json();
  return json({
    route: { ...route, catalogShardId },
    requestId,
    observability: { routeLookupMs, shardExecuteMs, metadataVersion: route.metadataVersion },
    result: shardPayload,
  });
}

/** `ring` is the index's pinned placement_ring_json (captured once at
 * /admin/create-index) — see hash.ts's indexShardIdForKey doc comment. Every
 * caller that resolves an index-shard placement must use THIS array, never
 * a freshly fetched live shard list. */
type IndexDefinition = { indexName: string; columns: string[]; ring: string[] };

/** Eng-review fix (Codex-found): an insert/upsert that omits an indexed
 * column and lets SQLite fill a column DEFAULT would otherwise get indexed
 * as `null` — computeIndexDeltas has no way to know the actual stored value
 * without re-reading the row, and re-reading after the write would break
 * /v1/tx's atomicity (the index delta must be a 2PC participant decided
 * before /begin, not a follow-up write after commit). Requiring every
 * indexed column explicitly up front avoids ever needing to guess or
 * reconstruct a DEFAULT's value, and keeps /v1/mutate and /v1/tx behaving
 * identically instead of one silently "fixing" it and the other not. */
function requireIndexedColumnsForInsert(
  op: "insert" | "update" | "upsert" | "delete",
  indexedColumns: string[],
  values: Record<string, unknown> | undefined,
): { code: string; message: string; fix: string } | null {
  if ((op !== "insert" && op !== "upsert") || indexedColumns.length === 0) return null;
  const missing = indexedColumns.filter((c) => !(values && c in values));
  if (missing.length === 0) return null;
  return {
    code: "INDEXED_COLUMN_REQUIRES_VALUE",
    message: `insert/upsert on an indexed table must explicitly supply every indexed column: missing ${missing.join(", ")}.`,
    fix: "Provide a value for every indexed column — relying on a SQL DEFAULT for an indexed column isn't supported, since the index entry is computed from the values you supply, not read back from the stored row.",
  };
}

/** Review Tier 1 #5: the reserved requestId namespace ShardDO uses for its
 * internal mirror deliveries. A client that could supply a requestId in this
 * namespace could pre-poison the future target shard's applied_requests with
 * a colliding entry and permanently block a mirror's dedupe — stalling
 * cutover forever — so the gateway rejects any client requestId starting with
 * it. */
const RESERVED_REQUEST_ID_PREFIX = "__cf:";

function rejectReservedRequestId(requestId: string | undefined): Response | null {
  if (requestId !== undefined && requestId.startsWith(RESERVED_REQUEST_ID_PREFIX)) {
    return json(
      {
        error: {
          code: "RESERVED_REQUEST_ID",
          message: `requestId must not start with the reserved prefix "${RESERVED_REQUEST_ID_PREFIX}".`,
          fix: "Use a different requestId.",
        },
      },
      400,
    );
  }
  return null;
}

/** Codex round-13 fix: fetch an index's CURRENT pinned placement ring from the
 * catalog (catalog-0 answers — index_rules is replicated to every catalog
 * shard). Used to re-resolve an index write's target after an INDEX_RING_FENCED
 * rejection. Returns [] if unknown/unreachable so the caller falls back. */
async function fetchIndexRing(env: Env, indexName: string): Promise<string[]> {
  try {
    const res = await routeToCatalog(env, "catalog-0", "/index-ring", { indexName });
    if (!res.ok) return [];
    return ((await res.json()) as { ring?: string[] }).ring ?? [];
  } catch {
    return [];
  }
}

/** Is this a 409 whose error code is INDEX_RING_FENCED? */
async function isIndexRingFenced(res: Response): Promise<boolean> {
  if (res.status !== 409) return false;
  try {
    return ((await res.clone().json()) as { error?: { code?: string } }).error?.code === "INDEX_RING_FENCED";
  } catch {
    return false;
  }
}

/** Codex round-14 P2: a blocking backfill index-entry write that participates in
 * the index-ring fence. Places the entry over `ring` (the index's PERSISTED
 * placement ring), labels it with indexName, and on an INDEX_RING_FENCED
 * rejection RE-RESOLVES the index's current ring and writes to the substitute —
 * so a drain that fenced a shard after /admin/create-index captured the ring
 * doesn't hard-fail the backfill or strand the entry on the drained shard.
 * Returns true on success. */
async function backfillWriteIndexEntry(
  env: Env,
  table: string,
  indexName: string,
  indexKeyJson: string,
  partitionKey: string,
  sourceShardId: string,
  tenantId: string,
  ring: string[],
): Promise<boolean> {
  const sql =
    "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)";
  const params = [table, indexName, indexKeyJson, partitionKey, sourceShardId, tenantId, new Date().toISOString()];
  const requestId = `create-index-backfill-write-${indexName}-${crypto.randomUUID()}`;
  const target = indexShardIdForKey(table, indexName, indexKeyJson, ring);
  const res = await routeToShard(env, target, "/execute", { sql, params, requestId, isMutation: true, indexName });
  if (res.ok) return true;
  if (await isIndexRingFenced(res)) {
    const fresh = await fetchIndexRing(env, indexName);
    if (fresh.length > 0) {
      const resolved = indexShardIdForKey(table, indexName, indexKeyJson, fresh);
      if (resolved !== target) {
        const retry = await routeToShard(env, resolved, "/execute", { sql, params, requestId, isMutation: true, indexName });
        if (retry.ok) return true;
      }
    }
  }
  return false;
}

/** Writes one __cf_indexes entry (insert/replace or delete), best-effort. On
 * failure, records a retry job on the BASE shard (not the index shard, which
 * may be the one that's unreachable) via /enqueue-index-job — ShardDO's
 * alarm() picks it up from there. Never throws: this always runs inside
 * ctx.waitUntil(), after the caller's response has already been sent, so
 * there's no one left to propagate an exception to.
 *
 * Codex round-13 fix: labels the write with `indexName` so the target shard can
 * enforce the index-ring write fence, and on an INDEX_RING_FENCED rejection
 * RE-RESOLVES the index's current ring and writes to the newly-resolved shard
 * (the substitute) — converting an in-flight write that resolved the OLD ring
 * before a drain repoint into a correct write, instead of stranding it on a
 * shard about to be decommissioned. The enqueued retry carries the same
 * structured fields so a later alarm retry can re-resolve too. */
async function writeIndexEntryBestEffort(
  env: Env,
  baseShardId: string,
  targetShardId: string,
  sql: string,
  params: unknown[],
  requestId: string,
  indexName: string,
  table: string,
  indexKeyJson: string,
): Promise<void> {
  try {
    const res = await routeToShard(env, targetShardId, "/execute", { sql, params, requestId, isMutation: true, indexName });
    if (res.ok) return;
    if (await isIndexRingFenced(res)) {
      // The target is being evacuated for this index — re-resolve and write to
      // the substitute. If re-resolution succeeds we're done; otherwise fall
      // through to the durable retry queue (which also re-resolves).
      const ring = await fetchIndexRing(env, indexName);
      if (ring.length > 0) {
        const resolved = indexShardIdForKey(table, indexName, indexKeyJson, ring);
        if (resolved !== targetShardId) {
          const retryRes = await routeToShard(env, resolved, "/execute", { sql, params, requestId, isMutation: true, indexName });
          if (retryRes.ok) return;
        }
      }
    }
    throw new Error(`shard responded ${res.status}`);
  } catch (error) {
    log("worker.index_write_failed_enqueuing_retry", {
      baseShardId,
      targetShardId,
      requestId,
      message: error instanceof Error ? error.message : String(error),
    });
    try {
      await routeToShard(env, baseShardId, "/enqueue-index-job", {
        targetShardId,
        sql,
        params,
        requestId,
        indexName,
        indexTable: table,
        indexKeyJson,
      });
    } catch (enqueueError) {
      // Base shard itself unreachable — nothing left to do; the write is
      // lost until a future write to the same row happens to correct it.
      // Known limitation, not silently swallowed: logged for visibility.
      log("worker.index_job_enqueue_failed", {
        baseShardId,
        targetShardId,
        requestId,
        message: enqueueError instanceof Error ? enqueueError.message : String(enqueueError),
      });
    }
  }
}

/** Eng-review fix (Codex-found): does `row` satisfy every predicate in
 * `where`? Used by /v1/tx's same-row multi-mutation tracking to decide
 * in-JS whether a mutation's `where` matches a simulated (not freshly
 * queried) row state, the same way SQLite would evaluate it against the
 * real row once /begin actually runs the batch. `null` never matches
 * (mirrors "UPDATE ... WHERE" against a nonexistent row affecting 0 rows).
 * The partition-key predicate itself needs no check here — every caller of
 * this function already scopes `row` to one specific partitionKey by
 * construction, matching compileMutation's own unconditional `pkCol = ?`. */
function simulatedRowMatchesWhere(row: Record<string, unknown> | null, where: Record<string, unknown> | undefined): boolean {
  if (row === null) return false;
  for (const [key, value] of Object.entries(where ?? {})) {
    if (row[key] !== value) return false;
  }
  return true;
}

type IndexDelta = { indexName: string; oldKeyJson: string | null; newKeyJson: string | null };

/** Pure computation, shared by Chunk 2's async /v1/mutate path and Chunk 3's
 * /v1/tx 2PC piggyback: for each registered index on the table, works out
 * whether its __cf_indexes entry needs to change. `beforeRow` is the row's
 * state before the mutation (null for insert, where none exists yet) —
 * needed to remove a now-stale index entry on update/delete. `afterValues`
 * is the mutation's own values (insert/update/upsert; undefined for delete)
 * — merged over beforeRow to get each indexed column's new value, so this
 * never needs a second read-after-write: whatever column isn't in the
 * caller's values is unchanged from beforeRow. Returns only the indexes
 * whose entry actually changes (oldKeyJson !== newKeyJson is always true in
 * the returned deltas). */
function computeIndexDeltas(
  indexes: IndexDefinition[],
  op: "insert" | "update" | "upsert" | "delete",
  beforeRow: Record<string, unknown> | null,
  afterValues: Record<string, unknown> | undefined,
): IndexDelta[] {
  // Eng-review fix: a null beforeRow is ambiguous between "insert (no prior
  // row can exist yet)" and "update/delete whose predicate matched nothing
  // (0 rows affected)". For insert, a null beforeRow legitimately means
  // "write a new entry from afterValues". For update specifically, it means
  // the mutation was a no-op — synthesizing a "new" entry from afterValues
  // in that case would write a phantom __cf_indexes row for a base-row
  // change that never actually happened. delete already can't hit this
  // (newKeyJson is unconditionally null for delete), so only update needs
  // the explicit no-op short-circuit.
  if (op === "update" && beforeRow === null) {
    return [];
  }
  const deltas: IndexDelta[] = [];
  for (const index of indexes) {
    const oldKeyJson = beforeRow ? JSON.stringify(index.columns.map((c) => beforeRow[c] ?? null)) : null;
    const newKeyJson =
      op === "delete"
        ? null
        : JSON.stringify(index.columns.map((c) => (afterValues && c in afterValues ? afterValues[c] : beforeRow?.[c] ?? null)));
    if (oldKeyJson !== newKeyJson) {
      deltas.push({ indexName: index.indexName, oldKeyJson, newKeyJson });
    }
  }
  return deltas;
}

/** Computes and dispatches (via ctx.waitUntil, non-blocking) the __cf_indexes
 * writes needed after a base-row mutation succeeds — see computeIndexDeltas
 * for what "needed" means. */
async function maintainIndexesAsync(
  env: Env,
  ctx: ExecutionContext,
  table: string,
  baseShardId: string,
  tenantId: string,
  partitionKey: string,
  indexes: IndexDefinition[],
  op: "insert" | "update" | "upsert" | "delete",
  beforeRow: Record<string, unknown> | null,
  afterValues: Record<string, unknown> | undefined,
): Promise<void> {
  if (indexes.length === 0) return;

  // Milestone 3, Chunk 2: each index's placement ring is its own PINNED
  // placement_ring_json (route.indexes already carries it), not a freshly
  // fetched live shard list — removes the /list-shards fan-out this used to
  // do on every mutation against an indexed table.
  const ringByIndex = new Map(indexes.map((i) => [i.indexName, i.ring]));

  const now = new Date().toISOString();
  const deltas = computeIndexDeltas(indexes, op, beforeRow, afterValues);
  for (const delta of deltas) {
    const ring = ringByIndex.get(delta.indexName) ?? [];
    if (ring.length === 0) continue; // no pinned ring recorded — nothing safe to write to
    if (delta.oldKeyJson !== null) {
      const oldShardId = indexShardIdForKey(table, delta.indexName, delta.oldKeyJson, ring);
      const requestId = `index-delete-${delta.indexName}-${crypto.randomUUID()}`;
      ctx.waitUntil(
        writeIndexEntryBestEffort(
          env,
          baseShardId,
          oldShardId,
          "DELETE FROM __cf_indexes WHERE table_name = ? AND index_name = ? AND index_key_json = ? AND partition_key = ?",
          [table, delta.indexName, delta.oldKeyJson, partitionKey],
          requestId,
          delta.indexName,
          table,
          delta.oldKeyJson,
        ),
      );
    }
    if (delta.newKeyJson !== null) {
      const newShardId = indexShardIdForKey(table, delta.indexName, delta.newKeyJson, ring);
      const requestId = `index-write-${delta.indexName}-${crypto.randomUUID()}`;
      ctx.waitUntil(
        writeIndexEntryBestEffort(
          env,
          baseShardId,
          newShardId,
          "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
          [table, delta.indexName, delta.newKeyJson, partitionKey, baseShardId, tenantId, now],
          requestId,
          delta.indexName,
          table,
          delta.newKeyJson,
        ),
      );
    }
  }
}

/** Single structured mutation, single-shard, non-transactional — routes like
 * /v1/sql but through compileMutation() for structural row-ownership
 * enforcement. An incremental, independently-testable deliverable that
 * de-risks the DSL before Chunk 3 builds the coordinator on top of it.
 * Milestone 2, Chunk 2: if the table carries any registered indexes, index
 * maintenance is dispatched via ctx.waitUntil() after the base write
 * succeeds and the response is ready — non-blocking for the caller, with
 * ShardDO's alarm()-driven retry queue as the repair path for a failed
 * attempt (see maintainIndexesAsync). */
async function handleV1Mutate(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const body = (await request.json()) as StructuredMutation & { requestId?: string };
  if (!body.table || !body.tenantId || !body.partitionKey) {
    return json({ error: "Missing required fields: table, tenantId, partitionKey." }, 400);
  }

  const catalogShardId = catalogShardIdForTenant(env, body.tenantId);
  const routeRes = await routeToCatalog(
    env,
    catalogShardId,
    "/route",
    { table: body.table, tenantId: body.tenantId, partitionKey: body.partitionKey },
    request.headers.get("authorization") ?? undefined,
  );
  if (!routeRes.ok) {
    return new Response(routeRes.body, { status: routeRes.status, headers: routeRes.headers });
  }

  const route = (await routeRes.json()) as {
    shardId: string;
    vbucket: number;
    catalogShardCount: number | null;
    partitionKeyColumn: string | null;
    indexes?: IndexDefinition[];
    migrationStatus?: string;
    targetShardId?: string;
  };

  const currentCount = catalogShardCount(env);
  if (route.catalogShardCount !== null && route.catalogShardCount !== currentCount) {
    return json(
      {
        error: `Catalog shard count mismatch: cluster was initialized with ${route.catalogShardCount} catalog shards, but this Worker is configured for ${currentCount} (CATALOG_SHARD_COUNT).`,
      },
      409,
    );
  }

  const partitionKeyColumn = route.partitionKeyColumn ?? UNSET_PARTITION_KEY_COLUMN;
  const validation = validateMutation(body, partitionKeyColumn);
  if (!validation.ok) {
    return json({ error: { code: validation.code, message: validation.error, fix: "Check the request against the error message above." } }, validation.status);
  }

  const indexes = route.indexes ?? [];
  const indexedColumns = Array.from(new Set(indexes.flatMap((i) => i.columns)));

  const missingIndexedValues = requireIndexedColumnsForInsert(body.op, indexedColumns, body.values);
  if (missingIndexedValues) {
    return json({ error: missingIndexedValues }, 400);
  }

  // For update/delete/upsert on an indexed table, read the row's current
  // state BEFORE the mutation — needed to remove its now-stale index
  // entries. upsert is included because it may hit the ON CONFLICT UPDATE
  // path just like a plain update (we can't tell from compileMutation's
  // output which branch will fire); if it turns out to insert instead,
  // beforeRow simply comes back null, which is the correct insert case
  // anyway. Not needed for a plain insert (no prior row can exist) or a
  // table with no indexes.
  let beforeRow: Record<string, unknown> | null = null;
  if (indexedColumns.length > 0 && (body.op === "update" || body.op === "delete" || body.op === "upsert")) {
    const safeTable = `"${body.table}"`;
    const safePkCol = `"${partitionKeyColumn}"`;
    const selectCols = indexedColumns.map((c) => `"${c}"`).join(", ");
    const preReadRes = await routeToShard(env, route.shardId, "/execute", {
      sql: `SELECT ${selectCols} FROM ${safeTable} WHERE ${safePkCol} = ?`,
      params: [body.partitionKey],
      requestId: `index-preread-${crypto.randomUUID()}`,
      isMutation: false,
    });
    if (preReadRes.ok) {
      const preReadBody = (await preReadRes.json()) as { rows?: Array<Record<string, unknown>> };
      beforeRow = preReadBody.rows?.[0] ?? null;
    }
  }

  const reservedError = rejectReservedRequestId(body.requestId);
  if (reservedError) return reservedError;
  const { sql, params } = compileMutation(body, partitionKeyColumn);
  const requestId = body.requestId ?? crypto.randomUUID();
  // Review Tier 1 #2: mid-migration, the SOURCE shard enqueues the mirror
  // atomically with the write.
  const mirrorTargetShardId =
    route.targetShardId && (route.migrationStatus === "backfilling" || route.migrationStatus === "cutover")
      ? route.targetShardId
      : undefined;
  const shardRes = await routeToShard(env, route.shardId, "/execute", {
    sql,
    params,
    requestId,
    isMutation: true,
    tenantId: body.tenantId,
    table: body.table,
    partitionKey: body.partitionKey,
    vbucket: route.vbucket,
    mirrorTargetShardId,
  });
  if (!shardRes.ok) {
    return new Response(shardRes.body, { status: shardRes.status, headers: shardRes.headers });
  }

  const shardPayload = (await shardRes.json()) as { rowsAffected?: number };
  const rowsAffected = shardPayload.rowsAffected ?? 0;

  // Eng-review fix: only maintain the index if the mutation actually
  // changed a row. A StructuredMutation's optional `where` can narrow
  // update/delete beyond the partitionKey (compileMutation ANDs it in), but
  // beforeRow above is pre-read by partitionKey alone — so a where clause
  // that doesn't match (0 rows affected) must NOT still delete/rewrite the
  // row's index entry based on a beforeRow that describes a row nothing
  // actually touched. Without this gate, a live, unchanged row would
  // silently vanish from index-query results.
  if (indexes.length > 0 && rowsAffected > 0) {
    ctx.waitUntil(
      maintainIndexesAsync(
        env,
        ctx,
        body.table,
        route.shardId,
        body.tenantId,
        body.partitionKey,
        indexes,
        body.op,
        beforeRow,
        body.op === "delete" ? undefined : body.values,
      ),
    );
  }

  return json({ ok: true, rowsAffected });
}

/** Cross-shard atomic write via CoordinatorDO's 2PC. Every mutation is
 * individually routed (so cross-tenant mismatches 401 the same way a single
 * /route call would — no separate check needed) and validated/compiled with
 * its own table's partitionKeyColumn, then grouped by shardId into the
 * participant list /begin expects.
 *
 * Milestone 2, Chunk 3: when a mutation's table carries any registered
 * index, its index-maintenance writes are piggybacked into the SAME 2PC
 * transaction as extra participants — computed with computeIndexDeltas, the
 * same pure logic Chunk 2's async /v1/mutate path uses, so both paths agree
 * on what "the index changed" means. Deliberately does NOT count these
 * synthetic index-participant keys against MAX_TX_PARTICIPANT_KEYS: that cap
 * bounds the blast radius of what the CALLER asked to touch, and index
 * maintenance is bookkeeping this system adds on the caller's behalf, not
 * additional caller-requested scope. */
async function handleV1Tx(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as StructuredOperation;
  if (!body.mutations || body.mutations.length === 0) {
    return json({ error: { code: "MISSING_MUTATIONS", message: "mutations must be a non-empty array." } }, 400);
  }
  if (!body.requestId) {
    return json({ error: { code: "MISSING_REQUEST_ID", message: "Missing requestId.", fix: "Provide a client-generated requestId for idempotent retries." } }, 400);
  }
  const reservedError = rejectReservedRequestId(body.requestId);
  if (reservedError) return reservedError;

  const distinctKeys = new Set(body.mutations.map((m) => participantKey(m)));
  if (distinctKeys.size > MAX_TX_PARTICIPANT_KEYS) {
    return json(
      {
        error: {
          code: "TOO_MANY_PARTICIPANTS",
          message: `Transaction touches ${distinctKeys.size} distinct rows, exceeding the cap of ${MAX_TX_PARTICIPANT_KEYS}.`,
          fix: "Split this into multiple smaller transactions.",
        },
      },
      400,
    );
  }

  type TxIntent = {
    sql: string;
    params: unknown[];
    tenantId: string;
    table: string;
    partitionKey: string;
    vbucket?: number;
    op?: "insert" | "update" | "delete" | "upsert";
    /** Milestone 3, Chunk 3: set when this intent's vbucket is mid-migration
     * — CoordinatorDO mirrors the committed intent to this target shard
     * post-commit (enqueuing on the source's __cf_mirror_pending on
     * failure). Never set on a synthetic __cf_indexes intent: index entries
     * are placed by pinned ring, not vbucket_map, so they never migrate via
     * this machinery. */
    mirrorTargetShardId?: string;
  };
  const compiledByShardId = new Map<string, TxIntent[]>();
  const currentCount = catalogShardCount(env);
  const authorization = request.headers.get("authorization") ?? undefined;

  function addIntent(shardId: string, intent: TxIntent): void {
    const existing = compiledByShardId.get(shardId);
    if (existing) {
      existing.push(intent);
    } else {
      compiledByShardId.set(shardId, [intent]);
    }
  }

  // Eng-review fix (Codex-found): each mutation's beforeRow pre-read hits
  // the real database, which only reflects what's already committed — never
  // what an EARLIER mutation in this SAME batch is about to do, since
  // /begin hasn't run yet. Two mutations touching the same row in one
  // /v1/tx call (e.g. insert v='a' then update v='b') would otherwise
  // compute the second mutation's delta against a stale/nonexistent prior
  // state, silently losing the index entry for the row's actual final
  // value. Tracks a simulated row per distinct (tenantId, table,
  // partitionKey) — same keying as rowKey()/participantKey() — seeded from
  // the first real pre-read and updated as each mutation is processed,
  // mirroring how SQLite itself sees each statement's effects within the
  // same transaction (later statements observe earlier ones' writes).
  const simulatedRowState = new Map<string, Record<string, unknown> | null>();

  for (const mutation of body.mutations) {
    if (!mutation.table || !mutation.tenantId || !mutation.partitionKey) {
      return json({ error: { code: "MISSING_FIELDS", message: "Each mutation requires table, tenantId, partitionKey." } }, 400);
    }

    const catalogShardId = catalogShardIdForTenant(env, mutation.tenantId);
    const routeRes = await routeToCatalog(
      env,
      catalogShardId,
      "/route",
      { table: mutation.table, tenantId: mutation.tenantId, partitionKey: mutation.partitionKey },
      authorization,
    );
    if (!routeRes.ok) {
      return new Response(routeRes.body, { status: routeRes.status, headers: routeRes.headers });
    }
    const route = (await routeRes.json()) as {
      shardId: string;
      vbucket: number;
      catalogShardCount: number | null;
      partitionKeyColumn: string | null;
      indexes?: IndexDefinition[];
      migrationStatus?: string;
      targetShardId?: string;
    };
    if (route.catalogShardCount !== null && route.catalogShardCount !== currentCount) {
      return json(
        {
          error: `Catalog shard count mismatch: cluster was initialized with ${route.catalogShardCount} catalog shards, but this Worker is configured for ${currentCount}.`,
        },
        409,
      );
    }

    const partitionKeyColumn = route.partitionKeyColumn ?? UNSET_PARTITION_KEY_COLUMN;
    const validation = validateMutation(mutation, partitionKeyColumn);
    if (!validation.ok) {
      return json({ error: { code: validation.code, message: validation.error, fix: "Check the request against the error message above." } }, validation.status);
    }

    const indexes = route.indexes ?? [];
    const indexedColumns = Array.from(new Set(indexes.flatMap((i) => i.columns)));

    const missingIndexedValues = requireIndexedColumnsForInsert(mutation.op, indexedColumns, mutation.values);
    if (missingIndexedValues) {
      return json({ error: missingIndexedValues }, 400);
    }

    // Same pre-read rationale as Chunk 2's /v1/mutate: needed to remove a
    // now-stale index entry on update/delete/upsert. Same known limitation
    // too — this read happens before /begin acquires any row lock, so a
    // concurrent write between the read and the 2PC prepare is possible;
    // accepted here for consistency with the already-shipped /v1/mutate path.
    //
    // rawRow is the row's actual state as of this point in the batch
    // (simulated, not necessarily what's in the DB yet) — fetched
    // UNCONDITIONALLY by partition key (not scoped to this mutation's
    // `where`), so a later mutation in the batch still sees the row's real
    // state even when THIS mutation's own `where` doesn't match it. `where`
    // matching is applied separately in JS (simulatedRowMatchesWhere), the
    // same way SQLite will evaluate it once /begin actually runs the batch.
    const stateKey = rowKey(mutation.tenantId, mutation.table, mutation.partitionKey);
    let rawRow: Record<string, unknown> | null = null;
    if (indexedColumns.length > 0 && mutation.op !== "insert") {
      if (simulatedRowState.has(stateKey)) {
        rawRow = simulatedRowState.get(stateKey)!;
      } else {
        const safeTable = `"${mutation.table}"`;
        const safePkCol = `"${partitionKeyColumn}"`;
        const selectCols = indexedColumns.map((c) => `"${c}"`).join(", ");
        const preReadRes = await routeToShard(env, route.shardId, "/execute", {
          sql: `SELECT ${selectCols} FROM ${safeTable} WHERE ${safePkCol} = ?`,
          params: [mutation.partitionKey],
          requestId: `tx-index-preread-${crypto.randomUUID()}`,
          isMutation: false,
        });
        if (preReadRes.ok) {
          const preReadBody = (await preReadRes.json()) as { rows?: Array<Record<string, unknown>> };
          rawRow = preReadBody.rows?.[0] ?? null;
        }
      }
    }

    // upsert always "hits" (it always inserts-or-updates); insert never
    // needs a prior row; update/delete only affect the row if it exists AND
    // matches any extra `where` predicates — mirrors what compileMutation's
    // WHERE clause will actually filter on once /begin runs it.
    const matched =
      mutation.op === "update" || mutation.op === "delete" ? simulatedRowMatchesWhere(rawRow, mutation.where) : true;
    const beforeRow: Record<string, unknown> | null = matched ? rawRow : null;

    const { sql, params } = compileMutation(mutation, partitionKeyColumn);
    const migrating =
      route.targetShardId !== undefined && (route.migrationStatus === "backfilling" || route.migrationStatus === "cutover");
    addIntent(route.shardId, {
      sql,
      params,
      tenantId: mutation.tenantId,
      table: mutation.table,
      partitionKey: mutation.partitionKey,
      vbucket: route.vbucket,
      op: mutation.op,
      ...(migrating ? { mirrorTargetShardId: route.targetShardId } : {}),
    });

    if (indexes.length > 0) {
      const deltas = computeIndexDeltas(indexes, mutation.op, beforeRow, mutation.op === "delete" ? undefined : mutation.values);
      if (deltas.length > 0) {
        // Milestone 3, Chunk 2: each index's placement ring is its own
        // PINNED placement_ring_json (already on route.indexes), never a
        // freshly fetched live shard list.
        const ringByIndex = new Map(indexes.map((i) => [i.indexName, i.ring]));
        const now = new Date().toISOString();
        for (const delta of deltas) {
          const ring = ringByIndex.get(delta.indexName) ?? [];
          if (ring.length === 0) continue; // no pinned ring recorded — nothing safe to write to
          const syntheticTable = `${SYNTHETIC_INDEX_TABLE_PREFIX}${delta.indexName}`;
          if (delta.oldKeyJson !== null) {
            const oldShardId = indexShardIdForKey(mutation.table, delta.indexName, delta.oldKeyJson, ring);
            addIntent(oldShardId, {
              sql: "DELETE FROM __cf_indexes WHERE table_name = ? AND index_name = ? AND index_key_json = ? AND partition_key = ?",
              params: [mutation.table, delta.indexName, delta.oldKeyJson, mutation.partitionKey],
              tenantId: mutation.tenantId,
              table: syntheticTable,
              partitionKey: delta.oldKeyJson,
            });
          }
          if (delta.newKeyJson !== null) {
            const newShardId = indexShardIdForKey(mutation.table, delta.indexName, delta.newKeyJson, ring);
            addIntent(newShardId, {
              sql: "INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
              params: [mutation.table, delta.indexName, delta.newKeyJson, mutation.partitionKey, route.shardId, mutation.tenantId, now],
              tenantId: mutation.tenantId,
              table: syntheticTable,
              partitionKey: delta.newKeyJson,
            });
          }
        }
      }
    }

    // Advance the simulated row state for any LATER mutation in this batch
    // targeting the same row.
    if (indexedColumns.length > 0) {
      let nextRow: Record<string, unknown> | null;
      if (mutation.op === "insert") {
        nextRow = { ...(mutation.values ?? {}), [partitionKeyColumn]: mutation.partitionKey };
      } else if (!matched) {
        nextRow = rawRow; // unaffected by this mutation — carry state forward unchanged
      } else if (mutation.op === "delete") {
        nextRow = null;
      } else {
        // update or upsert: merge caller's values over the row's prior state.
        nextRow = { ...(rawRow ?? {}), ...(mutation.values ?? {}), [partitionKeyColumn]: mutation.partitionKey };
      }
      simulatedRowState.set(stateKey, nextRow);
    }
  }

  // txId is derived from the first mutation's tenantId — every mutation is
  // guaranteed to share the same tenantId by this point, since a mismatched
  // tenantId would already have 401'd against that mutation's /route call
  // above (routing forwards the caller's own bearer token, which is scoped
  // to a single tenant).
  const txId = await sha256Hex(JSON.stringify([body.mutations[0].tenantId, body.requestId]));
  const participants = Array.from(compiledByShardId.entries()).map(([shardId, intents]) => ({ shardId, intents }));

  const coordinatorId = env.COORDINATOR.idFromName(txId);
  const coordinatorStub = env.COORDINATOR.get(coordinatorId);
  const beginRes = await coordinatorStub.fetch("https://coordinator.internal/begin", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ txId, participants }),
  });
  const beginBody = await beginRes.json();
  return new Response(JSON.stringify(beginBody), {
    status: beginRes.status,
    headers: { "content-type": "application/json" },
  });
}

async function handleAdminTxStatus(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as { txId?: string };
  if (!body.txId) {
    return json({ error: "Missing txId" }, 400);
  }
  const id = env.COORDINATOR.idFromName(body.txId);
  const stub = env.COORDINATOR.get(id);
  const res = await stub.fetch("https://coordinator.internal/tx-status", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ txId: body.txId }),
  });
  return new Response(res.body, { status: res.status, headers: res.headers });
}

async function handleAdminTxForceAbort(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as { txId?: string };
  if (!body.txId) {
    return json({ error: "Missing txId" }, 400);
  }
  const id = env.COORDINATOR.idFromName(body.txId);
  const stub = env.COORDINATOR.get(id);
  const res = await stub.fetch("https://coordinator.internal/force-abort", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ txId: body.txId }),
  });
  return new Response(res.body, { status: res.status, headers: res.headers });
}

/** Stage 4 (approved topology-lock design): admin inspection of the current
 * cluster-wide topology lock — a thin forwarder to catalog-0's own
 * /topology-lock-status (the lock's single canonical home). */
async function handleAdminTopologyLockStatus(request: Request, env: Env): Promise<Response> {
  const res = await routeToCatalog(env, "catalog-0", "/topology-lock-status", {}, request.headers.get("authorization") ?? undefined);
  return new Response(res.body, { status: res.status, headers: res.headers });
}

/** Stage 4: the operator escape hatch for a stuck topology lock (a crashed
 * operation that never released it, or one that's still heartbeating but
 * needs to be forcibly cleared) — the same class of manual override as
 * /admin/tx-force-abort for a wedged 2PC transaction. Forwards straight to
 * catalog-0's /release-topology-lock, which deletes the row IFF the given
 * operationId currently matches (idempotent no-op otherwise — an operator who
 * doesn't know the exact operationId should read /admin/topology-lock-status
 * first). */
async function handleAdminForceReleaseTopologyLock(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as { operationId?: string };
  if (!body.operationId) {
    return json({ error: { code: "MISSING_FIELDS", message: "Missing operationId.", fix: "Read /admin/topology-lock-status to find the current holder's operationId." } }, 400);
  }
  const res = await routeToCatalog(env, "catalog-0", "/release-topology-lock", { operationId: body.operationId }, request.headers.get("authorization") ?? undefined);
  return new Response(res.body, { status: res.status, headers: res.headers });
}

const MAX_INDEX_QUERY_LIMIT = 100;
const DEFAULT_INDEX_QUERY_LIMIT = 20;

/** Milestone 2, Chunk 4. Tenant-facing point lookup by a registered
 * secondary index — exact full-tuple only (no leftmost-prefix yet, see the
 * design doc's Open Questions). Resolves in three hops: CatalogDO validates
 * the tenant token and the index's columns, the computed index shard
 * resolves matching (partitionKey, sourceShardId) pairs, then each match's
 * base row is read from its own shard. Because /v1/mutate's index
 * maintenance is async (Chunk 2), a matched entry can be stale by the time
 * it's read — the base row is re-checked against the queried tuple before
 * being returned, so a stale delete/update never surfaces a wrong result;
 * it's silently excluded, same as if the index entry didn't exist yet. */
async function handleV1IndexQuery(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as {
    table?: string;
    indexName?: string;
    tenantId?: string;
    values?: Record<string, unknown>;
    limit?: number;
  };
  if (!body.table || !body.indexName || !body.tenantId || !body.values) {
    return json({ error: { code: "MISSING_FIELDS", message: "Missing table, indexName, tenantId, or values." } }, 400);
  }
  const limit = Math.max(1, Math.min(MAX_INDEX_QUERY_LIMIT, body.limit ?? DEFAULT_INDEX_QUERY_LIMIT));

  const catalogShardId = catalogShardIdForTenant(env, body.tenantId);
  const lookupRes = await routeToCatalog(
    env,
    catalogShardId,
    "/lookup-index",
    { table: body.table, indexName: body.indexName, tenantId: body.tenantId },
    request.headers.get("authorization") ?? undefined,
  );
  if (!lookupRes.ok) {
    return new Response(lookupRes.body, { status: lookupRes.status, headers: lookupRes.headers });
  }
  const lookupBody = (await lookupRes.json()) as { columns: string[]; partitionKeyColumn: string; ring: string[]; evacFromShards?: string[] };
  const missing = lookupBody.columns.filter((c) => !(c in (body.values as Record<string, unknown>)));
  if (missing.length > 0) {
    return json(
      {
        error: {
          code: "INCOMPLETE_INDEX_KEY",
          message: `Missing value(s) for indexed column(s): ${missing.join(", ")}.`,
          fix: "Exact full-tuple lookups require a value for every column the index covers (leftmost-prefix lookups are not yet supported).",
        },
      },
      400,
    );
  }

  // Milestone 3, Chunk 2: this index's PINNED placement ring
  // (index_rules.placement_ring_json, captured once at /admin/create-index)
  // — never a freshly fetched live shard list, so /admin/split-vbucket or
  // /admin/drain-shard changing the active shard set never moves where an
  // already-written index entry is found.
  const ring = lookupBody.ring;
  if (ring.length === 0) {
    return json({ error: { code: "NO_SHARDS", message: "This index has no recorded placement ring." } }, 400);
  }

  const indexKeyJson = JSON.stringify(lookupBody.columns.map((c) => (body.values as Record<string, unknown>)[c] ?? null));
  const indexShardId = indexShardIdForKey(body.table, body.indexName, indexKeyJson, ring);

  const safeTable = `"${body.table}"`;
  const safePkCol = `"${lookupBody.partitionKeyColumn}"`;
  const queriedValues = body.values as Record<string, unknown>;

  // Eng-review fix: LIMIT used to apply to the raw __cf_indexes scan before
  // the staleness re-check ran, so a run of stale entries at the front could
  // starve out live matches that exist further down the index — an
  // under-filled or empty result even though enough live rows exist. Instead,
  // page through raw entries (ordered by partition_key for a stable cursor)
  // and keep pulling batches until `limit` verified rows are collected or the
  // index is exhausted. rawScanCap bounds total work against a pathologically
  // stale index (e.g. after a burst of deletes whose async cleanup hasn't
  // caught up yet) rather than scanning without bound.
  const tenantCatalogShardId = catalogShardIdForTenant(env, body.tenantId);

  // Codex round-14 P2 (read-visibility during evacuation): ring evacuation
  // repoints the query-visible ring to the substitute BEFORE it copies the
  // draining shard's existing entries. During that window indexShardId (from
  // the repointed ring) can be the substitute, which doesn't yet hold a
  // pre-existing key's entry — so a lookup would wrongly return empty. While a
  // read-shadow (evacFromShards) exists for the index, DUAL-LOOK-UP: query the
  // current-ring shard AND the draining shard(s), and dedupe by partition_key
  // (a copied-but-not-yet-deleted entry can appear on both). Once evacuation
  // completes the shadow clears and reads hit only the substitute.
  const evacFromShards = lookupBody.evacFromShards ?? [];
  const indexShardsToQuery = Array.from(new Set([indexShardId, ...evacFromShards]));

  // Gather candidate entries (partition_key) for the exact key from every
  // relevant index shard, deduped by partition_key. rawScanCap bounds work
  // against a pathologically stale index (e.g. a burst of deletes whose async
  // cleanup hasn't caught up yet).
  //
  // Codex round-15 P1 #2: filter by tenant_id IN THE SQL PREDICATE, before
  // ORDER BY/LIMIT — never only during hydration. A shared indexed value can
  // have entries for many tenants; if another tenant owns the first rawScanCap
  // partition keys for this (table, index, key), a post-scan filter would leave
  // THIS tenant's matching entries past the cap, never scanned → an empty /
  // under-filled result despite live rows. Binding tenant_id here makes the cap
  // apply to this tenant's own entries only, on every shard queried.
  const rawScanCap = limit * 5;
  const candidateByPk = new Map<string, { partition_key: string }>();
  for (const shard of indexShardsToQuery) {
    const indexRes = await routeToShard(env, shard, "/execute", {
      sql: "SELECT partition_key FROM __cf_indexes WHERE table_name = ? AND index_name = ? AND index_key_json = ? AND tenant_id = ? ORDER BY partition_key ASC LIMIT ?",
      params: [body.table, body.indexName, indexKeyJson, body.tenantId, rawScanCap],
      requestId: `index-query-lookup-${crypto.randomUUID()}`,
      isMutation: false,
    });
    if (!indexRes.ok) {
      // The current-ring shard failing is a real error. A supplementary evac
      // shard failing (e.g. already decommissioned) is skipped — anything it
      // held has, by the time it's gone, been copied to the substitute.
      if (shard === indexShardId) {
        return new Response(indexRes.body, { status: indexRes.status, headers: indexRes.headers });
      }
      continue;
    }
    const indexBody = (await indexRes.json()) as { rows?: Array<{ partition_key: string }> };
    for (const m of indexBody.rows ?? []) {
      if (!candidateByPk.has(m.partition_key)) candidateByPk.set(m.partition_key, m);
    }
  }
  const ownMatches = Array.from(candidateByPk.values()).sort((a, b) =>
    a.partition_key < b.partition_key ? -1 : a.partition_key > b.partition_key ? 1 : 0,
  );

  // Resolve every candidate's CURRENT base shard in ONE tenant-authenticated
  // /route-batch call — Chunk 2's read-time re-routing (a migrated base row is
  // found on its new shard) is preserved (route-batch reads the live map).
  const shardByPk = new Map<string, string>();
  if (ownMatches.length > 0) {
    const batchRes = await routeToCatalog(
      env,
      tenantCatalogShardId,
      "/route-batch",
      { table: body.table, tenantId: body.tenantId, partitionKeys: ownMatches.map((m) => m.partition_key) },
      request.headers.get("authorization") ?? undefined,
    );
    if (!batchRes.ok) {
      return new Response(batchRes.body, { status: batchRes.status, headers: batchRes.headers });
    }
    const batchBody = (await batchRes.json()) as { routes: Array<{ partitionKey: string; shardId: string | null }> };
    for (const r of batchBody.routes) {
      if (r.shardId) shardByPk.set(r.partitionKey, r.shardId);
    }
  }

  // Hydrate each candidate (concurrently, order preserved) with the staleness
  // re-check, then take up to `limit`.
  const hydrated = await Promise.all(
    ownMatches.map(async (match): Promise<Record<string, unknown> | null> => {
      const shardId = shardByPk.get(match.partition_key);
      if (!shardId) return null; // no mapping (shouldn't happen) — skip
      const rowRes = await routeToShard(env, shardId, "/execute", {
        sql: `SELECT * FROM ${safeTable} WHERE ${safePkCol} = ?`,
        params: [match.partition_key],
        requestId: `index-query-hydrate-${crypto.randomUUID()}`,
        isMutation: false,
      });
      if (!rowRes.ok) return null; // base row/shard unreachable — skip rather than fail the whole query
      const rowBody = (await rowRes.json()) as { rows?: Array<Record<string, unknown>> };
      const row = rowBody.rows?.[0];
      if (!row) return null; // row deleted since the index entry was written — stale, exclude
      // Staleness re-check (async index maintenance): only surface a row that
      // still actually matches the queried tuple.
      const stillMatches = lookupBody.columns.every((c) => row[c] === queriedValues[c]);
      return stillMatches ? row : null;
    }),
  );
  const rows: Array<Record<string, unknown>> = [];
  for (const row of hydrated) {
    if (row === null) continue;
    rows.push(row);
    if (rows.length >= limit) break;
  }
  return json({ rows });
}

const DEFAULT_TABLE_SCAN_LIMIT = 100;
const MAX_TABLE_SCAN_LIMIT = 500;

/** Opaque cursor shape for POST /v1/table-scan: one afterPartitionKey per
 * shard in the tenant's catalog shard's pool at the time this cursor was
 * issued. A shard with no entry (either because the cursor predates it, or
 * because a fresh scan hasn't touched it yet) starts at "" — scan from the
 * beginning. Exported for direct unit testing of the encode/decode round-trip
 * and the merge invariant, independent of the HTTP route. */
export type TableScanCursor = { shardCursors: Record<string, string> };

/** Base64-JSON encode, UTF-8-safe (partition keys are arbitrary strings, so a
 * plain btoa() over the raw JSON string would throw on non-Latin1 input). */
export function encodeTableScanCursor(cursor: TableScanCursor): string {
  const bytes = new TextEncoder().encode(JSON.stringify(cursor));
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary);
}

/** Decodes a client-supplied cursor. Returns null for anything that fails to
 * base64/JSON-decode or doesn't have the expected shape — the caller turns
 * that into 400 INVALID_CURSOR rather than guessing at a partial cursor
 * (client-supplied input the Worker never itself produced this way is
 * otherwise unvalidated). Does NOT check the shard-id keys against the
 * current active shard set — that requires the live shard list, checked
 * separately by the caller once it's fetched one. */
export function decodeTableScanCursor(raw: string): TableScanCursor | null {
  try {
    const binary = atob(raw);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (
      parsed !== null &&
      typeof parsed === "object" &&
      "shardCursors" in parsed &&
      (parsed as { shardCursors: unknown }).shardCursors !== null &&
      typeof (parsed as { shardCursors: unknown }).shardCursors === "object" &&
      !Array.isArray((parsed as { shardCursors: unknown }).shardCursors) &&
      Object.values((parsed as { shardCursors: Record<string, unknown> }).shardCursors).every((v) => typeof v === "string")
    ) {
      return { shardCursors: (parsed as TableScanCursor).shardCursors };
    }
    return null;
  } catch {
    return null;
  }
}

type ShardScanPage = {
  shardId: string;
  rows: Array<{ partitionKey: string; row: Record<string, unknown> }>;
  // Both optional, defaulting to `rows.length` / undefined below, purely so
  // the many pre-existing unit tests below that construct pages by hand
  // (modelling "no skips occurred this call") don't all need updating — every
  // real caller (handleV1TableScan) always supplies both, straight off the
  // shard's response.
  ownerRowsScanned?: number;
  lastOwnerKeyScanned?: string;
};

/** Merges every shard's page into one ascending-by-partition_key result
 * (ties broken by shardId ascending — see criterion 2/the merge spec),
 * truncates to `overallLimit`, and computes the next per-shard cursor map per
 * TWO composed invariants (Codex round-4 fix: the second one below is new;
 * the first already existed and must keep working unchanged):
 *
 *  (a) never advance a shard's cursor past the last OWNER key it actually
 *      scanned (`lastOwnerKeyScanned`) — but only once that shard's owner
 *      query fully consumed its LIMIT (`ownerRowsScanned === perShardLimit`);
 *      short of that, the shard is genuinely exhausted and there's nothing
 *      to bound.
 *  (b) never advance a shard's cursor past the partition_key of the LAST ROW
 *      FROM THAT SHARD actually kept in the truncated response — never to a
 *      row that was fetched but then cut by the overall-limit truncation, so
 *      the next call re-fetches (never skips) it.
 *
 * These are NOT simply composed by taking whichever position is earlier
 * (round-5 fix: that was the original, INCORRECT composition — see the inline
 * comment above invariant (a)'s loop for why). Instead, per shard: if it
 * fetched zero rows this batch, or every row it fetched survived truncation,
 * (a) is free to advance the cursor past (b)'s position, all the way to
 * `lastOwnerKeyScanned`; if cross-shard truncation cut some (but not all) of
 * what it fetched, (a) must leave (b)'s bound untouched. (b) alone is what let
 * a skipped owner row (base row deleted between the shard's two queries — see
 * /tenant-scan-page) silently look like shard exhaustion, because it only
 * ever looked at rows the shard actually *delivered*, never how far it had
 * *scanned* — and naively letting (a) always win whenever it's later would
 * skip a row truncated away by the cross-shard merge. Composing them
 * correctly handles both failure modes without either one masking the other.
 * Exported (and kept side-effect-free) for direct unit testing of the
 * merge/truncate/exhaustion invariants — the largest risk item per the
 * spec. */
export function mergeTableScanPages(
  pages: ShardScanPage[],
  overallLimit: number,
  perShardLimit: number,
  priorShardCursors: Record<string, string>,
): { rows: Array<Record<string, unknown>>; nextShardCursors: Record<string, string> | null } {
  type Entry = { shardId: string; partitionKey: string; row: Record<string, unknown> };
  const all: Entry[] = [];
  for (const page of pages) {
    for (const r of page.rows) all.push({ shardId: page.shardId, partitionKey: r.partitionKey, row: r.row });
  }
  all.sort((a, b) => {
    if (a.partitionKey < b.partitionKey) return -1;
    if (a.partitionKey > b.partitionKey) return 1;
    return a.shardId < b.shardId ? -1 : a.shardId > b.shardId ? 1 : 0;
  });
  const truncated = all.slice(0, overallLimit);

  // Every shard this call touched keeps a cursor entry — either the prior
  // value (untouched, or every one of its rows got truncated away) or an
  // advanced one (some of its rows made the cut). A shard silently dropped
  // from the map would restart from "" next time, re-scanning from the
  // beginning instead of resuming — that's wasteful, not lossy, but still
  // wrong, so every fanned-out shard gets an explicit entry.
  const nextShardCursors: Record<string, string> = { ...priorShardCursors };
  for (const page of pages) {
    if (!(page.shardId in nextShardCursors)) nextShardCursors[page.shardId] = "";
  }
  // Invariant (b): never advance past the last row from a shard actually
  // kept in the truncated response.
  for (const entry of truncated) {
    const current = nextShardCursors[entry.shardId] ?? "";
    if (entry.partitionKey > current) nextShardCursors[entry.shardId] = entry.partitionKey;
  }
  // Invariant (a): a shard whose owner query fully consumed its LIMIT may
  // hold more owner rows beyond this batch, and it's safe to resume scanning
  // past everything it already scanned (lastOwnerKeyScanned) — but ONLY once
  // we know invariant (b) isn't holding the cursor back on purpose. There are
  // two genuinely different reasons a shard can end up with zero of its rows
  // in `truncated` this round, and they demand opposite treatment (round-5
  // fix: the composition below used to always take whichever of (a)/(b) was
  // EARLIER, which silently assumed every such case was the first one):
  //
  //  (A) cross-shard truncation cut rows this shard DID fetch, because other
  //      shards' rows sorted earlier — invariant (b) is already correctly
  //      pinning the cursor at the last row this shard actually got to keep
  //      (or its untouched prior cursor, if none survived), so those cut
  //      rows get re-fetched, not skipped, next call. (a) must not loosen
  //      that bound.
  //  (B) every owner key in this shard's OWN batch resolved to zero rows —
  //      e.g. every one of their base rows was deleted between
  //      /tenant-scan-page's two queries (see shard.ts). There is nothing
  //      fetched to hold back for, and since lastOwnerKeyScanned is always
  //      >= the prior cursor (the owner query is `partition_key > prior`),
  //      "the earlier of (a)/(b)" always picks the untouched prior cursor in
  //      this case — the cursor never advances, the next call re-issues the
  //      identical query, and the scan stalls forever instead of finishing.
  //
  // page.rows.length (what THIS shard itself fetched, before the cross-shard
  // merge/truncate step) vs. how many of that shard's rows survived into
  // `truncated` is what tells the two cases apart.
  const deliveredCountByShard: Record<string, number> = {};
  for (const entry of truncated) {
    deliveredCountByShard[entry.shardId] = (deliveredCountByShard[entry.shardId] ?? 0) + 1;
  }
  for (const page of pages) {
    const ownerRowsScanned = page.ownerRowsScanned ?? page.rows.length;
    if (ownerRowsScanned < perShardLimit) continue; // genuinely exhausted; nothing to bound
    if (page.lastOwnerKeyScanned === undefined) continue;
    const current = nextShardCursors[page.shardId] ?? "";
    if (page.rows.length === 0) {
      // Case (B): this shard fetched nothing at all, so invariant (b) never
      // touched it (there's no entry of its in `truncated` either way).
      // Nothing to hold back for — always safe, and necessary, to advance
      // past everything scanned.
      if (page.lastOwnerKeyScanned > current) nextShardCursors[page.shardId] = page.lastOwnerKeyScanned;
      continue;
    }
    const deliveredCount = deliveredCountByShard[page.shardId] ?? 0;
    if (deliveredCount < page.rows.length) {
      // Case (A): some of what this shard fetched was cut by cross-shard
      // truncation. Leave invariant (b)'s bound exactly as computed.
      continue;
    }
    // Everything this shard fetched made it into the response — safe to
    // advance past a trailing skipped-owner-row gap beyond the last
    // delivered row too.
    if (page.lastOwnerKeyScanned > current) nextShardCursors[page.shardId] = page.lastOwnerKeyScanned;
  }

  // nextCursor must be present whenever there's ANY reason to believe more
  // rows exist beyond what was just returned:
  //  - the OVERALL merge truncated away some fetched-but-unreturned rows
  //    (a real bug found during implementation: a shard can return fewer
  //    than its OWN perShardLimit — correctly signalling that shard has
  //    nothing further beyond what it fetched — while still having had one
  //    of ITS OWN fetched rows cut by the overall-limit truncation, because
  //    another shard's rows sorted earlier. Checking only "did any shard hit
  //    its own per-shard cap" misses this and silently drops the cut row —
  //    its cursor legitimately didn't advance past it, but with no
  //    nextCursor the client never calls again to pick it up), OR
  //  - any single shard's owner-row query fully consumed its LIMIT
  //    (`ownerRowsScanned === perShardLimit` — Codex round-4 fix: this used
  //    to check `page.rows.length >= perShardLimit`, the count of rows
  //    actually resolved/returned, which a single skipped owner row — its
  //    base row deleted between /tenant-scan-page's two queries — could pull
  //    below perShardLimit even though the owner query's LIMIT was fully
  //    consumed and __cf_row_owners may hold more keys beyond this batch).
  const anyRowsTruncatedAway = all.length > truncated.length;
  const anyShardMayHaveMore = anyRowsTruncatedAway || pages.some((page) => (page.ownerRowsScanned ?? page.rows.length) >= perShardLimit);
  return {
    rows: truncated.map((e) => e.row),
    nextShardCursors: anyShardMayHaveMore ? nextShardCursors : null,
  };
}

/** Milestone 4 (tenant-scoped table scan). Lists a tenant's own rows in a
 * registered table, cursor-paginated, with no arbitrary filters — the query
 * is mechanically constructed (table + tenantId + cursor + limit only),
 * matching the safe-by-construction pattern /v1/mutate's compileMutation
 * already established for writes, rather than the raw-SQL pattern that
 * failed for the old (pre-Milestone-3) tenant read path. This is the direct
 * replacement for that removed capability: /v1/index-query only supports
 * exact-tuple lookups, and there was otherwise no way for a tenant to
 * enumerate its own rows without already knowing an indexed value. */
async function handleV1TableScan(request: Request, env: Env): Promise<Response> {
  const body = (await request.json()) as { tenantId?: string; table?: string; limit?: number; cursor?: string | null };
  if (!body.tenantId || !body.table) {
    return json({ error: { code: "MISSING_FIELDS", message: "Missing tenantId or table.", fix: "Provide both tenantId and table." } }, 400);
  }
  // Codex P2 fix: reject non-integer/fractional/non-positive limits here too
  // (previously only the upper bound was enforced) -- defense in depth so a
  // malformed limit (e.g. 2.5) never even reaches a shard, where it used to
  // trip SQLite's LIMIT binding and, before this fix's shard.ts change, get
  // silently swallowed into a fake-empty {rows: []} instead of surfacing as
  // a real error.
  if (
    body.limit !== undefined &&
    (typeof body.limit !== "number" || !Number.isInteger(body.limit) || body.limit < 1 || body.limit > MAX_TABLE_SCAN_LIMIT)
  ) {
    return json(
      {
        error: {
          code: "LIMIT_EXCEEDED",
          message: `limit must be a positive integer no greater than ${MAX_TABLE_SCAN_LIMIT}.`,
          fix: `Omit limit (default ${DEFAULT_TABLE_SCAN_LIMIT}) or pass an integer in [1, ${MAX_TABLE_SCAN_LIMIT}].`,
        },
      },
      400,
    );
  }
  const limit = Math.max(1, Math.min(MAX_TABLE_SCAN_LIMIT, body.limit ?? DEFAULT_TABLE_SCAN_LIMIT));

  // Defense-in-depth (structurally unreachable in practice — table_rules
  // should never contain a __cf_*/sqlite_* name — checked explicitly anyway,
  // per the spec). Cheap and needs no network round trip, so it runs first.
  // Codex P2 fix: isInternalTableName expects a NORMALIZED (unquoted,
  // lowercased) name — passing the raw request value let a case variant
  // (e.g. "SQLite_master", "__CF_row_owners") slip past this guard, the same
  // bug class (case-sensitivity bypass of an internal-table guard) that took
  // several review rounds to fully close on /v1/sql earlier in this
  // project's history. normalizeTableName is the same normalization
  // mutationTargetIsInternal already applies before its own
  // isInternalTableName check, for consistency.
  if (isInternalTableName(normalizeTableName(body.table))) {
    return json(
      {
        error: {
          code: "INTERNAL_TABLE_ACCESS_FORBIDDEN",
          message: `Table ${body.table} is an internal system table and cannot be scanned.`,
          fix: "Table scans are only available for tenant-registered tables.",
        },
      },
      403,
    );
  }

  // Syntactic cursor validation only — the semantic "shard-id keys are a
  // subset of the CURRENT active shard set" check needs the live shard list,
  // fetched below (topology can change between two calls).
  let requestedCursor: TableScanCursor | null = null;
  if (body.cursor) {
    requestedCursor = decodeTableScanCursor(body.cursor);
    if (!requestedCursor) {
      return json(
        {
          error: {
            code: "INVALID_CURSOR",
            message: "cursor failed to decode.",
            fix: "Omit cursor to restart the scan from the beginning.",
          },
        },
        400,
      );
    }
  }

  const authorization = request.headers.get("authorization") ?? undefined;
  const catalogShardId = catalogShardIdForTenant(env, body.tenantId);

  // Auth: identical tenant-token verification to /v1/index-query — reuses
  // CatalogDO.checkTenantAuth via /lookup-table-scan (the same combined
  // "auth + registry gate" role /lookup-index plays for /v1/index-query),
  // rather than re-implementing the check here.
  const lookupRes = await routeToCatalog(env, catalogShardId, "/lookup-table-scan", { table: body.table, tenantId: body.tenantId }, authorization);
  if (!lookupRes.ok) {
    return new Response(lookupRes.body, { status: lookupRes.status, headers: lookupRes.headers });
  }
  const lookupBody = (await lookupRes.json()) as { partitionKeyColumn: string; provenanceComplete: boolean };

  const listRes = await routeToCatalog(env, catalogShardId, "/list-shards", {});
  if (!listRes.ok) {
    return new Response(listRes.body, { status: listRes.status, headers: listRes.headers });
  }
  const shardIds = ((await listRes.json()) as { shardIds: string[] }).shardIds;
  if (shardIds.length === 0) {
    return json({ error: { code: "NO_SHARDS", message: "No shards exist yet.", fix: "Call /admin/init first." } }, 400);
  }

  if (requestedCursor) {
    const activeShardIdSet = new Set(shardIds);
    const staleShardId = Object.keys(requestedCursor.shardCursors).find((id) => !activeShardIdSet.has(id));
    if (staleShardId !== undefined) {
      return json(
        {
          error: {
            code: "INVALID_CURSOR",
            message: `cursor names shard ${staleShardId}, which is no longer in this tenant's active shard set.`,
            fix: "Omit cursor to restart the scan from the beginning.",
          },
        },
        400,
      );
    }
  }
  const priorShardCursors = requestedCursor?.shardCursors ?? {};

  const startedAt = Date.now();
  const perShardLimit = Math.min(TENANT_SCAN_PAGE_SIZE, limit);

  type ShardPageOutcome = {
    shardId: string;
    ok: boolean;
    rows: Array<{ partitionKey: string; row: Record<string, unknown> }>;
    ownerRowsScanned: number;
    lastOwnerKeyScanned?: string;
  };
  const pageResults = await batchedMap(shardIds, SHARD_FANOUT_CONCURRENCY, async (shardId): Promise<ShardPageOutcome> => {
    const res = await routeToShard(env, shardId, "/tenant-scan-page", {
      table: body.table,
      partitionKeyColumn: lookupBody.partitionKeyColumn,
      tenantId: body.tenantId,
      afterPartitionKey: priorShardCursors[shardId] ?? "",
      limit: perShardLimit,
    });
    if (!res.ok) return { shardId, ok: false, rows: [], ownerRowsScanned: 0 };
    const resBody = (await res.json()) as {
      rows?: Array<{ partitionKey: string; row: Record<string, unknown> }>;
      ownerRowsScanned?: number;
      lastOwnerKeyScanned?: string;
    };
    const rows = resBody.rows ?? [];
    return {
      shardId,
      ok: true,
      rows,
      // Codex round-4 fix: ownerRowsScanned/lastOwnerKeyScanned are the
      // shard's true __cf_row_owners scan position (see /tenant-scan-page),
      // which mergeTableScanPages needs to detect shard exhaustion instead
      // of inferring it from rows.length -- a single owner row skipped for a
      // deleted base row must never look like exhaustion. The `?? rows.length`
      // fallback only matters if an older/mismatched shard build omits the
      // field; it reproduces the pre-fix (rows.length-based) inference rather
      // than crashing.
      ownerRowsScanned: resBody.ownerRowsScanned ?? rows.length,
      lastOwnerKeyScanned: resBody.lastOwnerKeyScanned,
    };
  });

  // Any shard failure fails the whole request — no silently-partial tenant
  // data in this MVP (see the spec's "Non-goals: Partial-result mode").
  const failed = pageResults.find((r) => !r.ok);
  if (failed) {
    return json(
      {
        error: {
          code: "SHARD_UNREACHABLE",
          shardId: failed.shardId,
          message: `Shard ${failed.shardId} did not respond.`,
          fix: "Retry — one or more shards did not respond.",
        },
      },
      502,
    );
  }

  const { rows, nextShardCursors } = mergeTableScanPages(
    pageResults.map((r) => ({ shardId: r.shardId, rows: r.rows, ownerRowsScanned: r.ownerRowsScanned, lastOwnerKeyScanned: r.lastOwnerKeyScanned })),
    limit,
    perShardLimit,
    priorShardCursors,
  );

  return json({
    rows,
    ...(nextShardCursors ? { nextCursor: encodeTableScanCursor({ shardCursors: nextShardCursors }) } : {}),
    provenance: {
      complete: lookupBody.provenanceComplete,
      ...(lookupBody.provenanceComplete
        ? {}
        : {
            fix: "Run POST /admin/backfill-provenance, then retry — some rows in this table have no owner recorded and are hidden from all scans until backfilled.",
          }),
    },
    scan: {
      catalogShardId,
      shardCount: shardIds.length,
      successCount: pageResults.length,
      scanMs: Date.now() - startedAt,
    },
  });
}

async function handleV1Scatter(request: Request, env: Env): Promise<Response> {
  // /v1/scatter reads across every tenant indiscriminately — that's inherently
  // an admin/operator operation, not a data-plane one, so it requires
  // ADMIN_TOKEN rather than a tenant token. The Worker's structural /admin/*
  // gate doesn't cover this path (it's under /v1/, not /admin/), so this
  // check is explicit rather than "for free" — the same class of bug that
  // previously left /admin/shard-stats unauthenticated.
  const scatterAuthError = requireAdminAuth(env, request);
  if (scatterAuthError) return scatterAuthError;

  const body = (await request.json()) as { sql: string; params?: unknown[]; limit?: number };

  if (!body.sql) {
    return json({ error: "Missing sql" }, 400);
  }
  if (isMutation(body.sql)) {
    return json({ error: "Scatter endpoint supports SELECT only." }, 400);
  }
  if (isDangerous(body.sql)) {
    return json({ error: "SQL statement not permitted." }, 403);
  }
  if (body.params !== undefined && !assertParamsArray(body.params)) {
    return json({ error: "params must be an array." }, 400);
  }

  const listResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({}));
  const failedList = firstCatalogFanOutFailure(listResults, "Failed to list shards from one or more catalog shards.");
  if (failedList) return failedList;
  const shardIds = listResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);

  const scatterStart = Date.now();
  const settled: Array<PromiseSettledResult<{ shardId: string; rows: unknown[] }>> = [];

  for (let i = 0; i < shardIds.length; i += SHARD_FANOUT_CONCURRENCY) {
    const batch = shardIds.slice(i, i + SHARD_FANOUT_CONCURRENCY);
    const batchSettled = await Promise.allSettled(
      batch.map(async (shardId) => {
        const shardRes = await routeToShard(env, shardId, "/execute", {
          sql: body.sql,
          params: body.params ?? [],
          requestId: crypto.randomUUID(),
          isMutation: false,
        });
        if (!shardRes.ok) {
          throw new Error(`shard ${shardId} responded ${shardRes.status}`);
        }
        const payload = (await shardRes.json()) as { rows?: unknown[] };
        return { shardId, rows: payload.rows ?? [] };
      }),
    );
    settled.push(...batchSettled);
  }

  const scatterMs = Date.now() - scatterStart;
  const outputs: Array<{ shardId: string; rows: unknown[] }> = [];
  const errors: Array<{ shardId: string; reason: string }> = [];

  for (let i = 0; i < settled.length; i++) {
    const result = settled[i];
    if (result.status === "fulfilled") {
      outputs.push(result.value);
    } else {
      errors.push({
        shardId: shardIds[i],
        reason: result.reason instanceof Error ? result.reason.message : String(result.reason),
      });
    }
  }

  const merged = outputs.flatMap((x) => x.rows);
  const capped = typeof body.limit === "number" ? merged.slice(0, body.limit) : merged;

  return json({
    observability: { scatterMs, shardCount: shardIds.length, successCount: outputs.length, errorCount: errors.length },
    rows: capped,
    perShard: outputs,
    ...(errors.length > 0 ? { errors } : {}),
  });
}

const ROUTES: Record<string, (request: Request, env: Env, ctx: ExecutionContext) => Promise<Response>> = {
  "/admin/init": handleAdminInit,
  "/admin/register-table": handleAdminRegisterTable,
  "/admin/create-table": handleAdminCreateTable,
  "/admin/split-vbucket": handleAdminSplitVbucket,
  "/admin/status": handleAdminStatus,
  "/admin/list-tables": handleAdminListTables,
  "/admin/drain-shard": handleAdminDrainShard,
  "/admin/shard-stats": handleAdminShardStats,
  "/admin/audit-log": handleAdminAuditLog,
  "/admin/register-tenant": handleAdminRegisterTenant,
  "/admin/revoke-tenant": handleAdminRevokeTenant,
  "/admin/set-partition-key-column": handleAdminSetPartitionKeyColumn,
  "/admin/create-index": handleAdminCreateIndex,
  "/admin/list-indexes": handleAdminListIndexes,
  "/admin/drop-index": handleAdminDropIndex,
  "/admin/tx-status": handleAdminTxStatus,
  "/admin/tx-force-abort": handleAdminTxForceAbort,
  "/admin/backfill-provenance": handleAdminBackfillProvenance,
  "/admin/set-row-owner": handleAdminSetRowOwner,
  "/admin/migrate-vbucket": handleAdminMigrateVbucket,
  "/admin/migrate-vbucket-status": handleAdminMigrateVbucketStatus,
  "/admin/migrate-vbucket-abort": handleAdminMigrateVbucketAbort,
  "/admin/drain-shard-status": handleAdminDrainShardStatus,
  "/admin/topology-lock-status": handleAdminTopologyLockStatus,
  "/admin/force-release-topology-lock": handleAdminForceReleaseTopologyLock,
  "/v1/sql": handleV1Sql,
  "/v1/mutate": handleV1Mutate,
  "/v1/tx": handleV1Tx,
  "/v1/index-query": handleV1IndexQuery,
  "/v1/table-scan": handleV1TableScan,
  "/v1/scatter": handleV1Scatter,
};

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    try {
      const url = new URL(request.url);

      if (request.method === "GET" && url.pathname === "/health") {
        return json({ ok: true, service: "cloudflare-shard-mvp" });
      }

      if (request.method !== "POST") {
        return json({ error: "Only POST is supported for this endpoint." }, 405);
      }

      // Structural safeguard: every /admin/* route requires the admin token,
      // checked once here rather than trusting each handler to remember to call
      // requireAdminAuth() itself (a per-handler check is exactly how
      // /admin/shard-stats ended up unauthenticated). CatalogDO applies its own
      // gate too for routes that pass through it — this is deliberately
      // redundant defense-in-depth, not a replacement for it.
      if (url.pathname.startsWith("/admin/")) {
        const authError = requireAdminAuth(env, request);
        if (authError) return authError;
      }

      const handler = ROUTES[url.pathname];
      if (handler) {
        return await handler(request, env, ctx);
      }

      return json({ error: `Unknown route: ${url.pathname}` }, 404);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log("worker.unhandled_error", { path: new URL(request.url).pathname, message });
      return json({ error: "Internal error." }, 500);
    }
  },
};
