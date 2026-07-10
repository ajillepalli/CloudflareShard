import { CatalogDO } from "./catalog";
import { ShardDO } from "./shard";
import { CoordinatorDO } from "./coordinator";
import { json } from "./http";
import { hashKey, indexShardIdForKey } from "./hash";
import { checkAdminAuth } from "./auth";
import { log } from "./log";
import { extractCreateTableName, isDangerous, isDangerousSchema, isMutation } from "./sql-safety";
import {
  compileMutation,
  IDENTIFIER_RE,
  participantKey,
  rowKey,
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
  const payload = (await request.json()) as Record<string, unknown>;
  const authorization = request.headers.get("authorization") ?? undefined;
  const results = await fanOutToAllCatalogs(env, "/register-table", () => payload, authorization);
  const failed = firstCatalogFanOutFailure(results, "One or more catalog shards failed to register the table.");
  if (failed) return failed;
  return json({ ok: true, catalogShardCount: results.length });
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

  const registerResults = await fanOutToAllCatalogs(
    env,
    "/register-table",
    () => ({ table: body.table, partitioning: body.partitioning, partitionKeyColumn: body.partitionKeyColumn }),
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

  const results = await fanOutToAllCatalogs(
    env,
    "/set-partition-key-column",
    () => body,
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

  const listResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({}));
  const listFailed = firstCatalogFanOutFailure(listResults, "Failed to list shards.");
  if (listFailed) return listFailed;
  const shardIds = listResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);
  if (shardIds.length === 0) {
    return json({ error: { code: "NO_SHARDS", message: "No shards exist yet.", fix: "Call /admin/init first." } }, 400);
  }

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
  // Milestone 3, Chunk 2: shardIds IS this index's pinned placement ring —
  // captured here, at creation time, and sent to CatalogDO to persist as
  // index_rules.placement_ring_json. Every later placement computation for
  // this index (write-path maintenance, /v1/index-query, this backfill loop
  // itself) must reuse this exact array for the index's entire lifetime.
  const registerResults = await fanOutToAllCatalogs(
    env,
    "/create-index",
    () => ({ indexName, table, columns, placementRing: shardIds }),
    request.headers.get("authorization") ?? undefined,
  );
  const registerFailed = firstCatalogFanOutFailure(registerResults, "Failed to register index.");
  if (registerFailed) return registerFailed;

  // Backfill: scan every shard's existing rows for this table, compute each
  // row's index entry, and write it to its own computed index shard.
  for (const shardId of shardIds) {
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
      const indexShardId = indexShardIdForKey(table, indexName, indexKeyJson, shardIds);
      const writeRes = await routeToShard(env, indexShardId, "/execute", {
        sql: `INSERT OR REPLACE INTO __cf_indexes (table_name, index_name, index_key_json, partition_key, source_shard_id, tenant_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
        params: [table, indexName, indexKeyJson, partitionKey, shardId, tenantId, new Date().toISOString()],
        requestId: `create-index-backfill-write-${indexName}-${crypto.randomUUID()}`,
        isMutation: true,
      });
      if (!writeRes.ok) {
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
  const res = await routeToCatalog(
    env,
    payload.catalogShardId,
    "/split-vbucket",
    payload,
    request.headers.get("authorization") ?? undefined,
  );
  return new Response(res.body, { status: res.status, headers: res.headers });
}

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

  for (const catalogShardId of catalogShardIds) {
    const [tablesRes, tenantsRes, vbMapRes, shardsRes] = await Promise.all([
      routeToCatalog(env, catalogShardId, "/list-tables", {}, authorization),
      routeToCatalog(env, catalogShardId, "/list-tenants", {}, authorization),
      routeToCatalog(env, catalogShardId, "/vbucket-map", {}, authorization),
      routeToCatalog(env, catalogShardId, "/list-shards", {}, authorization),
    ]);
    if (!tablesRes.ok) return new Response(tablesRes.body, { status: tablesRes.status, headers: tablesRes.headers });
    if (!tenantsRes.ok) return new Response(tenantsRes.body, { status: tenantsRes.status, headers: tenantsRes.headers });
    if (!vbMapRes.ok) return new Response(vbMapRes.body, { status: vbMapRes.status, headers: vbMapRes.headers });
    if (!shardsRes.ok) return new Response(shardsRes.body, { status: shardsRes.status, headers: shardsRes.headers });

    const tables = ((await tablesRes.json()) as { tables: Array<{ table_name: string; partition_key_column: string }> }).tables.filter(
      (t) => t.partition_key_column !== UNSET_PARTITION_KEY_COLUMN,
    );
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

  const res = await routeToCatalog(
    env,
    payload.catalogShardId,
    "/drain-shard",
    payload,
    request.headers.get("authorization") ?? undefined,
  );
  return new Response(res.body, { status: res.status, headers: res.headers });
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
  const body = (await request.json()) as SqlRequest;
  if (!body.sql || !body.table || !body.tenantId) {
    return json({ error: "Missing required fields: sql, table, tenantId." }, 400);
  }

  const mutating = isMutation(body.sql);

  if (isDangerous(body.sql)) {
    return json({ error: "SQL statement not permitted." }, 403);
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

  const requestId = body.requestId ?? crypto.randomUUID();
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
  });
  const shardExecuteMs = Date.now() - shardStart;

  if (!shardRes.ok) {
    return new Response(shardRes.body, { status: shardRes.status, headers: shardRes.headers });
  }

  // Milestone 3, Chunk 3: this vbucket is mid-migration — mirror the
  // now-applied write to the target with the same requestId, after source
  // success, never blocking or failing the client's response.
  if (mutating && route.targetShardId && (route.migrationStatus === "backfilling" || route.migrationStatus === "cutover")) {
    ctx.waitUntil(
      mirrorWriteBestEffort(env, route.shardId, route.targetShardId, body.sql, body.params ?? [], requestId, route.vbucket),
    );
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

/** Milestone 3, Chunk 3: mirrors an already-applied source write to the
 * migration target, best-effort — the client's write already succeeded on
 * the source (authoritative), so a mirror failure must never surface to the
 * client; it enqueues on the SOURCE shard's __cf_mirror_pending for
 * alarm-driven retry instead (the target may be the unreachable one).
 * Reuses the ORIGINAL requestId: the target's applied_requests dedupe makes
 * mirror + backfill + retry all safely re-appliable in any order. The
 * mirrored payload deliberately omits routing context (tenantId/table/
 * partitionKey/vbucket): the target must not enforce row locks or the
 * Chunk 4 fence against mirrored traffic (the source already enforced
 * both), and the target's own provenance for this row comes from Chunk 4's
 * /migrate-import, not from this mirror. Never throws — runs inside
 * ctx.waitUntil(). */
async function mirrorWriteBestEffort(
  env: Env,
  sourceShardId: string,
  targetShardId: string,
  sql: string,
  params: unknown[],
  requestId: string,
  vbucket: number,
): Promise<void> {
  try {
    const res = await routeToShard(env, targetShardId, "/execute", { sql, params, requestId, isMutation: true });
    if (res.ok) return;
    throw new Error(`target shard responded ${res.status}`);
  } catch (error) {
    log("worker.mirror_write_failed_enqueuing_retry", {
      sourceShardId,
      targetShardId,
      requestId,
      vbucket,
      message: error instanceof Error ? error.message : String(error),
    });
    try {
      await routeToShard(env, sourceShardId, "/enqueue-mirror-job", { targetShardId, sql, params, requestId, vbucket });
    } catch (enqueueError) {
      // Source shard itself unreachable for the enqueue — logged, not
      // swallowed silently. Chunk 4's cutover checksum (step 3) is the
      // backstop that catches a mirror lost this way before any flip.
      log("worker.mirror_job_enqueue_failed", {
        sourceShardId,
        targetShardId,
        requestId,
        vbucket,
        message: enqueueError instanceof Error ? enqueueError.message : String(enqueueError),
      });
    }
  }
}

/** Writes one __cf_indexes entry (insert/replace or delete), best-effort. On
 * failure, records a retry job on the BASE shard (not the index shard, which
 * may be the one that's unreachable) via /enqueue-index-job — ShardDO's
 * alarm() picks it up from there. Never throws: this always runs inside
 * ctx.waitUntil(), after the caller's response has already been sent, so
 * there's no one left to propagate an exception to. */
async function writeIndexEntryBestEffort(
  env: Env,
  baseShardId: string,
  targetShardId: string,
  sql: string,
  params: unknown[],
  requestId: string,
): Promise<void> {
  try {
    const res = await routeToShard(env, targetShardId, "/execute", { sql, params, requestId, isMutation: true });
    if (res.ok) return;
    throw new Error(`shard responded ${res.status}`);
  } catch (error) {
    log("worker.index_write_failed_enqueuing_retry", {
      baseShardId,
      targetShardId,
      requestId,
      message: error instanceof Error ? error.message : String(error),
    });
    try {
      await routeToShard(env, baseShardId, "/enqueue-index-job", { targetShardId, sql, params, requestId });
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

  const { sql, params } = compileMutation(body, partitionKeyColumn);
  const requestId = body.requestId ?? crypto.randomUUID();
  const shardRes = await routeToShard(env, route.shardId, "/execute", {
    sql,
    params,
    requestId,
    isMutation: true,
    tenantId: body.tenantId,
    table: body.table,
    partitionKey: body.partitionKey,
    vbucket: route.vbucket,
  });
  if (!shardRes.ok) {
    return new Response(shardRes.body, { status: shardRes.status, headers: shardRes.headers });
  }

  const shardPayload = (await shardRes.json()) as { rowsAffected?: number };
  const rowsAffected = shardPayload.rowsAffected ?? 0;

  // Milestone 3, Chunk 3: this vbucket is mid-migration — mirror the
  // now-applied compiled statement to the target with the same requestId,
  // after source success, never blocking or failing the client's response.
  if (route.targetShardId && (route.migrationStatus === "backfilling" || route.migrationStatus === "cutover")) {
    ctx.waitUntil(mirrorWriteBestEffort(env, route.shardId, route.targetShardId, sql, params, requestId, route.vbucket));
  }

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
          const syntheticTable = `__cf_indexes:${delta.indexName}`;
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
  const lookupBody = (await lookupRes.json()) as { columns: string[]; partitionKeyColumn: string; ring: string[] };
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
  const rows: Array<Record<string, unknown>> = [];
  const rawScanCap = limit * 5;
  let offset = 0;
  while (rows.length < limit && offset < rawScanCap) {
    const batchLimit = Math.min(limit, rawScanCap - offset);
    const indexRes = await routeToShard(env, indexShardId, "/execute", {
      sql: "SELECT partition_key, tenant_id FROM __cf_indexes WHERE table_name = ? AND index_name = ? AND index_key_json = ? ORDER BY partition_key ASC LIMIT ? OFFSET ?",
      params: [body.table, body.indexName, indexKeyJson, batchLimit, offset],
      requestId: `index-query-lookup-${crypto.randomUUID()}`,
      isMutation: false,
    });
    if (!indexRes.ok) {
      return new Response(indexRes.body, { status: indexRes.status, headers: indexRes.headers });
    }
    const indexBody = (await indexRes.json()) as { rows?: Array<{ partition_key: string; tenant_id: string }> };
    const matches = indexBody.rows ?? [];
    if (matches.length === 0) break; // index exhausted

    // Eng-review perf fix: each match's hydrate read is independent of every
    // other match's in this batch (different partition keys, potentially
    // different shards) — resolve the whole batch concurrently instead of
    // one round trip at a time. Promise.all preserves `matches`' order (the
    // ORDER BY partition_key ASC from the query above), so the sequential
    // push-until-limit loop below still yields a stable, deterministic
    // result across repeated calls.
    //
    // Milestone 3, Chunk 2: re-routes to the base row's CURRENT shard at
    // read time via the entry's recorded tenant_id (vbucket = hashKey
    // (tenant_id:table:partition_key) % total_vbuckets -> vbucket_map ->
    // shard, computed by CatalogDO's existing /route) — rather than
    // following the entry's static source_shard_id snapshot, which goes
    // stale the instant the base row migrates to a different shard. A
    // /route failure (including an auth mismatch, if tenant_id somehow
    // diverges from the querying tenant) is treated the same as an
    // unreachable shard: skip this match rather than fail the whole query.
    const hydrated = await Promise.all(
      matches.map(async (match): Promise<Record<string, unknown> | null> => {
        const rowCatalogShardId = catalogShardIdForTenant(env, match.tenant_id);
        const rerouteRes = await routeToCatalog(
          env,
          rowCatalogShardId,
          "/route",
          { table: body.table, tenantId: match.tenant_id, partitionKey: match.partition_key },
          request.headers.get("authorization") ?? undefined,
        );
        if (!rerouteRes.ok) return null; // couldn't resolve the base row's current shard — skip
        const rerouteBody = (await rerouteRes.json()) as { shardId: string };

        const rowRes = await routeToShard(env, rerouteBody.shardId, "/execute", {
          sql: `SELECT * FROM ${safeTable} WHERE ${safePkCol} = ?`,
          params: [match.partition_key],
          requestId: `index-query-hydrate-${crypto.randomUUID()}`,
          isMutation: false,
        });
        if (!rowRes.ok) return null; // base row/shard unreachable — skip rather than fail the whole query
        const rowBody = (await rowRes.json()) as { rows?: Array<Record<string, unknown>> };
        const row = rowBody.rows?.[0];
        if (!row) return null; // row deleted since the index entry was written — stale, exclude

        // Staleness re-check (Chunk 2's index maintenance is async): only
        // surface this row if it still actually matches the queried tuple.
        // Excludes a stale entry silently, exactly as if it didn't exist yet
        // — never returns a row that doesn't match what the caller asked for.
        const stillMatches = lookupBody.columns.every((c) => row[c] === queriedValues[c]);
        return stillMatches ? row : null;
      }),
    );
    for (const row of hydrated) {
      if (row === null) continue;
      rows.push(row);
      if (rows.length >= limit) break;
    }

    offset += matches.length;
    if (matches.length < batchLimit) break; // fewer than requested means exhausted
  }
  return json({ rows });
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
  "/v1/sql": handleV1Sql,
  "/v1/mutate": handleV1Mutate,
  "/v1/tx": handleV1Tx,
  "/v1/index-query": handleV1IndexQuery,
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
