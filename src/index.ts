import { CatalogDO } from "./catalog";
import { ShardDO } from "./shard";
import { json } from "./http";
import { hashKey } from "./hash";
import { checkAdminAuth } from "./auth";
import { log } from "./log";

export { CatalogDO, ShardDO };

export interface Env {
  CATALOG: DurableObjectNamespace<CatalogDO>;
  SHARD: DurableObjectNamespace<ShardDO>;
  ADMIN_TOKEN?: string;
  CATALOG_SHARD_COUNT?: string;
}

const DEFAULT_CATALOG_SHARD_COUNT = 4;
const SHARD_FANOUT_CONCURRENCY = 10;

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

function isMutation(sql: string): boolean {
  return /^(\s*)(insert|update|delete|replace|create|drop|alter)/i.test(sql);
}

/** Deny-list: block statements tenants must never be able to run. */
function isDangerous(sql: string): boolean {
  const s = sql.trim().toLowerCase();

  const noTrailingSemicolon = s.replace(/;\s*$/, "");

  // Disallow multi-statement payloads (e.g. "select 1; drop table ...").
  if (noTrailingSemicolon.includes(";")) return true;

  return /\b(drop|truncate|attach|detach|pragma|vacuum|reindex|alter|create)\b/.test(noTrailingSemicolon);
}

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
  const authError = requireAdminAuth(env, request);
  if (authError) return authError;

  const body = (await request.json()) as {
    table?: string;
    schema?: string;
    partitioning?: string;
  };
  if (!body.table || !body.schema) {
    return json({ error: "Missing table or schema." }, 400);
  }
  if (!/^\s*create\s+table\b/i.test(body.schema)) {
    return json({ error: "schema must be a CREATE TABLE statement." }, 400);
  }

  const registerResults = await fanOutToAllCatalogs(
    env,
    "/register-table",
    () => ({ table: body.table, partitioning: body.partitioning }),
    request.headers.get("authorization") ?? undefined,
  );
  const registerFailed = firstCatalogFanOutFailure(registerResults, "Failed to register table.");
  if (registerFailed) return registerFailed;

  const listResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({}));
  const listFailed = firstCatalogFanOutFailure(listResults, "Failed to list shards.");
  if (listFailed) return listFailed;
  const shardIds = listResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);

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

  return json({ ok: true, table: body.table, shardsApplied: shardResults.length });
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

async function handleAdminDrainShard(request: Request, env: Env): Promise<Response> {
  const payload = (await request.json()) as { shardId: string; catalogShardId?: string };
  if (!payload.catalogShardId) {
    return json({ error: "Missing catalogShardId. Shard ownership is scoped to a catalog shard." }, 400);
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

async function handleAdminShardStats(request: Request, env: Env): Promise<Response> {
  const authError = requireAdminAuth(env, request);
  if (authError) return authError;

  const body = (await request.json()) as { shardId: string };
  if (!body.shardId) {
    return json({ error: "Missing shardId" }, 400);
  }
  const res = await routeToShard(env, body.shardId, "/stats", {});
  return new Response(res.body, { status: res.status, headers: res.headers });
}

async function handleV1Sql(request: Request, env: Env): Promise<Response> {
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
  const routeRes = await routeToCatalog(env, catalogShardId, "/route", {
    table: body.table,
    tenantId: body.tenantId,
    partitionKey: body.partitionKey,
  });
  const routeLookupMs = Date.now() - routeStart;

  if (!routeRes.ok) {
    return new Response(routeRes.body, { status: routeRes.status, headers: routeRes.headers });
  }

  const route = (await routeRes.json()) as {
    shardId: string;
    vbucket: number;
    metadataVersion: number;
    catalogShardCount: number | null;
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

  const requestId = body.requestId ?? crypto.randomUUID();
  const shardStart = Date.now();
  const shardRes = await routeToShard(env, route.shardId, "/execute", {
    sql: body.sql,
    params: body.params ?? [],
    requestId,
    isMutation: mutating,
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

async function handleV1Scatter(request: Request, env: Env): Promise<Response> {
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

const ROUTES: Record<string, (request: Request, env: Env) => Promise<Response>> = {
  "/admin/init": handleAdminInit,
  "/admin/register-table": handleAdminRegisterTable,
  "/admin/create-table": handleAdminCreateTable,
  "/admin/split-vbucket": handleAdminSplitVbucket,
  "/admin/status": handleAdminStatus,
  "/admin/list-tables": handleAdminListTables,
  "/admin/drain-shard": handleAdminDrainShard,
  "/admin/shard-stats": handleAdminShardStats,
  "/v1/sql": handleV1Sql,
  "/v1/scatter": handleV1Scatter,
};

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      const url = new URL(request.url);

      if (request.method === "GET" && url.pathname === "/health") {
        return json({ ok: true, service: "cloudflare-shard-mvp" });
      }

      if (request.method !== "POST") {
        return json({ error: "Only POST is supported for this endpoint." }, 405);
      }

      const handler = ROUTES[url.pathname];
      if (handler) {
        return await handler(request, env);
      }

      return json({ error: `Unknown route: ${url.pathname}` }, 404);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log("worker.unhandled_error", { path: new URL(request.url).pathname, message });
      return json({ error: "Internal error." }, 500);
    }
  },
};
