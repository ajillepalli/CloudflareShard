import { CatalogDO } from "./catalog";
import { ShardDO } from "./shard";
import { json } from "./http";
import { hashKey } from "./hash";
import { isValidBearerToken } from "./auth";
import { log } from "./log";

export { CatalogDO, ShardDO };

export interface Env {
  CATALOG: DurableObjectNamespace<CatalogDO>;
  SHARD: DurableObjectNamespace<ShardDO>;
  ADMIN_TOKEN?: string;
  CATALOG_SHARD_COUNT?: string;
}

const DEFAULT_CATALOG_SHARD_COUNT = 4;

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
  if (!env.ADMIN_TOKEN) {
    return json({ error: "ADMIN_TOKEN is not configured." }, 500);
  }
  if (!isValidBearerToken(request.headers.get("authorization"), env.ADMIN_TOKEN)) {
    return json({ error: "Unauthorized." }, 401);
  }
  return null;
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

async function fanOutToAllCatalogs(
  env: Env,
  path: string,
  payloadFor: (catalogShardId: string) => unknown,
  authorization?: string,
): Promise<Array<{ catalogShardId: string; res: Response; body: unknown }>> {
  const catalogShardIds = allCatalogShardIds(env);
  return Promise.all(
    catalogShardIds.map(async (catalogShardId) => {
      const res = await routeToCatalog(env, catalogShardId, path, payloadFor(catalogShardId), authorization);
      const body = await res.json();
      return { catalogShardId, res, body };
    }),
  );
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

      if (url.pathname === "/admin/init") {
        const payload = (await request.json()) as Record<string, unknown>;
        const authorization = request.headers.get("authorization") ?? undefined;
        const results = await fanOutToAllCatalogs(
          env,
          "/init",
          (catalogShardId) => ({ ...payload, catalogShardId, catalogShardCount: catalogShardCount(env) }),
          authorization,
        );
        const failed = results.find((r) => !r.res.ok);
        if (failed) {
          return json({ error: "One or more catalog shards failed to initialize.", catalogShardId: failed.catalogShardId, details: failed.body }, failed.res.status);
        }
        return json({
          ok: true,
          catalogShardCount: results.length,
          catalogs: results.map((r) => ({ catalogShardId: r.catalogShardId, ...(r.body as object) })),
        });
      }

      if (url.pathname === "/admin/register-table") {
        const payload = (await request.json()) as Record<string, unknown>;
        const authorization = request.headers.get("authorization") ?? undefined;
        const results = await fanOutToAllCatalogs(env, "/register-table", () => payload, authorization);
        const failed = results.find((r) => !r.res.ok);
        if (failed) {
          return json({ error: "One or more catalog shards failed to register the table.", catalogShardId: failed.catalogShardId, details: failed.body }, failed.res.status);
        }
        return json({ ok: true, catalogShardCount: results.length });
      }

      if (url.pathname === "/admin/create-table") {
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
        const registerFailed = registerResults.find((r) => !r.res.ok);
        if (registerFailed) {
          return json(
            { error: "Failed to register table.", catalogShardId: registerFailed.catalogShardId, details: registerFailed.body },
            registerFailed.res.status,
          );
        }

        const listResults = await fanOutToAllCatalogs(env, "/list-shards", () => ({}));
        const listFailed = listResults.find((r) => !r.res.ok);
        if (listFailed) {
          return json(
            { error: "Failed to list shards.", catalogShardId: listFailed.catalogShardId, details: listFailed.body },
            listFailed.res.status,
          );
        }
        const shardIds = listResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);

        const shardResults = await Promise.all(
          shardIds.map(async (shardId) => {
            const res = await routeToShard(env, shardId, "/execute", {
              sql: body.schema,
              requestId: `create-table-${body.table}-${shardId}`,
              isMutation: true,
            });
            return { shardId, ok: res.ok, status: res.status, body: await res.json() };
          }),
        );
        const shardFailed = shardResults.find((r) => !r.ok);
        if (shardFailed) {
          return json(
            { error: "Failed to create table on one or more shards.", shardId: shardFailed.shardId, details: shardFailed.body },
            shardFailed.status,
          );
        }

        return json({ ok: true, table: body.table, shardsApplied: shardResults.length });
      }

      if (url.pathname === "/admin/split-vbucket") {
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

      if (url.pathname === "/admin/status") {
        const authorization = request.headers.get("authorization") ?? undefined;
        const results = await fanOutToAllCatalogs(env, "/status", () => ({}), authorization);
        const failed = results.find((r) => !r.res.ok);
        if (failed) {
          return json({ error: "One or more catalog shards failed to report status.", catalogShardId: failed.catalogShardId, details: failed.body }, failed.res.status);
        }
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

      if (url.pathname === "/admin/list-tables") {
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

      if (url.pathname === "/admin/drain-shard") {
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

      if (url.pathname === "/admin/shard-stats") {
        const body = (await request.json()) as { shardId: string };
        if (!body.shardId) {
          return json({ error: "Missing shardId" }, 400);
        }
        const res = await routeToShard(env, body.shardId, "/stats", {});
        return new Response(res.body, { status: res.status, headers: res.headers });
      }

      if (url.pathname === "/v1/sql") {
        const body = (await request.json()) as SqlRequest;
        if (!body.sql || !body.table || !body.tenantId) {
          return json(
            {
              error: "Missing required fields: sql, table, tenantId.",
            },
            400,
          );
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
              {
                error:
                  "Mutating SQL requires partitionKey for deterministic single-shard routing.",
              },
              400,
            );
          }

          return json(
            {
              error:
                "SELECT without partitionKey is not allowed on /v1/sql. Use /v1/scatter for fan-out reads.",
            },
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
          return new Response(routeRes.body, {
            status: routeRes.status,
            headers: routeRes.headers,
          });
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
          return new Response(shardRes.body, {
            status: shardRes.status,
            headers: shardRes.headers,
          });
        }

        const shardPayload = await shardRes.json();
        return json({
          route: { ...route, catalogShardId },
          requestId,
          observability: {
            routeLookupMs,
            shardExecuteMs,
            metadataVersion: route.metadataVersion,
          },
          result: shardPayload,
        });
      }

      if (url.pathname === "/v1/scatter") {
        const body = (await request.json()) as {
          sql: string;
          params?: unknown[];
          limit?: number;
        };

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
        const failedList = listResults.find((r) => !r.res.ok);
        if (failedList) {
          return json(
            { error: "Failed to list shards from one or more catalog shards.", catalogShardId: failedList.catalogShardId, details: failedList.body },
            failedList.res.status,
          );
        }
        const shardIds = listResults.flatMap((r) => (r.body as { shardIds: string[] }).shardIds);

        const scatterStart = Date.now();
        const CONCURRENCY = 10;
        const settled: Array<PromiseSettledResult<{ shardId: string; rows: unknown[] }>> = [];

        for (let i = 0; i < shardIds.length; i += CONCURRENCY) {
          const batch = shardIds.slice(i, i + CONCURRENCY);
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
          observability: {
            scatterMs,
            shardCount: shardIds.length,
            successCount: outputs.length,
            errorCount: errors.length,
          },
          rows: capped,
          perShard: outputs,
          ...(errors.length > 0 ? { errors } : {}),
        });
      }

      return json({ error: `Unknown route: ${url.pathname}` }, 404);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      log("worker.unhandled_error", { path: new URL(request.url).pathname, message });
      return json(
        {
          error: "Unhandled worker error",
          details: message,
        },
        500,
      );
    }
  },
};
