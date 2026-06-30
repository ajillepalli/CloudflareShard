import { CatalogDO } from "./catalog";
import { ShardDO } from "./shard";

export { CatalogDO, ShardDO };

export interface Env {
  CATALOG: DurableObjectNamespace<CatalogDO>;
  SHARD: DurableObjectNamespace<ShardDO>;
}

type SqlRequest = {
  sql: string;
  params?: unknown[];
  table: string;
  tenantId: string;
  partitionKey?: string;
  requestId?: string;
};

const json = (data: unknown, status = 200): Response =>
  new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });

function isMutation(sql: string): boolean {
  return /^(\s*)(insert|update|delete|replace|create|drop|alter)/i.test(sql);
}

/** Deny-list: block statements tenants must never be able to run. */
function isDangerous(sql: string): boolean {
  return /^\s*(drop\s+(table|index|trigger|view)|truncate|attach|detach|pragma|vacuum|reindex)/i.test(
    sql.trim(),
  );
}

function assertParamsArray(params: unknown): params is unknown[] {
  return Array.isArray(params);
}

async function routeToCatalog(env: Env, path: string, payload: unknown): Promise<Response> {
  const id = env.CATALOG.idFromName("cluster-catalog");
  const stub = env.CATALOG.get(id);
  return stub.fetch(`https://catalog.internal${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
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
        const payload = await request.json();
        const res = await routeToCatalog(env, "/init", payload);
        return new Response(res.body, { status: res.status, headers: res.headers });
      }

      if (url.pathname === "/admin/register-table") {
        const payload = await request.json();
        const res = await routeToCatalog(env, "/register-table", payload);
        return new Response(res.body, { status: res.status, headers: res.headers });
      }

      if (url.pathname === "/admin/split-vbucket") {
        const payload = await request.json();
        const res = await routeToCatalog(env, "/split-vbucket", payload);
        return new Response(res.body, { status: res.status, headers: res.headers });
      }

      if (url.pathname === "/admin/status") {
        const res = await routeToCatalog(env, "/status", {});
        return new Response(res.body, { status: res.status, headers: res.headers });
      }

      if (url.pathname === "/admin/list-tables") {
        const res = await routeToCatalog(env, "/list-tables", {});
        return new Response(res.body, { status: res.status, headers: res.headers });
      }

      if (url.pathname === "/admin/drain-shard") {
        const payload = await request.json();
        const res = await routeToCatalog(env, "/drain-shard", payload);
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

        const routeStart = Date.now();
        const routeRes = await routeToCatalog(env, "/route", {
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
        };

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
          route,
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

        const listRes = await routeToCatalog(env, "/list-shards", {});
        if (!listRes.ok) {
          return new Response(listRes.body, {
            status: listRes.status,
            headers: listRes.headers,
          });
        }

        const listPayload = (await listRes.json()) as { shardIds: string[] };
        const scatterStart = Date.now();

        const CONCURRENCY = 10;
        const settled: Array<PromiseSettledResult<{ shardId: string; rows: unknown[] }>> = [];
        for (let i = 0; i < listPayload.shardIds.length; i += CONCURRENCY) {
          const batch = listPayload.shardIds.slice(i, i + CONCURRENCY);
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
              shardId: listPayload.shardIds[i],
              reason: result.reason instanceof Error ? result.reason.message : String(result.reason),
            });
          }
        }

        const merged = outputs.flatMap((x) => x.rows);
        const capped = typeof body.limit === "number" ? merged.slice(0, body.limit) : merged;

        return json({
          observability: {
            scatterMs,
            shardCount: listPayload.shardIds.length,
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
      return json(
        {
          error: "Unhandled worker error",
          details: error instanceof Error ? error.message : String(error),
        },
        500,
      );
    }
  },
};
