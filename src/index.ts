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

        const routeRes = await routeToCatalog(env, "/route", {
          table: body.table,
          tenantId: body.tenantId,
          partitionKey: body.partitionKey,
        });

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
        const shardRes = await routeToShard(env, route.shardId, "/execute", {
          sql: body.sql,
          params: body.params ?? [],
          requestId,
          isMutation: mutating,
        });

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

        const listRes = await routeToCatalog(env, "/list-shards", {});
        if (!listRes.ok) {
          return new Response(listRes.body, {
            status: listRes.status,
            headers: listRes.headers,
          });
        }

        const listPayload = (await listRes.json()) as { shardIds: string[] };
        const outputs: Array<{ shardId: string; rows: unknown[] }> = [];

        for (const shardId of listPayload.shardIds) {
          const shardRes = await routeToShard(env, shardId, "/execute", {
            sql: body.sql,
            params: body.params ?? [],
            requestId: crypto.randomUUID(),
            isMutation: false,
          });

          if (!shardRes.ok) {
            continue;
          }

          const shardPayload = (await shardRes.json()) as { rows?: unknown[] };
          outputs.push({ shardId, rows: shardPayload.rows ?? [] });
        }

        const merged = outputs.flatMap((x) => x.rows);
        const capped = typeof body.limit === "number" ? merged.slice(0, body.limit) : merged;

        return json({ shardCount: outputs.length, rows: capped, perShard: outputs });
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
