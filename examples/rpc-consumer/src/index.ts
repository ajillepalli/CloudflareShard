/** Minimal demo Worker showing CloudflareShard's RPC/service-binding surface
 * (issue #14) in use from another Cloudflare Worker in the same account. A
 * real consumer wouldn't have access to CloudflareShardRpc's internal
 * TypeScript types (a separate npm package/repo, same as this one) — so this
 * interface is the documented RPC contract, hand-mirrored, not an import. */
export interface ShardApiBinding {
  mutate(
    tenantToken: string,
    body: {
      table: string;
      tenantId: string;
      partitionKey: string;
      op: "insert" | "update" | "delete" | "upsert";
      values?: Record<string, unknown>;
      where?: Record<string, unknown>;
      requestId?: string;
    },
  ): Promise<{ ok: true; rowsAffected: number }>;
  tableScan(
    tenantToken: string,
    body: { tenantId: string; table: string; limit?: number; cursor?: string | null },
  ): Promise<{
    rows: Array<Record<string, unknown>>;
    nextCursor?: string;
    provenance: { complete: boolean; fix?: string };
    scan: { catalogShardId: string; shardCount: number; successCount: number; scanMs: number };
  }>;
  indexQuery(
    tenantToken: string,
    body: { table: string; indexName: string; tenantId: string; values: Record<string, unknown>; limit?: number },
  ): Promise<{ rows: Array<Record<string, unknown>> }>;
}

export interface Env {
  SHARD_API: ShardApiBinding;
}

const json = (data: unknown, status = 200): Response =>
  new Response(JSON.stringify(data, null, 2), { status, headers: { "content-type": "application/json" } });

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/health") {
      return json({ ok: true, service: "cloudflare-shard-rpc-consumer" });
    }

    // POST /demo/write-and-scan: writes one row via RPC mutate(), then reads
    // it back via RPC tableScan() — the round trip a real consumer cares
    // about, with no HTTP request or Authorization header built anywhere in
    // this Worker's own code.
    if (request.method === "POST" && url.pathname === "/demo/write-and-scan") {
      const body = (await request.json()) as {
        tenantToken: string;
        tenantId: string;
        table: string;
        partitionKey: string;
        note?: string;
      };
      const mutateResult = await env.SHARD_API.mutate(body.tenantToken, {
        table: body.table,
        tenantId: body.tenantId,
        partitionKey: body.partitionKey,
        op: "insert",
        values: { id: body.partitionKey, note: body.note ?? "written via Durable Object RPC, not HTTP" },
      });
      const scanResult = await env.SHARD_API.tableScan(body.tenantToken, {
        tenantId: body.tenantId,
        table: body.table,
        limit: 10,
      });
      return json({ mutateResult, scanResult });
    }

    // POST /demo/index-query: exact-tuple lookup via RPC indexQuery(). Needs
    // an index already registered on `table` for `column` (via the existing
    // HTTP /admin/create-index — admin operations aren't in this issue's
    // RPC scope, see the follow-up issue).
    if (request.method === "POST" && url.pathname === "/demo/index-query") {
      const body = (await request.json()) as {
        tenantToken: string;
        tenantId: string;
        table: string;
        indexName: string;
        column: string;
        value: unknown;
      };
      const indexQueryResult = await env.SHARD_API.indexQuery(body.tenantToken, {
        table: body.table,
        indexName: body.indexName,
        tenantId: body.tenantId,
        values: { [body.column]: body.value },
      });
      return json({ indexQueryResult });
    }

    return json({ error: `Unknown route: ${url.pathname}` }, 404);
  },
};
