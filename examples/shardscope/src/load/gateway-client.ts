/** gateway-client.ts — the real HTTP implementation of ./transactions.ts's
 * TxExecutor, playing the same role examples/tpc-c-benchmark/src/client.mjs's
 * TenantClient does for the Node reference harness: turns a
 * (warehouseId, call) pair into an authenticated POST against
 * CloudflareShard's tenant data-plane routes (/v1/mutate, /v1/tx,
 * /v1/index-query, /v1/table-scan).
 *
 * Every call resolves its bearer token via an injected TokenProvider (see
 * ./token-provider.ts) — with today's stub TokenProvider, that means every
 * method here throws the same "pending T5" error the moment it's invoked,
 * before any fetch() ever happens. That's expected: this file's job is to be
 * fully correct WIRING for the day a real TokenProvider exists, not to work
 * today. ./load-driver.ts constructs one of these per load-driver instance
 * and passes it to ./transactions.ts's runOneTransaction() as the executor.
 */
import { tenantIdForWarehouse, type MutateCall, type MutateResult, type QueryResult, type TxExecutor } from "./transactions";
import type { TokenProvider } from "./token-provider";

export class GatewayError extends Error {
  readonly status: number;
  readonly code?: string;

  constructor(method: string, path: string, status: number, body: unknown) {
    const errObj = body && typeof body === "object" ? (body as { error?: { code?: string; message?: string } }).error : undefined;
    const code = errObj?.code;
    const msg = errObj?.message;
    super(`${method} ${path} -> ${status}${code ? ` ${code}` : ""}${msg ? `: ${msg}` : ""}`);
    this.name = "GatewayError";
    this.status = status;
    this.code = code;
  }
}

/** HTTP implementation of TxExecutor against a live (or wrangler-dev)
 * CloudflareShard gateway base URL. Mirrors client.mjs's TenantClient +
 * post() helper, just generalized to take warehouseId per-call instead of
 * being bound to one warehouse at construction time (transactions.ts's
 * New-Order needs to address BOTH the ordering warehouse's tenant and a
 * remote supply warehouse's tenant within one transaction — see
 * transactions.ts's processOrderLine). */
export class HttpTxExecutor implements TxExecutor {
  constructor(
    private readonly baseUrl: string,
    private readonly tokenProvider: TokenProvider,
  ) {}

  async mutate(warehouseId: number, call: MutateCall): Promise<MutateResult> {
    const tenantId = tenantIdForWarehouse(warehouseId);
    return this.post(warehouseId, "/v1/mutate", {
      ...call,
      tenantId,
      requestId: call.requestId ?? crypto.randomUUID(),
    }) as Promise<MutateResult>;
  }

  async tx(warehouseId: number, mutations: MutateCall[], requestId?: string): Promise<{ committed?: boolean; [k: string]: unknown }> {
    const tenantId = tenantIdForWarehouse(warehouseId);
    // Every mutation in a /v1/tx call must share the same tenantId as the
    // warehouse this call is addressed to — stamped here by construction,
    // same as client.mjs's tx() does, rather than trusted per-mutation.
    const stamped = mutations.map((m) => ({ ...m, tenantId }));
    return this.post(warehouseId, "/v1/tx", { mutations: stamped, requestId: requestId ?? crypto.randomUUID() }) as Promise<{
      committed?: boolean;
      [k: string]: unknown;
    }>;
  }

  async indexQuery(warehouseId: number, table: string, indexName: string, values: Record<string, unknown>, limit?: number): Promise<QueryResult> {
    const tenantId = tenantIdForWarehouse(warehouseId);
    return this.post(warehouseId, "/v1/index-query", { table, indexName, tenantId, values, limit }) as Promise<QueryResult>;
  }

  async tableScan(warehouseId: number, table: string, limit: number, cursor?: unknown): Promise<QueryResult> {
    const tenantId = tenantIdForWarehouse(warehouseId);
    return this.post(warehouseId, "/v1/table-scan", { tenantId, table, limit, cursor }) as Promise<QueryResult>;
  }

  private async post(warehouseId: number, path: string, body: Record<string, unknown>): Promise<unknown> {
    // Resolving the token is the FIRST thing every call does — with today's
    // stub TokenProvider this throws immediately, before any network I/O,
    // which is exactly the documented "pending T5" behavior (see this
    // file's header comment).
    const token = await this.tokenProvider.getTenantToken(warehouseId);
    const res = await fetch(`${this.baseUrl}${path}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    let json: unknown;
    try {
      json = text ? JSON.parse(text) : {};
    } catch {
      json = { raw: text };
    }
    if (!res.ok) {
      throw new GatewayError("POST", path, res.status, json);
    }
    return json;
  }
}
