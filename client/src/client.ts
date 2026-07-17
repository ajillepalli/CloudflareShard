import { HttpClient, type ClientOptions } from "./http.js";
import type {
  IndexQueryRequest,
  IndexQueryResponse,
  MutateOp,
  MutateRequest,
  MutateResponse,
  TableScanRequest,
  TableScanResponse,
  TxRequest,
  TxResponse,
} from "./types.js";

/** Tenant data-plane client: /v1/mutate, /v1/tx, /v1/index-query,
 * /v1/table-scan. Construct with a tenant bearer token (from
 * CloudflareShardAdminClient.registerTenant), not ADMIN_TOKEN -- these
 * routes reject an admin token with 401, the same way the raw HTTP API
 * does (see README's "Tenant authorization" section). */
export class CloudflareShardClient extends HttpClient {
  constructor(options: ClientOptions) {
    super(options);
  }

  /** Low-level escape hatch matching /v1/mutate's raw shape exactly.
   * Prefer insert/update/delete/upsert below for a more typed call site;
   * this exists for callers building `op` dynamically. requestId is
   * generated client-side if omitted, matching the server's own default
   * (see src/index.ts: `body.requestId ?? crypto.randomUUID()`) -- supplying
   * your own is only necessary when YOU need to retry the same logical
   * write with the same idempotency key. */
  async mutate(request: MutateRequest): Promise<MutateResponse> {
    return this.post<MutateResponse>("/v1/mutate", {
      ...request,
      requestId: request.requestId ?? crypto.randomUUID(),
    });
  }

  async insert(
    table: string,
    tenantId: string,
    partitionKey: string,
    values: Record<string, unknown>,
    requestId?: string,
  ): Promise<MutateResponse> {
    return this.mutate({ op: "insert", table, tenantId, partitionKey, values, requestId });
  }

  async update(
    table: string,
    tenantId: string,
    partitionKey: string,
    values: Record<string, unknown>,
    where?: Record<string, unknown>,
    requestId?: string,
  ): Promise<MutateResponse> {
    return this.mutate({ op: "update", table, tenantId, partitionKey, values, where, requestId });
  }

  async delete(
    table: string,
    tenantId: string,
    partitionKey: string,
    where?: Record<string, unknown>,
    requestId?: string,
  ): Promise<MutateResponse> {
    return this.mutate({ op: "delete", table, tenantId, partitionKey, where, requestId });
  }

  async upsert(
    table: string,
    tenantId: string,
    partitionKey: string,
    values: Record<string, unknown>,
    requestId?: string,
  ): Promise<MutateResponse> {
    return this.mutate({ op: "upsert", table, tenantId, partitionKey, values, requestId });
  }

  /** Atomically commits a batch of mutations, possibly spanning multiple
   * shards, via CoordinatorDO's two-phase commit. Every mutation must share
   * the same tenantId; capped at 8 distinct (tenantId, table, partitionKey)
   * rows (the coordinator's own participant cap). Unlike mutate(), the
   * server REQUIRES requestId (400 MISSING_REQUEST_ID) since it's the whole
   * transaction's idempotency key, not a per-write convenience default --
   * still filled in client-side if omitted, so you only need to pass your
   * own when you specifically need retry-safe resubmission. */
  async tx(mutations: MutateRequest[], requestId?: string): Promise<TxResponse> {
    const request: TxRequest = { mutations, requestId: requestId ?? crypto.randomUUID() };
    return this.post<TxResponse>("/v1/tx", request);
  }

  /** Exact full-tuple secondary-index lookup -- a value for every column
   * the index covers (leftmost-prefix lookups aren't supported). Throws
   * CloudflareShardError with status 425 (code INDEX_BUILDING) if the
   * index hasn't finished its initial backfill yet. */
  async indexQuery(request: IndexQueryRequest): Promise<IndexQueryResponse> {
    return this.post<IndexQueryResponse>("/v1/index-query", request);
  }

  /** One page of a tenant's own rows in a table, cursor-paginated, no
   * arbitrary filters. Use tableScanAll() below to page through everything
   * automatically. */
  async tableScan(request: TableScanRequest): Promise<TableScanResponse> {
    return this.post<TableScanResponse>("/v1/table-scan", request);
  }

  /** Pages through every one of a tenant's rows in a table automatically,
   * following nextCursor until the response omits it. Yields one page's
   * `rows` array at a time (not one row at a time) so callers can still see
   * per-page `provenance`/`scan` metadata if they want it via a manual
   * tableScan() loop instead. */
  async *tableScanAll(request: Omit<TableScanRequest, "cursor">): AsyncGenerator<Record<string, unknown>[], void, void> {
    let cursor: string | undefined;
    for (;;) {
      const page = await this.tableScan({ ...request, cursor });
      yield page.rows;
      if (!page.nextCursor) return;
      cursor = page.nextCursor;
    }
  }
}

export type { MutateOp };
