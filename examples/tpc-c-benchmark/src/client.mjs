// Minimal HTTP client for CloudflareShard's existing HTTP API (no RPC/service
// binding dependency -- see issue #16 non-goals). Uses Node 20+'s built-in
// fetch, no external HTTP dependency.

export class ApiError extends Error {
  constructor(method, path, status, body) {
    const code = body && body.error && body.error.code;
    const msg = body && body.error && body.error.message;
    super(`${method} ${path} -> ${status}${code ? ` ${code}` : ""}${msg ? `: ${msg}` : ""}`);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.body = body;
  }
}

async function post(baseUrl, token, path, body) {
  const res = await fetch(`${baseUrl}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body ?? {}),
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  if (!res.ok) {
    throw new ApiError("POST", path, res.status, json);
  }
  return json;
}

/** Thin wrapper bound to a base URL + admin token, for cluster/topology setup. */
export class AdminClient {
  constructor(baseUrl, adminToken) {
    this.baseUrl = baseUrl;
    this.adminToken = adminToken;
  }

  init(numShards, totalVBuckets) {
    return post(this.baseUrl, this.adminToken, "/admin/init", { numShards, totalVBuckets });
  }

  createTable(table, schema, partitionKeyColumn) {
    return post(this.baseUrl, this.adminToken, "/admin/create-table", { table, schema, partitionKeyColumn });
  }

  createIndex(indexName, table, columns) {
    return post(this.baseUrl, this.adminToken, "/admin/create-index", { indexName, table, columns });
  }

  registerTenant(tenantId, { rotate } = {}) {
    return post(this.baseUrl, this.adminToken, "/admin/register-tenant", { tenantId, ...(rotate ? { rotate: true } : {}) });
  }

  status() {
    return post(this.baseUrl, this.adminToken, "/admin/status", {});
  }
}

/** Thin wrapper bound to a base URL + one tenant's bearer token, for the
 * tenant data-plane routes (/v1/mutate, /v1/tx, /v1/index-query,
 * /v1/table-scan). One instance per warehouse tenant. */
export class TenantClient {
  constructor(baseUrl, token, tenantId) {
    this.baseUrl = baseUrl;
    this.token = token;
    this.tenantId = tenantId;
  }

  mutate({ op, table, partitionKey, values, where, requestId }) {
    return post(this.baseUrl, this.token, "/v1/mutate", {
      op,
      table,
      tenantId: this.tenantId,
      partitionKey,
      values,
      where,
      requestId: requestId ?? crypto.randomUUID(),
    });
  }

  tx(mutations, requestId) {
    // Every mutation in a /v1/tx call must share the same tenantId as this
    // client -- enforced by construction here rather than trusted per-call.
    const stamped = mutations.map((m) => ({ ...m, tenantId: this.tenantId }));
    return post(this.baseUrl, this.token, "/v1/tx", {
      mutations: stamped,
      requestId: requestId ?? crypto.randomUUID(),
    });
  }

  indexQuery(table, indexName, values, limit) {
    return post(this.baseUrl, this.token, "/v1/index-query", {
      table,
      indexName,
      tenantId: this.tenantId,
      values,
      limit,
    });
  }

  async tableScan(table, limit, cursor) {
    return post(this.baseUrl, this.token, "/v1/table-scan", {
      tenantId: this.tenantId,
      table,
      limit,
      cursor,
    });
  }

  /** Follow nextCursor until the scan is exhausted. Used where the benchmark
   * genuinely needs every row in a table (Stock-Level) rather than a single
   * bounded page. */
  async tableScanAll(table, pageLimit) {
    let rows = [];
    let cursor;
    for (;;) {
      const page = await this.tableScan(table, pageLimit, cursor);
      rows = rows.concat(page.rows ?? []);
      if (!page.nextCursor) break;
      cursor = page.nextCursor;
    }
    return rows;
  }
}

/** Bounded-concurrency worker pool: runs `fn(item, index)` for every item in
 * `items`, at most `limit` in flight at once. Used by the seeder so a few
 * hundred/thousand inserts don't run fully sequentially (slow) or fully
 * unbounded (too many concurrent connections to a local wrangler dev / a
 * live Worker). */
export async function runPool(items, limit, fn) {
  const results = new Array(items.length);
  let next = 0;
  async function worker() {
    for (;;) {
      const i = next++;
      if (i >= items.length) return;
      results[i] = await fn(items[i], i);
    }
  }
  const workers = Array.from({ length: Math.min(limit, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}
