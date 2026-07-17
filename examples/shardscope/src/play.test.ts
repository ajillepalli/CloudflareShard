/** play.test.ts — Shardscope Playground room backend tests.
 *
 * Two layers, mirroring this repo's existing split (reshard.test.ts exercises
 * reshard.ts's parse*()/call() functions directly against a fakeEnv();
 * chaos.test.ts does the same for chaos.ts's pure classifiers):
 *
 *   1. Unit tests against src/play.ts's parse*()/play*() functions directly,
 *      with a hand-built fakeEnv() (mocked SHARD_API + a minimal
 *      TENANT_TOKEN_STORE DO-namespace stub) — covers whitelist validation,
 *      correct token/tenantId/table plumbing, requestId defaulting/echoing,
 *      and read-only SQL enforcement.
 *
 *   2. Route-level tests calling src/index.ts's default export's fetch()
 *      DIRECTLY (no cloudflare:test/SELF): this repo's only vitest.config.ts
 *      wires @cloudflare/vitest-pool-workers against the ROOT wrangler.toml
 *      (the cloudflare-shard-mvp core Worker) — see
 *      src/load/reshard.integration.test.ts's header comment, which confirms
 *      this explicitly ("there is only one vitest config, no
 *      poolMatchGlobs"). `cloudflare:test`'s SELF binding therefore resolves
 *      to the CORE Worker, not this Shardscope Worker — there is no existing
 *      SELF.fetch harness that targets Shardscope's OWN src/index.ts (no
 *      test file in this package does that; grepped before writing this).
 *      Calling the exported `{ fetch(request, env) }` object directly is the
 *      correct in-process equivalent: identical routing, identical
 *      isGateAuthorized gate, identical JSON responses — the whole test
 *      still runs inside the real workerd/Miniflare pool-workers runtime
 *      (every test file in this monorepo does, per the one shared vitest
 *      config), so real Fetch API / crypto.randomUUID() semantics apply; only
 *      the Env bindings are hand-provided fakes, exactly like
 *      reshard.test.ts's own fakeEnv().
 */
import { describe, expect, it, vi } from "vitest";
import worker from "./index";
import {
  PlayValidationError,
  PLAYGROUND_TABLES,
  PLAYGROUND_WAREHOUSE_IDS,
  extractScatterFromTable,
  parsePlayIndexQueryInput,
  parsePlayMutateInput,
  parsePlayScatterInput,
  parsePlaySqlInput,
  parsePlayTableScanInput,
  parsePlayTxInput,
  playIndexQuery,
  playMutate,
  playScatter,
  playSql,
  playTableScan,
  playTx,
} from "./play";
import type { Env } from "./env";

// ----------------------------------------------------------------------------
// fakeEnv — same shape/spirit as reshard.test.ts's fakeEnv(): only what
// play.ts and index.ts's routing actually touch needs a real implementation.
// TENANT_TOKEN_STORE is a minimal DO-namespace stub whose /get-or-create
// fetch() deterministically derives a token from the requested warehouseId,
// so tests can assert the RIGHT tenant token/tenantId reached SHARD_API
// without needing a real Durable Object.
// ----------------------------------------------------------------------------

type MockShardApi = { [K in keyof Env["SHARD_API"]]: ReturnType<typeof vi.fn> };

function fakeTenantTokenStoreNamespace(): Env["TENANT_TOKEN_STORE"] {
  return {
    idFromName: () => ({}) as unknown as DurableObjectId,
    get: () =>
      ({
        fetch: async (_input: RequestInfo | URL, init?: RequestInit) => {
          const body = JSON.parse((init?.body as string) ?? "{}") as { warehouseId?: number };
          return new Response(JSON.stringify({ token: `demo-tenant-token-w${body.warehouseId}` }), {
            status: 200,
            headers: { "content-type": "application/json" },
          });
        },
      }) as unknown as DurableObjectStub,
  } as unknown as Env["TENANT_TOKEN_STORE"];
}

function fakeEnv(): Env & { SHARD_API: MockShardApi } {
  return {
    ADMIN_TOKEN: "test-admin-token",
    SHARDSCOPE_GATE_TOKEN: "test-gate-token",
    TENANT_TOKEN_STORE: fakeTenantTokenStoreNamespace(),
    SHARD_API: {
      adminStatus: vi.fn(),
      adminVbucketMap: vi.fn(),
      adminShardStats: vi.fn(),
      adminRegisterTenant: vi.fn(),
      adminSplitVbucket: vi.fn(),
      adminMigrateVbucket: vi.fn(),
      adminMigrateVbucketStatus: vi.fn(),
      adminMigrateVbucketAbort: vi.fn(),
      adminDrainShard: vi.fn(),
      adminDrainShardStatus: vi.fn(),
      adminTopologyLockStatus: vi.fn(),
      adminForceReleaseTopologyLock: vi.fn(),
      adminFaultInject: vi.fn(),
      adminFaultClear: vi.fn(),
      mutate: vi.fn(async () => ({ ok: true, rowsAffected: 1 })),
      tx: vi.fn(async () => ({ ok: true, committed: true })),
      indexQuery: vi.fn(async () => ({ rows: [{ s_key: "s-0001-000001", s_quantity: 42 }] })),
      tableScan: vi.fn(async () => ({ rows: [{ w_id: 1 }] })),
      sql: vi.fn(async () => ({ result: { rows: [{ s_quantity: 42 }] } })),
      scatter: vi.fn(async () => ({ rows: [{ w_id: 1 }] })),
    },
  } as unknown as Env & { SHARD_API: MockShardApi };
}

// ============================================================================
// Layer 1: parse*() whitelist validation
// ============================================================================

describe("play.ts — input whitelist validation", () => {
  it("rejects a warehouseId outside PLAYGROUND_WAREHOUSE_IDS", () => {
    expect(() => parsePlayMutateInput({ warehouseId: 999, op: "update", table: "tpcc_stock", partitionKey: "s-0001-000001" })).toThrow(
      PlayValidationError,
    );
  });

  it("rejects a table outside PLAYGROUND_TABLES", () => {
    expect(() =>
      parsePlayMutateInput({ warehouseId: 1, op: "update", table: "applied_requests", partitionKey: "x" }),
    ).toThrow(PlayValidationError);
  });

  it("rejects a table outside PLAYGROUND_TABLES for table-scan too", () => {
    expect(() => parsePlayTableScanInput({ warehouseId: 1, table: "__cf_row_owners" })).toThrow(PlayValidationError);
  });

  it("rejects an indexName not registered for the given table", () => {
    expect(() =>
      parsePlayIndexQueryInput({ warehouseId: 1, table: "tpcc_stock", indexName: "idx_customer_by_id", values: { i_id: 1 } }),
    ).toThrow(PlayValidationError);
  });

  it("accepts a valid indexName for its table", () => {
    const input = parsePlayIndexQueryInput({ warehouseId: 1, table: "tpcc_stock", indexName: "idx_stock_by_item", values: { i_id: 1 } });
    expect(input.indexName).toBe("idx_stock_by_item");
  });

  it("rejects a non-object body", () => {
    expect(() => parsePlayMutateInput(null)).toThrow(PlayValidationError);
    expect(() => parsePlayMutateInput("not-an-object")).toThrow(PlayValidationError);
  });

  it("rejects an invalid op", () => {
    expect(() => parsePlayMutateInput({ warehouseId: 1, op: "select", table: "tpcc_stock", partitionKey: "x" })).toThrow(PlayValidationError);
  });

  it("bounds tx mutations to MAX_TX_MUTATIONS", () => {
    const tooMany = Array.from({ length: 11 }, () => ({ op: "update", table: "tpcc_stock", partitionKey: "s-0001-000001" }));
    expect(() => parsePlayTxInput({ warehouseId: 1, mutations: tooMany })).toThrow(PlayValidationError);
  });

  it("rejects an empty mutations array for tx", () => {
    expect(() => parsePlayTxInput({ warehouseId: 1, mutations: [] })).toThrow(PlayValidationError);
  });

  it("bounds params array length for sql", () => {
    const tooManyParams = Array.from({ length: 21 }, (_, i) => i);
    expect(() =>
      parsePlaySqlInput({ warehouseId: 1, table: "tpcc_stock", partitionKey: "s-0001-000001", sql: "SELECT * FROM tpcc_stock", params: tooManyParams }),
    ).toThrow(PlayValidationError);
  });

  it("PLAYGROUND_WAREHOUSE_IDS and PLAYGROUND_TABLES are non-empty (sanity)", () => {
    expect(PLAYGROUND_WAREHOUSE_IDS.length).toBeGreaterThan(0);
    expect(PLAYGROUND_TABLES.length).toBeGreaterThan(0);
  });
});

// ============================================================================
// Layer 1: operator SQL read-only enforcement
// ============================================================================

describe("play.ts — playSql read-only enforcement", () => {
  const base = { warehouseId: 1 as const, table: "tpcc_stock" as const, partitionKey: "s-0001-000001" };

  it("allows a SELECT", () => {
    const input = parsePlaySqlInput({ ...base, sql: "SELECT * FROM tpcc_stock WHERE s_key = ?", params: ["s-0001-000001"] });
    expect(input.sql).toContain("SELECT");
  });

  it("allows EXPLAIN", () => {
    const input = parsePlaySqlInput({ ...base, sql: "EXPLAIN QUERY PLAN SELECT * FROM tpcc_stock" });
    expect(input.sql).toContain("EXPLAIN");
  });

  it("rejects an UPDATE", () => {
    expect(() => parsePlaySqlInput({ ...base, sql: "UPDATE tpcc_stock SET s_quantity = 0" })).toThrow(PlayValidationError);
  });

  it("rejects an INSERT", () => {
    expect(() => parsePlaySqlInput({ ...base, sql: "INSERT INTO tpcc_stock (s_key) VALUES ('x')" })).toThrow(PlayValidationError);
  });

  it("rejects a DELETE", () => {
    expect(() => parsePlaySqlInput({ ...base, sql: "DELETE FROM tpcc_stock" })).toThrow(PlayValidationError);
  });

  it("rejects DROP TABLE", () => {
    expect(() => parsePlaySqlInput({ ...base, sql: "DROP TABLE tpcc_stock" })).toThrow(PlayValidationError);
  });

  it("rejects CREATE TABLE", () => {
    expect(() => parsePlaySqlInput({ ...base, sql: "CREATE TABLE evil (x TEXT)" })).toThrow(PlayValidationError);
  });

  it("rejects ALTER TABLE", () => {
    expect(() => parsePlaySqlInput({ ...base, sql: "ALTER TABLE tpcc_stock ADD COLUMN x TEXT" })).toThrow(PlayValidationError);
  });

  it("rejects a comment-obfuscated DELETE (reuses core's own comment-stripping classifier)", () => {
    expect(() => parsePlaySqlInput({ ...base, sql: "-- innocuous\nDELETE FROM tpcc_stock" })).toThrow(PlayValidationError);
  });

  it("rejects a multi-statement payload", () => {
    expect(() => parsePlaySqlInput({ ...base, sql: "SELECT 1; DROP TABLE tpcc_stock" })).toThrow(PlayValidationError);
  });

  it("rejects PRAGMA (conservative: core's own isDangerous blocks all PRAGMA, not just writes)", () => {
    expect(() => parsePlaySqlInput({ ...base, sql: "PRAGMA table_info(tpcc_stock)" })).toThrow(PlayValidationError);
  });
});

// ============================================================================
// Layer 1: scatter's single-table/no-JOIN scope + read-only enforcement
// ============================================================================

describe("play.ts — extractScatterFromTable / playScatter scoping", () => {
  it("extracts a simple single-table FROM target", () => {
    expect(extractScatterFromTable("SELECT * FROM tpcc_stock WHERE s_quantity < 10")).toBe("tpcc_stock");
  });

  it("returns null for a JOIN query", () => {
    expect(extractScatterFromTable("SELECT * FROM tpcc_stock JOIN tpcc_warehouse ON 1=1")).toBeNull();
  });

  it("returns null for zero FROM occurrences", () => {
    expect(extractScatterFromTable("SELECT 1")).toBeNull();
  });

  it("parsePlayScatterInput accepts a single-table SELECT against a demo table", () => {
    const input = parsePlayScatterInput({ sql: "SELECT * FROM tpcc_stock LIMIT 10" });
    expect(input.sql).toContain("tpcc_stock");
  });

  it("parsePlayScatterInput rejects a table outside the demo whitelist", () => {
    expect(() => parsePlayScatterInput({ sql: "SELECT * FROM applied_requests" })).toThrow(PlayValidationError);
  });

  it("parsePlayScatterInput rejects a write even before table-scoping is considered", () => {
    expect(() => parsePlayScatterInput({ sql: "DELETE FROM tpcc_stock" })).toThrow(PlayValidationError);
  });

  it("parsePlayScatterInput rejects a JOIN across two tables", () => {
    expect(() => parsePlayScatterInput({ sql: "SELECT * FROM tpcc_stock s JOIN tpcc_warehouse w ON 1=1" })).toThrow(PlayValidationError);
  });
});

// ============================================================================
// Layer 1: execute*() functions — correct tenant/token/admin-token plumbing
// ============================================================================

describe("play.ts — playMutate/playTx/playIndexQuery/playTableScan (tenant-scoped happy path)", () => {
  it("playMutate resolves the demo tenant's token + tenantId and forwards to SHARD_API.mutate, echoing requestId", async () => {
    const env = fakeEnv();
    const input = parsePlayMutateInput({ warehouseId: 1, op: "update", table: "tpcc_stock", partitionKey: "s-0001-000001", values: { s_quantity: 41 }, where: { s_quantity: 42 } });
    const result = await playMutate(env, input);
    expect(env.SHARD_API.mutate).toHaveBeenCalledWith(
      "demo-tenant-token-w1",
      expect.objectContaining({ op: "update", table: "tpcc_stock", tenantId: "tpcc-w0001", partitionKey: "s-0001-000001" }),
    );
    expect(result.rowsAffected).toBe(1);
    expect(typeof result.requestId).toBe("string");
  });

  it("playMutate uses a caller-supplied requestId verbatim (never overwrites it)", async () => {
    const env = fakeEnv();
    const input = parsePlayMutateInput({ warehouseId: 1, op: "update", table: "tpcc_stock", partitionKey: "s-0001-000001", requestId: "fixed-req-id" });
    const result = await playMutate(env, input);
    expect(result.requestId).toBe("fixed-req-id");
    expect(env.SHARD_API.mutate).toHaveBeenCalledWith("demo-tenant-token-w1", expect.objectContaining({ requestId: "fixed-req-id" }));
  });

  it("playTx stamps every mutation with the same tenantId and forwards to SHARD_API.tx", async () => {
    const env = fakeEnv();
    const input = parsePlayTxInput({
      warehouseId: 2,
      mutations: [
        { op: "insert", table: "tpcc_order_line", partitionKey: "ol-1" },
        { op: "update", table: "tpcc_stock", partitionKey: "s-0002-000001" },
      ],
    });
    await playTx(env, input);
    expect(env.SHARD_API.tx).toHaveBeenCalledWith(
      "demo-tenant-token-w2",
      expect.objectContaining({
        mutations: [
          expect.objectContaining({ tenantId: "tpcc-w0002", table: "tpcc_order_line" }),
          expect.objectContaining({ tenantId: "tpcc-w0002", table: "tpcc_stock" }),
        ],
      }),
    );
  });

  it("playIndexQuery forwards table/indexName/tenantId/values/limit to SHARD_API.indexQuery", async () => {
    const env = fakeEnv();
    const input = parsePlayIndexQueryInput({ warehouseId: 1, table: "tpcc_stock", indexName: "idx_stock_by_item", values: { i_id: 7 }, limit: 5 });
    const result = (await playIndexQuery(env, input)) as { rows: unknown[] };
    expect(env.SHARD_API.indexQuery).toHaveBeenCalledWith("demo-tenant-token-w1", {
      table: "tpcc_stock",
      indexName: "idx_stock_by_item",
      tenantId: "tpcc-w0001",
      values: { i_id: 7 },
      limit: 5,
    });
    expect(result.rows.length).toBeGreaterThan(0);
  });

  it("playTableScan forwards tenantId/table/limit/cursor to SHARD_API.tableScan, defaulting limit", async () => {
    const env = fakeEnv();
    const input = parsePlayTableScanInput({ warehouseId: 3, table: "tpcc_warehouse" });
    await playTableScan(env, input);
    expect(env.SHARD_API.tableScan).toHaveBeenCalledWith("demo-tenant-token-w3", { tenantId: "tpcc-w0003", table: "tpcc_warehouse", limit: 20, cursor: undefined });
  });
});

describe("play.ts — playSql/playScatter (operator-scoped, ADMIN_TOKEN, never a tenant token)", () => {
  it("playSql calls SHARD_API.sql with env.ADMIN_TOKEN, never a tenant token", async () => {
    const env = fakeEnv();
    const input = parsePlaySqlInput({ warehouseId: 1, table: "tpcc_stock", partitionKey: "s-0001-000001", sql: "SELECT * FROM tpcc_stock WHERE s_key = ?", params: ["s-0001-000001"] });
    await playSql(env, input);
    expect(env.SHARD_API.sql).toHaveBeenCalledWith(
      "test-admin-token",
      expect.objectContaining({ tenantId: "tpcc-w0001", table: "tpcc_stock", partitionKey: "s-0001-000001" }),
    );
  });

  it("playScatter calls SHARD_API.scatter with env.ADMIN_TOKEN", async () => {
    const env = fakeEnv();
    const input = parsePlayScatterInput({ sql: "SELECT * FROM tpcc_stock LIMIT 5", limit: 5 });
    await playScatter(env, input);
    expect(env.SHARD_API.scatter).toHaveBeenCalledWith("test-admin-token", expect.objectContaining({ sql: input.sql, limit: 5 }));
  });
});

// ============================================================================
// Layer 2: route-level tests through src/index.ts's default export — the
// gate, whitelist-400s, and the idempotent-replay/mismatch demo contract.
// ============================================================================

// worker.fetch narrows `request.cf` to the INCOMING shape (see src/index.ts's
// header comment on its own fetch signature) — a plain `new Request(...)`
// here (no real edge `cf` data available in-process) is cast the same way a
// hand-built fakeEnv() is cast elsewhere in this file/reshard.test.ts: these
// play routes never read request.cf (only GET /api/edge does, per
// src/edge.ts), so the cast is safe and test-local.
function playRequest(path: string, body: unknown, opts: { authorized?: boolean } = {}): Request<unknown, IncomingRequestCfProperties> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.authorized !== false) headers.authorization = "Bearer test-gate-token";
  return new Request(`https://shardscope.internal${path}`, { method: "POST", headers, body: JSON.stringify(body) }) as Request<
    unknown,
    IncomingRequestCfProperties
  >;
}

describe("index.ts — /api/play/* gate + wiring", () => {
  it("rejects an unauthenticated request with 401, before ever touching SHARD_API", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(
      playRequest("/api/play/sql", { warehouseId: 1, table: "tpcc_stock", partitionKey: "x", sql: "SELECT 1" }, { authorized: false }),
      env,
    );
    expect(res.status).toBe(401);
    expect(env.SHARD_API.sql).not.toHaveBeenCalled();
  });

  it("authorized POST /api/play/sql rejects a write/DDL statement with a controlled 400, never calling SHARD_API", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(playRequest("/api/play/sql", { warehouseId: 1, table: "tpcc_stock", partitionKey: "s-0001-000001", sql: "DELETE FROM tpcc_stock" }), env);
    expect(res.status).toBe(400);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/read-only operator console/i);
    expect(env.SHARD_API.sql).not.toHaveBeenCalled();
  });

  it("authorized POST /api/play/sql allows a SELECT and forwards the shard's result", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(
      playRequest("/api/play/sql", { warehouseId: 1, table: "tpcc_stock", partitionKey: "s-0001-000001", sql: "SELECT * FROM tpcc_stock WHERE s_key = ?", params: ["s-0001-000001"] }),
      env,
    );
    expect(res.status).toBe(200);
    expect(env.SHARD_API.sql).toHaveBeenCalledTimes(1);
  });

  it("authorized POST /api/play/mutate with a bad warehouseId is rejected with 400 (input-whitelist rejection)", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(playRequest("/api/play/mutate", { warehouseId: 42, op: "update", table: "tpcc_stock", partitionKey: "s-0001-000001" }), env);
    expect(res.status).toBe(400);
    expect(env.SHARD_API.mutate).not.toHaveBeenCalled();
  });

  it("authorized POST /api/play/mutate with a bad table is rejected with 400 (input-whitelist rejection)", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(playRequest("/api/play/mutate", { warehouseId: 1, op: "update", table: "row_locks", partitionKey: "x" }), env);
    expect(res.status).toBe(400);
    expect(env.SHARD_API.mutate).not.toHaveBeenCalled();
  });

  it("authorized POST /api/play/mutate happy path returns 200 with the echoed requestId", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(playRequest("/api/play/mutate", { warehouseId: 1, op: "update", table: "tpcc_stock", partitionKey: "s-0001-000001", values: { s_quantity: 41 }, where: { s_quantity: 42 } }), env);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { rowsAffected: number; requestId: string };
    expect(body.rowsAffected).toBe(1);
    expect(typeof body.requestId).toBe("string");
  });

  it("idempotent replay: same requestId + SAME body twice both come back 200 with the identical cached result", async () => {
    const env = fakeEnv();
    // Simulates ShardDO's applied_requests cache: SAME rowsAffected both times,
    // never a second "fresh execute" outcome — see src/play.ts's playMutate
    // doc comment / ../chaos.ts's header comment for the real contract this
    // mocks the SHARD_API side of.
    env.SHARD_API.mutate.mockResolvedValue({ ok: true, rowsAffected: 1 });
    const payload = { warehouseId: 1, op: "update", table: "tpcc_stock", partitionKey: "s-0001-000001", values: { s_quantity: 41 }, where: { s_quantity: 42 }, requestId: "replay-req-1" };

    const first = await worker.fetch(playRequest("/api/play/mutate", payload), env);
    const second = await worker.fetch(playRequest("/api/play/mutate", payload), env);

    expect(first.status).toBe(200);
    expect(second.status).toBe(200);
    const firstBody = (await first.json()) as { rowsAffected: number; requestId: string };
    const secondBody = (await second.json()) as { rowsAffected: number; requestId: string };
    expect(firstBody.rowsAffected).toBe(1);
    expect(secondBody.rowsAffected).toBe(1);
    expect(firstBody.requestId).toBe("replay-req-1");
    expect(secondBody.requestId).toBe("replay-req-1");
    expect(env.SHARD_API.mutate).toHaveBeenCalledTimes(2);
  });

  it("mismatched replay: same requestId + DIFFERENT body -> the real 409 mismatch contract is forwarded honestly", async () => {
    const env = fakeEnv();
    const MISMATCH_BODY = { error: "requestId was already used with different sql/params — refusing to replay a mismatched result." };
    env.SHARD_API.mutate.mockRejectedValueOnce(new Error(`CloudflareShard RPC error 409: ${JSON.stringify(MISMATCH_BODY)}`));

    const res = await worker.fetch(
      playRequest("/api/play/mutate", { warehouseId: 1, op: "update", table: "tpcc_stock", partitionKey: "s-0001-000001", values: { s_quantity: 35 }, requestId: "replay-req-2" }),
      env,
    );

    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/requestId was already used with different sql\/params/);
  });

  it("does NOT leak internal error detail on an unrecognized SHARD_API failure — generic 502", async () => {
    const env = fakeEnv();
    env.SHARD_API.mutate.mockRejectedValueOnce(new Error("some internal DO/transport failure with a stack trace"));
    const res = await worker.fetch(playRequest("/api/play/mutate", { warehouseId: 1, op: "update", table: "tpcc_stock", partitionKey: "s-0001-000001" }), env);
    expect(res.status).toBe(502);
    const body = (await res.json()) as { error: string };
    expect(body.error).not.toContain("stack trace");
  });

  it("authorized POST /api/play/tx happy path returns 200", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(
      playRequest("/api/play/tx", {
        warehouseId: 1,
        mutations: [
          { op: "insert", table: "tpcc_order_line", partitionKey: "ol-1" },
          { op: "update", table: "tpcc_stock", partitionKey: "s-0001-000001" },
        ],
      }),
      env,
    );
    expect(res.status).toBe(200);
    expect(env.SHARD_API.tx).toHaveBeenCalledTimes(1);
  });

  it("authorized POST /api/play/index-query happy path returns 200", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(playRequest("/api/play/index-query", { warehouseId: 1, table: "tpcc_stock", indexName: "idx_stock_by_item", values: { i_id: 1 } }), env);
    expect(res.status).toBe(200);
    expect(env.SHARD_API.indexQuery).toHaveBeenCalledTimes(1);
  });

  it("authorized POST /api/play/table-scan happy path returns 200", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(playRequest("/api/play/table-scan", { warehouseId: 1, table: "tpcc_warehouse" }), env);
    expect(res.status).toBe(200);
    expect(env.SHARD_API.tableScan).toHaveBeenCalledTimes(1);
  });

  it("authorized POST /api/play/scatter happy path returns 200", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(playRequest("/api/play/scatter", { sql: "SELECT * FROM tpcc_stock LIMIT 5" }), env);
    expect(res.status).toBe(200);
    expect(env.SHARD_API.scatter).toHaveBeenCalledTimes(1);
  });

  it("authorized POST /api/play/scatter rejects a query outside the demo table whitelist with 400", async () => {
    const env = fakeEnv();
    const res = await worker.fetch(playRequest("/api/play/scatter", { sql: "SELECT * FROM applied_requests" }), env);
    expect(res.status).toBe(400);
    expect(env.SHARD_API.scatter).not.toHaveBeenCalled();
  });
});
