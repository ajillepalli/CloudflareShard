import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { ShardDO } from "./shard";
import type { CoordinatorDO } from "./coordinator";

async function freshShard() {
  const id = env.SHARD.idFromName(`shard-${crypto.randomUUID()}`);
  return env.SHARD.get(id);
}

function post(path: string, body: unknown) {
  return new Request(`https://shard.internal${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

/** Directly seeds a transactions row in the one CoordinatorDO instance that
 * owns this coordinatorTxId (one-DO-per-transaction — see Chunk 3's
 * cost-model decision) — must use the same idFromName() key ShardDO's own
 * sweep uses, or the seed lands on the wrong instance. */
async function seedCoordinatorDecision(coordinatorTxId: string, status: string | null): Promise<void> {
  if (status === null) return; // simulate "no record" by seeding nothing
  const id = env.COORDINATOR.idFromName(coordinatorTxId);
  const stub = env.COORDINATOR.get(id);
  // ensureSchema() only runs on fetch(), not on raw runInDurableObject storage
  // access — trigger it first with a harmless request.
  await stub.fetch(
    new Request("https://coordinator.internal/tx-status", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ txId: "schema-warmup" }),
    }),
  );
  await runInDurableObject(stub, async (_instance: CoordinatorDO, state: DurableObjectState) => {
    state.storage.sql.exec(
      `
      INSERT OR REPLACE INTO transactions (tx_id, status, participant_shards_json, operation_json, created_at, updated_at)
      VALUES (?, ?, '[]', '{}', ?, ?)
      `,
      coordinatorTxId,
      status,
      new Date().toISOString(),
      new Date().toISOString(),
    );
  });
}

async function makeStalePendingIntent(
  stub: Awaited<ReturnType<typeof freshShard>>,
  coordinatorTxId: string,
): Promise<void> {
  await stub.fetch(post("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)", requestId: `req-schema-${coordinatorTxId}`, isMutation: true }));
  await stub.fetch(
    post("/prepare", {
      coordinatorTxId,
      intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [coordinatorTxId], tenantId: "t1", table: "t", partitionKey: coordinatorTxId }],
    }),
  );
  await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
    const staleTime = new Date(Date.now() - 10 * 60 * 1000).toISOString();
    state.storage.sql.exec(
      "UPDATE pending_intents SET prepared_at = ? WHERE coordinator_tx_id = ?",
      staleTime,
      coordinatorTxId,
    );
  });
}

describe("ShardDO idempotent mutation replay", () => {
  it("applies a mutation once and replays the same result for a duplicate requestId", async () => {
    const stub = await freshShard();

    const create = await stub.fetch(
      post("/execute", {
        sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)",
        requestId: "req-schema",
        isMutation: true,
      }),
    );
    expect(create.status).toBe(200);

    const insertPayload = {
      sql: "INSERT INTO t (id, v) VALUES (?, ?)",
      params: ["1", "a"],
      requestId: "req-1",
      isMutation: true,
    };

    const first = await (await stub.fetch(post("/execute", insertPayload))).json();
    expect((first as { duplicated?: boolean }).duplicated).toBeUndefined();
    expect((first as { rowsAffected: number }).rowsAffected).toBe(1);

    const second = await (await stub.fetch(post("/execute", insertPayload))).json();
    expect((second as { duplicated: boolean }).duplicated).toBe(true);

    const countRes = await stub.fetch(
      post("/execute", {
        sql: "SELECT COUNT(*) as n FROM t",
        requestId: "req-count",
        isMutation: false,
      }),
    );
    const countBody = (await countRes.json()) as { rows: Array<{ n: number }> };
    expect(countBody.rows[0].n).toBe(1);
  });

  it("rejects a requestId reused with different sql/params instead of replaying a mismatched result", async () => {
    const stub = await freshShard();

    await stub.fetch(
      post("/execute", {
        sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)",
        requestId: "req-schema",
        isMutation: true,
      }),
    );

    const first = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["1", "a"],
        requestId: "req-shared",
        isMutation: true,
      }),
    );
    expect(first.status).toBe(200);

    const mismatched = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["2", "b"],
        requestId: "req-shared",
        isMutation: true,
      }),
    );
    expect(mismatched.status).toBe(409);
    const body = (await mismatched.json()) as { error: string };
    expect(body.error).toContain("different sql/params");

    const countRes = await stub.fetch(
      post("/execute", { sql: "SELECT COUNT(*) as n FROM t", requestId: "req-count", isMutation: false }),
    );
    const countBody = (await countRes.json()) as { rows: Array<{ n: number }> };
    expect(countBody.rows[0].n).toBe(1);
  });

  it("rolls back and does not record the request on a SQL error", async () => {
    const stub = await freshShard();

    const res = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO nonexistent_table (id) VALUES (?)",
        params: ["1"],
        requestId: "req-fail",
        isMutation: true,
      }),
    );
    expect(res.status).toBe(400);

    const retry = await stub.fetch(
      post("/execute", {
        sql: "CREATE TABLE IF NOT EXISTS t2 (id TEXT PRIMARY KEY)",
        requestId: "req-fail",
        isMutation: true,
      }),
    );
    expect(retry.status).toBe(200);
    const body = (await retry.json()) as { duplicated?: boolean };
    expect(body.duplicated).toBeUndefined();
  });
});

describe("ShardDO /stats", () => {
  it("reports row counts per table and the idempotency table size", async () => {
    const stub = await freshShard();
    await stub.fetch(
      post("/execute", {
        sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
        requestId: "req-schema",
        isMutation: true,
      }),
    );
    await stub.fetch(
      post("/execute", { sql: "INSERT INTO t (id) VALUES ('1')", requestId: "req-1", isMutation: true }),
    );
    await stub.fetch(
      post("/execute", { sql: "INSERT INTO t (id) VALUES ('2')", requestId: "req-2", isMutation: true }),
    );

    const res = await stub.fetch(post("/stats", {}));
    expect(res.status).toBe(200);
    const body = (await res.json()) as {
      ok: boolean;
      tables: Array<{ table: string; rowCount: number }>;
      idempotencyTableSize: number;
    };
    expect(body.ok).toBe(true);
    const t = body.tables.find((x) => x.table === "t");
    expect(t?.rowCount).toBe(2);
    expect(body.idempotencyTableSize).toBe(3); // req-schema, req-1, req-2
  });
});

describe("ShardDO /execute input validation", () => {
  it("returns 400 when sql or requestId is missing", async () => {
    const stub = await freshShard();
    const res = await stub.fetch(post("/execute", { requestId: "req-1" }));
    expect(res.status).toBe(400);
  });

  it("returns 400 for a non-mutation query with a SQL error", async () => {
    const stub = await freshShard();
    const res = await stub.fetch(
      post("/execute", { sql: "SELECT * FROM nonexistent_table", requestId: "req-q", isMutation: false }),
    );
    expect(res.status).toBe(400);
  });

  it("does not leak the raw SQL error to the caller", async () => {
    const stub = await freshShard();
    const res = await stub.fetch(
      post("/execute", { sql: "SELECT * FROM nonexistent_table", requestId: "req-q2", isMutation: false }),
    );
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("SQL execution failed.");
  });

  it("regression: derives isMutation from the SQL itself, ignoring a false caller-supplied flag", async () => {
    const stub = await freshShard();
    await stub.fetch(
      post("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)", requestId: "req-schema", isMutation: true }),
    );

    // Caller (or a comment-obfuscated bypass upstream) falsely claims this is
    // not a mutation. ShardDO must still classify it as one — via the
    // idempotent transactionSync path, recorded in applied_requests — not
    // silently execute it as a bare read.
    const res = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id) VALUES ('poisoned')",
        requestId: "req-lied",
        isMutation: false,
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { type: string; rowsAffected: number };
    expect(body.type).toBe("mutation");
    expect(body.rowsAffected).toBe(1);

    // Confirm it went through the idempotency path: replaying the same
    // requestId returns the cached result instead of re-executing.
    const replay = await stub.fetch(
      post("/execute", { sql: "INSERT INTO t (id) VALUES ('poisoned')", requestId: "req-lied", isMutation: false }),
    );
    const replayBody = (await replay.json()) as { duplicated: boolean };
    expect(replayBody.duplicated).toBe(true);
  });
});

describe("ShardDO route/method guards", () => {
  it("rejects non-POST methods with 405", async () => {
    const stub = await freshShard();
    const res = await stub.fetch(new Request("https://shard.internal/stats", { method: "GET" }));
    expect(res.status).toBe(405);
  });

  it("returns 404 for an unknown shard route", async () => {
    const stub = await freshShard();
    const res = await stub.fetch(post("/not-a-real-route", {}));
    expect(res.status).toBe(404);
  });
});

describe("ShardDO error boundary", () => {
  it("returns a clean 500 instead of an unhandled crash on malformed JSON to /execute", async () => {
    const stub = await freshShard();
    const res = await stub.fetch(
      new Request("https://shard.internal/execute", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{not valid json",
      }),
    );
    expect(res.status).toBe(500);
    const body = (await res.json()) as { error: string };
    expect(body.error).toBe("Internal error.");
  });
});

describe("ShardDO applied_requests pruning", () => {
  it("prunes rows older than the TTL when the alarm fires", async () => {
    const id = env.SHARD.idFromName(`shard-prune-${crypto.randomUUID()}`);
    const stub = env.SHARD.get(id);

    await stub.fetch(
      post("/execute", {
        sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
        requestId: "req-init",
        isMutation: true,
      }),
    );
    await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id) VALUES ('1')",
        requestId: "req-old",
        isMutation: true,
      }),
    );

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      const eightDaysAgo = new Date(Date.now() - 8 * 24 * 60 * 60 * 1000).toISOString();
      sql.exec("UPDATE applied_requests SET applied_at = ? WHERE request_id = ?", eightDaysAgo, "req-old");
      await instance.alarm();

      const remaining = Array.from(
        sql.exec("SELECT request_id FROM applied_requests WHERE request_id = ?", "req-old"),
      );
      expect(remaining.length).toBe(0);
    });
  });
});

describe("ShardDO 2PC: prepare/commit/abort", () => {
  async function createTable(stub: Awaited<ReturnType<typeof freshShard>>, schema: string) {
    const res = await stub.fetch(post("/execute", { sql: schema, requestId: `req-schema-${crypto.randomUUID()}`, isMutation: true }));
    expect(res.status).toBe(200);
  }

  it("prepare leaves the real table completely unchanged and invisible to a concurrent SELECT", async () => {
    const stub = await freshShard();
    await createTable(stub, "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)");

    const prepareRes = await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-1",
        intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-1", "hello"], tenantId: "t1", table: "t", partitionKey: "row-1" }],
      }),
    );
    expect(prepareRes.status).toBe(200);
    const prepareBody = (await prepareRes.json()) as { ok: boolean; prepared: number };
    expect(prepareBody.ok).toBe(true);
    expect(prepareBody.prepared).toBe(1);

    const checkRes = await stub.fetch(
      post("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-1"], requestId: "req-check", isMutation: false }),
    );
    const checkBody = (await checkRes.json()) as { rows: unknown[] };
    expect(checkBody.rows).toHaveLength(0);
  });

  it("commit applies the change and is idempotent", async () => {
    const stub = await freshShard();
    await createTable(stub, "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)");
    await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-2",
        intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-2", "hi"], tenantId: "t1", table: "t", partitionKey: "row-2" }],
      }),
    );

    const commitRes = await stub.fetch(post("/commit", { coordinatorTxId: "tx-2" }));
    expect(commitRes.status).toBe(200);

    const checkRes = await stub.fetch(
      post("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-2"], requestId: "req-check2", isMutation: false }),
    );
    const checkBody = (await checkRes.json()) as { rows: unknown[] };
    expect(checkBody.rows).toHaveLength(1);

    const commitRetry = await stub.fetch(post("/commit", { coordinatorTxId: "tx-2" }));
    expect(commitRetry.status).toBe(200);
    const retryBody = (await commitRetry.json()) as { alreadyResolved: boolean };
    expect(retryBody.alreadyResolved).toBe(true);

    const countRes = await stub.fetch(
      post("/execute", { sql: "SELECT COUNT(*) as n FROM t WHERE id = ?", params: ["row-2"], requestId: "req-check3", isMutation: false }),
    );
    const countBody = (await countRes.json()) as { rows: Array<{ n: number }> };
    expect(countBody.rows[0].n).toBe(1);
  });

  it("abort leaves no trace and is idempotent", async () => {
    const stub = await freshShard();
    await createTable(stub, "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)");
    await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-3",
        intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-3", "x"], tenantId: "t1", table: "t", partitionKey: "row-3" }],
      }),
    );

    const abortRes = await stub.fetch(post("/abort", { coordinatorTxId: "tx-3" }));
    expect(abortRes.status).toBe(200);

    const checkRes = await stub.fetch(
      post("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-3"], requestId: "req-check4", isMutation: false }),
    );
    const checkBody = (await checkRes.json()) as { rows: unknown[] };
    expect(checkBody.rows).toHaveLength(0);

    await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
      const intents = Array.from(state.storage.sql.exec("SELECT * FROM pending_intents WHERE coordinator_tx_id = ?", "tx-3"));
      expect(intents).toHaveLength(0);
      const locks = Array.from(state.storage.sql.exec("SELECT * FROM row_locks WHERE coordinator_tx_id = ?", "tx-3"));
      expect(locks).toHaveLength(0);
    });

    const abortRetry = await stub.fetch(post("/abort", { coordinatorTxId: "tx-3" }));
    expect(abortRetry.status).toBe(200);
  });

  it("rejects abort after commit (would violate atomicity)", async () => {
    const stub = await freshShard();
    await createTable(stub, "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)");
    await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-4",
        intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: ["row-4"], tenantId: "t1", table: "t", partitionKey: "row-4" }],
      }),
    );
    await stub.fetch(post("/commit", { coordinatorTxId: "tx-4" }));

    const abortRes = await stub.fetch(post("/abort", { coordinatorTxId: "tx-4" }));
    expect(abortRes.status).toBe(409);
    const body = (await abortRes.json()) as { error: { code: string } };
    expect(body.error.code).toBe("ALREADY_COMMITTED");
  });

  it("lock conflict across two coordinatorTxIds returns 409 with retryAfterMs", async () => {
    const stub = await freshShard();
    await createTable(stub, "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)");
    await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-5",
        intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: ["row-5"], tenantId: "t1", table: "t", partitionKey: "row-5" }],
      }),
    );

    const conflictRes = await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-6",
        intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: ["row-5-other"], tenantId: "t1", table: "t", partitionKey: "row-5" }],
      }),
    );
    expect(conflictRes.status).toBe(409);
    const body = (await conflictRes.json()) as { error: { code: string; retryAfterMs: number } };
    expect(body.error.code).toBe("TX_PARTICIPANT_LOCKED");
    expect(typeof body.error.retryAfterMs).toBe("number");
  });

  it("double-prepare with the same coordinatorTxId is a no-op", async () => {
    const stub = await freshShard();
    await createTable(stub, "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)");
    const intents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: ["row-7"], tenantId: "t1", table: "t", partitionKey: "row-7" }];

    const first = await stub.fetch(post("/prepare", { coordinatorTxId: "tx-7", intents }));
    expect(first.status).toBe(200);
    const second = await stub.fetch(post("/prepare", { coordinatorTxId: "tx-7", intents }));
    expect(second.status).toBe(200);

    await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
      const rows = Array.from(state.storage.sql.exec("SELECT * FROM pending_intents WHERE coordinator_tx_id = ?", "tx-7"));
      expect(rows).toHaveLength(1);
    });
  });

  it("a raw /execute mutation against a locked row is rejected 409", async () => {
    const stub = await freshShard();
    await createTable(stub, "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)");
    await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-8",
        intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: ["row-8"], tenantId: "t1", table: "t", partitionKey: "row-8" }],
      }),
    );

    const rawRes = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id) VALUES ('row-8-conflict')",
        requestId: "req-raw",
        isMutation: true,
        tenantId: "t1",
        table: "t",
        partitionKey: "row-8",
      }),
    );
    expect(rawRes.status).toBe(409);
    const body = (await rawRes.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_PARTICIPANT_LOCKED");
  });

  it("raw /execute without routing context is unaffected by locks (documented residual gap, not a bug)", async () => {
    const stub = await freshShard();
    await createTable(stub, "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)");
    await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-9",
        intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: ["row-9"], tenantId: "t1", table: "t", partitionKey: "row-9" }],
      }),
    );

    const rawRes = await stub.fetch(
      post("/execute", { sql: "INSERT INTO t (id) VALUES ('row-9-unlabeled')", requestId: "req-raw2", isMutation: true }),
    );
    expect(rawRes.status).toBe(200);
  });

  it("abort of an autoincrement-table insert does NOT leave a sequence gap (empirically verified, correcting the plan's original assumption)", async () => {
    // Earlier planning assumed SQLite's ROLLBACK doesn't reset sqlite_sequence,
    // so a prepare-then-abort cycle would permanently consume an autoincrement
    // id. Verified false for DO SQLite's transactionSync: the rollback covers
    // the whole transaction, including sqlite_sequence's internal bookkeeping,
    // exactly like it covers the row insert itself.
    const stub = await freshShard();
    await createTable(stub, "CREATE TABLE IF NOT EXISTS seq_t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)");

    await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-10",
        intents: [{ sql: "INSERT INTO seq_t (v) VALUES (?)", params: ["never-lands"], tenantId: "t1", table: "seq_t", partitionKey: "p1" }],
      }),
    );
    await stub.fetch(post("/abort", { coordinatorTxId: "tx-10" }));

    await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-11",
        intents: [{ sql: "INSERT INTO seq_t (v) VALUES (?)", params: ["lands"], tenantId: "t1", table: "seq_t", partitionKey: "p2" }],
      }),
    );
    await stub.fetch(post("/commit", { coordinatorTxId: "tx-11" }));

    const checkRes = await stub.fetch(
      post("/execute", { sql: "SELECT id FROM seq_t WHERE v = ?", params: ["lands"], requestId: "req-check5", isMutation: false }),
    );
    const checkBody = (await checkRes.json()) as { rows: Array<{ id: number }> };
    expect(checkBody.rows[0].id).toBe(1);
  });

  it("regression (Codex-found): a batch with two intents on the same (tenantId, table, partitionKey) prepares cleanly instead of crashing on a duplicate row_locks insert", async () => {
    const stub = await freshShard();
    await createTable(stub, "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)");

    const prepareRes = await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-dup-lock",
        intents: [
          { sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-dup", "a"], tenantId: "t1", table: "t", partitionKey: "row-dup" },
          { sql: "UPDATE t SET v = ? WHERE id = ?", params: ["b", "row-dup"], tenantId: "t1", table: "t", partitionKey: "row-dup" },
        ],
      }),
    );
    expect(prepareRes.status).toBe(200);
    const prepareBody = (await prepareRes.json()) as { ok: boolean; prepared: number };
    expect(prepareBody.ok).toBe(true);
    expect(prepareBody.prepared).toBe(2);

    const commitRes = await stub.fetch(post("/commit", { coordinatorTxId: "tx-dup-lock" }));
    expect(commitRes.status).toBe(200);

    const checkRes = await stub.fetch(
      post("/execute", { sql: "SELECT v FROM t WHERE id = ?", params: ["row-dup"], requestId: "req-check-dup", isMutation: false }),
    );
    const checkBody = (await checkRes.json()) as { rows: Array<{ v: string }> };
    expect(checkBody.rows).toHaveLength(1);
    expect(checkBody.rows[0].v).toBe("b");
  });
});

describe("ShardDO /invalidate-request", () => {
  it("clears a cached idempotency entry so a subsequent /execute with the same requestId genuinely re-runs", async () => {
    const stub = await freshShard();
    await stub.fetch(
      post("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)", requestId: "req-schema", isMutation: true }),
    );

    const first = await stub.fetch(
      post("/execute", { sql: "INSERT INTO t (id) VALUES (?)", params: ["1"], requestId: "req-invalidate", isMutation: true }),
    );
    expect(first.status).toBe(200);

    // Without invalidation, replaying this requestId just returns the cached
    // result. Simulate the caller having undone the effect out from under it
    // (e.g. DROP TABLE) — the cache would otherwise lie about what's real.
    await stub.fetch(post("/execute", { sql: "DROP TABLE t", requestId: "req-drop", isMutation: true }));
    await stub.fetch(post("/invalidate-request", { requestId: "req-invalidate" }));

    await stub.fetch(
      post("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)", requestId: "req-schema-2", isMutation: true }),
    );
    const retry = await stub.fetch(
      post("/execute", { sql: "INSERT INTO t (id) VALUES (?)", params: ["1"], requestId: "req-invalidate", isMutation: true }),
    );
    expect(retry.status).toBe(200);
    const retryBody = (await retry.json()) as { duplicated?: boolean; rowsAffected: number };
    expect(retryBody.duplicated).toBeUndefined();
    expect(retryBody.rowsAffected).toBe(1);
  });

  it("requires requestId", async () => {
    const stub = await freshShard();
    const res = await stub.fetch(post("/invalidate-request", {}));
    expect(res.status).toBe(400);
  });
});

describe("ShardDO 2PC: TTL sweep queries the coordinator, never unilaterally aborts", () => {
  it("applies the commit locally when the coordinator reports committed", async () => {
    const stub = await freshShard();
    const coordinatorTxId = `sweep-committed-${crypto.randomUUID()}`;
    await makeStalePendingIntent(stub, coordinatorTxId);
    await seedCoordinatorDecision(coordinatorTxId, "committed");

    await runInDurableObject(stub, async (instance: ShardDO) => {
      await instance.alarm();
    });

    const checkRes = await stub.fetch(
      post("/execute", { sql: "SELECT id FROM t WHERE id = ?", params: [coordinatorTxId], requestId: "req-sweep-check-1", isMutation: false }),
    );
    const checkBody = (await checkRes.json()) as { rows: unknown[] };
    expect(checkBody.rows).toHaveLength(1);

    await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
      const intents = Array.from(state.storage.sql.exec("SELECT status FROM pending_intents WHERE coordinator_tx_id = ?", coordinatorTxId)) as Array<{ status: string }>;
      expect(intents[0].status).toBe("committed");
    });
  });

  it("aborts locally when the coordinator reports aborted", async () => {
    const stub = await freshShard();
    const coordinatorTxId = `sweep-aborted-${crypto.randomUUID()}`;
    await makeStalePendingIntent(stub, coordinatorTxId);
    await seedCoordinatorDecision(coordinatorTxId, "aborted");

    await runInDurableObject(stub, async (instance: ShardDO) => {
      await instance.alarm();
    });

    const checkRes = await stub.fetch(
      post("/execute", { sql: "SELECT id FROM t WHERE id = ?", params: [coordinatorTxId], requestId: "req-sweep-check-2", isMutation: false }),
    );
    const checkBody = (await checkRes.json()) as { rows: unknown[] };
    expect(checkBody.rows).toHaveLength(0);

    await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
      const intents = Array.from(state.storage.sql.exec("SELECT * FROM pending_intents WHERE coordinator_tx_id = ?", coordinatorTxId));
      expect(intents).toHaveLength(0);
    });
  });

  it("aborts locally when the coordinator has no record of the txId (never durably persisted)", async () => {
    const stub = await freshShard();
    const coordinatorTxId = `sweep-not-found-${crypto.randomUUID()}`;
    await makeStalePendingIntent(stub, coordinatorTxId);
    // No seedCoordinatorDecision call — the coordinator genuinely has no record.

    await runInDurableObject(stub, async (instance: ShardDO) => {
      await instance.alarm();
    });

    await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
      const intents = Array.from(state.storage.sql.exec("SELECT * FROM pending_intents WHERE coordinator_tx_id = ?", coordinatorTxId));
      expect(intents).toHaveLength(0);
    });
  });

  it("regression: does NOT unilaterally abort when the coordinator reports the tx is still pending — leaves the intent and the lock untouched", async () => {
    const stub = await freshShard();
    const coordinatorTxId = `sweep-pending-${crypto.randomUUID()}`;
    await makeStalePendingIntent(stub, coordinatorTxId);
    await seedCoordinatorDecision(coordinatorTxId, "preparing");

    await runInDurableObject(stub, async (instance: ShardDO) => {
      await instance.alarm();
    });

    // This is the exact bug the Eng review caught and required a fix for:
    // a participant must NEVER independently decide to abort while the
    // coordinator's decision is still genuinely uncertain.
    await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
      const intents = Array.from(
        state.storage.sql.exec("SELECT status FROM pending_intents WHERE coordinator_tx_id = ?", coordinatorTxId),
      ) as Array<{ status: string }>;
      expect(intents).toHaveLength(1);
      expect(intents[0].status).toBe("prepared");

      const locks = Array.from(state.storage.sql.exec("SELECT * FROM row_locks WHERE coordinator_tx_id = ?", coordinatorTxId));
      expect(locks).toHaveLength(1);
    });
  });

});
