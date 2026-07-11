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

describe("ShardDO __cf_mirror_pending dual-write retry queue (Milestone 3, Chunk 3)", () => {
  it("a job enqueued via /enqueue-mirror-job is retried against the target and cleared by alarm(), applying the write there under the original requestId", async () => {
    const sourceStub = await freshShard();
    const targetShardName = `mirror-target-${crypto.randomUUID()}`;
    const targetStub = env.SHARD.get(env.SHARD.idFromName(targetShardName));

    // Target needs the table to exist for the mirrored INSERT to apply.
    await targetStub.fetch(
      post("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `req-schema-${crypto.randomUUID()}`, isMutation: true }),
    );

    const enqueueRes = await sourceStub.fetch(
      post("/enqueue-mirror-job", {
        targetShardId: targetShardName,
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["mirrored-row", "x"],
        requestId: "req-mirror-original",
        vbucket: 7,
      }),
    );
    expect(enqueueRes.status).toBe(200);

    const countBefore = await (await sourceStub.fetch(post("/mirror-pending-count", { vbucket: 7 }))).json();
    expect((countBefore as { count: number }).count).toBe(1);

    await runInDurableObject(sourceStub, async (instance: ShardDO) => {
      await instance.alarm();
    });

    const countAfter = await (await sourceStub.fetch(post("/mirror-pending-count", { vbucket: 7 }))).json();
    expect((countAfter as { count: number }).count).toBe(0);

    const checkRes = await targetStub.fetch(
      post("/execute", { sql: "SELECT v FROM t WHERE id = ?", params: ["mirrored-row"], requestId: "req-mirror-check", isMutation: false }),
    );
    const checkBody = (await checkRes.json()) as { rows: Array<{ v: string }> };
    expect(checkBody.rows).toHaveLength(1);

    // The write landed under the ORIGINAL requestId: replaying it on the
    // target dedupes rather than re-executing (the idempotency contract
    // that makes mirror + backfill + retry safely re-appliable).
    const replay = await targetStub.fetch(
      post("/execute", { sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["mirrored-row", "x"], requestId: "req-mirror-original", isMutation: true }),
    );
    const replayBody = (await replay.json()) as { duplicated: boolean };
    expect(replayBody.duplicated).toBe(true);
  });

  it("a mirror job whose target keeps failing stays queued with attempt_count incremented and exponential backoff from 1s, never dropped", async () => {
    const sourceStub = await freshShard();
    // Target shard exists but the table doesn't — the mirrored INSERT will
    // fail with a SQL error (400) on every attempt.
    const targetShardName = `mirror-failing-${crypto.randomUUID()}`;

    await sourceStub.fetch(
      post("/enqueue-mirror-job", {
        targetShardId: targetShardName,
        sql: "INSERT INTO missing_table (id) VALUES (?)",
        params: ["never-lands"],
        requestId: "req-mirror-fail",
        vbucket: 3,
      }),
    );

    await runInDurableObject(sourceStub, async (instance: ShardDO, state: DurableObjectState) => {
      await instance.alarm();
      const jobs = Array.from(
        state.storage.sql.exec("SELECT attempt_count, next_attempt_at FROM __cf_mirror_pending WHERE request_id = ?", "req-mirror-fail"),
      ) as Array<{ attempt_count: number; next_attempt_at: string }>;
      expect(jobs).toHaveLength(1);
      expect(jobs[0].attempt_count).toBe(1);
      // First retry delay = MIRROR_JOB_BASE_DELAY_MS * 2^0 = 1s.
      const delayMs = new Date(jobs[0].next_attempt_at).getTime() - Date.now();
      expect(delayMs).toBeGreaterThan(0);
      expect(delayMs).toBeLessThanOrEqual(1000 + 250);

      // Force it due again and re-run: attempt 2, delay doubles to 2s.
      state.storage.sql.exec(
        "UPDATE __cf_mirror_pending SET next_attempt_at = ? WHERE request_id = ?",
        new Date(Date.now() - 1).toISOString(),
        "req-mirror-fail",
      );
      await instance.alarm();
      const jobs2 = Array.from(
        state.storage.sql.exec("SELECT attempt_count, next_attempt_at FROM __cf_mirror_pending WHERE request_id = ?", "req-mirror-fail"),
      ) as Array<{ attempt_count: number; next_attempt_at: string }>;
      expect(jobs2).toHaveLength(1);
      expect(jobs2[0].attempt_count).toBe(2);
      const delay2Ms = new Date(jobs2[0].next_attempt_at).getTime() - Date.now();
      expect(delay2Ms).toBeGreaterThan(1000);
      expect(delay2Ms).toBeLessThanOrEqual(2000 + 250);
    });
  });

  it("/enqueue-mirror-job requires targetShardId, sql, requestId, and vbucket", async () => {
    const stub = await freshShard();
    const res = await stub.fetch(post("/enqueue-mirror-job", { targetShardId: "t", sql: "SELECT 1", requestId: "r" }));
    expect(res.status).toBe(400);
  });

  it("/mirror-pending-count scopes by vbucket so one vbucket's mirror debt doesn't block another's cutover", async () => {
    const stub = await freshShard();
    await stub.fetch(
      post("/enqueue-mirror-job", { targetShardId: "t1", sql: "SELECT 1", params: [], requestId: "r1", vbucket: 1 }),
    );
    await stub.fetch(
      post("/enqueue-mirror-job", { targetShardId: "t1", sql: "SELECT 1", params: [], requestId: "r2", vbucket: 2 }),
    );

    const forV1 = (await (await stub.fetch(post("/mirror-pending-count", { vbucket: 1 }))).json()) as { count: number };
    const forV2 = (await (await stub.fetch(post("/mirror-pending-count", { vbucket: 2 }))).json()) as { count: number };
    const total = (await (await stub.fetch(post("/mirror-pending-count", {}))).json()) as { count: number };
    expect(forV1.count).toBe(1);
    expect(forV2.count).toBe(1);
    expect(total.count).toBe(2);
  });

  // Review Tier 1 #3: a mirror delivered via the retry queue must carry
  // routing context so the TARGET writes its own __cf_row_owners provenance —
  // otherwise a row whose first appearance on the target is a mirror is
  // invisible to the cutover checksum's provenance-scoped selection.
  it("a mirror delivered via retry writes __cf_row_owners provenance on the target (routing context forwarded)", async () => {
    const sourceStub = await freshShard();
    const targetShardName = `mirror-prov-${crypto.randomUUID()}`;
    const targetStub = env.SHARD.get(env.SHARD.idFromName(targetShardName));
    await targetStub.fetch(
      post("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `req-schema-${crypto.randomUUID()}`, isMutation: true }),
    );

    await sourceStub.fetch(
      post("/enqueue-mirror-job", {
        targetShardId: targetShardName,
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["prov-row", "x"],
        requestId: "__cf:mirror:test",
        vbucket: 12,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "prov-row",
      }),
    );
    await runInDurableObject(sourceStub, async (instance: ShardDO) => {
      await instance.alarm();
    });

    // The target has the base row AND its provenance entry.
    await runInDurableObject(targetStub, async (_i: ShardDO, state: DurableObjectState) => {
      const rows = Array.from(
        state.storage.sql.exec("SELECT tenant_id, vbucket FROM __cf_row_owners WHERE table_name = ? AND partition_key = ?", "t", "prov-row"),
      ) as Array<{ tenant_id: string; vbucket: number }>;
      expect(rows).toHaveLength(1);
      expect(rows[0].tenant_id).toBe("tenant-1");
      expect(rows[0].vbucket).toBe(12);
    });
  });

  // Review Tier 1 #2: /drain-mirror-jobs actively delivers every queued
  // mirror for a vbucket and reports how many remain (cutover uses this
  // rather than passively waiting on the source's alarm cadence).
  it("/drain-mirror-jobs delivers all of a vbucket's queued mirrors now and reports remaining", async () => {
    const sourceStub = await freshShard();
    const targetShardName = `mirror-drain-${crypto.randomUUID()}`;
    const targetStub = env.SHARD.get(env.SHARD.idFromName(targetShardName));
    await targetStub.fetch(
      post("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `req-schema-${crypto.randomUUID()}`, isMutation: true }),
    );
    for (let i = 0; i < 3; i += 1) {
      await sourceStub.fetch(
        post("/enqueue-mirror-job", {
          targetShardId: targetShardName,
          sql: "INSERT INTO t (id, v) VALUES (?, ?)",
          params: [`d-${i}`, "x"],
          requestId: `__cf:mirror:d-${i}`,
          vbucket: 20,
          tenantId: "tenant-1",
          table: "t",
          partitionKey: `d-${i}`,
        }),
      );
    }

    const drainRes = await sourceStub.fetch(post("/drain-mirror-jobs", { vbucket: 20 }));
    expect(drainRes.status).toBe(200);
    expect(((await drainRes.json()) as { remaining: number }).remaining).toBe(0);

    const count = (await (await targetStub.fetch(post("/execute", { sql: "SELECT COUNT(*) AS n FROM t", requestId: "c", isMutation: false }))).json()) as {
      rows: Array<{ n: number }>;
    };
    expect(count.rows[0].n).toBe(3);
  });
});

describe("ShardDO migration fence and export/import (Milestone 3, Chunk 4)", () => {
  async function createTable(stub: Awaited<ReturnType<typeof freshShard>>) {
    await stub.fetch(
      post("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `req-schema-${crypto.randomUUID()}`, isMutation: true }),
    );
  }

  async function insertWithProvenance(stub: Awaited<ReturnType<typeof freshShard>>, id: string, v: string, vbucket: number) {
    const res = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: [id, v],
        requestId: `req-ins-${id}-${crypto.randomUUID()}`,
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: id,
        vbucket,
      }),
    );
    expect(res.status).toBe(200);
  }

  it("a fenced vbucket rejects new writes 409 VBUCKET_FENCED, still replays already-applied requestIds, ignores other vbuckets, and accepts again after unfence", async () => {
    const stub = await freshShard();
    await createTable(stub);
    await insertWithProvenance(stub, "pre-fence", "a", 5);

    // Capture the exact requestId of an applied write for the replay check.
    const appliedRequestId = `req-applied-${crypto.randomUUID()}`;
    await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["applied-row", "x"],
        requestId: appliedRequestId,
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "applied-row",
        vbucket: 5,
      }),
    );

    const fenceRes = await stub.fetch(post("/fence-vbucket", { vbucket: 5 }));
    expect(fenceRes.status).toBe(200);

    // New write to the fenced vbucket: rejected, retryable shape.
    const blocked = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["post-fence", "b"],
        requestId: `req-blocked-${crypto.randomUUID()}`,
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "post-fence",
        vbucket: 5,
      }),
    );
    expect(blocked.status).toBe(409);
    const blockedBody = (await blocked.json()) as { error: { code: string } };
    expect(blockedBody.error.code).toBe("VBUCKET_FENCED");

    // Replay of an ALREADY-APPLIED requestId returns the cached result, not
    // a spurious fence rejection.
    const replay = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["applied-row", "x"],
        requestId: appliedRequestId,
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "applied-row",
        vbucket: 5,
      }),
    );
    expect(replay.status).toBe(200);
    expect(((await replay.json()) as { duplicated: boolean }).duplicated).toBe(true);

    // A different vbucket on the same shard is unaffected.
    const otherVb = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["other-vb", "c"],
        requestId: `req-other-${crypto.randomUUID()}`,
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "other-vb",
        vbucket: 6,
      }),
    );
    expect(otherVb.status).toBe(200);

    // 2PC prepare against the fenced vbucket is rejected too.
    const prepBlocked = await stub.fetch(
      post("/prepare", {
        coordinatorTxId: `tx-fenced-${crypto.randomUUID()}`,
        intents: [
          { sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["tx-row", "d"], tenantId: "tenant-1", table: "t", partitionKey: "tx-row", vbucket: 5, op: "insert" },
        ],
      }),
    );
    expect(prepBlocked.status).toBe(409);
    expect(((await prepBlocked.json()) as { error: { code: string } }).error.code).toBe("VBUCKET_FENCED");

    await stub.fetch(post("/unfence-vbucket", { vbucket: 5 }));
    const afterUnfence = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["post-unfence", "e"],
        requestId: `req-unfenced-${crypto.randomUUID()}`,
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "post-unfence",
        vbucket: 5,
      }),
    );
    expect(afterUnfence.status).toBe(200);
  });

  it("export pages stably by partition key, import is idempotent, and source/target checksums match after a full copy", async () => {
    const source = await freshShard();
    const targetName = `migrate-target-${crypto.randomUUID()}`;
    const target = env.SHARD.get(env.SHARD.idFromName(targetName));
    await createTable(source);
    await createTable(target);

    // 5 rows in vbucket 9, 1 row in another vbucket that must NOT export.
    for (let i = 0; i < 5; i += 1) {
      await insertWithProvenance(source, `row-${i}`, `v${i}`, 9);
    }
    await insertWithProvenance(source, "row-other", "vx", 10);

    // Page with limit 2: 2 + 2 + 1.
    const pages: Array<Array<{ partitionKey: string; tenantId: string; row: Record<string, unknown> }>> = [];
    let afterPk = "";
    for (;;) {
      const res = await source.fetch(
        post("/migrate-export", { vbucket: 9, table: "t", partitionKeyColumn: "id", afterPartitionKey: afterPk, limit: 2 }),
      );
      expect(res.status).toBe(200);
      const body = (await res.json()) as { rows: Array<{ partitionKey: string; tenantId: string; row: Record<string, unknown> }> };
      if (body.rows.length === 0) break;
      pages.push(body.rows);
      afterPk = body.rows[body.rows.length - 1].partitionKey;

      const importRes = await target.fetch(post("/migrate-import", { vbucket: 9, table: "t", rows: body.rows }));
      expect(importRes.status).toBe(200);
      if (body.rows.length < 2) break;
    }
    expect(pages.map((p) => p.length)).toEqual([2, 2, 1]);
    const exportedKeys = pages.flat().map((r) => r.partitionKey);
    expect(exportedKeys).toEqual(["row-0", "row-1", "row-2", "row-3", "row-4"]); // stable ASC order, no row-other

    // Re-import the first page — idempotent, no duplicates.
    const reimport = await target.fetch(post("/migrate-import", { vbucket: 9, table: "t", rows: pages[0] }));
    expect(reimport.status).toBe(200);

    const countRes = await target.fetch(
      post("/execute", { sql: "SELECT COUNT(*) AS n FROM t", requestId: `req-count-${crypto.randomUUID()}`, isMutation: false }),
    );
    expect(((await countRes.json()) as { rows: Array<{ n: number }> }).rows[0].n).toBe(5);

    // Checksums agree between source and target for the migrated vbucket.
    const srcSum = (await (
      await source.fetch(post("/migrate-checksum", { vbucket: 9, table: "t", partitionKeyColumn: "id" }))
    ).json()) as { checksum: string; rowCount: number };
    const tgtSum = (await (
      await target.fetch(post("/migrate-checksum", { vbucket: 9, table: "t", partitionKeyColumn: "id" }))
    ).json()) as { checksum: string; rowCount: number };
    expect(srcSum.rowCount).toBe(5);
    expect(tgtSum.rowCount).toBe(5);
    expect(tgtSum.checksum).toBe(srcSum.checksum);

    // And a divergence IS detected: mutate one target row, checksum differs.
    await target.fetch(
      post("/execute", { sql: "UPDATE t SET v = 'tampered' WHERE id = 'row-0'", requestId: `req-tamper-${crypto.randomUUID()}`, isMutation: true }),
    );
    const tamperedSum = (await (
      await target.fetch(post("/migrate-checksum", { vbucket: 9, table: "t", partitionKeyColumn: "id" }))
    ).json()) as { checksum: string };
    expect(tamperedSum.checksum).not.toBe(srcSum.checksum);
  });

  it("/delete-vbucket-rows removes exactly one vbucket's rows + provenance and /unattributed-count reports rows lacking provenance", async () => {
    const stub = await freshShard();
    await createTable(stub);
    await insertWithProvenance(stub, "keep-me", "a", 1);
    await insertWithProvenance(stub, "delete-me", "b", 2);
    // A provenance-less row (pre-Chunk-0 write).
    await stub.fetch(
      post("/execute", { sql: "INSERT INTO t (id, v) VALUES ('no-prov', 'c')", requestId: `req-noprov-${crypto.randomUUID()}`, isMutation: true }),
    );

    const unattributed = (await (
      await stub.fetch(post("/unattributed-count", { tables: [{ table: "t", partitionKeyColumn: "id" }] }))
    ).json()) as { count: number };
    expect(unattributed.count).toBe(1);

    const delRes = await stub.fetch(
      post("/delete-vbucket-rows", { vbucket: 2, tables: [{ table: "t", partitionKeyColumn: "id" }] }),
    );
    expect(delRes.status).toBe(200);

    const remaining = await stub.fetch(
      post("/execute", { sql: "SELECT id FROM t ORDER BY id", requestId: `req-remaining-${crypto.randomUUID()}`, isMutation: false }),
    );
    const remainingBody = (await remaining.json()) as { rows: Array<{ id: string }> };
    expect(remainingBody.rows.map((r) => r.id)).toEqual(["keep-me", "no-prov"]);

    await runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
      const prov = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_row_owners WHERE vbucket = 2"));
      expect(prov).toHaveLength(0);
      const kept = Array.from(state.storage.sql.exec("SELECT partition_key FROM __cf_row_owners WHERE vbucket = 1"));
      expect(kept).toHaveLength(1);
    });
  });

  // Review Tier 2 #9: the checksum is computed incrementally (per-page sha256,
  // then sha256 of the page digests) so it stays O(page) memory on a large
  // vbucket. A multi-page vbucket must still produce IDENTICAL checksums on
  // two shards holding identical data, and differ under any divergence.
  it("checksum of a multi-page vbucket (> one page) matches between two shards with identical data and differs under divergence", async () => {
    const a = await freshShard();
    const b = await freshShard();
    const N = 1200; // spans 3 pages (MIGRATE_PAGE_SIZE = 500)
    for (const stub of [a, b]) {
      await createTable(stub);
      // Bulk-seed identical base rows + provenance (vbucket 5) via one CTE each.
      await stub.fetch(
        post("/execute", {
          sql: `WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < ${N}) INSERT INTO t (id, v) SELECT printf('p%05d', n), 'val' || n FROM seq`,
          requestId: `seed-rows-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      );
      await stub.fetch(
        post("/execute", {
          sql: `WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < ${N}) INSERT INTO __cf_row_owners (table_name, partition_key, tenant_id, vbucket, updated_at) SELECT 't', printf('p%05d', n), 'tenant-1', 5, '2026-01-01T00:00:00.000Z' FROM seq`,
          requestId: `seed-prov-${crypto.randomUUID()}`,
          isMutation: true,
        }),
      );
    }

    const checksumOf = async (stub: Awaited<ReturnType<typeof freshShard>>) =>
      (await (await stub.fetch(post("/migrate-checksum", { vbucket: 5, table: "t", partitionKeyColumn: "id" }))).json()) as {
        checksum: string;
        rowCount: number;
      };

    const ca = await checksumOf(a);
    const cb = await checksumOf(b);
    expect(ca.rowCount).toBe(N);
    expect(cb.rowCount).toBe(ca.rowCount);
    expect(cb.checksum).toBe(ca.checksum);

    // Diverge one row deep in the middle (page 2) — checksum must change.
    await b.fetch(post("/execute", { sql: "UPDATE t SET v = 'tampered' WHERE id = 'p00600'", requestId: `tamper-${crypto.randomUUID()}`, isMutation: true }));
    const cbTampered = await checksumOf(b);
    expect(cbTampered.checksum).not.toBe(ca.checksum);
    expect(cbTampered.rowCount).toBe(ca.rowCount); // same count, different content
  });
});

describe("ShardDO row provenance (Milestone 3, Chunk 0)", () => {
  async function createTable(stub: Awaited<ReturnType<typeof freshShard>>) {
    await stub.fetch(
      post("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `req-schema-${crypto.randomUUID()}`, isMutation: true }),
    );
  }

  async function provenanceRows(stub: Awaited<ReturnType<typeof freshShard>>, table: string, partitionKey: string) {
    return runInDurableObject(stub, async (_instance: ShardDO, state: DurableObjectState) => {
      return Array.from(
        state.storage.sql.exec(
          "SELECT table_name, partition_key, tenant_id, vbucket FROM __cf_row_owners WHERE table_name = ? AND partition_key = ?",
          table,
          partitionKey,
        ),
      ) as Array<{ table_name: string; partition_key: string; tenant_id: string; vbucket: number }>;
    });
  }

  it("an insert with full routing context writes a __cf_row_owners entry", async () => {
    const stub = await freshShard();
    await createTable(stub);

    const res = await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["row-1", "a"],
        requestId: "req-prov-1",
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "row-1",
        vbucket: 42,
      }),
    );
    expect(res.status).toBe(200);

    const rows = await provenanceRows(stub, "t", "row-1");
    expect(rows).toHaveLength(1);
    expect(rows[0].tenant_id).toBe("tenant-1");
    expect(rows[0].vbucket).toBe(42);
  });

  it("a delete with full routing context removes the __cf_row_owners entry", async () => {
    const stub = await freshShard();
    await createTable(stub);
    await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["row-2", "a"],
        requestId: "req-prov-2",
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "row-2",
        vbucket: 7,
      }),
    );
    expect(await provenanceRows(stub, "t", "row-2")).toHaveLength(1);

    await stub.fetch(
      post("/execute", {
        sql: "DELETE FROM t WHERE id = ?",
        params: ["row-2"],
        requestId: "req-prov-3",
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "row-2",
        vbucket: 7,
      }),
    );
    expect(await provenanceRows(stub, "t", "row-2")).toHaveLength(0);
  });

  it("an update with full routing context keeps a single provenance entry (no duplicate row)", async () => {
    const stub = await freshShard();
    await createTable(stub);
    await stub.fetch(
      post("/execute", {
        sql: "INSERT INTO t (id, v) VALUES (?, ?)",
        params: ["row-3", "a"],
        requestId: "req-prov-4",
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "row-3",
        vbucket: 1,
      }),
    );
    await stub.fetch(
      post("/execute", {
        sql: "UPDATE t SET v = ? WHERE id = ?",
        params: ["b", "row-3"],
        requestId: "req-prov-5",
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "row-3",
        vbucket: 1,
      }),
    );
    const rows = await provenanceRows(stub, "t", "row-3");
    expect(rows).toHaveLength(1);
    expect(rows[0].vbucket).toBe(1);
  });

  it("a write missing routing context (e.g. schema DDL) does not write provenance", async () => {
    const stub = await freshShard();
    await createTable(stub);
    const res = await stub.fetch(
      post("/execute", { sql: "INSERT INTO t (id, v) VALUES ('row-4', 'x')", requestId: "req-prov-6", isMutation: true }),
    );
    expect(res.status).toBe(200);
    expect(await provenanceRows(stub, "t", "row-4")).toHaveLength(0);
  });

  it("a mutation whose where clause matches nothing (0 rows affected) does not write provenance", async () => {
    const stub = await freshShard();
    await createTable(stub);
    const res = await stub.fetch(
      post("/execute", {
        sql: "UPDATE t SET v = ? WHERE id = ?",
        params: ["never-lands", "no-such-row"],
        requestId: "req-prov-7",
        isMutation: true,
        tenantId: "tenant-1",
        table: "t",
        partitionKey: "no-such-row",
        vbucket: 3,
      }),
    );
    expect(res.status).toBe(200);
    expect(await provenanceRows(stub, "t", "no-such-row")).toHaveLength(0);
  });

  it("a 2PC-committed base-row intent writes provenance, but a piggybacked synthetic intent without op/vbucket does not", async () => {
    const stub = await freshShard();
    await createTable(stub);

    const commitRes = await stub.fetch(
      post("/prepare", {
        coordinatorTxId: "tx-prov-1",
        intents: [
          {
            sql: "INSERT INTO t (id, v) VALUES (?, ?)",
            params: ["row-5", "a"],
            tenantId: "tenant-1",
            table: "t",
            partitionKey: "row-5",
            vbucket: 9,
            op: "insert",
          },
          {
            // Mirrors a synthetic __cf_indexes-maintenance intent: no op/vbucket.
            sql: "INSERT INTO t (id, v) VALUES (?, ?)",
            params: ["row-6", "b"],
            tenantId: "tenant-1",
            table: "t",
            partitionKey: "row-6",
          },
        ],
      }),
    );
    expect(commitRes.status).toBe(200);
    await stub.fetch(post("/commit", { coordinatorTxId: "tx-prov-1" }));

    expect(await provenanceRows(stub, "t", "row-5")).toHaveLength(1);
    expect(await provenanceRows(stub, "t", "row-6")).toHaveLength(0);
  });
});
