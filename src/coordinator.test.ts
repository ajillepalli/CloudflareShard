import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { sha256Hex } from "../packages/contracts/src/index.js";
import type { CoordinatorDO, TransactionManifestService } from "./coordinator";
import type { ShardDO } from "./shard";

async function freshCoordinator() {
  const id = env.COORDINATOR.idFromName(`coordinator-${crypto.randomUUID()}`);
  return env.COORDINATOR.get(id);
}

async function freshShard(name: string) {
  const id = env.SHARD.idFromName(name);
  return env.SHARD.get(id);
}

function post(path: string, body: unknown) {
  return new Request(`https://coordinator.internal${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

function shardPost(path: string, body: unknown) {
  return new Request(`https://shard.internal${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("CoordinatorDO shell (/tx-status, route guards, error boundary)", () => {
  it("/tx-status reports found:false for an unknown txId", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/tx-status", { txId: "never-existed" }));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { found: boolean };
    expect(body.found).toBe(false);
  });

  it("/tx-status requires txId", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/tx-status", {}));
    expect(res.status).toBe(400);
  });

  it("rejects non-POST methods with 405", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(new Request("https://coordinator.internal/tx-status", { method: "GET" }));
    expect(res.status).toBe(405);
  });

  it("returns 404 for an unknown coordinator route", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/not-a-real-route", {}));
    expect(res.status).toBe(404);
  });

  it("returns a clean 500 instead of an unhandled crash on malformed JSON", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(
      new Request("https://coordinator.internal/tx-status", {
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

describe("CoordinatorDO /begin (2PC orchestration)", () => {
  async function createTable(shardName: string) {
    const stub = await freshShard(shardName);
    await stub.fetch(shardPost("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)", requestId: `req-schema-${shardName}`, isMutation: true }));
    return stub;
  }

  it("commits every participant when all shards prepare successfully", async () => {
    const txId = `tx-begin-${crypto.randomUUID()}`;
    const shardA = `shard-a-${txId}`;
    const shardB = `shard-b-${txId}`;
    await createTable(shardA);
    await createTable(shardB);

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    const res = await coordinator.fetch(
      post("/begin", {
        txId,
        participants: [
          { shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-1", "a"], tenantId: "t1", table: "t", partitionKey: "row-1" }] },
          { shardId: shardB, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-2", "b"], tenantId: "t1", table: "t", partitionKey: "row-2" }] },
        ],
      }),
    );
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; status: string };
    expect(body.ok).toBe(true);
    expect(body.status).toBe("committed");

    const stubA = await freshShard(shardA);
    const checkA = await stubA.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-1"], requestId: "check-a", isMutation: false }));
    expect(((await checkA.json()) as { rows: unknown[] }).rows).toHaveLength(1);
    const stubB = await freshShard(shardB);
    const checkB = await stubB.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-2"], requestId: "check-b", isMutation: false }));
    expect(((await checkB.json()) as { rows: unknown[] }).rows).toHaveLength(1);
  });

  it("aborts every participant and leaves no trace when one shard fails to prepare", async () => {
    const txId = `tx-begin-fail-${crypto.randomUUID()}`;
    const shardA = `shard-a-${txId}`;
    const shardB = `shard-b-${txId}`;
    await createTable(shardA);
    await createTable(shardB);

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    const res = await coordinator.fetch(
      post("/begin", {
        txId,
        participants: [
          { shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-3", "a"], tenantId: "t1", table: "t", partitionKey: "row-3" }] },
          // References a nonexistent column — prepare's SQL execution fails on this shard.
          { shardId: shardB, intents: [{ sql: "INSERT INTO t (id, nonexistent_col) VALUES (?, ?)", params: ["row-4", "boom"], tenantId: "t1", table: "t", partitionKey: "row-4" }] },
        ],
      }),
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_ABORTED");

    const stubA = await freshShard(shardA);
    const checkA = await stubA.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-3"], requestId: "check-a2", isMutation: false }));
    expect(((await checkA.json()) as { rows: unknown[] }).rows).toHaveLength(0);

    const statusRes = await coordinator.fetch(post("/tx-status", { txId }));
    const statusBody = (await statusRes.json()) as { found: boolean; status: string };
    expect(statusBody.found).toBe(true);
    expect(statusBody.status).toBe("aborted");
  });

  it("is idempotent: retrying /begin with the same txId after commit returns the committed status without re-running prepare", async () => {
    const txId = `tx-begin-idem-${crypto.randomUUID()}`;
    const shardA = `shard-a-${txId}`;
    await createTable(shardA);

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    const participants = [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-5", "a"], tenantId: "t1", table: "t", partitionKey: "row-5" }] }];

    const first = await coordinator.fetch(post("/begin", { txId, participants }));
    expect(first.status).toBe(200);

    const retry = await coordinator.fetch(post("/begin", { txId, participants }));
    expect(retry.status).toBe(200);
    const retryBody = (await retry.json()) as { status: string };
    expect(retryBody.status).toBe("committed");

    const stubA = await freshShard(shardA);
    const countRes = await stubA.fetch(shardPost("/execute", { sql: "SELECT COUNT(*) as n FROM t WHERE id = ?", params: ["row-5"], requestId: "check-count", isMutation: false }));
    const countBody = (await countRes.json()) as { rows: Array<{ n: number }> };
    expect(countBody.rows[0].n).toBe(1);
  });

  it("rejects /begin with missing txId or empty participants", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/begin", { txId: "x", participants: [] }));
    expect(res.status).toBe(400);
  });

  it("regression (Codex-found): retrying an in-flight txId with a different participant set is rejected, not silently resumed with the new data", async () => {
    const txId = `tx-mismatch-inflight-${crypto.randomUUID()}`;
    const shardA = `shard-a-${txId}`;
    await createTable(shardA);

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    // Seed a still-in-flight transaction directly (simulating a /begin call
    // that started but never finished) with one operation_json payload...
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      const originalParticipants = [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-orig", "a"], tenantId: "t1", table: "t", partitionKey: "row-orig" }] }];
      state.storage.sql.exec(
        `INSERT INTO transactions (tx_id, status, participant_shards_json, operation_json, operation_hash, created_at, updated_at) VALUES (?, 'preparing', ?, ?, 'seeded-original-hash', ?, ?)`,
        txId,
        JSON.stringify([shardA]),
        JSON.stringify(originalParticipants),
        now,
        now,
      );
    });

    // ...then retry /begin with the SAME txId but a DIFFERENT mutation set —
    // this must be rejected, not silently applied under the original txId.
    const res = await coordinator.fetch(
      post("/begin", {
        txId,
        participants: [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-different", "z"], tenantId: "t1", table: "t", partitionKey: "row-different" }] }],
      }),
    );
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_ID_REQUEST_MISMATCH");

    const stubA = await freshShard(shardA);
    const checkRes = await stubA.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-different"], requestId: "check-mismatch", isMutation: false }));
    expect(((await checkRes.json()) as { rows: unknown[] }).rows).toHaveLength(0);
  });

  it("regression (Codex-found): retrying a COMMITTED txId with a different participant set is rejected, not returned as a stale success", async () => {
    const txId = `tx-mismatch-committed-${crypto.randomUUID()}`;
    const shardA = `shard-a-${txId}`;
    await createTable(shardA);

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    const originalParticipants = [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-committed", "a"], tenantId: "t1", table: "t", partitionKey: "row-committed" }] }];
    const first = await coordinator.fetch(post("/begin", { txId, participants: originalParticipants }));
    expect(first.status).toBe(200);

    const retryWithDifferentData = await coordinator.fetch(
      post("/begin", {
        txId,
        participants: [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-different-2", "z"], tenantId: "t1", table: "t", partitionKey: "row-different-2" }] }],
      }),
    );
    expect(retryWithDifferentData.status).toBe(409);
    const body = (await retryWithDifferentData.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_ID_REQUEST_MISMATCH");
  });

  it("regression (Codex-found): a transactions table from before operation_hash existed is migrated in place, not crashed on", async () => {
    const txId = `tx-migration-${crypto.randomUUID()}`;
    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);

    // Simulate a pre-migration transactions table (no operation_hash column)
    // by creating it directly, before ensureSchema() ever runs on this DO.
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      state.storage.sql.exec(`
        CREATE TABLE transactions (
          tx_id TEXT PRIMARY KEY,
          status TEXT NOT NULL,
          participant_shards_json TEXT NOT NULL,
          operation_json TEXT NOT NULL,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          last_error TEXT
        )
      `);
      const now = new Date().toISOString();
      state.storage.sql.exec(
        "INSERT INTO transactions (tx_id, status, participant_shards_json, operation_json, created_at, updated_at) VALUES (?, 'preparing', '[]', '[]', ?, ?)",
        txId,
        now,
        now,
      );
    });

    // A /begin retry against this pre-migration row must not 500 — it should
    // degrade to the same fail-closed mismatch rejection as any other
    // content mismatch, since the backfilled operation_hash default ('')
    // never equals a real hash.
    const res = await coordinator.fetch(post("/begin", { txId, participants: [{ shardId: "irrelevant-shard", intents: [] }] }));
    expect(res.status).toBe(409);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("TX_ID_REQUEST_MISMATCH");
  });

  it("two different txIds land on two different CoordinatorDO instances and don't interfere", async () => {
    const txA = `tx-iso-a-${crypto.randomUUID()}`;
    const txB = `tx-iso-b-${crypto.randomUUID()}`;
    const idA = env.COORDINATOR.idFromName(txA);
    const idB = env.COORDINATOR.idFromName(txB);
    expect(idA.equals(idB)).toBe(false);

    const shardA = `shard-iso-${txA}`;
    await createTable(shardA);
    const coordinatorA = env.COORDINATOR.get(idA);
    await coordinatorA.fetch(
      post("/begin", { txId: txA, participants: [{ shardId: shardA, intents: [{ sql: "INSERT INTO t (id, v) VALUES (?, ?)", params: ["row-iso", "a"], tenantId: "t1", table: "t", partitionKey: "row-iso" }] }] }),
    );

    const coordinatorB = env.COORDINATOR.get(idB);
    const statusB = await coordinatorB.fetch(post("/tx-status", { txId: txB }));
    const bodyB = (await statusB.json()) as { found: boolean };
    expect(bodyB.found).toBe(false);
  });
});

describe("CoordinatorDO /force-abort", () => {
  it("aborts a stuck transaction and marks it aborted", async () => {
    const txId = `tx-force-${crypto.randomUUID()}`;
    const shardA = `shard-force-${txId}`;
    const stub = await freshShard(shardA);
    await stub.fetch(shardPost("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)", requestId: "req-schema", isMutation: true }));
    await stub.fetch(shardPost("/prepare", { coordinatorTxId: txId, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: ["row-f"], tenantId: "t1", table: "t", partitionKey: "row-f" }] }));

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        `INSERT INTO transactions (tx_id, status, participant_shards_json, operation_json, created_at, updated_at) VALUES (?, 'preparing', ?, ?, ?, ?)`,
        txId,
        JSON.stringify([shardA]),
        JSON.stringify([{ shardId: shardA, intents: [] }]),
        new Date().toISOString(),
        new Date().toISOString(),
      );
    });

    const res = await coordinator.fetch(post("/force-abort", { txId }));
    expect(res.status).toBe(200);
    const body = (await res.json()) as { ok: boolean; status: string };
    expect(body.status).toBe("aborted");

    const checkRes = await stub.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-f"], requestId: "check-f", isMutation: false }));
    expect(((await checkRes.json()) as { rows: unknown[] }).rows).toHaveLength(0);
  });

  it("returns 404 for an unknown txId", async () => {
    const stub = await freshCoordinator();
    const res = await stub.fetch(post("/force-abort", { txId: "never-existed" }));
    expect(res.status).toBe(404);
  });
});

describe("CoordinatorDO recovery sweep (alarm-driven retry of an unacknowledged commit)", () => {
  it("converges: a queued recovery row is retried by the alarm and cleared once the shard acknowledges", async () => {
    const txId = `tx-recover-${crypto.randomUUID()}`;
    const shardA = `shard-recover-${txId}`;
    const stub = await freshShard(shardA);
    await stub.fetch(shardPost("/execute", { sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)", requestId: "req-schema", isMutation: true }));
    await stub.fetch(shardPost("/prepare", { coordinatorTxId: txId, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: ["row-r"], tenantId: "t1", table: "t", partitionKey: "row-r" }] }));

    const coordinatorId = env.COORDINATOR.idFromName(txId);
    const coordinator = env.COORDINATOR.get(coordinatorId);
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO transactions (tx_id, status, participant_shards_json, operation_json, created_at, updated_at) VALUES (?, 'committed', ?, ?, ?, ?)`,
        txId,
        JSON.stringify([shardA]),
        JSON.stringify([{ shardId: shardA, intents: [] }]),
        now,
        now,
      );
      state.storage.sql.exec(
        "INSERT INTO recovery_queue (tx_id, action, next_attempt_at, attempt_count) VALUES (?, '/commit', ?, 0)",
        txId,
        now,
      );
    });

    await runInDurableObject(coordinator, async (instance: CoordinatorDO) => {
      await instance.alarm();
    });

    const checkRes = await stub.fetch(shardPost("/execute", { sql: "SELECT * FROM t WHERE id = ?", params: ["row-r"], requestId: "check-r", isMutation: false }));
    expect(((await checkRes.json()) as { rows: unknown[] }).rows).toHaveLength(1);

    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const remaining = Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId));
      expect(remaining).toHaveLength(0);
    });
  });
});

describe("CoordinatorDO protocol-0 deploy-boundary adoption", () => {
  type LegacyIntent = {
    sql: string;
    params: string[];
    tenantId: string;
    table: string;
    partitionKey: string;
  };

  async function seedLegacyPreparedParticipant(shardName: string, txId: string, intents: LegacyIntent[]) {
    const shard = await freshShard(shardName);
    await shard.fetch(shardPost("/execute", {
      sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
      requestId: `schema-${txId}`,
      isMutation: true,
    }));
    expect((await shard.fetch(shardPost("/prepare", { coordinatorTxId: txId, intents }))).status).toBe(200);
    await runInDurableObject(shard, async (_instance: ShardDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "UPDATE pending_intents SET protocol_version = 0, operation_hash = '' WHERE coordinator_tx_id = ?",
        txId,
      );
    });
    return shard;
  }

  async function seedLegacyCoordinator(
    txId: string,
    shardName: string,
    intents: LegacyIntent[],
    options: { status?: "preparing" | "prepared" | "committed" | "aborted"; queueResume?: boolean } = {},
  ) {
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    const operationJson = JSON.stringify([{ shardId: shardName, intents }]);
    const predecessorHash = await sha256Hex(operationJson);
    const status = options.status ?? "prepared";
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO transactions
          (tx_id, status, participant_shards_json, operation_json, operation_hash, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        txId,
        status,
        JSON.stringify([shardName]),
        operationJson,
        predecessorHash,
        now,
        now,
      );
      if (options.queueResume) {
        state.storage.sql.exec(
          "INSERT INTO recovery_queue (tx_id, action, next_attempt_at, attempt_count) VALUES (?, 'commit', ?, 0)",
          txId,
          now,
        );
      }
    });
    return coordinator;
  }

  it("replays exact prepare adoption before manifest registration and converges to terminal commit", async () => {
    const txId = `tx-legacy-adopt-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const intents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }];
    const shard = await seedLegacyPreparedParticipant(shardName, txId, intents);
    const coordinator = await seedLegacyCoordinator(txId, shardName, intents);

    const resumed = await coordinator.fetch(post("/begin", { txId, participants: [{ shardId: shardName, intents }] }));
    expect(resumed.status).toBe(200);
    expect(((await resumed.json()) as { status: string }).status).toBe("committed");

    const read = await shard.fetch(shardPost("/execute", {
      sql: "SELECT id FROM t WHERE id = ?",
      params: [txId],
      requestId: `read-${txId}`,
      isMutation: false,
    }));
    expect(((await read.json()) as { rows: unknown[] }).rows).toHaveLength(1);
    await runInDurableObject(shard, async (_instance: ShardDO, state: DurableObjectState) => {
      const adopted = state.storage.sql.exec<{ protocol_version: number; operation_hash: string; status: string }>(
        "SELECT protocol_version, operation_hash, status FROM pending_intents WHERE coordinator_tx_id = ?",
        txId,
      ).one();
      expect(adopted.protocol_version).toBe(1);
      expect(adopted.operation_hash).toMatch(/^[a-f0-9]{64}$/);
      expect(adopted.status).toBe("committed");
    });
  });

  it("adopts an exact predecessor preparing row without stranding its participant lock", async () => {
    const txId = `tx-legacy-preparing-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const intents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }];
    const shard = await seedLegacyPreparedParticipant(shardName, txId, intents);
    const coordinator = await seedLegacyCoordinator(txId, shardName, intents, { status: "preparing" });

    const resumed = await coordinator.fetch(post("/begin", { txId, participants: [{ shardId: shardName, intents }] }));
    expect(resumed.status).toBe(200);
    expect(((await resumed.json()) as { status: string }).status).toBe("committed");

    await runInDurableObject(shard, async (_instance: ShardDO, state: DurableObjectState) => {
      expect(Array.from(state.storage.sql.exec("SELECT * FROM row_locks WHERE coordinator_tx_id = ?", txId))).toHaveLength(0);
      const intent = state.storage.sql.exec<{ protocol_version: number; status: string }>(
        "SELECT protocol_version, status FROM pending_intents WHERE coordinator_tx_id = ?",
        txId,
      ).one();
      expect(intent).toEqual({ protocol_version: 1, status: "committed" });
    });
  });

  it.each([
    ["committed", 200, "committed", undefined],
    ["aborted", 409, undefined, "TX_ABORTED"],
  ] as const)("returns the predecessor %s terminal result for an exact replay", async (status, httpStatus, expectedStatus, expectedCode) => {
    const txId = `tx-legacy-terminal-${status}-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const intents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }];
    const coordinator = await seedLegacyCoordinator(txId, shardName, intents, { status });

    const replay = await coordinator.fetch(post("/begin", { txId, participants: [{ shardId: shardName, intents }] }));
    expect(replay.status).toBe(httpStatus);
    const body = (await replay.json()) as { status?: string; error?: { code: string } };
    expect(body.status).toBe(expectedStatus);
    expect(body.error?.code).toBe(expectedCode);
  });

  it("accepts an exact predecessor retry when its stored participant order was caller-controlled", async () => {
    const txId = `tx-legacy-order-${crypto.randomUUID()}`;
    const shardAName = `shard-a-${txId}`;
    const shardBName = `shard-b-${txId}`;
    const intentsA = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [`a-${txId}`], tenantId: "t1", table: "t", partitionKey: `a-${txId}` }];
    const intentsB = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [`b-${txId}`], tenantId: "t1", table: "t", partitionKey: `b-${txId}` }];
    const shardA = await seedLegacyPreparedParticipant(shardAName, txId, intentsA);
    const shardB = await seedLegacyPreparedParticipant(shardBName, txId, intentsB);
    const storedParticipants = [
      { shardId: shardBName, intents: intentsB },
      { shardId: shardAName, intents: intentsA },
    ];
    const operationJson = JSON.stringify(storedParticipants);
    const predecessorHash = await sha256Hex(operationJson);
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    await coordinator.fetch(post("/tx-status", { txId: "schema-warmup" }));
    await runInDurableObject(coordinator, async (_instance: CoordinatorDO, state: DurableObjectState) => {
      const now = new Date().toISOString();
      state.storage.sql.exec(
        `INSERT INTO transactions
          (tx_id, status, participant_shards_json, operation_json, operation_hash, created_at, updated_at)
         VALUES (?, 'prepared', ?, ?, ?, ?, ?)`,
        txId,
        JSON.stringify([shardBName, shardAName]),
        operationJson,
        predecessorHash,
        now,
        now,
      );
    });

    // The gateway normalizes incoming participants to A,B. The legacy digest
    // must still be verified against the original stored B,A serialization.
    const resumed = await coordinator.fetch(post("/begin", { txId, participants: storedParticipants }));
    expect(resumed.status).toBe(200);
    expect(((await resumed.json()) as { status: string }).status).toBe("committed");

    for (const [shard, id] of [[shardA, `a-${txId}`], [shardB, `b-${txId}`]] as const) {
      const read = await shard.fetch(shardPost("/execute", {
        sql: "SELECT id FROM t WHERE id = ?",
        params: [id],
        requestId: `read-${id}`,
        isMutation: false,
      }));
      expect(((await read.json()) as { rows: unknown[] }).rows).toHaveLength(1);
    }
  });

  it("alarm resume rejects mismatched stored participant identity and aborts without committing", async () => {
    const txId = `tx-legacy-mismatch-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const coordinatorIntents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [`expected-${txId}`], tenantId: "t1", table: "t", partitionKey: `expected-${txId}` }];
    const participantIntents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [`different-${txId}`], tenantId: "t1", table: "t", partitionKey: `different-${txId}` }];
    const shard = await seedLegacyPreparedParticipant(shardName, txId, participantIntents);
    const coordinator = await seedLegacyCoordinator(txId, shardName, coordinatorIntents, { queueResume: true });

    await runInDurableObject(coordinator, async (instance: CoordinatorDO) => {
      await instance.alarm();
    });
    const status = await coordinator.fetch(post("/tx-status", { txId }));
    expect(((await status.json()) as { status: string }).status).toBe("aborted");

    await runInDurableObject(shard, async (_instance: ShardDO, state: DurableObjectState) => {
      expect(Array.from(state.storage.sql.exec("SELECT * FROM pending_intents WHERE coordinator_tx_id = ?", txId))).toHaveLength(0);
      expect(state.storage.sql.exec<{ decision: string }>("SELECT decision FROM participant_decision_tombstones WHERE tx_id = ?", txId).one().decision).toBe("abort");
    });
  });

  it("force-aborts an unadopted prepared row and its abort tombstone blocks a later commit", async () => {
    const txId = `tx-legacy-force-abort-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const intents = [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }];
    const shard = await seedLegacyPreparedParticipant(shardName, txId, intents);
    const coordinator = await seedLegacyCoordinator(txId, shardName, intents);

    const forced = await coordinator.fetch(post("/force-abort", { txId }));
    expect(forced.status).toBe(200);
    expect(((await forced.json()) as { status: string }).status).toBe("aborted");
    await runInDurableObject(shard, async (_instance: ShardDO, state: DurableObjectState) => {
      expect(Array.from(state.storage.sql.exec("SELECT * FROM pending_intents WHERE coordinator_tx_id = ?", txId))).toHaveLength(0);
      expect(Array.from(state.storage.sql.exec("SELECT * FROM row_locks WHERE coordinator_tx_id = ?", txId))).toHaveLength(0);
    });

    const lateCommit = await shard.fetch(shardPost("/commit", { coordinatorTxId: txId }));
    expect(lateCommit.status).toBe(409);
    expect(((await lateCommit.json()) as { error: { code: string } }).error.code).toBe("TX_DECISION_CONFLICT");
  });
});

describe("CoordinatorDO manifest admission and lifecycle", () => {
  it("opens after three admission failures, permits one half-open probe, and resets on success", async () => {
    const txId = `tx-circuit-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const shard = await freshShard(shardName);
    await shard.fetch(shardPost("/execute", {
      sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
      requestId: `schema-${txId}`,
      isMutation: true,
    }));
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      let failAdmission = true;
      let admissionCalls = 0;
      const service: TransactionManifestService = {
        async checkManifestAdmission() {
          admissionCalls += 1;
          return failAdmission
            ? {
                ok: false as const,
                status: "unavailable" as const,
                http_status: 503 as const,
                error: { schema_version: 1 as const, code: "TX_MANIFEST_UNAVAILABLE" as const, message: "test outage", http_status: 503, retryable: true },
                circuit: { count_toward_open: true as const, failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const },
              }
            : { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;

      const begin = () => instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: shardName, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      for (let attempt = 0; attempt < 3; attempt += 1) expect((await begin()).status).toBe(503);
      expect(admissionCalls).toBe(3);
      expect((await begin()).status).toBe(503);
      expect(admissionCalls, "open circuit must reject without another RPC").toBe(3);

      const opened = state.storage.sql.exec<{ open_until_ms: number; open_count: number }>(
        "SELECT open_until_ms, open_count FROM manifest_admission_circuit WHERE singleton = 1",
      ).one();
      expect(opened.open_count).toBe(1);
      expect(opened.open_until_ms).toBeGreaterThan(Date.now());

      state.storage.sql.exec("UPDATE manifest_admission_circuit SET open_until_ms = ? WHERE singleton = 1", Date.now() - 1);
      failAdmission = false;
      expect((await begin()).status).toBe(200);
      expect(admissionCalls).toBe(4);
      const reset = state.storage.sql.exec<{ failure_count: number; open_until_ms: number; open_count: number; half_open_probe: number }>(
        "SELECT failure_count, open_until_ms, open_count, half_open_probe FROM manifest_admission_circuit WHERE singleton = 1",
      ).one();
      expect(reset).toEqual({ failure_count: 0, open_until_ms: 0, open_count: 0, half_open_probe: 0 });
    });
  });

  it("keeps participants prepared while manifest registration is ambiguous, grows durable backoff, and converges by alarm", async () => {
    const txId = `tx-manifest-ambiguous-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const shard = await freshShard(shardName);
    await shard.fetch(shardPost("/execute", {
      sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
      requestId: `schema-${txId}`,
      isMutation: true,
    }));
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));
    let registrations = 0;
    const service: TransactionManifestService = {
      async checkManifestAdmission() {
        return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
      },
      async registerManifest(registration) {
        registrations += 1;
        if (registrations <= 2) throw new Error("ambiguous registration transport failure");
        return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
      },
      async releaseManifestRetention() {
        return { ok: true as const, status: "released" as const };
      },
    };

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;

      const begin = await instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: shardName, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect(begin.status).toBe(202);
      expect(((await begin.json()) as { status: string }).status).toBe("commit_pending_manifest");
      expect(registrations).toBe(1);

      expect(state.storage.sql.exec<{ attempt_count: number }>(
        "SELECT attempt_count FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one().attempt_count).toBe(1);
    });

    const beforeManifest = await shard.fetch(shardPost("/execute", {
      sql: "SELECT id FROM t WHERE id = ?",
      params: [txId],
      requestId: `read-before-manifest-${txId}`,
      isMutation: false,
    }));
    expect(((await beforeManifest.json()) as { rows: unknown[] }).rows).toHaveLength(0);

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;
      state.storage.sql.exec("UPDATE recovery_queue SET next_attempt_at = ? WHERE tx_id = ?", new Date(0).toISOString(), txId);
      await instance.alarm();
      expect(registrations).toBe(2);
      const secondRetry = state.storage.sql.exec<{ attempt_count: number; next_attempt_at: string }>(
        "SELECT attempt_count, next_attempt_at FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one();
      expect(secondRetry.attempt_count).toBe(2);
      expect(new Date(secondRetry.next_attempt_at).getTime() - Date.now()).toBeGreaterThanOrEqual(9_000);
    });

    const stillUncommitted = await shard.fetch(shardPost("/execute", {
      sql: "SELECT id FROM t WHERE id = ?",
      params: [txId],
      requestId: `read-second-ambiguity-${txId}`,
      isMutation: false,
    }));
    expect(((await stillUncommitted.json()) as { rows: unknown[] }).rows).toHaveLength(0);

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;
      state.storage.sql.exec("UPDATE recovery_queue SET next_attempt_at = ? WHERE tx_id = ?", new Date(0).toISOString(), txId);
      await instance.alarm();
      expect(registrations).toBe(3);
      const status = await instance.fetch(post("/tx-status", { txId }));
      expect(((await status.json()) as { status: string }).status).toBe("committed");
      expect(state.storage.sql.exec<{ action: string; attempt_count: number }>(
        "SELECT action, attempt_count FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one()).toEqual({ action: "release", attempt_count: 0 });
    });

    const committed = await shard.fetch(shardPost("/execute", {
      sql: "SELECT id FROM t WHERE id = ?",
      params: [txId],
      requestId: `read-committed-${txId}`,
      isMutation: false,
    }));
    expect(((await committed.json()) as { rows: unknown[] }).rows).toHaveLength(1);

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;
      state.storage.sql.exec("UPDATE recovery_queue SET next_attempt_at = ? WHERE tx_id = ?", new Date(0).toISOString(), txId);
      await instance.alarm();
      expect(Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId))).toHaveLength(0);
    });
  });

  it("quarantines deterministic manifest rejection and removes nonretryable recovery work", async () => {
    const txId = `tx-manifest-rejected-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const shard = await freshShard(shardName);
    await shard.fetch(shardPost("/execute", {
      sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
      requestId: `schema-${txId}`,
      isMutation: true,
    }));
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      const service: TransactionManifestService = {
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest() {
          return {
            ok: false as const,
            status: "rejected" as const,
            http_status: 503,
            error: { schema_version: 1 as const, code: "TX_VERSION_UNSUPPORTED" as const, message: "unsupported manifest version", http_status: 503, retryable: false },
          };
        },
        async releaseManifestRetention() {
          return { ok: true as const, status: "released" as const };
        },
      };
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;

      const begin = await instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: shardName, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect(begin.status).toBe(503);

      const durable = state.storage.sql.exec<{ status: string; decision: string; last_error: string }>(
        "SELECT status, decision, last_error FROM transactions WHERE tx_id = ?",
        txId,
      ).one();
      expect(durable.status).toBe("quarantined");
      expect(durable.decision).toBe("quarantined");
      expect(JSON.parse(durable.last_error)).toMatchObject({ code: "TX_VERSION_UNSUPPORTED" });
      expect(Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId))).toHaveLength(0);

      await instance.alarm();
      expect(Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId))).toHaveLength(0);
      const replay = await instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: shardName, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect(replay.status).toBe(409);
      expect(((await replay.json()) as { error: { code: string } }).error.code).toBe("TX_QUARANTINED");
    });
  });

  it("releases retention only after terminal commit and durably retries an ambiguous release", async () => {
    const txId = `tx-release-${crypto.randomUUID()}`;
    const shardName = `shard-${txId}`;
    const shard = await freshShard(shardName);
    await shard.fetch(shardPost("/execute", {
      sql: "CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)",
      requestId: `schema-${txId}`,
      isMutation: true,
    }));
    const coordinator = env.COORDINATOR.get(env.COORDINATOR.idFromName(txId));

    await runInDurableObject(coordinator, async (instance: CoordinatorDO, state: DurableObjectState) => {
      let releases = 0;
      const service: TransactionManifestService = {
        async checkManifestAdmission() {
          return { ok: true as const, status: "ready" as const, circuit_policy: { failure_threshold: 3 as const, failure_window_ms: 30_000 as const, maximum_cooldown_ms: 300_000 as const } };
        },
        async registerManifest(registration) {
          return { ok: true as const, status: "registered" as const, http_status: 200 as const, record_hash: registration.record_hash, quarantined: false as const };
        },
        async releaseManifestRetention() {
          releases += 1;
          if (releases === 1) throw new Error("ambiguous transport failure");
          return { ok: true as const, status: "released" as const };
        },
      };
      (instance as unknown as { coordinatorEnv: { CONTROL_PLANE?: TransactionManifestService } }).coordinatorEnv.CONTROL_PLANE = service;

      const committed = await instance.fetch(post("/begin", {
        txId,
        participants: [{ shardId: shardName, intents: [{ sql: "INSERT INTO t (id) VALUES (?)", params: [txId], tenantId: "t1", table: "t", partitionKey: txId }] }],
      }));
      expect(committed.status).toBe(200);
      expect(releases, "release must not run before terminal participant acknowledgements").toBe(0);
      expect(state.storage.sql.exec<{ action: string }>("SELECT action FROM recovery_queue WHERE tx_id = ?", txId).one().action).toBe("release");

      await instance.alarm();
      expect(releases).toBe(1);
      const retry = state.storage.sql.exec<{ action: string; attempt_count: number }>(
        "SELECT action, attempt_count FROM recovery_queue WHERE tx_id = ?",
        txId,
      ).one();
      expect(retry.action).toBe("release");
      expect(retry.attempt_count).toBe(1);

      state.storage.sql.exec("UPDATE recovery_queue SET next_attempt_at = ? WHERE tx_id = ?", new Date(0).toISOString(), txId);
      await instance.alarm();
      expect(releases).toBe(2);
      expect(Array.from(state.storage.sql.exec("SELECT * FROM recovery_queue WHERE tx_id = ?", txId))).toHaveLength(0);
    });
  });
});
