import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { CoordinatorDO } from "./coordinator";
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
