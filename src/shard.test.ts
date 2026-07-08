import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import type { ShardDO } from "./shard";

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
    expect(body.error).toContain("Unhandled shard error");
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
