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
