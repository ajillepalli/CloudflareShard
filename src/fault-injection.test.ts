import { env, listDurableObjectIds, reset, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it } from "vitest";
import type { ShardDO } from "./shard";
import { FAULT_MAX_MS } from "./shard";
import { AUTH, ALL_TEST_SHARD_IDS, initCluster, post, shardExecute } from "./index.test-helpers";

// Demo-only fault-injection primitive (Shardscope chaos mode) — see
// src/shard.ts's checkFaultInjection/handleFaultInject and src/index.ts's
// /admin/fault-inject, /admin/fault-clear for the implementation these tests
// exercise. Every test here is about the SAFETY REQUIREMENTS, not just
// happy-path behavior: off-by-default, admin-gated, time-bounded, and
// non-destructive.
//
// FAULT_INJECTION_ENABLED is deliberately NOT set in vitest.config.ts's
// shared miniflare bindings (mirroring "a normal production deployment never
// sets it") — tests that need it "on" mutate `env.FAULT_INJECTION_ENABLED`
// directly and restore it in `finally`/`afterEach`, since SELF's worker runs
// in the same isolate as the test and reads the same env object per request.
afterEach(async () => {
  env.FAULT_INJECTION_ENABLED = undefined;
  await reset();
});

describe("fault injection: off by default", () => {
  it("/admin/fault-inject returns 403 when FAULT_INJECTION_ENABLED is unset, and a normal shard request is unaffected", async () => {
    expect(env.FAULT_INJECTION_ENABLED).toBeUndefined();
    await initCluster(1, 16);

    const res = await post("/admin/fault-inject", { shardId: "catalog-0-shard-0", durationMs: 5000 }, AUTH());
    expect(res.status).toBe(403);
    const body = (await res.json()) as { error: string };
    expect(body.error).toMatch(/disabled/i);

    // The endpoint is inert: no fault could possibly have been set, so a
    // normal request to that shard behaves exactly as if fault injection
    // didn't exist.
    const exec = await shardExecute("catalog-0-shard-0", "SELECT 1 AS one");
    expect(exec.status).toBe(200);
  });

  it("/admin/fault-inject returns 403 even with a value other than the exact string \"true\"", async () => {
    env.FAULT_INJECTION_ENABLED = "TRUE";
    await initCluster(1, 16);
    const res = await post("/admin/fault-inject", { shardId: "catalog-0-shard-0", durationMs: 5000 }, AUTH());
    expect(res.status).toBe(403);
  });
});

describe("fault injection: enabled behavior", () => {
  it("injecting a fault makes subsequent requests to that shard 503, and a different shard is unaffected (bounded blast radius)", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);

    const targetShardId = "catalog-0-shard-0";
    const otherShardId = ALL_TEST_SHARD_IDS.find((id) => id !== targetShardId)!;

    const injectRes = await post("/admin/fault-inject", { shardId: targetShardId, durationMs: 5000 }, AUTH());
    expect(injectRes.status).toBe(200);
    const injectBody = (await injectRes.json()) as { ok: true; mode: string; faultExpiresAt: number };
    expect(injectBody.mode).toBe("unreachable");
    expect(injectBody.faultExpiresAt).toBeGreaterThan(Date.now());

    const blocked = await shardExecute(targetShardId, "SELECT 1 AS one");
    expect(blocked.status).toBe(503);

    const unaffected = await shardExecute(otherShardId, "SELECT 1 AS one");
    expect(unaffected.status).toBe(200);
  });

  it("the 503 body clearly identifies the fault and its expiry", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    await post("/admin/fault-inject", { shardId: "catalog-0-shard-0", durationMs: 5000 }, AUTH());

    const shardStub = env.SHARD.get(env.SHARD.idFromName("catalog-0-shard-0"));
    const res = await shardStub.fetch(
      new Request("https://shard.internal/stats", { method: "POST", headers: { "content-type": "application/json" }, body: "{}" }),
    );
    expect(res.status).toBe(503);
    const body = (await res.json()) as { error: string; faultExpiresAt: number };
    expect(body.error).toBe("fault-injected: shard temporarily unreachable");
    expect(typeof body.faultExpiresAt).toBe("number");
  });

  it("/admin/fault-clear ends the fault immediately, before its natural expiry", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    const shardId = "catalog-0-shard-0";

    await post("/admin/fault-inject", { shardId, durationMs: 20000 }, AUTH());
    expect((await shardExecute(shardId, "SELECT 1 AS one")).status).toBe(503);

    const clearRes = await post("/admin/fault-clear", { shardId }, AUTH());
    expect(clearRes.status).toBe(200);

    expect((await shardExecute(shardId, "SELECT 1 AS one")).status).toBe(200);
  });
});

describe("fault injection: auto-expiry (never permanent)", () => {
  it("after the fault window elapses, the shard serves normally again with no clear call", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    const shardId = "catalog-0-shard-0";

    const injectRes = await post("/admin/fault-inject", { shardId, durationMs: 30 }, AUTH());
    expect(injectRes.status).toBe(200);

    // Immediately after injecting, the fault should be active.
    expect((await shardExecute(shardId, "SELECT 1 AS one")).status).toBe(503);

    // Wait out the (very short) fault window — no /admin/fault-clear call is
    // ever made in this test.
    await new Promise((resolve) => setTimeout(resolve, 150));

    const res = await shardExecute(shardId, "SELECT 1 AS one");
    expect(res.status).toBe(200);
  });

  it("durationMs is clamped to FAULT_MAX_MS — a request for a huge duration never exceeds the cap", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    const shardId = "catalog-0-shard-0";

    const before = Date.now();
    const res = await post("/admin/fault-inject", { shardId, durationMs: 10 * 365 * 24 * 60 * 60 * 1000 }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { faultExpiresAt: number };
    const after = Date.now();

    // The requested duration (10 years) would put faultExpiresAt far, far in
    // the future; the enforced expiry must sit within FAULT_MAX_MS of "now",
    // never anywhere close to the requested value.
    expect(body.faultExpiresAt).toBeLessThanOrEqual(after + FAULT_MAX_MS);
    expect(body.faultExpiresAt).toBeGreaterThanOrEqual(before + FAULT_MAX_MS - 1000);
  });

  it("there is no way to request an unbounded/permanent fault (omitted or non-positive durationMs still clamps to FAULT_MAX_MS)", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    const shardId = "catalog-0-shard-0";

    const before = Date.now();
    const res = await post("/admin/fault-inject", { shardId, durationMs: -1 }, AUTH());
    expect(res.status).toBe(200);
    const body = (await res.json()) as { faultExpiresAt: number };
    expect(body.faultExpiresAt).toBeLessThanOrEqual(before + FAULT_MAX_MS + 1000);
  });
});

describe("fault injection: non-destructive", () => {
  it("injecting and clearing a fault touches nothing but reachability — stored rows survive identical", async () => {
    await initCluster(1, 16);
    const shardId = "catalog-0-shard-0";

    // Write directly via the shard's /execute route (bypassing the gateway's
    // routing so we deterministically know which shard holds the rows).
    await shardExecute(shardId, "INSERT INTO events (id, v) VALUES (?, ?)", ["row-1", "alpha"]);
    await shardExecute(shardId, "INSERT INTO events (id, v) VALUES (?, ?)", ["row-2", "beta"]);
    const before = (await shardExecute(shardId, "SELECT id, v FROM events ORDER BY id ASC")).rows;
    expect(before).toEqual([
      { id: "row-1", v: "alpha" },
      { id: "row-2", v: "beta" },
    ]);

    env.FAULT_INJECTION_ENABLED = "true";
    const injectRes = await post("/admin/fault-inject", { shardId, durationMs: 5000 }, AUTH());
    expect(injectRes.status).toBe(200);
    // Confirm the fault is actually active (reads/writes rejected) before
    // asserting non-destructiveness — otherwise this test would trivially
    // pass by never having exercised the fault path at all.
    expect((await shardExecute(shardId, "SELECT 1 AS one")).status).toBe(503);

    const clearRes = await post("/admin/fault-clear", { shardId }, AUTH());
    expect(clearRes.status).toBe(200);

    const after = (await shardExecute(shardId, "SELECT id, v FROM events ORDER BY id ASC")).rows;
    expect(after).toEqual(before);
  });
});

describe("fault injection: admin-gated", () => {
  it("/admin/fault-inject rejects a request with no admin token, even when the feature is enabled", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    const res = await post("/admin/fault-inject", { shardId: "catalog-0-shard-0", durationMs: 5000 }, undefined);
    expect([401, 403]).toContain(res.status);

    // No fault should have been set.
    const exec = await shardExecute("catalog-0-shard-0", "SELECT 1 AS one");
    expect(exec.status).toBe(200);
  });

  it("/admin/fault-clear rejects a request with no admin token", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    const res = await post("/admin/fault-clear", { shardId: "catalog-0-shard-0" }, undefined);
    expect([401, 403]).toContain(res.status);
  });
});

describe("fault injection: non-destructive on cold/unknown shards (Violation 1)", () => {
  it("fault-inject on a never-registered shardId is rejected AND does not instantiate that DO's storage", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);

    const coldShardId = "totally-unknown-shard-does-not-exist-zzz";
    const coldDoId = env.SHARD.idFromName(coldShardId).toString();

    // Sanity: the cold id has no storage before we do anything.
    const idsBefore = (await listDurableObjectIds(env.SHARD)).map((id) => id.toString());
    expect(idsBefore).not.toContain(coldDoId);

    const res = await post("/admin/fault-inject", { shardId: coldShardId, durationMs: 5000 }, AUTH());
    expect(res.status).toBe(404);
    const body = (await res.json()) as { error: { code: string } };
    expect(body.error.code).toBe("UNKNOWN_SHARD");

    // The critical assertion: rejecting the unknown shard must NOT have
    // materialized its Durable Object — env.SHARD.get()/.fetch() was never
    // called on it, so it still has zero storage (constructor's alarm-write
    // never ran). A cold shard is untouched.
    const idsAfter = (await listDurableObjectIds(env.SHARD)).map((id) => id.toString());
    expect(idsAfter).not.toContain(coldDoId);
  });

  it("fault-clear on a never-registered shardId is likewise rejected without instantiation", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    const coldShardId = "another-unknown-shard-yyy";
    const coldDoId = env.SHARD.idFromName(coldShardId).toString();

    const res = await post("/admin/fault-clear", { shardId: coldShardId }, AUTH());
    expect(res.status).toBe(404);

    const idsAfter = (await listDurableObjectIds(env.SHARD)).map((id) => id.toString());
    expect(idsAfter).not.toContain(coldDoId);
  });
});

describe("fault injection: absolute outage cap across re-injection (Violation 2)", () => {
  it("repeated re-injection cannot push expiry past firstInjectedAt + FAULT_MAX_MS", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    const shardId = "catalog-0-shard-0";

    // First injection opens the window and anchors firstInjectedAt ~= now.
    const firstRes = await post("/admin/fault-inject", { shardId, durationMs: FAULT_MAX_MS }, AUTH());
    expect(firstRes.status).toBe(200);
    const firstExpiry = ((await firstRes.json()) as { faultExpiresAt: number }).faultExpiresAt;
    // The absolute ceiling: the first injection's expiry can be at most
    // FAULT_MAX_MS beyond the moment it was recorded. Measured with a
    // timestamp taken AFTER the call returned (so it's >= the DO's own
    // firstInjectedAt), the expiry must not exceed it.
    const after = Date.now();
    const absoluteCeiling = after + FAULT_MAX_MS;
    expect(firstExpiry).toBeLessThanOrEqual(absoluteCeiling);

    // Re-inject several times, each requesting the full duration again. A
    // per-call cap would push expiry forward every time; the absolute cap must
    // pin it to the SAME deadline (firstInjectedAt + FAULT_MAX_MS), so a loop
    // can never keep the shard down longer than one FAULT_MAX_MS window.
    let lastExpiry = firstExpiry;
    for (let i = 0; i < 5; i += 1) {
      await new Promise((resolve) => setTimeout(resolve, 15));
      const res = await post("/admin/fault-inject", { shardId, durationMs: FAULT_MAX_MS }, AUTH());
      expect(res.status).toBe(200);
      const expiry = ((await res.json()) as { faultExpiresAt: number }).faultExpiresAt;
      // Never advances past the original deadline (never grows), and never
      // exceeds the absolute ceiling anchored at the first injection.
      expect(expiry).toBeLessThanOrEqual(firstExpiry);
      expect(expiry).toBeLessThanOrEqual(absoluteCeiling);
      lastExpiry = expiry;
    }
    // After 5 re-injections spanning ~75ms, the deadline is still the original
    // one — total outage is bounded by the first window, not extended.
    expect(lastExpiry).toBe(firstExpiry);
  });

  it("a fresh fault AFTER the previous window fully expired starts a NEW window (firstInjectedAt resets)", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    const shardId = "catalog-0-shard-0";

    // Open a very short window and let it lapse.
    const first = await post("/admin/fault-inject", { shardId, durationMs: 30 }, AUTH());
    const firstExpiry = ((await first.json()) as { faultExpiresAt: number }).faultExpiresAt;
    await new Promise((resolve) => setTimeout(resolve, 120));
    // Confirm it actually resumed (window closed).
    expect((await shardExecute(shardId, "SELECT 1 AS one")).status).toBe(200);

    // A separate later injection anchors a fresh firstInjectedAt, so its
    // expiry is well past the first (now-lapsed) window's expiry.
    const second = await post("/admin/fault-inject", { shardId, durationMs: 5000 }, AUTH());
    const secondExpiry = ((await second.json()) as { faultExpiresAt: number }).faultExpiresAt;
    expect(secondExpiry).toBeGreaterThan(firstExpiry);
  });
});

describe("fault injection: a faulted shard does no background work (Violation 3)", () => {
  it("while faulted, an alarm tick performs no sweep/prune work, and resumes after expiry", async () => {
    env.FAULT_INJECTION_ENABLED = "true";
    await initCluster(1, 16);
    const shardId = "catalog-0-shard-0";
    const stub = env.SHARD.get(env.SHARD.idFromName(shardId));

    // Touch the shard once so ensureSchema() has created applied_requests,
    // then seed a stale row the alarm's prune step (DELETE FROM
    // applied_requests WHERE applied_at < cutoff) would normally remove.
    await shardExecute(shardId, "SELECT 1 AS one");
    const staleId = `stale-${crypto.randomUUID()}`;
    await runInDurableObject(stub, async (_i: ShardDO, state: DurableObjectState) => {
      state.storage.sql.exec(
        "INSERT INTO applied_requests (request_id, request_hash, result_json, applied_at) VALUES (?, '', '{}', ?)",
        staleId,
        "2000-01-01T00:00:00.000Z",
      );
    });

    const staleCount = async (): Promise<number> =>
      runInDurableObject(stub, async (_i: ShardDO, state: DurableObjectState) => {
        const row = Array.from(
          state.storage.sql.exec("SELECT COUNT(*) AS n FROM applied_requests WHERE request_id = ?", staleId),
        )[0] as { n: number };
        return row.n;
      });

    expect(await staleCount()).toBe(1);

    // Inject a short fault, then fire the alarm WHILE faulted. The alarm must
    // skip all work (no prune), so the stale row survives. (runInDurableObject
    // reads state directly, bypassing the fetch() 503 gate, so we can still
    // observe SQL while the shard is "unreachable".)
    const injectRes = await post("/admin/fault-inject", { shardId, durationMs: 80 }, AUTH());
    expect(injectRes.status).toBe(200);
    await runInDurableObject(stub, async (instance: ShardDO) => {
      await instance.alarm();
    });
    expect(await staleCount()).toBe(1); // untouched: alarm did NO work while faulted

    // After the fault expires, the alarm resumes normally and prunes the row.
    await new Promise((resolve) => setTimeout(resolve, 130));
    await runInDurableObject(stub, async (instance: ShardDO) => {
      await instance.alarm();
    });
    expect(await staleCount()).toBe(0); // pruned: normal alarm work resumed
  });
});
