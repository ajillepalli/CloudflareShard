import { describe, expect, it, vi } from "vitest";
import * as chaosModule from "./chaos";
import {
  CHAOS_ATTACKS,
  ChaosPreconditionError,
  classifyBlipShardOfflineFire,
  classifyBlipFaultInjectError,
  classifyDoubleSubmit,
  classifyMismatchedReplay,
  parseAbortMigrationInput,
  parseBlipShardOfflineInput,
  parseDoubleSubmitInput,
  parseDrainHotNodeInput,
  parseHotShardOverride,
  parseHotVbucketOverride,
  parseMigrateHotVbucketInput,
  parseMismatchedReplayInput,
  parseSplitHotVbucketInput,
  pickBlipShardTarget,
  pickHotShardTarget,
  pickHotVbucketTarget,
  pickInFlightMigrationTarget,
  runBlipShardOfflineAttack,
  type BlipShardTarget,
  type LoadStatusLike,
  type VbucketMapLike,
} from "./chaos";
import type { Env } from "./env";

// ============================================================================
// double-submit classification — the ONLY thing that decides `survived` is
// the row's real before/after value (delta === 1), never the two calls'
// response bodies alone (the /v1/mutate wire format doesn't expose
// `duplicated` — see chaos.ts's header comment).
// ============================================================================

describe("chaos.ts — classifyDoubleSubmit", () => {
  const base = {
    tenantId: "tpcc-w0001",
    partitionKey: "s-0001-000001",
    requestId: "req-123",
    originalQty: 50,
  };

  it("survived=deduped when exactly one decrement landed and both calls reported the SAME cached success", () => {
    const outcome = classifyDoubleSubmit({
      ...base,
      callAStatus: 200,
      callARowsAffected: 1,
      callBStatus: 200,
      callBRowsAffected: 1,
      finalQty: 49,
    });
    expect(outcome.attack).toBe("double-submit");
    expect(outcome.survived).toBe(true);
    expect(outcome.note).toMatch(/deduped/i);
    expect(outcome.observed).toContain("49");
  });

  it("survived=true (no double effect) even when the second call's own CAS guard (not idempotency) blocked it", () => {
    const outcome = classifyDoubleSubmit({
      ...base,
      callAStatus: 200,
      callARowsAffected: 1,
      callBStatus: 200,
      callBRowsAffected: 0, // CAS guard caught it live, not a cached replay
      finalQty: 49,
    });
    expect(outcome.survived).toBe(true);
    expect(outcome.note).toMatch(/compare-and-swap guard/i);
  });

  it("survived=false when BOTH submissions actually applied (a genuine double-write)", () => {
    const outcome = classifyDoubleSubmit({
      ...base,
      callAStatus: 200,
      callARowsAffected: 1,
      callBStatus: 200,
      callBRowsAffected: 1,
      finalQty: 48, // decremented twice
    });
    expect(outcome.survived).toBe(false);
    expect(outcome.note).toMatch(/BOTH submissions applied/);
  });

  it("survived=false when neither submission's effect stuck", () => {
    const outcome = classifyDoubleSubmit({
      ...base,
      callAStatus: 500,
      callARowsAffected: null,
      callBStatus: 500,
      callBRowsAffected: null,
      finalQty: 50,
    });
    expect(outcome.survived).toBe(false);
    expect(outcome.note).toMatch(/NEITHER submission/);
  });
});

// ============================================================================
// mismatched-replay classification — THE spec requirement: this must be
// classified as survived=correctly-rejected, NOT a loss, when (and only
// when) the gateway's 409 carries the EXACT src/shard.ts contract string AND
// the row shows only the first write's effect.
// ============================================================================

describe("chaos.ts — classifyMismatchedReplay", () => {
  const base = {
    tenantId: "tpcc-w0001",
    partitionKey: "s-0001-000001",
    requestId: "req-456",
    originalQty: 50,
    firstStatus: 200,
    firstRowsAffected: 1,
  };

  it("survived=true (correctly rejected) on the exact src/shard.ts contract string + an untouched-by-replay row", () => {
    const outcome = classifyMismatchedReplay({
      ...base,
      secondStatus: 409,
      secondErrorMessage: "requestId was already used with different sql/params — refusing to replay a mismatched result.",
      finalQty: 49, // only the FIRST write's effect persisted
    });
    expect(outcome.attack).toBe("mismatched-replay");
    expect(outcome.survived).toBe(true);
    expect(outcome.note).toMatch(/Correctly rejected/);
  });

  it("survived=false when the replay is silently ACCEPTED instead of rejected — a genuine correctness bug, not a loss classification bug", () => {
    const outcome = classifyMismatchedReplay({
      ...base,
      secondStatus: 200,
      secondErrorMessage: null,
      finalQty: 43, // the mismatched second write actually applied
    });
    expect(outcome.survived).toBe(false);
    expect(outcome.note).toMatch(/ACCEPTED instead of rejected/);
  });

  it("survived=false when rejected, but with a DIFFERENT 409 (not the mismatch-hash contract) — e.g. a lock/fence 409", () => {
    const outcome = classifyMismatchedReplay({
      ...base,
      secondStatus: 409,
      secondErrorMessage: "This row is locked by an in-flight coordinated transaction.",
      finalQty: 49,
    });
    expect(outcome.survived).toBe(false);
    expect(outcome.note).toMatch(/different 409/);
  });

  it("survived=false when correctly rejected but the row shows unexpected drift (partial application)", () => {
    const outcome = classifyMismatchedReplay({
      ...base,
      secondStatus: 409,
      secondErrorMessage: "requestId was already used with different sql/params — refusing to replay a mismatched result.",
      finalQty: 43, // doesn't match the first-write-only expectation
    });
    expect(outcome.survived).toBe(false);
    expect(outcome.note).toMatch(/doesn't match the first write's expected effect/);
  });

  it("never classifies a correct rejection as a loss — the exact NOT-a-loss requirement", () => {
    const outcome = classifyMismatchedReplay({
      ...base,
      secondStatus: 409,
      secondErrorMessage: "requestId was already used with different sql/params — refusing to replay a mismatched result.",
      finalQty: 49,
    });
    // "survived" here IS the "not a loss" signal this module reports —
    // there is no separate loss counter in a ChaosOutcome to conflate it
    // with (that's ./load/correctness.ts's job, deliberately not
    // duplicated here).
    expect(outcome.survived).toBe(true);
  });
});

// ============================================================================
// Request/target shaping — parse*() input coercion.
// ============================================================================

describe("chaos.ts — parse*() input shaping", () => {
  it("parseDoubleSubmitInput / parseMismatchedReplayInput default warehouseId/itemId to 1 when omitted", () => {
    expect(parseDoubleSubmitInput({})).toEqual({ warehouseId: 1, itemId: 1 });
    expect(parseMismatchedReplayInput(null)).toEqual({ warehouseId: 1, itemId: 1 });
    expect(parseMismatchedReplayInput(undefined)).toEqual({ warehouseId: 1, itemId: 1 });
  });

  it("parseDoubleSubmitInput honors explicit positive integers, ignoring garbage", () => {
    expect(parseDoubleSubmitInput({ warehouseId: 3, itemId: 42 })).toEqual({ warehouseId: 3, itemId: 42 });
    expect(parseDoubleSubmitInput({ warehouseId: -1, itemId: 1.5 })).toEqual({ warehouseId: 1, itemId: 1 });
    expect(parseDoubleSubmitInput({ warehouseId: "not-a-number" })).toEqual({ warehouseId: 1, itemId: 1 });
  });

  it("parseHotShardOverride / parseHotVbucketOverride only accept non-empty strings and non-negative integers", () => {
    expect(parseHotShardOverride({ catalogShardId: "catalog-0", shardId: "shard-1" })).toEqual({
      catalogShardId: "catalog-0",
      shardId: "shard-1",
    });
    expect(parseHotShardOverride({})).toEqual({ catalogShardId: undefined, shardId: undefined });
    expect(parseHotVbucketOverride({ catalogShardId: "catalog-0", shardId: "shard-1", vbucket: 5 })).toEqual({
      catalogShardId: "catalog-0",
      shardId: "shard-1",
      vbucket: 5,
    });
    expect(parseHotVbucketOverride({ vbucket: -1 }).vbucket).toBeUndefined();
  });

  it("parseSplitHotVbucketInput / parseMigrateHotVbucketInput carry their own extra optional field through", () => {
    expect(parseSplitHotVbucketInput({ newShardId: "shard-9" }).newShardId).toBe("shard-9");
    expect(parseMigrateHotVbucketInput({ targetShardId: "shard-7" }).targetShardId).toBe("shard-7");
  });

  it("parseAbortMigrationInput / parseDrainHotNodeInput default to an empty override (auto-detect)", () => {
    expect(parseAbortMigrationInput({})).toEqual({ catalogShardId: undefined, vbucket: undefined });
    expect(parseDrainHotNodeInput({})).toEqual({ catalogShardId: undefined, shardId: undefined });
  });
});

// ============================================================================
// Target resolution (pickHotShardTarget / pickHotVbucketTarget /
// pickInFlightMigrationTarget) — pure functions over plain data, no fetch,
// no DO binding, no live cluster.
// ============================================================================

function vbucketMap(rows: Array<{ catalogShardId: string; vbucket: number; shardId: string; migrationStatus?: string }>): VbucketMapLike {
  const byCatalog = new Map<string, Array<{ vbucket: number; shardId: string; migrationStatus: string }>>();
  for (const r of rows) {
    const bucket = byCatalog.get(r.catalogShardId) ?? [];
    bucket.push({ vbucket: r.vbucket, shardId: r.shardId, migrationStatus: r.migrationStatus ?? "none" });
    byCatalog.set(r.catalogShardId, bucket);
  }
  return { catalogs: [...byCatalog.entries()].map(([catalogShardId, map]) => ({ catalogShardId, totalVBuckets: map.length, map })) };
}

describe("chaos.ts — pickHotShardTarget", () => {
  const map = vbucketMap([
    { catalogShardId: "catalog-0", vbucket: 0, shardId: "shard-1" },
    { catalogShardId: "catalog-0", vbucket: 1, shardId: "shard-2" },
    { catalogShardId: "catalog-1", vbucket: 0, shardId: "shard-3" },
  ]);

  it("an explicit override wins outright, without needing skew load running", () => {
    const idle: LoadStatusLike = { running: false, config: null };
    expect(pickHotShardTarget(idle, map, { catalogShardId: "catalog-9", shardId: "shard-9" })).toEqual({
      catalogShardId: "catalog-9",
      shardId: "shard-9",
    });
  });

  it("auto-resolves the catalog owning the load driver's skew targetShardId", () => {
    const running: LoadStatusLike = { running: true, config: { mode: "skew", targetShardId: "shard-2", baseUrl: "https://x" } };
    expect(pickHotShardTarget(running, map)).toEqual({ catalogShardId: "catalog-0", shardId: "shard-2" });
  });

  it("throws ChaosPreconditionError when no skew load is running and no override is given", () => {
    const idle: LoadStatusLike = { running: false, config: null };
    expect(() => pickHotShardTarget(idle, map)).toThrow(ChaosPreconditionError);

    const uniform: LoadStatusLike = { running: true, config: { mode: "uniform", targetShardId: null, baseUrl: "https://x" } };
    expect(() => pickHotShardTarget(uniform, map)).toThrow(ChaosPreconditionError);
  });

  it("throws ChaosPreconditionError when the skew target shard owns nothing in any live catalog", () => {
    const running: LoadStatusLike = { running: true, config: { mode: "skew", targetShardId: "shard-does-not-exist", baseUrl: "https://x" } };
    expect(() => pickHotShardTarget(running, map)).toThrow(ChaosPreconditionError);
  });
});

describe("chaos.ts — pickHotVbucketTarget", () => {
  const map = vbucketMap([
    { catalogShardId: "catalog-0", vbucket: 5, shardId: "shard-2" },
    { catalogShardId: "catalog-0", vbucket: 2, shardId: "shard-2" },
    { catalogShardId: "catalog-0", vbucket: 0, shardId: "shard-1" },
  ]);
  const running: LoadStatusLike = { running: true, config: { mode: "skew", targetShardId: "shard-2", baseUrl: "https://x" } };

  it("picks the lowest-numbered vBucket the resolved hot shard currently owns — deterministic", () => {
    expect(pickHotVbucketTarget(running, map)).toEqual({ catalogShardId: "catalog-0", shardId: "shard-2", vbucket: 2 });
  });

  it("a full explicit override (catalogShardId + shardId + vbucket) wins outright", () => {
    const idle: LoadStatusLike = { running: false, config: null };
    expect(pickHotVbucketTarget(idle, map, { catalogShardId: "catalog-9", shardId: "shard-9", vbucket: 42 })).toEqual({
      catalogShardId: "catalog-9",
      shardId: "shard-9",
      vbucket: 42,
    });
  });

  it("throws ChaosPreconditionError when the hot shard owns no vBuckets in its catalog", () => {
    const emptyMap = vbucketMap([{ catalogShardId: "catalog-0", vbucket: 0, shardId: "shard-1" }]);
    expect(() => pickHotVbucketTarget(running, emptyMap)).toThrow(ChaosPreconditionError);
  });
});

describe("chaos.ts — pickInFlightMigrationTarget", () => {
  it("finds the first row anywhere in the live map that isn't migrationStatus 'none'", () => {
    const map = vbucketMap([
      { catalogShardId: "catalog-0", vbucket: 0, shardId: "shard-1", migrationStatus: "none" },
      { catalogShardId: "catalog-0", vbucket: 1, shardId: "shard-2", migrationStatus: "backfilling" },
      { catalogShardId: "catalog-1", vbucket: 0, shardId: "shard-3", migrationStatus: "cutover" },
    ]);
    expect(pickInFlightMigrationTarget(map)).toEqual({ catalogShardId: "catalog-0", vbucket: 1, shardId: "shard-2" });
  });

  it("an explicit {catalogShardId, vbucket} override wins outright", () => {
    const map = vbucketMap([{ catalogShardId: "catalog-0", vbucket: 0, shardId: "shard-1", migrationStatus: "none" }]);
    expect(pickInFlightMigrationTarget(map, { catalogShardId: "catalog-5", vbucket: 9 })).toEqual({
      catalogShardId: "catalog-5",
      vbucket: 9,
      shardId: "unknown",
    });
  });

  it("throws ChaosPreconditionError when nothing is migrating anywhere and no override is given", () => {
    const map = vbucketMap([
      { catalogShardId: "catalog-0", vbucket: 0, shardId: "shard-1", migrationStatus: "none" },
      { catalogShardId: "catalog-1", vbucket: 0, shardId: "shard-3", migrationStatus: "none" },
    ]);
    expect(() => pickInFlightMigrationTarget(map)).toThrow(ChaosPreconditionError);
  });
});

// ============================================================================
// blip-shard-offline is now REAL — wired against the core's admin-gated
// fault-injection primitive (env.SHARD_API.adminFaultInject). Every attack
// this module wires has a runXxxAttack function, including this one.
// ============================================================================

describe("chaos.ts — blip-shard-offline is wired like every other attack", () => {
  it("blip-shard-offline is among the attacks this module actually wires", () => {
    expect((CHAOS_ATTACKS as readonly string[]).includes("blip-shard-offline")).toBe(true);
  });

  it("this module exports a run function for every wired attack, including blip-shard-offline", () => {
    const exportsOfChaosModule = Object.keys(chaosModule);
    const looksLikeARunner = (name: string) => /^run.*Attack$/.test(name);
    const runnerNames = exportsOfChaosModule.filter(looksLikeARunner);
    expect(runnerNames.some((n) => n.toLowerCase().includes("blip"))).toBe(true);
    expect(runnerNames.length).toBe(CHAOS_ATTACKS.length);
  });
});

// ============================================================================
// pickBlipShardTarget — target selection must avoid a shard that's currently
// the source OR target of an in-flight migration, so the clean "shard drops,
// cluster holds, shard recovers" story isn't muddied by the also-by-design
// "blipping a mid-reshard shard parks the topology op" interaction. Pure,
// no live cluster.
// ============================================================================

describe("chaos.ts — pickBlipShardTarget", () => {
  it("auto-picks the lowest shardId that is NOT currently mid-migration, skipping one that is", () => {
    const map = vbucketMap([
      { catalogShardId: "catalog-0", vbucket: 0, shardId: "shard-1", migrationStatus: "backfilling" },
      { catalogShardId: "catalog-0", vbucket: 1, shardId: "shard-2" },
      { catalogShardId: "catalog-0", vbucket: 2, shardId: "shard-3" },
    ]);
    const target = pickBlipShardTarget(map);
    expect(target).toEqual({ catalogShardId: "catalog-0", shardId: "shard-2", isMigrating: false });
  });

  it("also avoids a shard that is only a migration's TARGET (targetShardId), not just its source", () => {
    const map: VbucketMapLike = {
      catalogs: [
        {
          catalogShardId: "catalog-0",
          totalVBuckets: 2,
          map: [
            { vbucket: 0, shardId: "shard-1", migrationStatus: "backfilling", targetShardId: "shard-2" },
            { vbucket: 1, shardId: "shard-3", migrationStatus: "none" },
          ],
        },
      ],
    };
    const target = pickBlipShardTarget(map);
    // shard-1 (source) and shard-2 (target) are both mid-migration; only
    // shard-3 is clean.
    expect(target).toEqual({ catalogShardId: "catalog-0", shardId: "shard-3", isMigrating: false });
  });

  it("an explicit override wins outright, but still reports isMigrating honestly when the chosen target IS mid-migration", () => {
    const map = vbucketMap([{ catalogShardId: "catalog-0", vbucket: 0, shardId: "shard-1", migrationStatus: "cutover" }]);
    const target = pickBlipShardTarget(map, { catalogShardId: "catalog-0", shardId: "shard-1" });
    expect(target).toEqual({ catalogShardId: "catalog-0", shardId: "shard-1", isMigrating: true });
  });

  it("falls back to a migrating shard (isMigrating: true) rather than refusing, when every known shard is mid-migration", () => {
    const map = vbucketMap([
      { catalogShardId: "catalog-0", vbucket: 0, shardId: "shard-2", migrationStatus: "backfilling" },
      { catalogShardId: "catalog-0", vbucket: 1, shardId: "shard-1", migrationStatus: "cutover" },
    ]);
    const target = pickBlipShardTarget(map);
    expect(target.isMigrating).toBe(true);
    expect(target.shardId).toBe("shard-1"); // deterministic: lowest shardId even in the all-migrating fallback
  });

  it("throws ChaosPreconditionError when the live map has no shards at all and no override is given", () => {
    const empty: VbucketMapLike = { catalogs: [] };
    expect(() => pickBlipShardTarget(empty)).toThrow(ChaosPreconditionError);
  });
});

// ============================================================================
// classifyBlipFaultInjectError — a 403 (flag off) or 404 (unknown shard)
// from the core must be classified into a specific, honest message — never
// left as a generic/opaque failure. Pure, no live cluster, no mocked
// SHARD_API.
// ============================================================================

describe("chaos.ts — classifyBlipFaultInjectError", () => {
  const target: BlipShardTarget = { catalogShardId: "catalog-0", shardId: "shard-1", isMigrating: false };

  it("classifies the core's disabled-flag 403 message into a clear 'needs FAULT_INJECTION_ENABLED' explanation", () => {
    const message =
      'CloudflareShard RPC error 403: {"error":"Fault injection is disabled. Set FAULT_INJECTION_ENABLED=\\"true\\" to enable this demo-only endpoint — never in production."}';
    const result = classifyBlipFaultInjectError(message, target);
    expect(result).not.toBeNull();
    expect(result).toMatch(/FAULT_INJECTION_ENABLED/);
    expect(result).toMatch(/cloudflare-shard-mvp/);
    expect(result).toMatch(/intentional/i);
  });

  it("classifies the core's UNKNOWN_SHARD 404 message into a clear, target-specific explanation", () => {
    const message =
      'CloudflareShard RPC error 404: {"error":{"code":"UNKNOWN_SHARD","message":"Shard shard-1 is not a currently-known shard.","fix":"..."}}';
    const result = classifyBlipFaultInjectError(message, target);
    expect(result).not.toBeNull();
    expect(result).toMatch(/shard-1/);
    expect(result).toMatch(/not a currently-known shard/);
  });

  it("returns null for an unrelated error (e.g. a 409 lock-busy or a 500), letting it propagate unchanged", () => {
    expect(classifyBlipFaultInjectError('CloudflareShard RPC error 409: {"error":"MIGRATION_IN_PROGRESS"}', target)).toBeNull();
    expect(classifyBlipFaultInjectError("some unrelated network error", target)).toBeNull();
  });
});

describe("chaos.ts — runBlipShardOfflineAttack surfaces a 403 as a calm precondition, never a generic failure or a false ✗ broke", () => {
  function fakeEnv(adminFaultInject: ReturnType<typeof vi.fn>): Env {
    return {
      ADMIN_TOKEN: "test-admin-token",
      SHARDSCOPE_GATE_TOKEN: "test-gate-token",
      SHARD_API: {
        adminVbucketMap: vi.fn(async () => ({
          catalogs: [{ catalogShardId: "catalog-0", totalVBuckets: 1, map: [{ vbucket: 0, shardId: "shard-1", migrationStatus: "none" }] }],
        })),
        adminFaultInject,
      },
      LOAD_DRIVER: {
        idFromName: () => "singleton-id",
        get: () => ({
          fetch: async () => new Response(JSON.stringify({ running: false }), { status: 200 }),
        }),
      },
    } as unknown as Env;
  }

  it("a 403 (FAULT_INJECTION_ENABLED not set) rejects with ChaosPreconditionError, not a raw/opaque error", async () => {
    const adminFaultInject = vi.fn(async () => {
      throw new Error(
        'CloudflareShard RPC error 403: {"error":"Fault injection is disabled. Set FAULT_INJECTION_ENABLED=\\"true\\" to enable this demo-only endpoint — never in production."}',
      );
    });
    const env = fakeEnv(adminFaultInject);
    await expect(runBlipShardOfflineAttack(env, {})).rejects.toThrow(ChaosPreconditionError);
    await expect(runBlipShardOfflineAttack(env, {})).rejects.toThrow(/FAULT_INJECTION_ENABLED/);
  });

  it("an unrelated rejection (e.g. a 500) propagates UNCHANGED — not swallowed into a fabricated ChaosPreconditionError", async () => {
    const adminFaultInject = vi.fn(async () => {
      throw new Error("CloudflareShard RPC error 500: {\"error\":\"internal\"}");
    });
    const env = fakeEnv(adminFaultInject);
    await expect(runBlipShardOfflineAttack(env, {})).rejects.not.toThrow(ChaosPreconditionError);
    await expect(runBlipShardOfflineAttack(env, {})).rejects.toThrow(/CloudflareShard RPC error 500/);
  });

  it("a successful fire calls adminFaultInject with the resolved target and a duration under the core's 30s cap", async () => {
    const adminFaultInject = vi.fn(
      async (_adminToken: string, _body: { shardId: string; catalogShardId?: string; mode?: string; durationMs?: number }) => ({
        ok: true,
        mode: "unreachable",
        faultExpiresAt: Date.now() + 9000,
      }),
    );
    const env = fakeEnv(adminFaultInject);
    const outcome = await runBlipShardOfflineAttack(env, {});
    expect(adminFaultInject).toHaveBeenCalledWith(
      "test-admin-token",
      expect.objectContaining({ shardId: "shard-1", catalogShardId: "catalog-0", mode: "unreachable" }),
    );
    const call = adminFaultInject.mock.calls[0][1];
    expect(call.durationMs).toBeGreaterThan(0);
    expect(call.durationMs).toBeLessThanOrEqual(15000);
    expect(outcome.attack).toBe("blip-shard-offline");
    expect(outcome.survived).toBe(true);
  });
});

// ============================================================================
// classifyBlipShardOfflineFire — pure judge for the FIRING step (see this
// function's own doc comment in chaos.ts for why the FULL "survived the
// whole window" claim can't be judged synchronously). Fabricated data only.
// ============================================================================

describe("chaos.ts — classifyBlipShardOfflineFire", () => {
  const target: BlipShardTarget = { catalogShardId: "catalog-0", shardId: "shard-1", isMigrating: false };

  it("survived=true when the inject call came back ok and the meter was not already red", () => {
    const outcome = classifyBlipShardOfflineFire({
      target,
      durationMs: 9000,
      injectOk: true,
      injectResponseSummary: '{"ok":true,"mode":"unreachable","faultExpiresAt":123}',
      lostAtFireTime: 0,
      meterStateAtFireTime: "green",
      loadRunning: true,
    });
    expect(outcome.attack).toBe("blip-shard-offline");
    expect(outcome.survived).toBe(true);
    expect(outcome.note).toMatch(/genuinely injected/);
    expect(outcome.note).toMatch(/watch the always-visible T4 scoreboard/);
  });

  it("survived=false when the inject call did not come back ok", () => {
    const outcome = classifyBlipShardOfflineFire({
      target,
      durationMs: 9000,
      injectOk: false,
      injectResponseSummary: '{"ok":false}',
      lostAtFireTime: null,
      meterStateAtFireTime: null,
      loadRunning: false,
    });
    expect(outcome.survived).toBe(false);
    expect(outcome.note).toMatch(/did not come back ok/);
  });

  it("survived=false when the correctness meter already showed a loss at fire time (can't attribute it to this attack)", () => {
    const outcome = classifyBlipShardOfflineFire({
      target,
      durationMs: 9000,
      injectOk: true,
      injectResponseSummary: '{"ok":true}',
      lostAtFireTime: 3,
      meterStateAtFireTime: "red",
      loadRunning: true,
    });
    expect(outcome.survived).toBe(false);
    expect(outcome.note).toMatch(/already showed lost:3/);
  });

  it("notes when the target is mid-migration, so the button-mash / manual-override case is never silently muddied", () => {
    const migratingTarget: BlipShardTarget = { catalogShardId: "catalog-0", shardId: "shard-1", isMigrating: true };
    const outcome = classifyBlipShardOfflineFire({
      target: migratingTarget,
      durationMs: 9000,
      injectOk: true,
      injectResponseSummary: '{"ok":true}',
      lostAtFireTime: 0,
      meterStateAtFireTime: "green",
      loadRunning: true,
    });
    expect(outcome.note).toMatch(/mid-migration/);
    expect(outcome.note).toMatch(/PARK/);
  });

  it("observed honestly reports 'no load run currently active' instead of a stale lost count when no load is running", () => {
    const outcome = classifyBlipShardOfflineFire({
      target,
      durationMs: 9000,
      injectOk: true,
      injectResponseSummary: '{"ok":true}',
      lostAtFireTime: null,
      meterStateAtFireTime: null,
      loadRunning: false,
    });
    expect(outcome.observed).toMatch(/no load run currently active/);
  });
});

// ============================================================================
// parseBlipShardOfflineInput — request shaping: durationMs always ends up a
// positive integer at/under the core's absolute 30s cap (this file's own
// MAX_BLIP_DURATION_MS is more conservative still, at 15s).
// ============================================================================

describe("chaos.ts — parseBlipShardOfflineInput", () => {
  it("defaults durationMs when omitted or invalid", () => {
    expect(parseBlipShardOfflineInput({}).durationMs).toBeGreaterThan(0);
    expect(parseBlipShardOfflineInput({ durationMs: -5 }).durationMs).toBeGreaterThan(0);
    expect(parseBlipShardOfflineInput({ durationMs: "not-a-number" }).durationMs).toBeGreaterThan(0);
    expect(parseBlipShardOfflineInput({ durationMs: 1.5 }).durationMs).toBeGreaterThan(0);
  });

  it("clamps an explicit durationMs down to this file's MAX_BLIP_DURATION_MS (15000), itself under the core's 30000ms hard cap", () => {
    expect(parseBlipShardOfflineInput({ durationMs: 60000 }).durationMs).toBe(15000);
    expect(parseBlipShardOfflineInput({ durationMs: 25000 }).durationMs).toBe(15000);
  });

  it("honors a valid explicit durationMs under the cap", () => {
    expect(parseBlipShardOfflineInput({ durationMs: 5000 }).durationMs).toBe(5000);
  });

  it("carries an explicit {catalogShardId, shardId} override through", () => {
    const input = parseBlipShardOfflineInput({ catalogShardId: "catalog-0", shardId: "shard-9" });
    expect(input.catalogShardId).toBe("catalog-0");
    expect(input.shardId).toBe("shard-9");
  });
});
