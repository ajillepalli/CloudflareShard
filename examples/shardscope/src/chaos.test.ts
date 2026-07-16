import { describe, expect, it } from "vitest";
import * as chaosModule from "./chaos";
import {
  CHAOS_ATTACKS,
  CHAOS_NOT_WIRED_ATTACK,
  ChaosPreconditionError,
  classifyDoubleSubmit,
  classifyMismatchedReplay,
  parseAbortMigrationInput,
  parseDoubleSubmitInput,
  parseDrainHotNodeInput,
  parseHotShardOverride,
  parseHotVbucketOverride,
  parseMigrateHotVbucketInput,
  parseMismatchedReplayInput,
  parseSplitHotVbucketInput,
  pickHotShardTarget,
  pickHotVbucketTarget,
  pickInFlightMigrationTarget,
  type LoadStatusLike,
  type VbucketMapLike,
} from "./chaos";

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
// The not-wired attack: "blip shard offline" must never be callable through
// this module — no runXxxAttack function, not in the wired-attack registry.
// ============================================================================

describe("chaos.ts — the not-wired attack is honestly disabled, never faked", () => {
  it("CHAOS_NOT_WIRED_ATTACK is not among the attacks this module actually wires", () => {
    expect((CHAOS_ATTACKS as readonly string[]).includes(CHAOS_NOT_WIRED_ATTACK)).toBe(false);
  });

  it("this module exports no run function for the not-wired attack", () => {
    const exportsOfChaosModule = Object.keys(chaosModule);
    const looksLikeARunner = (name: string) => /^run.*Attack$/.test(name);
    const runnerNames = exportsOfChaosModule.filter(looksLikeARunner);
    // Every wired attack has a runXxxAttack function; none of them should be
    // nameable after the not-wired attack's key ("blip-shard-offline" ->
    // no "runBlipShardOfflineAttack").
    expect(runnerNames.some((n) => n.toLowerCase().includes("blip"))).toBe(false);
    // Sanity: the wired runners DO exist, so this isn't a vacuous check.
    expect(runnerNames.length).toBe(CHAOS_ATTACKS.length);
  });
});
