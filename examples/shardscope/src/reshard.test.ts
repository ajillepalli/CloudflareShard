import { describe, expect, it, vi } from "vitest";
import {
  ReshardValidationError,
  parseSplitVbucketInput,
  splitVbucket,
  parseMigrateVbucketInput,
  migrateVbucket,
  parseMigrateVbucketStatusQuery,
  migrateVbucketStatus,
  parseMigrateVbucketAbortInput,
  migrateVbucketAbort,
  parseDrainShardInput,
  drainShard,
  parseDrainShardStatusQuery,
  drainShardStatus,
  topologyLockStatus,
  parseForceReleaseTopologyLockInput,
  forceReleaseTopologyLock,
} from "./reshard";
import type { Env } from "./env";

/** Minimal fake Env — only ADMIN_TOKEN and a mocked SHARD_API are exercised
 * by reshard.ts's call*() functions, so nothing else on Env needs a real
 * implementation here (mirrors the pattern in
 * src/load/tenant-token-store.test.ts: exercise the pure logic directly
 * rather than standing up a real Durable Object / Miniflare binding). */
function fakeEnv(): Env & { SHARD_API: { [K in keyof Env["SHARD_API"]]: ReturnType<typeof vi.fn> } } {
  return {
    ADMIN_TOKEN: "test-admin-token",
    SHARDSCOPE_GATE_TOKEN: "test-gate-token",
    SHARD_API: {
      adminStatus: vi.fn(),
      adminVbucketMap: vi.fn(),
      adminShardStats: vi.fn(),
      adminRegisterTenant: vi.fn(),
      adminSplitVbucket: vi.fn(async () => ({ ok: true })),
      adminMigrateVbucket: vi.fn(async () => ({ ok: true })),
      adminMigrateVbucketStatus: vi.fn(async () => ({ status: "backfilling" })),
      adminMigrateVbucketAbort: vi.fn(async () => ({ ok: true, status: "aborted" })),
      adminDrainShard: vi.fn(async () => ({ ok: true })),
      adminDrainShardStatus: vi.fn(async () => ({ status: "migrating-vbuckets" })),
      adminTopologyLockStatus: vi.fn(async () => ({ held: false })),
      adminForceReleaseTopologyLock: vi.fn(async () => ({ ok: true, released: true })),
    },
  } as unknown as Env & { SHARD_API: { [K in keyof Env["SHARD_API"]]: ReturnType<typeof vi.fn> } };
}

describe("reshard.ts — split", () => {
  it("parses a valid split body, defaulting an omitted newShardId to undefined", () => {
    const input = parseSplitVbucketInput({ catalogShardId: "catalog-0", vbucket: 5 });
    expect(input).toEqual({ catalogShardId: "catalog-0", vbucket: 5, newShardId: undefined });
  });

  it("parses newShardId when explicitly given", () => {
    const input = parseSplitVbucketInput({ catalogShardId: "catalog-0", vbucket: 5, newShardId: "shard-9" });
    expect(input.newShardId).toBe("shard-9");
  });

  it("rejects a missing catalogShardId (vbucket ids are catalog-local)", () => {
    expect(() => parseSplitVbucketInput({ vbucket: 5 })).toThrow(ReshardValidationError);
  });

  it("rejects a negative or non-integer vbucket", () => {
    expect(() => parseSplitVbucketInput({ catalogShardId: "catalog-0", vbucket: -1 })).toThrow(ReshardValidationError);
    expect(() => parseSplitVbucketInput({ catalogShardId: "catalog-0", vbucket: 1.5 })).toThrow(ReshardValidationError);
    expect(() => parseSplitVbucketInput({ catalogShardId: "catalog-0", vbucket: "not-a-number" })).toThrow(ReshardValidationError);
  });

  it("rejects a non-object body", () => {
    expect(() => parseSplitVbucketInput(null)).toThrow(ReshardValidationError);
    expect(() => parseSplitVbucketInput("catalog-0")).toThrow(ReshardValidationError);
  });

  it("calls SHARD_API.adminSplitVbucket with env.ADMIN_TOKEN and the parsed payload, keeping the token out of the payload itself", async () => {
    const env = fakeEnv();
    const input = parseSplitVbucketInput({ catalogShardId: "catalog-0", vbucket: 5 });
    await splitVbucket(env, input);
    expect(env.SHARD_API.adminSplitVbucket).toHaveBeenCalledWith("test-admin-token", input);
    expect(JSON.stringify(input)).not.toContain("test-admin-token");
  });
});

describe("reshard.ts — migrate", () => {
  it("parses a valid migrate body with an explicit targetShardId", () => {
    const input = parseMigrateVbucketInput({ catalogShardId: "catalog-1", vbucket: 12, targetShardId: "shard-3" });
    expect(input).toEqual({ catalogShardId: "catalog-1", vbucket: 12, targetShardId: "shard-3" });
  });

  it("allows an omitted targetShardId (server auto-picks a fresh shard)", () => {
    const input = parseMigrateVbucketInput({ catalogShardId: "catalog-1", vbucket: 12 });
    expect(input.targetShardId).toBeUndefined();
  });

  it("calls SHARD_API.adminMigrateVbucket with env.ADMIN_TOKEN", async () => {
    const env = fakeEnv();
    const input = parseMigrateVbucketInput({ catalogShardId: "catalog-1", vbucket: 12, targetShardId: "shard-3" });
    await migrateVbucket(env, input);
    expect(env.SHARD_API.adminMigrateVbucket).toHaveBeenCalledWith("test-admin-token", input);
  });
});

describe("reshard.ts — migrate-status / migrate-abort (shared catalogShardId+vbucket ref)", () => {
  it("parses migrate-status from GET query params", () => {
    const params = new URLSearchParams({ catalogShardId: "catalog-0", vbucket: "7" });
    expect(parseMigrateVbucketStatusQuery(params)).toEqual({ catalogShardId: "catalog-0", vbucket: 7 });
  });

  it("rejects migrate-status query params missing vbucket", () => {
    const params = new URLSearchParams({ catalogShardId: "catalog-0" });
    expect(() => parseMigrateVbucketStatusQuery(params)).toThrow(ReshardValidationError);
  });

  it("parses migrate-abort from a POST JSON body", () => {
    expect(parseMigrateVbucketAbortInput({ catalogShardId: "catalog-0", vbucket: 7 })).toEqual({
      catalogShardId: "catalog-0",
      vbucket: 7,
    });
  });

  it("calls the right SHARD_API method for status vs. abort", async () => {
    const env = fakeEnv();
    const ref = { catalogShardId: "catalog-0", vbucket: 7 };
    await migrateVbucketStatus(env, ref);
    await migrateVbucketAbort(env, ref);
    expect(env.SHARD_API.adminMigrateVbucketStatus).toHaveBeenCalledWith("test-admin-token", ref);
    expect(env.SHARD_API.adminMigrateVbucketAbort).toHaveBeenCalledWith("test-admin-token", ref);
  });
});

describe("reshard.ts — drain / drain-status", () => {
  it("parses a valid drain body", () => {
    expect(parseDrainShardInput({ catalogShardId: "catalog-0", shardId: "shard-2" })).toEqual({
      catalogShardId: "catalog-0",
      shardId: "shard-2",
    });
  });

  it("rejects a missing shardId", () => {
    expect(() => parseDrainShardInput({ catalogShardId: "catalog-0" })).toThrow(ReshardValidationError);
  });

  it("parses drain-status from GET query params", () => {
    const params = new URLSearchParams({ catalogShardId: "catalog-0", shardId: "shard-2" });
    expect(parseDrainShardStatusQuery(params)).toEqual({ catalogShardId: "catalog-0", shardId: "shard-2" });
  });

  it("calls SHARD_API.adminDrainShard / adminDrainShardStatus with env.ADMIN_TOKEN", async () => {
    const env = fakeEnv();
    const input = { catalogShardId: "catalog-0", shardId: "shard-2" };
    await drainShard(env, input);
    await drainShardStatus(env, input);
    expect(env.SHARD_API.adminDrainShard).toHaveBeenCalledWith("test-admin-token", input);
    expect(env.SHARD_API.adminDrainShardStatus).toHaveBeenCalledWith("test-admin-token", input);
  });
});

describe("reshard.ts — topology lock status / force-release", () => {
  it("topologyLockStatus takes no payload, just env.ADMIN_TOKEN", async () => {
    const env = fakeEnv();
    await topologyLockStatus(env);
    expect(env.SHARD_API.adminTopologyLockStatus).toHaveBeenCalledWith("test-admin-token");
  });

  it("parses a valid force-release body", () => {
    expect(parseForceReleaseTopologyLockInput({ operationId: "op-123" })).toEqual({ operationId: "op-123" });
  });

  it("rejects a force-release body missing operationId — the operator must read lock-status first", () => {
    expect(() => parseForceReleaseTopologyLockInput({})).toThrow(ReshardValidationError);
  });

  it("calls SHARD_API.adminForceReleaseTopologyLock with env.ADMIN_TOKEN and the operationId", async () => {
    const env = fakeEnv();
    const input = parseForceReleaseTopologyLockInput({ operationId: "op-123" });
    await forceReleaseTopologyLock(env, input);
    expect(env.SHARD_API.adminForceReleaseTopologyLock).toHaveBeenCalledWith("test-admin-token", input);
  });
});
