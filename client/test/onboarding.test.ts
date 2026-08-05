import { describe, expect, it, vi } from "vitest";
import { runDoctor, runVerify, type OnboardingClock, type VerifyAdminApi, type VerifyTenantApi } from "../src/onboarding.js";
import { exitCodeFor } from "../src/onboarding-output.js";
import type { MutateRequest } from "../src/types.js";

const clock: OnboardingClock = { now: () => new Date("2026-08-05T12:00:00.000Z") };

function scanPage(rows: Record<string, unknown>[]) {
  return { rows, provenance: { complete: true }, scan: { catalogShardId: "catalog-0", shardCount: 2, successCount: 2, scanMs: 1 } };
}

describe("doctor domain result", () => {
  it("reports an uninitialized reachable target as ready with honest warnings", async () => {
    const result = await runDoctor({
      status: async () => ({ initialized: false, catalogShardCount: 4, shards: { total: 0, active: 0, draining: 0 }, catalogs: [] }),
    }, clock);

    expect(result.status).toBe("READY_WITH_WARNINGS");
    expect(result.checks.find((check) => check.id === "cluster_initialized")?.status).toBe("WARN");
    expect(result.checks.find((check) => check.id === "cloudflare_quota")?.status).toBe("WARN");
  });

  it("fails an initialized one-shard topology without suggesting a force reset", async () => {
    const result = await runDoctor({
      status: async () => ({ initialized: true, catalogShardCount: 1, shards: { total: 1, active: 1, draining: 0 }, catalogs: [] }),
    }, clock);

    expect(result.status).toBe("NOT_READY");
    expect(JSON.stringify(result)).toContain("will not force-reset");
  });

  it("returns a structured failure instead of throwing on auth/connectivity errors", async () => {
    const result = await runDoctor({ status: async () => { throw new Error("Bearer secret-token rejected"); } }, clock);
    expect(result.status).toBe("NOT_READY");
    expect(JSON.stringify(result)).not.toContain("secret-token");
  });

  it("redacts labeled secret values from failure details", async () => {
    const result = await runDoctor({ status: async () => { throw new Error("token=plain-secret-value"); } }, clock);
    expect(JSON.stringify(result)).not.toContain("plain-secret-value");
    expect(JSON.stringify(result)).toContain("token=[REDACTED]");
  });

  it.each([
    {
      name: "URL userinfo",
      message: "request to https://alice:supersecret@worker.example.test/status failed",
      forbidden: ["alice", "supersecret", "worker.example.test"],
    },
    {
      name: "URL query tokens and fragments",
      message: "request to https://worker.example.test/status?token=query-secret#fragment-secret failed",
      forbidden: ["worker.example.test", "query-secret", "fragment-secret"],
    },
    {
      name: "percent-encoded URL credentials",
      message: "request to https%3A%2F%2Falice%40corp%3Ap%2540ss%40worker.example.test%2Fstatus failed",
      forbidden: ["alice%40corp", "p%2540ss", "worker.example.test"],
    },
    {
      name: "nested and variant credential labels",
      message: '{"outer":{"client_secret":"nested-secret","apiKey":"nested-key","password":"nested-password"}}',
      forbidden: ["nested-secret", "nested-key", "nested-password"],
    },
  ])("redacts $name before returning a domain result", async ({ message, forbidden }) => {
    const result = await runDoctor({ status: async () => { throw new Error(message); } }, clock);
    const serialized = JSON.stringify(result);
    for (const value of forbidden) expect(serialized).not.toContain(value);
    expect(serialized).toContain("[REDACTED");
  });
});

describe("verify domain result", () => {
  type TxStatus = "commit_pending_manifest" | "committed" | "committed_pending_ack";

  function fixture(txStatus: TxStatus | [TxStatus, TxStatus] = "committed") {
    let initialized = false;
    const calls: string[] = [];
    const admin: VerifyAdminApi = {
      status: vi.fn(async () => ({
        initialized,
        catalogShardCount: 1,
        shards: initialized ? { total: 2, active: 2, draining: 0 } : { total: 0, active: 0, draining: 0 },
        catalogs: [],
      })),
      init: vi.fn(async (request) => { calls.push(`init:${String(request?.force)}`); initialized = true; return {}; }),
      createTable: vi.fn(async () => { calls.push("create-table"); return {}; }),
      backfillProvenance: vi.fn(async () => ({ ambiguous: [], orphaned: [] })),
      registerTenant: vi.fn(async () => ({ token: "tenant-secret-token" })),
      routeProbe: vi.fn(async (_table, _tenant, key) => ({ route: { shardId: key === "row-0" ? "shard-a" : "shard-b" } })),
    };
    const txStatuses = Array.isArray(txStatus) ? txStatus : [txStatus, txStatus];
    let txCall = 0;
    const tx = vi.fn(async (_mutations: MutateRequest[], _requestId?: string) => ({
      ok: true as const,
      txId: "tx-stable",
      status: txStatuses[Math.min(txCall++, txStatuses.length - 1)],
    }));
    const tableScan = vi.fn(async () => scanPage([
      { id: "row-0", marker: "marker-fixedrun" },
      { id: "row-1", marker: "marker-fixedrun" },
    ]));
    const tenant: VerifyTenantApi = {
      tx,
      tableScan,
    };
    return { admin, tenant, tx, tableScan, calls };
  }

  it("initializes without force, proves two placements, replays one txId, and verifies one row per key", async () => {
    const { admin, tenant, tx, calls } = fixture();
    const result = await runVerify(admin, () => tenant, { runId: "fixed-run", clock });

    expect(result.status).toBe("VERIFIED");
    expect(result.summary).toEqual({ distinctPlacements: 2, rowsVerified: 2, replayMatched: true, resourcesRetained: true });
    expect(calls).toContain("init:false");
    expect(tx).toHaveBeenCalledTimes(2);
    expect(tx.mock.calls[0][1]).toBe(tx.mock.calls[1][1]);
  });

  it("never overclaims VERIFIED when a durable commit has an outstanding acknowledgement", async () => {
    const { admin, tenant, tableScan } = fixture("committed_pending_ack");
    const result = await runVerify(admin, () => tenant, { runId: "fixed-run", clock });
    expect(result.status).toBe("PENDING_RECONCILIATION");
    expect(exitCodeFor(result)).toBe(5);
    expect(result.summary.rowsVerified).toBe(0);
    expect(result.checks.find((check) => check.id === "cross_shard_tx")).toMatchObject({
      status: "WARN",
      message: expect.stringContaining("participant acknowledgement is still pending"),
    });
    expect(result.checks.find((check) => check.id === "readback")?.message).toContain("Readback is deferred");
    expect(tableScan).not.toHaveBeenCalled();
  });

  it("defers readback and reports pending reconciliation while manifest registration blocks commit", async () => {
    const { admin, tenant, tableScan } = fixture("commit_pending_manifest");
    const result = await runVerify(admin, () => tenant, { runId: "fixed-run", clock });

    expect(result.status).toBe("PENDING_RECONCILIATION");
    expect(exitCodeFor(result)).toBe(5);
    expect(result.summary.rowsVerified).toBe(0);
    expect(result.checks.find((check) => check.id === "cross_shard_tx")).toMatchObject({
      status: "WARN",
      message: expect.stringContaining("participant commit is not yet authorized"),
    });
    expect(result.checks.find((check) => check.id === "readback")).toMatchObject({
      status: "WARN",
      message: expect.stringContaining("Readback is deferred"),
    });
    expect(tableScan).not.toHaveBeenCalled();
  });

  it.each(["commit_pending_manifest", "committed_pending_ack"] as const)(
    "allows strict readback when %s converges to committed on replay",
    async (pendingStatus) => {
      const { admin, tenant, tableScan } = fixture([pendingStatus, "committed"]);
      const result = await runVerify(admin, () => tenant, { runId: "fixed-run", clock });

      expect(result.status).toBe("VERIFIED");
      expect(exitCodeFor(result)).toBe(0);
      expect(result.summary.rowsVerified).toBe(2);
      expect(result.checks.find((check) => check.id === "cross_shard_tx")).toMatchObject({
        status: "PASS",
        message: expect.stringContaining("converged to committed"),
      });
      expect(tableScan).toHaveBeenCalled();
    },
  );

  it("refuses an initialized one-shard target rather than force-resetting it", async () => {
    const init = vi.fn();
    const admin: VerifyAdminApi = {
      status: async () => ({ initialized: true, catalogShardCount: 1, shards: { total: 1, active: 1, draining: 0 }, catalogs: [] }),
      init,
      createTable: vi.fn(),
      backfillProvenance: vi.fn(),
      registerTenant: vi.fn(),
      routeProbe: vi.fn(),
    };
    const result = await runVerify(admin, () => { throw new Error("not reached"); }, { runId: "fixed-run", clock });
    expect(result.status).toBe("FAILED");
    expect(init).not.toHaveBeenCalled();
  });

  it("fails when bounded route probes cannot prove distinct placement", async () => {
    const admin: VerifyAdminApi = {
      status: async () => ({ initialized: true, catalogShardCount: 1, shards: { total: 2, active: 2, draining: 0 }, catalogs: [] }),
      init: vi.fn(),
      createTable: vi.fn(async () => ({})),
      backfillProvenance: vi.fn(async () => ({ ambiguous: [], orphaned: [] })),
      registerTenant: vi.fn(async () => ({ token: "tenant-secret-token" })),
      routeProbe: vi.fn(async () => ({ route: { shardId: "same-shard" } })),
    };
    const unusedTenant: VerifyTenantApi = {
      tx: vi.fn(),
      tableScan: async () => scanPage([]),
    };
    const result = await runVerify(admin, () => unusedTenant, { runId: "fixed-run", maxRouteProbes: 3, clock });
    expect(result.status).toBe("FAILED");
    expect(result.checks.at(-1)?.message).toContain("within 3 bounded route probes");
  });
});
