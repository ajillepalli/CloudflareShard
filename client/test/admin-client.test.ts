import { describe, expect, it } from "vitest";
import { CloudflareShardAdminClient } from "../src/admin-client.js";
import { expectBearerToken, mockFetch, mockFetchSequence } from "./test-helpers.js";

describe("CloudflareShardAdminClient", () => {
  it("uses the admin bearer token (distinct from a tenant client's token)", async () => {
    const { fetchImpl, calls } = mockFetch(200, { initialized: false, catalogShardCount: 0, shards: { total: 0, active: 0, draining: 0 }, catalogs: [] });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "admin-token", fetchImpl });

    await client.status();

    expectBearerToken(calls[0], "admin-token");
    expect(calls[0].url).toBe("http://x/admin/status");
  });

  it("init() omits numShards/totalVBuckets/force when not provided, sending an empty body by default", async () => {
    const { fetchImpl, calls } = mockFetch(200, { ok: true, catalogShardCount: 4, catalogs: [] });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await client.init();

    expect(calls[0].body).toEqual({ numShards: undefined, totalVBuckets: undefined, force: undefined });
  });

  it("revokeTenant() surfaces tenantId/revoked, not just {ok: true} (found during a manual cross-check after Codex hit its usage limit)", async () => {
    const { fetchImpl } = mockFetch(200, { ok: true, tenantId: "t1", revoked: true });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    const res = await client.revokeTenant("t1");

    expect(res).toEqual({ ok: true, tenantId: "t1", revoked: true });
  });

  it("createTable() sends table/schema/partitionKeyColumn as given", async () => {
    const { fetchImpl, calls } = mockFetch(200, { ok: true, table: "events", shardsApplied: 4 });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await client.createTable({ table: "events", schema: "CREATE TABLE events (id TEXT PRIMARY KEY)", partitionKeyColumn: "id" });

    expect(calls[0].url).toBe("http://x/admin/create-table");
    expect(calls[0].body).toEqual({ table: "events", schema: "CREATE TABLE events (id TEXT PRIMARY KEY)", partitionKeyColumn: "id" });
  });

  it("shardStats() targets one specific shard, not a cluster-wide aggregate", async () => {
    const { fetchImpl, calls } = mockFetch(200, { ok: true, tables: [], idempotencyTableSize: 0, pendingIntentCount: 0, indexPendingJobCount: 0, indexEntryCount: 0, rowOwnerCount: 0 });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await client.shardStats("catalog-0-shard-0");

    expect(calls[0].body).toEqual({ shardId: "catalog-0-shard-0" });
  });

  it("routeProbe() uses a constant read-only query and returns authoritative placement", async () => {
    const { fetchImpl, calls } = mockFetch(200, { route: { shardId: "catalog-0-shard-1", catalogShardId: "catalog-0" } });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "admin", fetchImpl });

    const result = await client.routeProbe("events", "tenant-a", "row-7");

    expect(result.route.shardId).toBe("catalog-0-shard-1");
    expect(calls[0].url).toBe("http://x/v1/sql");
    expect(calls[0].body).toEqual({ sql: "SELECT 1", table: "events", tenantId: "tenant-a", partitionKey: "row-7" });
  });

  it("dropIndex() surfaces a partial-cleanup warning instead of erasing it (Codex review)", async () => {
    const { fetchImpl } = mockFetch(200, { ok: true, indexName: "idx", warning: "Physical cleanup failed on shard(s): catalog-1-shard-0." });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    const res = await client.dropIndex("idx");

    expect(res.warning).toContain("catalog-1-shard-0");
  });

  it("forceReleaseTopologyLock() surfaces released: false for a stale/mismatched operationId (Codex review: was erased from the type)", async () => {
    const { fetchImpl } = mockFetch(200, { ok: true, released: false });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    const res = await client.forceReleaseTopologyLock("stale-operation-id");

    expect(res.released).toBe(false);
  });

  it("migrateVbucketStatus() surfaces toShard/startedAt as null for a vbucket with no active migration (Codex review: was typed as non-null string)", async () => {
    const { fetchImpl } = mockFetch(200, { vbucket: 42, status: "none", fromShard: "catalog-0-shard-0", toShard: null, rowsCopied: 0, mirrorQueueDepth: 0, startedAt: null });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    const res = await client.migrateVbucketStatus({ catalogShardId: "catalog-0", vbucket: 42 });

    expect(res.toShard).toBeNull();
    expect(res.startedAt).toBeNull();
  });

  it("backfillProvenance() defaults to a full-cluster run (catalogShardId omitted) -- only that mode can certify a table (Codex review)", async () => {
    const { fetchImpl, calls } = mockFetch(200, { attributed: 0, ambiguous: [], orphaned: [] });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await client.backfillProvenance();

    expect(calls[0].body).toEqual({});
  });

  it("backfillProvenance() still accepts an explicit catalogShardId to scope the run", async () => {
    const { fetchImpl, calls } = mockFetch(200, { attributed: 0, ambiguous: [], orphaned: [] });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await client.backfillProvenance({ catalogShardId: "catalog-0" });

    expect(calls[0].body).toEqual({ catalogShardId: "catalog-0" });
  });

  it("txStatus() returns the actual found/status shape, not a bare txId/status pair (Codex review)", async () => {
    const { fetchImpl } = mockFetch(200, { found: true, status: "committed", decision: "commit", epoch: 1, operationHash: "a".repeat(64), commitAuthorized: true });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    const res = await client.txStatus({ txId: "tx-1" });

    expect(res).toEqual({ found: true, status: "committed", decision: "commit", epoch: 1, operationHash: "a".repeat(64), commitAuthorized: true });
  });

  it("txStatus() reports found: false for an unknown txId, with no status field at all", async () => {
    const { fetchImpl } = mockFetch(200, { found: false });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    const res = await client.txStatus({ txId: "unknown-tx" });

    expect(res).toEqual({ found: false });
  });

  it("txForceAbort() reports the resulting status: 'aborted' (Codex review: was missing from the type)", async () => {
    const { fetchImpl } = mockFetch(200, { ok: true, txId: "tx-1", status: "aborted" });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    const res = await client.txForceAbort({ txId: "tx-1" });

    expect(res.status).toBe("aborted");
  });

  describe("waitForIndexReady()", () => {
    it("resolves as soon as status flips to 'ready'", async () => {
      const { fetchImpl } = mockFetchSequence([
        { status: 200, body: { indexName: "idx", table: "t", status: "building", rowsCopied: 0, totalShards: 1, currentShardIndex: 0, currentShardId: "s0" } },
        { status: 200, body: { indexName: "idx", table: "t", status: "ready", rowsCopied: 10, totalShards: 1, currentShardIndex: 1, currentShardId: null } },
      ]);
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      const result = await client.waitForIndexReady("idx", { intervalMs: 1 });

      expect(result.status).toBe("ready");
    });

    it("throws if the index reaches 'failed', naming the index in the error", async () => {
      const { fetchImpl } = mockFetchSequence([{ status: 200, body: { indexName: "idx", table: "t", status: "failed", rowsCopied: 0, totalShards: 1, currentShardIndex: 0, currentShardId: null } }]);
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(client.waitForIndexReady("idx", { intervalMs: 1 })).rejects.toThrow(/idx/);
    });

    it("times out if the index never leaves 'building' within maxWaitMs", async () => {
      const { fetchImpl } = mockFetchSequence([{ status: 200, body: { indexName: "idx", table: "t", status: "building", rowsCopied: 0, totalShards: 1, currentShardIndex: 0, currentShardId: "s0" } }]);
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(client.waitForIndexReady("idx", { intervalMs: 5, maxWaitMs: 20 })).rejects.toThrow(/Timed out/);
    });
  });

  describe("waitForTransaction()", () => {
    const known = (status: "commit_pending_manifest" | "committed") => ({
      found: true,
      status,
      decision: "commit",
      epoch: 1,
      operationHash: "b".repeat(64),
      commitAuthorized: status === "committed",
    });

    it("polls pending manifest work until the coordinator is terminal", async () => {
      const { fetchImpl } = mockFetchSequence([
        { status: 200, body: known("commit_pending_manifest") },
        { status: 200, body: known("committed") },
      ]);
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      const result = await client.waitForTransaction("tx-1", { intervalMs: 1 });

      expect(result.status).toBe("committed");
    });

    it("fails fast for an unknown transaction", async () => {
      const { fetchImpl } = mockFetch(200, { found: false });
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(client.waitForTransaction("missing", { intervalMs: 1 })).rejects.toThrow(/not found/);
    });

    it("times out with the last non-terminal state", async () => {
      const { fetchImpl } = mockFetch(200, known("commit_pending_manifest"));
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(client.waitForTransaction("tx-1", { intervalMs: 5, maxWaitMs: 20 })).rejects.toThrow(/commit_pending_manifest/);
    });
  });

  describe("fleet restore", () => {
    const status = (phase: "previewing" | "previewed" | "reconciling" | "parked_lease_lost" | "complete" | "rolled_back" | "manual_repair_required") => ({
      protocol_version: 1,
      format_version: 1,
      restore_id: "restore-1",
      plan_hash: phase === "previewing" ? null : "a".repeat(64),
      fleet_id: "default",
      cutoff: "2026-08-05T12:00:00.000Z",
      phase,
      started_at: phase === "previewing" ? null : "2026-08-05T12:10:00.000Z",
      updated_at: "2026-08-05T12:20:00.000Z",
      completed_at: phase === "complete" || phase === "rolled_back" || phase === "manual_repair_required" ? "2026-08-05T12:20:00.000Z" : null,
      progress: { participants_total: 2, participants_restored: phase === "previewing" ? 0 : 2, transactions_total: 3, transactions_reconciled: phase === "complete" ? 3 : 1 },
      blockers: phase === "manual_repair_required"
        ? [{ code: "RESTORE_MANIFEST_GAP", message: "Missing evidence.", participant_id: null, tx_id: "tx-2" }]
        : [],
      report: phase === "complete"
        ? { discarded_write_count: 1, discarded_write_report_hash: "b".repeat(64), discarded_write_report_complete: true, measured_rpo_ms: 1000, measured_rto_ms: 2000, verified_at: "2026-08-05T12:20:00.000Z" }
        : null,
    });

    it("projects ergonomic preview input to the exact versioned wire request", async () => {
      const response = { ok: true, status: "previewing", restore_id: "restore-1", retry_after_ms: 1000 };
      const { fetchImpl, calls } = mockFetch(202, response);
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(client.restorePreview({
        fleetId: "default",
        cutoff: "2026-08-05T12:00:00.000Z",
        idempotencyKey: "preview-1",
      })).resolves.toEqual(response);

      expect(calls[0].url).toBe("http://x/admin/restore-preview");
      expect(calls[0].body).toEqual({
        protocol_version: 1,
        format_version: 1,
        fleet_id: "default",
        cutoff: "2026-08-05T12:00:00.000Z",
        idempotency_key: "preview-1",
      });
    });

    it("sends only restore identity and exact plan hash for execute/reconcile/rollback", async () => {
      const { fetchImpl, calls } = mockFetch(202, { ok: true, status: "accepted", restore_id: "restore-1", plan_hash: "a".repeat(64) });
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });
      const request = { restoreId: "restore-1", planHash: "a".repeat(64) };

      await client.restoreExecute(request);
      await client.restoreReconcile(request);
      await client.restoreRollback(request);

      expect(calls.map((call) => call.url)).toEqual([
        "http://x/admin/restore-execute",
        "http://x/admin/restore-reconcile",
        "http://x/admin/restore-rollback",
      ]);
      expect(calls[0].body).toEqual({
        protocol_version: 1, format_version: 1, restore_id: "restore-1", plan_hash: "a".repeat(64),
      });
      expect(calls[1].body).toEqual(calls[0].body);
      expect(calls[2].body).toEqual(calls[0].body);
    });

    it("rejects malformed successful restore responses instead of trusting type casts", async () => {
      const invalidPreview = new CloudflareShardAdminClient({
        baseUrl: "http://x",
        token: "t",
        fetchImpl: mockFetch(200, { ok: true, status: "previewed", plan: { restore_id: "restore-1" } }).fetchImpl,
      });
      await expect(invalidPreview.restorePreview({
        fleetId: "default", cutoff: "2026-08-05T12:00:00.000Z", idempotencyKey: "preview-1",
      })).rejects.toMatchObject({ code: "INVALID_RESTORE_RESPONSE", status: 502 });

      const invalidAccepted = new CloudflareShardAdminClient({
        baseUrl: "http://x",
        token: "t",
        fetchImpl: mockFetch(200, { ok: true, status: "queued", restore_id: "restore-1", plan_hash: "a".repeat(64) }).fetchImpl,
      });
      await expect(invalidAccepted.restoreExecute({
        restoreId: "restore-1", planHash: "a".repeat(64),
      })).rejects.toMatchObject({ code: "INVALID_RESTORE_RESPONSE", status: 502 });

      const invalidStatus = new CloudflareShardAdminClient({
        baseUrl: "http://x",
        token: "t",
        fetchImpl: mockFetch(200, { ...status("reconciling"), phase: "unknown" }).fetchImpl,
      });
      await expect(invalidStatus.restoreStatus({ restoreId: "restore-1" }))
        .rejects.toMatchObject({ code: "INVALID_RESTORE_RESPONSE", status: 502 });
    });

    it("rejects successful responses that are bound to a different restore request", async () => {
      const accepted = new CloudflareShardAdminClient({
        baseUrl: "http://x",
        token: "t",
        fetchImpl: mockFetch(202, { ok: true, status: "accepted", restore_id: "restore-other", plan_hash: "a".repeat(64) }).fetchImpl,
      });
      await expect(accepted.restoreExecute({ restoreId: "restore-1", planHash: "a".repeat(64) }))
        .rejects.toMatchObject({ code: "INVALID_RESTORE_RESPONSE", status: 502 });

      const mismatchedStatus = new CloudflareShardAdminClient({
        baseUrl: "http://x",
        token: "t",
        fetchImpl: mockFetch(200, { ...status("reconciling"), restore_id: "restore-other" }).fetchImpl,
      });
      await expect(mismatchedStatus.restoreStatus({ restoreId: "restore-1" }))
        .rejects.toMatchObject({ code: "INVALID_RESTORE_RESPONSE", status: 502 });
    });

    it("waitForRestore polls previewing work and returns complete", async () => {
      const { fetchImpl } = mockFetchSequence([
        { status: 200, body: status("previewing") },
        { status: 200, body: status("reconciling") },
        { status: 200, body: status("complete") },
      ]);
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(client.waitForRestore("restore-1", { intervalMs: 1 })).resolves.toMatchObject({ phase: "complete" });
    });

    it("waitForRestore returns rolled_back as a terminal result", async () => {
      const { fetchImpl } = mockFetch(200, status("rolled_back"));
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(client.waitForRestore("restore-1", { intervalMs: 1 })).resolves.toMatchObject({ phase: "rolled_back" });
    });

    it.each(["previewed", "parked_lease_lost"] as const)("waitForRestore returns %s without polling forever", async (phase) => {
      const { fetchImpl, calls } = mockFetch(200, status(phase));
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(client.waitForRestore("restore-1", { intervalMs: 1 })).resolves.toMatchObject({ phase });
      expect(calls).toHaveLength(1);
    });

    it("returns manual_repair_required as a visible terminal result", async () => {
      const { fetchImpl } = mockFetch(200, status("manual_repair_required"));
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(client.waitForRestore("restore-1", { intervalMs: 1 })).resolves.toMatchObject({
        phase: "manual_repair_required",
        blockers: [expect.objectContaining({ code: "RESTORE_MANIFEST_GAP" })],
      });
    });
  });
});
