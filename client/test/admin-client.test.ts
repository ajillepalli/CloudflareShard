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
    const { fetchImpl } = mockFetch(200, { found: true, status: "committed" });
    const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

    const res = await client.txStatus({ txId: "tx-1" });

    expect(res).toEqual({ found: true, status: "committed" });
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
});
