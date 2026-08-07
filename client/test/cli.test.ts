import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CloudflareShardAdminClient } from "../src/admin-client.js";
import { dispatch, isCommand, requireCanonicalUtcFlag, requireFlag, requireSha256Flag, run, usage } from "../src/cli.js";
import { mockFetch } from "./test-helpers.js";
import { credentialUrl } from "./redaction-fixtures.js";

describe("CLI", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("isCommand() recognizes exactly the documented commands", () => {
    expect(isCommand("doctor")).toBe(true);
    expect(isCommand("verify")).toBe(true);
    expect(isCommand("status")).toBe(true);
    expect(isCommand("create-index")).toBe(true);
    expect(isCommand("nonsense")).toBe(false);
    expect(isCommand(undefined)).toBe(false);
  });

  it("usage() documents every command", () => {
    const text = usage();
    for (const cmd of ["doctor", "verify", "init", "create-table", "register-table", "register-tenant", "create-index", "create-index-status", "status", "shard-stats", "list-tables", "list-indexes", "restore-preview", "restore-execute", "restore-status", "restore-reconcile", "restore-rollback"]) {
      expect(text).toContain(cmd);
    }
    expect(text).toContain("0 success, 2 invalid/unsafe input, 3 prerequisite failure");
    expect(text).toContain("4 verification failure, 5 pending reconciliation");
  });

  it("requireFlag() throws a helpful error naming the missing flag", () => {
    expect(() => requireFlag({}, "table")).toThrow(/--table/);
    expect(requireFlag({ table: "events" }, "table")).toBe("events");
  });

  it("requireSha256Flag() requires an exact lowercase plan digest", () => {
    expect(requireSha256Flag({ "plan-hash": "a".repeat(64) }, "plan-hash")).toBe("a".repeat(64));
    expect(() => requireSha256Flag({ "plan-hash": "ABC" }, "plan-hash")).toThrow(/lowercase SHA-256/);
  });

  it("requireCanonicalUtcFlag() rejects ambiguous or normalized cutoff input", () => {
    expect(requireCanonicalUtcFlag({ cutoff: "2026-08-05T12:00:00.000Z" }, "cutoff")).toBe("2026-08-05T12:00:00.000Z");
    expect(() => requireCanonicalUtcFlag({ cutoff: "2026-08-05 12:00:00" }, "cutoff")).toThrow(/canonical UTC/);
  });

  it("restore-status exits 4 after printing a manual repair state", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => Response.json({
      protocol_version: 1,
      format_version: 1,
      restore_id: "restore-1",
      plan_hash: "a".repeat(64),
      fleet_id: "default",
      cutoff: "2026-08-05T12:00:00.000Z",
      phase: "manual_repair_required",
      started_at: "2026-08-05T12:10:00.000Z",
      updated_at: "2026-08-05T12:20:00.000Z",
      completed_at: null,
      progress: { participants_total: 1, participants_restored: 0, transactions_total: 0, transactions_reconciled: 0 },
      blockers: [],
      report: null,
    })));
    let stdout = "";
    vi.spyOn(process.stdout, "write").mockImplementation(((chunk: string | Uint8Array) => { stdout += String(chunk); return true; }) as typeof process.stdout.write);

    await expect(run(["restore-status", "--url", "http://x", "--token", "t", "--restore-id", "restore-1"])).resolves.toBe(4);
    expect(stdout).toContain("manual_repair_required");
  });

  it("converts receipt filesystem rejection into stable JSON and exit 3 without rejecting", async () => {
    const directory = await mkdtemp(path.join(tmpdir(), "cfs-cli-receipt-failure-"));
    const blockingFile = path.join(directory, "not-a-directory");
    await writeFile(blockingFile, "block mkdir", "utf8");
    vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({
      initialized: false,
      catalogShardCount: 1,
      shards: { total: 0, active: 0, draining: 0 },
      catalogs: [],
    }), { status: 200, headers: { "content-type": "application/json" } })));
    let stdout = "";
    let stderr = "";
    vi.spyOn(process.stdout, "write").mockImplementation(((chunk: string | Uint8Array) => { stdout += String(chunk); return true; }) as typeof process.stdout.write);
    vi.spyOn(process.stderr, "write").mockImplementation(((chunk: string | Uint8Array) => { stderr += String(chunk); return true; }) as typeof process.stderr.write);

    await expect(run(["doctor", "--url", credentialUrl("alice", "supersecret", "worker.example.test"), "--token", "admin-secret", "--receipt-dir", blockingFile, "--output", "json"])).resolves.toBe(3);

    const output = JSON.parse(stdout) as { receipt: unknown; failure?: { code?: string; category?: string } };
    expect(output.receipt).toBeNull();
    expect(output.failure).toEqual(expect.objectContaining({ code: "RECEIPT_WRITE_FAILED", category: "PREREQUISITE" }));
    expect(stdout).not.toContain("supersecret");
    expect(stdout).not.toContain("admin-secret");
    expect(stderr).toBe("");
  });

  describe("dispatch()", () => {
    it("status maps to client.status() with no flags needed", async () => {
      const { fetchImpl, calls } = mockFetch(200, { initialized: true, catalogShardCount: 1, shards: { total: 1, active: 1, draining: 0 }, catalogs: [] });
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await dispatch(client, "status", {});

      expect(calls[0].url).toBe("http://x/admin/status");
    });

    it("create-index splits the columns flag on commas", async () => {
      const { fetchImpl, calls } = mockFetch(200, { ok: true, indexName: "idx", table: "events", columns: ["a", "b"], status: "building" });
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await dispatch(client, "create-index", { "index-name": "idx", table: "events", columns: "a, b" });

      expect(calls[0].body).toEqual({ indexName: "idx", table: "events", columns: ["a", "b"] });
    });

    it("init passes through --num-shards/--total-vbuckets as numbers and --force as a boolean", async () => {
      const { fetchImpl, calls } = mockFetch(200, { ok: true, catalogShardCount: 4, catalogs: [] });
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await dispatch(client, "init", { "num-shards": "4", "total-vbuckets": "256", force: true });

      expect(calls[0].body).toEqual({ numShards: 4, totalVBuckets: 256, force: true });
    });

    it("create-table requires --table/--schema/--partition-key-column, throwing if any is missing", async () => {
      const { fetchImpl } = mockFetch(200, { ok: true, table: "events", shardsApplied: 1 });
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(dispatch(client, "create-table", { table: "events" })).rejects.toThrow(/--schema/);
    });

    it("restore-preview maps required operator flags to the versioned wire body", async () => {
      const { fetchImpl, calls } = mockFetch(202, { ok: true, status: "previewing", restore_id: "restore-1", retry_after_ms: 1000 });
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await dispatch(client, "restore-preview", {
        "fleet-id": "default",
        cutoff: "2026-08-05T12:00:00.000Z",
        "idempotency-key": "preview-1",
      });

      expect(calls[0].body).toEqual({
        protocol_version: 1,
        format_version: 1,
        fleet_id: "default",
        cutoff: "2026-08-05T12:00:00.000Z",
        idempotency_key: "preview-1",
      });
      await expect(dispatch(client, "restore-preview", {
        "fleet-id": "default", cutoff: "2026-08-05 12:00:00", "idempotency-key": "preview-2",
      })).rejects.toThrow(/canonical UTC/);
    });

    it("restore-execute requires the exact plan hash as destructive confirmation", async () => {
      const { fetchImpl, calls } = mockFetch(202, { ok: true, status: "accepted", restore_id: "restore-1", plan_hash: "a".repeat(64) });
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(dispatch(client, "restore-execute", { "restore-id": "restore-1" })).rejects.toThrow(/--plan-hash/);
      await dispatch(client, "restore-execute", { "restore-id": "restore-1", "plan-hash": "a".repeat(64) });

      expect(calls[0].body).toEqual({
        protocol_version: 1, format_version: 1, restore_id: "restore-1", plan_hash: "a".repeat(64),
      });
    });

    it("restore-rollback requires the exact plan hash as destructive confirmation", async () => {
      const { fetchImpl, calls } = mockFetch(202, { ok: true, status: "accepted", restore_id: "restore-1", plan_hash: "a".repeat(64) });
      const client = new CloudflareShardAdminClient({ baseUrl: "http://x", token: "t", fetchImpl });

      await expect(dispatch(client, "restore-rollback", { "restore-id": "restore-1" })).rejects.toThrow(/--plan-hash/);
      await dispatch(client, "restore-rollback", { "restore-id": "restore-1", "plan-hash": "a".repeat(64) });

      expect(calls[0].url).toBe("http://x/admin/restore-rollback");
      expect(calls[0].body).toEqual({
        protocol_version: 1, format_version: 1, restore_id: "restore-1", plan_hash: "a".repeat(64),
      });
    });
  });
});
