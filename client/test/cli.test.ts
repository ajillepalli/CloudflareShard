import { describe, expect, it } from "vitest";
import { CloudflareShardAdminClient } from "../src/admin-client.js";
import { dispatch, isCommand, requireFlag, usage } from "../src/cli.js";
import { mockFetch } from "./test-helpers.js";

describe("CLI", () => {
  it("isCommand() recognizes exactly the documented commands", () => {
    expect(isCommand("status")).toBe(true);
    expect(isCommand("create-index")).toBe(true);
    expect(isCommand("nonsense")).toBe(false);
    expect(isCommand(undefined)).toBe(false);
  });

  it("usage() documents every command", () => {
    const text = usage();
    for (const cmd of ["init", "create-table", "register-table", "register-tenant", "create-index", "create-index-status", "status", "shard-stats", "list-tables", "list-indexes"]) {
      expect(text).toContain(cmd);
    }
  });

  it("requireFlag() throws a helpful error naming the missing flag", () => {
    expect(() => requireFlag({}, "table")).toThrow(/--table/);
    expect(requireFlag({ table: "events" }, "table")).toBe("events");
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
  });
});
