import { describe, expect, it } from "vitest";
import { CloudflareShardClient } from "../src/client.js";
import { CloudflareShardError } from "../src/errors.js";
import { expectBearerToken, mockFetch } from "./test-helpers.js";

describe("CloudflareShardClient", () => {
  it("sends POST requests with the bearer token, content-type, and a trailing-slash-trimmed baseUrl", async () => {
    const { fetchImpl, calls } = mockFetch(200, { ok: true, rowsAffected: 1 });
    const client = new CloudflareShardClient({ baseUrl: "http://127.0.0.1:8787/", token: "tenant-token", fetchImpl });

    await client.insert("events", "t1", "e1", { body: "hi" }, "req-1");

    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe("http://127.0.0.1:8787/v1/mutate");
    expect(calls[0].method).toBe("POST");
    expect(calls[0].headers["content-type"]).toBe("application/json");
    expectBearerToken(calls[0], "tenant-token");
  });

  it("insert/update/delete/upsert compile to mutate's op field correctly", async () => {
    const { fetchImpl, calls } = mockFetch(200, { ok: true, rowsAffected: 1 });
    const client = new CloudflareShardClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await client.insert("events", "t1", "e1", { v: "a" });
    await client.update("events", "t1", "e1", { v: "b" });
    await client.delete("events", "t1", "e1");
    await client.upsert("events", "t1", "e1", { v: "c" });

    expect(calls.map((c) => (c.body as { op: string }).op)).toEqual(["insert", "update", "delete", "upsert"]);
  });

  it("auto-generates a requestId for mutate when none is supplied", async () => {
    const { fetchImpl, calls } = mockFetch(200, { ok: true, rowsAffected: 1 });
    const client = new CloudflareShardClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await client.insert("events", "t1", "e1", { v: "a" });

    const body = calls[0].body as { requestId: string };
    expect(typeof body.requestId).toBe("string");
    expect(body.requestId.length).toBeGreaterThan(0);
  });

  it("preserves a caller-supplied requestId for mutate instead of overwriting it", async () => {
    const { fetchImpl, calls } = mockFetch(200, { ok: true, rowsAffected: 1 });
    const client = new CloudflareShardClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await client.insert("events", "t1", "e1", { v: "a" }, "my-request-id");

    expect((calls[0].body as { requestId: string }).requestId).toBe("my-request-id");
  });

  it("upsert() forwards conflictColumns as the ON CONFLICT target (Codex review: was missing from the type)", async () => {
    const { fetchImpl, calls } = mockFetch(200, { ok: true, rowsAffected: 1 });
    const client = new CloudflareShardClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await client.upsert("events", "t1", "e1", { email: "a@example.com" }, ["email"]);

    expect((calls[0].body as { conflictColumns: string[] }).conflictColumns).toEqual(["email"]);
  });

  it("tx() fills in requestId when omitted, since the server rejects it as missing otherwise", async () => {
    const { fetchImpl, calls } = mockFetch(200, { ok: true, txId: "tx-1", status: "committed" });
    const client = new CloudflareShardClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await client.tx([{ op: "insert", table: "events", tenantId: "t1", partitionKey: "e1", values: { v: "a" } }]);

    const body = calls[0].body as { requestId: string };
    expect(typeof body.requestId).toBe("string");
    expect(body.requestId.length).toBeGreaterThan(0);
  });

  it("throws CloudflareShardError (not a raw fetch Response) on a non-2xx response", async () => {
    const { fetchImpl } = mockFetch(425, { error: { code: "INDEX_BUILDING", message: "Index is still building.", fix: "Retry once /admin/create-index-status reports 'ready'." } });
    const client = new CloudflareShardClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await expect(client.indexQuery({ table: "events", indexName: "events_by_v", tenantId: "t1", values: { v: "a" } })).rejects.toThrow(CloudflareShardError);
    await expect(client.indexQuery({ table: "events", indexName: "events_by_v", tenantId: "t1", values: { v: "a" } })).rejects.toMatchObject({
      status: 425,
      code: "INDEX_BUILDING",
    });
  });

  it("throws CloudflareShardError (not a raw SyntaxError) when an error response body isn't valid JSON (Codex review)", async () => {
    const fetchImpl = (async () => new Response("<html>502 Bad Gateway</html>", { status: 502 })) as unknown as typeof fetch;
    const client = new CloudflareShardClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await expect(client.indexQuery({ table: "events", indexName: "events_by_v", tenantId: "t1", values: { v: "a" } })).rejects.toMatchObject({
      name: "CloudflareShardError",
      status: 502,
    });
  });

  it("throws CloudflareShardError (not a raw SyntaxError) even for a 2xx response with a non-JSON body", async () => {
    const fetchImpl = (async () => new Response("not json", { status: 200 })) as unknown as typeof fetch;
    const client = new CloudflareShardClient({ baseUrl: "http://x", token: "t", fetchImpl });

    await expect(client.indexQuery({ table: "events", indexName: "events_by_v", tenantId: "t1", values: { v: "a" } })).rejects.toMatchObject({
      name: "CloudflareShardError",
      status: 200,
    });
  });

  it("tableScanAll() pages through every shard's rows automatically until nextCursor is omitted", async () => {
    let call = 0;
    const fetchImpl = (async () => {
      call += 1;
      const body =
        call === 1
          ? { rows: [{ id: "a" }], nextCursor: "cursor-1", provenance: { complete: true }, scan: { catalogShardId: "catalog-0", shardCount: 1, successCount: 1, scanMs: 1 } }
          : { rows: [{ id: "b" }], provenance: { complete: true }, scan: { catalogShardId: "catalog-0", shardCount: 1, successCount: 1, scanMs: 1 } };
      return new Response(JSON.stringify(body), { status: 200, headers: { "content-type": "application/json" } });
    }) as unknown as typeof fetch;
    const client = new CloudflareShardClient({ baseUrl: "http://x", token: "t", fetchImpl });

    const pages: unknown[] = [];
    for await (const page of client.tableScanAll({ tenantId: "t1", table: "events" })) {
      pages.push(page);
    }

    expect(pages).toEqual([[{ id: "a" }], [{ id: "b" }]]);
    expect(call).toBe(2);
  });
});
