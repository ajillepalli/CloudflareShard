// One-off, manually-run script (not part of `npm test`) exercising the
// built SDK against a real `wrangler dev` instance end to end -- catches
// drift between the hand-mirrored types in src/types.ts and the Worker's
// actual response shapes, which mocked-fetch unit tests can't catch.
// Usage: node scripts/verify-live.mjs (run `npm run build` first).
import assert from "node:assert/strict";
import { CloudflareShardAdminClient } from "../dist/index.js";

const BASE_URL = process.env.CLOUDFLARESHARD_URL ?? "http://127.0.0.1:8787";
const ADMIN_TOKEN = process.env.CLOUDFLARESHARD_ADMIN_TOKEN ?? "BucketMap2026!";

const admin = new CloudflareShardAdminClient({ baseUrl: BASE_URL, token: ADMIN_TOKEN });

function step(name, fn) {
  return (async () => {
    process.stdout.write(`- ${name} ... `);
    const result = await fn();
    process.stdout.write("ok\n");
    return result;
  })();
}

const suffix = Date.now().toString(36);
const table = `sdk_verify_${suffix}`;
const indexName = `sdk_verify_${suffix}_by_v`;

await step("init", () => admin.init({ numShards: 1, totalVBuckets: 4, force: true }));

await step("createTable", async () => {
  const res = await admin.createTable({
    table,
    schema: `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`,
    partitionKeyColumn: "id",
  });
  assert.equal(res.ok, true);
  assert.equal(res.table, table);
  assert.equal(typeof res.shardsApplied, "number");
});

const tenantId = "sdk-verify-tenant";
let tenantToken;
await step("registerTenant", async () => {
  const res = await admin.registerTenant({ tenantId, rotate: true });
  assert.equal(res.ok, true);
  assert.equal(res.tenantId, tenantId);
  assert.equal(typeof res.token, "string");
  tenantToken = res.token;
});

const { CloudflareShardClient } = await import("../dist/index.js");
const tenant = new CloudflareShardClient({ baseUrl: BASE_URL, token: tenantToken });

await step("insert (mutate)", async () => {
  const res = await tenant.insert(table, tenantId, "row-1", { v: "alpha" });
  assert.equal(res.ok, true);
  assert.equal(res.rowsAffected, 1);
});

await step("tx", async () => {
  const res = await tenant.tx([
    { op: "insert", table, tenantId, partitionKey: "row-2", values: { v: "beta" } },
    { op: "insert", table, tenantId, partitionKey: "row-3", values: { v: "gamma" } },
  ]);
  assert.equal(res.ok, true);
  assert.equal(res.status, "committed");
});

await step("createIndex + waitForIndexReady", async () => {
  const res = await admin.createIndex({ indexName, table, columns: ["v"] });
  assert.equal(res.ok, true);
  const status = await admin.waitForIndexReady(indexName, { intervalMs: 100, maxWaitMs: 30000 });
  assert.equal(status.status, "ready");
});

await step("indexQuery", async () => {
  const res = await tenant.indexQuery({ table, indexName, tenantId, values: { v: "alpha" } });
  assert.equal(res.rows.length, 1);
  assert.equal(res.rows[0].id, "row-1");
});

await step("tableScanAll", async () => {
  const ids = [];
  for await (const page of tenant.tableScanAll({ tenantId, table })) {
    for (const row of page) ids.push(row.id);
  }
  assert.deepEqual(ids.sort(), ["row-1", "row-2", "row-3"]);
});

await step("status", async () => {
  const res = await admin.status();
  assert.equal(res.initialized, true);
  assert.equal(typeof res.catalogShardCount, "number");
});

await step("shardStats", async () => {
  const res = await admin.shardStats("catalog-0-shard-0");
  assert.equal(res.ok, true);
  assert.ok(res.tables.some((t) => t.table === table));
});

await step("listTables", async () => {
  const res = await admin.listTables();
  assert.ok(res.tables.some((t) => t.table_name === table));
});

await step("listIndexes", async () => {
  const res = await admin.listIndexes();
  assert.ok(res.indexes.some((i) => i.indexName === indexName));
});

await step("topologyLockStatus (no active operation)", async () => {
  const res = await admin.topologyLockStatus();
  assert.equal(res.held, false);
});

await step("error normalization (indexQuery with a bogus index)", async () => {
  try {
    await tenant.indexQuery({ table, indexName: "does-not-exist", tenantId, values: { v: "alpha" } });
    assert.fail("expected a CloudflareShardError");
  } catch (err) {
    assert.equal(err.name, "CloudflareShardError");
    assert.equal(typeof err.status, "number");
    assert.ok(err.status >= 400);
  }
});

process.stdout.write("\nAll live verification steps passed.\n");
