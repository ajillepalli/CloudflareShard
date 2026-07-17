// One-off, manually-run script (not part of `npm test`) exercising the
// built SDK against a real `wrangler dev` instance end to end -- catches
// drift between the hand-mirrored types in src/types.ts and the Worker's
// actual response shapes, which mocked-fetch unit tests can't catch.
// Usage: node scripts/verify-live.mjs (run `npm run build` first).
import assert from "node:assert/strict";
import { CloudflareShardAdminClient } from "../dist/index.js";

const BASE_URL = process.env.CLOUDFLARESHARD_URL ?? "http://127.0.0.1:8787";
const ADMIN_TOKEN = process.env.CLOUDFLARESHARD_ADMIN_TOKEN;
if (!ADMIN_TOKEN) {
  console.error("Set CLOUDFLARESHARD_ADMIN_TOKEN to the target deployment's ADMIN_TOKEN before running this script.");
  process.exit(1);
}

// Codex review (P1): this script calls /admin/init with force: true, which
// resets cluster metadata and the shard/vbucket map -- destructive against
// any deployment with real data. Pointing CLOUDFLARESHARD_URL at a live
// Worker (the same env var name/pattern used for both local dev and real
// deployments) and running this "verification" script would wipe it. Only
// run the destructive init step against localhost by default; a non-local
// target needs an explicit, unambiguous opt-in.
const targetHost = new URL(BASE_URL).hostname;
const isLocalTarget = targetHost === "127.0.0.1" || targetHost === "localhost" || targetHost === "::1";
if (!isLocalTarget && process.env.I_UNDERSTAND_THIS_WILL_RESET_CLUSTER_TOPOLOGY !== "true") {
  console.error(
    `Refusing to run against non-local target ${BASE_URL}: this script calls /admin/init with force: true, ` +
      "which resets cluster topology and would destroy real data on a live deployment.\n" +
      "If you really intend to run this against a disposable/test deployment, set " +
      "I_UNDERSTAND_THIS_WILL_RESET_CLUSTER_TOPOLOGY=true and re-run.",
  );
  process.exit(1);
}

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

// The following steps target fixes from Codex review round 1 on PR #24 --
// each one previously had an inaccurate type, verified here against the
// real server (not just mocked-fetch unit tests).

await step("upsert() with conflictColumns", async () => {
  const res = await tenant.upsert(table, tenantId, "row-1", { v: "alpha-updated" }, ["id"]);
  assert.equal(res.ok, true);
  // Codex round-7 fix: /v1/mutate's index maintenance is dispatched via
  // ctx.waitUntil and isn't guaranteed done by the time this call returns
  // (docs/SPEC.md) -- querying immediately can legitimately race and see
  // zero rows even on a correct deployment. Poll briefly instead of
  // asserting on the first attempt.
  let check;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    check = await tenant.indexQuery({ table, indexName, tenantId, values: { v: "alpha-updated" } });
    if (check.rows.length > 0) break;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  assert.equal(check.rows.length, 1);
  assert.equal(check.rows[0].id, "row-1");
});

await step("backfillProvenance() full-cluster (catalogShardId omitted)", async () => {
  const res = await admin.backfillProvenance();
  assert.equal(typeof res.attributed, "number");
  assert.ok(Array.isArray(res.ambiguous));
  assert.ok(Array.isArray(res.orphaned));
});

await step("txStatus() found/status shape", async () => {
  const txRes = await tenant.tx([{ op: "insert", table, tenantId, partitionKey: "row-tx-status", values: { v: "delta" } }]);
  const status = await admin.txStatus({ txId: txRes.txId });
  assert.equal(status.found, true);
  assert.equal(typeof status.status, "string");

  const unknown = await admin.txStatus({ txId: "no-such-tx-id" });
  assert.equal(unknown.found, false);
  assert.equal("status" in unknown, false);
});

process.stdout.write("\nAll live verification steps passed.\n");
