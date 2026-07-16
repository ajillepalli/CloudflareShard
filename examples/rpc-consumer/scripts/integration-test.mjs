#!/usr/bin/env node
// Real service-binding integration test — proves the RPC path actually
// works end to end, not just that CloudflareShardRpc's methods are
// syntactically callable in-process. Requires TWO already-running instances:
// the main CloudflareShard Worker and this consumer Worker, each started with
// `wrangler dev` (locally, Wrangler's dev registry wires the consumer's
// `SHARD_API` service binding to the main Worker automatically) or as two
// real deployments in the same Cloudflare account.
//
// Setup (admin/table/tenant/index registration) goes through the main
// Worker's existing HTTP admin API directly — admin operations aren't part
// of this issue's RPC scope (see the follow-up issue). Only the actual
// mutate/tableScan/indexQuery calls happen over the service binding, via the
// consumer Worker's own HTTP demo routes.
//
// Usage: node scripts/integration-test.mjs <main-worker-url> <consumer-worker-url> <admin-token>
// Example (local dev): node scripts/integration-test.mjs http://localhost:8787 http://localhost:8788 test-admin-token

const [, , mainUrl, consumerUrl, adminToken] = process.argv;

if (!mainUrl || !consumerUrl || !adminToken) {
  console.error("Usage: node scripts/integration-test.mjs <main-worker-url> <consumer-worker-url> <admin-token>");
  process.exit(1);
}

// Suffixed per run — /admin/create-table rejects an already-existing table,
// and index names are also global, so a fixed name would only work once
// against a persistent dev/deployed Worker.
const RUN_ID = Date.now();
const TABLE = `rpc_demo_items_${RUN_ID}`;
const TENANT_ID = `rpc-demo-tenant-${RUN_ID}`;
const INDEX_NAME = `idx_note_${RUN_ID}`;

const results = [];

async function check(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
    console.log(`[PASS] ${name}`);
  } catch (error) {
    results.push({ name, ok: false, error: error instanceof Error ? error.message : String(error) });
    console.error(`[FAIL] ${name}: ${error instanceof Error ? error.message : error}`);
  }
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function adminCall(path, body) {
  const res = await fetch(`${mainUrl}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json", authorization: `Bearer ${adminToken}` },
    body: JSON.stringify(body),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`${path} -> ${res.status}: ${JSON.stringify(json)}`);
  return json;
}

async function consumerCall(path, body) {
  const res = await fetch(`${consumerUrl}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`${path} -> ${res.status}: ${JSON.stringify(json)}`);
  return json;
}

let tenantToken;

async function main() {
  await check("main Worker /health", async () => {
    const res = await fetch(`${mainUrl}/health`);
    assert(res.status === 200, `expected 200, got ${res.status}`);
  });

  await check("consumer Worker /health", async () => {
    const res = await fetch(`${consumerUrl}/health`);
    assert(res.status === 200, `expected 200, got ${res.status}`);
  });

  await check("init cluster (setup, via main Worker's existing HTTP admin API — no-op if already initialized)", async () => {
    try {
      await adminCall("/admin/init", { numShards: 4, totalVBuckets: 256 });
    } catch (error) {
      // Already initialized from a prior run against this dev instance — fine.
      if (!/already/i.test(String(error))) throw error;
    }
  });

  await check("register tenant (setup, via main Worker's existing HTTP admin API)", async () => {
    const body = await adminCall("/admin/register-tenant", { tenantId: TENANT_ID });
    assert(typeof body.token === "string" && body.token.length > 0, "expected a tenant token back");
    tenantToken = body.token;
  });

  await check("create table (setup, via main Worker's existing HTTP admin API)", async () => {
    await adminCall("/admin/create-table", {
      table: TABLE,
      schema: `CREATE TABLE ${TABLE} (id TEXT PRIMARY KEY, note TEXT)`,
      partitionKeyColumn: "id",
    });
  });

  await check("create index on note (setup, via main Worker's existing HTTP admin API)", async () => {
    await adminCall("/admin/create-index", { table: TABLE, indexName: INDEX_NAME, columns: ["note"] });
  });

  let scanRowNote;
  await check("RPC mutate() + tableScan() round trip, over the real service binding", async () => {
    const result = await consumerCall("/demo/write-and-scan", {
      tenantToken,
      tenantId: TENANT_ID,
      table: TABLE,
      partitionKey: `row-${Date.now()}`,
    });
    assert(result.mutateResult.ok === true, "expected mutateResult.ok === true");
    assert(result.mutateResult.rowsAffected === 1, `expected rowsAffected === 1, got ${result.mutateResult.rowsAffected}`);
    assert(Array.isArray(result.scanResult.rows), "expected scanResult.rows to be an array");
    assert(result.scanResult.rows.length >= 1, "expected at least one row back from tableScan");
    scanRowNote = result.scanResult.rows[0].note;
  });

  await check("RPC indexQuery() exact-tuple lookup, over the real service binding", async () => {
    const result = await consumerCall("/demo/index-query", {
      tenantToken,
      tenantId: TENANT_ID,
      table: TABLE,
      indexName: INDEX_NAME,
      column: "note",
      value: scanRowNote,
    });
    assert(Array.isArray(result.indexQueryResult.rows), "expected indexQueryResult.rows to be an array");
    assert(result.indexQueryResult.rows.length >= 1, "expected at least one row back from indexQuery");
  });

  await check("RPC adminListTables() (admin op), over the real service binding", async () => {
    const result = await consumerCall("/demo/admin-list-tables", { adminToken });
    assert(result.adminListTablesResult != null, "expected a non-null adminListTablesResult");
  });

  await check("RPC adminTopologyLockStatus() (topology op), over the real service binding", async () => {
    const result = await consumerCall("/demo/admin-topology-lock-status", { adminToken });
    assert(result.adminTopologyLockStatusResult != null, "expected a non-null adminTopologyLockStatusResult");
  });

  const failed = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failed.length}/${results.length} checks passed.`);
  if (failed.length > 0) process.exit(1);
}

main();
