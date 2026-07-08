#!/usr/bin/env node
// Post-deploy smoke test. Read-only against cluster state except for a
// dedicated "smoke_test" table/tenant namespace, so it's safe to run
// against a live deployment without touching real tenant data. Does NOT
// call /admin/init — a cluster must already be initialized.
//
// Usage: node scripts/smoke-test.mjs <base-url> <admin-token>
// Example: node scripts/smoke-test.mjs https://cloudflare-shard-mvp.example.workers.dev secret-token

const [, , baseUrl, adminToken] = process.argv;

if (!baseUrl || !adminToken) {
  console.error("Usage: node scripts/smoke-test.mjs <base-url> <admin-token>");
  process.exit(1);
}

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

async function main() {
  await check("GET /health returns ok", async () => {
    const res = await fetch(`${baseUrl}/health`);
    assert(res.status === 200, `expected 200, got ${res.status}`);
    const body = await res.json();
    assert(body.ok === true, "expected { ok: true }");
  });

  await check("POST /admin/status is authenticated and cluster is initialized", async () => {
    const res = await fetch(`${baseUrl}/admin/status`, {
      method: "POST",
      headers: { authorization: `Bearer ${adminToken}` },
      body: "{}",
    });
    assert(res.status === 200, `expected 200, got ${res.status}`);
    const body = await res.json();
    assert(body.initialized === true, "cluster is not initialized — run /admin/init first, smoke test will not do this for you");
  });

  await check("POST /admin/status rejects a bad token", async () => {
    const res = await fetch(`${baseUrl}/admin/status`, {
      method: "POST",
      headers: { authorization: "Bearer wrong-token" },
      body: "{}",
    });
    assert(res.status === 401, `expected 401, got ${res.status}`);
  });

  const tenantId = `smoke-test-${Date.now()}`;

  await check("POST /admin/create-table (smoke_test) round-trips", async () => {
    const res = await fetch(`${baseUrl}/admin/create-table`, {
      method: "POST",
      headers: { authorization: `Bearer ${adminToken}`, "content-type": "application/json" },
      body: JSON.stringify({
        table: "smoke_test",
        schema: "CREATE TABLE IF NOT EXISTS smoke_test (id TEXT PRIMARY KEY, v TEXT)",
      }),
    });
    assert(res.status === 200, `expected 200, got ${res.status}: ${await res.text()}`);
  });

  await check("POST /v1/sql insert + select round-trips through routing and a shard", async () => {
    const insertRes = await fetch(`${baseUrl}/v1/sql`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        table: "smoke_test",
        tenantId,
        partitionKey: "p1",
        sql: "INSERT INTO smoke_test (id, v) VALUES (?, ?)",
        params: ["smoke-row", "ok"],
      }),
    });
    assert(insertRes.status === 200, `insert expected 200, got ${insertRes.status}: ${await insertRes.text()}`);

    const selectRes = await fetch(`${baseUrl}/v1/sql`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        table: "smoke_test",
        tenantId,
        partitionKey: "p1",
        sql: "SELECT v FROM smoke_test WHERE id = ?",
        params: ["smoke-row"],
      }),
    });
    assert(selectRes.status === 200, `select expected 200, got ${selectRes.status}`);
    const body = await selectRes.json();
    assert(body.result?.rows?.[0]?.v === "ok", "expected round-tripped row to read back 'ok'");
  });

  const failed = results.filter((r) => !r.ok);
  console.log(`\n${results.length - failed.length}/${results.length} checks passed.`);
  if (failed.length > 0) process.exit(1);
}

main();
