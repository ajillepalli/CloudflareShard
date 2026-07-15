import { SELF, env, runInDurableObject } from "cloudflare:test";
import { expect } from "vitest";
import { hashKey } from "./hash";
import type { CatalogDO } from "./catalog";
import type { ShardDO } from "./shard";

export function tenantForCatalogShard(catalogIndex: number, catalogShardCount: number): string {
  for (let i = 0; ; i += 1) {
    const tenantId = `tenant-${i}`;
    if (hashKey(tenantId) % catalogShardCount === catalogIndex) {
      return tenantId;
    }
  }
}

export function post(path: string, body: unknown, authorization?: string) {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (authorization) headers.authorization = authorization;
  return SELF.fetch(`https://worker.internal${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
}

export const AUTH = () => `Bearer ${env.ADMIN_TOKEN}`;

export async function initCluster(numShards = 2, totalVBuckets = 16) {
  const res = await post("/admin/init", { numShards, totalVBuckets, force: true }, AUTH());
  expect(res.status).toBe(200);
  const createRes = await post(
    "/admin/create-table",
    {
      table: "events",
      // PR review round 12: /admin/create-table now rejects IF NOT EXISTS
      // schemas outright (see handleAdminCreateTable) — isolated per-test
      // storage means no test needs this to be silently idempotent.
      schema: "CREATE TABLE events (id TEXT PRIMARY KEY, v TEXT)",
      partitionKeyColumn: "id",
    },
    AUTH(),
  );
  expect(createRes.status).toBe(200);
}

// rotate: true makes this idempotent across tests that share a catalog shard
// (tenant_auth isn't wiped by /admin/init's force:true, unlike vbucket/shard
// state) — a tenantId reused across test cases would otherwise 409 on the
// second registration.
export async function registerTenant(tenantId: string): Promise<string> {
  const res = await post("/admin/register-tenant", { tenantId, rotate: true }, AUTH());
  expect(res.status).toBe(200);
  const body = (await res.json()) as { token: string };
  return `Bearer ${body.token}`;
}

/** ShardDO/CatalogDO storage isn't wiped by /admin/init's force:true (only
 * catalog/shard/vbucket assignment resets — see the established pattern
 * from the drain-shard and tenant-registration tests above). Each
 * create-index test below uses its own dedicated table + index name rather
 * than the shared "events" table, so backfill scans and index_rules
 * registration never leak across tests. */
export async function createIndexTestTable(table: string): Promise<void> {
  const res = await post(
    "/admin/create-table",
    { table, schema: `CREATE TABLE ${table} (id TEXT PRIMARY KEY, v TEXT)`, partitionKeyColumn: "id" },
    AUTH(),
  );
  expect(res.status).toBe(200);
}

// With numShards:1, each of the 4 default catalog shards still gets its own
// physical shard (catalog-0-shard-0 .. catalog-3-shard-0) — 4 total, not 1.
// indexShardIdForKey hashes into that full pool, so a given index entry can
// land on any of them; tests must search all four, not assume shard 0.
export const ALL_TEST_SHARD_IDS = ["catalog-0-shard-0", "catalog-1-shard-0", "catalog-2-shard-0", "catalog-3-shard-0"];

/** Polls __cf_indexes across every shard in the pool until the predicate
 * matches the combined row set, or the attempt budget runs out —
 * ctx.waitUntil()'s index-maintenance work runs after the response is
 * already sent, so a test asserting on its effect can't just check
 * synchronously after the /v1/mutate call resolves. */
export async function pollIndexRows(
  indexName: string,
  predicate: (rows: Array<{ partition_key: string; index_key_json: string }>) => boolean,
): Promise<Array<{ partition_key: string; index_key_json: string }>> {
  for (let attempt = 0; attempt < 50; attempt++) {
    const rows: Array<{ partition_key: string; index_key_json: string }> = [];
    for (const shardId of ALL_TEST_SHARD_IDS) {
      const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));
      await runInDurableObject(shardStub, async (_instance: unknown, state: DurableObjectState) => {
        rows.push(
          ...(Array.from(
            state.storage.sql.exec(
              "SELECT partition_key, index_key_json FROM __cf_indexes WHERE index_name = ? ORDER BY partition_key ASC",
              indexName,
            ),
          ) as Array<{ partition_key: string; index_key_json: string }>),
        );
      });
    }
    rows.sort((a, b) => (a.partition_key < b.partition_key ? -1 : a.partition_key > b.partition_key ? 1 : 0));
    if (predicate(rows)) return rows;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`pollIndexRows timed out waiting for predicate on index ${indexName}`);
}

/** Finds two partitionKey values that route to different shardIds under the
 * given tenant/table, by probing /v1/sql's route echo (established pattern:
 * see "routes different tenants" above). Needed to build genuine multi-shard
 * /v1/tx test fixtures. */
export async function findPartitionKeyPairOnDifferentShards(token: string, tenantId: string, table: string): Promise<[string, string]> {
  const seen = new Map<string, string>();
  for (let i = 0; i < 200; i++) {
    const partitionKey = `pk-${i}`;
    const res = await post(
      "/v1/sql",
      { sql: "SELECT 1", table, tenantId, partitionKey },
      AUTH(),
    );
    const body = (await res.json()) as { route: { shardId: string } };
    seen.set(partitionKey, body.route.shardId);
    const distinct = new Set(seen.values());
    if (distinct.size > 1) {
      const entries = Array.from(seen.entries());
      const first = entries[0];
      const second = entries.find(([, shardId]) => shardId !== first[1])!;
      return [first[0], second[0]];
    }
  }
  throw new Error("Could not find two partition keys on different shards.");
}

/** Writes a row directly to a shard bypassing every write path that would
 * normally record __cf_row_owners (Milestone 3, Chunk 0) — simulates a row
 * written before Chunk 0 shipped, which is exactly what
 * /admin/backfill-provenance exists to repair. */
export async function insertRowBypassingProvenance(shardId: string, table: string, id: string, v: string): Promise<void> {
  const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));
  const res = await shardStub.fetch(
    new Request("https://shard.internal/execute", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        sql: `INSERT INTO ${table} (id, v) VALUES (?, ?)`,
        params: [id, v],
        requestId: `bypass-insert-${table}-${id}-${crypto.randomUUID()}`,
        isMutation: true,
      }),
    }),
  );
  expect(res.status).toBe(200);
}

export async function rowOwnerEntries(shardId: string, table: string, partitionKey: string) {
  const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));
  return runInDurableObject(shardStub, async (_instance: ShardDO, state: DurableObjectState) => {
    return Array.from(
      state.storage.sql.exec(
        "SELECT tenant_id, vbucket FROM __cf_row_owners WHERE table_name = ? AND partition_key = ?",
        table,
        partitionKey,
      ),
    ) as Array<{ tenant_id: string; vbucket: number }>;
  });
}

/** Runs a SQL statement against one shard directly via its /execute route. */
export async function shardExecute(shardId: string, sql: string, params: unknown[] = []): Promise<{ status: number; rows: Array<Record<string, unknown>> }> {
  const shardStub = env.SHARD.get(env.SHARD.idFromName(shardId));
  const res = await shardStub.fetch(
    new Request("https://shard.internal/execute", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ sql, params, requestId: `test-exec-${crypto.randomUUID()}`, isMutation: undefined }),
    }),
  );
  const body = (await res.json()) as { rows?: Array<Record<string, unknown>> };
  return { status: res.status, rows: body.rows ?? [] };
}

/** Polls a shard-side SELECT until the predicate matches — mirror writes run
 * in ctx.waitUntil() after the client response, so tests can't assert
 * synchronously. */
export async function pollShardRows(
  shardId: string,
  sql: string,
  params: unknown[],
  predicate: (rows: Array<Record<string, unknown>>) => boolean,
): Promise<Array<Record<string, unknown>>> {
  for (let attempt = 0; attempt < 50; attempt++) {
    const { rows } = await shardExecute(shardId, sql, params);
    if (predicate(rows)) return rows;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`pollShardRows timed out on shard ${shardId}: ${sql}`);
}

/** Marks a vbucket as migrating directly on catalog-0's vbucket_map — the
 * state Chunk 4's /admin/migrate-vbucket sets through orchestration. */
export async function setMigrationState(vbucket: number, status: string, targetShardId: string | null): Promise<void> {
  const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
  await runInDurableObject(catalogStub, async (_instance: CatalogDO, state: DurableObjectState) => {
    state.storage.sql.exec(
      "UPDATE vbucket_map SET migration_status = ?, target_shard_id = ? WHERE vbucket = ?",
      status,
      targetShardId,
      vbucket,
    );
  });
}

/** Drives catalog-0's alarm-based migration orchestration until the vbucket's
 * migration reports status 'none' (completed), or the tick budget runs out. */
export async function driveMigrationToCompletion(vbucket: number, maxTicks = 25): Promise<void> {
  const catalogStub = env.CATALOG.get(env.CATALOG.idFromName("catalog-0"));
  for (let tick = 0; tick < maxTicks; tick += 1) {
    await runInDurableObject(catalogStub, async (instance: CatalogDO) => {
      await instance.alarm();
    });
    const statusRes = await post("/admin/migrate-vbucket-status", { catalogShardId: "catalog-0", vbucket }, AUTH());
    const statusBody = (await statusRes.json()) as { status: string };
    if (statusBody.status === "none") return;
    // Give shard-side alarms (mirror retries) a moment between ticks.
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`migration of vbucket ${vbucket} did not complete within ${maxTicks} ticks`);
}

/** Finds `count` partition keys that all hash into the SAME vbucket as
 * `seedKey` for the given tenant/table (so a whole batch of writes exercises
 * one migrating vbucket). */
export function partitionKeysInSameVbucket(tenantId: string, table: string, seedKey: string, count: number, totalVBuckets: number): string[] {
  const wanted = hashKey(`${tenantId}:${table}:${seedKey}`) % totalVBuckets;
  const keys: string[] = [seedKey];
  for (let i = 0; keys.length < count; i += 1) {
    const candidate = `${seedKey}-${i}`;
    if (hashKey(`${tenantId}:${table}:${candidate}`) % totalVBuckets === wanted) {
      keys.push(candidate);
    }
  }
  return keys;
}

/** The migration provenance gate is deliberately shard-wide, so orphaned
 * rows left behind by earlier tests in this file (e.g. the Chunk 2
 * PROVENANCE_MISSING_FOR_INDEX test, the Chunk 1 orphan-reporting test)
 * would keep the gate closed for every migration test. Purges every
 * unattributed row from both catalog-0 shards. */
export async function purgeUnattributedRows(): Promise<void> {
  const tablesRes = await post("/admin/list-tables", {}, AUTH());
  const tablesBody = (await tablesRes.json()) as { tables: Array<{ table_name: string; partition_key_column: string }> };
  for (const shardId of ["catalog-0-shard-0", "catalog-0-shard-1"]) {
    for (const t of tablesBody.tables) {
      if (t.partition_key_column === "__unset__") continue;
      await shardExecute(
        shardId,
        `DELETE FROM "${t.table_name}" WHERE "${t.partition_key_column}" IN (
           SELECT b."${t.partition_key_column}" FROM "${t.table_name}" b
           LEFT JOIN __cf_row_owners ro ON ro.table_name = ? AND ro.partition_key = b."${t.partition_key_column}"
           WHERE ro.partition_key IS NULL
         )`,
        [t.table_name],
      );
    }
  }
}

export function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}
