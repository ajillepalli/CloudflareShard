# cloudflare-shard-client

Typed TypeScript SDK and CLI for [CloudflareShard](../README.md)'s HTTP API —
tenant data-plane (`mutate`/`tx`/`index-query`/`table-scan`) and admin routes
(`init`/`create-table`/`create-index`/`status`/`shard-stats` and more), so you
don't have to hand-write `fetch()` calls or re-derive the request/response
shapes from `docs/SPEC.md` yourself. See issue #22.

Zero runtime dependencies, ESM-only, targets Node 20+ (uses the global
`fetch` and `crypto.randomUUID()` Node ships natively).

## Install

Not yet published to npm — for now, use it from within this repo:

```bash
cd client
npm install
npm run build
```

## Two clients, two tokens

CloudflareShard's HTTP API has two auth tiers, and this SDK mirrors that with
two client classes:

- **`CloudflareShardAdminClient`** — every `/admin/*` route (`init`,
  `createTable`, `createIndex`, `status`, `shardStats`, `splitVbucket`, ...).
  Construct with `ADMIN_TOKEN`.
- **`CloudflareShardClient`** — the tenant data-plane (`mutate`, `tx`,
  `indexQuery`, `tableScan`). Construct with a tenant's bearer token (from
  `registerTenant()`). `CloudflareShardAdminClient` extends this, so an admin
  client has both, but a tenant token cannot call `/admin/*` routes (the
  server rejects it with 401) — keep the two separate in your own code the
  same way the API itself does.

## Quickstart

```ts
import { CloudflareShardAdminClient, CloudflareShardClient } from "cloudflare-shard-client";

const admin = new CloudflareShardAdminClient({
  baseUrl: "http://127.0.0.1:8787", // or your deployed Worker's URL
  token: process.env.ADMIN_TOKEN!,
});

await admin.init({ numShards: 4, totalVBuckets: 256 });

await admin.createTable({
  table: "events",
  schema: "CREATE TABLE events (id TEXT PRIMARY KEY, user_id TEXT, body TEXT)",
  partitionKeyColumn: "id",
});

const { token } = await admin.registerTenant({ tenantId: "t1" });
const tenant = new CloudflareShardClient({ baseUrl: "http://127.0.0.1:8787", token });

await tenant.insert("events", "t1", "e1", { user_id: "user-1", body: "hello" });

// Secondary index + read path
await admin.createIndex({ indexName: "events_by_user", table: "events", columns: ["user_id"] });
await admin.waitForIndexReady("events_by_user"); // polls createIndexStatus() until 'ready' or 'failed'

const { rows } = await tenant.indexQuery({
  table: "events",
  indexName: "events_by_user",
  tenantId: "t1",
  values: { user_id: "user-1" },
});

// Cross-shard atomic transaction
await tenant.tx([
  { op: "insert", table: "events", tenantId: "t1", partitionKey: "e2", values: { user_id: "user-1", body: "a" } },
  { op: "insert", table: "events", tenantId: "t1", partitionKey: "e3", values: { user_id: "user-1", body: "b" } },
]);

// Page through every one of a tenant's rows in a table automatically
for await (const page of tenant.tableScanAll({ tenantId: "t1", table: "events" })) {
  console.log(page.length, "rows in this page");
}
```

## Error handling

Every non-2xx response throws `CloudflareShardError` instead of returning a
response you have to check `.ok` on yourself. It normalizes both error body
shapes the API uses (`{error: {code, message, fix}}` and the older
`{error: "string"}`) into one typed shape:

```ts
import { CloudflareShardError } from "cloudflare-shard-client";

try {
  await tenant.indexQuery({ table: "events", indexName: "events_by_user", tenantId: "t1", values: { user_id: "user-1" } });
} catch (err) {
  if (err instanceof CloudflareShardError) {
    console.error(err.status, err.code, err.message, err.fix);
    // e.g. 425 "INDEX_BUILDING" "Index is still building." "Retry once .../create-index-status reports 'ready'."
  }
}
```

## requestId and idempotency

`mutate()`/`insert()`/`update()`/`delete()`/`upsert()` and `tx()` all accept
an optional `requestId` and generate one with `crypto.randomUUID()` if you
don't supply it — matching `/v1/mutate`'s own server-side default, and
filling in the value `/v1/tx` requires (it 400s without one, since it's the
whole transaction's idempotency key). Pass your own `requestId` explicitly
only when *you* need retry-safe resubmission of the exact same write.

## CLI

Covers the admin routes you'd otherwise run by hand with curl:

```bash
npm run build
node dist/cli.js --help

export CLOUDFLARESHARD_URL=http://127.0.0.1:8787
export CLOUDFLARESHARD_ADMIN_TOKEN=<your ADMIN_TOKEN>

node dist/cli.js init --num-shards 4 --total-vbuckets 256
node dist/cli.js create-table --table events --schema "CREATE TABLE events (id TEXT PRIMARY KEY, body TEXT)" --partition-key-column id
node dist/cli.js status
node dist/cli.js shard-stats --shard-id catalog-0-shard-0
```

`--url`/`--token` flags work instead of the env vars if you prefer. Every
command prints its JSON response to stdout and exits non-zero with a
human-readable error on stderr for anything that fails.

## What's covered

**Tenant data-plane (`CloudflareShardClient`):** `mutate`, `insert`,
`update`, `delete`, `upsert`, `tx`, `indexQuery`, `tableScan`,
`tableScanAll` (auto-paginating).

**Admin (`CloudflareShardAdminClient`, additive over the above):** `init`,
`registerTable`, `createTable`, `setPartitionKeyColumn`, `registerTenant`,
`revokeTenant`, `createIndex`, `createIndexStatus`, `waitForIndexReady`,
`dropIndex`, `listIndexes`, `listTables`, `status`, `shardStats`,
`topologyLockStatus`, `forceReleaseTopologyLock`, `splitVbucket`,
`migrateVbucket`, `migrateVbucketStatus`, `migrateVbucketAbort`,
`drainShard`, `drainShardStatus`, `backfillProvenance`, `setRowOwner`,
`txStatus`, `txForceAbort`.

**CLI:** `init`, `create-table`, `register-table`, `register-tenant`,
`create-index`, `create-index-status`, `status`, `shard-stats`,
`list-tables`, `list-indexes`.

Not yet wrapped: `/v1/sql`, `/v1/scatter` (admin-only, raw-SQL escape
hatches — deliberately not given a typed wrapper, matching their
trust-based/debugging role in the API itself), and the remaining
lower-traffic admin routes (`audit-log`, `holds-topology-lock`, etc.) — open
an issue or send a PR if you need one of those typed.

## Types

Every request/response shape lives in `src/types.ts`, hand-mirrored from the
Worker's actual handlers (not generated) — see that file's header comment.
If you add or change a route in the main Worker (`src/index.ts`,
`src/catalog.ts`, `src/shard.ts` at the repo root), update the matching type
here too.

## Development

```bash
npm run typecheck   # tsc --noEmit, covers src/ and test/
npm test            # vitest — mocked-fetch unit tests, no network
npm run build        # emits dist/

# Full end-to-end check against a real server (not part of `npm test`):
# in the repo root: npm run dev
node scripts/verify-live.mjs
```

`verify-live.mjs` calls `/admin/init` with `force: true`, which resets
cluster topology -- **destructive** against a deployment with real data. It
only runs against `localhost`/`127.0.0.1` by default; pointing
`CLOUDFLARESHARD_URL` at anything else requires setting
`I_UNDERSTAND_THIS_WILL_RESET_CLUSTER_TOPOLOGY=true` explicitly. Never run
it against a live/production deployment.
