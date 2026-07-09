# CloudflareShard MVP

A concrete MVP for a sharded SQL layer on top of Cloudflare Durable Objects (SQLite-backed).

## What this prototype demonstrates

- One logical SQL endpoint in a Worker.
- Catalog DO as control plane for table registry and vBucket map.
- Shard DOs as single-threaded SQLite execution nodes.
- Deterministic single-shard routing via tenantId + table + partitionKey.
- Scatter read endpoint for fan-out SELECT.
- Manual vBucket reassignment for basic rebalancing.
- Mutation idempotency via requestId, rejecting replay with a mismatched SQL/params pair instead of returning a stale result.

## Project layout

- `src/index.ts`: Gateway worker router and public API.
- `src/catalog.ts`: Catalog durable object (metadata, routing, map changes).
- `src/shard.ts`: Shard durable object (SQLite execution + idempotency).
- `docs/SPEC.md`: Concrete architecture and protocol spec.

## Prerequisites

- Node.js 20+
- Cloudflare account + Wrangler authentication

## Setup

```powershell
git clone https://github.com/ajillepalli/CloudflareShard.git
cd CloudflareShard
npm install
```

## Run locally

```powershell
npm run dev
```

## Deploy

```powershell
npm run deploy
```

## API quickstart

### 1) Initialize cluster metadata and shard map

```bash
curl -X POST http://127.0.0.1:8787/admin/init \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"numShards":4,"totalVBuckets":256}'
```

`numShards` is clamped to 1-256 and `totalVBuckets` to 64-65536 (defaults 8 and 1024)
so a single call can't provision an oversized, unrollbackable cluster.

### 2) Register a logical table

Every table requires a `partitionKeyColumn` — the column that holds each
row's partition key. This is mandatory, not optional: it's what lets
`/v1/mutate` and coordinated transactions (Milestone 1) structurally enforce
that a mutation only ever touches the one row/partition it claims to.

```bash
curl -X POST http://127.0.0.1:8787/admin/register-table \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"table":"events","partitionKeyColumn":"id"}'
```

### 3) Create the table's schema (admin-mediated, applies to every shard)

Schema changes (`CREATE TABLE`, etc.) are not allowed through `/v1/sql` — that
endpoint's deny-list blocks `CREATE`/`DROP`/`ALTER` to keep tenant-supplied SQL
restricted to data operations. Use `/admin/create-table` instead; it registers
the table and applies the schema to every physical shard across every catalog
shard so any tenant's rows land on a shard that already has the table.

```bash
curl -X POST http://127.0.0.1:8787/admin/create-table \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "table":"events",
    "schema":"CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, user_id TEXT, body TEXT, created_at TEXT)",
    "partitionKeyColumn":"id"
  }'
```

The `schema`'s `CREATE TABLE` name must match `table` exactly — a mismatch is
rejected with a 400 rather than silently creating a differently-named table.
`partitionKeyColumn` is validated against the schema's actual columns (via
`PRAGMA table_info`, not a hand-rolled parser) after the table is created on
every shard — if it doesn't exist, the table is rolled back (dropped from
every shard) and the call fails with a 400, rather than registering a table
whose structured/transactional paths could never work.

Tables registered before this validation existed (including anything live
from `v1.0.0.0`) carry a `'__unset__'` sentinel and are rejected from
`/v1/mutate` and coordinated transactions with a 409 until an operator runs:

```bash
curl -X POST http://127.0.0.1:8787/admin/set-partition-key-column \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"table":"events","partitionKeyColumn":"id"}'
```

Raw `/v1/sql` against an unupgraded table is unaffected — this gate only
blocks the new structured paths.

### 4) Register a tenant

`/v1/sql` and `/v1/mutate` are data-plane routes and require a tenant bearer
token, not `ADMIN_TOKEN` — this isolates apps/environments within one
deployment (see "Tenant authorization" below). `/register-tenant` returns the
plaintext token exactly once; store it.

```bash
curl -X POST http://127.0.0.1:8787/admin/register-tenant \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"tenantId":"t1"}'
```

Response: `{"ok":true,"tenantId":"t1","token":"<save this>"}`. Export it for
the rest of this walkthrough:

```bash
export TENANT_TOKEN=<token from the response above>
```

### 5) Insert data

```bash
curl -X POST http://127.0.0.1:8787/v1/sql \
  -H "content-type: application/json" \
  -H "authorization: Bearer $TENANT_TOKEN" \
  -d '{
    "table":"events",
    "tenantId":"t1",
    "partitionKey":"user-1",
    "requestId":"req-1",
    "sql":"INSERT INTO events (id, user_id, body, created_at) VALUES (?, ?, ?, ?)",
    "params":["e1","user-1","hello","2026-06-29T00:00:00Z"]
  }'
```

### 6) Query same partition

```bash
curl -X POST http://127.0.0.1:8787/v1/sql \
  -H "content-type: application/json" \
  -H "authorization: Bearer $TENANT_TOKEN" \
  -d '{
    "table":"events",
    "tenantId":"t1",
    "partitionKey":"user-1",
    "sql":"SELECT * FROM events WHERE user_id = ?",
    "params":["user-1"]
  }'
```

### 7) Structured mutation (row-owned, single-shard)

`/v1/mutate` is an alternative to raw SQL for writes: instead of a SQL string,
you describe the operation (`insert`/`update`/`delete`/`upsert`) and its
target. The partition-key column is always forced into the affected row —
`compileMutation` ANDs it into the `WHERE` clause for `update`/`delete` (even
if you supply no `where` at all, it only ever touches the one partitioned
row/set) and force-sets it in `values` for `insert`/`upsert`. This is what
raw `/v1/sql` cannot guarantee, since it's a trust-based passthrough.

```bash
curl -X POST http://127.0.0.1:8787/v1/mutate \
  -H "content-type: application/json" \
  -H "authorization: Bearer $TENANT_TOKEN" \
  -d '{
    "op":"insert",
    "table":"events",
    "tenantId":"t1",
    "partitionKey":"e1",
    "values":{"user_id":"user-1","body":"hello","created_at":"2026-06-29T00:00:00Z"}
  }'
```

Response: `{"ok":true,"rowsAffected":1}`.

### 8) Cross-shard atomic transaction

`/v1/tx` atomically commits a batch of structured mutations that may span
multiple shards, via two-phase commit (`CoordinatorDO`). Every mutation must
share the same `tenantId`; the batch is capped at 8 distinct
`(tenantId, table, partitionKey)` rows. `requestId` is required and doubles as
the transaction's idempotency key — retrying the same `requestId` returns the
prior outcome instead of re-running 2PC.

```bash
curl -X POST http://127.0.0.1:8787/v1/tx \
  -H "content-type: application/json" \
  -H "authorization: Bearer $TENANT_TOKEN" \
  -d '{
    "mutations": [
      {"op":"insert","table":"events","tenantId":"t1","partitionKey":"e1","values":{"user_id":"user-1","body":"a"}},
      {"op":"insert","table":"events","tenantId":"t1","partitionKey":"e2","values":{"user_id":"user-1","body":"b"}}
    ],
    "requestId": "req-tx-1"
  }'
```

Response: `{"ok":true,"txId":"...","status":"committed"}`. If any participant
shard fails to prepare, every participant is rolled back and the call returns
409 (`TX_ABORTED`) — nothing is left half-applied.

An operator can check on or force-abort a stuck transaction:

```bash
curl -X POST http://127.0.0.1:8787/admin/tx-status \
  -H "content-type: application/json" -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"txId":"..."}'

curl -X POST http://127.0.0.1:8787/admin/tx-force-abort \
  -H "content-type: application/json" -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"txId":"..."}'
```

### 9) Fan-out query (all shards)

`/v1/scatter` reads across every tenant indiscriminately, so it's an admin
operation — it requires `ADMIN_TOKEN`, not a tenant token.

```bash
curl -X POST http://127.0.0.1:8787/v1/scatter \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{
    "sql":"SELECT id, user_id, body FROM events",
    "params":[],
    "limit":100
  }'
```

### 10) Move one vBucket to a new shard (manual split prototype)

The cluster is partitioned across a fixed set of catalog shards (see
"Catalog sharding" below); `vbucket` numbering is local to one catalog shard,
so `catalogShardId` is required.

```bash
curl -X POST http://127.0.0.1:8787/admin/split-vbucket \
  -H "content-type: application/json" \
  -H "authorization: Bearer $ADMIN_TOKEN" \
  -d '{"catalogShardId":"catalog-0","vbucket":42,"newShardId":"shard-hotfix-1"}'
```

## Catalog sharding

The cluster is partitioned across a fixed, well-known set of catalog shards
(`catalog-0`, `catalog-1`, ... — count controlled by the `CATALOG_SHARD_COUNT`
var, default 4). A tenant's catalog shard is computed by hashing `tenantId` —
there's no lookup step, which avoids the bootstrapping problem of sharding the
metadata store itself. Cluster-wide admin operations (`/admin/init`,
`/admin/register-table`, `/admin/create-table`, `/admin/status`) fan out to
every catalog shard; shard-scoped operations (`/admin/split-vbucket`,
`/admin/drain-shard`) require an explicit `catalogShardId` since vBucket/shard
identifiers are local to one catalog shard.

`/admin/audit-log` fans out to every catalog shard and merges each shard's
last 100 admin actions (`/init`, `/register-table`, `/split-vbucket`,
`/drain-shard`) into one list sorted newest-first, tagged with the
`catalogShardId` that logged each entry.

`/admin/drain-shard` first checks the target shard's in-flight `/v1/tx`
transaction count and rejects with 409 (`SHARD_HAS_IN_FLIGHT_TRANSACTIONS`)
if any are still prepared — retry once they resolve (Chunk 3's recovery loop
bounds how long that takes), or use `/admin/tx-force-abort` to unstick one
manually. A shard that's already draining rejects any *new* `/v1/tx`/`/v1/sql`
write with 503.

## Tenant authorization

`/v1/sql` and `/v1/mutate` require a tenant bearer token (`POST
/admin/register-tenant {"tenantId": "..."}`, `ADMIN_TOKEN`-gated), separate
from `ADMIN_TOKEN` itself. This isolates apps/environments *within* one
deployment — it is not a multi-customer-SaaS boundary. In this project's
current self-hosted distribution model, the deploying developer holds both
the operator role (`ADMIN_TOKEN`) and every tenant role (`tenant_auth`
tokens) — but the two are kept structurally distinct in the code so a future
hosted layer (a genuinely separate operator) could be added without a
rewrite.

`POST /admin/register-tenant {"tenantId": "...", "rotate": true}` issues a
new token for an already-registered tenant, invalidating the old one
immediately (no grace period — a scheduled rotation can break in-flight
callers with zero overlap window; a known limitation, not yet addressed).
`POST /admin/revoke-tenant {"tenantId": "..."}` disables a tenant's access.

`/v1/scatter` reads across every tenant indiscriminately, so it requires
`ADMIN_TOKEN` rather than a tenant token.

## Known MVP limitations

- No SQL parser or policy sandboxing yet.
- No automatic backfill/dual-write during split.
- Cross-shard transactions (`/v1/tx`) are bounded to 8 participant rows.
- Global secondary indexes are not implemented.

## Next production steps

1. ~~Add authenticated tenant authorization in Gateway.~~ Done (Milestone 1 Chunk 0).
2. Introduce SQL allowlist/parser and bounded query plans.
3. Add automated split controller with backfill and dual-write cutover.
4. Add index service and query planner enhancements.
5. Add observability and SLO alerting per shard and per route.
