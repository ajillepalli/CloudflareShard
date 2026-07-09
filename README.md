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
  -d '{"numShards":4,"totalVBuckets":256}'
```

`numShards` is clamped to 1-256 and `totalVBuckets` to 64-65536 (defaults 8 and 1024)
so a single call can't provision an oversized, unrollbackable cluster.

### 2) Register a logical table

```bash
curl -X POST http://127.0.0.1:8787/admin/register-table \
  -H "content-type: application/json" \
  -d '{"table":"events"}'
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
    "schema":"CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, user_id TEXT, body TEXT, created_at TEXT)"
  }'
```

The `schema`'s `CREATE TABLE` name must match `table` exactly — a mismatch is
rejected with a 400 rather than silently creating a differently-named table.

### 4) Insert data

```bash
curl -X POST http://127.0.0.1:8787/v1/sql \
  -H "content-type: application/json" \
  -d '{
    "table":"events",
    "tenantId":"t1",
    "partitionKey":"user-1",
    "requestId":"req-1",
    "sql":"INSERT INTO events (id, user_id, body, created_at) VALUES (?, ?, ?, ?)",
    "params":["e1","user-1","hello","2026-06-29T00:00:00Z"]
  }'
```

### 5) Query same partition

```bash
curl -X POST http://127.0.0.1:8787/v1/sql \
  -H "content-type: application/json" \
  -d '{
    "table":"events",
    "tenantId":"t1",
    "partitionKey":"user-1",
    "sql":"SELECT * FROM events WHERE user_id = ?",
    "params":["user-1"]
  }'
```

### 6) Fan-out query (all shards)

```bash
curl -X POST http://127.0.0.1:8787/v1/scatter \
  -H "content-type: application/json" \
  -d '{
    "sql":"SELECT id, user_id, body FROM events",
    "params":[],
    "limit":100
  }'
```

### 7) Move one vBucket to a new shard (manual split prototype)

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

## Known MVP limitations

- No SQL parser or policy sandboxing yet.
- No automatic backfill/dual-write during split.
- Cross-shard transactions are not implemented.
- Global secondary indexes are not implemented.

## Next production steps

1. Add authenticated tenant authorization in Gateway.
2. Introduce SQL allowlist/parser and bounded query plans.
3. Add automated split controller with backfill and dual-write cutover.
4. Add index service and query planner enhancements.
5. Add observability and SLO alerting per shard and per route.
