# CloudflareShard MVP

A concrete MVP for a sharded SQL layer on top of Cloudflare Durable Objects (SQLite-backed).

## What this prototype demonstrates

- One logical SQL endpoint in a Worker.
- Catalog DO as control plane for table registry and vBucket map.
- Shard DOs as single-threaded SQLite execution nodes.
- Deterministic single-shard routing via tenantId + table + partitionKey.
- Scatter read endpoint for fan-out SELECT.
- Manual vBucket reassignment for basic rebalancing.
- Mutation idempotency via requestId.

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
cd CloudflareShared
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

### 2) Register a logical table

```bash
curl -X POST http://127.0.0.1:8787/admin/register-table \
  -H "content-type: application/json" \
  -d '{"table":"events"}'
```

### 3) Create table (route to one shard)

```bash
curl -X POST http://127.0.0.1:8787/v1/sql \
  -H "content-type: application/json" \
  -d '{
    "table":"events",
    "tenantId":"t1",
    "partitionKey":"user-1",
    "sql":"CREATE TABLE IF NOT EXISTS events (id TEXT PRIMARY KEY, user_id TEXT, body TEXT, created_at TEXT)",
    "params":[]
  }'
```

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

```bash
curl -X POST http://127.0.0.1:8787/admin/split-vbucket \
  -H "content-type: application/json" \
  -d '{"vbucket":42,"newShardId":"shard-hotfix-1"}'
```

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
# Cloudflare Shard MVP

A concrete MVP for a Durable Object based sharded SQLite service on Cloudflare.

This project provides:
- Catalog Durable Object for vBucket routing metadata.
- Shard Durable Object for SQLite query execution.
- Gateway Worker endpoints for single-shard SQL and scatter reads.
- Admin APIs to initialize and rebalance cluster mappings.

## Project Structure

- src/index.ts: Gateway Worker routes.
- src/catalog.ts: Control-plane Durable Object.
- src/shard.ts: Data-plane Durable Object.
- docs/SPEC.md: Concrete architecture and protocol specification.

## Prerequisites

- Node.js 20+
- Cloudflare account
- Wrangler authenticated (wrangler login)

## Install

1. cd C:/Users/ananth.jillepalli/repos/CloudflareShard
2. npm install

## Run Locally

1. npm run dev
2. In a separate terminal, initialize cluster:

PowerShell example:
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8787/admin/init" -ContentType "application/json" -Body '{"numShards":4,"totalVBuckets":256}'

3. Register a logical table:
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8787/admin/register-table" -ContentType "application/json" -Body '{"table":"orders"}'

4. Create table on one shard (tenant + partitionKey define route):
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8787/v1/sql" -ContentType "application/json" -Body '{"sql":"CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, tenant_id TEXT, customer_id TEXT, amount REAL)","table":"orders","tenantId":"t1","partitionKey":"c1"}'

5. Insert row:
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8787/v1/sql" -ContentType "application/json" -Body '{"sql":"INSERT INTO orders (id, tenant_id, customer_id, amount) VALUES (?, ?, ?, ?)","params":["o1","t1","c1",42.5],"table":"orders","tenantId":"t1","partitionKey":"c1"}'

6. Query same partition:
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8787/v1/sql" -ContentType "application/json" -Body '{"sql":"SELECT * FROM orders WHERE customer_id = ?","params":["c1"],"table":"orders","tenantId":"t1","partitionKey":"c1"}'

7. Scatter query (debug/admin use):
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8787/v1/scatter" -ContentType "application/json" -Body '{"sql":"SELECT * FROM orders","limit":100}'

## Deploy

- npm run deploy

## Current MVP Limitations

- SQL is not fully parsed; caller provides table + partitionKey metadata.
- Cross-shard joins are not supported.
- Rebalance endpoint updates map only; no live row migration yet.
- Scatter reads are expensive and intended for operational/admin use.

## Next Steps

- Add SQL parser to infer table and partition predicate.
- Add background vBucket mover and dual-write cutover.
- Add auth middleware and tenant policy checks.
- Add global secondary index service for non-partition queries.
