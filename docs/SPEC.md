# Cloudflare Durable Object Sharded SQL: Concrete Spec (MVP + v1)

## 1) Goals

- Provide one logical SQL endpoint to application developers.
- Automatically route I/O to many Durable Object SQLite shards.
- Keep strict ACID within a shard.
- Allow online scale-out before any shard reaches 10 GB.
- Keep control-plane metadata strongly consistent via a single catalog Durable Object.

## 2) Non-goals (MVP)

- Full ANSI SQL parser/planner.
- Transparent cross-shard joins.
- Strict global serializable transactions across shards.
- Zero-downtime live data movement in first cut.

## 3) Components

- Gateway Worker
  - Public endpoint.
  - AuthN/AuthZ hook.
  - Request validation.
  - Routes to catalog and shard DOs.

- CatalogDO (control plane)
  - Owns table registry.
  - Owns vBucket to shard map.
  - Owns metadata version.
  - Exposes route and rebalance APIs.

- ShardDO (data plane)
  - Owns one SQLite database.
  - Executes SQL statements serially.
  - Maintains idempotency table for at-least-once retries.

## 4) Logical Data Partitioning

- Partition function input:
  - tenantId
  - table
  - partitionKey

- Hashing:
  - FNV-1a 32-bit on composite key tenantId:table:partitionKey.

- Virtual partitions:
  - totalVBuckets default 1024 (configurable).
  - vbucket = hash(composite) mod totalVBuckets.

- Mapping:
  - Catalog maps vbucket -> shardId.
  - Rebalance changes only mapping entries, not hash function.

## 5) Catalog Schema

Table: cluster_config
- singleton INTEGER PRIMARY KEY CHECK(singleton = 1)
- total_vbuckets INTEGER NOT NULL
- metadata_version INTEGER NOT NULL DEFAULT 1
- initialized_at TEXT NOT NULL

Table: shards
- shard_id TEXT PRIMARY KEY
- status TEXT NOT NULL (active, draining)
- created_at TEXT NOT NULL

Table: vbucket_map
- vbucket INTEGER PRIMARY KEY
- shard_id TEXT NOT NULL
- map_version INTEGER NOT NULL
- updated_at TEXT NOT NULL

Table: table_rules
- table_name TEXT PRIMARY KEY
- partitioning TEXT NOT NULL (hash for MVP)
- created_at TEXT NOT NULL

## 6) Shard Schema

Table: applied_requests
- request_id TEXT PRIMARY KEY
- result_json TEXT NOT NULL
- applied_at TEXT NOT NULL

Application tables are created by tenant SQL statements routed to target shards.

## 7) Public HTTP API (Gateway Worker)

POST /admin/init
Request:
- numShards number (default 8)
- totalVBuckets number (default 1024)
- force boolean (optional)

Response:
- ok boolean
- numShards
- totalVBuckets

POST /admin/register-table
Request:
- table string
- partitioning string (optional, default hash)

Response:
- ok
- table
- metadataVersion

POST /admin/split-vbucket
Request:
- vbucket number
- newShardId string (optional)

Response:
- ok
- vbucket
- fromShard
- toShard
- metadataVersion

POST /v1/sql
Request:
- sql string
- params array (optional)
- table string
- tenantId string
- partitionKey string (required for all writes and single-shard reads)
- requestId string (optional)

Response:
- route { shardId, vbucket, metadataVersion }
- requestId
- result (query or mutation payload)

POST /v1/scatter
Request:
- sql string (SELECT only)
- params array (optional)
- limit number (optional)

Response:
- shardCount
- rows
- perShard

## 8) Routing Algorithm

1. Validate table, tenantId, partitionKey.
2. Catalog verifies table is registered.
3. Compute vbucket.
4. Read vbucket_map to find shardId.
5. Forward request to ShardDO named shardId.
6. Return shard response with routing metadata.

## 9) Write Idempotency Contract

- Client may provide requestId.
- If omitted, Gateway generates UUID.
- For mutating SQL:
  - Shard checks applied_requests by requestId.
  - If found, returns previously stored result.
  - If not found, executes mutation in transaction and records requestId.

This prevents duplicate writes after network retries.

## 10) Transaction Semantics

- Single-shard operations:
  - Strong, local ACID semantics.

- Cross-shard operations (future):
  - Saga mode default.
  - Optional 2PC mode behind feature flag.

## 11) Rebalancing and Split (MVP)

Trigger conditions (future automation):
- shard DB size > threshold (example 7 GB soft limit)
- sustained write QPS above threshold
- p95 latency above threshold

MVP manual split flow:
1. Create/ensure destination shard exists.
2. Update vbucket_map for selected vbucket.
3. Increment metadata_version.

v1 automated safe split flow (planned):
1. Mark vbucket as splitting in catalog.
2. Start dual-write for affected keyspace.
3. Backfill rows in chunks.
4. Atomic map flip with new metadata version.
5. Stop dual-write and drain old shard path.

## 12) Query Planning Rules (MVP)

- Writes must be single-shard and require partitionKey.
- Reads without partitionKey are rejected on /v1/sql.
- Fan-out reads allowed only through /v1/scatter and should be capped.

## 13) Observability (required for production)

Emit structured logs and counters for:
- route_lookup_ms
- shard_execute_ms
- shard_row_count
- scatter_shard_count
- duplicate_request_hits
- metadata_version

## 14) Security and Multi-tenancy

- Require authenticated principal at Gateway.
- Verify tenantId belongs to principal before route.
- Enforce SQL policy allowlist in production (MVP currently permissive).

## 15) Migration Path to Production

- Replace permissive SQL with parsed/validated subset.
- Add query planner and local pre-aggregation for scatter reads.
- Add global secondary index service.
- Add automated split controller and background mover.
- Add backups and restore drills per shard.
