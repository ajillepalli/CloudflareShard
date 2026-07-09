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
- Zero-downtime live data movement in first cut.

Cross-shard transactional writes (2PC) were originally scoped out of the MVP (see prior
revision) but were reclassified as a non-negotiable day-one requirement, alongside global
secondary indexes, per the founder's explicit decision during product review — see
Milestone 1/2 in the `feature/next-stage` design doc. Section 10 reflects this.

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
- numShards number (default 8, clamped to 1-256)
- totalVBuckets number (default 1024, clamped to 64-65536)
- force boolean (optional)

Response:
- ok boolean
- numShards
- totalVBuckets

POST /admin/register-table
Request:
- table string
- partitioning string (optional, default hash)
- partitionKeyColumn string (required — the column holding each row's partition key)

Response:
- ok
- table
- metadataVersion

POST /admin/create-table
Request:
- table string
- schema string (must be a `CREATE TABLE` statement whose table name matches `table`)
- partitionKeyColumn string (required — validated via `PRAGMA table_info` against the created schema; the table is dropped from every shard and the call fails 400 if the column doesn't exist)

Response:
- ok
- table

POST /admin/set-partition-key-column (ADMIN_TOKEN)
Request:
- table string
- partitionKeyColumn string (validated via `PRAGMA table_info` against a live shard's schema)

Response:
- ok
- table
- partitionKeyColumn

Upgrades a table still carrying the `'__unset__'` sentinel (registered before `partitionKeyColumn` was mandatory, including anything live from `v1.0.0.0`) — such tables are otherwise rejected from `/v1/mutate` and coordinated transactions with a 409.

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

POST /admin/register-tenant (ADMIN_TOKEN)
Request:
- tenantId string
- rotate boolean (optional — issues a new token for an already-registered tenant, invalidating the old one immediately)

Response:
- ok
- tenantId
- token string (plaintext, returned exactly once)

POST /admin/revoke-tenant (ADMIN_TOKEN)
Request:
- tenantId string

Response:
- ok
- tenantId
- revoked boolean

POST /v1/sql (tenant bearer token)
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

POST /v1/mutate (tenant bearer token)
Request (a single StructuredMutation — see structured-op.ts):
- op string ("insert" | "update" | "delete" | "upsert")
- table string
- tenantId string
- partitionKey string
- where object (optional — additional predicates; never substitutes for the partition-key predicate, which is always injected)
- values object (optional — required for insert/upsert; the partition-key column is always force-set here regardless of what's supplied)
- conflictColumns array (optional, upsert only — defaults to [partitionKeyColumn])
- requestId string (optional — shares /v1/sql's requestId-based idempotency contract)

Response:
- ok boolean
- rowsAffected number

Row-ownership is structural, not conditional: `compileMutation` always ANDs the partition-key predicate into `update`/`delete`'s `WHERE` clause and force-sets it in `insert`/`upsert`'s `values` — an absent `where` still only ever touches the one partitioned row/set, never the whole table. Rejected 409 (`PARTITION_KEY_COLUMN_UNSET`) against a table still carrying the `'__unset__'` sentinel.

POST /v1/tx (tenant bearer token)
Request:
- mutations array of StructuredMutation (see /v1/mutate above), 1 or more, bounded to 8 distinct (tenantId, table, partitionKey) rows
- requestId string (required — the transaction's idempotency key; a retry with the same requestId returns the prior outcome rather than re-running 2PC)

Response (200, committed):
- ok: true
- txId string
- status: "committed" (or "committed_pending_ack" if a commit acknowledgement from one or more shards is still outstanding and queued for alarm-driven retry — the transaction is durably committed either way, only the ack is pending)

Response (409, aborted):
- error { code: "TX_ABORTED", message, details } — prepare failed on at least one participant shard; every participant was rolled back (or had nothing to roll back, given /prepare's shadow-write design)

Drives `CoordinatorDO`'s two-phase commit across every shard touched by the mutation set: each mutation is individually routed and validated (so a cross-tenant mutation in the same batch 401s the same way a lone `/v1/mutate` call would, with no separate check needed), grouped by shardId, then `/begin` fans out `/prepare` to all participants, aborts everyone on any failure, or fans out `/commit` on universal success.

POST /admin/tx-status (ADMIN_TOKEN)
Request:
- txId string

Response:
- found boolean
- status string (if found — preparing/prepared/committing/committed/aborted)

POST /admin/tx-force-abort (ADMIN_TOKEN)
Request:
- txId string

Response:
- ok boolean
- txId
- status: "aborted"

Manual escape hatch for a transaction stuck past a reasonable window (visible via `/admin/tx-status`) — aborts every participant shard and marks the transaction aborted. Rejects 409 if the transaction already committed.

POST /v1/scatter (ADMIN_TOKEN — reads across every tenant indiscriminately, so this is an admin operation, not a data-plane one)
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
  - If found and the stored request hash matches the incoming (sql, params), returns the previously stored result.
  - If found but the hash differs, rejects the replay with a 409 instead of returning a stale or wrong result.
  - If not found, executes mutation in transaction and records requestId plus a hash of (sql, params).

This prevents duplicate writes after network retries and stops a reused requestId from being replayed against different SQL/params.

## 10) Transaction Semantics

- Single-shard operations:
  - Strong, local ACID semantics.

- Cross-shard operations (Milestone 1 — Transaction Coordinator):
  - Full 2PC is the only mode, not feature-flagged. Cross-shard transactional writes were
    reclassified from "future, saga-first" to a non-negotiable day-one requirement (see
    Section 2). A caller derives a `StructuredMutation` set; the coordinator resolves
    participant shards and drives prepare/commit/abort across all of them atomically.
  - Bounded to at most 8 participant shards per transaction.
  - Requires the structured mutation contract (Section 7, `/v1/mutate`) and a mandatory
    partition-key-column convention on every registered table — both shipped.
  - **Shard-level primitives (implementation status: shipped and reachable via `/v1/tx`).**
    Each `ShardDO` exposes internal `/prepare`, `/commit`, `/abort`, `/tx-status`, and
    `/pending-intent-count` routes implementing the participant side of 2PC: `/prepare`
    validates a mutation by executing it inside a transaction and forcing a rollback (so a
    concurrent read never sees it), then durably records a lock + pending intent in a
    separate transaction; `/commit` re-executes for real; `/abort` has nothing to undo,
    since prepare never left anything applied. These are DO-binding-only, called from
    `CoordinatorDO`, never directly by the Worker.
  - **CoordinatorDO (Milestone 1 Chunk 3 — shipped).** One `CoordinatorDO` instance per
    transaction (`env.COORDINATOR.idFromName(txId)`, no sharding — see the cost-model
    decision in the milestone plan: Cloudflare DO billing has no per-instantiation cost, so
    the simpler keying wins over a sharded pool at this project's realistic near-term scale).
    `txId = sha256Hex([mutations[0].tenantId, requestId])`. `/begin` persists the
    transaction and its participants, fans out `/prepare` to every participant shard, aborts
    everyone on any failure, or fans out `/commit` on universal success. A commit
    acknowledgement that fails to reach one or more shards is queued in `recovery_queue` and
    retried via `alarm()` with exponential backoff — the transaction is already durably
    committed at that point, so this only affects when the shard-side state catches up, not
    correctness. `/v1/tx` (Section 7) is the public entry point.
  - Raw `/v1/sql` and `/v1/mutate` mutations against a row locked by an in-flight
    coordinated transaction reject 409 (`TX_PARTICIPANT_LOCKED`).

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

- Require authenticated principal at Gateway — implemented via `tenant_auth` bearer tokens (`/admin/register-tenant`), checked in `CatalogDO.handleRoute` before any routing info is returned.
- Verify tenantId belongs to principal before route — implemented: the caller's bearer token is hashed and compared against the claimed `tenantId`'s stored hash; missing/wrong/revoked tokens are all rejected with 401.
- This is a per-deployment authorization boundary (isolating apps/environments within one self-hosted deployment), not a multi-customer-SaaS boundary — see README.md's "Tenant authorization" section for the operator/tenant distinction this milestone's distribution model assumes.
- Enforce SQL policy allowlist in production (MVP currently permissive).

## 15) Migration Path to Production

- Replace permissive SQL with parsed/validated subset.
- Add query planner and local pre-aggregation for scatter reads.
- Add global secondary index service.
- Add automated split controller and background mover.
- Add backups and restore drills per shard.
