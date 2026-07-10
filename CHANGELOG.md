# Changelog

All notable changes to this project are documented in this file.

## [Unreleased] — Milestone 2: Cross-Shard Index Service

On branch `feature/milestone-2-index-service` (PR #6, not yet merged to `main`).

### Added
- `/admin/create-index` — registers a secondary index (composite-capable) and backfills it against every existing row on every shard. Registers before backfilling (not after), so a row written mid-backfill is never permanently missed; backfill re-reads each row immediately before writing its index entry to avoid clobbering a concurrent write's fresher data.
- `/admin/list-indexes`, `/admin/drop-index` — index registry management. `DROP INDEX` unregisters in the catalog before fanning out physical cleanup, so queries see it gone immediately.
- `/v1/index-query` — the first tenant-facing, non-partition-key read path (exact full-tuple lookups only). Resolves in three hops without `/v1/scatter`'s full-cluster fan-out; re-verifies a matched row against the queried tuple before returning it, since async index maintenance can lag.
- Hybrid index-maintenance consistency: `/v1/mutate` maintains indexes asynchronously via `ctx.waitUntil()` with an alarm-driven retry queue (`index_pending_jobs`) for failed writes; `/v1/tx` maintains them synchronously as extra 2PC participants in the same transaction as the base row.
- `index_rules.status` (`building`/`ready`) — an index is live for write-path maintenance from the moment it's registered, but rejects reads (425 `INDEX_BUILDING`) until its backfill has fully completed.
- `/admin/drain-shard` and `/admin/split-vbucket` now reject 409 while any secondary index is registered cluster-wide — both change the active shard set that index placement hashes over, which would otherwise silently orphan existing index entries.
- Non-unique indexes only in this milestone; unique-index support and leftmost-prefix/range queries are tracked in `TODOS.md` for a future increment.

### Fixed (two independent `/plan-eng-review` + `codex review` passes against the implemented code)
- A row written during `CREATE INDEX`'s backfill window could be permanently unindexed — closed by registering before backfilling.
- Zero-row `update`/`delete` (a `where` clause that matches nothing) could still silently delete/rewrite a still-live row's index entry — closed on both `/v1/mutate` (gated on `rowsAffected`) and `/v1/tx` (shared WHERE-clause matching plus a null-`beforeRow` no-op rule).
- `/v1/index-query`'s `limit` was applied before the staleness re-check, so a run of stale entries could starve out live matches — fixed by paging raw entries and re-verifying batch by batch.
- Two mutations touching the same row within one `/v1/tx` batch (e.g. insert then update) computed the second mutation's index delta from a stale pre-transaction read, losing the index entry for the row's actual final value — fixed by tracking simulated per-row state across the batch.
- An insert/upsert omitting an indexed column that relies on a SQL `DEFAULT` would have indexed `null` instead of the real value — both write paths now require indexed columns explicitly (400 `INDEXED_COLUMN_REQUIRES_VALUE`).

## [1.1.0.0] - 2026-07-09

Milestone 1: cross-shard transactional writes via genuine two-phase commit, plus the data-plane tenant authorization and structured mutation contract it depends on (PR #4, PR #5).

### Added
- `tenant_auth` bearer tokens (`/admin/register-tenant`, `/admin/revoke-tenant`) — `CatalogDO./route` requires a valid tenant token before returning routing info, isolating apps/environments within one deployment.
- `/v1/mutate` — single-shard, row-owned structured mutations (`compileMutation`/`validateMutation`), plus a mandatory `partition_key_column` on every table (sentinel-tagged and 409-rejected for tables registered before this existed, upgradable via `/admin/set-partition-key-column`).
- `ShardDO` 2PC participant primitives: `/prepare` (validate-then-rollback shadow write, so nothing is visible to concurrent reads, then durably records a lock + pending intent), `/commit`, `/abort` — all idempotent. TTL sweep queries the coordinator's authoritative decision rather than unilaterally aborting on timeout.
- `CoordinatorDO` — one instance per transaction (`env.COORDINATOR.idFromName(txId)`, no sharding). `/v1/tx` drives `/begin`, which fans out `/prepare` to every participant shard, aborts everyone on any failure, or fans out `/commit` on universal success; an unacknowledged commit/abort is queued and retried by `alarm()` with backoff.
- `/admin/tx-status` and `/admin/tx-force-abort` — operator visibility and a manual escape hatch for a stuck transaction.
- `/admin/drain-shard` now blocks (409 `SHARD_HAS_IN_FLIGHT_TRANSACTIONS`) draining a shard with in-flight prepared transactions.

### Fixed (Codex review pass against the merged diff, PR #5)
- `ShardDO.handlePrepare` crashed with an uncaught `row_locks` PRIMARY KEY violation on a `/v1/tx` batch with two mutations against the same row on one shard (e.g. insert-then-update) — fixed with `INSERT OR IGNORE`.
- `CoordinatorDO./begin` had no idempotency-mismatch check: retrying an existing `txId` with a *different* mutation set could silently resume 2PC with new data or return a stale "committed" for content never applied — fixed by hashing the participant set per transaction and rejecting mismatches 409 (`TX_ID_REQUEST_MISMATCH`).
- `/admin/create-table`'s rollback (on a `partitionKeyColumn` mismatch) dropped the physical table but left the create-table `requestId` cached as "applied," blocking the documented fix-and-retry flow — fixed by adding `ShardDO./invalidate-request` and calling it alongside the rollback's `DROP TABLE`.

## [1.0.0.0] - 2026-07-08

First shippable release of the sharded-SQL platform: multi-shard catalog routing, an admin control plane, and a security-hardened SQL execution path.

### Added
- Catalog sharding: the control plane is now split across multiple catalog-shard Durable Objects (`catalog-0..N-1`), routed by a pure hash function on tenant ID — no lookup or bootstrapping step required.
- `/admin/create-table` — admin-mediated schema creation, fanned out to every shard.
- `/admin/shard-stats` — per-shard row counts and idempotency-table size.
- `/admin/audit-log` — queryable audit trail of admin actions (`/init`, `/register-table`, `/split-vbucket`, `/drain-shard`).
- Structured JSON logging across the Worker, Catalog, and Shard layers.
- A non-destructive post-deploy smoke test (`scripts/smoke-test.mjs`).
- vitest + `@cloudflare/vitest-pool-workers` test suite (114 tests) covering routing, admin auth, idempotency, and error boundaries.
- `TODOS.md` tracking open decisions: distribution/positioning, automatic split heuristics, cross-tenant analytics.

### Changed
- Every admin endpoint now requires a bearer admin token, enforced both per-route and structurally at the Worker's `/admin/*` gate.
- Admin token comparison is constant-time, closing a timing side-channel.
- Shard/vbucket fan-out during `/admin/init` is now batched with a concurrency cap instead of unbounded parallel requests, preventing resource exhaustion on large clusters.
- `/admin/init` clamps `numShards` and `totalVBuckets` to a bounded range so a single call can't provision an oversized, unrollbackable cluster.
- Error responses no longer leak raw SQL or driver error messages to callers.

### Fixed
- Closed a critical bypass where a SQL comment (e.g. `-- x\nDELETE FROM t`) defeated mutation classification, letting an unauthenticated `/v1/scatter` call execute destructive writes across every shard as if it were a read. Shard execution now derives the mutation classification itself instead of trusting the caller.
- Closed the same class of bypass via a leading `WITH`/CTE clause (e.g. `WITH x AS (SELECT 1) DELETE FROM events`), found by a Codex adversarial pass against the final diff.
- Idempotent mutation replay now detects a `requestId` reused with different SQL/params and rejects the replay instead of silently returning a stale result. The mismatch check now uses SHA-256 instead of a 32-bit hash, which collided easily enough to defeat the check.
- `/admin/create-table` now verifies the `CREATE TABLE` statement's table name matches the declared table name, rejecting a schema that would create a differently-named table.
- `/admin/init` now rejects non-numeric `numShards`/`totalVBuckets` instead of silently propagating `NaN` into shard/vbucket provisioning (which mapped every vbucket to a nonexistent `shard-NaN` while still returning 200 OK).
- `/admin/audit-log` is now actually reachable through the Worker — it was documented and tested against `CatalogDO` directly, but never registered in the Worker's route table.
- Replaced raw `BEGIN`/`COMMIT` SQL (rejected by Durable Object SQLite storage) with `ctx.storage.transactionSync()`.
- Fixed `/stats` querying Cloudflare's internal `_cf_METADATA` system table.
- Added a schema migration guard so pre-existing catalog/shard tables get missing columns backfilled instead of failing.
- Added error boundaries so malformed request bodies return a clean error response instead of an unhandled crash.
